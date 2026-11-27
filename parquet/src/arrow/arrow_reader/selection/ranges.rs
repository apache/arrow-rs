// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Mapping row selections onto ranges: the byte ranges
//! of the data pages that must be fetched ([`RowSelection::scan_ranges`]) and
//! the expansion of a selection to batch boundaries.
//!
//! Page selection checks bitmap-backed selections directly, while batch
//! expansion streams their runs using [`MaskRunIter`].
//!
//! [`MaskRunIter`]: crate::arrow::arrow_reader::MaskRunIter

use super::{RowSelection, RowSelector};
use crate::file::page_index::offset_index::PageLocation;
use arrow_buffer::BooleanBuffer;
use arrow_buffer::bit_chunk_iterator::BitChunks;
use std::ops::Range;

/// Checks each page for a selected bit without expanding the mask into runs.
pub(super) fn scan_ranges_from_mask(
    mask: &BooleanBuffer,
    page_locations: &[PageLocation],
) -> Vec<Range<u64>> {
    let mut ranges = Vec::new();
    let mut row_start = 0;
    for (idx, page) in page_locations.iter().enumerate() {
        if row_start >= mask.len() {
            break;
        }
        let row_end = page_locations
            .get(idx + 1)
            .map_or(mask.len(), |next| next.first_row_index as usize)
            .min(mask.len());

        let chunks = BitChunks::new(
            mask.values(),
            mask.offset() + row_start,
            row_end - row_start,
        );
        if chunks.iter_padded().any(|word| word != 0) {
            let start = page.offset as u64;
            ranges.push(start..start + page.compressed_page_size as u64);
        }
        row_start = row_end;
    }
    ranges
}

/// Byte ranges of the data pages containing at least one selected row.
///
/// The cost is `O(log pages)` for each skip and `O(pages)` for each selected
/// run in the pages that it covers. Thus, a selection of a few rows of a
/// large column chunk does not visit all pages.
#[inline]
pub(super) fn scan_ranges_from_selectors<I>(
    selectors: I,
    page_locations: &[PageLocation],
) -> Vec<Range<u64>>
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut ranges: Vec<Range<u64>> = vec![];
    if page_locations.is_empty() {
        return ranges;
    }
    // The first row that the selectors did not cover yet
    let mut row_offset = 0;
    // The page that holds `row_offset`
    let mut page_idx = 0;
    let mut current_page_included = false;

    for selector in selectors {
        let end = row_offset + selector.row_count;
        if selector.skip {
            // Go to the page that holds the first row after the skip
            let after = page_locations[page_idx..]
                .partition_point(|page| page.first_row_index as usize <= end);
            if after > 1 {
                page_idx += after - 1;
                current_page_included = false;
            }
            row_offset = end;
            continue;
        }
        // Include each page that holds a row of this run
        while row_offset < end {
            let page = &page_locations[page_idx];
            if !current_page_included {
                let start = page.offset as u64;
                ranges.push(start..start + page.compressed_page_size as u64);
                current_page_included = true;
            }
            match page_locations.get(page_idx + 1) {
                Some(next_page) if next_page.first_row_index as usize <= end => {
                    row_offset = next_page.first_row_index as usize;
                    page_idx += 1;
                    current_page_included = false;
                }
                _ => row_offset = end,
            }
        }
    }

    ranges
}

/// Grows each selected run to the batch boundaries containing it, merging
/// overlapping or adjacent ranges.
#[inline]
pub(super) fn expand_to_batch_boundaries_from_selectors<I>(
    selectors: I,
    batch_size: usize,
    total_rows: usize,
) -> RowSelection
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut row_offset = 0;

    // Selectors advance in row order, so expanded ranges can be merged as we go.
    for selector in selectors {
        let row_start = row_offset;
        row_offset += selector.row_count;
        if selector.skip {
            continue;
        }

        // Round the start down and the end up to cover complete batches.
        let batch_start = (row_start / batch_size) * batch_size;
        let batch_end = row_offset.next_multiple_of(batch_size).min(total_rows);

        // Ends are non-decreasing because selectors advance in row order.
        match ranges.last_mut() {
            // The ranges overlap or touch, so extend the last range.
            Some(last) if batch_start <= last.end => last.end = batch_end,
            // There is a gap, or this is the first selected range.
            _ => ranges.push(batch_start..batch_end),
        }
    }

    RowSelection::from_consecutive_ranges(ranges.into_iter(), total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scan that visits every page, as a reference.
    fn scan_all_pages(selectors: &[RowSelector], pages: &[PageLocation]) -> Vec<Range<u64>> {
        let mut ranges = vec![];
        let mut row = 0;
        for selector in selectors {
            let run = row..row + selector.row_count;
            row = run.end;
            if selector.skip || run.is_empty() {
                continue;
            }
            for (idx, page) in pages.iter().enumerate() {
                let first = page.first_row_index as usize;
                let end = pages
                    .get(idx + 1)
                    .map_or(usize::MAX, |next| next.first_row_index as usize);
                let range =
                    page.offset as u64..(page.offset + page.compressed_page_size as i64) as u64;
                if first < run.end && run.start < end && ranges.last() != Some(&range) {
                    ranges.push(range);
                }
            }
        }
        ranges
    }

    #[test]
    fn scan_ranges_from_selectors_matches_a_scan_of_all_pages() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..5_000 {
            let mut first_row = 0;
            let pages: Vec<PageLocation> = (0..rng.random_range(0..12))
                .map(|idx| {
                    let page = PageLocation {
                        offset: 100 * idx,
                        compressed_page_size: 100,
                        first_row_index: first_row,
                    };
                    first_row += rng.random_range(1..8);
                    page
                })
                .collect();
            // Runs that are empty, end at a page boundary, cover many pages,
            // and go past the last page.
            let mut skip = rng.random_bool(0.5);
            let selectors: Vec<RowSelector> = (0..rng.random_range(0..10))
                .map(|_| {
                    skip = !skip || rng.random_bool(0.1);
                    let row_count = [0, 1, 2, 3, 7, 20, 60][rng.random_range(0..7)];
                    match skip {
                        true => RowSelector::skip(row_count),
                        false => RowSelector::select(row_count),
                    }
                })
                .collect();
            assert_eq!(
                scan_ranges_from_selectors(selectors.iter().copied(), &pages),
                scan_all_pages(&selectors, &pages),
                "{selectors:?} {pages:?}"
            );
        }
    }

    fn page_locations(starts: impl IntoIterator<Item = usize>) -> Vec<PageLocation> {
        starts
            .into_iter()
            .enumerate()
            .map(|(idx, row)| PageLocation {
                offset: (idx * 23 + 5) as i64,
                compressed_page_size: (idx % 13 + 1) as i32,
                first_row_index: row as i64,
            })
            .collect()
    }

    fn assert_mask_scan_ranges(
        mask: &BooleanBuffer,
        offset: usize,
        pages: &[PageLocation],
        total_rows: usize,
    ) {
        // Build the reference from individual bits, independent of word iterators.
        let selectors = RowSelection::from_consecutive_ranges(
            mask.iter()
                .enumerate()
                .filter_map(|(row, selected)| selected.then_some(row..row + 1)),
            mask.len(),
        );
        // Set padding bits to ensure they cannot cause extra pages to load.
        let padded = BooleanBuffer::from_iter(
            std::iter::repeat_n(true, offset)
                .chain(mask.iter())
                .chain(std::iter::repeat_n(true, 64)),
        );
        let selection = RowSelection::from_boolean_buffer(padded.slice(offset, mask.len()));
        assert_eq!(
            selection.scan_ranges(pages),
            selectors.scan_ranges(pages),
            "mask={mask:?}, offset={offset}, pages={pages:?}"
        );
        assert_eq!(
            selection.row_ranges_for_selected_pages(pages, total_rows),
            selectors.row_ranges_for_selected_pages(pages, total_rows)
        );
        assert!(selection.scan_ranges(&[]).is_empty());
    }

    #[test]
    fn test_mask_scan_ranges_matches_selectors() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};

        for len in [0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 511, 2049] {
            for page_rows in [1, 7, 31, 63, 64, 65, 127, 257] {
                // Include pages beyond the selection and a final partial page.
                let total_rows = len + 37;
                let pages = page_locations((0..total_rows).step_by(page_rows));
                for pattern in 0..7 {
                    let mut rng = StdRng::seed_from_u64(42);
                    let mask = BooleanBuffer::from_iter((0..len).map(|row| match pattern {
                        0 => false,
                        1 => true,
                        2 => row % 2 == 0,
                        3 => row % page_rows == page_rows - 1,
                        4 => rng.random_bool(0.01),
                        5 => rng.random_bool(0.5),
                        _ => rng.random_bool(0.99),
                    }));
                    for offset in [0, 1, 3, 7, 31, 63, 64, 65] {
                        assert_mask_scan_ranges(&mask, offset, &pages, total_rows);
                    }
                }
            }
        }
    }

    #[test]
    fn test_mask_scan_ranges_single_bits_and_padding() {
        let pages = page_locations([0, 1, 130, 257, 386, 512, 549]);
        for len in [130, 257, 511, 512] {
            let selected_rows = std::iter::once(None).chain(
                [
                    0, 1, 7, 8, 55, 56, 63, 64, 65, 127, 128, 129, 130, 255, 256, 257, 510, 511,
                ]
                .into_iter()
                .filter(|&row| row < len)
                .map(Some),
            );
            for selected_row in selected_rows {
                let mask = BooleanBuffer::from_iter((0..len).map(|row| selected_row == Some(row)));
                for offset in [0, 1, 7, 8, 31, 63, 64, 65] {
                    assert_mask_scan_ranges(&mask, offset, &pages, 600);
                }
            }
        }
    }

    #[test]
    fn test_scan_ranges() {
        let index = vec![
            PageLocation {
                offset: 0,
                compressed_page_size: 10,
                first_row_index: 0,
            },
            PageLocation {
                offset: 10,
                compressed_page_size: 10,
                first_row_index: 10,
            },
            PageLocation {
                offset: 20,
                compressed_page_size: 10,
                first_row_index: 20,
            },
            PageLocation {
                offset: 30,
                compressed_page_size: 10,
                first_row_index: 30,
            },
            PageLocation {
                offset: 40,
                compressed_page_size: 10,
                first_row_index: 40,
            },
            PageLocation {
                offset: 50,
                compressed_page_size: 10,
                first_row_index: 50,
            },
            PageLocation {
                offset: 60,
                compressed_page_size: 10,
                first_row_index: 60,
            },
        ];

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select across page boundaries
            RowSelector::select(12),
            // Skip final page
            RowSelector::skip(12),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, false]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60]);
        assert_eq!(
            selection.row_ranges_for_selected_pages(&index, 70),
            vec![10..20, 20..30, 40..50, 50..60]
        );

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select across page boundaries
            RowSelector::select(12),
            RowSelector::skip(1),
            // Select across page boundaries including final page
            RowSelector::select(8),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to page boundary
            RowSelector::skip(5),
            RowSelector::select(5),
            // Skip full page past page boundary
            RowSelector::skip(12),
            // Select to final page boundary
            RowSelector::select(12),
            RowSelector::skip(1),
            // Skip across final page boundary
            RowSelector::skip(8),
            // Select from final page
            RowSelector::select(4),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

        let selection = RowSelection::from(vec![
            // Skip first page
            RowSelector::skip(10),
            // Multiple selects in same page
            RowSelector::select(3),
            RowSelector::skip(3),
            RowSelector::select(4),
            // Select to remaining in page and first row of next page
            RowSelector::skip(5),
            RowSelector::select(6),
            // Skip remaining
            RowSelector::skip(50),
        ]);

        let ranges = selection.scan_ranges(&index);

        // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
        assert_eq!(ranges, vec![10..20, 20..30, 30..40]);
    }
}
