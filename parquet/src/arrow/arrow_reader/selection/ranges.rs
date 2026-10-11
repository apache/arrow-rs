// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
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

        let selected = if row_start == row_end {
            // Multiple pages can start at the same row. Preserve the run
            // scanner's behavior for these zero-width page ranges.
            mask.value(row_start)
        } else {
            UnalignedBitChunk::new(
                mask.values(),
                mask.offset() + row_start,
                row_end - row_start,
            )
            .iter()
            .any(|word| word != 0)
        };
        if selected {
            let start = page.offset as u64;
            ranges.push(start..start + page.compressed_page_size as u64);
        }
        row_start = row_end;
    }
    ranges
}

/// Byte ranges of the data pages containing at least one selected row.
#[inline]
pub(super) fn scan_ranges_from_selectors<I>(
    selectors: I,
    page_locations: &[PageLocation],
) -> Vec<Range<u64>>
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut ranges: Vec<Range<u64>> = vec![];
    let mut row_offset = 0;

    let mut pages = page_locations.iter().peekable();
    let mut selectors = selectors.into_iter();
    let mut current_selector = selectors.next();
    let mut current_page = pages.next();

    let mut current_page_included = false;

    while let Some((selector, page)) = current_selector.as_mut().zip(current_page) {
        if !(selector.skip || current_page_included) {
            let start = page.offset as u64;
            let end = start + page.compressed_page_size as u64;
            ranges.push(start..end);
            current_page_included = true;
        }

        if let Some(next_page) = pages.peek() {
            if row_offset + selector.row_count > next_page.first_row_index as usize {
                let remaining_in_page = next_page.first_row_index as usize - row_offset;
                selector.row_count -= remaining_in_page;
                row_offset += remaining_in_page;
                current_page = pages.next();
                current_page_included = false;
            } else {
                if row_offset + selector.row_count == next_page.first_row_index as usize {
                    current_page = pages.next();
                    current_page_included = false;
                }
                row_offset += selector.row_count;
                current_selector = selectors.next();
            }
        } else {
            if !(selector.skip || current_page_included) {
                let start = page.offset as u64;
                let end = start + page.compressed_page_size as u64;
                ranges.push(start..end);
            }
            current_selector = selectors.next()
        }
    }

    ranges
}

/// Grows each selected run to the batch boundaries containing it, merging the
/// runs that overlap as a result.
#[inline]
pub(super) fn expand_to_batch_boundaries_from_selectors<I>(
    selectors: I,
    batch_size: usize,
    total_rows: usize,
) -> RowSelection
where
    I: IntoIterator<Item = RowSelector>,
{
    let mut expanded_ranges = Vec::new();
    let mut row_offset = 0;

    for selector in selectors {
        if !selector.skip {
            let start = row_offset;
            let end = row_offset + selector.row_count;

            // Expand start to batch boundary
            let expanded_start = (start / batch_size) * batch_size;
            // Expand end to batch boundary
            let expanded_end = end.div_ceil(batch_size) * batch_size;
            let expanded_end = expanded_end.min(total_rows);

            expanded_ranges.push(expanded_start..expanded_end);
        }
        row_offset += selector.row_count;
    }

    // Sort ranges by start position
    expanded_ranges.sort_by_key(|range| range.start);

    // Merge overlapping or consecutive ranges
    let mut merged_ranges: Vec<Range<usize>> = Vec::new();
    for range in expanded_ranges {
        if let Some(last) = merged_ranges.last_mut() {
            if range.start <= last.end {
                // Overlapping or consecutive - merge them
                last.end = last.end.max(range.end);
            } else {
                // No overlap - add new range
                merged_ranges.push(range);
            }
        } else {
            // First range
            merged_ranges.push(range);
        }
    }

    RowSelection::from_consecutive_ranges(merged_ranges.into_iter(), total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BooleanArray;

    #[test]
    fn test_mask_scan_ranges_matches_selectors() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};

        for len in [0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 511, 2049] {
            for page_rows in [1, 7, 31, 63, 64, 65, 127, 257] {
                // Include pages beyond the selection and a final partial page.
                let total_rows = len + 37;
                let pages: Vec<_> = (0..total_rows)
                    .step_by(page_rows)
                    .enumerate()
                    .map(|(idx, start)| PageLocation {
                        offset: (idx * 23 + 5) as i64,
                        compressed_page_size: (idx % 13 + 1) as i32,
                        first_row_index: start as i64,
                    })
                    .collect();

                for offset in [0, 1, 3, 7, 31, 63, 64, 65] {
                    for pattern in 0..7 {
                        let mut rng = StdRng::seed_from_u64(42);
                        let mask = BooleanBuffer::from_iter((0..offset + len + 64).map(|row| {
                            if !(offset..offset + len).contains(&row) {
                                // Padding must not cause extra pages to load.
                                return true;
                            }
                            let row = row - offset;
                            match pattern {
                                0 => false,
                                1 => true,
                                2 => row % 2 == 0,
                                3 => row % page_rows == page_rows - 1,
                                4 => rng.random_bool(0.01),
                                5 => rng.random_bool(0.5),
                                _ => rng.random_bool(0.99),
                            }
                        }))
                        .slice(offset, len);
                        let selectors =
                            RowSelection::from_filters(&[BooleanArray::new(mask.clone(), None)]);
                        let selection = RowSelection::from_boolean_buffer(mask);
                        assert_eq!(
                            selection.scan_ranges(&pages),
                            selectors.scan_ranges(&pages),
                            "len={len}, page_rows={page_rows}, offset={offset}, pattern={pattern}"
                        );
                        assert_eq!(
                            selection.row_ranges_for_selected_pages(&pages, total_rows),
                            selectors.row_ranges_for_selected_pages(&pages, total_rows)
                        );
                        assert!(selection.scan_ranges(&[]).is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn test_mask_scan_ranges_repeated_page_row_offsets() {
        let pages: Vec<_> = [0, 0, 1, 1, 7, 8, 31, 32, 63, 63, 64, 65, 127, 128, 129]
            .into_iter()
            .enumerate()
            .map(|(idx, row)| PageLocation {
                offset: (idx * 10) as i64,
                compressed_page_size: 10,
                first_row_index: row,
            })
            .collect();
        assert!(
            RowSelection::from_boolean_buffer(BooleanBuffer::new_unset(0))
                .scan_ranges(&pages)
                .is_empty()
        );
        for len in [1, 7, 63, 64, 65, 128, 129, 257] {
            for offset in [0, 1, 3, 7, 63, 65] {
                for selected_row in 0..len {
                    let mask = BooleanBuffer::from_iter(
                        (0..offset + len + 64).map(|row| row == offset + selected_row),
                    )
                    .slice(offset, len);
                    let selectors =
                        RowSelection::from_filters(&[BooleanArray::new(mask.clone(), None)]);
                    let selection = RowSelection::from_boolean_buffer(mask);
                    assert_eq!(selection.scan_ranges(&pages), selectors.scan_ranges(&pages));
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
