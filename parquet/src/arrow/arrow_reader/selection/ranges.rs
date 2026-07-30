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

//! Mapping the [`RowSelector`] runs of a selection onto ranges: the byte ranges
//! of the data pages that must be fetched ([`RowSelection::scan_ranges`]) and
//! the expansion of a selection to batch boundaries.
//!
//! Both are shared by the selector and mask backings, which stream their runs
//! from a slice and a [`MaskRunIter`] respectively.
//!
//! [`MaskRunIter`]: crate::arrow::arrow_reader::MaskRunIter

use super::{RowSelection, RowSelector};
use crate::file::page_index::offset_index::PageLocation;
use std::ops::Range;

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

                continue;
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
        if selector.skip {
            row_offset += selector.row_count;
        } else {
            let start = row_offset;
            let end = row_offset + selector.row_count;

            // Expand start to batch boundary
            let expanded_start = (start / batch_size) * batch_size;
            // Expand end to batch boundary
            let expanded_end = end.div_ceil(batch_size) * batch_size;
            let expanded_end = expanded_end.min(total_rows);

            expanded_ranges.push(expanded_start..expanded_end);
            row_offset += selector.row_count;
        }
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
