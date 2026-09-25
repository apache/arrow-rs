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

//! The byte ranges a column chunk read loads, each tagged with the selected
//! rows it serves.
//!
//! The push decoder uses these spans to decide which pages the next batch
//! needs and when a page can be released.

use crate::arrow::arrow_reader::RowSelection;
use crate::file::page_index::offset_index::PageLocation;
use std::ops::Range;

/// What a [`PageSpan`] contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpanKind {
    /// The dictionary page of a column chunk.
    Dictionary,
    /// One data page.
    Data,
    /// A complete column chunk, because page locations are not known.
    ColumnChunk,
}

/// A byte range that a column chunk read loads, and the selected rows it
/// serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageSpan {
    /// Byte range in the file.
    pub(crate) range: Range<u64>,
    /// First selected row this range serves.
    pub(crate) first_row: u64,
    /// One past the last selected row this range serves.
    pub(crate) last_row: u64,
    pub(crate) kind: SpanKind,
}

/// The spans that reading one column chunk loads, in page order.
///
/// * `chunk`: the byte range of the column chunk.
/// * `locations`: the page locations from the offset index. Without them
///   (or when empty), the column chunk is one [`SpanKind::ColumnChunk`] span.
/// * `fetch_selection`: the selection the decoder uses to choose pages. The
///   spans are the pages that
///   [`RowSelection::scan_ranges`] returns, plus the dictionary page. `None`
///   reads every page.
/// * `rows`: counts the selected rows of the row group, which can differ from
///   `fetch_selection` (for example when the fetch is expanded to batch
///   boundaries).
/// * `first_row`: the selected rows before this row group. It is added to
///   every row position.
pub(crate) fn column_page_spans(
    chunk: Range<u64>,
    locations: Option<&[PageLocation]>,
    fetch_selection: Option<&RowSelection>,
    rows: &SelectedRows,
    row_count: usize,
    first_row: u64,
    out: &mut Vec<PageSpan>,
) {
    let row_group_rows = first_row..first_row + rows.selected_before(row_count);
    let Some(locations) = locations.filter(|locations| !locations.is_empty()) else {
        out.push(PageSpan {
            range: chunk,
            first_row: row_group_rows.start,
            last_row: row_group_rows.end,
            kind: SpanKind::ColumnChunk,
        });
        return;
    };

    // Without a selection the decoder reads every page. With one, it reads
    // the pages `scan_ranges` returns, in page order.
    let fetched = fetch_selection.map(|selection| selection.scan_ranges(locations));
    let mut fetched = fetched.as_deref().map(|ranges| ranges.iter().peekable());

    let dictionary_idx = out.len();
    let first_data_offset = locations[0].offset as u64;
    let has_dictionary = first_data_offset != chunk.start;
    if has_dictionary {
        out.push(PageSpan {
            range: chunk.start..first_data_offset,
            first_row: row_group_rows.start,
            last_row: row_group_rows.end,
            kind: SpanKind::Dictionary,
        });
    }

    let mut data_rows: Option<Range<u64>> = None;
    for (idx, location) in locations.iter().enumerate() {
        let start = location.offset as u64;
        if let Some(fetched) = fetched.as_mut()
            && fetched.next_if(|range| range.start == start).is_none()
        {
            continue;
        }
        let raw_end = locations
            .get(idx + 1)
            .map(|next| next.first_row_index as usize)
            .unwrap_or(row_count);
        let page_first_row = first_row + rows.selected_before(location.first_row_index as usize);
        let page_last_row = first_row + rows.selected_before(raw_end);
        out.push(PageSpan {
            range: start..start + location.compressed_page_size as u64,
            first_row: page_first_row,
            last_row: page_last_row,
            kind: SpanKind::Data,
        });
        data_rows = Some(match data_rows {
            Some(rows) => rows.start.min(page_first_row)..rows.end.max(page_last_row),
            None => page_first_row..page_last_row,
        });
    }

    // The dictionary serves exactly the rows of the data pages read.
    if let (true, Some(rows)) = (has_dictionary, data_rows) {
        let dictionary = &mut out[dictionary_idx];
        dictionary.first_row = rows.start;
        dictionary.last_row = rows.end;
    }
}

/// Counts the selected rows before a position in a row group.
pub(crate) struct SelectedRows {
    /// `(first raw row, selected rows before it, selected)` per selector.
    /// `None` when every row is selected.
    runs: Option<Vec<(usize, u64, bool)>>,
}

impl SelectedRows {
    pub(crate) fn new(selection: Option<&RowSelection>, row_count: usize) -> Self {
        let runs = selection.map(|selection| {
            let mut runs = Vec::new();
            let mut raw = 0;
            let mut selected = 0;
            for selector in selection.iter() {
                if selector.row_count == 0 {
                    continue;
                }
                runs.push((raw, selected, !selector.skip));
                raw += selector.row_count;
                if !selector.skip {
                    selected += selector.row_count as u64;
                }
            }
            // A selection shorter than the row group skips the trailing rows.
            if raw < row_count {
                runs.push((raw, selected, false));
            }
            runs
        });
        Self { runs }
    }

    /// The number of selected rows in `0..raw`.
    pub(crate) fn selected_before(&self, raw: usize) -> u64 {
        let Some(runs) = &self.runs else {
            return raw as u64;
        };
        let idx = runs.partition_point(|(start, _, _)| *start < raw);
        let Some(&(start, selected_before, selected)) = idx.checked_sub(1).map(|idx| &runs[idx])
        else {
            return 0;
        };
        if selected {
            selected_before + (raw - start) as u64
        } else {
            selected_before
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::RowSelector;

    #[test]
    fn selected_rows_counts_selected_positions() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(3),
            RowSelector::select(2),
        ]);
        let rows = SelectedRows::new(Some(&selection), 30);
        let counts: Vec<_> = [0, 10, 12, 15, 18, 19, 20, 30]
            .into_iter()
            .map(|raw| rows.selected_before(raw))
            .collect();
        assert_eq!(counts, vec![0, 0, 2, 5, 5, 6, 7, 7]);
        assert_eq!(SelectedRows::new(None, 30).selected_before(30), 30);
    }

    fn location(offset: i64, size: i32, first_row_index: i64) -> PageLocation {
        PageLocation {
            offset,
            compressed_page_size: size,
            first_row_index,
        }
    }

    #[test]
    fn spans_tag_pages_with_selected_rows() {
        // A dictionary page at 0..10, then three data pages of 10 rows.
        let locations = vec![location(10, 5, 0), location(15, 5, 10), location(20, 5, 20)];
        let selection = RowSelection::from(vec![
            RowSelector::skip(12),
            RowSelector::select(3),
            RowSelector::skip(10),
            RowSelector::select(5),
        ]);
        let rows = SelectedRows::new(Some(&selection), 30);
        let mut spans = vec![];
        column_page_spans(
            0..25,
            Some(&locations),
            Some(&selection),
            &rows,
            30,
            100,
            &mut spans,
        );
        let spans: Vec<_> = spans
            .iter()
            .map(|s| (s.range.clone(), s.first_row, s.last_row, s.kind))
            .collect();
        assert_eq!(
            spans,
            vec![
                (0..10, 100, 108, SpanKind::Dictionary),
                (15..20, 100, 103, SpanKind::Data),
                (20..25, 103, 108, SpanKind::Data),
            ]
        );

        // Without page locations, the column chunk is one span.
        let mut spans = vec![];
        column_page_spans(0..25, None, Some(&selection), &rows, 30, 0, &mut spans);
        assert_eq!(
            spans,
            vec![PageSpan {
                range: 0..25,
                first_row: 0,
                last_row: 8,
                kind: SpanKind::ColumnChunk
            }]
        );
    }
}
