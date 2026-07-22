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

//! Window planning for bounded streaming decode.
//!
//! A *window* is a contiguous row span of a row group whose boundaries fall on
//! selected-row multiples of `batch_size`. Each window is decoded with the
//! existing sparse-selection machinery (its own [`DataRequest`] and
//! [`ReadPlanBuilder`]), so the sequence of emitted batches is identical to
//! decoding the whole row group at once, while only one window's bytes (plus
//! pinned dictionary/boundary pages) need to be resident at a time.

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::{ReadPlanBuilder, RowSelection, RowSelectionPolicy, RowSelector};
use crate::arrow::in_memory_row_group::DictionaryPageCache;
use crate::arrow::push_decoder::reader_builder::data::{DataRequest, DataRequestBuilder};
use crate::file::metadata::ParquetMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

/// One decode window: the data it needs and the plan to decode it.
#[derive(Debug)]
pub(super) struct WindowPlan {
    pub data_request: DataRequest,
    pub plan_builder: ReadPlanBuilder,
}

/// State for decoding a row group in bounded windows.
#[derive(Debug)]
pub(super) struct WindowedRead {
    pub row_group_idx: usize,
    pub row_count: usize,
    /// Remaining windows, front is next to decode
    pub windows: VecDeque<WindowPlan>,
    /// How many remaining windows still need each range. Ranges shared by
    /// multiple windows (dictionary pages, window-boundary pages) are kept in
    /// the push buffers until their last use.
    range_uses: HashMap<(u64, u64), usize>,
    /// Full requested byte spans of the column chunks large enough to be
    /// streamed. Advertised to the fetch layer via
    /// `ParquetPushDecoder::upcoming_fetch_plan` so it can decide which
    /// coalesced requests to consume incrementally.
    pub streamable: Vec<Range<u64>>,
    /// Decompressed dictionary pages shared across this row group's windows,
    /// so each is decompressed once rather than once per window
    pub dictionary_page_cache: Arc<DictionaryPageCache>,
}

impl WindowedRead {
    /// Called after the front window's reader has been built: decrement the
    /// use count of each of its ranges. Returns a set of ranges that are
    /// still needed by later windows (and so must be retained in the push
    /// buffers).
    pub(super) fn finish_front_window(&mut self, ranges: &[Range<u64>]) -> HashMap<(u64, u64), ()> {
        let mut retained = HashMap::new();
        for range in ranges {
            let key = (range.start, range.end);
            if let Some(uses) = self.range_uses.get_mut(&key) {
                *uses -= 1;
                if *uses == 0 {
                    self.range_uses.remove(&key);
                } else {
                    retained.insert(key, ());
                }
            }
        }
        retained
    }
}

/// Per-column byte estimator over the offset index.
struct ColumnPages {
    /// First row index of each page, ascending, starting at 0
    first_rows: Vec<u64>,
    /// Prefix sums of compressed page sizes: `prefix[i]` = bytes of pages `0..i`
    prefix: Vec<u64>,
}

impl ColumnPages {
    fn new(offset_index: &OffsetIndexMetaData) -> Self {
        let locations = offset_index.page_locations();
        let mut first_rows = Vec::with_capacity(locations.len());
        let mut prefix = Vec::with_capacity(locations.len() + 1);
        prefix.push(0);
        let mut total = 0u64;
        for location in locations {
            first_rows.push(location.first_row_index as u64);
            total += location.compressed_page_size as u64;
            prefix.push(total);
        }
        Self { first_rows, prefix }
    }

    /// Compressed bytes of the pages overlapping rows `lo..hi`.
    ///
    /// This intentionally ignores the row selection within the span (an
    /// overestimate), which can only make windows smaller than the target.
    fn bytes_for_span(&self, lo: u64, hi: u64) -> u64 {
        if self.first_rows.is_empty() || lo >= hi {
            return 0;
        }
        // last page whose first row is <= lo
        let idx_lo = self.first_rows.partition_point(|&fr| fr <= lo) - 1;
        // last page whose first row is < hi
        let idx_hi = self.first_rows.partition_point(|&fr| fr < hi) - 1;
        self.prefix[idx_hi + 1] - self.prefix[idx_lo]
    }
}

/// Attempt to plan a windowed read of a row group.
///
/// `selection` is the final (budget-applied) selection for the row group, or
/// `None` if all rows are selected. Returns `None` when windowing does not
/// apply: no offset index, no projected chunk requests at least
/// `stream_threshold` bytes, or fewer than two windows would result.
#[expect(clippy::too_many_arguments)]
pub(super) fn plan_windows(
    row_group_idx: usize,
    row_count: usize,
    batch_size: usize,
    parquet_metadata: &ParquetMetaData,
    projection: &ProjectionMask,
    selection: Option<&RowSelection>,
    row_selection_policy: RowSelectionPolicy,
    options: &crate::arrow::arrow_reader::BoundedStreamingOptions,
) -> Option<WindowedRead> {
    let stream_threshold = options.stream_threshold;
    if batch_size == 0 || row_count == 0 {
        return None;
    }
    let offset_index = parquet_metadata
        .offset_index()
        .filter(|index| !index.is_empty())
        .map(|index| index[row_group_idx].as_slice())?;

    let row_group_meta = parquet_metadata.row_group(row_group_idx);
    let num_columns = row_group_meta.columns().len();
    if offset_index.len() != num_columns {
        return None;
    }

    // Determine which projected chunks are large enough to stream, based on
    // the bytes the current selection will actually request from them.
    let mut streamable: Vec<Range<u64>> = Vec::new();
    let mut total_requested: u64 = 0;
    for (idx, column_offset_index) in offset_index.iter().enumerate() {
        if !projection.leaf_included(idx) {
            continue;
        }
        let (start, len) = row_group_meta.column(idx).byte_range();
        let requested: u64 = match selection {
            Some(selection) => selection
                .scan_ranges(column_offset_index.page_locations())
                .iter()
                .map(|r| r.end - r.start)
                .sum(),
            None => len,
        };
        total_requested += requested;
        if requested >= stream_threshold {
            streamable.push(start..start + len);
        }
    }
    // See BoundedStreamingOptions::min_window_bytes_per_column: windows must
    // amortize per-window, per-column fixed costs
    let projected_columns = (0..num_columns)
        .filter(|idx| projection.leaf_included(*idx))
        .count() as u64;
    let window_bytes = options
        .window_bytes
        .max(projected_columns * options.min_window_bytes_per_column);
    if streamable.is_empty() || total_requested <= window_bytes {
        return None;
    }

    // Per-column byte estimators for the projected columns
    let estimators: Vec<ColumnPages> = (0..num_columns)
        .filter(|idx| projection.leaf_included(*idx))
        .map(|idx| ColumnPages::new(&offset_index[idx]))
        .collect();

    // Row-space positions after each full batch of selected rows
    let batch_ends = batch_end_positions(selection, row_count, batch_size);
    if batch_ends.len() < 2 {
        return None;
    }

    // Greedily grow windows batch-by-batch until the estimated bytes exceed
    // the target
    let mut window_spans: Vec<Range<u64>> = Vec::new();
    let mut window_start = 0u64;
    let mut prev_end: Option<u64> = None;
    for &batch_end in &batch_ends {
        let estimate: u64 = estimators
            .iter()
            .map(|e| e.bytes_for_span(window_start, batch_end))
            .sum();
        if estimate > window_bytes {
            if let Some(prev) = prev_end {
                window_spans.push(window_start..prev);
                window_start = prev;
            }
        }
        prev_end = Some(batch_end);
    }
    // final window runs to the end of the row group
    window_spans.push(window_start..row_count as u64);

    if window_spans.len() < 2 {
        return None;
    }

    // Build the per-window selections, data requests and read plans
    let mut working = selection
        .cloned()
        .unwrap_or_else(|| RowSelection::from(vec![RowSelector::select(row_count)]));
    let mut windows = VecDeque::with_capacity(window_spans.len());
    let mut range_uses: HashMap<(u64, u64), usize> = HashMap::new();
    for span in &window_spans {
        let front = working.split_off((span.end - span.start) as usize);
        let mut selectors = Vec::new();
        if span.start > 0 {
            selectors.push(RowSelector::skip(span.start as usize));
        }
        selectors.extend(front.iter().filter(|s| s.row_count > 0).cloned());
        if (span.end as usize) < row_count {
            selectors.push(RowSelector::skip(row_count - span.end as usize));
        }
        let window_selection = RowSelection::from(selectors);

        let data_request = DataRequestBuilder::new(
            row_group_idx,
            row_count,
            batch_size,
            parquet_metadata,
            projection,
        )
        .with_selection(Some(&window_selection))
        .build();

        for range in data_request.ranges() {
            *range_uses.entry((range.start, range.end)).or_insert(0) += 1;
        }

        let plan_builder = ReadPlanBuilder::new(batch_size)
            .with_selection(Some(window_selection))
            .with_row_selection_policy(row_selection_policy);

        windows.push_back(WindowPlan {
            data_request,
            plan_builder,
        });
    }

    Some(WindowedRead {
        row_group_idx,
        row_count,
        windows,
        range_uses,
        streamable,
        dictionary_page_cache: Arc::new(DictionaryPageCache::default()),
    })
}

/// Returns the row-space end position of each `batch_size`-selected-rows
/// batch, including a final (possibly partial) batch ending after the last
/// selected row.
fn batch_end_positions(
    selection: Option<&RowSelection>,
    row_count: usize,
    batch_size: usize,
) -> Vec<u64> {
    let mut ends = Vec::new();
    let mut row_pos = 0usize;
    let mut selected_in_batch = 0usize;
    let mut any_selected_since_last = false;

    let all_rows;
    let selectors: Box<dyn Iterator<Item = RowSelector>> = match selection {
        Some(selection) => Box::new(selection.iter().cloned()),
        None => {
            all_rows = [RowSelector::select(row_count)];
            Box::new(all_rows.iter().cloned())
        }
    };

    for selector in selectors {
        if selector.skip {
            row_pos += selector.row_count;
            continue;
        }
        let mut remaining = selector.row_count;
        while remaining > 0 {
            let take = remaining.min(batch_size - selected_in_batch);
            selected_in_batch += take;
            row_pos += take;
            remaining -= take;
            any_selected_since_last = true;
            if selected_in_batch == batch_size {
                ends.push(row_pos as u64);
                selected_in_batch = 0;
                any_selected_since_last = false;
            }
        }
    }
    if any_selected_since_last {
        ends.push(row_pos as u64);
    }
    ends
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_end_positions_no_selection() {
        let ends = batch_end_positions(None, 10, 4);
        assert_eq!(ends, vec![4, 8, 10]);
    }

    #[test]
    fn test_batch_end_positions_with_selection() {
        // skip 2, select 3, skip 1, select 4 => selected rows at 2,3,4, 6,7,8,9
        let selection = RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(4),
        ]);
        let ends = batch_end_positions(Some(&selection), 10, 2);
        // batches of 2 selected rows end after rows 3 (2,3), 6 (4,6), 8 (7,8),
        // and the final partial batch after row 9
        assert_eq!(ends, vec![4, 7, 9, 10]);
    }
}
