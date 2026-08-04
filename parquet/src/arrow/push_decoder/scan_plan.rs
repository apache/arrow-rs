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

//! Page-granular scan planning: what bytes a scan needs, in decode order.
//!
//! [`ParquetPushDecoder`](super::ParquetPushDecoder) reports what it needs one
//! *row group* at a time: [`DecodeResult::NeedsData`](crate::DecodeResult) does
//! not resolve until every projected byte of the row group is buffered. That is
//! the right granularity when the caller wants a whole row group's reader, but
//! it forces callers who schedule their own I/O to buffer a full row group
//! before any decoding starts, and it gives them no way to ask "what does the
//! *next batch* need?".
//!
//! [`plan_scan_ranges`] answers that question. It decomposes a scan into the
//! individual pages it will read, in the order decoding will need them, and
//! tags each with the span of selected rows it serves. A caller can then:
//!
//! * fetch only what the next batch needs (low time-to-first-batch),
//! * bound resident bytes by dropping pages once the decode cursor passes
//!   `last_row` (memory bounded by a readahead window, not by row-group size),
//! * and issue readahead as far ahead as its own byte budget allows.
//!
//! The plan is *demand*, not *schedule*: it says which bytes the query will
//! read and when they are first needed. How many to request at once, how far to
//! read ahead, and whether to merge nearby ranges into fewer requests are
//! caller policy — they depend on the storage medium, not on the file.
//!
//! # Units
//!
//! The plan is deliberately not expressed in a fixed number of rows or bytes.
//! Pages carry row tags, so a caller working to a byte budget takes pages until
//! the budget is met, and a caller working in rows takes pages until the row
//! tags cover its window. This matters for schemas with large values: a fixed
//! row count can imply an unbounded number of bytes (8192 rows of 1 MB values
//! is 8 GB), so scheduling to a byte budget is the safer default.
//!
//! Note that a caller can never fetch less than what one output batch needs —
//! [`ParquetRecordBatchReader`](crate::arrow::arrow_reader::ParquetRecordBatchReader)
//! decodes `batch_size` rows at a time — so with very large values the floor on
//! resident bytes is one batch's worth of pages. Lowering `batch_size` is the
//! lever for that; this plan cannot subdivide a batch.
//!
//! # Example
//!
//! ```no_run
//! # use parquet::arrow::push_decoder::plan_scan_ranges;
//! # use parquet::arrow::ProjectionMask;
//! # use parquet::file::metadata::ParquetMetaData;
//! # fn get_metadata() -> ParquetMetaData { unimplemented!() }
//! # fn fetch(ranges: &[std::ops::Range<u64>]) { unimplemented!() }
//! let metadata = get_metadata();
//! let plan = plan_scan_ranges(&metadata, &[0, 1], &ProjectionMask::all(), None)
//!     .expect("offset index required for page-granular planning");
//!
//! // Fetch pages needed for the first 1024 selected rows.
//! let first: Vec<_> = plan
//!     .ranges
//!     .iter()
//!     .filter(|p| p.first_row < 1024)
//!     .map(|p| p.range.clone())
//!     .collect();
//! fetch(&first);
//! ```

use std::ops::Range;

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::RowSelection;
use crate::file::metadata::ParquetMetaData;

/// A byte range a scan will read, tagged with the selected rows it serves.
///
/// Row positions are in *selected-row space*: row 0 is the first row the scan
/// will output, so a caller that has emitted `n` rows can drop every range
/// whose `last_row <= n`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRange {
    /// Byte range in the file.
    pub range: Range<u64>,
    /// First selected row this range is needed for.
    pub first_row: u64,
    /// One past the last selected row this range is needed for. After the
    /// decode cursor reaches this, the bytes can be released.
    pub last_row: u64,
}

impl PlannedRange {
    /// Number of bytes in this range.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Whether this range is empty.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
}

/// The pages a scan will read, ordered by when decoding needs them.
///
/// See [`plan_scan_ranges`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPlan {
    /// Pages in decode-need order. Within one `first_row`, ranges that serve a
    /// wider row span (dictionary pages) come first, so a caller taking a
    /// prefix of this list always has a decodable set.
    pub ranges: Vec<PlannedRange>,
    /// Total rows the scan will output across all planned row groups.
    pub total_selected_rows: u64,
}

impl ScanPlan {
    /// Total bytes across every planned range.
    pub fn total_bytes(&self) -> u64 {
        self.ranges.iter().map(|p| p.len()).sum()
    }
}

/// Plan the pages a scan will read, in the order decoding needs them.
///
/// `row_groups` are read in the order given. `selection`, when present, applies
/// across the concatenated rows of those row groups, matching
/// [`ParquetPushDecoderBuilder::with_row_selection`](super::ParquetPushDecoderBuilder::with_row_selection).
/// Pages containing no selected row are omitted, preserving page skipping.
///
/// Returns `None` when the metadata has no offset index: page locations are
/// what make page-granular planning possible, so callers should fall back to
/// row-group-granular fetching (load the page index with
/// [`ArrowReaderOptions::with_page_index`](crate::arrow::arrow_reader::ArrowReaderOptions::with_page_index)
/// to enable it).
pub fn plan_scan_ranges(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    projection: &ProjectionMask,
    selection: Option<&RowSelection>,
) -> Option<ScanPlan> {
    let offset_index = metadata.offset_index()?;

    let total_raw: u64 = row_groups
        .iter()
        .map(|&rg| metadata.row_group(rg).num_rows() as u64)
        .sum();
    let prefix = SelectedPrefix::new(selection, total_raw);

    let mut ranges: Vec<PlannedRange> = Vec::new();
    let mut rg_raw_start = 0u64;
    for &rg_idx in row_groups {
        let row_group = metadata.row_group(rg_idx);
        let rg_rows = row_group.num_rows() as u64;
        let rg_first_row = prefix.selected_before(rg_raw_start);
        let rg_last_row = prefix.selected_before(rg_raw_start + rg_rows);

        // Row group contributes no output rows: skip it entirely.
        if rg_first_row == rg_last_row {
            rg_raw_start += rg_rows;
            continue;
        }

        for (col_idx, column) in row_group.columns().iter().enumerate() {
            if !projection.leaf_included(col_idx) {
                continue;
            }
            let locations = offset_index.get(rg_idx)?.get(col_idx)?.page_locations();
            if locations.is_empty() {
                // No page locations for a projected column: cannot plan at
                // page granularity for this file.
                return None;
            }

            // Anything before the first data page is the dictionary page,
            // needed for every row in the chunk.
            let (chunk_start, _chunk_len) = column.byte_range();
            let first_page_offset = locations[0].offset as u64;
            if first_page_offset != chunk_start {
                ranges.push(PlannedRange {
                    range: chunk_start..first_page_offset,
                    first_row: rg_first_row,
                    last_row: rg_last_row,
                });
            }

            for (i, location) in locations.iter().enumerate() {
                let raw_first = rg_raw_start + location.first_row_index as u64;
                let raw_end = locations
                    .get(i + 1)
                    .map(|next| rg_raw_start + next.first_row_index as u64)
                    .unwrap_or(rg_raw_start + rg_rows);
                let first_row = prefix.selected_before(raw_first);
                let last_row = prefix.selected_before(raw_end);
                if first_row == last_row {
                    // No selected rows on this page.
                    continue;
                }
                let start = location.offset as u64;
                ranges.push(PlannedRange {
                    range: start..start + location.compressed_page_size as u64,
                    first_row,
                    last_row,
                });
            }
        }
        rg_raw_start += rg_rows;
    }

    // Decode-need order. Ties break toward the wider row span so a dictionary
    // page precedes the data pages that need it, then by file offset so a
    // caller merging adjacent ranges sees them together.
    ranges.sort_by_key(|p| (p.first_row, std::cmp::Reverse(p.last_row), p.range.start));

    Some(ScanPlan {
        ranges,
        total_selected_rows: prefix.total_selected,
    })
}

/// Maps raw row positions (concatenated rows of the scanned row groups) to
/// selected-row positions, in `O(log n)` per lookup.
struct SelectedPrefix {
    /// `(raw_start, selected_before_raw_start, is_skip)` per selector run.
    runs: Vec<(u64, u64, bool)>,
    total_raw: u64,
    total_selected: u64,
}

impl SelectedPrefix {
    fn new(selection: Option<&RowSelection>, total_raw: u64) -> Self {
        let Some(selection) = selection else {
            return Self {
                runs: vec![(0, 0, false)],
                total_raw,
                total_selected: total_raw,
            };
        };
        let mut runs = Vec::new();
        let mut raw = 0u64;
        let mut selected = 0u64;
        for selector in selection.iter() {
            runs.push((raw, selected, selector.skip));
            raw += selector.row_count as u64;
            if !selector.skip {
                selected += selector.row_count as u64;
            }
        }
        // Rows past the end of the selection are not selected.
        runs.push((raw, selected, true));
        Self {
            runs,
            total_raw,
            total_selected: selected,
        }
    }

    fn selected_before(&self, raw: u64) -> u64 {
        let raw = raw.min(self.total_raw);
        let idx = self.runs.partition_point(|(start, _, _)| *start <= raw) - 1;
        let (start, selected, skip) = self.runs[idx];
        if skip {
            selected
        } else {
            selected + (raw - start)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelector};
    use crate::file::properties::WriterProperties;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use std::sync::Arc;

    /// Two row groups of 200 rows, 50 rows per page, two Int64 columns.
    fn test_file() -> (Bytes, ArrowReaderMetadata) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200))
            .set_data_page_row_count_limit(50)
            // Page limits are only evaluated at write-batch boundaries, so the
            // batch size has to be <= the page row limit for pages to split.
            .set_write_batch_size(50)
            .set_dictionary_enabled(false)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
        for rg in 0..2i64 {
            let base = rg * 200;
            let a: Vec<i64> = (base..base + 200).collect();
            let b: Vec<i64> = (base..base + 200).map(|v| v * 10).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
        let bytes = Bytes::from(buf);
        let options = ArrowReaderOptions::new().with_page_index(true);
        let metadata = ArrowReaderMetadata::load(&bytes, options).unwrap();
        (bytes, metadata)
    }

    #[test]
    fn plans_every_page_in_row_order() {
        let (_, metadata) = test_file();
        let plan =
            plan_scan_ranges(metadata.metadata(), &[0, 1], &ProjectionMask::all(), None).unwrap();

        assert_eq!(plan.total_selected_rows, 400);
        // 2 row groups x 2 columns x 4 pages
        assert_eq!(plan.ranges.len(), 16);
        // Ordered by first needed row.
        let firsts: Vec<u64> = plan.ranges.iter().map(|p| p.first_row).collect();
        assert!(firsts.windows(2).all(|w| w[0] <= w[1]), "{firsts:?}");
        // Each page serves 50 rows, both columns cover the same spans.
        assert_eq!(plan.ranges[0].first_row, 0);
        assert_eq!(plan.ranges[0].last_row, 50);
        assert_eq!(plan.ranges[1].first_row, 0);
        assert_eq!(plan.ranges[1].last_row, 50);
        assert_eq!(plan.ranges.last().unwrap().last_row, 400);
    }

    #[test]
    fn projection_excludes_unprojected_columns() {
        let (_, metadata) = test_file();
        let mask = ProjectionMask::leaves(metadata.parquet_schema(), [0]);
        let plan = plan_scan_ranges(metadata.metadata(), &[0, 1], &mask, None).unwrap();
        // One column only: half the pages.
        assert_eq!(plan.ranges.len(), 8);
    }

    #[test]
    fn selection_skips_pages_with_no_selected_rows() {
        let (_, metadata) = test_file();
        // Select rows 0..50 and 300..350; middle pages are skippable.
        let selection = RowSelection::from(vec![
            RowSelector::select(50),
            RowSelector::skip(250),
            RowSelector::select(50),
            RowSelector::skip(50),
        ]);
        let plan = plan_scan_ranges(
            metadata.metadata(),
            &[0, 1],
            &ProjectionMask::all(),
            Some(&selection),
        )
        .unwrap();

        assert_eq!(plan.total_selected_rows, 100);
        // 2 columns x (1 page in RG0 + 1 page in RG1)
        assert_eq!(plan.ranges.len(), 4);
        // Selected-row space: first page pair serves rows 0..50, second 50..100.
        assert_eq!(plan.ranges[0].first_row, 0);
        assert_eq!(plan.ranges[0].last_row, 50);
        assert_eq!(plan.ranges[3].first_row, 50);
        assert_eq!(plan.ranges[3].last_row, 100);
    }

    #[test]
    fn row_group_subset_is_planned_in_given_order() {
        let (_, metadata) = test_file();
        let plan =
            plan_scan_ranges(metadata.metadata(), &[1], &ProjectionMask::all(), None).unwrap();
        assert_eq!(plan.total_selected_rows, 200);
        assert_eq!(plan.ranges.len(), 8);
        assert_eq!(plan.ranges[0].first_row, 0);
    }

    #[test]
    fn no_offset_index_returns_none() {
        let (bytes, _) = test_file();
        // Load without the page index.
        let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new()).unwrap();
        assert!(
            plan_scan_ranges(metadata.metadata(), &[0], &ProjectionMask::all(), None).is_none()
        );
    }

    #[test]
    fn planned_bytes_cover_the_projected_column_chunks() {
        let (_, metadata) = test_file();
        let plan =
            plan_scan_ranges(metadata.metadata(), &[0, 1], &ProjectionMask::all(), None).unwrap();
        let planned: u64 = plan.total_bytes();
        let chunks: u64 = [0usize, 1]
            .iter()
            .flat_map(|&rg| {
                metadata
                    .metadata()
                    .row_group(rg)
                    .columns()
                    .iter()
                    .map(|c| c.byte_range().1)
            })
            .sum();
        // Every byte of every projected chunk is planned exactly once (no
        // selection, so nothing is skipped).
        assert_eq!(planned, chunks);
    }
}
