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

//! [`ScanPlan`]: the byte ranges a push decoder scan may read, in the order
//! decoding needs them.
//!
//! See [`ParquetPushDecoder::scan_plan`](super::ParquetPushDecoder::scan_plan).

use std::cmp::Reverse;
use std::ops::Range;
use std::sync::Arc;

use crate::arrow::arrow_reader::{ReadPlanBuilder, RowSelection};
use crate::errors::ParquetError;
use crate::file::metadata::page_index::RowGroupPageIndex;

use super::reader_builder::{BudgetedReadPlan, ScanPlanConfig};
use super::remaining::{NextRowGroup, RowGroupFrontier};

/// A byte range that a scan may read, with the rows and stage it serves.
///
/// Returned by [`ParquetPushDecoder::scan_plan`](super::ParquetPushDecoder::scan_plan).
///
/// # Rows
///
/// `first_row..last_row` are positions in the rows the decoder *plans* to
/// read: row selections and, for scans without a [`RowFilter`], offset and
/// limit are applied. Row 0 is the first planned row of the first planned row
/// group, and positions continue across row groups.
///
/// For a scan without a [`RowFilter`], planned row `n` is output row `n`. A
/// caller that has received `n` rows from the decoder can therefore release
/// every range whose `last_row <= n`.
///
/// With a [`RowFilter`], positions count the rows the first predicate sees.
/// Offset and limit apply after the predicates, so they are not applied here.
///
/// [`RowFilter`]: crate::arrow::arrow_reader::RowFilter
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlannedRange {
    /// Byte range in the file.
    pub range: Range<u64>,
    /// First planned row this range serves.
    pub first_row: u64,
    /// One past the last planned row this range serves.
    ///
    /// This can equal `first_row` for a page that the decoder reads only to
    /// complete a batch for its predicate cache.
    pub last_row: u64,
    /// Row group index in the file.
    pub row_group: usize,
    /// Leaf column index in the file.
    pub column: usize,
    /// What the range contains.
    pub kind: PageKind,
    /// The decoding stage that first reads this range.
    pub stage: ScanStage,
    /// `true` if the decoder may not read this range, depending on predicate
    /// results.
    ///
    /// Always `false` for a scan without a [`RowFilter`]. With a [`RowFilter`],
    /// ranges for later predicates and for the output columns are
    /// conditional, because earlier predicates can remove every row they
    /// serve. Ranges for the first predicate are conditional only if a limit is
    /// set and the range is not in the first planned row group, because the
    /// limit can be reached first.
    ///
    /// [`RowFilter`]: crate::arrow::arrow_reader::RowFilter
    pub conditional: bool,
}

impl PlannedRange {
    /// Number of bytes in this range.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Returns `true` if this range contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
}

/// What a [`PlannedRange`] contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PageKind {
    /// The dictionary page of a column chunk. It serves every planned row in
    /// the column chunk.
    Dictionary,
    /// One data page.
    Data,
    /// A complete column chunk. The plan uses this when the column has no
    /// offset index, so page locations are unknown.
    ColumnChunk,
}

/// The decoding stage that first reads a [`PlannedRange`].
///
/// The decoder reads a column once per row group. A column that a predicate
/// reads is not read again for the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScanStage {
    /// Evaluation of the predicate at this index in the
    /// [`RowFilter`](crate::arrow::arrow_reader::RowFilter).
    Predicate(usize),
    /// Decoding of the output columns.
    Projection,
}

/// The byte ranges that a push decoder scan may read, in the order decoding
/// needs them.
///
/// Created by [`ParquetPushDecoder::scan_plan`](super::ParquetPushDecoder::scan_plan),
/// which documents the contract.
///
/// The iterator plans one row group at a time, when the caller asks for its
/// first range. It owns its state, so the caller can keep it while it pushes
/// data to the decoder.
#[derive(Debug, Clone)]
pub struct ScanPlan {
    /// The decoder's row-group queue, selections and offset/limit budget, as
    /// they were when the decoder was built.
    frontier: RowGroupFrontier,
    /// The decoder's projection, predicates and batch size.
    config: Arc<ScanPlanConfig>,
    /// Whether an output limit is set.
    has_limit: bool,
    /// Whether no row group has been planned yet.
    at_first_row_group: bool,
    /// Planned rows before the next row group.
    next_row: u64,
    /// Planned ranges of the current row group not yet returned.
    pending: std::vec::IntoIter<PlannedRange>,
    /// Whether planning has ended.
    done: bool,
}

impl ScanPlan {
    pub(super) fn new(frontier: RowGroupFrontier, config: ScanPlanConfig) -> Self {
        let has_limit = frontier.budget.limit().is_some();
        Self {
            frontier,
            config: Arc::new(config),
            has_limit,
            at_first_row_group: true,
            next_row: 0,
            pending: Vec::new().into_iter(),
            done: false,
        }
    }

    /// Plan the next row group the decoder will read.
    ///
    /// Returns `Ok(None)` when no row group remains. A row group that the
    /// offset/limit budget removes gives an empty `Vec`.
    fn plan_next_row_group(&mut self) -> Result<Option<Vec<PlannedRange>>, ParquetError> {
        // Use the decoder's own row-group walk, so row groups that the
        // selection or the budget removes are skipped here too.
        let Some(NextRowGroup {
            row_group_idx,
            row_count,
            selection,
            budget,
        }) = self.frontier.next_readable_row_group()?
        else {
            return Ok(None);
        };

        let filtered = self.frontier.has_predicates;
        let selection = if filtered {
            // Predicates see every selected row. Offset and limit apply to
            // their output, so they cannot be applied before decoding.
            selection
        } else {
            // Apply offset and limit as the decoder does before it requests
            // the output columns.
            let plan_builder =
                ReadPlanBuilder::new(self.config.batch_size).with_selection(selection);
            let BudgetedReadPlan {
                plan_builder,
                rows_after_budget,
                remaining_budget,
                ..
            } = budget.apply_to_plan(plan_builder, row_count);
            self.frontier
                .update_budget_after_row_group(remaining_budget);
            if rows_after_budget == 0 {
                return Ok(Some(vec![]));
            }
            plan_builder.selection().cloned()
        };

        let rows = SelectedRows::new(selection.as_ref(), row_count);
        let first_row = self.next_row;
        self.next_row += rows.selected_before(row_count);

        let metadata = Arc::clone(&self.frontier.parquet_metadata);
        let page_index = metadata
            .page_index()
            .is_some_and(|page_index| page_index.has_offset_indexes())
            .then(|| metadata.page_index_for_row_group(row_group_idx));
        let row_group = metadata.row_group(row_group_idx);

        // Predicate columns that are cached for the output are fetched with the
        // selection expanded to batch boundaries. See `fetch_ranges`.
        let expanded_selection = match (&selection, &self.config.cache_projection) {
            (Some(selection), Some(_)) if filtered => {
                Some(selection.expand_to_batch_boundaries(self.config.batch_size, row_count))
            }
            _ => None,
        };

        let columns = RowGroupColumns {
            row_group_idx,
            row_count,
            first_row,
            rows: &rows,
            page_index: page_index.as_ref(),
        };
        let stages = self
            .config
            .predicate_projections
            .iter()
            .enumerate()
            .map(|(idx, mask)| (ScanStage::Predicate(idx), mask))
            .chain(std::iter::once((
                ScanStage::Projection,
                &self.config.projection,
            )));

        let mut planned_columns = vec![false; row_group.columns().len()];
        let mut ranges = vec![];
        for (stage, mask) in stages {
            let conditional = filtered
                && (stage != ScanStage::Predicate(0)
                    || (self.has_limit && !self.at_first_row_group));
            let stage_start = ranges.len();
            for (column_idx, chunk) in row_group.columns().iter().enumerate() {
                // The decoder reuses a column that an earlier stage read.
                if !mask.leaf_included(column_idx) || planned_columns[column_idx] {
                    continue;
                }
                planned_columns[column_idx] = true;
                let is_cached = self
                    .config
                    .cache_projection
                    .as_ref()
                    .is_some_and(|cache| cache.leaf_included(column_idx));
                let fetch_selection = match (stage, &expanded_selection) {
                    (ScanStage::Predicate(_), Some(expanded)) if is_cached => Some(expanded),
                    _ => selection.as_ref(),
                };
                let (chunk_start, chunk_len) = chunk.byte_range();
                columns.plan(
                    column_idx,
                    chunk_start..chunk_start + chunk_len,
                    fetch_selection,
                    stage,
                    conditional,
                    &mut ranges,
                );
            }
            // Order by when decoding needs each range. For equal first rows,
            // the wider span (a dictionary page) comes first, so any prefix of
            // the plan can be decoded.
            ranges[stage_start..]
                .sort_by_key(|p| (p.first_row, Reverse(p.last_row), p.range.start));
        }
        self.at_first_row_group = false;
        Ok(Some(ranges))
    }
}

impl Iterator for ScanPlan {
    type Item = PlannedRange;

    fn next(&mut self) -> Option<PlannedRange> {
        loop {
            if let Some(range) = self.pending.next() {
                return Some(range);
            }
            if self.done {
                return None;
            }
            match self.plan_next_row_group() {
                Ok(Some(ranges)) => self.pending = ranges.into_iter(),
                // The decoder reports the same error when it reaches this row
                // group. The plan is advisory, so it ends here.
                Ok(None) | Err(_) => self.done = true,
            }
        }
    }
}

/// The parts of one row group that are the same for each of its columns.
struct RowGroupColumns<'a> {
    row_group_idx: usize,
    row_count: usize,
    /// Planned rows before this row group.
    first_row: u64,
    rows: &'a SelectedRows,
    page_index: Option<&'a RowGroupPageIndex>,
}

impl RowGroupColumns<'_> {
    /// Plan the ranges of one column chunk.
    ///
    /// `fetch_selection` is the selection the decoder uses to choose pages.
    /// The ranges are the same bytes that `InMemoryRowGroup::fetch_ranges`
    /// requests, split at page boundaries when page locations are known.
    fn plan(
        &self,
        column_idx: usize,
        chunk: Range<u64>,
        fetch_selection: Option<&RowSelection>,
        stage: ScanStage,
        conditional: bool,
        out: &mut Vec<PlannedRange>,
    ) {
        let entry = |range, first_row, last_row, kind| PlannedRange {
            range,
            first_row,
            last_row,
            row_group: self.row_group_idx,
            column: column_idx,
            kind,
            stage,
            conditional,
        };
        let row_group_rows =
            self.first_row..self.first_row + self.rows.selected_before(self.row_count);

        let locations = self
            .page_index
            .and_then(|page_index| page_index.offset_index(column_idx))
            .map(|offset_index| offset_index.page_locations())
            .filter(|locations| !locations.is_empty());
        let Some(locations) = locations else {
            out.push(entry(
                chunk,
                row_group_rows.start,
                row_group_rows.end,
                PageKind::ColumnChunk,
            ));
            return;
        };

        // Without a selection the decoder reads every page. With one, it
        // reads the pages `scan_ranges` returns, in page order.
        let fetched = fetch_selection.map(|selection| selection.scan_ranges(locations));
        let mut fetched = fetched.as_deref().map(|ranges| ranges.iter().peekable());

        let dictionary_idx = out.len();
        let first_data_offset = locations[0].offset as u64;
        let has_dictionary = first_data_offset != chunk.start;
        if has_dictionary {
            out.push(entry(
                chunk.start..first_data_offset,
                row_group_rows.start,
                row_group_rows.end,
                PageKind::Dictionary,
            ));
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
                .unwrap_or(self.row_count);
            let first_row =
                self.first_row + self.rows.selected_before(location.first_row_index as usize);
            let last_row = self.first_row + self.rows.selected_before(raw_end);
            out.push(entry(
                start..start + location.compressed_page_size as u64,
                first_row,
                last_row,
                PageKind::Data,
            ));
            data_rows = Some(match data_rows {
                Some(rows) => rows.start.min(first_row)..rows.end.max(last_row),
                None => first_row..last_row,
            });
        }

        // The dictionary serves exactly the rows of the data pages read.
        if let (true, Some(rows)) = (has_dictionary, data_rows) {
            let dictionary = &mut out[dictionary_idx];
            dictionary.first_row = rows.start;
            dictionary.last_row = rows.end;
        }
    }
}

/// Counts the selected rows before a position in a row group.
struct SelectedRows {
    /// `(first raw row, selected rows before it, selected)` per selector.
    /// `None` when every row is selected.
    runs: Option<Vec<(usize, u64, bool)>>,
}

impl SelectedRows {
    fn new(selection: Option<&RowSelection>, row_count: usize) -> Self {
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
    fn selected_before(&self, raw: usize) -> u64 {
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
    use crate::DecodeResult;
    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelector,
    };
    use crate::arrow::push_decoder::{ParquetPushDecoderBuilder, RowGroupSelection};
    use crate::arrow::{ArrowWriter, ProjectionMask};
    use crate::file::metadata::PageIndexPolicy;
    use crate::file::properties::WriterProperties;
    use arrow::compute::kernels::cmp::{gt, lt};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_buffer::BooleanBuffer;
    use bytes::Bytes;
    use std::sync::LazyLock;

    /// Two row groups of 200 rows, 50 rows per data page, three columns:
    /// `a` (Int64, plain), `b` (Int64, dictionary) and `c` (Utf8, dictionary).
    static FILE: LazyLock<Bytes> = LazyLock::new(|| {
        let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
        let b: ArrayRef = Arc::new(Int64Array::from_iter_values((0..400).map(|v| v % 7)));
        let c: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..400).map(|v| format!("value {}", v % 13)),
        ));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200))
            .set_data_page_row_count_limit(50)
            // Page limits are checked between write batches.
            .set_write_batch_size(50)
            .set_column_dictionary_enabled("a".into(), false)
            .build();
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buffer)
    });

    fn metadata(page_index: bool) -> ArrowReaderMetadata {
        let policy = if page_index {
            PageIndexPolicy::Required
        } else {
            PageIndexPolicy::Skip
        };
        let options = ArrowReaderOptions::new().with_page_index_policy(policy);
        ArrowReaderMetadata::load(&*FILE, options).unwrap()
    }

    fn builder() -> ParquetPushDecoderBuilder {
        ParquetPushDecoderBuilder::new_with_metadata(metadata(true)).with_batch_size(64)
    }

    fn fetch(range: &Range<u64>) -> Bytes {
        FILE.slice(range.start as usize..range.end as usize)
    }

    fn mask(leaves: impl IntoIterator<Item = usize>) -> ProjectionMask {
        ProjectionMask::leaves(metadata(true).parquet_schema(), leaves)
    }

    /// Sort and merge ranges into disjoint, non-adjacent ranges.
    fn union(ranges: impl IntoIterator<Item = Range<u64>>) -> Vec<Range<u64>> {
        let mut ranges: Vec<_> = ranges.into_iter().filter(|r| !r.is_empty()).collect();
        ranges.sort_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = vec![];
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
                _ => merged.push(range),
            }
        }
        merged
    }

    /// Returns `true` if every byte of `inner` is in `outer`.
    fn covers(outer: &[Range<u64>], inner: &[Range<u64>]) -> bool {
        inner.iter().all(|range| {
            outer
                .iter()
                .any(|o| o.start <= range.start && range.end <= o.end)
        })
    }

    /// Decode the whole scan, supplying exactly the requested bytes. Returns
    /// the requested ranges and the number of output rows.
    fn demand(builder: ParquetPushDecoderBuilder) -> (Vec<Range<u64>>, u64) {
        let mut decoder = builder.build().unwrap();
        let mut requested = vec![];
        let mut rows = 0;
        loop {
            match decoder.try_decode().unwrap() {
                DecodeResult::NeedsData(ranges) => {
                    let data = ranges.iter().map(fetch).collect();
                    requested.extend(ranges.iter().cloned());
                    decoder.push_ranges(ranges, data).unwrap();
                }
                DecodeResult::Data(batch) => rows += batch.num_rows() as u64,
                DecodeResult::Finished => return (requested, rows),
            }
        }
    }

    /// Check the invariants of a plan for a scan without a row filter.
    ///
    /// `builder` is called twice: once for the plan, once to decode.
    fn check_unfiltered(
        name: &str,
        builder: impl Fn() -> ParquetPushDecoderBuilder,
    ) -> Vec<PlannedRange> {
        let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
        let (requested, rows) = demand(builder());

        assert_eq!(
            union(plan.iter().map(|p| p.range.clone())),
            union(requested),
            "{name}: planned bytes differ from requested bytes"
        );
        for p in &plan {
            assert!(!p.conditional, "{name}: {p:?}");
            assert_eq!(p.stage, ScanStage::Projection, "{name}: {p:?}");
        }
        // Planned rows are output rows: for each column, the data pages tile
        // the output rows in order.
        let columns: std::collections::BTreeSet<_> = plan.iter().map(|p| p.column).collect();
        for column in columns {
            let mut next = 0;
            for p in plan
                .iter()
                .filter(|p| p.column == column && p.kind != PageKind::Dictionary)
            {
                assert_eq!(p.first_row, next, "{name}: gap before {p:?}");
                assert!(p.last_row > p.first_row, "{name}: empty {p:?}");
                next = p.last_row;
            }
            assert_eq!(next, rows, "{name}: column {column} rows");
        }
        plan
    }

    #[test]
    fn plan_matches_demand_without_row_filter() {
        fn sel(selectors: Vec<RowSelector>) -> RowSelection {
            RowSelection::from(selectors)
        }
        type Case = (&'static str, Box<dyn Fn() -> ParquetPushDecoderBuilder>);
        let cases: Vec<Case> = vec![
            ("all", Box::new(builder)),
            (
                "projection",
                Box::new(|| builder().with_projection(mask([2]))),
            ),
            (
                "row groups reversed",
                Box::new(|| builder().with_row_groups(vec![1, 0])),
            ),
            (
                "global selection",
                Box::new(|| {
                    builder().with_row_selection(sel(vec![
                        RowSelector::skip(60),
                        RowSelector::select(10),
                        RowSelector::skip(200),
                        RowSelector::select(30),
                    ]))
                }),
            ),
            (
                "mask selection",
                Box::new(|| {
                    builder().with_row_selection(RowSelection::from_boolean_buffer(
                        BooleanBuffer::from_iter((0..400).map(|i| i % 97 == 3)),
                    ))
                }),
            ),
            (
                "per row group selections with duplicates",
                Box::new(|| {
                    builder().with_row_group_selections(vec![
                        RowGroupSelection::new(
                            1,
                            Some(sel(vec![RowSelector::skip(150), RowSelector::select(10)])),
                        ),
                        RowGroupSelection::new(0, None),
                        RowGroupSelection::new(1, Some(sel(vec![RowSelector::select(5)]))),
                        RowGroupSelection::new(0, Some(sel(vec![RowSelector::skip(200)]))),
                    ])
                }),
            ),
            (
                "offset and limit",
                Box::new(|| builder().with_offset(5).with_limit(12)),
            ),
            (
                "offset past first row group",
                Box::new(|| builder().with_offset(230).with_limit(60)),
            ),
            (
                "selection, offset and limit",
                Box::new(|| {
                    builder()
                        .with_row_selection(sel(vec![
                            RowSelector::skip(190),
                            RowSelector::select(10),
                            RowSelector::select(100),
                        ]))
                        .with_offset(5)
                        .with_limit(12)
                }),
            ),
            ("limit zero", Box::new(|| builder().with_limit(0))),
            (
                "no page index",
                Box::new(|| {
                    ParquetPushDecoderBuilder::new_with_metadata(metadata(false))
                        .with_row_selection(sel(vec![
                            RowSelector::skip(60),
                            RowSelector::select(10),
                        ]))
                }),
            ),
        ];
        for (name, builder) in cases {
            check_unfiltered(name, builder);
        }
    }

    #[test]
    fn plan_has_pages_with_offset_index() {
        let plan = check_unfiltered("all", || builder().with_projection(mask([0, 1])));
        // Column a: 4 plain data pages per row group. Column b: a dictionary
        // page, then 4 data pages per row group.
        let kinds = |row_group, column| -> Vec<PageKind> {
            plan.iter()
                .filter(|p| p.row_group == row_group && p.column == column)
                .map(|p| p.kind)
                .collect()
        };
        assert_eq!(kinds(0, 0), vec![PageKind::Data; 4]);
        let mut expected = vec![PageKind::Dictionary];
        expected.extend([PageKind::Data; 4]);
        assert_eq!(kinds(1, 1), expected);

        // The dictionary page serves every row of its row group and comes
        // before the data pages.
        let dictionary = plan
            .iter()
            .position(|p| p.row_group == 1 && p.kind == PageKind::Dictionary)
            .unwrap();
        assert_eq!(
            (plan[dictionary].first_row, plan[dictionary].last_row),
            (200, 400)
        );
        assert!(plan[..dictionary].iter().all(|p| p.first_row < 200));

        // Plan order is decode order.
        assert!(plan.windows(2).all(|w| w[0].first_row <= w[1].first_row));
    }

    #[test]
    fn plan_has_column_chunks_without_offset_index() {
        let plan = check_unfiltered("no page index", || {
            ParquetPushDecoderBuilder::new_with_metadata(metadata(false)).with_limit(250)
        });
        let entries: Vec<_> = plan
            .iter()
            .map(|p| (p.row_group, p.column, p.kind, p.first_row, p.last_row))
            .collect();
        let mut expected = vec![];
        for (row_group, rows) in [(0, 0..200), (1, 200..250)] {
            for column in 0..3 {
                expected.push((
                    row_group,
                    column,
                    PageKind::ColumnChunk,
                    rows.start,
                    rows.end,
                ));
            }
        }
        assert_eq!(entries, expected);
    }

    #[test]
    fn plan_tags_row_filter_stages() {
        // Predicate on `a`, then on `b`; output `a` and `c`.
        let filter = || {
            RowFilter::new(vec![
                Box::new(ArrowPredicateFn::new(mask([0]), |batch| {
                    gt(batch.column(0), &Int64Array::new_scalar(100))
                })),
                Box::new(ArrowPredicateFn::new(mask([1]), |batch| {
                    lt(batch.column(0), &Int64Array::new_scalar(3))
                })),
            ])
        };
        for limit in [None, Some(10)] {
            let builder = || {
                // The selection keeps a few rows of scattered pages, so the
                // cached predicate column `a` is fetched with its selection
                // expanded to batch boundaries.
                let builder = builder()
                    .with_projection(mask([0, 2]))
                    .with_row_selection(RowSelection::from(vec![
                        RowSelector::skip(70),
                        RowSelector::select(3),
                        RowSelector::skip(260),
                        RowSelector::select(20),
                    ]))
                    .with_row_filter(filter());
                match limit {
                    Some(limit) => builder.with_limit(limit),
                    None => builder,
                }
            };
            let plan: Vec<_> = builder().build().unwrap().scan_plan().collect();
            let (requested, _) = demand(builder());
            assert!(
                covers(&union(plan.iter().map(|p| p.range.clone())), &requested),
                "limit {limit:?}: requested bytes are not planned"
            );

            for p in &plan {
                let expected_stage = match p.column {
                    0 => ScanStage::Predicate(0),
                    1 => ScanStage::Predicate(1),
                    _ => ScanStage::Projection,
                };
                assert_eq!(p.stage, expected_stage, "{p:?}");
                let expected_conditional = match p.stage {
                    ScanStage::Predicate(0) => limit.is_some() && p.row_group > 0,
                    _ => true,
                };
                assert_eq!(
                    p.conditional, expected_conditional,
                    "limit {limit:?}: {p:?}"
                );
            }
            // Stages are in evaluation order within each row group, and the
            // offset/limit do not remove rows before the predicates run.
            for row_group in 0..2 {
                let stages: Vec<_> = plan
                    .iter()
                    .filter(|p| p.row_group == row_group)
                    .map(|p| p.stage)
                    .collect();
                assert!(stages.is_sorted(), "{stages:?}");
            }
            assert_eq!(plan.iter().map(|p| p.last_row).max(), Some(23));
        }
    }

    #[test]
    fn plan_is_stable_and_independent_of_buffers() {
        let mut decoder = builder().build().unwrap();
        let before: Vec<_> = decoder.scan_plan().collect();
        let file_range = 0..FILE.len() as u64;
        decoder
            .push_range(file_range.clone(), fetch(&file_range))
            .unwrap();
        let DecodeResult::Data(_) = decoder.try_next_reader().unwrap() else {
            panic!("expected a reader");
        };
        assert_eq!(decoder.scan_plan().collect::<Vec<_>>(), before);
    }

    #[test]
    fn rebuilt_decoder_plans_remaining_row_groups() {
        let mut decoder = builder().build().unwrap();
        let file_range = 0..FILE.len() as u64;
        decoder
            .push_range(file_range.clone(), fetch(&file_range))
            .unwrap();
        let DecodeResult::Data(_) = decoder.try_next_reader().unwrap() else {
            panic!("expected a reader");
        };
        let decoder = decoder.into_builder().unwrap().build().unwrap();
        let plan: Vec<_> = decoder.scan_plan().collect();
        assert!(!plan.is_empty());
        assert!(plan.iter().all(|p| p.row_group == 1), "{plan:?}");
        assert_eq!(plan[0].first_row, 0);
    }

    #[test]
    fn plan_ends_before_invalid_row_group() {
        let plan: Vec<_> = builder()
            .with_row_groups(vec![0, 5])
            .build()
            .unwrap()
            .scan_plan()
            .collect();
        assert!(!plan.is_empty());
        assert!(plan.iter().all(|p| p.row_group == 0), "{plan:?}");
    }

    #[test]
    fn plan_is_lazy() {
        let decoder = builder().build().unwrap();
        let mut plan = decoder.scan_plan();
        let first = plan.next().unwrap();
        assert_eq!(first.row_group, 0);
        // Only the first row group has been planned.
        assert!(plan.pending.as_slice().iter().all(|p| p.row_group == 0));
        assert_eq!(plan.next_row, 200);
    }

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
}
