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

//! Batch-granular decoding of one row group. This module is the design
//! document for [`FetchGranularity::Batch`]. The user-visible behavior is in
//! the documentation of [`FetchGranularity::Batch`].
//!
//! | | Row-group mode ([`super::RowGroupReaderBuilder::try_build`]) | [`IncrementalRowGroup`] |
//! |---|---|---|
//! | Readers built | after all bytes of the row group are buffered | before any byte is buffered, one time per row group |
//! | Column chunk data | [`ColumnChunkData::Dense`] or `Sparse` (immutable) | [`ColumnChunkData::Shared`] over one [`PageStore`] (mutable) |
//! | Bytes requested | all bytes of the row group | the pages of the next step |
//!
//! # Page flow
//!
//! ```text
//!            push                 ingest (move)               get
//!  caller ─────────▶ PushBuffers ──────────────▶ PageStore ◀─────── column readers
//! ```
//!
//! Before each step, [`IncrementalRowGroup::try_next`]:
//!
//! 1. Computes the pages that the step reads.
//! 2. Returns [`IncrementalResult::NeedsData`] if a page is not in the
//!    [`PageStore`] or in [`PushBuffers`].
//! 3. Else, moves the pages from [`PushBuffers`] into the [`PageStore`] and
//!    runs the step.
//!
//! The [`PageStore`] holds the pages until the row group is finished.
//!
//! # Why the readers do not read a page that is not in the store
//!
//! With an offset index, a column reader:
//!
//! * loads a page only when it decodes a value from the page or skips a part
//!   of the page;
//! * skips a full page with the offset index only, without loading it;
//! * does not read after the last record that it must return, because a page
//!   ends at a record boundary.
//!
//! Thus, the pages of step 1 are all of the pages that the readers load.
//!
//! # Readers
//!
//! The decoder keeps one [`ArrayReader`] per predicate and one for the
//! output, for the full row group. Thus, it decodes each page and each
//! dictionary one time per reader. Scans without predicates use the same
//! steps: each window passes to the queue without I/O. See [`Stage`].
//!
//! The batches are the same as in the row-group mode.
//!
//! [`FetchGranularity::Batch`]: crate::arrow::push_decoder::FetchGranularity::Batch

use super::RowBudget;
use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReader, ArrayReaderBuilder};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, ReadPlan, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy, RowSelector,
};
use crate::arrow::in_memory_row_group::{ColumnChunkData, InMemoryRowGroup};
use crate::arrow::push_decoder::page_store::PageStore;
use crate::arrow::schema::ParquetField;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::file::metadata::page_index::RowGroupPageIndex;
use crate::file::reader::ChunkReader;
use crate::util::push_buffers::PushBuffers;
use arrow_array::{Array, RecordBatch};
use arrow_select::filter::prep_null_mask_filter;
use std::ops::Range;
use std::sync::Arc;

/// The result of [`IncrementalRowGroup::try_next`].
#[derive(Debug)]
pub(super) enum IncrementalResult {
    /// The bytes that the next step needs.
    NeedsData(Vec<Range<u64>>),
    /// The next output batch. If `last` is `true`, the row group is finished.
    Batch { batch: RecordBatch, last: bool },
    /// The row group is finished, without more batches.
    Finished,
}

/// The options of the enclosing [`super::RowGroupReaderBuilder`] that an
/// [`IncrementalRowGroup`] uses.
#[derive(Debug, Clone)]
pub(super) struct IncrementalConfig {
    pub(super) batch_size: usize,
    pub(super) projection: ProjectionMask,
    pub(super) metadata: Arc<ParquetMetaData>,
    pub(super) fields: Option<Arc<ParquetField>>,
    pub(super) metrics: ArrowReaderMetrics,
}

/// The stage of an [`IncrementalRowGroup`].
///
/// ```text
///          next window                     idx == number of predicates
///  ┌──────┐ ──────────▶ Predicate { idx: 0 } ─▶ … ─▶ queue_survivors ──┐
///  │ Idle │ ◀─────────────────────────────────────────────────────────┘
///  └──────┘ ◀──────────────────────┐
///     │                            │ return the batch
///     └──────▶ Output { out } ─────┘
/// ```
///
/// | In `Idle`, if | Next stage |
/// |---|---|
/// | the queue has more than `batch_size` rows, or no window is left and the queue is not empty | `Output`, with the first `batch_size` rows of the queue |
/// | no window is left and the queue is empty | none. The row group is finished. |
/// | else | `Predicate { idx: 0 }`, with the next window that has selected rows |
///
/// `Output` needs *more than* `batch_size` rows in the queue, because the
/// decoder must know if a batch is the last batch of the row group. Without
/// predicates, `Predicate { idx: 0 }` goes to `queue_survivors` at once.
///
/// If a stage needs bytes, the stage does not change and
/// [`IncrementalRowGroup::try_next`] returns [`IncrementalResult::NeedsData`].
/// All row ranges are row numbers in the row group.
enum Stage {
    Idle,
    /// Evaluate predicate `idx` on the rows `cand` of the current window.
    Predicate {
        cand: Vec<Range<usize>>,
        idx: usize,
    },
    /// Decode one output batch for the rows `out`.
    Output {
        out: Vec<Range<usize>>,
    },
}

/// Decodes one row group one batch at a time. See the module documentation.
pub(super) struct IncrementalRowGroup {
    config: IncrementalConfig,
    row_group_idx: usize,
    row_count: usize,
    /// The pages that the readers can read. All readers share it.
    store: Arc<PageStore>,
    /// The offset/limit budget that is left after the rows given so far.
    budget: RowBudget,
    /// The predicates, if any.
    filter: Option<RowFilter>,
    /// The row selection of this row group, as row ranges.
    base: Vec<Range<usize>>,
    /// The predicates got all rows before this row. A multiple of
    /// `batch_size`.
    window_start: usize,
    /// No window is left to filter.
    filter_done: bool,
    /// One reader per predicate, and the row at which each reader is.
    pred_readers: Vec<Option<Box<dyn ArrayReader>>>,
    pred_pos: Vec<usize>,
    /// The queue: the rows that passed all predicates and the offset/limit
    /// budget, and are not output yet. Sorted.
    ready: Vec<Range<usize>>,
    ready_rows: usize,
    /// The output reader, and the row at which it is.
    out_reader: Option<Box<dyn ArrayReader>>,
    out_pos: usize,
    stage: Stage,
    finished: bool,
}

impl std::fmt::Debug for IncrementalRowGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalRowGroup")
            .field("row_group_idx", &self.row_group_idx)
            .field("row_count", &self.row_count)
            .field("budget", &self.budget)
            .field("window_start", &self.window_start)
            .field("ready_rows", &self.ready_rows)
            .field("finished", &self.finished)
            .finish()
    }
}

impl IncrementalRowGroup {
    /// Prepare to decode a row group. `filter` holds the predicates, if any.
    pub(super) fn new(
        config: IncrementalConfig,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        budget: RowBudget,
        filter: Option<RowFilter>,
    ) -> Self {
        let base = match &selection {
            Some(selection) => selection_to_ranges(selection, 0),
            None => std::iter::once(0..row_count).collect(),
        };
        let num_predicates = filter.as_ref().map_or(0, |filter| filter.predicates.len());
        Self {
            config,
            row_group_idx,
            row_count,
            store: Arc::new(PageStore::default()),
            budget,
            filter,
            base,
            window_start: 0,
            filter_done: false,
            pred_readers: (0..num_predicates).map(|_| None).collect(),
            pred_pos: vec![0; num_predicates],
            ready: vec![],
            ready_rows: 0,
            out_reader: None,
            out_pos: 0,
            stage: Stage::Idle,
            finished: false,
        }
    }

    /// The index of the row group.
    pub(super) fn row_group_idx(&self) -> usize {
        self.row_group_idx
    }

    /// The bytes in the [`PageStore`]. Does not include [`PushBuffers`].
    pub(super) fn buffered_bytes(&self) -> u64 {
        self.store.buffered_bytes()
    }

    /// The offset/limit budget that is left for the next row groups. The value
    /// is final when the row group is finished.
    pub(super) fn remaining_budget(&self) -> RowBudget {
        self.budget
    }

    /// Returns the [`RowFilter`], if any.
    pub(super) fn take_filter(&mut self) -> Option<RowFilter> {
        self.filter.take()
    }

    fn num_predicates(&self) -> usize {
        self.filter
            .as_ref()
            .map_or(0, |filter| filter.predicates.len())
    }

    /// Runs the next step. See *Page flow* in the module documentation.
    pub(super) fn try_next(
        &mut self,
        buffers: &mut PushBuffers,
    ) -> Result<IncrementalResult, ParquetError> {
        if self.finished {
            return Ok(IncrementalResult::Finished);
        }
        let result = self.step(buffers);
        if result.is_err() {
            // The state of the readers is unknown.
            self.finish();
        }
        result
    }

    /// Mark the row group finished and release the pages in the store. The
    /// caller releases the bytes of the row group in [`PushBuffers`].
    fn finish(&mut self) {
        self.finished = true;
        self.store.clear();
        self.pred_readers
            .iter_mut()
            .for_each(|reader| *reader = None);
        self.out_reader = None;
        self.ready.clear();
        self.ready_rows = 0;
    }

    /// Steps 2 and 3 of *Page flow* in the module documentation.
    ///
    /// Returns the `ranges` that are not in the store or in `buffers`. If none,
    /// moves the `ranges` from `buffers` into the store and returns an empty
    /// `Vec`.
    fn ingest(
        &self,
        buffers: &mut PushBuffers,
        ranges: impl IntoIterator<Item = Range<u64>>,
    ) -> Result<Vec<Range<u64>>, ParquetError> {
        let mut wanted: Vec<Range<u64>> = ranges
            .into_iter()
            .filter(|range| !range.is_empty() && !self.store.contains(range))
            .collect();
        wanted.sort_by_key(|range| range.start);
        wanted.dedup();
        let missing: Vec<Range<u64>> = wanted
            .iter()
            .filter(|range| !buffers.has_range(range))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Ok(missing);
        }
        for range in &wanted {
            let len = usize::try_from(range.end - range.start)
                .map_err(|e| ParquetError::General(format!("range too large: {e}")))?;
            let data = buffers.get_bytes(range.start, len).map_err(|e| {
                ParquetError::General(format!(
                    "Internal Error: missing data for range {range:?} in buffers: {e}"
                ))
            })?;
            self.store.insert(range.clone(), data);
        }
        // Release all ranges in one call, so that `buffers` changes one time
        // for each run of adjacent pages.
        buffers.release_ranges(&wanted);
        Ok(vec![])
    }

    fn step(&mut self, buffers: &mut PushBuffers) -> Result<IncrementalResult, ParquetError> {
        let batch_size = self.config.batch_size;
        loop {
            match std::mem::replace(&mut self.stage, Stage::Idle) {
                Stage::Idle => {
                    // See the table in `Stage`.
                    if self.ready_rows > batch_size || (self.filter_done && self.ready_rows > 0) {
                        let out = take_rows(&mut self.ready, batch_size);
                        self.ready_rows -= total_rows(&out);
                        self.stage = Stage::Output { out };
                        continue;
                    }
                    if self.filter_done {
                        self.finish();
                        return Ok(IncrementalResult::Finished);
                    }
                    // Filter the next window that has selected rows.
                    let Some(next) = next_row_at_or_after(&self.base, self.window_start) else {
                        self.filter_done = true;
                        continue;
                    };
                    let start = next - next % batch_size;
                    let end = (start + batch_size).min(self.row_count);
                    let cand = intersect(&self.base, start..end);
                    self.window_start = end;
                    self.stage = Stage::Predicate { cand, idx: 0 };
                }
                Stage::Predicate { cand, idx } => {
                    if cand.is_empty() || idx == self.num_predicates() {
                        self.queue_survivors(cand);
                        continue;
                    }
                    if let Some(missing) = self.step_predicate(buffers, cand, idx)? {
                        return Ok(IncrementalResult::NeedsData(missing));
                    }
                }
                Stage::Output { out } => {
                    let batch = match self.step_output(buffers, out)? {
                        Ok(batch) => batch,
                        Err(missing) => return Ok(IncrementalResult::NeedsData(missing)),
                    };
                    let last = self.filter_done && self.ready_rows == 0;
                    if last {
                        self.finish();
                    }
                    return Ok(IncrementalResult::Batch { batch, last });
                }
            }
        }
    }

    /// Apply the offset/limit budget to the rows `cand` that passed all
    /// predicates, and add them to the queue.
    fn queue_survivors(&mut self, cand: Vec<Range<usize>>) {
        let kept = if total_rows(&cand) == 0 {
            vec![]
        } else {
            let plan_builder = ReadPlanBuilder::new(self.config.batch_size)
                .with_selection(Some(ranges_to_selection(&cand, 0)));
            let budgeted = self.budget.apply_to_plan(plan_builder, self.row_count);
            self.budget = budgeted.remaining_budget;
            budgeted
                .plan_builder
                .selection()
                .map(|selection| selection_to_ranges(selection, 0))
                .unwrap_or_default()
        };
        self.ready_rows += total_rows(&kept);
        self.ready.extend(kept);
        if self.budget.is_exhausted()
            || next_row_at_or_after(&self.base, self.window_start).is_none()
        {
            self.filter_done = true;
        }
        self.stage = Stage::Idle;
    }

    /// The byte ranges that [`InMemoryRowGroup::fetch_ranges`] returns for
    /// these rows and columns. The row-group mode requests the same ranges.
    fn fetch_ranges(&self, projection: &ProjectionMask, rows: &[Range<usize>]) -> Vec<Range<u64>> {
        let num_columns = self
            .config
            .metadata
            .row_group(self.row_group_idx)
            .columns()
            .len();
        let planning = InMemoryRowGroup {
            row_count: self.row_count,
            column_chunks: vec![None; num_columns],
            page_index: row_group_page_index(&self.config.metadata, self.row_group_idx),
            row_group_idx: self.row_group_idx,
            metadata: &self.config.metadata,
        };
        planning
            .fetch_ranges(
                projection,
                Some(&ranges_to_selection(rows, 0)),
                self.config.batch_size,
                None,
            )
            .ranges
    }

    /// Build a reader for `projection` over the store.
    fn build_reader(
        &self,
        projection: &ProjectionMask,
    ) -> Result<Box<dyn ArrayReader>, ParquetError> {
        let shared = shared_row_group(
            &self.config.metadata,
            self.row_group_idx,
            self.row_count,
            projection,
            &self.store,
        );
        ArrayReaderBuilder::new(&shared, &self.config.metrics)
            .with_batch_size(self.config.batch_size)
            .with_parquet_metadata(&self.config.metadata)
            .build_array_reader(self.config.fields.as_deref(), projection)
    }

    /// Evaluate predicate `idx` on the rows `cand`.
    ///
    /// Returns the missing byte ranges, or `None` and sets the stage to
    /// predicate `idx + 1` with the rows that pass.
    fn step_predicate(
        &mut self,
        buffers: &mut PushBuffers,
        cand: Vec<Range<usize>>,
        idx: usize,
    ) -> Result<Option<Vec<Range<u64>>>, ParquetError> {
        let projection = self
            .filter
            .as_ref()
            .and_then(|filter| filter.predicates.get(idx))
            .ok_or_else(|| general_err!("Internal Error: predicate {idx} missing"))?
            .projection()
            .clone();
        let missing = self.ingest(buffers, self.fetch_ranges(&projection, &cand))?;
        if !missing.is_empty() {
            self.stage = Stage::Predicate { cand, idx };
            return Ok(Some(missing));
        }

        let array_reader = match self.pred_readers[idx].take() {
            Some(array_reader) => array_reader,
            None => self.build_reader(&projection)?,
        };
        let pos = self.pred_pos[idx];
        let relative = ranges_to_selection(&cand, pos);
        let consumed = relative.total_row_count();
        let mut reader = ParquetRecordBatchReader::new(array_reader, self.window_plan(&cand, pos));
        let predicate = self
            .filter
            .as_mut()
            .and_then(|filter| filter.predicates.get_mut(idx))
            .ok_or_else(|| general_err!("Internal Error: predicate {idx} missing"))?;
        let mut filters = vec![];
        let mut result = Ok(());
        for batch in reader.by_ref() {
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    result = Err(ParquetError::ArrowError(e.to_string()));
                    break;
                }
            };
            let input_rows = batch.num_rows();
            let filter = match predicate.evaluate(batch) {
                Ok(filter) => filter,
                Err(e) => {
                    result = Err(e.into());
                    break;
                }
            };
            if filter.len() != input_rows {
                result = Err(arrow_err!(
                    "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                    filter.len()
                ));
                break;
            }
            filters.push(match filter.null_count() {
                0 => filter,
                _ => prep_null_mask_filter(&filter),
            });
        }
        self.pred_readers[idx] = Some(reader.into_array_reader());
        result?;
        self.pred_pos[idx] = pos + consumed;

        let passed = relative.and_then(&RowSelection::from_filters(&filters));
        self.stage = Stage::Predicate {
            cand: selection_to_ranges(&passed, pos),
            idx: idx + 1,
        };
        Ok(None)
    }

    /// Decode one output batch for the rows `out`.
    ///
    /// Returns the missing byte ranges as the inner `Err`, and does not change
    /// the stage.
    fn step_output(
        &mut self,
        buffers: &mut PushBuffers,
        out: Vec<Range<usize>>,
    ) -> Result<Result<RecordBatch, Vec<Range<u64>>>, ParquetError> {
        let missing = self.ingest(buffers, self.fetch_ranges(&self.config.projection, &out))?;
        if !missing.is_empty() {
            self.stage = Stage::Output { out };
            return Ok(Err(missing));
        }

        let array_reader = match self.out_reader.take() {
            Some(array_reader) => array_reader,
            None => self.build_reader(&self.config.projection)?,
        };
        let pos = self.out_pos;
        let consumed = ranges_to_selection(&out, pos).total_row_count();
        // `out` has `batch_size` rows or less. Thus, the plan gives one batch,
        // which is the batch that the row-group mode gives at this point.
        let mut reader = ParquetRecordBatchReader::new(array_reader, self.window_plan(&out, pos));
        let batch = reader.next();
        let extra = match &batch {
            Some(Ok(_)) => reader.next(),
            _ => None,
        };
        self.out_reader = Some(reader.into_array_reader());
        self.out_pos = pos + consumed;
        if extra.is_some() {
            return Err(general_err!(
                "Internal Error: incremental output plan produced more than one batch"
            ));
        }
        match batch {
            Some(Ok(batch)) => Ok(Ok(batch)),
            Some(Err(e)) => Err(ParquetError::ArrowError(e.to_string())),
            None => Err(general_err!(
                "Internal Error: incremental output plan produced no batch"
            )),
        }
    }

    /// The read plan for the rows `rows`, for a reader at row `pos`.
    ///
    /// The plan uses [`RowSelectionPolicy::Selectors`]. A mask decodes all
    /// rows of a batch, so it can read a page that the store does not hold.
    fn window_plan(&self, rows: &[Range<usize>], pos: usize) -> ReadPlan {
        ReadPlanBuilder::new(self.config.batch_size)
            .with_selection(Some(ranges_to_selection(rows, pos)))
            .with_row_selection_policy(RowSelectionPolicy::Selectors)
            .build()
    }
}

/// The page index of `row_group_idx`, if the file has offset indexes.
fn row_group_page_index(
    metadata: &ParquetMetaData,
    row_group_idx: usize,
) -> Option<RowGroupPageIndex> {
    metadata
        .page_index()
        .is_some_and(|page_index| page_index.has_offset_indexes())
        .then(|| metadata.page_index_for_row_group(row_group_idx))
}

/// A row group whose `projection` columns read from `store`.
///
/// The readers keep an `Arc` of the store. Thus, they can read the pages
/// that the decoder adds after the readers are built.
fn shared_row_group<'a>(
    metadata: &'a ParquetMetaData,
    row_group_idx: usize,
    row_count: usize,
    projection: &ProjectionMask,
    store: &Arc<PageStore>,
) -> InMemoryRowGroup<'a> {
    let column_chunks = metadata
        .row_group(row_group_idx)
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            projection.leaf_included(idx).then(|| {
                Arc::new(ColumnChunkData::Shared {
                    length: column.byte_range().1 as usize,
                    store: Arc::clone(store),
                })
            })
        })
        .collect();
    InMemoryRowGroup {
        row_count,
        column_chunks,
        page_index: row_group_page_index(metadata, row_group_idx),
        row_group_idx,
        metadata,
    }
}

/// Row ranges to a [`RowSelection`] that starts at row `from`.
fn ranges_to_selection(ranges: &[Range<usize>], from: usize) -> RowSelection {
    let mut selectors = Vec::with_capacity(ranges.len() * 2);
    let mut pos = from;
    for range in ranges {
        debug_assert!(range.start >= pos, "ranges must be sorted and disjoint");
        if range.start > pos {
            selectors.push(RowSelector::skip(range.start - pos));
        }
        selectors.push(RowSelector::select(range.end - range.start));
        pos = range.end;
    }
    RowSelection::from(selectors)
}

/// The selected rows of a [`RowSelection`] that starts at row `from`, as row
/// ranges. The inverse of [`ranges_to_selection`].
fn selection_to_ranges(selection: &RowSelection, from: usize) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = vec![];
    let mut pos = from;
    for selector in selection.iter() {
        if !selector.skip && selector.row_count > 0 {
            match out.last_mut() {
                Some(last) if last.end == pos => last.end = pos + selector.row_count,
                _ => out.push(pos..pos + selector.row_count),
            }
        }
        pos += selector.row_count;
    }
    out
}

fn total_rows(ranges: &[Range<usize>]) -> usize {
    ranges.iter().map(|r| r.end - r.start).sum()
}

/// The first row of `ranges` at or after `row`.
fn next_row_at_or_after(ranges: &[Range<usize>], row: usize) -> Option<usize> {
    let idx = ranges.partition_point(|r| r.end <= row);
    ranges.get(idx).map(|r| r.start.max(row))
}

/// Clip `ranges` to `window`.
fn intersect(ranges: &[Range<usize>], window: Range<usize>) -> Vec<Range<usize>> {
    let first = ranges.partition_point(|r| r.end <= window.start);
    ranges[first..]
        .iter()
        .take_while(|r| r.start < window.end)
        .filter_map(|r| {
            let start = r.start.max(window.start);
            let end = r.end.min(window.end);
            (start < end).then_some(start..end)
        })
        .collect()
}

/// Remove and return the first `n` rows of `ranges`.
fn take_rows(ranges: &mut Vec<Range<usize>>, n: usize) -> Vec<Range<usize>> {
    let mut taken = vec![];
    let mut remaining = n;
    let mut consumed = 0;
    for range in ranges.iter_mut() {
        if remaining == 0 {
            break;
        }
        let len = range.end - range.start;
        if len <= remaining {
            taken.push(range.clone());
            remaining -= len;
            consumed += 1;
        } else {
            taken.push(range.start..range.start + remaining);
            range.start += remaining;
            remaining = 0;
        }
    }
    ranges.drain(..consumed);
    taken
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_and_selection_round_trip() {
        let ranges = vec![3..7, 10..12];
        let selection = ranges_to_selection(&ranges, 0);
        assert_eq!(selection.row_count(), 6);
        assert_eq!(selection_to_ranges(&selection, 0), ranges);
        let relative = ranges_to_selection(&ranges, 3);
        assert_eq!(relative.total_row_count(), 9);
        assert_eq!(selection_to_ranges(&relative, 3), ranges);
    }

    #[test]
    fn intersect_clips_to_window() {
        let ranges = vec![0..10, 20..30];
        assert_eq!(intersect(&ranges, 5..25), vec![5..10, 20..25]);
        assert_eq!(intersect(&ranges, 10..20), Vec::<Range<usize>>::new());
        assert_eq!(intersect(&ranges, 0..100), ranges);
    }

    #[test]
    fn next_row_finds_the_next_selected_row() {
        let ranges = vec![5..10, 20..30];
        assert_eq!(next_row_at_or_after(&ranges, 0), Some(5));
        assert_eq!(next_row_at_or_after(&ranges, 7), Some(7));
        assert_eq!(next_row_at_or_after(&ranges, 10), Some(20));
        assert_eq!(next_row_at_or_after(&ranges, 30), None);
    }

    #[test]
    fn take_rows_splits_ranges() {
        let mut ranges = vec![0..10, 20..30];
        assert_eq!(take_rows(&mut ranges, 4), vec![0..4]);
        assert_eq!(ranges, vec![4..10, 20..30]);
        assert_eq!(take_rows(&mut ranges, 8), vec![4..10, 20..22]);
        assert_eq!(ranges, vec![22..30]);
        assert_eq!(take_rows(&mut ranges, 100), vec![22..30]);
        assert!(ranges.is_empty());
    }
}
