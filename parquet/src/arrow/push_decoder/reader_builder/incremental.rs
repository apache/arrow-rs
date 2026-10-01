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

//! Batch-granular decoding of one row group. See [`IncrementalRowGroup`].

use super::RowBudget;
use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{
    ArrayReader, ArrayReaderBuilder, CacheOptionsBuilder, RowGroupCache,
};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::{LoadedRowRanges, RowSelectionStrategy};
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, ReadPlan, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy,
};
use crate::arrow::in_memory_row_group::{
    ColumnChunkData, InMemoryRowGroup, dictionary_range, page_range,
};
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
use std::sync::{Arc, RwLock};

/// The result of [`IncrementalRowGroup::try_next`].
#[derive(Debug)]
pub(crate) enum IncrementalResult {
    /// The bytes that the next step needs.
    NeedsData(Vec<Range<u64>>),
    /// The next output batch. `remaining_budget` is `Some` if this batch
    /// finishes the row group.
    Batch {
        batch: RecordBatch,
        remaining_budget: Option<RowBudget>,
    },
    /// The row group is finished, without more batches.
    Finished { remaining_budget: RowBudget },
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
    /// The columns in the predicate cache. On a cache miss, the output reads
    /// the column again for the full cache batch (`batch_size` rows, aligned
    /// to a multiple of `batch_size`). Thus, the fetch of these columns uses
    /// cache batch boundaries.
    pub(super) cache_projection: ProjectionMask,
    /// The maximum size in bytes of the predicate cache.
    pub(super) max_predicate_cache_size: usize,
    pub(super) row_selection_policy: RowSelectionPolicy,
}

/// The role of an array reader for the predicate cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheRole {
    /// A predicate reader. It writes the cached columns to the cache.
    Producer,
    /// The output reader. It reads the cached columns from the cache.
    Consumer,
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

/// Decodes one row group one batch at a time, for
/// [`FetchGranularity::Batch`].
///
/// The column readers are built before any byte is pushed. They read from a
/// [`PageStore`] that the decoder fills one step at a time:
///
/// ```text
///            push                 ingest (move)               get
///  caller ─────────▶ PushBuffers ──────────────▶ PageStore ◀─────── column readers
///                                                    │
///                                                    ▼ remove, when no reader
///                                                      needs the page again
/// ```
///
/// [`Self::release_passed_pages`] removes the data pages. Dictionary pages,
/// and column chunks without an offset index, stay until the row group is
/// finished.
///
/// Each step (see [`Stage`]) requests only the pages that it reads. This is
/// sound with an offset index, because a column reader:
///
/// * loads a page only when it decodes or skips a part of the page;
/// * skips a full page with the offset index only, without loading it;
/// * does not read after the last record that it must return, because a page
///   ends at a record boundary.
///
/// There is one reader per predicate and one for the output, for the full
/// row group. [`FetchGranularity::Batch`] describes the behavior that users
/// see.
///
/// [`FetchGranularity::Batch`]: crate::arrow::push_decoder::FetchGranularity::Batch
pub(super) struct IncrementalRowGroup {
    config: IncrementalConfig,
    row_group_idx: usize,
    row_count: usize,
    /// The pages that the readers can read. All readers share it.
    store: Arc<PageStore>,
    /// The byte ranges of each column chunk. See [`ColumnChunkPages`].
    chunks: Vec<ColumnChunkPages>,
    /// The offset/limit budget that is left after the rows given so far.
    budget: RowBudget,
    /// The predicates, if any.
    filter: Option<RowFilter>,
    /// The predicate cache. The predicate readers write the columns of
    /// [`IncrementalConfig::cache_projection`] to it, and the output reader
    /// reads them from it, as in the row-group mode.
    cache: Arc<RwLock<RowGroupCache>>,
    /// The row selection of this row group, as row ranges.
    base: Vec<Range<usize>>,
    /// The predicates got all rows before this row. A multiple of
    /// `batch_size`, so that windows align with the batches of the predicate
    /// cache.
    window_start: usize,
    /// The predicates do not read rows before this row again.
    predicate_resume_row: usize,
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
    /// One entry per read column. See
    /// [`IncrementalRowGroup::release_passed_pages`].
    release: Vec<ReleaseCursor>,
    finished: bool,
}

/// The readers of one column, and its next data page to release.
struct ReleaseCursor {
    /// The index of the column in [`IncrementalRowGroup::chunks`].
    column: usize,
    /// The data pages before this index are released.
    next: usize,
    /// A predicate reads this column.
    predicate: bool,
    /// The output reads this column.
    output: bool,
    /// The output reads this column from the predicate cache. On a cache
    /// miss, it reads the column again for the full cache batch.
    cached: bool,
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
    /// On success, this method takes the filter. On error, it leaves the
    /// filter in place.
    pub(super) fn new(
        config: IncrementalConfig,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        budget: RowBudget,
        filter: &mut Option<RowFilter>,
    ) -> Result<Self, ParquetError> {
        let chunks = ColumnChunkPages::for_row_group(&config.metadata, row_group_idx)?;
        let filter = filter.take();
        let base = match &selection {
            Some(selection) => selection_to_ranges(selection, 0),
            None => std::iter::once(0..row_count).collect(),
        };
        let num_predicates = filter.as_ref().map_or(0, |filter| filter.predicates.len());
        let cache = Arc::new(RwLock::new(RowGroupCache::new(
            config.batch_size,
            config.max_predicate_cache_size,
        )));
        let release = (0..chunks.len())
            .filter_map(|column| {
                let predicate = filter.as_ref().is_some_and(|filter| {
                    filter
                        .predicates
                        .iter()
                        .any(|predicate| predicate.projection().leaf_included(column))
                });
                let output = config.projection.leaf_included(column);
                let cached = output && config.cache_projection.leaf_included(column);
                (predicate || output).then_some(ReleaseCursor {
                    column,
                    next: 0,
                    predicate,
                    output,
                    cached,
                })
            })
            .collect();
        Ok(Self {
            config,
            row_group_idx,
            row_count,
            store: Arc::new(PageStore::default()),
            chunks,
            budget,
            filter,
            cache,
            base,
            window_start: 0,
            predicate_resume_row: 0,
            filter_done: false,
            pred_readers: (0..num_predicates).map(|_| None).collect(),
            pred_pos: vec![0; num_predicates],
            ready: vec![],
            ready_rows: 0,
            out_reader: None,
            out_pos: 0,
            stage: Stage::Idle,
            release,
            finished: false,
        })
    }

    /// The index of the row group.
    pub(super) fn row_group_idx(&self) -> usize {
        self.row_group_idx
    }

    /// The bytes in the [`PageStore`]. Does not include [`PushBuffers`].
    pub(super) fn buffered_bytes(&self) -> u64 {
        self.store.buffered_bytes()
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

    /// Runs the next step, or returns the bytes that it needs.
    pub(super) fn try_next(
        &mut self,
        buffers: &mut PushBuffers,
    ) -> Result<IncrementalResult, ParquetError> {
        if self.finished {
            return Ok(IncrementalResult::Finished {
                remaining_budget: self.budget,
            });
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

    /// Moves `ranges` from `buffers` into the store.
    ///
    /// Returns the `ranges` that are not in the store or in `buffers`, and
    /// then moves nothing.
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
                .map_err(|e| general_err!("range too large: {e}"))?;
            let data = buffers.get_bytes(range.start, len).map_err(|e| {
                general_err!("Internal Error: missing data for range {range:?} in buffers: {e}")
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
                        return Ok(IncrementalResult::Finished {
                            remaining_budget: self.budget,
                        });
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
                    self.predicate_resume_row = start;
                    self.stage = Stage::Predicate { cand, idx: 0 };
                }
                Stage::Predicate { cand, idx } => {
                    if cand.is_empty() || idx == self.num_predicates() {
                        self.queue_survivors(cand);
                        self.release_passed_pages(buffers);
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
                    } else {
                        self.release_passed_pages(buffers);
                    }
                    return Ok(IncrementalResult::Batch {
                        batch,
                        remaining_budget: last.then_some(self.budget),
                    });
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
        self.predicate_resume_row = self.window_start;
        if self.budget.is_exhausted()
            || next_row_at_or_after(&self.base, self.window_start).is_none()
        {
            self.filter_done = true;
        }
        self.stage = Stage::Idle;
    }

    /// The byte ranges that the rows `rows` of `projection` read. These are
    /// the ranges that the row-group mode requests for the same rows
    /// ([`InMemoryRowGroup::fetch_ranges`]). The cached columns use cache
    /// batch boundaries (see [`IncrementalConfig::cache_projection`]).
    fn fetch_ranges(&self, projection: &ProjectionMask, rows: &[Range<usize>]) -> Vec<Range<u64>> {
        let cache_projection = &self.config.cache_projection;
        let any_cached = (0..self.chunks.len())
            .any(|idx| projection.leaf_included(idx) && cache_projection.leaf_included(idx));
        let expanded = if any_cached {
            expand_to_batch_boundaries(rows, self.config.batch_size, self.row_count)
        } else {
            vec![]
        };
        let mut ranges = vec![];
        for (idx, chunk) in self.chunks.iter().enumerate() {
            if projection.leaf_included(idx) {
                let rows = if cache_projection.leaf_included(idx) {
                    &expanded
                } else {
                    rows
                };
                chunk.push_ranges(rows, &mut ranges);
            }
        }
        ranges
    }

    /// Build a predicate reader for `projection`. It writes the cached
    /// columns to the predicate cache.
    fn build_predicate_reader(
        &self,
        projection: &ProjectionMask,
    ) -> Result<Box<dyn ArrayReader>, ParquetError> {
        self.build_reader(projection, CacheRole::Producer)
    }

    /// Build the output reader. It reads the cached columns from the
    /// predicate cache.
    fn build_output_reader(&self) -> Result<Box<dyn ArrayReader>, ParquetError> {
        self.build_reader(&self.config.projection, CacheRole::Consumer)
    }

    /// Build a reader for `projection` over the store.
    fn build_reader(
        &self,
        projection: &ProjectionMask,
        role: CacheRole,
    ) -> Result<Box<dyn ArrayReader>, ParquetError> {
        let shared = shared_row_group(
            &self.config.metadata,
            self.row_group_idx,
            self.row_count,
            projection,
            &self.store,
        );
        let cache_options = CacheOptionsBuilder::new(&self.config.cache_projection, &self.cache);
        let cache_options = match role {
            CacheRole::Producer => cache_options.producer(),
            CacheRole::Consumer => cache_options.consumer(),
        };
        ArrayReaderBuilder::new(&shared, &self.config.metrics)
            .with_batch_size(self.config.batch_size)
            .with_cache_options(Some(&cache_options))
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
            None => self.build_predicate_reader(&projection)?,
        };
        let pos = self.pred_pos[idx];
        let relative = ranges_to_selection(&cand, pos);
        let consumed = relative.total_row_count();
        let plan = self.window_plan(&projection, &cand, pos);
        let mut reader = ParquetRecordBatchReader::new(array_reader, plan);
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
            None => self.build_output_reader()?,
        };
        let pos = self.out_pos;
        let consumed = ranges_to_selection(&out, pos).total_row_count();
        // `out` has `batch_size` rows or less. Thus, the plan gives one batch,
        // which is the batch that the row-group mode gives at this point.
        let plan = self.window_plan(&self.config.projection, &out, pos);
        let mut reader = ParquetRecordBatchReader::new(array_reader, plan);
        let batch = reader.next();
        // Only ask for another batch if the plan has rows left. With
        // selectors, `next` on an exhausted plan still decodes an empty batch
        // of every column. `is_exhausted` is always false for a plan without
        // a selection (`RowSelectionCursor::All`). That case is safe, because
        // `window_plan` always attaches a selection.
        let extra = match &batch {
            Some(Ok(_)) if !reader.is_exhausted() => reader.next(),
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

    /// Release the data pages that no reader reads again, from the store and
    /// from `buffers`.
    fn release_passed_pages(&mut self, buffers: &mut PushBuffers) {
        // A data page is released when its end row is at or before the first
        // row that a reader of its column can read again:
        //
        // | Reader of the column      | First row that it can read again        |
        // |---------------------------|-----------------------------------------|
        // | a predicate               | `predicate_resume_row`                  |
        // | the output                | the first queued row, else              |
        // |                           | `predicate_resume_row`; not after it    |
        // | the output, from the      | the output row, rounded down to a       |
        // | predicate cache           | multiple of `batch_size`                |
        //
        // On a cache miss, the output reads a cached column again for the
        // full cache batch (`batch_size` rows, aligned to a multiple of
        // `batch_size`), so it can read rows before the first queued row.
        //
        // If more than one reader reads the column, the smallest row applies.
        let predicate_row = self.predicate_resume_row;
        // The callers run after an output batch is done, or after a window
        // is queued, so no output batch is in progress.
        debug_assert!(matches!(self.stage, Stage::Idle));
        debug_assert!(self.ready.first().is_none_or(|r| r.start >= self.out_pos));
        let output_row = self
            .ready
            .first()
            .map_or(predicate_row, |r| r.start)
            .min(predicate_row);
        let cached_output_row = output_row - output_row % self.config.batch_size;

        let mut released = vec![];
        for cursor in &mut self.release {
            // A column without an offset index is one column chunk. It is
            // released when the row group is finished.
            let ColumnChunkPages::Pages { data, .. } = &self.chunks[cursor.column] else {
                continue;
            };
            let mut row = usize::MAX;
            if cursor.predicate {
                row = row.min(predicate_row);
            }
            if cursor.cached {
                row = row.min(cached_output_row);
            } else if cursor.output {
                row = row.min(output_row);
            }
            while let Some((range, _)) = data.get(cursor.next) {
                let end_row = data
                    .get(cursor.next + 1)
                    .map_or(self.row_count, |(_, first_row)| *first_row);
                if end_row > row {
                    break;
                }
                self.store.remove(range.start);
                released.push(range.clone());
                cursor.next += 1;
            }
        }
        buffers.release_ranges(&released);
    }

    /// The rows of the pages that the rows `rows` of `projection` read, in all
    /// columns that have an offset index. `None` if these are all rows of the
    /// row group. The row-group mode computes the same rows with
    /// `loaded_row_ranges_for_projection`.
    fn loaded_rows(
        &self,
        projection: &ProjectionMask,
        rows: &[Range<usize>],
    ) -> Option<Vec<Range<usize>>> {
        self.chunks
            .iter()
            .enumerate()
            .filter(|(idx, _)| projection.leaf_included(*idx))
            .filter_map(|(_, chunk)| chunk.page_rows(rows, self.row_count))
            .map(|rows| RowSelection::from_consecutive_ranges(rows.into_iter(), self.row_count))
            .reduce(|loaded, column| loaded.intersection(&column))
            .filter(|loaded| loaded.skipped_row_count() != 0)
            .map(|loaded| selection_to_ranges(&loaded, 0))
    }

    /// A [`ReadPlanBuilder`] that selects the rows `rows`, for a reader at row
    /// `pos`.
    fn plan_builder(&self, rows: &[Range<usize>], pos: usize) -> ReadPlanBuilder {
        ReadPlanBuilder::new(self.config.batch_size)
            .with_selection(Some(ranges_to_selection(rows, pos)))
    }

    /// The read plan for the rows `rows` of `projection`, for a reader at row
    /// `pos`. It uses the configured [`RowSelectionPolicy`], as the row-group
    /// mode does.
    fn window_plan(
        &self,
        projection: &ProjectionMask,
        rows: &[Range<usize>],
        pos: usize,
    ) -> ReadPlan {
        // Resolve the policy from the rows of this window only. The plan
        // starts at `pos`, and a reader that is behind would add a long first
        // skip, which changes the result of `Auto`.
        let window_start = rows.first().map_or(pos, |range| range.start);
        let strategy = self
            .plan_builder(rows, window_start)
            .with_row_selection_policy(self.config.row_selection_policy)
            .resolve_selection_strategy();
        let builder = self.plan_builder(rows, pos);
        let builder = match strategy {
            RowSelectionStrategy::Selectors => {
                builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
            }
            // A mask decodes all rows, so it must not read a page that is not
            // loaded. Limit it to the rows of the loaded pages, as
            // `prepare_selection_for_page_skipping` does for a row group.
            RowSelectionStrategy::Mask => {
                let loaded = self.loaded_rows(projection, rows).map(|loaded| {
                    // Make the rows relative to `pos`.
                    let ranges = loaded.into_iter().filter_map(|range| {
                        let start = range.start.max(pos);
                        (start < range.end).then(|| start - pos..range.end - pos)
                    });
                    LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
                        ranges,
                        self.row_count - pos,
                    ))
                });
                builder
                    .with_row_selection_policy(RowSelectionPolicy::Mask)
                    .with_loaded_row_ranges(loaded)
            }
        };
        builder.build()
    }
}

/// The byte ranges of one column chunk, from the offset index. Built one time
/// per row group, so that each step finds its pages with a binary search.
enum ColumnChunkPages {
    /// No offset index: a read of any row reads the full column chunk.
    Chunk(Range<u64>),
    Pages {
        /// The dictionary page, if any.
        dictionary: Option<Range<u64>>,
        /// `(byte range, first row)` of each data page, in row order.
        data: Vec<(Range<u64>, usize)>,
    },
}

impl ColumnChunkPages {
    /// Returns an error if the offset index of a column chunk with rows has
    /// no pages. The decoder cannot find the pages of such a column chunk.
    fn for_row_group(
        metadata: &ParquetMetaData,
        row_group_idx: usize,
    ) -> Result<Vec<Self>, ParquetError> {
        let page_index = row_group_page_index(metadata, row_group_idx);
        let row_group = metadata.row_group(row_group_idx);
        row_group
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let (start, len) = column.byte_range();
                let Some(locations) = page_index
                    .as_ref()
                    .and_then(|page_index| page_index.page_locations(idx))
                else {
                    return Ok(Self::Chunk(start..start + len));
                };
                if locations.is_empty() {
                    if row_group.num_rows() > 0 {
                        return Err(general_err!(
                            "Invalid offset index: column {idx} of row group {row_group_idx} \
                             has {} rows but no page locations",
                            row_group.num_rows()
                        ));
                    }
                    return Ok(Self::Chunk(start..start + len));
                }
                let data = locations
                    .iter()
                    .map(|location| (page_range(location), location.first_row_index as usize))
                    .collect();
                Ok(Self::Pages {
                    dictionary: dictionary_range(start, locations),
                    data,
                })
            })
            .collect()
    }

    /// The rows of the data pages that hold one of the sorted rows `rows`.
    /// `None` without an offset index, or without data pages.
    fn page_rows(&self, rows: &[Range<usize>], row_count: usize) -> Option<Vec<Range<usize>>> {
        let Self::Pages { data, .. } = self else {
            return None;
        };
        if data.is_empty() {
            return None;
        }
        let mut out: Vec<Range<usize>> = vec![];
        for rows in rows {
            let mut page = data
                .partition_point(|(_, first_row)| *first_row <= rows.start)
                .saturating_sub(1);
            while page < data.len() && data[page].1 < rows.end {
                let end = data.get(page + 1).map_or(row_count, |(_, first)| *first);
                let page_rows = data[page].1..end;
                match out.last_mut() {
                    Some(last) if last.end >= page_rows.start => {
                        last.end = last.end.max(page_rows.end)
                    }
                    _ => out.push(page_rows),
                }
                page += 1;
            }
        }
        Some(out)
    }

    /// Append the byte ranges that the sorted rows `rows` read: the
    /// dictionary page and each data page that holds one of the rows.
    fn push_ranges(&self, rows: &[Range<usize>], out: &mut Vec<Range<u64>>) {
        let (dictionary, data) = match self {
            Self::Chunk(range) => {
                out.push(range.clone());
                return;
            }
            Self::Pages { dictionary, data } => (dictionary, data),
        };
        out.extend(dictionary.iter().cloned());
        // The index of the first page that is not pushed yet. A page can hold
        // rows of more than one range, so it is pushed one time only.
        let mut next = 0;
        for rows in rows {
            let holds_start = data
                .partition_point(|(_, first_row)| *first_row <= rows.start)
                .saturating_sub(1);
            let mut page = holds_start.max(next);
            while page < data.len() && data[page].1 < rows.end {
                out.push(data[page].0.clone());
                page += 1;
            }
            next = next.max(page);
        }
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
    let end = ranges.last().map_or(from, |range| range.end);
    RowSelection::from_consecutive_ranges(
        ranges
            .iter()
            .map(|range| range.start - from..range.end - from),
        end - from,
    )
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

/// Expand the sorted rows `rows` to full batches of `batch_size` rows,
/// aligned to a multiple of `batch_size`, as
/// [`RowSelection::expand_to_batch_boundaries`] does.
///
/// This does not use the [`RowSelection`] method, because that needs a
/// conversion of `rows` to a [`RowSelection`] and back on each fetch. This
/// function works on the ranges directly, in one pass without allocation of
/// selectors.
fn expand_to_batch_boundaries(
    rows: &[Range<usize>],
    batch_size: usize,
    row_count: usize,
) -> Vec<Range<usize>> {
    let mut expanded: Vec<Range<usize>> = vec![];
    for range in rows {
        let start = range.start - range.start % batch_size;
        let end = range.end.div_ceil(batch_size) * batch_size;
        let range = start..end.min(row_count);
        match expanded.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => expanded.push(range),
        }
    }
    expanded
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
    use crate::arrow::arrow_reader::selection::RowSelectionCursor;
    use crate::arrow::push_decoder::test::{
        test_file_parquet_metadata, test_file_parquet_metadata_with_offset_index,
    };
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    /// One row group of 2000 rows. Pages end at about 200 bytes, so each
    /// column has its own page boundaries. Column "b" has a dictionary page.
    fn many_pages_metadata() -> Arc<ParquetMetaData> {
        use crate::arrow::ArrowWriter;
        use crate::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
        use crate::file::properties::WriterProperties;
        use arrow_array::{ArrayRef, Int64Array, StringArray};

        let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..2000));
        let b: ArrayRef = Arc::new(Int64Array::from_iter_values((0..2000).map(|i| i % 10)));
        let c: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..2000).map(|i| "x".repeat(i % 17)),
        ));
        let batch = RecordBatch::try_from_iter([("a", a), ("b", b), ("c", c)]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_column_dictionary_enabled("b".into(), true)
            .set_data_page_size_limit(200)
            .set_write_batch_size(5)
            .build();
        let mut file = vec![];
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&bytes::Bytes::from(file))
            .unwrap();
        Arc::new(metadata)
    }

    /// Sorted, disjoint row ranges in `0..row_count`: each has 1 to 39 rows,
    /// and the gap before the next has 1 to `max_gap - 1` rows.
    fn random_rows(rng: &mut StdRng, row_count: usize, max_gap: usize) -> Vec<Range<usize>> {
        let mut rows = vec![];
        let mut row = rng.random_range(0..row_count);
        while row < row_count {
            let end = (row + rng.random_range(1..40)).min(row_count);
            rows.push(row..end);
            row = end + rng.random_range(1..max_gap);
        }
        rows
    }

    /// [`ColumnChunkPages::push_ranges`] gives the ranges of
    /// [`InMemoryRowGroup::fetch_ranges`], with and without an offset index.
    #[test]
    fn test_fuzz_column_chunk_pages_match_fetch_ranges() {
        let many_pages = many_pages_metadata();
        let page_index = many_pages.page_index_for_row_group(0);
        let pages = (0..3)
            .map(|column| page_index.page_locations(column).unwrap().len())
            .collect::<Vec<_>>();
        assert!(pages.iter().all(|&n| n > 5), "{pages:?}");
        assert!(pages.windows(2).any(|w| w[0] != w[1]), "{pages:?}");
        for metadata in [
            many_pages,
            test_file_parquet_metadata_with_offset_index(),
            test_file_parquet_metadata(),
        ] {
            let row_count = metadata.row_group(0).num_rows() as usize;
            let num_columns = metadata.row_group(0).num_columns();
            let chunks = ColumnChunkPages::for_row_group(&metadata, 0).unwrap();
            let planning = InMemoryRowGroup {
                row_count,
                column_chunks: vec![None; num_columns],
                page_index: row_group_page_index(&metadata, 0),
                row_group_idx: 0,
                metadata: &metadata,
            };
            let mut rng = StdRng::seed_from_u64(0);
            for _ in 0..200 {
                let rows = random_rows(&mut rng, row_count, 60);
                let mut actual = vec![];
                for chunk in &chunks {
                    chunk.push_ranges(&rows, &mut actual);
                }
                let expected = planning
                    .fetch_ranges(
                        &ProjectionMask::all(),
                        Some(&ranges_to_selection(&rows, 0)),
                        100,
                        None,
                    )
                    .ranges;
                assert_eq!(actual, expected, "{rows:?}");
            }
        }
    }

    /// An offset index without page locations for a column chunk with rows
    /// gives a clear error.
    #[test]
    fn test_empty_page_locations_error() {
        use crate::file::metadata::page_index::PageIndexBuilder;
        use crate::file::page_index::offset_index::OffsetIndexMetaData;

        let metadata = test_file_parquet_metadata();
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.row_group(0).num_columns();
        let mut builder = PageIndexBuilder::new(num_row_groups, num_columns);
        builder.allocate_offset_indexes(num_row_groups, num_columns);
        builder.put_offset_index(
            OffsetIndexMetaData {
                page_locations: vec![],
                unencoded_byte_array_data_bytes: None,
            },
            0,
            0,
        );
        let metadata = Arc::unwrap_or_clone(metadata)
            .into_builder()
            .set_page_index(Some(Arc::new(builder.build())))
            .build();
        let err = ColumnChunkPages::for_row_group(&metadata, 0)
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("column 0 of row group 0") && err.contains("no page locations"),
            "{err}"
        );
    }

    /// A config with `batch_size` 100, all columns, no predicate cache and
    /// the selection policy `policy`.
    fn test_config(
        metadata: Arc<ParquetMetaData>,
        policy: RowSelectionPolicy,
    ) -> IncrementalConfig {
        let num_columns = metadata.row_group(0).num_columns();
        IncrementalConfig {
            batch_size: 100,
            projection: ProjectionMask::all(),
            metadata,
            fields: None,
            metrics: ArrowReaderMetrics::disabled(),
            cache_projection: ProjectionMask::none(num_columns),
            max_predicate_cache_size: 0,
            row_selection_policy: policy,
        }
    }

    /// [`IncrementalRowGroup::loaded_rows`] gives the rows of
    /// `loaded_row_ranges_for_projection`, which the row-group mode uses.
    #[test]
    fn test_fuzz_loaded_rows_match_the_row_group_mode() {
        use super::super::loaded_row_ranges_for_projection;
        let metadata = many_pages_metadata();
        let row_count = metadata.row_group(0).num_rows() as usize;
        let config = test_config(Arc::clone(&metadata), RowSelectionPolicy::Mask);
        let row_group = IncrementalRowGroup::new(
            config,
            0,
            row_count,
            None,
            RowBudget::new(None, None),
            &mut None,
        )
        .unwrap();
        let schema = metadata.file_metadata().schema_descr();
        let mut rng = StdRng::seed_from_u64(0);
        let mut limited = 0;
        for _ in 0..200 {
            let rows = random_rows(&mut rng, row_count, 400);
            let columns: Vec<usize> = (0..3).filter(|_| rng.random_bool(0.6)).collect();
            let projection = ProjectionMask::leaves(schema, columns);
            let expected = loaded_row_ranges_for_projection(
                Some(&ranges_to_selection(&rows, 0)),
                &projection,
                row_group_page_index(&metadata, 0),
                3,
                row_count,
            )
            .map(|loaded| loaded.ranges().to_vec());
            limited += usize::from(expected.is_some());
            assert_eq!(
                row_group.loaded_rows(&projection, &rows),
                expected,
                "{rows:?}"
            );
        }
        assert!(limited > 50, "{limited}");
    }

    /// The windows use the configured policy, as the row-group mode does. A
    /// plan ends at its last selected row: `step_output` checks for a second
    /// batch only if the plan has rows left after the first batch.
    #[test]
    fn test_window_plan_uses_the_configured_policy_and_ends_after_its_rows() {
        for policy in [RowSelectionPolicy::Selectors, RowSelectionPolicy::Mask] {
            let config = test_config(test_file_parquet_metadata_with_offset_index(), policy);
            let row_group = IncrementalRowGroup::new(
                config,
                0,
                200,
                None,
                RowBudget::new(None, None),
                &mut None,
            )
            .unwrap();
            // A reader at the start, and a reader behind the window.
            for (rows, pos) in [(vec![0..10, 20..30], 0), (vec![150..160, 170..180], 20)] {
                let mut plan = row_group.window_plan(&ProjectionMask::all(), &rows, pos);
                match (policy, plan.row_selection_cursor_mut()) {
                    (RowSelectionPolicy::Selectors, RowSelectionCursor::Selectors(cursor)) => {
                        // Reading the selected rows exhausts the plan.
                        let mut selected = 0;
                        while !cursor.is_empty() {
                            let selector = cursor.next_selector();
                            if !selector.skip {
                                selected += selector.row_count;
                            }
                        }
                        assert_eq!(selected, 20, "{rows:?} {pos}");
                    }
                    (RowSelectionPolicy::Mask, RowSelectionCursor::Mask(_)) => {}
                    _ => panic!("{policy:?} {rows:?} {pos}: wrong cursor"),
                }
            }
        }
    }

    #[test]
    fn test_fuzz_expand_to_batch_boundaries_matches_row_selection() {
        let mut rng = StdRng::seed_from_u64(0);
        for _ in 0..200 {
            let row_count = rng.random_range(1..500);
            let batch_size = rng.random_range(1..50);
            let rows = random_rows(&mut rng, row_count, 60);
            let expected =
                ranges_to_selection(&rows, 0).expand_to_batch_boundaries(batch_size, row_count);
            let actual = expand_to_batch_boundaries(&rows, batch_size, row_count);
            assert_eq!(
                selection_to_ranges(&expected, 0),
                actual,
                "{rows:?} {batch_size}"
            );
        }
    }
}
