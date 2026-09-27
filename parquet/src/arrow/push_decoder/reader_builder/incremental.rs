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
//! document for [`FetchGranularity::Batch`]. The user-visible behavior,
//! including when bytes are released, is in the documentation of
//! [`FetchGranularity::Batch`].
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
//!                                                    │
//!                                                    ▼ remove, when no reader
//!                                                      needs the page again
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
//! # Modes
//!
//! | [`Mode`] | Used when | Readers, kept for the full row group | One step |
//! |---|---|---|---|
//! | [`Unfiltered`] | no predicates. The selection is final. | one [`ParquetRecordBatchReader`] | one output batch |
//! | [`Filtered`] | predicates. The selection is known only after the predicates run. | one [`ArrayReader`] per predicate, and one for the output | one stage of [`Stage`] |
//!
//! Both modes give the same batches as the row-group mode.
//!
//! [`FetchGranularity::Batch`]: crate::arrow::push_decoder::FetchGranularity::Batch

use super::{
    BudgetedReadPlan, RowBudget, loaded_row_ranges_for_projection,
    prepare_selection_for_page_skipping,
};
use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{
    ArrayReader, ArrayReaderBuilder, CacheOptionsBuilder, RowGroupCache,
};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::{LoadedRowRanges, RowSelectionStrategy};
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, ReadPlan, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy, RowSelector,
};
use crate::arrow::in_memory_row_group::{ColumnChunkData, InMemoryRowGroup};
use crate::arrow::push_decoder::page_spans::{PageSpan, SelectedRows, SpanKind, column_page_spans};
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
    pub(super) row_selection_policy: RowSelectionPolicy,
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
    /// The leaf columns that a predicate or the output reads.
    read_columns: ProjectionMask,
    mode: Mode,
    finished: bool,
}

impl std::fmt::Debug for IncrementalRowGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalRowGroup")
            .field("row_group_idx", &self.row_group_idx)
            .field("row_count", &self.row_count)
            .field("budget", &self.budget)
            .field("filtered", &matches!(self.mode, Mode::Filtered(_)))
            .field("finished", &self.finished)
            .finish()
    }
}

/// See *Modes* in the module documentation.
enum Mode {
    Unfiltered(Unfiltered),
    Filtered(Box<Filtered>),
}

/// The state of a row group without predicates.
struct Unfiltered {
    /// The reader for the full row group. `None` if it reads no rows.
    reader: Option<ParquetRecordBatchReader>,
    /// The pages that are not released, sorted by `first_row`. See
    /// [`PageSpan`] for the row positions.
    pages: Vec<PageSpan>,
    /// The number of output rows given so far.
    emitted: u64,
    /// The number of output rows of the row group.
    total: u64,
}

/// The stage of a [`Filtered`] row group.
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

/// The state of a row group with predicates. See [`Stage`].
struct Filtered {
    filter: RowFilter,
    cache: Arc<RwLock<RowGroupCache>>,
    /// See [`ColumnPages::cached`].
    cache_projection: ProjectionMask,
    /// The row selection of this row group, as row ranges.
    base: Vec<Range<usize>>,
    /// The predicates got all rows before this row. A multiple of
    /// `batch_size`, so that windows align with the batches of the predicate
    /// cache.
    window_start: usize,
    /// The predicates do not read rows before this row again.
    filter_frontier: usize,
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
    /// The data pages of each read column. See
    /// [`IncrementalRowGroup::release_passed_pages`].
    columns: Vec<ColumnPages>,
}

/// The data pages of one column chunk, in row order.
struct ColumnPages {
    /// `(byte range, end row)` of each data page. The end row is exclusive.
    pages: Vec<(Range<u64>, usize)>,
    /// The pages before this index are released.
    next: usize,
    /// A predicate reads this column.
    predicate: bool,
    /// The output reads this column.
    output: bool,
    /// The output reads this column from the predicate cache.
    ///
    /// If a value is not in the cache (a cache miss), the output reads the
    /// column again for the full cache batch. A cache batch is `batch_size`
    /// rows, aligned to a multiple of `batch_size`. Thus, the fetch and the
    /// release of a cached column use cache batch boundaries.
    cached: bool,
}

impl IncrementalRowGroup {
    /// Prepare to decode a row group without predicates.
    pub(super) fn try_new_unfiltered(
        config: IncrementalConfig,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        budget: RowBudget,
    ) -> Result<Self, ParquetError> {
        let page_index = row_group_page_index(&config.metadata, row_group_idx);
        let BudgetedReadPlan {
            plan_builder,
            rows_after_budget,
            remaining_budget,
            ..
        } = budget.apply_to_plan(
            ReadPlanBuilder::new(config.batch_size).with_selection(selection),
            row_count,
        );
        let read_columns = config.projection.clone();
        let store = Arc::new(PageStore::default());

        if rows_after_budget == 0 {
            return Ok(Self {
                config,
                row_group_idx,
                row_count,
                store,
                budget: remaining_budget,
                read_columns,
                mode: Mode::Unfiltered(Unfiltered {
                    reader: None,
                    pages: vec![],
                    emitted: 0,
                    total: 0,
                }),
                finished: false,
            });
        }

        let num_columns = config.metadata.file_metadata().schema_descr().num_columns();
        let plan_builder = prepare_selection_for_page_skipping(
            plan_builder.with_row_selection_policy(config.row_selection_policy),
            &config.projection,
            row_group_page_index(&config.metadata, row_group_idx),
            num_columns,
            row_count,
        );

        // The pages the reader loads, tagged with the output rows they serve.
        // These are the ranges `fetch_ranges` requests, split at pages.
        let selection = plan_builder.selection();
        let rows = SelectedRows::new(selection, row_count);
        let total = rows.selected_before(row_count);
        let row_group = config.metadata.row_group(row_group_idx);
        let mut pages = vec![];
        for (column_idx, chunk) in row_group.columns().iter().enumerate() {
            if !config.projection.leaf_included(column_idx) {
                continue;
            }
            let (start, len) = chunk.byte_range();
            let locations = page_index
                .as_ref()
                .and_then(|page_index| page_index.page_locations(column_idx))
                .map(|locations| locations.as_slice());
            column_page_spans(
                start..start + len,
                locations,
                selection,
                &rows,
                row_count,
                0,
                &mut pages,
            );
        }
        pages.sort_by_key(|p| (p.first_row, p.range.start));

        let plan = plan_builder.build();
        let array_reader = {
            let shared = shared_row_group(
                &config.metadata,
                row_group_idx,
                row_count,
                &config.projection,
                &store,
            );
            ArrayReaderBuilder::new(&shared, &config.metrics)
                .with_batch_size(config.batch_size)
                .with_parquet_metadata(&config.metadata)
                .build_array_reader(config.fields.as_deref(), &config.projection)?
        };

        Ok(Self {
            config,
            row_group_idx,
            row_count,
            store,
            budget: remaining_budget,
            read_columns,
            mode: Mode::Unfiltered(Unfiltered {
                reader: Some(ParquetRecordBatchReader::new(array_reader, plan)),
                pages,
                emitted: 0,
                total,
            }),
            finished: false,
        })
    }

    /// Prepare to decode a row group with the predicates of `filter`.
    ///
    /// `cache_projection`: the columns that the output reads from the
    /// predicate cache (see [`ColumnPages::cached`]).
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new_filtered(
        config: IncrementalConfig,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        budget: RowBudget,
        filter: RowFilter,
        cache_projection: ProjectionMask,
        max_predicate_cache_size: usize,
    ) -> Self {
        let base = match &selection {
            Some(selection) => selection_to_ranges(selection, 0),
            None => std::iter::once(0..row_count).collect(),
        };
        let mut read_columns = config.projection.clone();
        for predicate in &filter.predicates {
            read_columns.union(predicate.projection());
        }

        // A column without an offset index has no pages here. It is one
        // column chunk, released by `finish`.
        let page_index = row_group_page_index(&config.metadata, row_group_idx);
        let num_columns = config.metadata.row_group(row_group_idx).columns().len();
        let columns = (0..num_columns)
            .filter(|&idx| read_columns.leaf_included(idx))
            .map(|idx| {
                let locations = page_index
                    .as_ref()
                    .and_then(|page_index| page_index.page_locations(idx));
                let pages = locations
                    .map(|locations| {
                        locations
                            .iter()
                            .enumerate()
                            .map(|(page, location)| {
                                let start = location.offset as u64;
                                let end_row = locations
                                    .get(page + 1)
                                    .map(|next| next.first_row_index as usize)
                                    .unwrap_or(row_count);
                                (start..start + location.compressed_page_size as u64, end_row)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                ColumnPages {
                    pages,
                    next: 0,
                    predicate: filter
                        .predicates
                        .iter()
                        .any(|predicate| predicate.projection().leaf_included(idx)),
                    output: config.projection.leaf_included(idx),
                    cached: cache_projection.leaf_included(idx),
                }
            })
            .collect();

        let num_predicates = filter.predicates.len();
        let cache = Arc::new(RwLock::new(RowGroupCache::new(
            config.batch_size,
            max_predicate_cache_size,
        )));
        Self {
            config,
            row_group_idx,
            row_count,
            store: Arc::new(PageStore::default()),
            budget,
            read_columns,
            mode: Mode::Filtered(Box::new(Filtered {
                filter,
                cache,
                cache_projection,
                base,
                window_start: 0,
                filter_frontier: 0,
                filter_done: false,
                pred_readers: (0..num_predicates).map(|_| None).collect(),
                pred_pos: vec![0; num_predicates],
                ready: vec![],
                ready_rows: 0,
                out_reader: None,
                out_pos: 0,
                stage: Stage::Idle,
                columns,
            })),
            finished: false,
        }
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

    /// Returns the [`RowFilter`] of a row group with predicates, and puts an
    /// empty [`RowFilter`] in its place.
    pub(super) fn take_filter(&mut self) -> Option<RowFilter> {
        match &mut self.mode {
            Mode::Unfiltered(_) => None,
            Mode::Filtered(filtered) => Some(std::mem::replace(
                &mut filtered.filter,
                RowFilter::new(vec![]),
            )),
        }
    }

    /// Runs the next step. See *Page flow* in the module documentation.
    pub(super) fn try_next(
        &mut self,
        buffers: &mut PushBuffers,
    ) -> Result<IncrementalResult, ParquetError> {
        if self.finished {
            return Ok(IncrementalResult::Finished);
        }
        let result = match &self.mode {
            Mode::Unfiltered(_) => self.try_next_unfiltered(buffers),
            Mode::Filtered(_) => self.try_next_filtered(buffers),
        };
        if result.is_err() {
            // The state of the readers is unknown. Release all bytes.
            self.finish(buffers);
        }
        result
    }

    /// Mark the row group finished and release its bytes.
    fn finish(&mut self, buffers: &mut PushBuffers) {
        self.finished = true;
        self.store.clear();
        match &mut self.mode {
            Mode::Unfiltered(state) => {
                state.reader = None;
                state.pages.clear();
            }
            Mode::Filtered(state) => {
                state
                    .pred_readers
                    .iter_mut()
                    .for_each(|reader| *reader = None);
                state.out_reader = None;
                state.ready.clear();
                state.ready_rows = 0;
            }
        }
        // Release all bytes of the column chunks that the row group read.
        // This includes bytes that were pushed but not requested.
        let row_group = self.config.metadata.row_group(self.row_group_idx);
        let chunks: Vec<Range<u64>> = row_group
            .columns()
            .iter()
            .enumerate()
            .filter(|(idx, _)| self.read_columns.leaf_included(*idx))
            .map(|(_, chunk)| {
                let (start, len) = chunk.byte_range();
                start..start + len
            })
            .collect();
        buffers.release_ranges(&chunks);
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

    fn try_next_unfiltered(
        &mut self,
        buffers: &mut PushBuffers,
    ) -> Result<IncrementalResult, ParquetError> {
        let batch_size = self.config.batch_size as u64;
        let Mode::Unfiltered(state) = &self.mode else {
            unreachable!("unfiltered mode")
        };
        if state.emitted >= state.total {
            self.finish(buffers);
            return Ok(IncrementalResult::Finished);
        }

        // The pages of the next `batch_size` output rows.
        let emitted = state.emitted;
        let batch_end = emitted + batch_size;
        let needed: Vec<Range<u64>> = state
            .pages
            .iter()
            .take_while(|p| p.first_row < batch_end)
            .filter(|p| p.last_row > emitted)
            .map(|p| p.range.clone())
            .collect();
        let missing = self.ingest(buffers, needed)?;
        if !missing.is_empty() {
            return Ok(IncrementalResult::NeedsData(missing));
        }

        let Mode::Unfiltered(state) = &mut self.mode else {
            unreachable!("unfiltered mode")
        };
        let reader = state
            .reader
            .as_mut()
            .ok_or_else(|| general_err!("Internal Error: incremental reader missing"))?;
        let batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(ParquetError::ArrowError(e.to_string())),
            None => {
                return Err(general_err!(
                    "Internal Error: row group {} ended after {} of {} rows",
                    self.row_group_idx,
                    state.emitted,
                    state.total
                ));
            }
        };
        state.emitted += batch.num_rows() as u64;
        let emitted = state.emitted;
        if emitted >= state.total {
            self.finish(buffers);
            return Ok(IncrementalResult::Batch { batch, last: true });
        }

        // Release the data pages whose rows are all before `emitted`. The
        // reader loaded these pages already. `pages` is sorted by
        // `first_row`, so only the pages in `..prefix` can end before
        // `emitted`.
        let prefix = state.pages.partition_point(|p| p.first_row < emitted);
        let mut idx = 0;
        let mut released = vec![];
        state.pages.retain(|page| {
            let passed = idx < prefix && page.last_row <= emitted && page.kind == SpanKind::Data;
            idx += 1;
            if passed {
                self.store.remove(page.range.start);
                released.push(page.range.clone());
            }
            !passed
        });
        buffers.release_ranges(&released);
        Ok(IncrementalResult::Batch { batch, last: false })
    }

    fn try_next_filtered(
        &mut self,
        buffers: &mut PushBuffers,
    ) -> Result<IncrementalResult, ParquetError> {
        let batch_size = self.config.batch_size;
        loop {
            let Mode::Filtered(state) = &mut self.mode else {
                unreachable!("filtered mode")
            };
            match std::mem::replace(&mut state.stage, Stage::Idle) {
                Stage::Idle => {
                    // See the table in `Stage`.
                    if state.ready_rows > batch_size || (state.filter_done && state.ready_rows > 0)
                    {
                        let out = take_rows(&mut state.ready, batch_size);
                        state.ready_rows -= total_rows(&out);
                        state.stage = Stage::Output { out };
                        continue;
                    }
                    if state.filter_done {
                        self.finish(buffers);
                        return Ok(IncrementalResult::Finished);
                    }
                    // Filter the next window that has selected rows.
                    let Some(next) = next_row_at_or_after(&state.base, state.window_start) else {
                        state.filter_done = true;
                        continue;
                    };
                    let start = next - next % batch_size;
                    let end = (start + batch_size).min(self.row_count);
                    let cand = intersect(&state.base, start..end);
                    state.window_start = end;
                    state.filter_frontier = start;
                    state.stage = Stage::Predicate { cand, idx: 0 };
                }
                Stage::Predicate { cand, idx } => {
                    if cand.is_empty() || idx == state.filter.predicates.len() {
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
                    let Mode::Filtered(state) = &self.mode else {
                        unreachable!("filtered mode")
                    };
                    if state.filter_done && state.ready_rows == 0 {
                        self.finish(buffers);
                        return Ok(IncrementalResult::Batch { batch, last: true });
                    }
                    self.release_passed_pages(buffers);
                    return Ok(IncrementalResult::Batch { batch, last: false });
                }
            }
        }
    }

    /// Apply the offset/limit budget to the rows `cand` that passed all
    /// predicates, and add them to the queue.
    fn queue_survivors(&mut self, cand: Vec<Range<usize>>) {
        let batch_size = self.config.batch_size;
        let selected = total_rows(&cand);
        let kept = if selected == 0 {
            vec![]
        } else {
            let plan_builder = ReadPlanBuilder::new(batch_size)
                .with_selection(Some(ranges_to_selection(&cand, 0)));
            let BudgetedReadPlan {
                plan_builder,
                remaining_budget,
                ..
            } = self.budget.apply_to_plan(plan_builder, self.row_count);
            self.budget = remaining_budget;
            plan_builder
                .selection()
                .map(|selection| selection_to_ranges(selection, 0))
                .unwrap_or_default()
        };
        let budget_exhausted = self.budget.is_exhausted();
        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        state.ready_rows += total_rows(&kept);
        state.ready.extend(kept);
        state.filter_frontier = state.window_start;
        if budget_exhausted || next_row_at_or_after(&state.base, state.window_start).is_none() {
            state.filter_done = true;
        }
        state.stage = Stage::Idle;
    }

    /// The byte ranges that the output reads for the rows `out`.
    fn output_ranges(&self, out: &[Range<usize>]) -> Vec<Range<u64>> {
        let Mode::Filtered(state) = &self.mode else {
            unreachable!("filtered mode")
        };
        // See `ColumnPages::cached`.
        self.fetch_ranges(
            &self.config.projection,
            &ranges_to_selection(out, 0),
            Some(&state.cache_projection),
        )
    }

    /// The byte ranges that [`InMemoryRowGroup::fetch_ranges`] returns for
    /// these rows and columns. The row-group mode requests the same ranges.
    fn fetch_ranges(
        &self,
        projection: &ProjectionMask,
        selection: &RowSelection,
        cache_projection: Option<&ProjectionMask>,
    ) -> Vec<Range<u64>> {
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
                Some(selection),
                self.config.batch_size,
                cache_projection,
            )
            .ranges
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
        let Mode::Filtered(state) = &self.mode else {
            unreachable!("filtered mode")
        };
        let projection = state.filter.predicates[idx].projection().clone();
        // See `ColumnPages::cached`.
        let ranges = self.fetch_ranges(
            &projection,
            &ranges_to_selection(&cand, 0),
            Some(&state.cache_projection),
        );
        let missing = self.ingest(buffers, ranges)?;
        if !missing.is_empty() {
            let Mode::Filtered(state) = &mut self.mode else {
                unreachable!("filtered mode")
            };
            state.stage = Stage::Predicate { cand, idx };
            return Ok(Some(missing));
        }

        if state.pred_readers[idx].is_none() {
            let shared = shared_row_group(
                &self.config.metadata,
                self.row_group_idx,
                self.row_count,
                &projection,
                &self.store,
            );
            let cache_options =
                CacheOptionsBuilder::new(&state.cache_projection, &state.cache).producer();
            let array_reader = ArrayReaderBuilder::new(&shared, &self.config.metrics)
                .with_batch_size(self.config.batch_size)
                .with_cache_options(Some(&cache_options))
                .with_parquet_metadata(&self.config.metadata)
                .build_array_reader(self.config.fields.as_deref(), &projection)?;
            let Mode::Filtered(state) = &mut self.mode else {
                unreachable!("filtered mode")
            };
            state.pred_readers[idx] = Some(array_reader);
        }

        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        let pos = state.pred_pos[idx];
        let relative = ranges_to_selection(&cand, pos);
        let consumed = relative.total_row_count();
        let plan = self.window_plan(&projection, &cand, pos);
        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        let array_reader = state.pred_readers[idx]
            .take()
            .ok_or_else(|| general_err!("Internal Error: predicate reader missing"))?;
        let mut reader = ParquetRecordBatchReader::new(array_reader, plan);
        let predicate = state.filter.predicates[idx].as_mut();
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
        state.pred_readers[idx] = Some(reader.into_array_reader());
        result?;
        state.pred_pos[idx] = pos + consumed;

        let passed = relative.and_then(&RowSelection::from_filters(&filters));
        state.stage = Stage::Predicate {
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
        let ranges = self.output_ranges(&out);
        let missing = self.ingest(buffers, ranges)?;
        if !missing.is_empty() {
            let Mode::Filtered(state) = &mut self.mode else {
                unreachable!("filtered mode")
            };
            state.stage = Stage::Output { out };
            return Ok(Err(missing));
        }

        let Mode::Filtered(state) = &self.mode else {
            unreachable!("filtered mode")
        };
        if state.out_reader.is_none() {
            let shared = shared_row_group(
                &self.config.metadata,
                self.row_group_idx,
                self.row_count,
                &self.config.projection,
                &self.store,
            );
            let cache_options =
                CacheOptionsBuilder::new(&state.cache_projection, &state.cache).consumer();
            let array_reader = ArrayReaderBuilder::new(&shared, &self.config.metrics)
                .with_batch_size(self.config.batch_size)
                .with_cache_options(Some(&cache_options))
                .with_parquet_metadata(&self.config.metadata)
                .build_array_reader(self.config.fields.as_deref(), &self.config.projection)?;
            let Mode::Filtered(state) = &mut self.mode else {
                unreachable!("filtered mode")
            };
            state.out_reader = Some(array_reader);
        }

        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        let pos = state.out_pos;
        let consumed = ranges_to_selection(&out, pos).total_row_count();
        let plan = self.window_plan(&self.config.projection, &out, pos);
        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        let array_reader = state
            .out_reader
            .take()
            .ok_or_else(|| general_err!("Internal Error: output reader missing"))?;
        // `out` has `batch_size` rows or less. Thus, the plan gives one batch,
        // which is the batch that the row-group mode gives at this point.
        let mut reader = ParquetRecordBatchReader::new(array_reader, plan);
        let batch = reader.next();
        let extra = match &batch {
            Some(Ok(_)) => reader.next(),
            _ => None,
        };
        state.out_reader = Some(reader.into_array_reader());
        state.out_pos = pos + consumed;
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

    /// The read plan for the rows `rows` of `projection`, for a reader at row
    /// `pos`.
    ///
    /// | Strategy of the configured [`RowSelectionPolicy`] | Plan |
    /// |---|---|
    /// | [`RowSelectionStrategy::Selectors`] | selectors |
    /// | [`RowSelectionStrategy::Mask`] | mask, limited to the rows of the loaded pages, as in [`prepare_selection_for_page_skipping`]. A mask decodes all rows, so it must not read a page that is not loaded. |
    fn window_plan(
        &self,
        projection: &ProjectionMask,
        rows: &[Range<usize>],
        pos: usize,
    ) -> ReadPlan {
        let builder = ReadPlanBuilder::new(self.config.batch_size)
            .with_selection(Some(ranges_to_selection(rows, pos)))
            .with_row_selection_policy(self.config.row_selection_policy);
        let builder = match builder.resolve_selection_strategy() {
            RowSelectionStrategy::Selectors => {
                builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
            }
            RowSelectionStrategy::Mask => {
                let num_columns = self
                    .config
                    .metadata
                    .file_metadata()
                    .schema_descr()
                    .num_columns();
                let loaded = loaded_row_ranges_for_projection(
                    Some(&ranges_to_selection(rows, 0)),
                    projection,
                    row_group_page_index(&self.config.metadata, self.row_group_idx),
                    num_columns,
                    self.row_count,
                )
                .map(|loaded| {
                    // Rows relative to `pos`.
                    let ranges = loaded.ranges().iter().filter_map(|range| {
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

    /// Release the data pages that no reader reads again.
    ///
    /// A data page is released when its end row is at or before the first
    /// row that a reader of its column can read again:
    ///
    /// | Reader of the column | First row that it can read again |
    /// |---|---|
    /// | a predicate | `filter_frontier` |
    /// | the output | the first row of `Stage::Output`, else of the queue, else `filter_frontier`. Not after `filter_frontier`. |
    /// | the output, from the predicate cache | the row of the output, rounded down to a multiple of `batch_size` (see [`ColumnPages::cached`]) |
    ///
    /// If more than one reader reads the column, the smallest row applies.
    fn release_passed_pages(&mut self, buffers: &mut PushBuffers) {
        let batch_size = self.config.batch_size;
        let Mode::Filtered(state) = &mut self.mode else {
            unreachable!("filtered mode")
        };
        let predicate_row = state.filter_frontier;
        let output_row = match &state.stage {
            Stage::Output { out } => out.first().map(|r| r.start),
            _ => None,
        }
        .or_else(|| state.ready.first().map(|r| r.start))
        .unwrap_or(predicate_row)
        .min(predicate_row);
        let cached_output_row = output_row - output_row % batch_size;

        let mut released = vec![];
        for column in &mut state.columns {
            let mut row = usize::MAX;
            if column.predicate {
                row = row.min(predicate_row);
            }
            if column.output {
                row = row.min(if column.cached {
                    cached_output_row
                } else {
                    output_row
                });
            }
            while let Some((range, end_row)) = column.pages.get(column.next) {
                if *end_row > row {
                    break;
                }
                self.store.remove(range.start);
                released.push(range.clone());
                column.next += 1;
            }
        }
        buffers.release_ranges(&released);
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
