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

mod data;
mod filter;
mod stages;

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReaderBuilder, CacheOptions, RowGroupCache};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::{LoadedRowRanges, RowSelectionStrategy};
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, PredicateOptions, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy,
};
use crate::arrow::in_memory_row_group::{ColumnChunkData, InMemoryRowGroup};
use crate::arrow::push_decoder::RequestGranularity;
use crate::arrow::push_decoder::reader_builder::data::DataRequestBuilder;
use crate::arrow::push_decoder::reader_builder::filter::CacheInfo;
use crate::arrow::push_decoder::scan_plan::{
    BudgetedReadPlan, RowBudget, RowGroupFrontier, ScanPlanBuilder,
};
use crate::arrow::schema::ParquetField;
use crate::errors::ParquetError;
use crate::file::metadata::page_index::RowGroupPageIndex;
use crate::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use crate::util::push_buffers::{PushBuffers, SharedPushBuffers};
use arrow_array::RecordBatch;
use bytes::Bytes;
use data::DataRequest;
use filter::AdvanceResult;
use filter::FilterInfo;
pub(crate) use stages::{Stage, StageSchedule};
use std::ops::Range;
use std::sync::{Arc, RwLock};

/// The current row group being read, its read plan, and its offset/limit budget.
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
    budget: RowBudget,
}

/// This is the inner state machine for reading a single row group.
#[derive(Debug)]
enum RowGroupDecoderState {
    Start {
        row_group_info: RowGroupInfo,
    },
    /// Planning filters, but haven't yet requested data to evaluate them
    Filters {
        row_group_info: RowGroupInfo,
        /// Any previously read column chunk data from prior filters
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
        filter_info: FilterInfo,
    },
    /// Needs data to evaluate current filter
    WaitingOnFilterData {
        row_group_info: RowGroupInfo,
        filter_info: FilterInfo,
        data_request: DataRequest,
    },
    /// Know what data to actually read, after all predicates
    StartData {
        row_group_info: RowGroupInfo,
        /// Any previously read column chunk data from the filtering phase
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
        /// Any cached filter results
        cache_info: Option<CacheInfo>,
    },
    /// Needs data to proceed with reading the output
    ///
    /// With [`RequestGranularity::Batch`], `data_request` holds only the pages
    /// of the next output batch, and `batches` holds the output reader. After
    /// each batch, the state comes back here for the pages of the next batch.
    WaitingOnData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        /// Any cached filter results
        cache_info: Option<CacheInfo>,
        /// `Some` with [`RequestGranularity::Batch`]
        batches: Option<Box<BatchOutput>>,
    },
    /// Finished (or not yet started) reading this group
    Finished,
}

impl RowGroupDecoderState {
    /// The index of the row group, if one is active.
    fn row_group_idx(&self) -> Option<usize> {
        match self {
            Self::Start { row_group_info }
            | Self::Filters { row_group_info, .. }
            | Self::WaitingOnFilterData { row_group_info, .. }
            | Self::StartData { row_group_info, .. }
            | Self::WaitingOnData { row_group_info, .. } => Some(row_group_info.row_group_idx),
            Self::Finished => None,
        }
    }
}

/// The output of a row group that is decoded one batch at a time, see
/// [`RowGroupDecoderState::WaitingOnData`].
struct BatchOutput {
    /// The output reader, built when the pages of the first batch are pushed.
    /// It reads the column chunks that the predicates did not read from the
    /// shared [`PushBuffers`], so it sees the pages pushed later.
    reader: Option<ParquetRecordBatchReader>,
    /// The plan of the reader, until the reader is built.
    plan_builder: Option<ReadPlanBuilder>,
    /// The column chunks that the predicates read.
    column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
    /// The selected rows that the reader did not return yet, sorted.
    remaining: Vec<Range<usize>>,
}

impl std::fmt::Debug for BatchOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchOutput")
            .field("has_reader", &self.reader.is_some())
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// The result of [`RowGroupReaderBuilder::try_build_incremental`].
#[derive(Debug)]
pub(crate) enum IncrementalResult {
    /// The bytes that the next batch needs.
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

/// The byte range of a column chunk in the file.
pub(crate) fn column_chunk_range(column: &ColumnChunkMetaData) -> Range<u64> {
    let (start, length) = column.byte_range();
    start..start + length
}

#[derive(Debug)]
pub(crate) enum RowGroupBuildResult {
    /// The active row group is complete without producing a reader.
    Finished {
        /// Budget remaining after applying this row group's selection.
        remaining_budget: RowBudget,
    },
    /// More bytes are needed before the active row group can make progress.
    NeedsData(Vec<Range<u64>>),
    /// The next batch of a row group decoded with [`RequestGranularity::Batch`].
    /// `remaining_budget` is `Some` if this batch finishes the row group.
    Batch {
        batch: RecordBatch,
        remaining_budget: Option<RowBudget>,
    },
    /// The active row group produced a reader.
    Data {
        batch_reader: ParquetRecordBatchReader,
        /// Budget remaining after applying this row group's selection.
        remaining_budget: RowBudget,
    },
}

/// Result of a state transition
#[derive(Debug)]
struct NextState {
    next_state: RowGroupDecoderState,
    /// result to return, if any
    ///
    /// * `Some`: the processing should stop and return the result
    /// * `None`: processing should continue
    result: Option<RowGroupBuildResult>,
}

impl NextState {
    /// The next state with no result.
    ///
    /// This indicates processing should continue
    fn again(next_state: RowGroupDecoderState) -> Self {
        Self {
            next_state,
            result: None,
        }
    }

    /// Create a NextState with a result that should be returned
    fn result(next_state: RowGroupDecoderState, result: RowGroupBuildResult) -> Self {
        Self {
            next_state,
            result: Some(result),
        }
    }
}

/// Builder for [`ParquetRecordBatchReader`] for a single row group
///
/// This struct drives the main state machine for decoding each row group -- it
/// determines what data is needed, and then assembles the
/// `ParquetRecordBatchReader` when all data is available.
#[derive(Debug)]
pub(crate) struct RowGroupReaderBuilder {
    /// The output batch size
    batch_size: usize,

    /// What columns to project (produce in each output batch)
    projection: ProjectionMask,

    /// The Parquet file metadata
    metadata: Arc<ParquetMetaData>,

    /// Top level parquet schema and arrow schema mapping
    fields: Option<Arc<ParquetField>>,

    /// Optional filter
    filter: Option<RowFilter>,

    /// The size in bytes of the predicate cache to use
    ///
    /// See [`RowGroupCache`] for details.
    max_predicate_cache_size: usize,

    /// The metrics collector
    metrics: ArrowReaderMetrics,

    /// Strategy for materialising row selections
    row_selection_policy: RowSelectionPolicy,

    /// Current state of the decoder.
    ///
    /// It is taken when processing, and must be put back before returning
    /// it is a bug error if it is not put back after transitioning states.
    state: Option<RowGroupDecoderState>,

    /// The underlying data store. Shared with the output reader of a row
    /// group that is decoded with [`RequestGranularity::Batch`].
    buffers: SharedPushBuffers,

    /// What each decoding stage fetches. Kept here because the filter is
    /// moved out of the builder while a row group is decoded.
    stages: Arc<StageSchedule>,
    /// How [`Self::try_build_incremental`] fetches and decodes row groups.
    request_granularity: RequestGranularity,
    /// `try_build_incremental` started the active row group.
    batch_mode_active: bool,
}

/// The parts of a [`RowGroupReaderBuilder`] needed to rebuild it, recovered by
/// [`RowGroupReaderBuilder::into_parts`].
///
/// `metadata` is not included: it is a whole-file property carried alongside
/// `schema` in `RemainingRowGroupsParts`.
#[derive(Debug)]
pub(crate) struct RowGroupReaderBuilderParts {
    pub batch_size: usize,
    pub projection: ProjectionMask,
    pub fields: Option<Arc<ParquetField>>,
    pub filter: Option<RowFilter>,
    pub max_predicate_cache_size: usize,
    pub metrics: ArrowReaderMetrics,
    pub row_selection_policy: RowSelectionPolicy,
    /// Bytes already pushed into the decoder, carried across a rebuild so they
    /// are not re-requested.
    pub buffers: PushBuffers,
    pub request_granularity: RequestGranularity,
}

impl RowGroupReaderBuilder {
    /// Create a new RowGroupReaderBuilder
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        batch_size: usize,
        projection: ProjectionMask,
        metadata: Arc<ParquetMetaData>,
        fields: Option<Arc<ParquetField>>,
        filter: Option<RowFilter>,
        metrics: ArrowReaderMetrics,
        max_predicate_cache_size: usize,
        buffers: PushBuffers,
        row_selection_policy: RowSelectionPolicy,
        request_granularity: RequestGranularity,
    ) -> Self {
        let (predicate_projections, cache_projection) = match &filter {
            Some(filter) => (
                filter
                    .predicates
                    .iter()
                    .map(|predicate| predicate.projection().clone())
                    .collect(),
                Self::compute_cache_projection_inner(
                    filter,
                    &projection,
                    &metadata,
                    max_predicate_cache_size,
                ),
            ),
            None => (vec![], None),
        };
        let stages =
            StageSchedule::new(projection.clone(), predicate_projections, cache_projection);
        Self {
            batch_size,
            projection,
            metadata,
            fields,
            filter,
            metrics,
            max_predicate_cache_size,
            row_selection_policy,
            state: Some(RowGroupDecoderState::Finished),
            buffers: Arc::new(std::sync::Mutex::new(buffers)),
            stages: Arc::new(stages),
            request_granularity,
            batch_mode_active: false,
        }
    }

    /// Decompose into [`RowGroupReaderBuilderParts`] so the builder can be
    /// reconstructed. The runtime decode `state` is discarded; `metadata` is
    /// recovered from the frontier instead (see `RemainingRowGroups::into_parts`).
    pub(crate) fn into_parts(self) -> RowGroupReaderBuilderParts {
        // If a new field is added to `RowGroupReaderBuilder`, it must be added here and in `RowGroupReaderBuilderParts`,
        // or at least evaluate how it should be handled in the decomposition and reconstruction of the builder.
        let Self {
            batch_size,
            projection,
            metadata: _,
            fields,
            filter,
            max_predicate_cache_size,
            metrics,
            row_selection_policy,
            state: _,
            buffers,
            // Recomputed from `filter` when the builder is rebuilt.
            stages: _,
            request_granularity,
            batch_mode_active: _,
        } = self;
        // A reader of the discarded state can hold a reference.
        let buffers = std::mem::take(&mut *buffers.lock().unwrap());
        RowGroupReaderBuilderParts {
            batch_size,
            projection,
            fields,
            filter,
            max_predicate_cache_size,
            metrics,
            row_selection_policy,
            buffers,
            request_granularity,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    // `&mut`: the method changes the shared buffers.
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub fn push_data(
        &mut self,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        self.buffers.lock().unwrap().push_ranges(ranges, buffers)
    }

    /// True iff the inner state is `Finished`. This is the only state in
    /// which it is safe to decompose the builder via [`Self::into_parts`],
    /// because no `RowGroupInfo`, `FilterInfo`, or in-flight `DataRequest`
    /// is referencing the row-group-scoped decode state.
    pub(crate) fn is_finished(&self) -> bool {
        matches!(self.state, Some(RowGroupDecoderState::Finished))
    }

    /// Returns the total number of buffered bytes available
    pub fn buffered_bytes(&self) -> u64 {
        self.buffers.lock().unwrap().buffered_bytes()
    }

    /// How [`ParquetPushDecoder::try_decode`] fetches and decodes row groups.
    ///
    /// [`ParquetPushDecoder::try_decode`]: crate::arrow::push_decoder::ParquetPushDecoder::try_decode
    pub(crate) fn request_granularity(&self) -> RequestGranularity {
        self.request_granularity
    }

    /// Returns true if `try_decode` decodes the active row group one batch at
    /// a time. While this is true, `try_next_reader` returns an error.
    pub(crate) fn is_incremental(&self) -> bool {
        self.batch_mode_active && self.has_active_row_group()
    }

    /// Decode the active row group one batch at a time. See
    /// [`RequestGranularity::Batch`]. This runs the same state machine as
    /// [`Self::try_build`].
    pub(crate) fn try_build_incremental(&mut self) -> Result<IncrementalResult, ParquetError> {
        if !self.batch_mode_active {
            if !matches!(self.state, Some(RowGroupDecoderState::Start { .. })) {
                return Err(general_err!(
                    "Internal Error: try_build_incremental called for a row group of try_build"
                ));
            }
            self.batch_mode_active = true;
        }
        let result = self.try_build();
        if !self.has_active_row_group() || result.is_err() {
            self.batch_mode_active = false;
        }
        Ok(match result? {
            RowGroupBuildResult::NeedsData(ranges) => IncrementalResult::NeedsData(ranges),
            RowGroupBuildResult::Finished { remaining_budget } => {
                IncrementalResult::Finished { remaining_budget }
            }
            RowGroupBuildResult::Batch {
                batch,
                remaining_budget,
            } => IncrementalResult::Batch {
                batch,
                remaining_budget,
            },
            RowGroupBuildResult::Data { .. } => {
                return Err(general_err!(
                    "Internal Error: reader built with RequestGranularity::Batch"
                ));
            }
        })
    }

    /// Clear any staged ranges currently buffered for future decode work.
    // `&mut`: the method changes the shared buffers.
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub fn clear_all_ranges(&mut self) {
        self.buffers.lock().unwrap().clear_all_ranges();
    }

    /// take the current state, leaving None in its place.
    ///
    /// Returns an error if there the state wasn't put back after the previous
    /// call to [`Self::take_state`].
    ///
    /// Any code that calls this method must ensure that the state is put back
    /// before returning, otherwise the reader will error next time it is called
    fn take_state(&mut self) -> Result<RowGroupDecoderState, ParquetError> {
        self.state.take().ok_or_else(|| {
            ParquetError::General(String::from(
                "Internal Error: RowGroupReader in invalid state",
            ))
        })
    }

    /// Returns true if this builder is currently decoding a row group.
    pub(crate) fn has_active_row_group(&self) -> bool {
        !matches!(self.state, Some(RowGroupDecoderState::Finished))
    }

    /// Setup this reader to read the next row group
    pub(crate) fn next_row_group(
        &mut self,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        budget: RowBudget,
    ) -> Result<(), ParquetError> {
        let state = self.take_state()?;
        if !matches!(state, RowGroupDecoderState::Finished) {
            return Err(ParquetError::General(format!(
                "Internal Error: next_row_group called while still reading a row group. Expected Finished state, got {state:?}"
            )));
        }
        let plan_builder = ReadPlanBuilder::new(self.batch_size)
            .with_selection(selection)
            .with_row_selection_policy(self.row_selection_policy);

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            budget,
        };

        self.state = Some(RowGroupDecoderState::Start { row_group_info });
        Ok(())
    }

    /// Try to build the next `ParquetRecordBatchReader` for the active row group.
    ///
    /// Returns [`RowGroupBuildResult::NeedsData`] if more data is needed,
    /// [`RowGroupBuildResult::Data`] if a reader is ready, or
    /// [`RowGroupBuildResult::Finished`] if the row group completed without
    /// producing a reader.
    pub(crate) fn try_build(&mut self) -> Result<RowGroupBuildResult, ParquetError> {
        loop {
            let current_state = self.take_state()?;
            // Try to transition the decoder.
            match self.try_transition(current_state)? {
                // Either produced a batch reader, needed input, or finished
                NextState {
                    next_state,
                    result: Some(result),
                } => {
                    // put back the next state
                    self.state = Some(next_state);
                    return Ok(result);
                }
                // completed one internal state, maybe can proceed further
                NextState {
                    next_state,
                    result: None,
                } => {
                    // continue processing
                    self.state = Some(next_state);
                }
            }
        }
    }

    /// Current state --> next state + optional output
    ///
    /// This is the main state transition function for the row group reader
    /// and encodes the row group decoding state machine.
    ///
    /// # Notes
    ///
    /// This structure is used to reduce the indentation level of the main loop
    /// in try_build
    fn try_transition(
        &mut self,
        current_state: RowGroupDecoderState,
    ) -> Result<NextState, ParquetError> {
        let result = match current_state {
            RowGroupDecoderState::Start { row_group_info } => {
                debug_assert!(
                    !row_group_info.budget.is_exhausted(),
                    "RowGroupFrontier should not hand off row groups after the output limit is exhausted"
                );

                let column_chunks = None; // no prior column chunks

                let Some(filter) = self.filter.take() else {
                    // no filter, start trying to read data immediately
                    return Ok(NextState::again(RowGroupDecoderState::StartData {
                        row_group_info,
                        column_chunks,
                        cache_info: None,
                    }));
                };
                // no predicates in filter, so start reading immediately
                if filter.predicates.is_empty() {
                    return Ok(NextState::again(RowGroupDecoderState::StartData {
                        row_group_info,
                        column_chunks,
                        cache_info: None,
                    }));
                }

                // we have predicates to evaluate
                let cache_projection =
                    self.compute_cache_projection(row_group_info.row_group_idx, &filter);

                let cache_info = CacheInfo::new(
                    cache_projection,
                    Arc::new(RwLock::new(RowGroupCache::new(
                        self.batch_size,
                        self.max_predicate_cache_size,
                    ))),
                );

                let filter_info = FilterInfo::new(filter, cache_info);
                NextState::again(RowGroupDecoderState::Filters {
                    row_group_info,
                    filter_info,
                    column_chunks,
                })
            }
            // need to evaluate filters
            RowGroupDecoderState::Filters {
                row_group_info,
                column_chunks,
                filter_info,
            } => {
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget,
                } = row_group_info;

                // If nothing is selected, we are done with this row group
                if !plan_builder.selects_any() {
                    // ruled out entire row group
                    self.filter = Some(filter_info.into_filter());
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        RowGroupBuildResult::Finished {
                            remaining_budget: budget,
                        },
                    ));
                }

                // Make a request for the data needed to evaluate the current predicate
                let fetch = self.stages.fetch(Stage::Predicate(filter_info.index()));

                // need to fetch pages the column needs for decoding, figure
                // that out based on the current selection and projection
                let data_request = DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    fetch.projection, // use the predicate's projection
                )
                .with_selection(plan_builder.selection())
                // Cached output columns reuse these predicate-stage chunks. Expand their
                // selection to cache batch boundaries so a cache miss can safely fetch a
                // complete batch from the retained sparse column data.
                .with_cache_projection(fetch.cache_projection)
                .with_column_chunks(column_chunks)
                .build();

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget,
                };

                NextState::again(RowGroupDecoderState::WaitingOnFilterData {
                    row_group_info,
                    filter_info,
                    data_request,
                })
            }
            RowGroupDecoderState::WaitingOnFilterData {
                row_group_info,
                data_request,
                mut filter_info,
            } => {
                // figure out what ranges we still need
                let needed_ranges = data_request.needed_ranges(&self.buffers.lock().unwrap());
                if !needed_ranges.is_empty() {
                    // still need data
                    return Ok(NextState::result(
                        RowGroupDecoderState::WaitingOnFilterData {
                            row_group_info,
                            filter_info,
                            data_request,
                        },
                        RowGroupBuildResult::NeedsData(needed_ranges),
                    ));
                }

                // otherwise we have all the data we need to evaluate the predicate
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    mut plan_builder,
                    budget,
                } = row_group_info;

                let predicate = filter_info.current();

                let row_group = data_request.try_into_in_memory_row_group(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    predicate.projection(),
                    &mut self.buffers.lock().unwrap(),
                )?;

                let cache_options = filter_info.cache_builder().producer();

                let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .with_batch_size(self.batch_size)
                    .with_cache_options(Some(&cache_options))
                    .with_parquet_metadata(&self.metadata)
                    .build_array_reader(self.fields.as_deref(), predicate.projection())?;

                // Auto resolution and loaded ranges are projection-specific, so restore the
                // configured policy before preparing each predicate.
                plan_builder = plan_builder.with_row_selection_policy(self.row_selection_policy);

                // Prepare selection execution for pages pruned during fetch.
                plan_builder = prepare_selection_for_page_skipping(
                    plan_builder,
                    predicate.projection(),
                    self.row_group_offset_index(row_group_idx),
                    self.metadata.file_metadata().schema_descr().num_columns(),
                    row_count,
                );

                // When this is the final predicate in the chain and an output
                // limit is set, tell the filter evaluation to stop once enough
                // matching rows have been accumulated.
                let predicate_limit = filter_info
                    .is_last()
                    .then(|| budget.selected_row_limit())
                    .flatten();

                // Evaluate the filter via `with_predicate_options`, opting into
                // early termination when this is the final predicate and an
                // output limit was set.
                let mut predicate_options =
                    PredicateOptions::new(array_reader, filter_info.current_mut());
                if let Some(limit) = predicate_limit {
                    predicate_options = predicate_options.with_limit(limit, row_count);
                }
                plan_builder = plan_builder.with_predicate_options(predicate_options)?;

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget,
                };

                // Take back the column chunks that were read
                let column_chunks = Some(row_group.column_chunks);

                // advance to the next predicate, if any
                match filter_info.advance() {
                    AdvanceResult::Continue(filter_info) => {
                        NextState::again(RowGroupDecoderState::Filters {
                            row_group_info,
                            column_chunks,
                            filter_info,
                        })
                    }
                    // done with predicates, proceed to reading data
                    AdvanceResult::Done(filter, cache_info) => {
                        // remember we need to put back the filter
                        assert!(self.filter.is_none());
                        self.filter = Some(filter);
                        NextState::again(RowGroupDecoderState::StartData {
                            row_group_info,
                            column_chunks,
                            cache_info: Some(cache_info),
                        })
                    }
                }
            }
            RowGroupDecoderState::StartData {
                row_group_info,
                column_chunks,
                cache_info,
            } => {
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget,
                } = row_group_info;

                let BudgetedReadPlan {
                    mut plan_builder,
                    rows_before_budget,
                    rows_after_budget,
                    remaining_budget,
                } = budget.apply_to_plan(plan_builder, row_count);

                if rows_before_budget == 0 {
                    // ruled out entire row group
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        RowGroupBuildResult::Finished { remaining_budget },
                    ));
                }

                if rows_after_budget == 0 {
                    // no rows left after applying limit/offset
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        RowGroupBuildResult::Finished { remaining_budget },
                    ));
                }

                let fetch = self.stages.fetch(Stage::Projection);
                if self.batch_mode_active {
                    plan_builder =
                        plan_builder.with_row_selection_policy(self.row_selection_policy);
                    plan_builder = prepare_selection_for_page_skipping(
                        plan_builder,
                        &self.projection,
                        self.row_group_offset_index(row_group_idx),
                        self.metadata.file_metadata().schema_descr().num_columns(),
                        row_count,
                    );
                    let remaining = match plan_builder.selection() {
                        Some(selection) => selection_to_ranges(selection, 0),
                        None => std::iter::once(0..row_count).collect(),
                    };
                    let batches = Box::new(BatchOutput {
                        reader: None,
                        plan_builder: Some(plan_builder),
                        column_chunks,
                        remaining,
                    });
                    let data_request = self.next_batch_request(row_group_idx, row_count, &batches);
                    let row_group_info = RowGroupInfo {
                        row_group_idx,
                        row_count,
                        // The plan is in `batches`
                        plan_builder: ReadPlanBuilder::new(self.batch_size),
                        budget: remaining_budget,
                    };
                    return Ok(NextState::again(RowGroupDecoderState::WaitingOnData {
                        row_group_info,
                        data_request,
                        cache_info,
                        batches: Some(batches),
                    }));
                }
                let data_request = DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    fetch.projection,
                )
                .with_selection(plan_builder.selection())
                .with_column_chunks(column_chunks)
                .with_cache_projection(fetch.cache_projection)
                .build();

                plan_builder = plan_builder.with_row_selection_policy(self.row_selection_policy);

                plan_builder = prepare_selection_for_page_skipping(
                    plan_builder,
                    &self.projection,
                    self.row_group_offset_index(row_group_idx),
                    self.metadata.file_metadata().schema_descr().num_columns(),
                    row_count,
                );

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget: remaining_budget,
                };

                NextState::again(RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                    cache_info,
                    batches: None,
                })
            }
            // Waiting on the pages of the next batch
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
                batches: Some(batches),
            } => self.try_next_output_batch(row_group_info, data_request, cache_info, batches)?,
            // Waiting on data to proceed with reading the output
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
                batches: None,
            } => {
                let needed_ranges = data_request.needed_ranges(&self.buffers.lock().unwrap());
                if !needed_ranges.is_empty() {
                    // still need data
                    return Ok(NextState::result(
                        RowGroupDecoderState::WaitingOnData {
                            row_group_info,
                            data_request,
                            cache_info,
                            batches: None,
                        },
                        RowGroupBuildResult::NeedsData(needed_ranges),
                    ));
                }
                // otherwise we have all the data we need to proceed
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    budget,
                } = row_group_info;

                let row_group = data_request.try_into_in_memory_row_group(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    &self.projection,
                    &mut self.buffers.lock().unwrap(),
                )?;

                let plan = plan_builder.build();

                // if we have any cached results, connect them up
                let array_reader_builder = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .with_batch_size(self.batch_size)
                    .with_parquet_metadata(&self.metadata);
                let array_reader = if let Some(cache_info) = cache_info.as_ref() {
                    let cache_options: CacheOptions = cache_info.builder().consumer();
                    array_reader_builder
                        .with_cache_options(Some(&cache_options))
                        .build_array_reader(self.fields.as_deref(), &self.projection)
                } else {
                    array_reader_builder
                        .build_array_reader(self.fields.as_deref(), &self.projection)
                }?;

                let reader = ParquetRecordBatchReader::new(array_reader, plan);
                NextState::result(
                    RowGroupDecoderState::Finished,
                    RowGroupBuildResult::Data {
                        batch_reader: reader,
                        remaining_budget: budget,
                    },
                )
            }
            RowGroupDecoderState::Finished => {
                return Err(ParquetError::General(String::from(
                    "Internal Error: try_build called without an active row group",
                )));
            }
        };
        Ok(result)
    }

    /// The pages that the next output batch of `batches` reads and that the
    /// predicates did not read.
    fn next_batch_request(
        &self,
        row_group_idx: usize,
        row_count: usize,
        batches: &BatchOutput,
    ) -> DataRequest {
        let rows = take_rows(&mut batches.remaining.clone(), self.batch_size);
        let selection = ranges_to_selection(&rows, row_count);
        DataRequestBuilder::new(
            row_group_idx,
            row_count,
            self.batch_size,
            &self.metadata,
            &self.projection,
        )
        .with_selection(Some(&selection))
        .with_column_chunks(batches.column_chunks.clone())
        .build()
    }

    /// Decode the next output batch of a row group decoded with
    /// [`RequestGranularity::Batch`], or return the pages that it needs.
    // `&mut`: the method changes the shared buffers.
    #[expect(clippy::needless_pass_by_ref_mut)]
    fn try_next_output_batch(
        &mut self,
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        cache_info: Option<CacheInfo>,
        mut batches: Box<BatchOutput>,
    ) -> Result<NextState, ParquetError> {
        let needed_ranges = data_request.needed_ranges(&self.buffers.lock().unwrap());
        if !needed_ranges.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                    cache_info,
                    batches: Some(batches),
                },
                RowGroupBuildResult::NeedsData(needed_ranges),
            ));
        }
        let RowGroupInfo {
            row_group_idx,
            row_count,
            ..
        } = row_group_info;

        if batches.reader.is_none() {
            let reader = self.build_batch_reader(
                row_group_idx,
                row_count,
                &mut batches,
                cache_info.as_ref(),
            )?;
            batches.reader = Some(reader);
        }
        take_rows(&mut batches.remaining, self.batch_size);
        let batch = match batches.reader.as_mut().and_then(Iterator::next) {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(ParquetError::ArrowError(e.to_string())),
            None => {
                return Err(general_err!(
                    "Internal Error: the output reader ended before the selected rows"
                ));
            }
        };

        if batches.remaining.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::Finished,
                RowGroupBuildResult::Batch {
                    batch,
                    remaining_budget: Some(row_group_info.budget),
                },
            ));
        }
        let data_request = self.next_batch_request(row_group_idx, row_count, &batches);
        Ok(NextState::result(
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
                batches: Some(batches),
            },
            RowGroupBuildResult::Batch {
                batch,
                remaining_budget: None,
            },
        ))
    }

    /// Build the output reader of `batches`. The column chunks that the
    /// predicates did not read are [`ColumnChunkData::Shared`] views of the
    /// shared [`PushBuffers`].
    fn build_batch_reader(
        &self,
        row_group_idx: usize,
        row_count: usize,
        batches: &mut BatchOutput,
        cache_info: Option<&CacheInfo>,
    ) -> Result<ParquetRecordBatchReader, ParquetError> {
        let metadata = self.metadata.row_group(row_group_idx);
        let mut column_chunks = batches
            .column_chunks
            .clone()
            .unwrap_or_else(|| vec![None; metadata.columns().len()]);
        for (idx, chunk) in column_chunks.iter_mut().enumerate() {
            if chunk.is_none() && self.projection.leaf_included(idx) {
                *chunk = Some(Arc::new(ColumnChunkData::Shared {
                    length: metadata.column(idx).byte_range().1 as usize,
                    buffers: Arc::clone(&self.buffers),
                }));
            }
        }
        let row_group = InMemoryRowGroup {
            page_index: self.row_group_offset_index(row_group_idx),
            column_chunks,
            row_count,
            row_group_idx,
            metadata: &self.metadata,
        };
        let array_reader_builder = ArrayReaderBuilder::new(&row_group, &self.metrics)
            .with_batch_size(self.batch_size)
            .with_parquet_metadata(&self.metadata);
        let cache_options = cache_info.map(|cache_info| cache_info.builder().consumer());
        let array_reader = array_reader_builder
            .with_cache_options(cache_options.as_ref())
            .build_array_reader(self.fields.as_deref(), &self.projection)?;
        let plan = batches
            .plan_builder
            .take()
            .ok_or_else(|| general_err!("Internal Error: output plan missing"))?
            .build();
        Ok(ParquetRecordBatchReader::new(array_reader, plan))
    }

    /// The index of the active row group, if any.
    pub(crate) fn active_row_group_idx(&self) -> Option<usize> {
        self.state
            .as_ref()
            .and_then(RowGroupDecoderState::row_group_idx)
    }

    /// Remove the buffered bytes of all column chunks of a row group. This
    /// includes bytes that the caller pushed but the decoder did not request,
    /// for example the bytes between the requested ranges of a larger pushed
    /// buffer.
    // `&mut`: the method changes the shared buffers.
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) fn release_row_group(&mut self, row_group_idx: usize) {
        let ranges: Vec<Range<u64>> = self
            .metadata
            .row_group(row_group_idx)
            .columns()
            .iter()
            .map(column_chunk_range)
            .collect();
        self.buffers.lock().unwrap().release_ranges(&ranges);
    }

    /// Remove the buffered bytes outside the read column chunks of
    /// `row_groups`. A column chunk is read if the output or a predicate
    /// reads its column. Indexes that are not in the file are ignored.
    // `&mut`: the method changes the shared buffers.
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) fn release_unread_bytes(&mut self, row_groups: impl IntoIterator<Item = usize>) {
        if self.buffers.lock().unwrap().buffered_bytes() == 0 {
            return;
        }
        let mut read_columns = self.projection.clone();
        if let Some(filter) = &self.filter {
            for predicate in &filter.predicates {
                read_columns.union(predicate.projection());
            }
        }
        let mut keep = vec![];
        for row_group_idx in row_groups {
            let Some(row_group) = self.metadata.row_groups().get(row_group_idx) else {
                continue;
            };
            keep.extend(
                row_group
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(column_idx, _)| read_columns.leaf_included(*column_idx))
                    .map(|(_, column)| column_chunk_range(column)),
            );
        }
        self.buffers.lock().unwrap().retain_ranges(&keep);
    }

    /// A [`ScanPlanBuilder`] that plans the same ranges as this builder, for
    /// the row groups in `frontier`.
    pub(crate) fn scan_plan_builder(&self, frontier: RowGroupFrontier) -> ScanPlanBuilder {
        ScanPlanBuilder::new(frontier, self.batch_size, Arc::clone(&self.stages))
    }

    /// Which columns should be cached?
    ///
    /// Returns the columns that are used by the filters *and* then used in the
    /// final projection, excluding any nested columns.
    fn compute_cache_projection(&self, row_group_idx: usize, filter: &RowFilter) -> ProjectionMask {
        let meta = self.metadata.row_group(row_group_idx);
        let cache_projection = Self::compute_cache_projection_inner(
            filter,
            &self.projection,
            &self.metadata,
            self.max_predicate_cache_size,
        );
        match cache_projection {
            Some(projection) => projection,
            None => ProjectionMask::none(meta.columns().len()),
        }
    }

    /// An associated function, so that [`Self::new`] can call it before the
    /// builder exists.
    fn compute_cache_projection_inner(
        filter: &RowFilter,
        projection: &ProjectionMask,
        metadata: &ParquetMetaData,
        max_predicate_cache_size: usize,
    ) -> Option<ProjectionMask> {
        // Do not compute the projection mask if the predicate cache is disabled
        if max_predicate_cache_size == 0 {
            return None;
        }
        let mut cache_projection = filter.predicates.first()?.projection().clone();
        for predicate in &filter.predicates {
            cache_projection.union(predicate.projection());
        }
        cache_projection.intersect(projection);
        // Exclude leaves belonging to roots that span multiple parquet leaves (i.e. nested columns)
        cache_projection.without_nested_types(metadata.file_metadata().schema_descr())
    }

    /// Get the offset index for the specified row group, if any
    fn row_group_offset_index(&self, row_group_idx: usize) -> Option<RowGroupPageIndex> {
        if self
            .metadata
            .page_index()
            .is_some_and(|pi| pi.has_offset_indexes())
        {
            Some(self.metadata.page_index_for_row_group(row_group_idx))
        } else {
            None
        }
    }
}

/// Prepare row selection execution when page pruning produced sparse column data.
///
/// Some pages can be skipped during row-group construction if they are not read
/// by the selections. This means that the data pages for those rows are never
/// loaded and definition/repetition levels are never read. When using
/// `RowSelections` selection works because `skip_records()` handles this
/// case and skips the page accordingly.
///
/// However, with the current mask design, all values covered by a mask chunk
/// are decoded before the mask filter is applied. Thus a chunk cannot cross a
/// page that was skipped during row-group construction.
///
/// A simple example:
/// * the page size is 2, the mask is 100001, row selection should be read(1) skip(4) read(1)
/// * the `ColumnChunkData` would be page1(10), page2(skipped), page3(01)
///
/// Mask execution records the row ranges loaded for every projected column, so
/// each mask chunk stays within loaded data and `skip_records()` crosses the
/// gaps. This applies both to an explicit mask policy and to Auto when it
/// resolves to mask execution.
fn prepare_selection_for_page_skipping(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    page_index: Option<RowGroupPageIndex>,
    num_columns: usize,
    total_rows: usize,
) -> ReadPlanBuilder {
    // With no selection there are no skipped pages and no execution strategy
    // to prepare. Preserve Auto so a first predicate can choose its backing
    // while constructing the resulting selection.
    if plan_builder.selection().is_none() {
        return plan_builder;
    }

    match plan_builder.resolve_selection_strategy() {
        RowSelectionStrategy::Mask => {
            let loaded = loaded_row_ranges_for_projection(
                plan_builder.selection(),
                projection_mask,
                page_index,
                num_columns,
                total_rows,
            );
            plan_builder
                .with_row_selection_policy(RowSelectionPolicy::Mask)
                .with_loaded_row_ranges(loaded)
        }
        RowSelectionStrategy::Selectors => {
            plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
        }
    }
}

/// Computes row ranges for which every projected column has page data loaded.
fn loaded_row_ranges_for_projection(
    selection: Option<&RowSelection>,
    projection_mask: &ProjectionMask,
    page_index: Option<RowGroupPageIndex>,
    num_columns: usize,
    total_rows: usize,
) -> Option<LoadedRowRanges> {
    let selection = selection?;
    let page_index = page_index?;

    (0..num_columns)
        .into_iter()
        .filter_map(|leaf_idx| {
            let column_metadata = page_index.offset_index(leaf_idx)?;
            let pages = column_metadata.page_locations();
            (projection_mask.leaf_included(leaf_idx) && !pages.is_empty()).then(|| {
                RowSelection::from_consecutive_ranges(
                    selection
                        .row_ranges_for_selected_pages(pages, total_rows)
                        .into_iter(),
                    total_rows,
                )
            })
        })
        .reduce(|loaded, column| loaded.intersection(&column))
        .filter(|loaded| loaded.skipped_row_count() != 0)
        .map(LoadedRowRanges::from_selection)
}

/// Row ranges to a [`RowSelection`] of `row_count` rows.
fn ranges_to_selection(ranges: &[Range<usize>], row_count: usize) -> RowSelection {
    RowSelection::from_consecutive_ranges(ranges.iter().cloned(), row_count)
}

/// The selected rows of `selection`, as sorted row ranges.
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
    use crate::arrow::array_reader::StructArrayReader;
    use crate::arrow::array_reader::test_util::make_int32_page_reader;
    use crate::arrow::arrow_reader::ArrowPredicateFn;
    use crate::arrow::arrow_reader::{RowSelection, RowSelector};
    use crate::file::metadata::page_index::{PageIndexBuilder, PageIndexProvider};
    use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
    use arrow_array::BooleanArray;
    use arrow_schema::{DataType as ArrowType, Field, Fields};

    #[test]
    // Verify that the size of RowGroupDecoderState does not grow too large
    fn test_structure_size() {
        assert_eq!(std::mem::size_of::<RowGroupDecoderState>(), 240);
    }

    #[test]
    fn test_loaded_row_ranges_intersect_column_page_boundaries() {
        let mut page_index = PageIndexBuilder::new(1, 2);
        let column = |first_rows: &[i64]| OffsetIndexMetaData {
            page_locations: first_rows
                .iter()
                .enumerate()
                .map(|(idx, first_row_index)| PageLocation {
                    offset: (idx * 10) as i64,
                    compressed_page_size: 10,
                    first_row_index: *first_row_index,
                })
                .collect(),
            unencoded_byte_array_data_bytes: None,
        };
        page_index.put_offset_index(column(&[0, 4, 8]), 0, 0);
        page_index.put_offset_index(column(&[0, 6, 10]), 0, 1);
        let page_index: Option<Arc<dyn PageIndexProvider>> = Some(Arc::new(page_index.build()));
        let page_index = RowGroupPageIndex::new(0, page_index);
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(9),
            RowSelector::select(1),
        ]);

        let loaded = loaded_row_ranges_for_projection(
            Some(&selection),
            &ProjectionMask::all(),
            Some(page_index),
            2,
            12,
        )
        .unwrap();

        assert_eq!(loaded.ranges(), &[0..4, 10..12]);
    }

    #[test]
    fn test_page_skipping_preparation_preserves_first_predicate_auto_mask() {
        let policy = RowSelectionPolicy::Auto { threshold: 4 };
        let plan_builder = ReadPlanBuilder::new(4).with_row_selection_policy(policy);

        let prepared =
            prepare_selection_for_page_skipping(plan_builder, &ProjectionMask::all(), None, 1, 12);
        assert_eq!(prepared.row_selection_policy(), &policy);
        assert!(prepared.selection().is_none());

        let data: Vec<i32> = (0..12).collect();
        let levels = vec![0; data.len()];
        let leaf = make_int32_page_reader(&data, &levels, &levels, 0, 0, None);
        let struct_type = ArrowType::Struct(Fields::from(vec![Field::new(
            "c0",
            ArrowType::Int32,
            false,
        )]));
        let struct_reader = StructArrayReader::new(struct_type, vec![leaf], 0, 0, false, None);
        let mut offset = 0usize;
        let mut predicate = ArrowPredicateFn::new(ProjectionMask::all(), move |batch| {
            let end = offset + batch.num_rows();
            let filter =
                BooleanArray::from((offset..end).map(|row| row % 2 == 0).collect::<Vec<_>>());
            offset = end;
            Ok(filter)
        });

        let prepared = prepared
            .with_predicate_options(PredicateOptions::new(
                Box::new(struct_reader),
                &mut predicate,
            ))
            .unwrap();
        let selection = prepared.selection().expect("first predicate selection");
        let reference = RowSelection::from_filters(&[BooleanArray::from(
            (0..12).map(|row| row % 2 == 0).collect::<Vec<_>>(),
        )]);

        assert_eq!(selection, &reference);
        assert!(selection.as_mask().is_some());
    }

    #[test]
    fn test_auto_keeps_mask_when_page_pruning_skips_pages() {
        let mut page_index = PageIndexBuilder::new(1, 1);
        page_index.put_offset_index(
            OffsetIndexMetaData {
                page_locations: [0, 2, 4, 6, 8, 10]
                    .into_iter()
                    .enumerate()
                    .map(|(idx, first_row_index)| PageLocation {
                        offset: (idx * 10) as i64,
                        compressed_page_size: 10,
                        first_row_index,
                    })
                    .collect(),
                unencoded_byte_array_data_bytes: None,
            },
            0,
            0,
        );
        let page_index: Option<Arc<dyn PageIndexProvider>> = Some(Arc::new(page_index.build()));
        let page_index = RowGroupPageIndex::new(0, page_index);
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(10),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(12)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 32 });

        let prepared = prepare_selection_for_page_skipping(
            plan_builder,
            &ProjectionMask::all(),
            Some(page_index),
            1,
            12,
        );

        assert_eq!(prepared.row_selection_policy(), &RowSelectionPolicy::Mask);
    }
}
