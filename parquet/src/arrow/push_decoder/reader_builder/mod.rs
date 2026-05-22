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

mod cost_model;
mod data;
mod filter;
mod selection_policy;

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReader, ArrayReaderBuilder, CacheOptions, RowGroupCache};
use crate::arrow::arrow_reader::metrics::{ArrowReaderMetrics, ArrowReaderPhase};
use crate::arrow::arrow_reader::selection::RowGroupExecutionMode;
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, PredicateOptions, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy, RowSelector,
};
use crate::arrow::in_memory_row_group::{ColumnChunkData, InMemoryRowGroup};
use crate::arrow::push_decoder::reader_builder::cost_model::RowGroupCostModelState;
use crate::arrow::push_decoder::reader_builder::data::DataRequestBuilder;
use crate::arrow::push_decoder::reader_builder::filter::CacheInfo;
use crate::arrow::push_decoder::reader_builder::selection_policy::{
    ExpensiveOutputProfile, resolve_selection_policy_for_expensive_output,
};
use crate::arrow::schema::ParquetField;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::util::push_buffers::PushBuffers;
use bytes::Bytes;
use data::DataRequest;
use filter::AdvanceResult;
use filter::FilterInfo;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

/// The current row group being read, its read plan, and its offset/limit budget.
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
    base_selection: Option<RowSelection>,
    budget: RowBudget,
}

enum CostModelTransition {
    ContinuePushdown,
    /// The current row group already evaluated predicates and produced a
    /// selection, but Auto now prefers post-filter for this scan shape. Decode
    /// the current row group's output once and apply the existing selection
    /// after decode instead of evaluating predicates a second time.
    StartPostSelection {
        selection: RowSelection,
    },
}

enum FilterExecutionPlan {
    /// No predicate work remains for this row group; proceed to output planning.
    ReadOutput,
    /// Decode the union of output and predicate columns once, then evaluate
    /// predicates on decoded batches.
    PostFilter { filter: Arc<Mutex<RowFilter>> },
    /// Decode predicate columns first, build a RowSelection, then read output.
    Pushdown { filter_info: FilterInfo },
}

/// This is the inner state machine for reading a single row group.
///
/// The top-level flow is:
///
/// ```text
/// Start
///   +-- no filter / no predicates ----------------------> StartData
///   +-- Auto chooses post-filter ------------------------> WaitingOnPostFilterData
///   +-- predicate pushdown ------------------------------> Filters
///
/// Filters -> WaitingOnFilterData -> Filters | StartData
///
/// StartData
///   +-- no rows after selection/limit -------------------> Finished
///   +-- output data needed ------------------------------> WaitingOnData
///
/// WaitingOnData
///   +-- Auto switches current row group to post-selection > WaitingOnPostSelectionData
///   +-- output reader ready -----------------------------> Finished
/// ```
///
/// Each state arm delegates to a `transition_*` method so the dispatch table
/// remains readable before diving into the details for each phase.
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
    /// Needs data to read the row group once and apply the filter after decode.
    WaitingOnPostFilterData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        read_projection: ProjectionMask,
        filter: Arc<Mutex<RowFilter>>,
    },
    /// Needs data to read the row group once and apply an already-computed
    /// selection after decode.
    WaitingOnPostSelectionData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        selection: RowSelection,
        cache_info: Option<CacheInfo>,
    },
    /// Needs data to proceed with reading the output
    WaitingOnData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        /// Any cached filter results
        cache_info: Option<CacheInfo>,
    },
    /// Finished (or not yet started) reading this group
    Finished,
}

/// Running offset/limit budget shared across row groups.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct RowBudget {
    offset: Option<usize>,
    limit: Option<usize>,
}

impl RowBudget {
    pub(crate) fn new(offset: Option<usize>, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }

    pub(crate) fn is_exhausted(self) -> bool {
        matches!(self.limit, Some(0))
    }

    pub(crate) fn is_unbounded(self) -> bool {
        self.offset.is_none() && self.limit.is_none()
    }

    /// The offset still to be skipped before the next readable row group.
    pub(crate) fn offset(self) -> Option<usize> {
        self.offset
    }

    /// The number of output rows still permitted across the remaining row groups.
    pub(crate) fn limit(self) -> Option<usize> {
        self.limit
    }

    /// Returns how many selected rows remain after applying this budget.
    pub(crate) fn rows_after(self, rows_before_budget: usize) -> usize {
        let rows_after_offset = rows_before_budget.saturating_sub(self.offset.unwrap_or(0));
        match self.limit {
            Some(limit) => rows_after_offset.min(limit),
            None => rows_after_offset,
        }
    }

    /// Returns the number of selected rows needed before applying the offset.
    fn selected_row_limit(self) -> Option<usize> {
        self.limit
            .map(|limit| limit.saturating_add(self.offset.unwrap_or(0)))
    }

    fn apply_to_plan(self, plan_builder: ReadPlanBuilder, row_count: usize) -> BudgetedReadPlan {
        let rows_before_budget = plan_builder.num_rows_selected().unwrap_or(row_count);
        let plan_builder = plan_builder
            .limited(row_count)
            .with_offset(self.offset)
            .with_limit(self.limit)
            .build_limited();
        let rows_after_budget = self.rows_after(rows_before_budget);

        BudgetedReadPlan {
            plan_builder,
            rows_before_budget,
            rows_after_budget,
            remaining_budget: self.advance(rows_before_budget, rows_after_budget),
        }
    }

    /// Advance the budget past one row group.
    ///
    /// `rows_before_budget` is the number of rows selected before applying the
    /// budget, and `rows_after_budget` is the number retained for output from
    /// this row group.
    pub(crate) fn advance(mut self, rows_before_budget: usize, rows_after_budget: usize) -> Self {
        if let Some(offset) = &mut self.offset {
            // Reduction is either because of offset or limit, as limit is applied
            // after offset has been "exhausted" can just use saturating sub here.
            *offset = offset.saturating_sub(rows_before_budget - rows_after_budget);
        }

        if rows_after_budget != 0 {
            if let Some(limit) = &mut self.limit {
                *limit -= rows_after_budget;
            }
        }

        self
    }
}

#[derive(Debug)]
struct BudgetedReadPlan {
    /// Read plan after applying this row group's share of the offset/limit budget.
    plan_builder: ReadPlanBuilder,
    /// Number of rows selected by row selection and predicates before applying
    /// this row group's offset/limit budget.
    rows_before_budget: usize,
    /// Number of selected rows that remain to be read after applying this row
    /// group's offset/limit budget.
    rows_after_budget: usize,
    /// Budget remaining for later row groups.
    remaining_budget: RowBudget,
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
    /// The active row group produced a reader.
    Data {
        batch_reader: Box<ParquetRecordBatchReader>,
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

    /// Predicate state reused by later row groups once Auto chooses post-filter.
    post_filter: Option<Arc<Mutex<RowFilter>>>,

    /// The size in bytes of the predicate cache to use
    ///
    /// See [`RowGroupCache`] for details.
    max_predicate_cache_size: usize,

    /// The metrics collector
    metrics: ArrowReaderMetrics,

    /// Strategy for materialising row selections
    row_selection_policy: RowSelectionPolicy,

    /// Row-group-local cost-model state used by Auto policy.
    cost_model_state: RowGroupCostModelState,

    /// Whether this builder may switch Auto policy to post-filter by cost.
    post_filter_cost_model_enabled: bool,

    /// Current state of the decoder.
    ///
    /// It is taken when processing, and must be put back before returning
    /// it is a bug error if it is not put back after transitioning states.
    state: Option<RowGroupDecoderState>,

    /// The underlying data store
    buffers: PushBuffers,
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
    ) -> Self {
        Self {
            batch_size,
            projection,
            metadata,
            fields,
            filter,
            post_filter: None,
            metrics,
            max_predicate_cache_size,
            row_selection_policy,
            cost_model_state: RowGroupCostModelState::default(),
            post_filter_cost_model_enabled: true,
            state: Some(RowGroupDecoderState::Finished),
            buffers,
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
            post_filter: _,
            max_predicate_cache_size,
            metrics,
            row_selection_policy,
            cost_model_state: _,
            post_filter_cost_model_enabled: _,
            state: _,
            buffers,
        } = self;
        RowGroupReaderBuilderParts {
            batch_size,
            projection,
            fields,
            filter,
            max_predicate_cache_size,
            metrics,
            row_selection_policy,
            buffers,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.buffers.push_ranges(ranges, buffers);
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
        self.buffers.buffered_bytes()
    }

    /// Clear any staged ranges currently buffered for future decode work.
    pub fn clear_all_ranges(&mut self) {
        self.buffers.clear_all_ranges();
    }

    /// Disable post-filter cost modeling for APIs that hand row-group readers back to
    /// callers before they are consumed.
    pub(crate) fn disable_post_filter_cost_model(&mut self) {
        self.post_filter_cost_model_enabled = false;
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
            .with_selection(selection.clone())
            .with_row_selection_policy(self.row_selection_policy);

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: selection,
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
        match current_state {
            RowGroupDecoderState::Start { row_group_info } => self.transition_start(row_group_info),
            RowGroupDecoderState::Filters {
                row_group_info,
                column_chunks,
                filter_info,
            } => self.transition_filters(row_group_info, column_chunks, filter_info),
            RowGroupDecoderState::WaitingOnFilterData {
                row_group_info,
                data_request,
                filter_info,
            } => self.transition_waiting_on_filter_data(row_group_info, data_request, filter_info),
            RowGroupDecoderState::StartData {
                row_group_info,
                column_chunks,
                cache_info,
            } => self.transition_start_data(row_group_info, column_chunks, cache_info),
            RowGroupDecoderState::WaitingOnPostFilterData {
                row_group_info,
                data_request,
                read_projection,
                filter,
            } => self.transition_waiting_on_post_filter_data(
                row_group_info,
                data_request,
                read_projection,
                filter,
            ),
            RowGroupDecoderState::WaitingOnPostSelectionData {
                row_group_info,
                data_request,
                selection,
                cache_info,
            } => self.transition_waiting_on_post_selection_data(
                row_group_info,
                data_request,
                selection,
                cache_info,
            ),
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
            } => self.transition_waiting_on_data(row_group_info, data_request, cache_info),
            RowGroupDecoderState::Finished => Err(ParquetError::General(String::from(
                "Internal Error: try_build called without an active row group",
            ))),
        }
    }

    fn transition_start(
        &mut self,
        row_group_info: RowGroupInfo,
    ) -> Result<NextState, ParquetError> {
        debug_assert!(
            !row_group_info.budget.is_exhausted(),
            "RowGroupFrontier should not hand off row groups after the output limit is exhausted"
        );

        let column_chunks = None;

        if let Some(filter) = self.post_filter.as_ref().cloned() {
            return self.start_post_filter(row_group_info, filter);
        }

        let Some(filter) = self.filter.take() else {
            return Ok(NextState::again(RowGroupDecoderState::StartData {
                row_group_info,
                column_chunks,
                cache_info: None,
            }));
        };

        match self.plan_filter_execution(&row_group_info, filter) {
            FilterExecutionPlan::ReadOutput => {
                Ok(NextState::again(RowGroupDecoderState::StartData {
                    row_group_info,
                    column_chunks,
                    cache_info: None,
                }))
            }
            FilterExecutionPlan::PostFilter { filter } => {
                self.start_post_filter(row_group_info, filter)
            }
            FilterExecutionPlan::Pushdown { filter_info } => {
                Ok(NextState::again(RowGroupDecoderState::Filters {
                    row_group_info,
                    filter_info,
                    column_chunks,
                }))
            }
        }
    }

    fn plan_filter_execution(
        &mut self,
        row_group_info: &RowGroupInfo,
        filter: RowFilter,
    ) -> FilterExecutionPlan {
        if filter.predicates.is_empty() {
            return FilterExecutionPlan::ReadOutput;
        }

        if self.should_start_with_post_filter(
            &filter,
            row_group_info.row_group_idx,
            row_group_info.budget,
        ) {
            return FilterExecutionPlan::PostFilter {
                filter: self.install_post_filter(filter),
            };
        }

        if self.should_use_post_filter_by_cost(row_group_info.budget) {
            if self
                .post_filter_read_projection(&filter, row_group_info.budget)
                .is_some()
            {
                return FilterExecutionPlan::PostFilter {
                    filter: self.install_post_filter(filter),
                };
            }

            self.cost_model_state = RowGroupCostModelState::UsePushdown;
        }

        let cache_projection = self.compute_cache_projection(row_group_info.row_group_idx, &filter);
        let cache_info = CacheInfo::new(
            cache_projection,
            Arc::new(RwLock::new(RowGroupCache::new(
                self.batch_size,
                self.max_predicate_cache_size,
            ))),
        );
        let filter_info = FilterInfo::new(filter, cache_info);
        FilterExecutionPlan::Pushdown { filter_info }
    }

    fn install_post_filter(&mut self, filter: RowFilter) -> Arc<Mutex<RowFilter>> {
        let filter = Arc::new(Mutex::new(filter));
        self.post_filter = Some(Arc::clone(&filter));
        filter
    }

    fn transition_filters(
        &mut self,
        row_group_info: RowGroupInfo,
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
        filter_info: FilterInfo,
    ) -> Result<NextState, ParquetError> {
        let RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection,
            budget,
        } = row_group_info;

        if !plan_builder.selects_any() {
            self.filter = Some(filter_info.into_filter());
            return Ok(NextState::result(
                RowGroupDecoderState::Finished,
                RowGroupBuildResult::Finished {
                    remaining_budget: budget,
                },
            ));
        }

        let predicate = filter_info.current();
        let data_request =
            self.metrics
                .time_phase(ArrowReaderPhase::PredicateRangePlanning, || {
                    DataRequestBuilder::new(
                        row_group_idx,
                        row_count,
                        self.batch_size,
                        &self.metadata,
                        predicate.projection(),
                    )
                    .with_selection(plan_builder.selection())
                    .with_cache_projection(Some(filter_info.cache_projection()))
                    .with_column_chunks(column_chunks)
                    .build()
                });

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection,
            budget,
        };

        Ok(NextState::again(
            RowGroupDecoderState::WaitingOnFilterData {
                row_group_info,
                filter_info,
                data_request,
            },
        ))
    }

    fn transition_waiting_on_filter_data(
        &mut self,
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        mut filter_info: FilterInfo,
    ) -> Result<NextState, ParquetError> {
        let needed_ranges = data_request.needed_ranges(&self.buffers);
        if !needed_ranges.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::WaitingOnFilterData {
                    row_group_info,
                    filter_info,
                    data_request,
                },
                RowGroupBuildResult::NeedsData(needed_ranges),
            ));
        }

        let RowGroupInfo {
            row_group_idx,
            row_count,
            mut plan_builder,
            base_selection,
            budget,
        } = row_group_info;

        let predicate = filter_info.current();
        let row_group = data_request.try_into_in_memory_row_group(
            row_group_idx,
            row_count,
            &self.metadata,
            predicate.projection(),
            &mut self.buffers,
        )?;

        let cache_options = filter_info.cache_builder().producer();
        let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
            .with_batch_size(self.batch_size)
            .with_cache_options(Some(&cache_options))
            .with_parquet_metadata(&self.metadata)
            .build_array_reader(self.fields.as_deref(), predicate.projection())?;

        plan_builder = self.resolve_output_selection_policy(
            plan_builder,
            predicate.projection(),
            row_group_idx,
            row_count,
        );

        let predicate_limit = filter_info
            .is_last()
            .then(|| budget.selected_row_limit())
            .flatten();
        let mut predicate_options = PredicateOptions::new(array_reader, filter_info.current_mut())
            .with_metrics(self.metrics.clone());
        if let Some(limit) = predicate_limit {
            predicate_options = predicate_options.with_limit(limit, row_count);
        }
        plan_builder = plan_builder.with_predicate_options(predicate_options)?;

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection,
            budget,
        };
        let column_chunks = Some(row_group.column_chunks);

        Ok(match filter_info.advance() {
            AdvanceResult::Continue(filter_info) => {
                NextState::again(RowGroupDecoderState::Filters {
                    row_group_info,
                    column_chunks,
                    filter_info,
                })
            }
            AdvanceResult::Done(filter, cache_info) => {
                assert!(self.filter.is_none());
                self.filter = Some(filter);
                NextState::again(RowGroupDecoderState::StartData {
                    row_group_info,
                    column_chunks,
                    cache_info: Some(cache_info),
                })
            }
        })
    }

    fn transition_start_data(
        &mut self,
        row_group_info: RowGroupInfo,
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
        cache_info: Option<CacheInfo>,
    ) -> Result<NextState, ParquetError> {
        let RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection,
            budget,
        } = row_group_info;

        let BudgetedReadPlan {
            mut plan_builder,
            rows_before_budget,
            rows_after_budget,
            remaining_budget,
        } = budget.apply_to_plan(plan_builder, row_count);

        if rows_before_budget == 0 || rows_after_budget == 0 {
            return Ok(NextState::result(
                RowGroupDecoderState::Finished,
                RowGroupBuildResult::Finished { remaining_budget },
            ));
        }

        let data_request = self
            .metrics
            .time_phase(ArrowReaderPhase::OutputRangePlanning, || {
                DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    &self.projection,
                )
                .with_selection(plan_builder.selection())
                .with_column_chunks(column_chunks)
                // Final projection fetch shouldn't expand selection for cache
                // so don't call with_cache_projection here.
                .build()
            });

        plan_builder = self
            .metrics
            .time_phase(ArrowReaderPhase::OutputSelectionResolve, || {
                self.resolve_output_selection_policy(
                    plan_builder,
                    &self.projection,
                    row_group_idx,
                    row_count,
                )
            });

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection,
            budget: remaining_budget,
        };

        Ok(NextState::again(RowGroupDecoderState::WaitingOnData {
            row_group_info,
            data_request,
            cache_info,
        }))
    }

    fn transition_waiting_on_post_filter_data(
        &mut self,
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        read_projection: ProjectionMask,
        filter: Arc<Mutex<RowFilter>>,
    ) -> Result<NextState, ParquetError> {
        let needed_ranges = data_request.needed_ranges(&self.buffers);
        if !needed_ranges.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::WaitingOnPostFilterData {
                    row_group_info,
                    data_request,
                    read_projection,
                    filter,
                },
                RowGroupBuildResult::NeedsData(needed_ranges),
            ));
        }

        let RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: _,
            budget,
        } = row_group_info;

        let row_group = data_request.try_into_in_memory_row_group(
            row_group_idx,
            row_count,
            &self.metadata,
            &read_projection,
            &mut self.buffers,
        )?;
        let plan = plan_builder.build_with_metrics(&self.metrics);
        let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
            .with_batch_size(self.batch_size)
            .with_parquet_metadata(&self.metadata)
            .build_array_reader(self.fields.as_deref(), &read_projection)?;
        let reader = ParquetRecordBatchReader::new_post_filter(
            array_reader,
            plan,
            filter,
            self.metadata.file_metadata().schema_descr(),
            &read_projection,
            &self.projection,
            self.metrics.clone(),
        )?;

        self.metrics
            .record_cost_model_row_group(RowGroupExecutionMode::PostFilter);
        Ok(NextState::result(
            RowGroupDecoderState::Finished,
            RowGroupBuildResult::Data {
                batch_reader: Box::new(reader),
                remaining_budget: budget,
            },
        ))
    }

    fn transition_waiting_on_post_selection_data(
        &mut self,
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        selection: RowSelection,
        cache_info: Option<CacheInfo>,
    ) -> Result<NextState, ParquetError> {
        let needed_ranges = data_request.needed_ranges(&self.buffers);
        if !needed_ranges.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::WaitingOnPostSelectionData {
                    row_group_info,
                    data_request,
                    selection,
                    cache_info,
                },
                RowGroupBuildResult::NeedsData(needed_ranges),
            ));
        }

        let RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: _,
            budget,
        } = row_group_info;

        let row_group = data_request.try_into_in_memory_row_group(
            row_group_idx,
            row_count,
            &self.metadata,
            &self.projection,
            &mut self.buffers,
        )?;
        let plan = plan_builder.build_with_metrics(&self.metrics);
        let array_reader = self.build_projection_reader(&row_group, cache_info.as_ref())?;
        let reader = ParquetRecordBatchReader::new_post_selection_filter(
            array_reader,
            plan,
            selection,
            self.metrics.clone(),
        );

        self.metrics
            .record_cost_model_row_group(RowGroupExecutionMode::PostFilter);
        Ok(NextState::result(
            RowGroupDecoderState::Finished,
            RowGroupBuildResult::Data {
                batch_reader: Box::new(reader),
                remaining_budget: budget,
            },
        ))
    }

    fn transition_waiting_on_data(
        &mut self,
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        cache_info: Option<CacheInfo>,
    ) -> Result<NextState, ParquetError> {
        match self.resolve_cost_model_transition(&row_group_info, cache_info.as_ref())? {
            CostModelTransition::ContinuePushdown => {}
            CostModelTransition::StartPostSelection { selection } => {
                let column_chunks = data_request.into_dense_column_chunks();
                return self.start_post_selection_filter(
                    row_group_info,
                    selection,
                    cache_info,
                    column_chunks,
                );
            }
        }

        let needed_ranges = data_request.needed_ranges(&self.buffers);
        if !needed_ranges.is_empty() {
            return Ok(NextState::result(
                RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                    cache_info,
                },
                RowGroupBuildResult::NeedsData(needed_ranges),
            ));
        }

        let RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: _,
            budget,
        } = row_group_info;

        let row_group = data_request.try_into_in_memory_row_group(
            row_group_idx,
            row_count,
            &self.metadata,
            &self.projection,
            &mut self.buffers,
        )?;
        let plan = plan_builder.build_with_metrics(&self.metrics);
        let array_reader = self.build_projection_reader(&row_group, cache_info.as_ref())?;
        let reader =
            ParquetRecordBatchReader::new_with_metrics(array_reader, plan, self.metrics.clone());

        Ok(NextState::result(
            RowGroupDecoderState::Finished,
            RowGroupBuildResult::Data {
                batch_reader: Box::new(reader),
                remaining_budget: budget,
            },
        ))
    }

    fn resolve_cost_model_transition(
        &mut self,
        row_group_info: &RowGroupInfo,
        cache_info: Option<&CacheInfo>,
    ) -> Result<CostModelTransition, ParquetError> {
        if cache_info.is_none()
            || !matches!(
                self.cost_model_state,
                RowGroupCostModelState::Observing { .. }
            )
            || !self.post_filter_cost_model_supported(row_group_info.budget)
        {
            return Ok(CostModelTransition::ContinuePushdown);
        }

        let decision = row_group_info
            .plan_builder
            .resolve_selection_strategy_decision();
        let observed_selection = row_group_info.plan_builder.selection().cloned();

        self.observe_cost_model_candidate(
            decision,
            row_group_info.row_count,
            row_group_info.budget,
        );

        if matches!(self.cost_model_state, RowGroupCostModelState::UsePostFilter) {
            if row_group_info.base_selection.is_none() {
                let selection = observed_selection.unwrap_or_else(|| {
                    RowSelection::from(vec![RowSelector::select(row_group_info.row_count)])
                });
                return Ok(CostModelTransition::StartPostSelection { selection });
            }

            self.ensure_post_filter_state()?;
            self.metrics
                .record_cost_model_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
            // This row group was already planned with a base selection, so keep
            // its current pushdown path. The state above enables post-filter
            // execution for later row groups.
            return Ok(CostModelTransition::ContinuePushdown);
        }

        self.metrics
            .record_cost_model_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
        Ok(CostModelTransition::ContinuePushdown)
    }

    fn ensure_post_filter_state(&mut self) -> Result<(), ParquetError> {
        if self.post_filter.is_some() {
            return Ok(());
        }

        let filter = self.filter.take().ok_or_else(|| {
            ParquetError::General(
                "post-filter cost model selected without a row filter".to_string(),
            )
        })?;
        self.post_filter = Some(Arc::new(Mutex::new(filter)));
        Ok(())
    }

    fn resolve_output_selection_policy(
        &self,
        plan_builder: ReadPlanBuilder,
        projection: &ProjectionMask,
        row_group_idx: usize,
        row_count: usize,
    ) -> ReadPlanBuilder {
        resolve_selection_policy_for_expensive_output(
            plan_builder.with_row_selection_policy(self.row_selection_policy),
            projection,
            self.row_group_offset_index(row_group_idx),
            row_count,
            ExpensiveOutputProfile::from_row_group(
                self.metadata.row_group(row_group_idx),
                projection,
                row_count,
            ),
        )
    }

    fn build_projection_reader(
        &self,
        row_group: &InMemoryRowGroup<'_>,
        cache_info: Option<&CacheInfo>,
    ) -> Result<Box<dyn ArrayReader>, ParquetError> {
        let array_reader_builder = ArrayReaderBuilder::new(row_group, &self.metrics)
            .with_batch_size(self.batch_size)
            .with_parquet_metadata(&self.metadata);

        if let Some(cache_info) = cache_info {
            let cache_options: CacheOptions = cache_info.builder().consumer();
            array_reader_builder
                .with_cache_options(Some(&cache_options))
                .build_array_reader(self.fields.as_deref(), &self.projection)
        } else {
            array_reader_builder.build_array_reader(self.fields.as_deref(), &self.projection)
        }
    }

    fn start_post_filter(
        &mut self,
        row_group_info: RowGroupInfo,
        filter: Arc<Mutex<RowFilter>>,
    ) -> Result<NextState, ParquetError> {
        let RowGroupInfo {
            row_group_idx,
            row_count,
            base_selection,
            budget,
            ..
        } = row_group_info;

        let mut plan_builder = ReadPlanBuilder::new(self.batch_size)
            .with_selection(base_selection)
            .with_row_selection_policy(self.row_selection_policy);

        if !plan_builder.selects_any() {
            return Ok(NextState::result(
                RowGroupDecoderState::Finished,
                RowGroupBuildResult::Finished {
                    remaining_budget: budget,
                },
            ));
        }

        let read_projection = {
            let filter = filter.lock().map_err(|_| {
                ParquetError::General("post-filter predicate state was poisoned".to_string())
            })?;
            self.post_filter_read_projection_for_filter(&filter, budget)
                .ok_or_else(|| {
                    ParquetError::General(
                        "post-filter cost model selected an unsupported projection".to_string(),
                    )
                })?
        };

        let data_request = self
            .metrics
            .time_phase(ArrowReaderPhase::OutputRangePlanning, || {
                DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    &read_projection,
                )
                .with_selection(plan_builder.selection())
                .build()
            });

        if plan_builder.selection().is_some() {
            plan_builder =
                self.metrics
                    .time_phase(ArrowReaderPhase::OutputSelectionResolve, || {
                        self.resolve_output_selection_policy(
                            plan_builder,
                            &read_projection,
                            row_group_idx,
                            row_count,
                        )
                    });
        }

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: None,
            budget,
        };

        Ok(NextState::again(
            RowGroupDecoderState::WaitingOnPostFilterData {
                row_group_info,
                data_request,
                read_projection,
                filter,
            },
        ))
    }

    fn start_post_selection_filter(
        &mut self,
        row_group_info: RowGroupInfo,
        selection: RowSelection,
        cache_info: Option<CacheInfo>,
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
    ) -> Result<NextState, ParquetError> {
        let RowGroupInfo {
            row_group_idx,
            row_count,
            base_selection,
            budget,
            ..
        } = row_group_info;

        let plan_builder = ReadPlanBuilder::new(self.batch_size)
            .with_selection(base_selection)
            .with_row_selection_policy(self.row_selection_policy);

        let data_request = self
            .metrics
            .time_phase(ArrowReaderPhase::OutputRangePlanning, || {
                DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    &self.projection,
                )
                .with_selection(plan_builder.selection())
                .with_column_chunks(column_chunks)
                .build()
            });

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
            base_selection: None,
            budget,
        };

        Ok(NextState::again(
            RowGroupDecoderState::WaitingOnPostSelectionData {
                row_group_info,
                data_request,
                selection,
                cache_info,
            },
        ))
    }

    /// Which columns should be cached?
    ///
    /// Returns the columns that are used by the filters *and* then used in the
    /// final projection, excluding any nested columns.
    fn compute_cache_projection(&self, row_group_idx: usize, filter: &RowFilter) -> ProjectionMask {
        let meta = self.metadata.row_group(row_group_idx);
        match self.compute_cache_projection_inner(filter) {
            Some(projection) => projection,
            None => ProjectionMask::none(meta.columns().len()),
        }
    }

    fn compute_cache_projection_inner(&self, filter: &RowFilter) -> Option<ProjectionMask> {
        // Do not compute the projection mask if the predicate cache is disabled
        if self.max_predicate_cache_size == 0 {
            return None;
        }
        let mut cache_projection = filter.predicates.first()?.projection().clone();
        for predicate in filter.predicates.iter() {
            cache_projection.union(predicate.projection());
        }
        cache_projection.intersect(&self.projection);
        self.exclude_nested_columns_from_cache(&cache_projection)
    }

    /// Exclude leaves belonging to roots that span multiple parquet leaves (i.e. nested columns)
    fn exclude_nested_columns_from_cache(&self, mask: &ProjectionMask) -> Option<ProjectionMask> {
        mask.without_nested_types(self.metadata.file_metadata().schema_descr())
    }

    /// Get the offset index for the specified row group, if any
    fn row_group_offset_index(&self, row_group_idx: usize) -> Option<&[OffsetIndexMetaData]> {
        self.metadata
            .offset_index()
            .filter(|index| !index.is_empty())
            .and_then(|index| index.get(row_group_idx))
            .map(|columns| columns.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Verify that the size of RowGroupDecoderState does not grow too large
    fn test_structure_size() {
        assert_eq!(std::mem::size_of::<RowGroupDecoderState>(), 288);
    }

    #[test]
    fn test_row_budget_offset_limit_across_row_groups() {
        let first =
            RowBudget::new(Some(225), Some(20)).apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(first.rows_before_budget, 200);
        assert_eq!(first.rows_after_budget, 0);
        assert_eq!(first.remaining_budget, RowBudget::new(Some(25), Some(20)));
        assert_eq!(first.plan_builder.num_rows_selected(), Some(0));

        let second = first
            .remaining_budget
            .apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(second.rows_before_budget, 200);
        assert_eq!(second.rows_after_budget, 20);
        assert_eq!(second.remaining_budget, RowBudget::new(Some(0), Some(0)));
        assert_eq!(second.plan_builder.num_rows_selected(), Some(20));
    }

    #[test]
    fn test_row_budget_limit_only() {
        let budgeted =
            RowBudget::new(None, Some(20)).apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(budgeted.rows_before_budget, 200);
        assert_eq!(budgeted.rows_after_budget, 20);
        assert_eq!(budgeted.remaining_budget, RowBudget::new(None, Some(0)));
        assert_eq!(budgeted.plan_builder.num_rows_selected(), Some(20));
    }

    #[test]
    fn test_row_budget_empty_selection() {
        let empty_selection = RowSelection::from(vec![RowSelector::skip(200)]);
        let budgeted = RowBudget::new(Some(10), Some(20)).apply_to_plan(
            ReadPlanBuilder::new(1024).with_selection(Some(empty_selection)),
            200,
        );
        assert_eq!(budgeted.rows_before_budget, 0);
        assert_eq!(budgeted.rows_after_budget, 0);
        assert_eq!(
            budgeted.remaining_budget,
            RowBudget::new(Some(10), Some(20))
        );
        assert_eq!(budgeted.plan_builder.num_rows_selected(), Some(0));
    }
}
