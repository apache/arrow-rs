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
mod fallback;
mod filter;
mod selection_policy;

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReaderBuilder, CacheOptions, RowGroupCache};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::RowGroupExecutionMode;
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, PredicateOptions, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy, RowSelector,
};
use crate::arrow::in_memory_row_group::ColumnChunkData;
use crate::arrow::push_decoder::reader_builder::data::DataRequestBuilder;
use crate::arrow::push_decoder::reader_builder::fallback::RowGroupFallbackState;
use crate::arrow::push_decoder::reader_builder::filter::CacheInfo;
use crate::arrow::push_decoder::reader_builder::selection_policy::{
    ExpensiveOutputProfile, resolve_selection_policy_for_expensive_output,
};
#[cfg(test)]
use crate::arrow::push_decoder::reader_builder::selection_policy::{
    loaded_ranges_for_projection, resolve_selection_policy_for_projection,
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

enum FallbackTransition {
    ContinuePushdown,
    StartPostSelection { selection: RowSelection },
    EnablePostFilter,
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
    /// Needs data to read the row group once and apply the filter after decode.
    WaitingOnPostFilterData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        read_projection: ProjectionMask,
        filter: Arc<Mutex<RowFilter>>,
    },
    /// Needs data to read the row group once and apply an already-computed
    /// fallback selection after decode.
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

    /// Predicate state reused by later row groups once Auto fallback switches to post-filter.
    post_filter: Option<Arc<Mutex<RowFilter>>>,

    /// The size in bytes of the predicate cache to use
    ///
    /// See [`RowGroupCache`] for details.
    max_predicate_cache_size: usize,

    /// The metrics collector
    metrics: ArrowReaderMetrics,

    /// Strategy for materialising row selections
    row_selection_policy: RowSelectionPolicy,

    /// Row-group-local fallback state used by Auto policy.
    fallback_state: RowGroupFallbackState,

    /// Whether this builder may switch Auto policy to post-filter fallback.
    post_filter_fallback_enabled: bool,

    /// Current state of the decoder.
    ///
    /// It is taken when processing, and must be put back before returning
    /// it is a bug error if it is not put back after transitioning states.
    state: Option<RowGroupDecoderState>,

    /// The underlying data store
    buffers: PushBuffers,
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
            fallback_state: RowGroupFallbackState::default(),
            post_filter_fallback_enabled: true,
            state: Some(RowGroupDecoderState::Finished),
            buffers,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.buffers.push_ranges(ranges, buffers);
    }

    /// Returns the total number of buffered bytes available
    pub fn buffered_bytes(&self) -> u64 {
        self.buffers.buffered_bytes()
    }

    /// Clear any staged ranges currently buffered for future decode work.
    pub fn clear_all_ranges(&mut self) {
        self.buffers.clear_all_ranges();
    }

    /// Disable post-filter fallback for APIs that hand row-group readers back to
    /// callers before they are consumed.
    pub(crate) fn disable_post_filter_fallback(&mut self) {
        self.post_filter_fallback_enabled = false;
    }

    fn ensure_post_filter_state(&mut self) -> Result<(), ParquetError> {
        if self.post_filter.is_some() {
            return Ok(());
        }

        let filter = self.filter.take().ok_or_else(|| {
            ParquetError::General("post-filter fallback selected without a row filter".to_string())
        })?;
        self.post_filter = Some(Arc::new(Mutex::new(filter)));
        Ok(())
    }

    fn resolve_fallback_transition(
        &mut self,
        row_group_info: &RowGroupInfo,
        cache_info: Option<&CacheInfo>,
    ) -> Result<FallbackTransition, ParquetError> {
        if cache_info.is_none()
            || !matches!(self.fallback_state, RowGroupFallbackState::Observing { .. })
            || !self.post_filter_fallback_supported(row_group_info.budget)
        {
            return Ok(FallbackTransition::ContinuePushdown);
        }

        let decision = row_group_info
            .plan_builder
            .resolve_selection_strategy_decision();
        let observed_selection = row_group_info.plan_builder.selection().cloned();

        self.observe_fallback_candidate(decision, row_group_info.row_count, row_group_info.budget);

        if matches!(
            self.fallback_state,
            RowGroupFallbackState::UsePostFilter { .. }
        ) {
            if row_group_info.base_selection.is_none() {
                let selection = observed_selection.unwrap_or_else(|| {
                    RowSelection::from(vec![RowSelector::select(row_group_info.row_count)])
                });
                return Ok(FallbackTransition::StartPostSelection { selection });
            }

            self.ensure_post_filter_state()?;
            self.metrics
                .record_fallback_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
            return Ok(FallbackTransition::EnablePostFilter);
        }

        self.metrics
            .record_fallback_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
        Ok(FallbackTransition::ContinuePushdown)
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
        let result = match current_state {
            RowGroupDecoderState::Start { row_group_info } => {
                debug_assert!(
                    !row_group_info.budget.is_exhausted(),
                    "RowGroupFrontier should not hand off row groups after the output limit is exhausted"
                );

                let column_chunks = None; // no prior column chunks

                if let Some(filter) = self.post_filter.as_ref().cloned() {
                    return self.start_post_filter(row_group_info, filter);
                }

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
                };

                if self.should_use_post_filter_fallback(row_group_info.budget) {
                    if self
                        .post_filter_read_projection(&filter, row_group_info.budget)
                        .is_some()
                    {
                        let filter = Arc::new(Mutex::new(filter));
                        self.post_filter = Some(Arc::clone(&filter));
                        return self.start_post_filter(row_group_info, filter);
                    }

                    self.fallback_state = RowGroupFallbackState::UsePushdown;
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
                    base_selection,
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
                let predicate = filter_info.current();

                // need to fetch pages the column needs for decoding, figure
                // that out based on the current selection and projection
                let data_request = DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    predicate.projection(), // use the predicate's projection
                )
                .with_selection(plan_builder.selection())
                // Fetch predicate columns; expand selection only for cached predicate columns
                .with_cache_projection(Some(filter_info.cache_projection()))
                .with_column_chunks(column_chunks)
                .build();

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    base_selection,
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
                let needed_ranges = data_request.needed_ranges(&self.buffers);
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

                plan_builder = plan_builder.with_row_selection_policy(self.row_selection_policy);
                plan_builder = resolve_selection_policy_for_expensive_output(
                    plan_builder,
                    predicate.projection(),
                    self.row_group_offset_index(row_group_idx),
                    row_count,
                    ExpensiveOutputProfile::from_row_group(
                        self.metadata.row_group(row_group_idx),
                        predicate.projection(),
                        row_count,
                    ),
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
                    base_selection,
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
                    base_selection,
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

                let data_request = DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    &self.projection,
                )
                .with_selection(plan_builder.selection())
                .with_column_chunks(column_chunks)
                // Final projection fetch shouldn't expand selection for cache
                // so don't call with_cache_projection here
                .build();

                plan_builder = plan_builder.with_row_selection_policy(self.row_selection_policy);
                plan_builder = resolve_selection_policy_for_expensive_output(
                    plan_builder,
                    &self.projection,
                    self.row_group_offset_index(row_group_idx),
                    row_count,
                    ExpensiveOutputProfile::from_row_group(
                        self.metadata.row_group(row_group_idx),
                        &self.projection,
                        row_count,
                    ),
                );

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    base_selection,
                    budget: remaining_budget,
                };

                NextState::again(RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                    cache_info,
                })
            }
            RowGroupDecoderState::WaitingOnPostFilterData {
                row_group_info,
                data_request,
                read_projection,
                filter,
            } => {
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
                )?;

                self.metrics
                    .record_fallback_row_group(RowGroupExecutionMode::PostFilter);

                NextState::result(
                    RowGroupDecoderState::Finished,
                    RowGroupBuildResult::Data {
                        batch_reader: Box::new(reader),
                        remaining_budget: budget,
                    },
                )
            }
            RowGroupDecoderState::WaitingOnPostSelectionData {
                row_group_info,
                data_request,
                selection,
                cache_info,
            } => {
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

                let reader = ParquetRecordBatchReader::new_post_selection_filter(
                    array_reader,
                    plan,
                    selection,
                );

                self.metrics
                    .record_fallback_row_group(RowGroupExecutionMode::PostFilter);

                NextState::result(
                    RowGroupDecoderState::Finished,
                    RowGroupBuildResult::Data {
                        batch_reader: Box::new(reader),
                        remaining_budget: budget,
                    },
                )
            }
            // Waiting on data to proceed with reading the output
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
            } => {
                match self.resolve_fallback_transition(&row_group_info, cache_info.as_ref())? {
                    FallbackTransition::ContinuePushdown | FallbackTransition::EnablePostFilter => {
                    }
                    FallbackTransition::StartPostSelection { selection } => {
                        let column_chunks = data_request.into_dense_column_chunks();
                        // The current row group already computed a pushdown selection. Apply that
                        // selection after decode instead of evaluating the predicates again.
                        //
                        // Sparse predicate chunks may not cover the base selection. Dense chunks
                        // are safe to reuse and preserve predicate-cache IO behavior.
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
                    // still need data
                    return Ok(NextState::result(
                        RowGroupDecoderState::WaitingOnData {
                            row_group_info,
                            data_request,
                            cache_info,
                        },
                        RowGroupBuildResult::NeedsData(needed_ranges),
                    ));
                }

                // otherwise we have all the data we need to proceed
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
                        batch_reader: Box::new(reader),
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
            self.post_filter_read_projection(&filter, budget)
                .ok_or_else(|| {
                    ParquetError::General(
                        "post-filter fallback selected an unsupported projection".to_string(),
                    )
                })?
        };

        let data_request = DataRequestBuilder::new(
            row_group_idx,
            row_count,
            self.batch_size,
            &self.metadata,
            &read_projection,
        )
        .with_selection(plan_builder.selection())
        .build();

        plan_builder = plan_builder.with_row_selection_policy(self.row_selection_policy);
        plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &read_projection,
            self.row_group_offset_index(row_group_idx),
            row_count,
            ExpensiveOutputProfile::from_row_group(
                self.metadata.row_group(row_group_idx),
                &read_projection,
                row_count,
            ),
        );

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

        let data_request = DataRequestBuilder::new(
            row_group_idx,
            row_count,
            self.batch_size,
            &self.metadata,
            &self.projection,
        )
        .with_selection(plan_builder.selection())
        .with_column_chunks(column_chunks)
        .build();

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
    use crate::arrow::arrow_reader::selection::LoadedRowRanges;
    use crate::arrow::arrow_reader::{RowSelection, RowSelectionCursor, RowSelector};
    use crate::file::page_index::offset_index::PageLocation;

    #[test]
    fn test_resolve_selection_policy_preserves_mask_choice() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(99),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });

        assert_eq!(
            resolve_selection_policy_for_projection(
                plan_builder,
                &ProjectionMask::all(),
                None,
                101
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_resolve_selection_policy_preserves_selector_choice() {
        let selection = RowSelection::from(vec![RowSelector::select(128)]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1 });

        assert_eq!(
            resolve_selection_policy_for_projection(
                plan_builder,
                &ProjectionMask::all(),
                None,
                128
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_resolve_selection_policy_respects_explicit_policy() {
        let selection = RowSelection::from(vec![RowSelector::select(1), RowSelector::skip(1)]);
        let mask_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection.clone()))
            .with_row_selection_policy(RowSelectionPolicy::Mask);
        let selector_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Selectors);

        assert_eq!(
            resolve_selection_policy_for_projection(mask_builder, &ProjectionMask::all(), None, 2)
                .row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
        assert_eq!(
            resolve_selection_policy_for_projection(
                selector_builder,
                &ProjectionMask::all(),
                None,
                2
            )
            .row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_sparse_loaded_ranges_force_selectors() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(4),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_dense_loaded_ranges_preserve_mask() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_explicit_mask_keeps_sparse_loaded_ranges() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(4),
            RowSelector::select(1),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Mask);
        let offset_index = sparse_test_offset_index();

        let plan_builder = resolve_selection_policy_for_projection(
            plan_builder,
            &ProjectionMask::all(),
            Some(&offset_index),
            6,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );

        let mut plan = plan_builder.build();
        let RowSelectionCursor::Mask(cursor) = plan.row_selection_cursor_mut() else {
            panic!("expected mask cursor");
        };
        assert!(cursor.is_sparse());
    }

    #[test]
    fn test_loaded_ranges_intersects_many_ranges_across_projected_columns() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(1),
            RowSelector::skip(39),
            RowSelector::select(1),
            RowSelector::skip(39),
            RowSelector::select(1),
            RowSelector::skip(9),
        ]);
        let offset_index = vec![
            offset_index_column(&[0, 20, 40, 60, 80]),
            offset_index_column(&[0, 15, 35, 55, 75]),
            offset_index_column(&[0, 10, 30, 50, 70, 90]),
        ];

        let loaded = loaded_ranges_for_projection(
            Some(&selection),
            &ProjectionMask::all(),
            Some(&offset_index),
            100,
        );

        assert_eq!(
            loaded,
            Some(LoadedRowRanges::new(vec![10..15, 50..55, 90..100], 100))
        );
    }

    #[test]
    fn test_auto_expensive_fragmented_output_prefers_selectors() {
        let selection = q38_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_800,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_expensive_fragmented_output_prefers_selectors_without_selector_count_gate() {
        let selection = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
            RowSelector::select(1),
            RowSelector::skip(12),
        ]);
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            52,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Selectors
        );
    }

    #[test]
    fn test_auto_cheap_fragmented_output_keeps_mask() {
        let selection = q38_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 8.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_800,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    #[test]
    fn test_auto_moderate_selectivity_expensive_output_keeps_mask() {
        let selection = q26_like_fragmented_selection();
        let plan_builder = ReadPlanBuilder::new(1024)
            .with_selection(Some(selection))
            .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 1024 });
        let profile = ExpensiveOutputProfile {
            variable_width_columns: 1,
            uncompressed_bytes_per_row: 64.0,
        };

        let plan_builder = resolve_selection_policy_for_expensive_output(
            plan_builder,
            &ProjectionMask::all(),
            None,
            7_200,
            profile,
        );

        assert_eq!(
            plan_builder.row_selection_policy(),
            &RowSelectionPolicy::Mask
        );
    }

    fn q38_like_fragmented_selection() -> RowSelection {
        let mut selectors = Vec::new();
        for _ in 0..600 {
            selectors.push(RowSelector::select(1));
            selectors.push(RowSelector::skip(12));
        }
        RowSelection::from(selectors)
    }

    fn q26_like_fragmented_selection() -> RowSelection {
        let mut selectors = Vec::new();
        for _ in 0..600 {
            selectors.push(RowSelector::select(2));
            selectors.push(RowSelector::skip(10));
        }
        RowSelection::from(selectors)
    }

    fn sparse_test_offset_index() -> Vec<OffsetIndexMetaData> {
        vec![OffsetIndexMetaData {
            page_locations: vec![
                PageLocation {
                    offset: 0,
                    compressed_page_size: 10,
                    first_row_index: 0,
                },
                PageLocation {
                    offset: 10,
                    compressed_page_size: 10,
                    first_row_index: 2,
                },
                PageLocation {
                    offset: 20,
                    compressed_page_size: 10,
                    first_row_index: 4,
                },
            ],
            unencoded_byte_array_data_bytes: None,
        }]
    }

    fn offset_index_column(first_rows: &[i64]) -> OffsetIndexMetaData {
        OffsetIndexMetaData {
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
        }
    }

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
