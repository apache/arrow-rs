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

use crate::DecodeResult;
use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReaderBuilder, CacheOptions, RowGroupCache};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::{
    FallbackObservation, FallbackTriggerReason, LoadedRowRanges, RowGroupExecutionMode,
    RowSelectionShape, RowSelectionStrategy, RowSelectionStrategyDecision,
};
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, PredicateOptions, ReadPlanBuilder, RowFilter, RowSelection,
    RowSelectionPolicy, RowSelector,
};
use crate::arrow::in_memory_row_group::ColumnChunkData;
use crate::arrow::push_decoder::reader_builder::data::DataRequestBuilder;
use crate::arrow::push_decoder::reader_builder::filter::CacheInfo;
use crate::arrow::schema::{ParquetField, ParquetFieldType};
use crate::basic::Type as PhysicalType;
use crate::errors::ParquetError;
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::util::push_buffers::PushBuffers;
use bytes::Bytes;
use data::DataRequest;
use filter::AdvanceResult;
use filter::FilterInfo;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

/// The current row group being read and the read plan
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
    base_selection: Option<RowSelection>,
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

#[allow(dead_code)]
#[derive(Debug)]
enum RowGroupFallbackState {
    Observing { observation: FallbackObservation },
    UsePushdown,
    UsePostFilter { reason: FallbackTriggerReason },
}

impl Default for RowGroupFallbackState {
    fn default() -> Self {
        Self::Observing {
            observation: FallbackObservation::default(),
        }
    }
}

/// Result of a state transition
#[derive(Debug)]
struct NextState {
    next_state: RowGroupDecoderState,
    /// result to return, if any
    ///
    /// * `Some`: the processing should stop and return the result
    /// * `None`: processing should continue
    result: Option<DecodeResult<ParquetRecordBatchReader>>,
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
    fn result(
        next_state: RowGroupDecoderState,
        result: DecodeResult<ParquetRecordBatchReader>,
    ) -> Self {
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

    /// Shared filter state used once Auto fallback switches to post-filter.
    post_filter: Option<Arc<Mutex<RowFilter>>>,

    /// Limit to apply to remaining row groups (decremented as rows are read)
    limit: Option<usize>,

    /// Offset to apply to remaining row groups (decremented as rows are read)
    offset: Option<usize>,

    /// The size in bytes of the predicate cache to use
    ///
    /// See [`RowGroupCache`] for details.
    max_predicate_cache_size: usize,

    /// The metrics collector
    metrics: ArrowReaderMetrics,

    /// Strategy for materialising row selections
    row_selection_policy: RowSelectionPolicy,

    /// Row-group-local fallback state used by Auto policy.
    #[allow(dead_code)]
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
        limit: Option<usize>,
        offset: Option<usize>,
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
            limit,
            offset,
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

    /// Setup this reader to read the next row group
    pub(crate) fn next_row_group(
        &mut self,
        row_group_idx: usize,
        row_count: usize,
        selection: Option<RowSelection>,
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
        };

        self.state = Some(RowGroupDecoderState::Start { row_group_info });
        Ok(())
    }

    /// Try to build the next `ParquetRecordBatchReader` from this RowGroupReader.
    ///
    /// If more data is needed, returns [`DecodeResult::NeedsData`] with the
    /// ranges of data that are needed to proceed.
    ///
    /// If a [`ParquetRecordBatchReader`] is ready, it is returned in
    /// `DecodeResult::Data`.
    pub(crate) fn try_build(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
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
                // Short-circuit once the overall output limit is exhausted.
                //
                // `self.limit` tracks how many more rows the reader is still
                // allowed to emit and is decremented as each row group is
                // planned in `StartData`, so `Some(0)` means earlier row
                // groups have already produced the full requested output.
                if matches!(self.limit, Some(0)) {
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        DecodeResult::Finished,
                    ));
                }

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

                if self.should_use_post_filter_fallback() {
                    if self.post_filter_read_projection(&filter).is_some() {
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
                } = row_group_info;

                // If nothing is selected, we are done with this row group
                if !plan_builder.selects_any() {
                    // ruled out entire row group
                    self.filter = Some(filter_info.into_filter());
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        DecodeResult::Finished,
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
                        DecodeResult::NeedsData(needed_ranges),
                    ));
                }

                // otherwise we have all the data we need to evaluate the predicate
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    mut plan_builder,
                    base_selection,
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
                let predicate_limit = self
                    .limit
                    .filter(|_| filter_info.is_last())
                    .map(|l| l.saturating_add(self.offset.unwrap_or(0)));

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
                } = row_group_info;

                // Compute the number of rows in the selection before applying limit and offset
                let rows_before = plan_builder.num_rows_selected().unwrap_or(row_count);

                if rows_before == 0 {
                    // ruled out entire row group
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        DecodeResult::Finished,
                    ));
                }

                // Apply any limit and offset
                let mut plan_builder = plan_builder
                    .limited(row_count)
                    .with_offset(self.offset)
                    .with_limit(self.limit)
                    .build_limited();

                let rows_after = plan_builder.num_rows_selected().unwrap_or(row_count);

                // Update running offset and limit for after the current row group is read
                if let Some(offset) = &mut self.offset {
                    // Reduction is either because of offset or limit, as limit is applied
                    // after offset has been "exhausted" can just use saturating sub here
                    *offset = offset.saturating_sub(rows_before - rows_after)
                }

                if rows_after == 0 {
                    // no rows left after applying limit/offset
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        DecodeResult::Finished,
                    ));
                }

                if let Some(limit) = &mut self.limit {
                    *limit -= rows_after;
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
                        DecodeResult::NeedsData(needed_ranges),
                    ));
                }

                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    base_selection: _,
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

                NextState::result(RowGroupDecoderState::Finished, DecodeResult::Data(reader))
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
                        DecodeResult::NeedsData(needed_ranges),
                    ));
                }

                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    base_selection: _,
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

                NextState::result(RowGroupDecoderState::Finished, DecodeResult::Data(reader))
            }
            // Waiting on data to proceed with reading the output
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
            } => {
                if cache_info.is_some()
                    && matches!(self.fallback_state, RowGroupFallbackState::Observing { .. })
                    && self.post_filter_fallback_supported()
                {
                    let decision = row_group_info
                        .plan_builder
                        .resolve_selection_strategy_decision();
                    let fallback_selection = row_group_info.plan_builder.selection().cloned();
                    self.observe_fallback_candidate(decision, row_group_info.row_count);

                    if matches!(
                        self.fallback_state,
                        RowGroupFallbackState::UsePostFilter { .. }
                    ) {
                        if row_group_info.base_selection.is_none() {
                            let selection = fallback_selection.unwrap_or_else(|| {
                                RowSelection::from(vec![RowSelector::select(
                                    row_group_info.row_count,
                                )])
                            });
                            let column_chunks = data_request.into_dense_column_chunks();
                            // Sparse predicate chunks may not cover the base
                            // selection. Dense chunks are safe to reuse and
                            // preserve predicate-cache IO behavior.
                            return self.start_post_selection_filter(
                                row_group_info,
                                selection,
                                cache_info,
                                column_chunks,
                            );
                        }

                        if self.post_filter.is_none() {
                            let filter = self.filter.take().ok_or_else(|| {
                                ParquetError::General(
                                    "post-filter fallback selected without a row filter"
                                        .to_string(),
                                )
                            })?;
                            self.post_filter = Some(Arc::new(Mutex::new(filter)));
                        }
                    }

                    self.metrics
                        .record_fallback_row_group(RowGroupExecutionMode::Pushdown(
                            decision.strategy,
                        ));
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
                        DecodeResult::NeedsData(needed_ranges),
                    ));
                }

                // otherwise we have all the data we need to proceed
                let RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                    base_selection: _,
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
                NextState::result(RowGroupDecoderState::Finished, DecodeResult::Data(reader))
            }
            RowGroupDecoderState::Finished => {
                // nothing left to read
                NextState::result(RowGroupDecoderState::Finished, DecodeResult::Finished)
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
            ..
        } = row_group_info;

        let mut plan_builder = ReadPlanBuilder::new(self.batch_size)
            .with_selection(base_selection)
            .with_row_selection_policy(self.row_selection_policy);

        if !plan_builder.selects_any() {
            return Ok(NextState::result(
                RowGroupDecoderState::Finished,
                DecodeResult::Finished,
            ));
        }

        let read_projection = {
            let filter = filter.lock().map_err(|_| {
                ParquetError::General("post-filter predicate state was poisoned".to_string())
            })?;
            self.post_filter_read_projection(&filter).ok_or_else(|| {
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

    fn should_use_post_filter_fallback(&self) -> bool {
        matches!(
            self.fallback_state,
            RowGroupFallbackState::UsePostFilter { .. }
        ) && self.post_filter_fallback_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && self.limit.is_none()
            && self.offset.is_none()
            && !self.has_virtual_columns()
    }

    fn post_filter_read_projection(&self, filter: &RowFilter) -> Option<ProjectionMask> {
        if !self.should_use_post_filter_fallback() {
            return None;
        }

        self.build_post_filter_read_projection(filter)
    }

    fn build_post_filter_read_projection(&self, filter: &RowFilter) -> Option<ProjectionMask> {
        let mut read_projection = self.projection.clone();
        read_projection.union(&filter.union_projection()?);

        if self.post_filter_supports_projection(&read_projection) {
            Some(read_projection)
        } else {
            None
        }
    }

    fn post_filter_supports_projection(&self, projection: &ProjectionMask) -> bool {
        let schema = self.metadata.file_metadata().schema_descr();
        (0..schema.num_columns()).all(|leaf_idx| {
            !projection.leaf_included(leaf_idx) || schema.get_column_root(leaf_idx).is_primitive()
        })
    }

    fn observe_fallback_candidate(
        &mut self,
        decision: RowSelectionStrategyDecision,
        row_count: usize,
    ) {
        if !matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. }) {
            return;
        }

        let RowGroupFallbackState::Observing { observation } = &mut self.fallback_state else {
            return;
        };

        let mut shape = decision.shape;
        if shape.total_rows() == 0 {
            shape = RowSelectionShape {
                selected_rows: row_count,
                skipped_rows: 0,
                selector_count: 1,
                selected_run_count: 1,
                skipped_run_count: 0,
            };
        }

        observation.observed_row_groups += 1;
        observation.shape.add_assign(shape);
        self.metrics.record_fallback_observed_row_group();

        let reason = observation.trigger_reason();
        if matches!(reason, FallbackTriggerReason::ObservationIncomplete) {
            self.metrics.record_fallback_trigger(reason);
            return;
        }

        let should_fallback = observation.should_fallback();
        self.metrics.record_fallback_trigger(reason);

        if should_fallback && self.post_filter_fallback_supported() {
            self.fallback_state = RowGroupFallbackState::UsePostFilter { reason };
        } else {
            self.fallback_state = RowGroupFallbackState::UsePushdown;
        }
    }

    fn post_filter_fallback_supported(&self) -> bool {
        let Some(filter) = self.filter.as_ref() else {
            return false;
        };
        self.post_filter_fallback_enabled
            && matches!(self.row_selection_policy, RowSelectionPolicy::Auto { .. })
            && self.limit.is_none()
            && self.offset.is_none()
            && !self.has_virtual_columns()
            && self.build_post_filter_read_projection(filter).is_some()
    }

    fn has_virtual_columns(&self) -> bool {
        self.fields
            .as_deref()
            .is_some_and(parquet_field_has_virtual_columns)
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

fn parquet_field_has_virtual_columns(field: &ParquetField) -> bool {
    match &field.field_type {
        ParquetFieldType::Primitive { .. } => false,
        ParquetFieldType::Group { children } => {
            children.iter().any(parquet_field_has_virtual_columns)
        }
        ParquetFieldType::Virtual(_) => true,
    }
}

#[cfg(test)]
fn resolve_selection_policy_for_projection(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> ReadPlanBuilder {
    resolve_selection_policy_for_expensive_output(
        plan_builder,
        projection_mask,
        offset_index,
        total_rows,
        ExpensiveOutputProfile::default(),
    )
}

fn resolve_selection_policy_for_expensive_output(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
    output_profile: ExpensiveOutputProfile,
) -> ReadPlanBuilder {
    let loaded = loaded_ranges_for_projection(
        plan_builder.selection(),
        projection_mask,
        offset_index,
        total_rows,
    );
    let loaded_is_sparse = loaded.as_ref().is_some_and(LoadedRowRanges::is_sparse);
    let sparse_loaded = loaded.filter(LoadedRowRanges::is_sparse);

    match plan_builder.row_selection_policy() {
        RowSelectionPolicy::Auto { .. } => {
            let decision = plan_builder.resolve_selection_strategy_decision();
            match decision.strategy {
                RowSelectionStrategy::Mask
                    if loaded_is_sparse
                        || should_prefer_selectors_for_expensive_output(
                            decision.shape,
                            output_profile,
                        ) =>
                {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
                }
                RowSelectionStrategy::Mask => {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Mask)
                }
                RowSelectionStrategy::Selectors => {
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
                }
            }
        }
        RowSelectionPolicy::Mask => plan_builder.with_loaded_row_ranges(sparse_loaded),
        RowSelectionPolicy::Selectors => plan_builder,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ExpensiveOutputProfile {
    variable_width_columns: usize,
    uncompressed_bytes_per_row: f64,
}

impl ExpensiveOutputProfile {
    fn from_row_group(
        row_group: &RowGroupMetaData,
        projection_mask: &ProjectionMask,
        total_rows: usize,
    ) -> Self {
        if total_rows == 0 {
            return Self::default();
        }

        let mut variable_width_columns = 0;
        let mut uncompressed_bytes = 0u64;
        for leaf_idx in 0..row_group.num_columns() {
            if !projection_mask.leaf_included(leaf_idx) {
                continue;
            }

            let column = row_group.column(leaf_idx);
            if column.column_type() == PhysicalType::BYTE_ARRAY {
                variable_width_columns += 1;
            }
            uncompressed_bytes += column.uncompressed_size().max(0) as u64;
        }

        Self {
            variable_width_columns,
            uncompressed_bytes_per_row: uncompressed_bytes as f64 / total_rows as f64,
        }
    }
}

fn should_prefer_selectors_for_expensive_output(
    shape: RowSelectionShape,
    output_profile: ExpensiveOutputProfile,
) -> bool {
    let selected_ratio = shape.selected_ratio();
    output_profile.variable_width_columns > 0
        && output_profile.uncompressed_bytes_per_row >= 16.0
        && selected_ratio > 0.0
        && selected_ratio < 0.10
        && shape.average_selected_run_length() <= 4.0
}

fn loaded_ranges_for_projection(
    selection: Option<&RowSelection>,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> Option<LoadedRowRanges> {
    let selection = selection?;
    let columns = offset_index?;
    let mut ranges: Option<Vec<Range<usize>>> = None;

    for (leaf_idx, column) in columns.iter().enumerate() {
        if !projection_mask.leaf_included(leaf_idx) {
            continue;
        }
        let column_ranges = selection.selected_page_row_ranges(column.page_locations(), total_rows);
        ranges = Some(match ranges {
            Some(existing) => intersect_ranges(existing, column_ranges),
            None => column_ranges,
        });
    }

    ranges.map(|ranges| LoadedRowRanges::new(coalesce_adjacent_ranges(ranges), total_rows))
}

fn intersect_ranges(left: Vec<Range<usize>>, right: Vec<Range<usize>>) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() && right_idx < right.len() {
        let l = &left[left_idx];
        let r = &right[right_idx];
        let start = l.start.max(r.start);
        let end = l.end.min(r.end);

        if start < end {
            out.push(start..end);
        }

        if l.end <= r.end {
            left_idx += 1;
        } else {
            right_idx += 1;
        }
    }

    out
}

fn coalesce_adjacent_ranges(ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if last.end == range.start {
                last.end = range.end;
                continue;
            }
        }
        out.push(range);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(std::mem::size_of::<RowGroupDecoderState>(), 256);
    }
}
