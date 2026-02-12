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
use crate::arrow::array_reader::{ArrayReader, ArrayReaderBuilder, CacheMode, RowGroupCache};
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, PredicateBatchObserver, PushdownFilterEvalMode,
    ReadPlanBuilder, RowFilter, RowSelection, RowSelectionPolicy,
};
use crate::arrow::in_memory_row_group::ColumnChunkData;
use crate::arrow::push_decoder::reader_builder::data::DataRequestBuilder;
use crate::arrow::push_decoder::reader_builder::filter::CacheInfo;
use crate::arrow::schema::{ParquetField, ParquetFieldType};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::util::push_buffers::PushBuffers;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray, new_empty_array};
use arrow_schema::DataType as ArrowType;
use arrow_select::filter::{filter, filter_record_batch, prep_null_mask_filter};
use bytes::Bytes;
use data::DataRequest;
use filter::AdvanceResult;
use filter::FilterInfo;
use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, RwLock};

/// The current row group being read and the read plan
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
}

/// Observer that stores exact selected values for one cached column.
struct ExactSelectedObserver {
    cache: Arc<RwLock<RowGroupCache>>,
    column_idx: usize,
}

impl PredicateBatchObserver for ExactSelectedObserver {
    fn observe(
        &mut self,
        batch: &RecordBatch,
        filter_values: &BooleanArray,
    ) -> crate::errors::Result<()> {
        if batch.num_columns() != 1 {
            return Ok(());
        }
        let selected = filter(batch.column(0).as_ref(), filter_values)?;
        let _inserted = self
            .cache
            .write()
            .unwrap()
            .insert_selected(self.column_idx, selected);
        Ok(())
    }
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
    WaitingOnData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
        /// Any cached filter results
        cache_info: Option<CacheInfo>,
    },
    /// Finished (or not yet started) reading this group
    Finished,
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

    /// How pushed-down predicates are evaluated during filtering.
    pushdown_filter_eval_mode: PushdownFilterEvalMode,

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
        pushdown_filter_eval_mode: PushdownFilterEvalMode,
    ) -> Self {
        Self {
            batch_size,
            projection,
            metadata,
            fields,
            filter,
            limit,
            offset,
            metrics,
            max_predicate_cache_size,
            row_selection_policy,
            pushdown_filter_eval_mode,
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
            .with_selection(selection)
            .with_row_selection_policy(self.row_selection_policy);

        let row_group_info = RowGroupInfo {
            row_group_idx,
            row_count,
            plan_builder,
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
                };

                // We have predicates to evaluate. Decide whether to keep the
                // existing "predicate->selection->second decode" flow, or use a
                // projection-driven single-pass flow that can avoid the second decode.
                let use_projection_filter_eval =
                    self.use_projection_filter_eval(row_group_info.row_group_idx, &filter);
                let cache_projection = if use_projection_filter_eval {
                    self.compute_projection_filter_eval_cache_projection(
                        row_group_info.row_group_idx,
                    )
                } else {
                    self.compute_cache_projection(row_group_info.row_group_idx, &filter)
                };
                let exact_selected_column = if use_projection_filter_eval {
                    None
                } else {
                    self.compute_exact_selected_column(
                        row_group_info.row_group_idx,
                        &filter,
                        &cache_projection,
                    )
                };
                let cache_mode = if exact_selected_column.is_some() {
                    CacheMode::ExactSelected
                } else {
                    CacheMode::Raw
                };

                let cache_info = CacheInfo::new(
                    cache_projection,
                    Arc::new(RwLock::new(RowGroupCache::new(
                        self.batch_size,
                        self.max_predicate_cache_size,
                    ))),
                    cache_mode,
                    exact_selected_column,
                    use_projection_filter_eval,
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
                let predicate_input_projection = if filter_info.use_projection_filter_eval() {
                    self.projection.clone()
                } else {
                    predicate.projection().clone()
                };

                // need to fetch pages the column needs for decoding, figure
                // that out based on the current selection and projection
                let data_request = DataRequestBuilder::new(
                    row_group_idx,
                    row_count,
                    self.batch_size,
                    &self.metadata,
                    &predicate_input_projection,
                )
                .with_selection(plan_builder.selection())
                // Expand the fetched selection for cache columns when caching is enabled.
                .with_cache_projection(Some(filter_info.cache_projection()))
                .with_column_chunks(column_chunks)
                .build();

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
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
                } = row_group_info;

                let predicate = filter_info.current();
                let predicate_input_projection = if filter_info.use_projection_filter_eval() {
                    self.projection.clone()
                } else {
                    predicate.projection().clone()
                };

                let row_group = data_request.try_into_in_memory_row_group(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    &predicate_input_projection,
                    &mut self.buffers,
                )?;

                if filter_info.use_projection_filter_eval() {
                    // Single-pass path:
                    // 1) decode the output projection once
                    // 2) evaluate predicates against that decoded batch
                    // 3) keep only selected rows and return directly
                    //
                    // Rationale: for overlap-heavy shapes (for example, filtering and
                    // projecting the same column), this avoids the second decode pass and
                    // can remove substantial cache/selection orchestration overhead.
                    let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
                        .with_parquet_metadata(&self.metadata)
                        .build_array_reader(self.fields.as_deref(), &self.projection)?;
                    let predicate_rows_reader =
                        ParquetRecordBatchReader::new(array_reader, plan_builder.clone().build());
                    let mut filter = filter_info.into_filter();
                    let predicate_column_indices: Vec<Option<Vec<usize>>> = filter
                        .predicates
                        .iter()
                        .map(|predicate| {
                            self.projection_indices_for_predicate(
                                &self.projection,
                                predicate.projection(),
                            )
                        })
                        .collect::<Result<_, _>>()?;

                    let mut selected_batches = Vec::new();
                    for maybe_batch in predicate_rows_reader {
                        let mut batch = maybe_batch?;
                        for (predicate, projection_indices) in filter
                            .predicates
                            .iter_mut()
                            .zip(predicate_column_indices.iter())
                        {
                            if batch.num_rows() == 0 {
                                break;
                            }
                            let predicate_batch = match projection_indices.as_ref() {
                                Some(indices) => {
                                    self.project_batch_for_predicate(&batch, indices)?
                                }
                                None => batch.clone(),
                            };
                            let mut mask = predicate.evaluate(predicate_batch)?;
                            if mask.len() != batch.num_rows() {
                                return Err(ParquetError::General(format!(
                                    "ArrowPredicate returned {} rows, expected {}",
                                    mask.len(),
                                    batch.num_rows()
                                )));
                            }
                            if mask.null_count() > 0 {
                                mask = prep_null_mask_filter(&mask);
                            }
                            if mask.true_count() == 0 {
                                batch = batch.slice(0, 0);
                                break;
                            }
                            if mask.true_count() != batch.num_rows() {
                                batch = filter_record_batch(&batch, &mask)?;
                            }
                        }
                        if batch.num_rows() > 0 {
                            selected_batches.push(batch);
                        }
                    }

                    self.filter = Some(filter);
                    // Apply global offset/limit here because this path bypasses StartData,
                    // where those are normally enforced.
                    let selected_batches =
                        self.apply_offset_limit_to_selected_batches(selected_batches);
                    if selected_batches.is_empty() {
                        return Ok(NextState::result(
                            RowGroupDecoderState::Finished,
                            DecodeResult::Finished,
                        ));
                    }

                    let array_reader =
                        Box::new(PrecomputedStructArrayReader::try_new(selected_batches)?);
                    let reader = ParquetRecordBatchReader::new(
                        array_reader,
                        ReadPlanBuilder::new(self.batch_size).build(),
                    );
                    return Ok(NextState::result(
                        RowGroupDecoderState::Finished,
                        DecodeResult::Data(reader),
                    ));
                }

                let cache_options = filter_info.producer_cache_options();

                let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .with_cache_options(Some(&cache_options))
                    .with_parquet_metadata(&self.metadata)
                    .build_array_reader(self.fields.as_deref(), &predicate_input_projection)?;

                plan_builder = override_selector_strategy_if_needed(
                    plan_builder,
                    &predicate_input_projection,
                    self.row_group_offset_index(row_group_idx),
                );

                plan_builder = if filter_info.use_projection_filter_eval() {
                    self.with_predicate_from_projection_reader(
                        plan_builder,
                        array_reader,
                        filter_info.current_mut(),
                        &predicate_input_projection,
                    )?
                } else if let Some(column_idx) = filter_info.exact_selected_column() {
                    let mut observer = ExactSelectedObserver {
                        cache: cache_options.cache.clone(),
                        column_idx,
                    };
                    plan_builder.with_predicate_with_observer(
                        array_reader,
                        filter_info.current_mut(),
                        Some(&mut observer),
                    )?
                } else {
                    plan_builder.with_predicate(array_reader, filter_info.current_mut())?
                };

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
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

                plan_builder = if cache_info
                    .as_ref()
                    .and_then(|c| c.exact_selected_column())
                    .is_some()
                {
                    // ExactSelected cache stream is selected-row-only and requires selectors.
                    plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
                } else {
                    plan_builder.with_row_selection_policy(self.row_selection_policy)
                };

                plan_builder = override_selector_strategy_if_needed(
                    plan_builder,
                    &self.projection,
                    self.row_group_offset_index(row_group_idx),
                );

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                };

                NextState::again(RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                    cache_info,
                })
            }
            // Waiting on data to proceed with reading the output
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
                cache_info,
            } => {
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
                } = row_group_info;

                let row_group = data_request.try_into_in_memory_row_group(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    &self.projection,
                    &mut self.buffers,
                )?;

                let plan = plan_builder.build();

                // if we have any cached results, connect them up
                let array_reader_builder = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .with_parquet_metadata(&self.metadata);
                let array_reader = if let Some(cache_info) = cache_info.as_ref() {
                    let cache_options = cache_info.consumer_options();
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

    fn with_predicate_from_projection_reader(
        &self,
        mut plan_builder: ReadPlanBuilder,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
        reader_projection: &ProjectionMask,
    ) -> Result<ReadPlanBuilder, ParquetError> {
        let projection_indices =
            self.projection_indices_for_predicate(reader_projection, predicate.projection())?;
        let reader = ParquetRecordBatchReader::new(array_reader, plan_builder.clone().build());
        let mut filters = Vec::new();
        for maybe_batch in reader {
            let batch = maybe_batch?;
            let input_rows = batch.num_rows();
            let predicate_batch = match projection_indices.as_ref() {
                Some(indices) => self.project_batch_for_predicate(&batch, indices)?,
                None => batch.clone(),
            };
            let filter = predicate.evaluate(predicate_batch)?;
            if filter.len() != input_rows {
                return Err(ParquetError::General(format!(
                    "ArrowPredicate returned {} rows, expected {input_rows}",
                    filter.len()
                )));
            }
            let filter = match filter.null_count() {
                0 => filter,
                _ => prep_null_mask_filter(&filter),
            };
            filters.push(filter);
        }

        let raw = RowSelection::from_filters(&filters);
        let selection = match plan_builder.selection().cloned() {
            Some(selection) => selection.and_then(&raw),
            None => raw,
        };
        plan_builder = plan_builder.with_selection(Some(selection));
        Ok(plan_builder)
    }

    fn projection_indices_for_predicate(
        &self,
        reader_projection: &ProjectionMask,
        predicate_projection: &ProjectionMask,
    ) -> Result<Option<Vec<usize>>, ParquetError> {
        if reader_projection == predicate_projection {
            return Ok(None);
        }

        let num_leaves = self.metadata.file_metadata().schema_descr().num_columns();
        let reader_leaves = Self::leaf_indices(reader_projection, num_leaves);
        let predicate_leaves = Self::leaf_indices(predicate_projection, num_leaves);

        let mut index_by_leaf = HashMap::with_capacity(reader_leaves.len());
        for (idx, leaf_idx) in reader_leaves.iter().enumerate() {
            index_by_leaf.insert(*leaf_idx, idx);
        }

        let mut projection_indices = Vec::with_capacity(predicate_leaves.len());
        for leaf_idx in predicate_leaves {
            let Some(idx) = index_by_leaf.get(&leaf_idx).copied() else {
                return Err(ParquetError::General(format!(
                    "Predicate leaf {leaf_idx} not present in reader projection"
                )));
            };
            projection_indices.push(idx);
        }
        Ok(Some(projection_indices))
    }

    fn project_batch_for_predicate(
        &self,
        batch: &RecordBatch,
        projection_indices: &[usize],
    ) -> Result<RecordBatch, ParquetError> {
        let projected_schema = Arc::new(batch.schema().project(projection_indices)?);
        let projected_columns = projection_indices
            .iter()
            .map(|idx| batch.column(*idx).clone())
            .collect();
        Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
    }

    fn leaf_indices(mask: &ProjectionMask, num_leaves: usize) -> Vec<usize> {
        (0..num_leaves)
            .filter(|idx| mask.leaf_included(*idx))
            .collect()
    }

    fn use_projection_filter_eval(&self, row_group_idx: usize, filter: &RowFilter) -> bool {
        let default_auto_mode = matches!(
            self.pushdown_filter_eval_mode,
            PushdownFilterEvalMode::RowSelection
        );
        let mode_enabled = match self.pushdown_filter_eval_mode {
            PushdownFilterEvalMode::DecodeProjectionThenFilter => true,
            // In default row-selection mode, auto-enable only for single-predicate
            // queries. This keeps behavior conservative while still unlocking the
            // no-second-decode optimization for common regression shapes.
            PushdownFilterEvalMode::RowSelection => filter.predicates.len() == 1,
        };
        if !mode_enabled {
            return false;
        }
        // Virtual output columns (for example `row_number`) rely on synthetic
        // readers and currently require the existing row-selection flow.
        if self.has_virtual_output_columns() {
            return false;
        }
        if !self.is_non_nested_projection(&self.projection) {
            return false;
        }

        let num_leaves = self.metadata.row_group(row_group_idx).columns().len();
        let output_leaves: HashMap<usize, ()> = Self::leaf_indices(&self.projection, num_leaves)
            .into_iter()
            .map(|idx| (idx, ()))
            .collect();

        filter.predicates.iter().all(|predicate| {
            let projection = predicate.projection();
            let predicate_leaves = Self::leaf_indices(projection, num_leaves);
            self.is_non_nested_projection(projection)
                && predicate_leaves
                    .iter()
                    .all(|idx| output_leaves.contains_key(idx))
                // Default auto-mode should remain conservative: only enable
                // when predicate/output leaves are exactly the same shape.
                && (!default_auto_mode
                    || (predicate_leaves.len() == output_leaves.len()
                        && output_leaves
                            .keys()
                            .all(|idx| predicate_leaves.contains(idx))))
        })
    }

    fn has_virtual_output_columns(&self) -> bool {
        fn contains_virtual(field: &ParquetField) -> bool {
            match &field.field_type {
                ParquetFieldType::Virtual(_) => true,
                ParquetFieldType::Group { children } => children.iter().any(contains_virtual),
                ParquetFieldType::Primitive { .. } => false,
            }
        }

        self.fields.as_deref().is_some_and(contains_virtual)
    }

    fn compute_projection_filter_eval_cache_projection(
        &self,
        row_group_idx: usize,
    ) -> ProjectionMask {
        let meta = self.metadata.row_group(row_group_idx);
        self.exclude_nested_columns_from_cache(&self.projection)
            .unwrap_or_else(|| ProjectionMask::none(meta.columns().len()))
    }

    fn is_non_nested_projection(&self, mask: &ProjectionMask) -> bool {
        self.exclude_nested_columns_from_cache(mask)
            .as_ref()
            .is_some_and(|non_nested| non_nested == mask)
    }

    fn apply_offset_limit_to_selected_batches(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Vec<RecordBatch> {
        // This helper mirrors the row-group level offset/limit behavior used by
        // ReadPlanBuilder, but operates on already materialized filtered batches.
        let mut batches = batches;
        if let Some(offset_remaining) = self.offset.as_mut() {
            if *offset_remaining > 0 {
                let mut to_skip = *offset_remaining;
                let mut after_offset = Vec::new();
                for batch in batches {
                    if to_skip == 0 {
                        after_offset.push(batch);
                        continue;
                    }
                    if to_skip >= batch.num_rows() {
                        to_skip -= batch.num_rows();
                    } else {
                        let sliced = batch.slice(to_skip, batch.num_rows() - to_skip);
                        after_offset.push(sliced);
                        to_skip = 0;
                    }
                }
                *offset_remaining = to_skip;
                batches = after_offset;
            }
        }

        if let Some(limit_remaining) = self.limit.as_mut() {
            let mut keep = *limit_remaining;
            let mut limited = Vec::new();
            for batch in batches {
                if keep == 0 {
                    break;
                }
                if batch.num_rows() <= keep {
                    keep -= batch.num_rows();
                    limited.push(batch);
                } else {
                    limited.push(batch.slice(0, keep));
                    keep = 0;
                    break;
                }
            }
            *limit_remaining = keep;
            batches = limited;
        }

        batches
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

    /// Returns the cached column index for ExactSelected mode, if applicable.
    ///
    /// This prototype is intentionally conservative:
    /// - exactly one predicate
    /// - exactly one cached leaf
    /// - predicate projection contains exactly that same leaf
    fn compute_exact_selected_column(
        &self,
        row_group_idx: usize,
        filter: &RowFilter,
        cache_projection: &ProjectionMask,
    ) -> Option<usize> {
        // ExactSelected requires all projected outputs to stay synchronized by
        // row position; virtual columns (for example `row_number`) are produced
        // by synthetic readers and are not part of this cache stream.
        if self.has_virtual_output_columns()
            || self.max_predicate_cache_size == 0
            || filter.predicates.len() != 1
        {
            return None;
        }
        let num_cols = self.metadata.row_group(row_group_idx).columns().len();
        let cached_cols: Vec<usize> = (0..num_cols)
            .filter(|idx| cache_projection.leaf_included(*idx))
            .collect();
        if cached_cols.len() != 1 {
            return None;
        }
        let predicate_cols: Vec<usize> = (0..num_cols)
            .filter(|idx| filter.predicates[0].projection().leaf_included(*idx))
            .collect();
        if predicate_cols.len() != 1 || predicate_cols[0] != cached_cols[0] {
            return None;
        }
        Some(cached_cols[0])
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

/// ArrayReader backed by precomputed filtered batches.
///
/// This is used by the single-pass filter path to return already-selected rows
/// through the same `ParquetRecordBatchReader` interface without re-decoding.
#[derive(Debug)]
struct PrecomputedStructArrayReader {
    data_type: ArrowType,
    remaining: VecDeque<ArrayRef>,
    current: Option<(ArrayRef, usize)>,
    staged: Vec<ArrayRef>,
}

impl PrecomputedStructArrayReader {
    fn try_new(batches: Vec<RecordBatch>) -> Result<Self, ParquetError> {
        if batches.is_empty() {
            return Err(ParquetError::General(
                "Precomputed reader requires at least one batch".to_string(),
            ));
        }

        let schema = batches[0].schema();
        for batch in batches.iter().skip(1) {
            if batch.schema() != schema {
                return Err(ParquetError::General(
                    "Precomputed reader requires consistent batch schemas".to_string(),
                ));
            }
        }

        let remaining = batches
            .into_iter()
            .map(|batch| Arc::new(StructArray::from(batch)) as ArrayRef)
            .collect();

        Ok(Self {
            data_type: ArrowType::Struct(schema.fields().clone()),
            remaining,
            current: None,
            staged: Vec::new(),
        })
    }
}

impl ArrayReader for PrecomputedStructArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, num_records: usize) -> Result<usize, ParquetError> {
        let mut read = 0;
        while read < num_records {
            if self.current.is_none() {
                self.current = self.remaining.pop_front().map(|array| (array, 0));
            }
            let Some((array, offset)) = self.current.as_mut() else {
                break;
            };

            let available = array.len().saturating_sub(*offset);
            if available == 0 {
                self.current = None;
                continue;
            }

            let take = (num_records - read).min(available);
            self.staged.push(array.slice(*offset, take));
            *offset += take;
            read += take;

            if *offset >= array.len() {
                self.current = None;
            }
        }
        Ok(read)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize, ParquetError> {
        let mut skipped = 0;
        while skipped < num_records {
            if self.current.is_none() {
                self.current = self.remaining.pop_front().map(|array| (array, 0));
            }
            let Some((array, offset)) = self.current.as_mut() else {
                break;
            };

            let available = array.len().saturating_sub(*offset);
            if available == 0 {
                self.current = None;
                continue;
            }

            let take = (num_records - skipped).min(available);
            *offset += take;
            skipped += take;

            if *offset >= array.len() {
                self.current = None;
            }
        }
        Ok(skipped)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef, ParquetError> {
        if self.staged.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }
        if self.staged.len() == 1 {
            return Ok(self.staged.pop().expect("len checked"));
        }
        let arrays = self.staged.drain(..).collect::<Vec<_>>();
        Ok(arrow_select::concat::concat(
            &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )?)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

/// Override the selection strategy if needed.
///
/// Some pages can be skipped during row-group construction if they are not read
/// by the selections. This means that the data pages for those rows are never
/// loaded and definition/repetition levels are never read. When using
/// `RowSelections` selection works because `skip_records()` handles this
/// case and skips the page accordingly.
///
/// However, with the current mask design, all values must be read and decoded
/// and then a mask filter is applied. Thus if any pages are skipped during
/// row-group construction, the data pages are missing and cannot be decoded.
///
/// A simple example:
/// * the page size is 2, the mask is 100001, row selection should be read(1) skip(4) read(1)
/// * the `ColumnChunkData` would be page1(10), page2(skipped), page3(01)
///
/// Using the row selection to skip(4), page2 won't be read at all, so in this
/// case we can't decode all the rows and apply a mask. To correctly apply the
/// bit mask, we need all 6 values be read, but page2 is not in memory.
fn override_selector_strategy_if_needed(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
) -> ReadPlanBuilder {
    // override only applies to Auto policy, If the policy is already Mask or Selectors, respect that
    let RowSelectionPolicy::Auto { .. } = plan_builder.row_selection_policy() else {
        return plan_builder;
    };

    let preferred_strategy = plan_builder.resolve_selection_strategy();

    let force_selectors = matches!(preferred_strategy, RowSelectionStrategy::Mask)
        && plan_builder.selection().is_some_and(|selection| {
            selection.should_force_selectors(projection_mask, offset_index)
        });

    let resolved_strategy = if force_selectors {
        RowSelectionStrategy::Selectors
    } else {
        preferred_strategy
    };

    // override the plan builder strategy with the resolved one
    let new_policy = match resolved_strategy {
        RowSelectionStrategy::Mask => RowSelectionPolicy::Mask,
        RowSelectionStrategy::Selectors => RowSelectionPolicy::Selectors,
    };

    plan_builder.with_row_selection_policy(new_policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Verify that the size of RowGroupDecoderState does not grow too large
    fn test_structure_size() {
        assert_eq!(std::mem::size_of::<RowGroupDecoderState>(), 224);
    }
}
