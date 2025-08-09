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

use crate::arrow::array_reader::ArrayReaderBuilder;
use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, ReadPlanBuilder, RowFilter, RowSelection,
};
use crate::arrow::in_memory_row_group::ColumnChunkData;
use crate::arrow::schema::ParquetField;
use crate::arrow::ProjectionMask;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::util::push_buffers::PushBuffers;
use crate::DecodeResult;
use bytes::Bytes;
use data::DataRequest;
use filter::AdvanceResult;
use filter::FilterInfo;
use std::ops::Range;
use std::sync::Arc;

/// Identifies the current row group being read and what
/// the read plan is for it
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
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
    /// Know what to actually read, after all predicates
    StartData {
        row_group_info: RowGroupInfo,
        /// Any previously read column chunk data from the filtering phase
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
    },
    /// Needs data to proceed with reading the output
    WaitingOnData {
        row_group_info: RowGroupInfo,
        data_request: DataRequest,
    },
    /// Finished (or not yet started) reading any row groups
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

    /// The metrics collector
    metrics: ArrowReaderMetrics,

    /// Maximum size of predicate cache
    #[expect(dead_code)]
    max_predicate_cache_size: usize,

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
    /// before returning, otherwise the reader error next time it is called
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
        let plan_builder = ReadPlanBuilder::new(self.batch_size).with_selection(selection.clone());
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
            match self.try_transition(current_state)? {
                NextState {
                    next_state,
                    result: Some(result),
                } => {
                    // put back the next state
                    self.state = Some(next_state);
                    return Ok(result);
                }
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
                    }));
                };
                let Some(filter_info) = FilterInfo::try_new(filter) else {
                    // no predicates in filter, so start reading immediate
                    return Ok(NextState::again(RowGroupDecoderState::StartData {
                        row_group_info,
                        column_chunks,
                    }));
                };
                // we have predicates to evaluate
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

                // need to fetch pages the column needs for decoding, figure
                // that out based on the current selection and projection
                let data_request = DataRequest::new(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    predicate.projection(), // use the predicate's projection
                    plan_builder.selection(),
                    column_chunks,
                );

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

                let predicate = filter_info.current_mut();

                let row_group = data_request.try_into_in_memory_row_group(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    predicate.projection(),
                    &mut self.buffers,
                )?;

                let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .build_array_reader(self.fields.as_deref(), predicate.projection())?;

                // Evaluate the predicate and update the plan builder
                plan_builder = plan_builder.with_predicate(array_reader, predicate)?;

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
                    AdvanceResult::Done(filter) => {
                        // remember we need to put back the filter
                        assert!(self.filter.is_none());
                        self.filter = Some(filter);
                        NextState::again(RowGroupDecoderState::StartData {
                            row_group_info,
                            column_chunks,
                        })
                    }
                }
            }
            RowGroupDecoderState::StartData {
                row_group_info,
                column_chunks,
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
                let plan_builder = plan_builder
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

                let data_request = DataRequest::new(
                    row_group_idx,
                    row_count,
                    &self.metadata,
                    &self.projection,
                    plan_builder.selection(),
                    column_chunks,
                );

                let row_group_info = RowGroupInfo {
                    row_group_idx,
                    row_count,
                    plan_builder,
                };

                NextState::again(RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                })
            }
            // Waiting on data to proceed with reading the output
            RowGroupDecoderState::WaitingOnData {
                row_group_info,
                data_request,
            } => {
                let needed_ranges = data_request.needed_ranges(&self.buffers);
                if !needed_ranges.is_empty() {
                    // still need data
                    return Ok(NextState::result(
                        RowGroupDecoderState::WaitingOnData {
                            row_group_info,
                            data_request,
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

                let array_reader = ArrayReaderBuilder::new(&row_group, &self.metrics)
                    .build_array_reader(self.fields.as_deref(), &self.projection)?;

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
}
