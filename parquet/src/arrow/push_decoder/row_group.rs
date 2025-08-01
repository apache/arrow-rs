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

use crate::arrow::array_reader::ArrayReaderBuilder;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, ReadPlanBuilder, RowFilter, RowSelection,
};
use crate::arrow::in_memory_row_group::{ColumnChunkData, FetchRanges, InMemoryRowGroup};
use crate::arrow::schema::ParquetField;
use crate::arrow::ProjectionMask;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::ChunkReader;
use crate::util::push_buffers::PushBuffers;
use crate::DecodeResult;
use bytes::Bytes;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

/// Builder that encapsulates state machine for decoding data is needed
/// to begin reading the next chunk of rows from a Parquet file.
///
/// This is typically a row group but the author has aspirations to
/// extend the pattern to data boundaries other than RowGroups in the future.
#[derive(Debug)]
pub(crate) struct RowGroupReaderBuilder {
    /// The underlying Parquet metadata
    parquet_metadata: Arc<ParquetMetaData>,

    /// The row groups that have not yet been read
    row_groups: VecDeque<usize>,

    /// Remaining selection to apply to the next row groups
    selection: Option<RowSelection>,

    /// State for building the reader for the current row group
    factory: InnerRowGroupReaderBuilder,
}

impl RowGroupReaderBuilder {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        file_len: u64,
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        projection: ProjectionMask,
        batch_size: usize,
        selection: Option<RowSelection>,
        fields: Option<Arc<ParquetField>>,
        filter: Option<RowFilter>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        let buffers = PushBuffers::new(file_len);
        let factory = InnerRowGroupReaderBuilder::new(
            batch_size,
            projection,
            Arc::clone(&parquet_metadata),
            fields,
            filter,
            limit,
            offset,
            buffers,
        );
        Self {
            parquet_metadata,
            row_groups: VecDeque::from(row_groups),
            selection,
            factory,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.factory.buffers.push_ranges(ranges, buffers);
    }

    /// Return the total number of bytes buffered so far
    pub fn buffered_bytes(&self) -> u64 {
        self.factory.buffers.buffered_bytes()
    }

    /// returns [`ParquetRecordBatchReader`] suitable for reading the next
    /// group of rows from the Parquet data, or the list of data ranges still
    /// needed to proceed
    pub fn try_next_reader(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        loop {
            // Are we ready yet to start reading?
            let result: DecodeResult<ParquetRecordBatchReader> = self.factory.try_build()?;
            match result {
                DecodeResult::Finished => {
                    // reader is done, proceed to the next row group
                    // fall through to the next row group
                    // This happens if the row group was completely filtered out
                }
                DecodeResult::NeedsData(ranges) => {
                    // need more data to proceed
                    return Ok(DecodeResult::NeedsData(ranges));
                }
                DecodeResult::Data(batch_reader) => {
                    // ready to read the row group
                    return Ok(DecodeResult::Data(batch_reader));
                }
            }

            // No current reader, proceed to the next row group if any
            let row_group_idx = match self.row_groups.pop_front() {
                None => return Ok(DecodeResult::Finished),
                Some(idx) => idx,
            };

            let row_count: usize = self
                .parquet_metadata
                .row_group(row_group_idx)
                .num_rows()
                .try_into()
                .map_err(|e| ParquetError::General(format!("Row count overflow: {e}")))?;

            let selection = self.selection.as_mut().map(|s| s.split_off(row_count));
            self.factory
                .next_row_group(row_group_idx, row_count, selection)?;
            // the next iteration will try to build the reader for the new row group
        }
    }
}

/// Identifies the current row group being read and what
/// the read plan is for it
#[derive(Debug)]
struct RowGroupInfo {
    row_group_idx: usize,
    row_count: usize,
    plan_builder: ReadPlanBuilder,
}

#[derive(Debug)]
struct FilterInfo {
    /// The predicates to evaluate, in order
    ///
    /// These must be owned by FilterInfo because they may be mutated as part of
    /// evaluation
    filter: RowFilter,
    /// The next filter to be evaluated
    next_predicate: NonZeroUsize,
}

enum AdvanceResult {
    /// advanced to the next predicate
    Continue(FilterInfo),
    /// no more predicates returns the row filter
    Done(RowFilter),
}
impl FilterInfo {
    /// Create a new FilterInfo if there are any predicates to evaluate
    ///
    /// returns None if there are no predicates in the filter
    fn try_new(filter: RowFilter) -> Option<Self> {
        if filter.predicates.is_empty() {
            None
        } else {
            Some(Self {
                filter,
                next_predicate: NonZeroUsize::new(1).expect("1 is always non-zero"),
            })
        }
    }

    /// Advance to the next predicate, returning either the updated FilterInfo
    /// or the completed RowFilter if there are no more predicates
    pub fn advance(mut self) -> AdvanceResult {
        if self.next_predicate.get() >= self.filter.predicates.len() {
            AdvanceResult::Done(self.filter)
        } else {
            self.next_predicate = self
                .next_predicate
                .checked_add(1)
                .expect("no usize overflow");
            AdvanceResult::Continue(self)
        }
    }

    /// Return the current predicate to evaluate, mutablely
    /// Panics if done() is true
    pub fn current_mut(&mut self) -> &mut dyn ArrowPredicate {
        self.filter
            .predicates
            .get_mut(self.next_predicate.get() - 1)
            .expect("current predicate out of bounds")
            .as_mut()
    }

    /// Return the current predicate to evaluate
    /// Panics if done() is true
    pub fn current(&self) -> &dyn ArrowPredicate {
        self.filter
            .predicates
            .get(self.next_predicate.get() - 1)
            .expect("current predicate out of bounds")
            .as_ref()
    }

    /// Returns the inner filter, consuming this FilterInfo
    pub fn into_filter(self) -> RowFilter {
        self.filter
    }
}

/// Contains in-progress state to construct InMemoryRowGroups
#[derive(Debug)]
struct DataRequest {
    /// Any previously read column chunk data
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    /// The ranges of data that are needed next
    ranges: Vec<Range<u64>>,
    /// Optional page start offsets each requested range. This is used
    /// to create the relevant InMemoryRowGroup
    page_start_offsets: Option<Vec<Vec<u64>>>,
}

impl DataRequest {
    /// return what ranges are still needed to satisfy this request. Returns an empty vec
    /// if all ranges are satisfied
    pub fn needed_ranges(&self, buffers: &PushBuffers) -> Vec<Range<u64>> {
        self.ranges
            .iter()
            .filter(|&range| !buffers.has_range(range))
            .cloned()
            .collect()
    }

    /// Returns the chunks from the buffers that satisfy this request
    fn get_chunks(&self, buffers: &PushBuffers) -> Result<Vec<Bytes>, ParquetError> {
        self.ranges
            .iter()
            .map(|range| {
                let length: usize = (range.end - range.start)
                    .try_into()
                    .expect("overflow for offset");
                // should have all the data due to the check above
                buffers.get_bytes(range.start, length).map_err(|e| {
                    ParquetError::General(format!(
                        "Internal Error missing data for range {range:?} in buffers: {e}",
                    ))
                })
            })
            .collect()
    }

    /// Creates a new DataRequest for the specified row group, with no
    /// previously read column chunks
    pub fn new(
        row_group_idx: usize,
        row_count: usize,
        parquet_metadata: &ParquetMetaData,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
    ) -> Self {
        let row_group_meta_data = parquet_metadata.row_group(row_group_idx);

        // If no previously read column chunks are provided, create a new location to hold them
        let column_chunks =
            column_chunks.unwrap_or_else(|| vec![None; row_group_meta_data.columns().len()]);

        // Create an InMemoryRowGroup to hold the column chunks, this is a
        // temporary structure used to tell the ArrowReaders what pages are
        // needed for decoding
        let row_group = InMemoryRowGroup {
            row_count,
            column_chunks,
            offset_index: Self::get_offset_index(parquet_metadata, row_group_idx),
            row_group_idx,
            metadata: parquet_metadata,
        };

        let FetchRanges {
            ranges,
            page_start_offsets,
        } = row_group.fetch_ranges(projection, selection);

        Self {
            // Save any previously read column chunks
            column_chunks: row_group.column_chunks,
            ranges,
            page_start_offsets,
        }
    }

    /// Create a new InMemoryRowGroup, and fill it with provided data
    ///
    /// Assumes that all needed data is present in the buffers
    /// and clears any explicitly requested ranges
    pub fn try_into_in_memory_row_group<'a>(
        self,
        row_group_idx: usize,
        row_count: usize,
        parquet_metadata: &'a ParquetMetaData,
        projection: &ProjectionMask,
        buffers: &mut PushBuffers,
    ) -> Result<InMemoryRowGroup<'a>, ParquetError> {
        let chunks = self.get_chunks(buffers)?;

        let Self {
            column_chunks,
            ranges,
            page_start_offsets,
        } = self;

        // Create an InMemoryRowGroup to hold the column chunks, this is a
        // temporary structure used to tell the ArrowReaders what pages are
        // needed for decoding
        let mut in_memory_row_group = InMemoryRowGroup {
            row_count,
            column_chunks,
            offset_index: Self::get_offset_index(parquet_metadata, row_group_idx),
            row_group_idx,
            metadata: parquet_metadata,
        };

        in_memory_row_group.fill_column_chunks(projection, page_start_offsets, chunks);

        // Clear the ranges that were explicitly requested
        buffers.clear_ranges(&ranges);

        Ok(in_memory_row_group)
    }

    fn get_offset_index(
        parquet_metadata: &ParquetMetaData,
        row_group_idx: usize,
    ) -> Option<&[OffsetIndexMetaData]> {
        parquet_metadata
            .offset_index()
            // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
            .filter(|index| !index.is_empty())
            .map(|x| x[row_group_idx].as_slice())
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

/// State required for decoding a single RowGroup

#[derive(Debug)]
struct InnerRowGroupReaderBuilder {
    batch_size: usize,

    projection: ProjectionMask,

    metadata: Arc<ParquetMetaData>,

    /// Top level parquet schema and arrow schema mapping
    fields: Option<Arc<ParquetField>>,

    /// Optional filter
    filter: Option<RowFilter>,

    /// Limit to apply to remaining row groups.
    limit: Option<usize>,

    /// Offset to apply to remaining row groups.
    offset: Option<usize>,

    /// Current state of the decoder.
    ///
    /// It is taken when processing, and must be put back before returning
    /// it is a bug error if it is not put back after transitioning states.
    state: Option<RowGroupDecoderState>,

    /// The underlying data store
    buffers: PushBuffers,
}

impl InnerRowGroupReaderBuilder {
    /// Create a new RowGroupReaderBuilder
    #[expect(clippy::too_many_arguments)]
    fn new(
        batch_size: usize,
        projection: ProjectionMask,
        metadata: Arc<ParquetMetaData>,
        fields: Option<Arc<ParquetField>>,
        filter: Option<RowFilter>,
        limit: Option<usize>,
        offset: Option<usize>,
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
            state: Some(RowGroupDecoderState::Finished),
            buffers,
        }
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
    fn next_row_group(
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
    fn try_build(&mut self) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        // Note: All return paths must set `self.state` prior to returning
        // otherwise the next call to try_build will error
        loop {
            match self.take_state()? {
                RowGroupDecoderState::Start { row_group_info } => {
                    let column_chunks = None; // no prior column chunks

                    let Some(filter) = self.filter.take() else {
                        // no filter, start trying to read data immediately
                        self.state = Some(RowGroupDecoderState::StartData {
                            row_group_info,
                            column_chunks,
                        });
                        continue;
                    };
                    let Some(filter_info) = FilterInfo::try_new(filter) else {
                        // no predicates in filter, so start reading immediate
                        self.state = Some(RowGroupDecoderState::StartData {
                            row_group_info,
                            column_chunks,
                        });
                        continue;
                    };
                    // we have predicates to evaluate
                    self.state = Some(RowGroupDecoderState::Filters {
                        row_group_info,
                        filter_info,
                        column_chunks,
                    });
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
                        self.state = Some(RowGroupDecoderState::Finished);
                        self.filter = Some(filter_info.into_filter());
                        return Ok(DecodeResult::Finished);
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

                    self.state = Some(RowGroupDecoderState::WaitingOnFilterData {
                        row_group_info,
                        filter_info,
                        data_request,
                    });
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
                        self.state = Some(RowGroupDecoderState::WaitingOnFilterData {
                            row_group_info,
                            filter_info,
                            data_request,
                        });
                        return Ok(DecodeResult::NeedsData(needed_ranges));
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

                    let array_reader = ArrayReaderBuilder::new(&row_group)
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
                            self.state = Some(RowGroupDecoderState::Filters {
                                row_group_info,
                                column_chunks,
                                filter_info,
                            });
                        }
                        // done with predicates, proceed to reading data
                        AdvanceResult::Done(filter) => {
                            // remember we need to put back the filter
                            assert!(self.filter.is_none());
                            self.filter = Some(filter);
                            self.state = Some(RowGroupDecoderState::StartData {
                                row_group_info,
                                column_chunks,
                            });
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
                        self.state = Some(RowGroupDecoderState::Finished);
                        return Ok(DecodeResult::Finished); // ruled out entire row group
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
                        self.state = Some(RowGroupDecoderState::Finished);
                        return Ok(DecodeResult::Finished); // ruled out entire row group
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

                    self.state = Some(RowGroupDecoderState::WaitingOnData {
                        row_group_info,
                        data_request,
                    });
                }
                // Waiting on data to proceed with reading the output
                RowGroupDecoderState::WaitingOnData {
                    row_group_info,
                    data_request,
                } => {
                    let needed_ranges = data_request.needed_ranges(&self.buffers);
                    if !needed_ranges.is_empty() {
                        // still need data
                        self.state = Some(RowGroupDecoderState::WaitingOnData {
                            row_group_info,
                            data_request,
                        });
                        return Ok(DecodeResult::NeedsData(needed_ranges));
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

                    let array_reader = ArrayReaderBuilder::new(&row_group)
                        .build_array_reader(self.fields.as_deref(), &self.projection)?;

                    let reader = ParquetRecordBatchReader::new(array_reader, plan);
                    self.state = Some(RowGroupDecoderState::Finished); // done with this row group
                    return Ok(DecodeResult::Data(reader));
                }
                RowGroupDecoderState::Finished => {
                    // nothing left to read
                    self.state = Some(RowGroupDecoderState::Finished);
                    return Ok(DecodeResult::Finished);
                }
            }
        }
    }
}
