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

//! [`DataRequest`] tracks and holds data needed to construct InMemoryRowGroups

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::RowSelection;
use crate::arrow::in_memory_row_group::{ColumnChunkData, FetchRanges, InMemoryRowGroup};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::ChunkReader;
use crate::util::push_buffers::PushBuffers;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// Contains in-progress state to construct InMemoryRowGroups
///
/// See [`DataRequestBuilder`] for creating new requests
#[derive(Debug)]
pub(super) struct DataRequest {
    /// Any previously read column chunk data
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    /// The ranges of data that are needed next
    ranges: Vec<Range<u64>>,
    /// Optional page start offsets for each requested range. This is used
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
            offset_index: get_offset_index(parquet_metadata, row_group_idx),
            row_group_idx,
            metadata: parquet_metadata,
        };

        in_memory_row_group.fill_column_chunks(projection, page_start_offsets, chunks);

        // Clear the ranges that were explicitly requested
        buffers.clear_ranges(&ranges);

        Ok(in_memory_row_group)
    }
}

/// Builder for [`DataRequest`]
pub(super) struct DataRequestBuilder<'a> {
    /// The row group index
    row_group_idx: usize,
    /// The number of rows in the row group
    row_count: usize,
    /// The batch size to read
    batch_size: usize,
    /// The parquet metadata
    parquet_metadata: &'a ParquetMetaData,
    /// The projection mask (which columns to read)
    projection: &'a ProjectionMask,
    /// Optional row selection to apply
    selection: Option<&'a RowSelection>,
    /// Optional projection mask if using
    /// [`RowGroupCache`](crate::arrow::array_reader::RowGroupCache)
    /// for caching decoded columns.
    cache_projection: Option<&'a ProjectionMask>,
    /// Any previously read column chunks
    column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
}

impl<'a> DataRequestBuilder<'a> {
    pub(super) fn new(
        row_group_idx: usize,
        row_count: usize,
        batch_size: usize,
        parquet_metadata: &'a ParquetMetaData,
        projection: &'a ProjectionMask,
    ) -> Self {
        Self {
            row_group_idx,
            row_count,
            batch_size,
            parquet_metadata,
            projection,
            selection: None,
            cache_projection: None,
            column_chunks: None,
        }
    }

    /// Set an optional row selection to apply
    pub(super) fn with_selection(mut self, selection: Option<&'a RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    /// set columns to cache, if any
    pub(super) fn with_cache_projection(
        mut self,
        cache_projection: Option<&'a ProjectionMask>,
    ) -> Self {
        self.cache_projection = cache_projection;
        self
    }

    /// Provide any previously read column chunks
    pub(super) fn with_column_chunks(
        mut self,
        column_chunks: Option<Vec<Option<Arc<ColumnChunkData>>>>,
    ) -> Self {
        self.column_chunks = column_chunks;
        self
    }

    pub(crate) fn build(self) -> DataRequest {
        let Self {
            row_group_idx,
            row_count,
            batch_size,
            parquet_metadata,
            projection,
            selection,
            cache_projection,
            column_chunks,
        } = self;

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
            offset_index: get_offset_index(parquet_metadata, row_group_idx),
            row_group_idx,
            metadata: parquet_metadata,
        };

        let FetchRanges {
            ranges,
            page_start_offsets,
        } = row_group.fetch_ranges(projection, selection, batch_size, cache_projection);

        DataRequest {
            // Save any previously read column chunks
            column_chunks: row_group.column_chunks,
            ranges,
            page_start_offsets,
        }
    }
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
