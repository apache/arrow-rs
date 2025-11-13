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

use crate::DecodeResult;
use crate::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use crate::arrow::push_decoder::reader_builder::RowGroupReaderBuilder;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// State machine that tracks the remaining high level chunks (row groups) of
/// Parquet data are left to read.
///
/// This is currently a row group, but the author aspires to extend the pattern
/// to data boundaries other than RowGroups in the future.
#[derive(Debug)]
pub(crate) struct RemainingRowGroups {
    /// The underlying Parquet metadata
    parquet_metadata: Arc<ParquetMetaData>,

    /// The row groups that have not yet been read
    row_groups: VecDeque<usize>,

    /// Remaining selection to apply to the next row groups
    selection: Option<RowSelection>,

    /// State for building the reader for the current row group
    row_group_reader_builder: RowGroupReaderBuilder,
}

impl RemainingRowGroups {
    pub fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
        row_group_reader_builder: RowGroupReaderBuilder,
    ) -> Self {
        Self {
            parquet_metadata,
            row_groups: VecDeque::from(row_groups),
            selection,
            row_group_reader_builder,
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.row_group_reader_builder.push_data(ranges, buffers);
    }

    /// Return the total number of bytes buffered so far
    pub fn buffered_bytes(&self) -> u64 {
        self.row_group_reader_builder.buffered_bytes()
    }

    /// returns [`ParquetRecordBatchReader`] suitable for reading the next
    /// group of rows from the Parquet data, or the list of data ranges still
    /// needed to proceed
    pub fn try_next_reader(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        loop {
            // Are we ready yet to start reading?
            let result: DecodeResult<ParquetRecordBatchReader> =
                self.row_group_reader_builder.try_build()?;
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
            self.row_group_reader_builder
                .next_row_group(row_group_idx, row_count, selection)?;
            // the next iteration will try to build the reader for the new row group
        }
    }
}
