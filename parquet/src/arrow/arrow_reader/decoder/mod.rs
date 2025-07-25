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

//! A Parquet "Push" Decoder for decoding values from a Parquet file
//! with data provided by the caller (rather than directly read from an
//! underlying reader).

mod buffers;
mod row_group;

use crate::arrow::arrow_reader::decoder::row_group::{RowGroupReaderBuilder, RowGroupReaderResult};
use crate::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaDataReader;
use arrow_array::RecordBatch;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// A builder for [`ParquetDecoder`].
#[derive(Debug)]
pub struct ParquetDecoderBuilder {
    file_len: u64,
    // TODO optional metadata
    // Configuration options for the decoder
    // e.g., batch size, compression, etc.
}

impl ParquetDecoderBuilder {
    /// Create a new `ParquetDecoderBuilder` with default options
    ///
    /// The file length must be specified
    pub fn new(file_len: u64) -> Self {
        Self { file_len }
    }

    /// Create the decoder with the configured options
    pub fn build(self) -> Result<ParquetDecoder, ParquetError> {
        let Self { file_len } = self;
        // Initialize the decoder with the configured options
        let rg_reader_builder = RowGroupReaderBuilder::new(file_len);

        Ok(ParquetDecoder {
            state: ParquetDecoderState::Start { rg_reader_builder },
        })
    }
}

/// A Description of what data is needed to read the next batch of data
/// from a Parquet file.
#[derive(Debug)]
pub enum DecodeResult {
    /// The decoder needs more data to make progres.
    NeedsData {
        // TOOD distinguish between minimim needed to make progress and what could be used?
        /// The ranges of data from the underlying reader that are needed
        ranges: Vec<Range<u64>>,
    },
    /// The decoder produced a new batch of data
    Batch(RecordBatch),
    /// The decoder finished processing all data requested
    Finished,
}

/// A push based Parquet Decoder
///
/// This is a lower level interface for decoding Parquet data that does not
/// require an underlying reader and therefore offers lower level control over
/// how data is fetched and decoded.
#[derive(Debug)]
pub struct ParquetDecoder {
    /// The inner state.
    ///
    /// This state is consumed on every transition and a new state is produced
    /// so the Rust compiler can ensure that the state is always valid and
    /// transitions are not missed.
    state: ParquetDecoderState,
}

impl ParquetDecoder {
    /// Attempt to decode the next batch of data
    ///
    /// If the decoder needs more data to proceed, it will return the data needed
    ///
    /// This will return `None` if the decoder is finished
    pub fn try_decode(&mut self) -> Result<DecodeResult, ParquetError> {
        let current_state = std::mem::replace(&mut self.state, ParquetDecoderState::Finished);
        let (new_state, decode_result) = current_state.try_transition()?;
        self.state = new_state;
        Ok(decode_result)
    }

    /// Push data into the decoder for processing
    ///
    /// This should correspond to the data ranges requested by the decoder
    pub fn push_data(
        &mut self,
        ranges: Vec<Range<u64>>,
        data: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        let current_state = std::mem::replace(&mut self.state, ParquetDecoderState::Finished);
        self.state = current_state.push_data(ranges, data)?;
        Ok(())
    }
}

#[derive(Debug)]
enum ParquetDecoderState {
    /// Starting State (reading footer)
    Start {
        rg_reader_builder: RowGroupReaderBuilder,
    },
    /// The decoder is reading the footer of the Parquet file
    DecodingMetadata {
        metadata_reader: ParquetMetaDataReader,
        rg_reader_builder: RowGroupReaderBuilder,
    },
    /// Reading data needed to decode the next RowGroup
    ReadingRowGroup {
        /// Builder
        rg_reader_builder: RowGroupReaderBuilder,
    },
    /// The decoder is actively decoding a RowGroup
    DecodingRowGroup {
        /// Current active reader
        record_batch_reader: ParquetRecordBatchReader,
        /// Row groups to decode after this one
        rg_reader_builder: RowGroupReaderBuilder,
    },
    /// The decoder has finished processing all data
    Finished,
}

impl ParquetDecoderState {
    /// Current state --> next state + output
    ///
    /// This is the main state machine logic for the ParquetDecoder.
    fn try_transition(self) -> Result<(Self, DecodeResult), ParquetError> {
        match self {
            Self::Start { rg_reader_builder } => {
                let file_len = rg_reader_builder.file_len();
                let Some(start_offset) = file_len.checked_sub(8) else {
                    return Err(ParquetError::General(format!(
                        "Parquet files are at least 8 bytes long, but file length is {file_len}"
                    )));
                };

                // stay in the same state, and ask for data
                Ok((
                    ParquetDecoderState::Start { rg_reader_builder },
                    DecodeResult::NeedsData {
                        ranges: vec![start_offset..file_len],
                    },
                ))
            }
            Self::DecodingMetadata {
                rg_reader_builder,
                mut metadata_reader,
            } => {
                let maybe_metadata = metadata_reader
                    .try_parse_sized(rg_reader_builder.buffers(), rg_reader_builder.file_len());

                match maybe_metadata {
                    Ok(()) => {
                        // Metadata successfully parsed, proceed to decode the row groups
                        let metadata = metadata_reader.finish()?;

                        let rg_reader_builder = rg_reader_builder.with_metadata(Arc::new(metadata));

                        // Metadata is ready, now start creating RowGroupDecoders
                        Self::ReadingRowGroup { rg_reader_builder }.try_transition()
                    }
                    Err(ParquetError::NeedMoreData(needed)) => {
                        let needed = needed as u64;
                        let file_len = rg_reader_builder.file_len();
                        let Some(start_offset) = file_len.checked_sub(needed) else {
                            return Err(ParquetError::General(format!(
                                "Parquet metadata reader needs at least {needed} bytes, but file length is only {file_len}"
                            )));
                        };
                        let needed_range = start_offset..start_offset + needed;
                        let next_state = Self::DecodingMetadata {
                            metadata_reader,
                            rg_reader_builder,
                        };
                        // needs bytes at the end of the file
                        let result = DecodeResult::NeedsData {
                            ranges: vec![needed_range],
                        };
                        Ok((next_state, result))
                    }
                    Err(e) => Err(e), // pass through other errors
                }
            }
            Self::ReadingRowGroup {
                mut rg_reader_builder,
            } => {
                match rg_reader_builder.try_next_reader()? {
                    // If we have a next reader, we can transition to decoding it
                    RowGroupReaderResult::Ready {
                        record_batch_reader,
                    } => {
                        // Transition to decoding the row group
                        return Self::DecodingRowGroup {
                            record_batch_reader,
                            rg_reader_builder,
                        }
                        .try_transition();
                    }
                    // If there are no more readers, we are finished
                    RowGroupReaderResult::NeedsData { ranges } => {
                        // If we need more data, we return the ranges needed and stay in Reading
                        // RowGroup state
                        return Ok((
                            Self::ReadingRowGroup { rg_reader_builder },
                            DecodeResult::NeedsData { ranges },
                        ));
                    }
                }
            }
            Self::DecodingRowGroup { .. } => {
                todo!()
            }
            Self::Finished => Ok((Self::Finished, DecodeResult::Finished)),
        }
    }

    /// Push data, and transition state if needed
    ///
    /// This should correspond to the data ranges requested by the decoder
    pub fn push_data(
        self,
        ranges: Vec<Range<u64>>,
        data: Vec<Bytes>,
    ) -> Result<Self, ParquetError> {
        match self {
            // it is ok to get data before we asked for it
            ParquetDecoderState::Start {
                mut rg_reader_builder,
            } => {
                rg_reader_builder.buffers_mut().push_ranges(ranges, data);
                let metadata_reader = ParquetMetaDataReader::new();
                Ok(ParquetDecoderState::DecodingMetadata {
                    rg_reader_builder,
                    metadata_reader,
                })
            }
            ParquetDecoderState::DecodingMetadata {
                mut metadata_reader,
                mut rg_reader_builder,
            } => {
                rg_reader_builder.buffers_mut().push_ranges(ranges, data);
                Ok(ParquetDecoderState::DecodingMetadata {
                    metadata_reader,
                    rg_reader_builder,
                })
            }
            ParquetDecoderState::ReadingRowGroup {
                mut rg_reader_builder,
            } => {
                // Push data to the RowGroupReaderBuilder
                rg_reader_builder.buffers_mut().push_ranges(ranges, data);
                Ok(ParquetDecoderState::ReadingRowGroup { rg_reader_builder })
            }
            // it is ok to get data before we asked for it
            ParquetDecoderState::DecodingRowGroup {
                record_batch_reader,
                mut rg_reader_builder,
            } => {
                rg_reader_builder.buffers_mut().push_ranges(ranges, data);
                Ok(ParquetDecoderState::DecodingRowGroup {
                    record_batch_reader,
                    rg_reader_builder,
                })
            }
            ParquetDecoderState::Finished => Err(ParquetError::General(
                "Cannot push data to a finished decoder".to_string(),
            )),
        }
    }
}
