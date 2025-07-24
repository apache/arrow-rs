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

use crate::arrow::arrow_reader::{ReadPlan, RowSelection};
use crate::errors::ParquetError;
use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use crate::file::reader::{ChunkReader, Length};
use arrow_array::RecordBatch;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use arrow_schema::SchemaRef;
use crate::arrow::ProjectionMask;

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
        Ok(ParquetDecoder {
            state: ParquetDecoderState::Start { file_len },
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
    Start { file_len: u64 },
    /// The decoder is reading the footer of the Parquet file
    DecodingMetadata {
        file_len: u64,
        buffers: Buffers,
        metadata_decoder: ParquetMetaDataReader,
    },
    /// The decoder is actively decoding a RowGroup
    DecodingRowGroup {
        row_group_decoder: RowGroupDecoder,

        // Row groups to decode after this one
        remaining_row_groups: VecDeque<RowGroupDecoder>,
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
            Self::Start { file_len } => {
                let Some(start_offset) = file_len.checked_sub(8) else {
                    return Err(ParquetError::General(format!(
                        "Parquet files are at least 8 bytes long, but file length is {file_len}"
                    )));
                };

                // stay in the same state, and ask for data
                Ok((
                    ParquetDecoderState::Start { file_len },
                    DecodeResult::NeedsData {
                        ranges: vec![start_offset..file_len],
                    },
                ))
            }
            Self::DecodingMetadata {
                file_len,
                buffers,
                mut metadata_decoder,
            } => {
                match metadata_decoder.try_parse_sized(&buffers, file_len) {
                    Ok(()) => {
                        // Metadata successfully parsed, proceed to decode the row groups
                        let metadata = metadata_decoder.finish()?;

                        let mut row_group_decoders = create_row_group_decoders(file_len, Arc::new(metadata));
                        // no row groups to decode, done
                        let Some(row_group_decoder) = row_group_decoders.pop_front() else {
                            return Ok((Self::Finished, DecodeResult::Finished));
                        };

                        // row groups are setup, try and decode the first one
                        Self::DecodingRowGroup {
                            row_group_decoder,
                            remaining_row_groups: row_group_decoders,
                        }
                            .try_transition()
                    }
                    Err(ParquetError::NeedMoreData(needed)) => {
                        let needed = needed as u64;
                        let Some(start_offset) = file_len.checked_sub(needed) else {
                            return Err(ParquetError::General(format!(
                                "Parquet metadata reader needs at least {needed} bytes, but file length is only {file_len}"
                            )));
                        };
                        let needed_range = start_offset..start_offset + needed;
                        let next_state = Self::DecodingMetadata {
                            file_len,
                            buffers,
                            metadata_decoder,
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
            ParquetDecoderState::Start { file_len } => {
                let buffers = Buffers {
                    file_len,
                    offset: 0,
                    ranges,
                    buffers: data,
                };
                Ok(ParquetDecoderState::DecodingMetadata {
                    file_len,
                    buffers,
                    metadata_decoder: ParquetMetaDataReader::new(), // Initialize the metadata decoder
                })
            }
            ParquetDecoderState::DecodingMetadata { .. } => {
                todo!()
            }
            ParquetDecoderState::DecodingRowGroup { .. } => {
                todo!()
            }
            ParquetDecoderState::Finished => Err(ParquetError::General(
                "Cannot push data to a finished decoder".to_string(),
            )),
        }
    }
}

/// Sets up the initial state for decoding RowGroups
fn create_row_group_decoders(
    file_len: u64,
    parquet_metadata: Arc<ParquetMetaData>,
) -> VecDeque<RowGroupDecoder> {
    todo!()
}

/// State required for decoding a RowGroup
///
/// The idea is eventually these could be decoded in parallel so keep all the
/// decoding logic in a single struct.
#[derive(Debug)]
struct RowGroupDecoder {
    file_len: u64,
    current_plan: ReadPlan,
    current_row_group: usize,
}

/// Holds multiple buffers of data that have been requested by the ParquetDecoder
///
/// This is the in-memory buffer for the ParquetDecoder
///
/// Features it has:
/// 1. Zero copy as much as possible
/// 2. Keeps non contiguous ranges of bytes
///
/// Features it should have:
/// 1. Maybe(??) coalsecing
/// 1. Release buffers that are no longer used (like once metadata scanning is done, can release any just footer data)
/// 2. A way for users to more carefully control what is in the cache (don't clear, for example??)
#[derive(Debug, Clone)]
pub struct Buffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The ranges of data that are available for decoding (not adjusted for offset)
    ranges: Vec<Range<u64>>,
    /// The buffers of data that can be used to decode the Parquet file
    buffers: Vec<Bytes>,
}

impl Buffers {
    fn iter(&self) -> impl Iterator<Item = (&Range<u64>, &Bytes)> {
        self.ranges.iter().zip(self.buffers.iter())
    }

    /// Specify a new offset
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }
}

impl Length for Buffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

/// less efficinet implementation of Read for Buffers
impl std::io::Read for Buffers {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Find the range that contains the start offset
        let mut found = false;
        for (range, data) in self.iter() {
            if range.start <= self.offset && range.end >= self.offset + buf.len() as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (self.offset - range.start) as usize;
                let end_offset = start_offset + buf.len();
                let slice = data.slice(start_offset..end_offset);
                buf.copy_from_slice(slice.as_ref());
                found = true;
            }
        }
        if found {
            // If we found the range, we can return the number of bytes read
            // advance our offset
            self.offset += buf.len() as u64;
            Ok(buf.len())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available",
            ))
        }
    }
}

impl ChunkReader for Buffers {
    type T = Self;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(self.clone().with_offset(self.offset + start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        // find the range that contains the start offset
        for (range, data) in self.iter() {
            if range.start <= start && range.end >= start + length as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (start - range.start) as usize;
                return Ok(data.slice(start_offset..start_offset + length));
            }
        }
        todo!("Handle case where requests span ranges");
    }
}
