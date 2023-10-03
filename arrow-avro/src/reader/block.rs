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

//! Decoder for [`Block`]

use crate::reader::vlq::VLQDecoder;
use arrow_schema::ArrowError;

#[derive(Debug, Default)]
pub struct Block {
    pub count: usize,
    pub data: Vec<u8>,
    pub sync: [u8; 16],
}

#[derive(Debug)]
pub struct BlockDecoder {
    state: BlockDecoderState,
    in_progress: Block,
    vlq_decoder: VLQDecoder,
    bytes_remaining: usize,
}

#[derive(Debug)]
enum BlockDecoderState {
    Count,
    Size,
    Data,
    Sync,
    Finished,
}

impl Default for BlockDecoder {
    fn default() -> Self {
        Self {
            state: BlockDecoderState::Count,
            in_progress: Default::default(),
            vlq_decoder: Default::default(),
            bytes_remaining: 0,
        }
    }
}

impl BlockDecoder {
    /// Parse [`Block`] from `buf`, returning the number of bytes read
    pub fn decode(&mut self, mut buf: &[u8]) -> Result<usize, ArrowError> {
        let max_read = buf.len();
        while !buf.is_empty() {
            match self.state {
                BlockDecoderState::Count => {
                    if let Some(c) = self.vlq_decoder.long(&mut buf) {
                        self.in_progress.count = c.try_into().map_err(|_| {
                            ArrowError::ParseError(format!(
                                "Block count cannot be negative, got {c}"
                            ))
                        })?;

                        self.state = BlockDecoderState::Size;
                    }
                }
                BlockDecoderState::Size => {
                    if let Some(c) = self.vlq_decoder.long(&mut buf) {
                        self.bytes_remaining = c.try_into().map_err(|_| {
                            ArrowError::ParseError(format!(
                                "Block size cannot be negative, got {c}"
                            ))
                        })?;

                        self.in_progress.data.reserve(self.bytes_remaining);
                        self.state = BlockDecoderState::Data;
                    }
                }
                BlockDecoderState::Data => {
                    let to_read = self.bytes_remaining.min(buf.len());
                    self.in_progress.data.extend_from_slice(&buf[..to_read]);
                    buf = &buf[to_read..];
                    self.bytes_remaining -= to_read;
                    if self.bytes_remaining == 0 {
                        self.bytes_remaining = 16;
                        self.state = BlockDecoderState::Sync;
                    }
                }
                BlockDecoderState::Sync => {
                    let to_decode = buf.len().min(self.bytes_remaining);
                    let write = &mut self.in_progress.sync[16 - to_decode..];
                    write[..to_decode].copy_from_slice(&buf[..to_decode]);
                    self.bytes_remaining -= to_decode;
                    buf = &buf[to_decode..];
                    if self.bytes_remaining == 0 {
                        self.state = BlockDecoderState::Finished;
                    }
                }
                BlockDecoderState::Finished => return Ok(max_read - buf.len()),
            }
        }
        Ok(max_read)
    }

    /// Flush this decoder returning the parsed [`Block`] if any
    pub fn flush(&mut self) -> Option<Block> {
        match self.state {
            BlockDecoderState::Finished => {
                self.state = BlockDecoderState::Count;
                Some(std::mem::take(&mut self.in_progress))
            }
            _ => None,
        }
    }
}
