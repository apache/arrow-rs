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

use crate::errors::AvroError;
use crate::reader::vlq::VLQDecoder;

/// A file data block
///
/// <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
#[derive(Debug, Default)]
pub struct Block {
    /// The number of objects in this block
    pub count: usize,
    /// The serialized objects within this block
    pub data: Vec<u8>,
    /// The sync marker
    pub sync: [u8; 16],
}

/// A decoder for [`Block`]
#[derive(Debug)]
pub struct BlockDecoder {
    state: BlockDecoderState,
    in_progress: Block,
    vlq_decoder: VLQDecoder,
    bytes_remaining: usize,
}

#[derive(Debug)]
pub(crate) enum BlockDecoderState {
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
    ///
    /// This method can be called multiple times with consecutive chunks of data, allowing
    /// integration with chunked IO systems like [`BufRead::fill_buf`]
    ///
    /// All errors should be considered fatal, and decoding aborted
    ///
    /// Once an entire [`Block`] has been decoded this method will not read any further
    /// input bytes, until [`Self::flush`] is called. Afterwards [`Self::decode`]
    /// can then be used again to read the next block, if any
    ///
    /// [`BufRead::fill_buf`]: std::io::BufRead::fill_buf
    pub fn decode(&mut self, mut buf: &[u8]) -> Result<usize, AvroError> {
        let max_read = buf.len();
        while !buf.is_empty() {
            match self.state {
                BlockDecoderState::Count => {
                    if let Some(c) = self.vlq_decoder.long(&mut buf)? {
                        self.in_progress.count = c.try_into().map_err(|_| {
                            AvroError::ParseError(format!(
                                "Block count cannot be negative, got {c}"
                            ))
                        })?;

                        self.state = BlockDecoderState::Size;
                    }
                }
                BlockDecoderState::Size => {
                    if let Some(c) = self.vlq_decoder.long(&mut buf)? {
                        self.bytes_remaining = c.try_into().map_err(|_| {
                            AvroError::ParseError(format!("Block size cannot be negative, got {c}"))
                        })?;

                        // Only reserve what the current input backs: the block size is
                        // input specified so could be an extreme value (e.g. i64::MAX)
                        // in case of corrupted/malicious input. The rest is reserved
                        // lazily by `extend_from_slice` below as data arrives.
                        self.in_progress
                            .data
                            .reserve(self.bytes_remaining.min(buf.len()));
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

#[cfg(feature = "async")]
impl BlockDecoder {
    pub(crate) fn state(&self) -> &BlockDecoderState {
        &self.state
    }

    pub(crate) fn bytes_remaining(&self) -> usize {
        self.bytes_remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Zig-zag encode `value` as an Avro `long` (variable-length integer).
    fn encode_long(value: i64, out: &mut Vec<u8>) {
        let mut n = ((value << 1) ^ (value >> 63)) as u64;
        while n >= 0x80 {
            out.push((n as u8) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }

    #[test]
    fn test_oversized_block_size_bounds_reserve() {
        // A block advertising `i64::MAX` bytes must not reserve that up front when only
        // a few payload bytes are present, or a crafted OCF aborts on a huge alloc (#10234).
        let mut buf = Vec::new();
        encode_long(1, &mut buf); // object count
        encode_long(i64::MAX, &mut buf); // attacker-controlled block size
        buf.extend_from_slice(&[0u8; 8]); // a handful of real bytes

        let mut decoder = BlockDecoder::default();
        let read = decoder.decode(&buf).unwrap();

        assert_eq!(read, buf.len(), "all available input should be consumed");
        assert!(
            decoder.in_progress.data.capacity() <= buf.len(),
            "capacity {} must stay bounded by available input {}, not the advertised i64::MAX",
            decoder.in_progress.data.capacity(),
            buf.len(),
        );
    }

    #[test]
    fn test_negative_block_size_errors() {
        let mut buf = Vec::new();
        encode_long(1, &mut buf); // object count
        encode_long(-1, &mut buf); // invalid (negative) block size

        let mut decoder = BlockDecoder::default();
        let err = decoder.decode(&buf).unwrap_err();
        assert!(
            err.to_string().contains("Block size cannot be negative"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_well_formed_block_round_trips() {
        // The capped reserve must not change decoding of a normal block.
        let payload = [1u8, 2, 3, 4];
        let sync = [7u8; 16];
        let mut buf = Vec::new();
        encode_long(2, &mut buf); // object count
        encode_long(payload.len() as i64, &mut buf); // block size
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&sync);

        let mut decoder = BlockDecoder::default();
        assert_eq!(decoder.decode(&buf).unwrap(), buf.len());
        let block = decoder.flush().expect("a complete block");
        assert_eq!(block.count, 2);
        assert_eq!(block.data, payload);
        assert_eq!(block.sync, sync);
    }
}
