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
                    let start = 16 - self.bytes_remaining;
                    let end = start + to_decode;
                    self.in_progress.sync[start..end].copy_from_slice(&buf[..to_decode]);
                    self.bytes_remaining -= to_decode;
                    buf = &buf[to_decode..];
                    if self.bytes_remaining == 0 {
                        self.state = BlockDecoderState::Finished;
                    }
                }
                BlockDecoderState::Finished => {
                    return Ok(max_read - buf.len());
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::ArrowError;
    use std::convert::TryFrom;

    fn encode_vlq(value: i64) -> Vec<u8> {
        let mut buf = vec![];
        let mut ux = ((value << 1) ^ (value >> 63)) as u64; // ZigZag

        loop {
            let mut byte = (ux & 0x7F) as u8;
            ux >>= 7;
            if ux != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if ux == 0 {
                break;
            }
        }
        buf
    }

    #[test]
    fn test_empty_input() {
        let mut decoder = BlockDecoder::default();
        let buf = [];
        let read = decoder.decode(&buf).unwrap();
        assert_eq!(read, 0);
        assert!(decoder.flush().is_none());
    }

    #[test]
    fn test_single_block_full_buffer() {
        let mut decoder = BlockDecoder::default();
        let count_encoded = encode_vlq(10);
        let size_encoded = encode_vlq(4);
        let data = vec![1u8, 2, 3, 4];
        let sync_marker = vec![0xAB; 16];
        let mut input = Vec::new();
        input.extend_from_slice(&count_encoded);
        input.extend_from_slice(&size_encoded);
        input.extend_from_slice(&data);
        input.extend_from_slice(&sync_marker);
        let read = decoder.decode(&input).unwrap();
        assert_eq!(read, input.len());
        let block = decoder.flush().expect("Should produce a finished block");
        assert_eq!(block.count, 10);
        assert_eq!(block.data, data);
        let expected_sync: [u8; 16] = <[u8; 16]>::try_from(&sync_marker[..16]).unwrap();
        assert_eq!(block.sync, expected_sync);
    }

    #[test]
    fn test_single_block_partial_buffer() {
        let mut decoder = BlockDecoder::default();
        let count_encoded = encode_vlq(2);
        let size_encoded = encode_vlq(3);
        let data = vec![10u8, 20, 30];
        let sync_marker = vec![0xCD; 16];
        let mut input = Vec::new();
        input.extend_from_slice(&count_encoded);
        input.extend_from_slice(&size_encoded);
        input.extend_from_slice(&data);
        input.extend_from_slice(&sync_marker);
        // Split into 3 parts
        let part1 = &input[0..1];
        let part2 = &input[1..2];
        let part3 = &input[2..];
        let read = decoder.decode(part1).unwrap();
        assert_eq!(read, 1);
        assert!(decoder.flush().is_none());
        let read = decoder.decode(part2).unwrap();
        assert_eq!(read, 1);
        assert!(decoder.flush().is_none());
        let read = decoder.decode(part3).unwrap();
        assert_eq!(read, part3.len());
        let block = decoder.flush().expect("Should produce a finished block");
        assert_eq!(block.count, 2);
        assert_eq!(block.data, data);
        let expected_sync: [u8; 16] = <[u8; 16]>::try_from(&sync_marker[..16]).unwrap();
        assert_eq!(block.sync, expected_sync);
    }

    #[test]
    fn test_multiple_blocks_in_one_buffer() {
        let mut decoder = BlockDecoder::default();
        // Block1
        let block1_count = encode_vlq(1);
        let block1_size = encode_vlq(2);
        let block1_data = vec![0x01, 0x02];
        let block1_sync = vec![0xAA; 16];
        // Block2
        let block2_count = encode_vlq(3);
        let block2_size = encode_vlq(1);
        let block2_data = vec![0x99];
        let block2_sync = vec![0xBB; 16];
        let mut input = Vec::new();
        input.extend_from_slice(&block1_count);
        input.extend_from_slice(&block1_size);
        input.extend_from_slice(&block1_data);
        input.extend_from_slice(&block1_sync);
        input.extend_from_slice(&block2_count);
        input.extend_from_slice(&block2_size);
        input.extend_from_slice(&block2_data);
        input.extend_from_slice(&block2_sync);
        let read1 = decoder.decode(&input).unwrap();
        let block1 = decoder.flush().expect("First block should be complete");
        assert_eq!(block1.count, 1);
        assert_eq!(block1.data, block1_data);
        let expected_sync1: [u8; 16] = <[u8; 16]>::try_from(&block1_sync[..16]).unwrap();
        assert_eq!(block1.sync, expected_sync1);
        let remainder = &input[read1..];
        decoder.decode(remainder).unwrap();
        let block2 = decoder.flush().expect("Second block should be complete");
        assert_eq!(block2.count, 3);
        assert_eq!(block2.data, block2_data);
        let expected_sync2: [u8; 16] = <[u8; 16]>::try_from(&block2_sync[..16]).unwrap();
        assert_eq!(block2.sync, expected_sync2);
    }

    #[test]
    fn test_negative_count_should_error() {
        let mut decoder = BlockDecoder::default();
        let bad_count = encode_vlq(-1);
        let size = encode_vlq(5);
        let mut input = Vec::new();
        input.extend_from_slice(&bad_count);
        input.extend_from_slice(&size);
        let err = decoder.decode(&input).unwrap_err();
        match err {
            ArrowError::ParseError(msg) => {
                assert!(
                    msg.contains("Block count cannot be negative"),
                    "Expected negative count parse error, got: {msg}"
                );
            }
            _ => panic!("Unexpected error type: {err:?}"),
        }
    }

    #[test]
    fn test_negative_size_should_error() {
        let mut decoder = BlockDecoder::default();
        let count = encode_vlq(5);
        let bad_size = encode_vlq(-10);
        let mut input = Vec::new();
        input.extend_from_slice(&count);
        input.extend_from_slice(&bad_size);
        let err = decoder.decode(&input).unwrap_err();
        match err {
            ArrowError::ParseError(msg) => {
                assert!(
                    msg.contains("Block size cannot be negative"),
                    "Expected negative size parse error, got: {msg}"
                );
            }
            _ => panic!("Unexpected error type: {err:?}"),
        }
    }

    #[test]
    fn test_partial_sync_across_multiple_calls() {
        let mut decoder = BlockDecoder::default();
        let count_encoded = encode_vlq(1);
        let size_encoded = encode_vlq(2);
        let data = vec![0x01, 0x02];
        let sync_marker = vec![0xCC; 16];
        let mut input = Vec::new();
        input.extend_from_slice(&count_encoded);
        input.extend_from_slice(&size_encoded);
        input.extend_from_slice(&data);
        input.extend_from_slice(&sync_marker);
        let split_point = input.len() - 4;
        let part1 = &input[..split_point];
        let part2 = &input[split_point..];
        let read1 = decoder.decode(part1).unwrap();
        assert_eq!(read1, part1.len());
        assert!(decoder.flush().is_none());
        let read2 = decoder.decode(part2).unwrap();
        assert_eq!(read2, part2.len());
        let block = decoder.flush().expect("Block should be complete now");
        assert_eq!(block.count, 1);
        assert_eq!(block.data, data);
        let expected_sync: [u8; 16] = <[u8; 16]>::try_from(&sync_marker[..16]).unwrap();
        assert_eq!(block.sync, expected_sync, "Should match [0xCC; 16]");
    }

    #[test]
    fn test_already_finished_state() {
        let mut decoder = BlockDecoder::default();
        let count_encoded = encode_vlq(2);
        let size_encoded = encode_vlq(1);
        let data = vec![0xAB];
        let sync_marker = vec![0xFF; 16];
        let mut input = Vec::new();
        input.extend_from_slice(&count_encoded);
        input.extend_from_slice(&size_encoded);
        input.extend_from_slice(&data);
        input.extend_from_slice(&sync_marker);
        let read = decoder.decode(&input).unwrap();
        assert_eq!(read, input.len());
        let block = decoder.flush().expect("Should have a block");
        assert_eq!(block.count, 2);
        assert_eq!(block.data, data);
        let expected_sync: [u8; 16] = <[u8; 16]>::try_from(&sync_marker[..16]).unwrap();
        assert_eq!(block.sync, expected_sync);
        let read2 = decoder.decode(&[]).unwrap();
        assert_eq!(read2, 0);
        assert!(decoder.flush().is_none());
    }
}
