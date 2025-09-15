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

use std::mem;

use super::rle::RleEncoder;

use crate::basic::Encoding;
use crate::data_type::AsBytes;
use crate::util::bit_util::{ceil, num_required_bits, BitWriter};

/// Computes max buffer size for level encoder/decoder based on encoding, max
/// repetition/definition level and number of total buffered values (includes null
/// values).
#[inline]
pub fn max_buffer_size(encoding: Encoding, max_level: i16, num_buffered_values: usize) -> usize {
    let bit_width = num_required_bits(max_level as u64);
    match encoding {
        Encoding::RLE => RleEncoder::max_buffer_size(bit_width, num_buffered_values),
        #[allow(deprecated)]
        Encoding::BIT_PACKED => ceil(num_buffered_values * bit_width as usize, 8),
        _ => panic!("Unsupported encoding type {encoding}"),
    }
}

/// Encoder for definition/repetition levels.
/// Currently only supports Rle and BitPacked (dev/null) encoding, including v2.
pub enum LevelEncoder {
    Rle(RleEncoder),
    RleV2(RleEncoder),
    BitPacked(u8, BitWriter),
}

impl LevelEncoder {
    /// Creates new level encoder based on encoding, max level and underlying byte buffer.
    /// For bit packed encoding it is assumed that buffer is already allocated with
    /// `levels::max_buffer_size` method.
    ///
    /// Used to encode levels for Data Page v1.
    ///
    /// Panics, if encoding is not supported.
    pub fn v1(encoding: Encoding, max_level: i16, capacity: usize) -> Self {
        let capacity_bytes = max_buffer_size(encoding, max_level, capacity);
        let mut buffer = Vec::with_capacity(capacity_bytes);
        let bit_width = num_required_bits(max_level as u64);
        match encoding {
            Encoding::RLE => {
                // Reserve space for length header
                buffer.extend_from_slice(&[0; 4]);
                LevelEncoder::Rle(RleEncoder::new_from_buf(bit_width, buffer))
            }
            #[allow(deprecated)]
            Encoding::BIT_PACKED => {
                // Here we set full byte buffer without adjusting for num_buffered_values,
                // because byte buffer will already be allocated with size from
                // `max_buffer_size()` method.
                LevelEncoder::BitPacked(bit_width, BitWriter::new_from_buf(buffer))
            }
            _ => panic!("Unsupported encoding type {encoding}"),
        }
    }

    /// Creates new level encoder based on RLE encoding. Used to encode Data Page v2
    /// repetition and definition levels.
    pub fn v2(max_level: i16, capacity: usize) -> Self {
        let capacity_bytes = max_buffer_size(Encoding::RLE, max_level, capacity);
        let buffer = Vec::with_capacity(capacity_bytes);
        let bit_width = num_required_bits(max_level as u64);
        LevelEncoder::RleV2(RleEncoder::new_from_buf(bit_width, buffer))
    }

    /// Put/encode levels vector into this level encoder.
    /// Returns number of encoded values that are less than or equal to length of the
    /// input buffer.
    #[inline]
    pub fn put(&mut self, buffer: &[i16]) -> usize {
        let mut num_encoded = 0;
        match *self {
            LevelEncoder::Rle(ref mut encoder) | LevelEncoder::RleV2(ref mut encoder) => {
                for value in buffer {
                    encoder.put(*value as u64);
                    num_encoded += 1;
                }
                encoder.flush();
            }
            LevelEncoder::BitPacked(bit_width, ref mut encoder) => {
                for value in buffer {
                    encoder.put_value(*value as u64, bit_width as usize);
                    num_encoded += 1;
                }
                encoder.flush();
            }
        }
        num_encoded
    }

    /// Finalizes level encoder, flush all intermediate buffers and return resulting
    /// encoded buffer. Returned buffer is already truncated to encoded bytes only.
    #[inline]
    pub fn consume(self) -> Vec<u8> {
        match self {
            LevelEncoder::Rle(encoder) => {
                let mut encoded_data = encoder.consume();
                // Account for the buffer offset
                let encoded_len = encoded_data.len() - mem::size_of::<i32>();
                let len = (encoded_len as i32).to_le();
                let len_bytes = len.as_bytes();
                encoded_data[0..len_bytes.len()].copy_from_slice(len_bytes);
                encoded_data
            }
            LevelEncoder::RleV2(encoder) => encoder.consume(),
            LevelEncoder::BitPacked(_, encoder) => encoder.consume(),
        }
    }
}
