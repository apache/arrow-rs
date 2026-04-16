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

use crate::data_type::AsBytes;
use crate::util::bit_util::num_required_bits;

/// Encoder for definition/repetition levels.
/// Currently only supports Rle and BitPacked (dev/null) encoding, including v2.
pub enum LevelEncoder {
    Rle(RleEncoder),
    RleV2(RleEncoder),
}

impl LevelEncoder {
    /// Creates a new streaming level encoder for Data Page v1.
    ///
    /// This does not require knowing the number of values
    /// upfront, making it suitable for incremental encoding where levels are fed in
    /// as they arrive via [`put_with_observer`](Self::put_with_observer).
    pub fn v1_streaming(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        // Reserve space for length header
        let buffer = vec![0u8; 4];
        LevelEncoder::Rle(RleEncoder::new_from_buf(bit_width, buffer))
    }

    /// Creates a new streaming RLE level encoder for Data Page v2.
    ///
    /// This does not require knowing the number of values
    /// upfront, making it suitable for incremental encoding where levels are fed in
    /// as they arrive via [`put_with_observer`](Self::put_with_observer).
    pub fn v2_streaming(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        LevelEncoder::RleV2(RleEncoder::new_from_buf(bit_width, Vec::new()))
    }

    /// Put/encode levels vector into this level encoder and calls
    /// `observer(value, count)` for each run of identical values encountered
    /// during encoding.
    ///
    /// Returns number of encoded values that are less than or equal to length
    /// of the input buffer.
    ///
    /// This method does **not** flush the underlying encoder, so it can be called
    /// incrementally across multiple batches without forcing run boundaries.
    /// The encoder is flushed automatically when [`consume`](Self::consume) is called.
    #[inline]
    pub fn put_with_observer<F>(&mut self, buffer: &[i16], mut observer: F) -> usize
    where
        F: FnMut(i16, usize),
    {
        match *self {
            LevelEncoder::Rle(ref mut encoder) | LevelEncoder::RleV2(ref mut encoder) => {
                let mut remaining = buffer;
                while let Some((&value, rest)) = remaining.split_first() {
                    encoder.put(value as u64);
                    // After put(), check if the encoder just entered RLE
                    // accumulation mode. If so, scan ahead for the rest of
                    // this run to batch the observer call and bulk-extend.
                    if encoder.is_accumulating_rle(value as u64) {
                        let run_len = rest.iter().take_while(|&&v| v == value).count();
                        if run_len > 0 {
                            encoder.extend_run(run_len);
                        }
                        observer(value, 1 + run_len);
                        remaining = &rest[run_len..];
                    } else {
                        observer(value, 1);
                        remaining = rest;
                    }
                }
                buffer.len()
            }
        }
    }

    /// Finalizes level encoder, flush all intermediate buffers and return resulting
    /// encoded buffer. Returned buffer is already truncated to encoded bytes only.
    #[inline]
    #[allow(unused)]
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
        }
    }

    /// Flushes all intermediate buffers, passes the encoded data to `f`, then
    /// resets the encoder for reuse while retaining the buffer allocation.
    #[inline]
    pub fn flush_to<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        let result = match self {
            LevelEncoder::Rle(encoder) => {
                let data = encoder.flush_buffer_mut();
                // Patch the 4-byte length header reserved at the start of the buffer
                let encoded_len = (data.len() - mem::size_of::<i32>()) as i32;
                data[..4].copy_from_slice(&encoded_len.to_le_bytes());
                f(data)
            }
            LevelEncoder::RleV2(encoder) => f(encoder.flush_buffer()),
        };
        match self {
            LevelEncoder::Rle(encoder) => {
                encoder.clear();
                // Re-reserve the 4-byte length header for the next page
                encoder.skip(mem::size_of::<i32>());
            }
            LevelEncoder::RleV2(encoder) => encoder.clear(),
        }
        result
    }
}
