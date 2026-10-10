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

    /// Put/encode levels vector into this level encoder and call
    /// `observer(value, count)` with the number of occurrences of each distinct
    /// value in `buffer`.
    ///
    /// Returns number of encoded values that are less than or equal to length
    /// of the input buffer.
    ///
    /// This method does **not** flush the underlying encoder, so it can be called
    /// incrementally across multiple batches without forcing run boundaries.
    /// The encoder is flushed automatically when [`consume`](Self::consume) is called.
    #[inline]
    pub fn put_with_observer<F>(&mut self, buffer: &[i16], observer: F) -> usize
    where
        F: FnMut(i16, usize),
    {
        match *self {
            LevelEncoder::Rle(ref mut encoder) | LevelEncoder::RleV2(ref mut encoder) => {
                encoder.put_batch(buffer);
                observe_level_counts(buffer, observer);
                buffer.len()
            }
        }
    }

    /// Encode `count` repetitions of a single level value, calling
    /// `observer(value, count)` exactly once.
    ///
    /// This can be used to encode uniform runs without allocating a level
    /// buffer. Like [`Self::put_with_observer`], but specialized for a single
    /// repeated level value.
    ///
    /// This is O(1) amortized for RLE-based encoders (after a small warmup).
    #[inline]
    pub fn put_n_with_observer<F>(&mut self, value: i16, count: usize, mut observer: F)
    where
        F: FnMut(i16, usize),
    {
        let encoder = match *self {
            LevelEncoder::Rle(ref mut encoder) | LevelEncoder::RleV2(ref mut encoder) => encoder,
        };

        // Feed values individually until the encoder enters RLE accumulation
        // mode for this value, or until we've encoded everything.
        let mut remaining = count;
        while remaining > 0 && !encoder.is_accumulating_rle(value as u64) {
            encoder.put(value as u64);
            remaining -= 1;
        }
        // If we're now in accumulation mode, bulk-extend the rest.
        if remaining > 0 {
            encoder.extend_run(remaining);
        }
        observer(value, count);
    }

    /// Finalizes level encoder, flush all intermediate buffers and return resulting
    /// encoded buffer. Returned buffer is already truncated to encoded bytes only.
    #[inline]
    #[cfg_attr(all(not(feature = "experimental"), not(test)), expect(unused))]
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

/// Calls `observer(level, count)` with the number of occurrences of each
/// distinct level in `levels`
fn observe_level_counts(levels: &[i16], mut observer: impl FnMut(i16, usize)) {
    let Some(&first) = levels.first() else {
        return;
    };

    // Vectorized passes, one per level, as levels are bounded by the nesting
    // depth and typically take only 2 or 3 distinct values
    const MAX_PASSES: u16 = 8;

    let (min, max) = levels
        .iter()
        .fold((first, first), |(min, max), v| (min.min(*v), max.max(*v)));

    if max.abs_diff(min) >= MAX_PASSES {
        levels
            .chunk_by(|a, b| a == b)
            .for_each(|run| observer(run[0], run.len()));
        return;
    }

    // The count of the smallest level is whatever the other levels leave over
    let mut remaining = levels.len();
    for level in min + 1..=max {
        let count = count_equal(levels, level);
        if count > 0 {
            observer(level, count);
            remaining -= count;
        }
    }
    observer(min, remaining);
}

/// Returns the number of `levels` equal to `level`
fn count_equal(levels: &[i16], level: i16) -> usize {
    // Narrow accumulators fit more lanes per vector register, and summing at
    // most `u16::MAX` values can't overflow them
    levels
        .chunks(u16::MAX as usize)
        .map(|block| {
            block
                .iter()
                .fold(0u16, |count, v| count + (*v == level) as u16) as usize
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode `count` repetitions of `value` using `put_with_observer` and
    /// return the raw encoded bytes.
    fn reference_encode(max_level: i16, values: &[i16]) -> Vec<u8> {
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_with_observer(values, |_, _| {});
        enc.consume()
    }

    #[test]
    fn test_put_with_observer_matches_put() {
        use rand::prelude::*;
        use std::collections::BTreeMap;

        let mut rng = StdRng::seed_from_u64(42);
        // Includes ranges past the vectorized counting passes
        for max_level in [1, 2, 3, 7, 8, 15, 100] {
            for _ in 0..20 {
                // Mix random stretches with runs long enough to trigger RLE
                let mut levels: Vec<i16> = Vec::new();
                while levels.len() < 2000 {
                    let n = rng.random_range(1..40);
                    if rng.random_bool(0.5) {
                        let v = rng.random_range(0..=max_level);
                        levels.extend(std::iter::repeat_n(v, n));
                    } else {
                        levels.extend((0..n).map(|_| rng.random_range(0..=max_level)));
                    }
                }

                let mut expected =
                    RleEncoder::new_from_buf(num_required_bits(max_level as u64), Vec::new());
                let mut expected_counts = BTreeMap::new();
                for level in &levels {
                    expected.put(*level as u64);
                    *expected_counts.entry(*level).or_insert(0) += 1;
                }

                let mut actual = LevelEncoder::v2_streaming(max_level);
                let mut actual_counts = BTreeMap::new();
                let mut remaining = levels.as_slice();
                while !remaining.is_empty() {
                    let (batch, rest) = remaining.split_at(rng.random_range(1..=remaining.len()));
                    actual.put_with_observer(batch, |level, count| {
                        *actual_counts.entry(level).or_insert(0) += count;
                    });
                    remaining = rest;
                }

                assert_eq!(actual.consume(), expected.consume());
                assert_eq!(actual_counts, expected_counts);
            }
        }
    }

    #[test]
    fn test_put_n_with_observer_large_run() {
        // Large count exercises the bulk extend_run path (past the 8-value warmup).
        let max_level = 3;
        let count = 10_000;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(2, count, |_, _| {});
        assert_eq!(enc.consume(), reference_encode(max_level, &vec![2; count]));
    }

    #[test]
    fn test_put_n_with_observer_small_count() {
        // Count smaller than the RLE warmup threshold — only the per-element loop runs.
        let max_level = 3;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(1, 5, |_, _| {});
        assert_eq!(enc.consume(), reference_encode(max_level, &[1; 5]));
    }

    #[test]
    fn test_put_n_with_observer_exact_threshold() {
        // Exactly 8 values: the warmup loop completes and extend_run gets 0.
        let max_level = 3;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(3, 8, |_, _| {});
        assert_eq!(enc.consume(), reference_encode(max_level, &[3; 8]));
    }

    #[test]
    fn test_put_n_with_observer_single_value() {
        let max_level = 1;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(1, 1, |_, _| {});
        assert_eq!(enc.consume(), reference_encode(max_level, &[1]));
    }

    #[test]
    fn test_put_n_with_observer_zero_count() {
        let max_level = 3;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(2, 0, |_, _| {});
        assert_eq!(enc.consume(), reference_encode(max_level, &[]));
    }

    #[test]
    fn test_put_n_with_observer_calls_observer_exactly_once() {
        let mut enc = LevelEncoder::v2_streaming(3);
        let mut calls: Vec<(i16, usize)> = Vec::new();
        enc.put_n_with_observer(2, 500, |val, cnt| calls.push((val, cnt)));
        assert_eq!(calls, vec![(2, 500)]);
    }

    #[test]
    fn test_put_n_with_observer_zero_count_calls_observer() {
        let mut enc = LevelEncoder::v2_streaming(3);
        let mut calls: Vec<(i16, usize)> = Vec::new();
        enc.put_n_with_observer(1, 0, |val, cnt| calls.push((val, cnt)));
        assert_eq!(calls, vec![(1, 0)]);
    }

    #[test]
    fn test_put_n_with_observer_followed_by_different_value() {
        // Two consecutive put_n calls with different values — verifies that
        // the encoder correctly transitions between runs.
        let max_level = 3;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        let mut calls: Vec<(i16, usize)> = Vec::new();
        enc.put_n_with_observer(1, 100, |v, c| calls.push((v, c)));
        enc.put_n_with_observer(3, 200, |v, c| calls.push((v, c)));
        assert_eq!(calls, vec![(1, 100), (3, 200)]);

        let reference = reference_encode(max_level, &[&[1i16; 100][..], &[3i16; 200]].concat());
        assert_eq!(enc.consume(), reference);
    }

    #[test]
    fn test_put_n_with_observer_interleaved_with_put_with_observer() {
        // Mix put_n_with_observer and put_with_observer to verify they compose.
        let max_level = 3;
        let mut enc = LevelEncoder::v2_streaming(max_level);
        enc.put_n_with_observer(2, 50, |_, _| {});
        enc.put_with_observer(&[0, 0, 1, 1, 3], |_, _| {});
        enc.put_n_with_observer(2, 50, |_, _| {});

        let input = [&[2i16; 50][..], &[0, 0, 1, 1, 3], &[2i16; 50]].concat();
        assert_eq!(enc.consume(), reference_encode(max_level, &input));
    }

    #[test]
    fn test_put_n_with_observer_v1_roundtrip() {
        // Also verify V1 (Rle variant with length header) works correctly.
        let max_level = 3;
        let mut enc = LevelEncoder::v1_streaming(max_level);
        enc.put_n_with_observer(2, 1000, |_, _| {});

        let mut ref_enc = LevelEncoder::v1_streaming(max_level);
        ref_enc.put_with_observer(&[2; 1000], |_, _| {});
        assert_eq!(enc.consume(), ref_enc.consume());
    }
}
