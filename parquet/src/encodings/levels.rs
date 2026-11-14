// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::mem;

use super::rle::{BIT_PACK_GROUP_SIZE, RleEncoder, RunObserver};

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
    /// `observer(value, count)` so that, over all calls, `count` adds up to the
    /// number of times `value` occurs in `buffer`. Each value present is reported at
    /// least once; how its occurrences are split between calls is unspecified.
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
                let mut counter = LevelCounter::new(encoder.bit_width(), &mut observer);
                encoder.put_batch_observed(buffer, &mut counter);
                counter.finish();
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

/// Widest levels [`LevelCounter`] counts in a histogram rather than one by one.
const MAX_COUNTED_LEVEL_BITS: u8 = 4;

/// Passes the runs of levels the encoder finds straight to the observer, and counts
/// the levels of bit packed groups, which have no runs worth reporting, without
/// calling it: per group for one-bit levels, and in a small histogram for levels up
/// to [`MAX_COUNTED_LEVEL_BITS`] bits. Reports those counts on [`LevelCounter::finish`].
struct LevelCounter<'a, F: FnMut(i16, usize)> {
    bit_width: u8,
    observer: &'a mut F,
    /// One-bit levels in packed groups: how many there are, and how many are 1.
    packed: usize,
    packed_ones: usize,
    counts: [usize; 1 << MAX_COUNTED_LEVEL_BITS],
}

impl<'a, F: FnMut(i16, usize)> LevelCounter<'a, F> {
    fn new(bit_width: u8, observer: &'a mut F) -> Self {
        Self {
            bit_width,
            observer,
            packed: 0,
            packed_ones: 0,
            counts: [0; 1 << MAX_COUNTED_LEVEL_BITS],
        }
    }

    fn finish(self) {
        let Self {
            observer,
            packed,
            packed_ones,
            counts,
            ..
        } = self;
        for (level, count) in [(0, packed - packed_ones), (1, packed_ones)] {
            if count != 0 {
                observer(level, count);
            }
        }
        for (level, &count) in counts.iter().enumerate() {
            if count != 0 {
                observer(level as i16, count);
            }
        }
    }
}

impl<F: FnMut(i16, usize)> RunObserver<i16> for LevelCounter<'_, F> {
    #[inline]
    fn run(&mut self, value: i16, count: usize) {
        (self.observer)(value, count)
    }

    #[inline]
    fn packed_group(&mut self, group: &[i16; BIT_PACK_GROUP_SIZE]) {
        if self.bit_width <= 1 {
            self.packed += BIT_PACK_GROUP_SIZE;
            self.packed_ones += group.iter().map(|&level| level as usize).sum::<usize>();
        } else if self.bit_width <= MAX_COUNTED_LEVEL_BITS {
            for &level in group {
                self.counts[level as usize] += 1;
            }
        } else {
            for &level in group {
                (self.observer)(level, 1);
            }
        }
    }
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
    fn test_put_with_observer_reports_level_counts() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(1);
        for max_level in [0i16, 1, 2, 3, 7, 15, 16, 40] {
            for _ in 0..50 {
                let len = rng.random_range(0..3000);
                let run_heavy = rng.random_bool(0.5);
                let mut levels = Vec::with_capacity(len);
                while levels.len() < len {
                    let level = rng.random_range(0..=max_level);
                    let run = if run_heavy {
                        rng.random_range(1..40)
                    } else {
                        1
                    };
                    levels.extend(std::iter::repeat_n(level, run.min(len - levels.len())));
                }
                let mut expected = vec![0usize; max_level as usize + 1];
                for &level in &levels {
                    expected[level as usize] += 1;
                }

                let mut encoder = LevelEncoder::v2_streaming(max_level);
                let mut observed = vec![0usize; max_level as usize + 1];
                for chunk in levels.chunks(rng.random_range(1..700)) {
                    encoder.put_with_observer(chunk, |level, count| {
                        assert!(count > 0);
                        observed[level as usize] += count;
                    });
                }
                assert_eq!(observed, expected, "max level {max_level}");
                assert_eq!(
                    encoder.consume(),
                    reference_encode_per_value(max_level, &levels)
                );
            }
        }
    }

    /// Encodes `values` one at a time through the encoder's per-value path.
    fn reference_encode_per_value(max_level: i16, values: &[i16]) -> Vec<u8> {
        let bit_width = num_required_bits(max_level as u64);
        let mut encoder = RleEncoder::new_from_buf(bit_width, Vec::new());
        for &value in values {
            encoder.put(value as u64);
        }
        encoder.consume()
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
