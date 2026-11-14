// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::errors::Result;

/// A buffer that supports padding with nulls
pub trait ValuesBuffer {
    /// Create a new buffer with capacity for at least `capacity` logical values
    fn with_capacity(capacity: usize) -> Self;

    /// Reserve capacity for `additional` more logical values.
    ///
    /// Callers should use this once the level decoders have determined how
    /// many output slots are actually needed for the next read.
    fn reserve_exact(&mut self, additional: usize);

    /// If a column contains nulls, more level data may be read than value data, as null
    /// values are not encoded. Therefore, first the levels data is read, the null count
    /// determined, and then the corresponding number of values read to a [`ValuesBuffer`].
    ///
    /// It is then necessary to move this values data into positions that correspond to
    /// the non-null level positions. This is what this method does.
    ///
    /// It is provided with:
    ///
    /// - `read_offset` - the offset in [`ValuesBuffer`] to start null padding from
    /// - `values_read` - the number of values read
    /// - `levels_read` - the number of levels read
    /// - `valid_mask` - a packed mask of valid levels
    ///
    /// Returns an error if the inputs are inconsistent, for example because the
    /// decoded data was corrupt. This must not panic on such input.
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()>;
}

impl<T: Copy + Default> ValuesBuffer for Vec<T> {
    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn reserve_exact(&mut self, additional: usize) {
        Vec::reserve_exact(self, additional)
    }

    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()> {
        self.resize(read_offset + levels_read, T::default());
        spread_values(self, read_offset, read_offset + values_read, valid_mask);
        Ok(())
    }
}

/// Moves the values in `buffer[start..value_end]` up to the positions in
/// `start..buffer.len()` whose bits are set in `valid_mask`, keeping their order.
///
/// Works down from the last level, a 64-bit mask word at a time, picking the cheapest
/// way to fill each word: one block move when all its levels are valid, nothing when
/// all are null, a move per valid level when they are few, and otherwise a branchless
/// write of every level, which is possible because positions of nulls may hold any
/// value. Inconsistent input leaves unspecified values but does not panic.
fn spread_values<T: Copy>(buffer: &mut [T], start: usize, mut value_end: usize, valid_mask: &[u8]) {
    let mut level_end = buffer.len();
    // Once as many values as levels remain, they are all in place.
    while level_end > value_end && value_end > start {
        let word_start = ((level_end - 1) & !63).max(start);
        let len = level_end - word_start;
        let word = mask_bits(valid_mask, word_start, len);
        let valid = word.count_ones() as usize;
        if valid > value_end - start {
            return;
        }
        if valid == len {
            buffer.copy_within(value_end - len..value_end, word_start);
            value_end -= len;
        } else if valid * 2 <= len {
            // Few valid levels: visit just those, highest first. Reversing the word
            // once keeps the loop's dependency on clearing the lowest bit, which
            // is cheaper than clearing the highest.
            let mut reversed = word.reverse_bits();
            while reversed != 0 {
                let bit = 63 - reversed.trailing_zeros() as usize;
                value_end -= 1;
                buffer[word_start + bit] = buffer[value_end];
                reversed &= reversed - 1;
            }
        } else {
            // Mostly valid: write every level with the last value not yet moved and
            // consume it only at valid levels. Levels below the lowest valid one are
            // nulls and are skipped, so `value_end - 1 >= start` holds throughout.
            let lowest = word_start + word.trailing_zeros() as usize;
            for level in (lowest..level_end).rev() {
                buffer[level] = buffer[value_end - 1];
                value_end -= ((word >> (level - word_start)) & 1) as usize;
            }
        }
        level_end = word_start;
    }
}

/// The `len <= 64` bits of `mask` starting at bit `offset`, which must not cross a
/// 64-bit boundary. Bits past the end of `mask` read as zero.
pub(crate) fn mask_bits(mask: &[u8], offset: usize, len: usize) -> u64 {
    let aligned = offset & !63;
    let byte = aligned / 8;
    let word = match mask.get(byte..byte + 8) {
        Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap()),
        None => {
            let mut bytes = [0u8; 8];
            let available = &mask[byte.min(mask.len())..];
            bytes[..available.len()].copy_from_slice(available);
            u64::from_le_bytes(bytes)
        }
    };
    let word = word >> (offset - aligned);
    if len == 64 {
        word
    } else {
        word & ((1 << len) - 1)
    }
}

/// The number of bits set in `mask[start..end]`, counting bits past its end as zero.
pub(crate) fn count_set_bits(mask: &[u8], start: usize, end: usize) -> usize {
    let mut count = 0;
    let mut offset = start;
    while offset < end {
        let len = (64 - (offset & 63)).min(end - offset);
        count += mask_bits(mask, offset, len).count_ones() as usize;
        offset += len;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::Buffer;
    use rand::prelude::*;

    #[test]
    fn test_pad_nulls_places_values_at_valid_levels() {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..2000 {
            let read_offset = rng.random_range(0..150);
            let levels_read = rng.random_range(0..300);
            let density = [0.0, 0.02, 0.5, 0.98, 1.0][rng.random_range(0..5)];
            let prior: Vec<bool> = (0..read_offset).map(|_| rng.random_bool(0.5)).collect();
            let valid: Vec<bool> = (0..levels_read).map(|_| rng.random_bool(density)).collect();
            let mask = Buffer::from_iter(prior.iter().chain(&valid).copied());

            let prefix: Vec<i64> = (0..read_offset as i64).map(|i| -i - 1).collect();
            let values_read = valid.iter().filter(|v| **v).count();
            let mut buffer = prefix.clone();
            buffer.extend((0..values_read as i64).map(|i| i * 10));
            buffer
                .pad_nulls(read_offset, values_read, levels_read, mask.as_slice())
                .unwrap();

            assert_eq!(buffer.len(), read_offset + levels_read);
            assert_eq!(&buffer[..read_offset], prefix.as_slice());
            let mut next = 0;
            for (i, is_valid) in valid.iter().enumerate() {
                if *is_valid {
                    assert_eq!(
                        buffer[read_offset + i],
                        next * 10,
                        "level {i} of {levels_read}"
                    );
                    next += 1;
                }
            }
        }
    }

    #[test]
    fn test_pad_nulls_inconsistent_input_does_not_panic() {
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..2000 {
            let read_offset = rng.random_range(0..100);
            let levels_read = rng.random_range(0..200);
            let values_read = rng.random_range(0..=levels_read);
            let bits: Vec<bool> = (0..read_offset + levels_read + rng.random_range(0..70))
                .map(|_| rng.random_bool(0.5))
                .collect();
            let mask = Buffer::from_iter(bits.iter().copied());
            let mut buffer = vec![1u32; read_offset + values_read];
            buffer
                .pad_nulls(read_offset, values_read, levels_read, mask.as_slice())
                .unwrap();
            assert_eq!(buffer.len(), read_offset + levels_read);
        }
    }
}
