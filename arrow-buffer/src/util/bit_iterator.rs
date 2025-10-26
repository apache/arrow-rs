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

//! Types for iterating over packed bitmasks

use crate::bit_chunk_iterator::{UnalignedBitChunk, UnalignedBitChunkIterator};
use crate::bit_util::{ceil, get_bit_raw};

/// Iterator over the bits within a packed bitmask
///
/// To efficiently iterate over just the set bits see [`BitIndexIterator`] and [`BitSliceIterator`]
#[derive(Clone)]
pub struct BitIterator<'a> {
    buffer: &'a [u8],
    current_offset: usize,
    end_offset: usize,
}

impl<'a> BitIterator<'a> {
    /// Create a new [`BitIterator`] from the provided `buffer`,
    /// and `offset` and `len` in bits
    ///
    /// # Panic
    ///
    /// Panics if `buffer` is too short for the provided offset and length
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        let end_offset = offset.checked_add(len).unwrap();
        let required_len = ceil(end_offset, 8);
        assert!(
            buffer.len() >= required_len,
            "BitIterator buffer too small, expected {required_len} got {}",
            buffer.len()
        );

        Self {
            buffer,
            current_offset: offset,
            end_offset,
        }
    }
}

impl Iterator for BitIterator<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset == self.end_offset {
            return None;
        }
        // Safety:
        // offsets in bounds
        let v = unsafe { get_bit_raw(self.buffer.as_ptr(), self.current_offset) };
        self.current_offset += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_bits = self.end_offset - self.current_offset;
        (remaining_bits, Some(remaining_bits))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.len()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // Check if we can advance to the desired offset.
        // When n is 0 it means we want the next() value
        // and when n is 1 we want the next().next() value
        // so adding n to the current offset and not n - 1
        match self.current_offset.checked_add(n) {
            // Yes, and still within bounds
            Some(new_offset) if new_offset < self.end_offset => {
                self.current_offset = new_offset;
            }

            // Either overflow or would exceed end_offset
            _ => {
                self.current_offset = self.end_offset;
                return None;
            }
        }

        self.next()
    }

    fn last(mut self) -> Option<Self::Item> {
        // If already at the end, return None
        if self.current_offset == self.end_offset {
            return None;
        }

        // Go to the one before the last bit
        self.current_offset = self.end_offset - 1;

        // Return the last bit
        self.next()
    }

    fn max(self) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Ord,
    {
        if self.current_offset == self.end_offset {
            return None;
        }

        // true is greater than false so we only need to check if there's any true bit
        let mut bit_index_iter = BitIndexIterator::new(
            self.buffer,
            self.current_offset,
            self.end_offset - self.current_offset,
        );

        if bit_index_iter.next().is_some() {
            return Some(true);
        }

        // We know the iterator is not empty and there are no set bits so false is the max
        Some(false)
    }
}

impl ExactSizeIterator for BitIterator<'_> {}

impl DoubleEndedIterator for BitIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_offset == self.end_offset {
            return None;
        }
        self.end_offset -= 1;
        // Safety:
        // offsets in bounds
        let v = unsafe { get_bit_raw(self.buffer.as_ptr(), self.end_offset) };
        Some(v)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        // Check if we can advance to the desired offset.
        // When n is 0 it means we want the next_back() value
        // and when n is 1 we want the next_back().next_back() value
        // so subtracting n to the current offset and not n - 1
        match self.end_offset.checked_sub(n) {
            // Yes, and still within bounds
            Some(new_offset) if self.current_offset < new_offset => {
                self.end_offset = new_offset;
            }

            // Either underflow or would exceed current_offset
            _ => {
                self.current_offset = self.end_offset;
                return None;
            }
        }

        self.next_back()
    }
}

/// Iterator of contiguous ranges of set bits within a provided packed bitmask
///
/// Returns `(usize, usize)` each representing an interval where the corresponding
/// bits in the provides mask are set
///
/// the first value is the start of the range (inclusive) and the second value is the end of the range (exclusive)
///
#[derive(Debug)]
pub struct BitSliceIterator<'a> {
    iter: UnalignedBitChunkIterator<'a>,
    len: usize,
    current_offset: i64,
    current_chunk: u64,
}

impl<'a> BitSliceIterator<'a> {
    /// Create a new [`BitSliceIterator`] from the provided `buffer`,
    /// and `offset` and `len` in bits
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        let chunk = UnalignedBitChunk::new(buffer, offset, len);
        let mut iter = chunk.iter();

        let current_offset = -(chunk.lead_padding() as i64);
        let current_chunk = iter.next().unwrap_or(0);

        Self {
            iter,
            len,
            current_offset,
            current_chunk,
        }
    }

    /// Returns `Some((chunk_offset, bit_offset))` for the next chunk that has at
    /// least one bit set, or None if there is no such chunk.
    ///
    /// Where `chunk_offset` is the bit offset to the current `u64` chunk
    /// and `bit_offset` is the offset of the first `1` bit in that chunk
    fn advance_to_set_bit(&mut self) -> Option<(i64, u32)> {
        loop {
            if self.current_chunk != 0 {
                // Find the index of the first 1
                let bit_pos = self.current_chunk.trailing_zeros();
                return Some((self.current_offset, bit_pos));
            }

            self.current_chunk = self.iter.next()?;
            self.current_offset += 64;
        }
    }
}

impl Iterator for BitSliceIterator<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // Used as termination condition
        if self.len == 0 {
            return None;
        }

        let (start_chunk, start_bit) = self.advance_to_set_bit()?;

        // Set bits up to start
        self.current_chunk |= (1 << start_bit) - 1;

        loop {
            if self.current_chunk != u64::MAX {
                // Find the index of the first 0
                let end_bit = self.current_chunk.trailing_ones();

                // Zero out up to end_bit
                self.current_chunk &= !((1 << end_bit) - 1);

                return Some((
                    (start_chunk + start_bit as i64) as usize,
                    (self.current_offset + end_bit as i64) as usize,
                ));
            }

            match self.iter.next() {
                Some(next) => {
                    self.current_chunk = next;
                    self.current_offset += 64;
                }
                None => {
                    return Some((
                        (start_chunk + start_bit as i64) as usize,
                        std::mem::replace(&mut self.len, 0),
                    ));
                }
            }
        }
    }
}

/// An iterator of `usize` whose index in a provided bitmask is true
///
/// This provides the best performance on most masks, apart from those which contain
/// large runs and therefore favour [`BitSliceIterator`]
#[derive(Debug)]
pub struct BitIndexIterator<'a> {
    current_chunk: u64,
    chunk_offset: i64,
    iter: UnalignedBitChunkIterator<'a>,
}

impl<'a> BitIndexIterator<'a> {
    /// Create a new [`BitIndexIterator`] from the provide `buffer`,
    /// and `offset` and `len` in bits
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        let chunks = UnalignedBitChunk::new(buffer, offset, len);
        let mut iter = chunks.iter();

        let current_chunk = iter.next().unwrap_or(0);
        let chunk_offset = -(chunks.lead_padding() as i64);

        Self {
            current_chunk,
            chunk_offset,
            iter,
        }
    }
}

impl Iterator for BitIndexIterator<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_chunk != 0 {
                let bit_pos = self.current_chunk.trailing_zeros();
                self.current_chunk ^= 1 << bit_pos;
                return Some((self.chunk_offset + bit_pos as i64) as usize);
            }

            self.current_chunk = self.iter.next()?;
            self.chunk_offset += 64;
        }
    }
}

/// An iterator of u32 whose index in a provided bitmask is true
/// Respects arbitrary offsets and slice lead/trail padding exactly like BitIndexIterator
#[derive(Debug)]
pub struct BitIndexU32Iterator<'a> {
    curr: u64,
    chunk_offset: i64,
    iter: UnalignedBitChunkIterator<'a>,
}

impl<'a> BitIndexU32Iterator<'a> {
    /// Create a new [BitIndexU32Iterator] from the provided buffer,
    /// offset and len in bits.
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        // Build the aligned chunks (including prefix/suffix masked)
        let chunks = UnalignedBitChunk::new(buffer, offset, len);
        let mut iter = chunks.iter();

        // First 64-bit word (masked for lead padding), or 0 if empty
        let curr = iter.next().unwrap_or(0);
        // Negative lead padding ensures the first bit in curr maps to index 0
        let chunk_offset = -(chunks.lead_padding() as i64);

        Self {
            curr,
            chunk_offset,
            iter,
        }
    }
}

impl<'a> Iterator for BitIndexU32Iterator<'a> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        loop {
            if self.curr != 0 {
                // Position of least-significant set bit
                let tz = self.curr.trailing_zeros();
                // Clear that bit
                self.curr &= self.curr - 1;
                // Return global index = chunk_offset + tz
                return Some((self.chunk_offset + tz as i64) as u32);
            }
            // Advance to next 64-bit chunk
            match self.iter.next() {
                Some(next_chunk) => {
                    // Move offset forward by 64 bits
                    self.chunk_offset += 64;
                    self.curr = next_chunk;
                }
                None => return None,
            }
        }
    }
}

/// Calls the provided closure for each index in the provided null mask that is set,
/// using an adaptive strategy based on the null count
///
/// Ideally this would be encapsulated in an [`Iterator`] that would determine the optimal
/// strategy up front, and then yield indexes based on this.
///
/// Unfortunately, external iteration based on the resulting [`Iterator`] would match the strategy
/// variant on each call to [`Iterator::next`], and LLVM generally cannot eliminate this.
///
/// One solution to this might be internal iteration, e.g. [`Iterator::try_fold`], however,
/// it is currently [not possible] to override this for custom iterators in stable Rust.
///
/// As such this is the next best option
///
/// [not possible]: https://github.com/rust-lang/rust/issues/69595
#[inline]
pub fn try_for_each_valid_idx<E, F: FnMut(usize) -> Result<(), E>>(
    len: usize,
    offset: usize,
    null_count: usize,
    nulls: Option<&[u8]>,
    f: F,
) -> Result<(), E> {
    let valid_count = len - null_count;

    if valid_count == len {
        (0..len).try_for_each(f)
    } else if null_count != len {
        BitIndexIterator::new(nulls.unwrap(), offset, len).try_for_each(f)
    } else {
        Ok(())
    }
}

// Note: further tests located in arrow_select::filter module

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BooleanBuffer;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::fmt::Debug;
    use std::iter::Copied;
    use std::slice::Iter;

    #[test]
    fn test_bit_iterator_size_hint() {
        let mut b = BitIterator::new(&[0b00000011], 0, 2);
        assert_eq!(
            b.size_hint(),
            (2, Some(2)),
            "Expected size_hint to be (2, Some(2))"
        );

        b.next();
        assert_eq!(
            b.size_hint(),
            (1, Some(1)),
            "Expected size_hint to be (1, Some(1)) after one bit consumed"
        );

        b.next();
        assert_eq!(
            b.size_hint(),
            (0, Some(0)),
            "Expected size_hint to be (0, Some(0)) after all bits consumed"
        );
    }

    #[test]
    fn test_bit_iterator() {
        let mask = &[0b00010010, 0b00100011, 0b00000101, 0b00010001, 0b10010011];
        let actual: Vec<_> = BitIterator::new(mask, 0, 5).collect();
        assert_eq!(actual, &[false, true, false, false, true]);

        let actual: Vec<_> = BitIterator::new(mask, 4, 5).collect();
        assert_eq!(actual, &[true, false, false, false, true]);

        let actual: Vec<_> = BitIterator::new(mask, 12, 14).collect();
        assert_eq!(
            actual,
            &[
                false, true, false, false, true, false, true, false, false, false, false, false,
                true, false
            ]
        );

        assert_eq!(BitIterator::new(mask, 0, 0).count(), 0);
        assert_eq!(BitIterator::new(mask, 40, 0).count(), 0);
    }

    #[test]
    #[should_panic(expected = "BitIterator buffer too small, expected 3 got 2")]
    fn test_bit_iterator_bounds() {
        let mask = &[223, 23];
        BitIterator::new(mask, 17, 0);
    }

    #[test]
    fn test_bit_index_u32_iterator_basic() {
        let mask = &[0b00010010, 0b00100011];

        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 0, 16).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 0, 16)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);

        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 4, 8).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 4, 8)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);

        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 10, 4).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 10, 4)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);

        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 0, 0).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 0, 0)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_iterator_all_set() {
        let mask = &[0xFF, 0xFF];
        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 0, 16).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 0, 16)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_iterator_none_set() {
        let mask = &[0x00, 0x00];
        let result: Vec<u32> = BitIndexU32Iterator::new(mask, 0, 16).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, 0, 16)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_cross_chunk() {
        let mut buf = vec![0u8; 16];
        for bit in 60..68 {
            let byte = (bit / 8) as usize;
            let bit_in_byte = bit % 8;
            buf[byte] |= 1 << bit_in_byte;
        }
        let offset = 58;
        let len = 10;

        let result: Vec<u32> = BitIndexU32Iterator::new(&buf, offset, len).collect();
        let expected: Vec<u32> = BitIndexIterator::new(&buf, offset, len)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_unaligned_offset() {
        let mask = &[0b0110_1100, 0b1010_0000];
        let offset = 2;
        let len = 12;

        let result: Vec<u32> = BitIndexU32Iterator::new(mask, offset, len).collect();
        let expected: Vec<u32> = BitIndexIterator::new(mask, offset, len)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_long_all_set() {
        let len = 200;
        let num_bytes = len / 8 + if len % 8 != 0 { 1 } else { 0 };
        let bytes = vec![0xFFu8; num_bytes];

        let result: Vec<u32> = BitIndexU32Iterator::new(&bytes, 0, len).collect();
        let expected: Vec<u32> = BitIndexIterator::new(&bytes, 0, len)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bit_index_u32_none_set() {
        let len = 50;
        let num_bytes = len / 8 + if len % 8 != 0 { 1 } else { 0 };
        let bytes = vec![0u8; num_bytes];

        let result: Vec<u32> = BitIndexU32Iterator::new(&bytes, 0, len).collect();
        let expected: Vec<u32> = BitIndexIterator::new(&bytes, 0, len)
            .map(|i| i as u32)
            .collect();
        assert_eq!(result, expected);
    }

    trait SharedBetweenBitIteratorAndSliceIter:
        ExactSizeIterator<Item = bool> + DoubleEndedIterator<Item = bool>
    {
    }
    impl<T: ?Sized + ExactSizeIterator<Item = bool> + DoubleEndedIterator<Item = bool>>
        SharedBetweenBitIteratorAndSliceIter for T
    {
    }

    fn get_bit_iterator_cases() -> impl Iterator<Item = (BooleanBuffer, Vec<bool>)> {
        let mut rng = StdRng::seed_from_u64(42);

        [0, 1, 6, 8, 100, 164]
            .map(|len| {
                let source = (0..len).map(|_| rng.random_bool(0.5)).collect::<Vec<_>>();

                (BooleanBuffer::from(source.as_slice()), source)
            })
            .into_iter()
    }

    fn setup_and_assert(
        setup_iters: impl Fn(&mut dyn SharedBetweenBitIteratorAndSliceIter),
        assert_fn: impl Fn(BitIterator, Copied<Iter<bool>>),
    ) {
        for (boolean_buffer, source) in get_bit_iterator_cases() {
            // Not using `boolean_buffer.iter()` in case the implementation change to not call BitIterator internally
            // in which case the test would not test what it intends to test
            let mut actual = BitIterator::new(boolean_buffer.values(), 0, boolean_buffer.len());
            let mut expected = source.iter().copied();

            setup_iters(&mut actual);
            setup_iters(&mut expected);

            assert_fn(actual, expected);
        }
    }

    /// Trait representing an operation on a BitIterator
    /// that can be compared against a slice iterator
    trait BitIteratorOp {
        /// What the operation returns (e.g. Option<bool> for last/max, usize for count, etc)
        type Output: PartialEq + Debug;

        /// The name of the operation, used for error messages
        const NAME: &'static str;

        /// Get the value of the operation for the provided iterator
        /// This will be either a BitIterator or a slice iterator to make sure they produce the same result
        fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(iter: T) -> Self::Output;
    }

    /// Helper function that will assert that the provided operation
    /// produces the same result for both BitIterator and slice iterator
    /// under various consumption patterns (e.g. some calls to next/next_back/consume_all/etc)
    fn assert_bit_iterator_cases<O: BitIteratorOp>() {
        setup_and_assert(
            |_iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {},
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();
                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                iter.next();
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from the start (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                iter.next_back();
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from the end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                iter.next();
                iter.next_back();
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming 1 element from start and end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                while iter.len() > 1 {
                    iter.next();
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the start but 1 (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                while iter.len() > 1 {
                    iter.next_back();
                }
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the end but 1 (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                while iter.next().is_some() {}
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the start (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );

        setup_and_assert(
            |iter: &mut dyn SharedBetweenBitIteratorAndSliceIter| {
                while iter.next_back().is_some() {}
            },
            |actual, expected| {
                let current_iterator_values: Vec<bool> = expected.clone().collect();

                assert_eq!(
                    O::get_value(actual),
                    O::get_value(expected),
                    "Failed on op {} for new iter after consuming all from the end (left actual, right expected) ({current_iterator_values:?})",
                    O::NAME
                );
            },
        );
    }

    #[test]
    fn assert_bit_iterator_count() {
        struct CountOp;

        impl BitIteratorOp for CountOp {
            type Output = usize;
            const NAME: &'static str = "count";

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(iter: T) -> Self::Output {
                iter.count()
            }
        }

        assert_bit_iterator_cases::<CountOp>()
    }

    #[test]
    fn assert_bit_iterator_last() {
        struct LastOp;

        impl BitIteratorOp for LastOp {
            type Output = Option<bool>;
            const NAME: &'static str = "last";

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(iter: T) -> Self::Output {
                iter.last()
            }
        }

        assert_bit_iterator_cases::<LastOp>()
    }

    #[test]
    fn assert_bit_iterator_max() {
        struct MaxOp;

        impl BitIteratorOp for MaxOp {
            type Output = Option<bool>;
            const NAME: &'static str = "max";

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(iter: T) -> Self::Output {
                iter.max()
            }
        }

        assert_bit_iterator_cases::<MaxOp>()
    }

    #[test]
    fn assert_bit_iterator_nth_0() {
        struct NthOp<const BACK: bool>;

        impl<const BACK: bool> BitIteratorOp for NthOp<BACK> {
            type Output = Option<bool>;
            const NAME: &'static str = if BACK { "nth_back(0)" } else { "nth(0)" };

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(mut iter: T) -> Self::Output {
                if BACK { iter.nth_back(0) } else { iter.nth(0) }
            }
        }

        assert_bit_iterator_cases::<NthOp<false>>();
        assert_bit_iterator_cases::<NthOp<true>>();
    }

    #[test]
    fn assert_bit_iterator_nth_1() {
        struct NthOp<const BACK: bool>;

        impl<const BACK: bool> BitIteratorOp for NthOp<BACK> {
            type Output = Option<bool>;
            const NAME: &'static str = if BACK { "nth_back(1)" } else { "nth(1)" };

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(mut iter: T) -> Self::Output {
                if BACK { iter.nth_back(1) } else { iter.nth(1) }
            }
        }

        assert_bit_iterator_cases::<NthOp<false>>();
        assert_bit_iterator_cases::<NthOp<true>>();
    }

    #[test]
    fn assert_bit_iterator_nth_after_end() {
        struct NthOp<const BACK: bool>;

        impl<const BACK: bool> BitIteratorOp for NthOp<BACK> {
            type Output = Option<bool>;
            const NAME: &'static str = if BACK {
                "nth_back(iter.len() + 1)"
            } else {
                "nth(iter.len() + 1)"
            };

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(mut iter: T) -> Self::Output {
                if BACK {
                    iter.nth_back(iter.len() + 1)
                } else {
                    iter.nth(iter.len() + 1)
                }
            }
        }

        assert_bit_iterator_cases::<NthOp<false>>();
        assert_bit_iterator_cases::<NthOp<true>>();
    }

    #[test]
    fn assert_bit_iterator_nth_len() {
        struct NthOp<const BACK: bool>;

        impl<const BACK: bool> BitIteratorOp for NthOp<BACK> {
            type Output = Option<bool>;
            const NAME: &'static str = if BACK {
                "nth_back(iter.len())"
            } else {
                "nth(iter.len())"
            };

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(mut iter: T) -> Self::Output {
                if BACK {
                    iter.nth_back(iter.len())
                } else {
                    iter.nth(iter.len())
                }
            }
        }

        assert_bit_iterator_cases::<NthOp<false>>();
        assert_bit_iterator_cases::<NthOp<true>>();
    }

    #[test]
    fn assert_bit_iterator_nth_last() {
        struct NthOp<const BACK: bool>;

        impl<const BACK: bool> BitIteratorOp for NthOp<BACK> {
            type Output = Option<bool>;
            const NAME: &'static str = if BACK {
                "nth_back(iter.len().saturating_sub(1))"
            } else {
                "nth(iter.len().saturating_sub(1))"
            };

            fn get_value<T: SharedBetweenBitIteratorAndSliceIter>(mut iter: T) -> Self::Output {
                if BACK {
                    iter.nth_back(iter.len().saturating_sub(1))
                } else {
                    iter.nth(iter.len().saturating_sub(1))
                }
            }
        }

        assert_bit_iterator_cases::<NthOp<false>>();
        assert_bit_iterator_cases::<NthOp<true>>();
    }

    #[test]
    fn assert_bit_iterator_nth_and_reuse() {
        setup_and_assert(
            |_| {},
            |actual, expected| {
                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        #[allow(clippy::iter_nth_zero)]
                        let actual_val = actual.nth(0);
                        #[allow(clippy::iter_nth_zero)]
                        let expected_val = expected.nth(0);
                        assert_eq!(actual_val, expected_val, "Failed on nth(0)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth(1);
                        let expected_val = expected.nth(1);
                        assert_eq!(actual_val, expected_val, "Failed on nth(1)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth(2);
                        let expected_val = expected.nth(2);
                        assert_eq!(actual_val, expected_val, "Failed on nth(2)");
                    }
                }
            },
        );
    }

    #[test]
    fn assert_bit_iterator_nth_back_and_reuse() {
        setup_and_assert(
            |_| {},
            |actual, expected| {
                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        #[allow(clippy::iter_nth_zero)]
                        let actual_val = actual.nth_back(0);
                        let expected_val = expected.nth_back(0);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(0)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth_back(1);
                        let expected_val = expected.nth_back(1);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(1)");
                    }
                }

                {
                    let mut actual = actual.clone();
                    let mut expected = expected.clone();
                    for _ in 0..expected.len() {
                        let actual_val = actual.nth_back(2);
                        let expected_val = expected.nth_back(2);
                        assert_eq!(actual_val, expected_val, "Failed on nth_back(2)");
                    }
                }
            },
        );
    }
}
