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

impl<'a> Iterator for BitIterator<'a> {
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
}

impl<'a> ExactSizeIterator for BitIterator<'a> {}

impl<'a> DoubleEndedIterator for BitIterator<'a> {
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
}

/// Iterator of contiguous ranges of set bits within a provided packed bitmask
///
/// Returns `(usize, usize)` each representing an interval where the corresponding
/// bits in the provides mask are set
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

impl<'a> Iterator for BitSliceIterator<'a> {
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

impl<'a> Iterator for BitIndexIterator<'a> {
    type Item = usize;

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
}
