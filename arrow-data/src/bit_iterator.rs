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

use arrow_buffer::bit_chunk_iterator::{UnalignedBitChunk, UnalignedBitChunkIterator};
use std::result::Result;

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
    /// Create a new [`BitSliceIterator`] from the provide `buffer`,
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
        let selectivity = valid_count as f64 / len as f64;
        if selectivity > 0.8 {
            BitSliceIterator::new(nulls.unwrap(), offset, len)
                .flat_map(|(start, end)| start..end)
                .try_for_each(f)
        } else {
            BitIndexIterator::new(nulls.unwrap(), offset, len).try_for_each(f)
        }
    } else {
        Ok(())
    }
}

// Note: tests located in filter module
