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

use crate::bit_chunk_iterator::BitChunks;
use crate::bit_iterator::{BitIndexIterator, BitIterator, BitSliceIterator};
use crate::{
    bit_util, buffer_bin_and, buffer_bin_or, buffer_bin_xor, buffer_unary_not, Buffer,
    MutableBuffer,
};
use std::ops::{BitAnd, BitOr, BitXor, Not};

/// A slice-able [`Buffer`] containing bit-packed booleans
#[derive(Debug, Clone, Eq)]
pub struct BooleanBuffer {
    buffer: Buffer,
    offset: usize,
    len: usize,
}

impl PartialEq for BooleanBuffer {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }

        let lhs = self.bit_chunks().iter_padded();
        let rhs = other.bit_chunks().iter_padded();
        lhs.zip(rhs).all(|(a, b)| a == b)
    }
}

impl BooleanBuffer {
    /// Create a new [`BooleanBuffer`] from a [`Buffer`], an `offset` and `length` in bits
    ///
    /// # Panics
    ///
    /// This method will panic if `buffer` is not large enough
    pub fn new(buffer: Buffer, offset: usize, len: usize) -> Self {
        let total_len = offset.saturating_add(len);
        let bit_len = buffer.len().saturating_mul(8);
        assert!(total_len <= bit_len);
        Self {
            buffer,
            offset,
            len,
        }
    }

    /// Invokes `f` with indexes `0..len` collecting the boolean results into a new `BooleanBuffer`
    pub fn collect_bool<F: FnMut(usize) -> bool>(len: usize, f: F) -> Self {
        let buffer = MutableBuffer::collect_bool(len, f);
        Self::new(buffer.into(), 0, len)
    }

    /// Returns the number of set bits in this buffer
    pub fn count_set_bits(&self) -> usize {
        self.buffer.count_set_bits_offset(self.offset, self.len)
    }

    /// Returns a `BitChunks` instance which can be used to iterate over
    /// this buffer's bits in `u64` chunks
    pub fn bit_chunks(&self) -> BitChunks {
        BitChunks::new(self.values(), self.offset, self.len)
    }

    /// Returns `true` if the bit at index `i` is set
    ///
    /// # Panics
    ///
    /// Panics if `i >= self.len()`
    #[inline]
    #[deprecated(note = "use BooleanBuffer::value")]
    pub fn is_set(&self, i: usize) -> bool {
        self.value(i)
    }

    /// Returns the offset of this [`BooleanBuffer`] in bits
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the length of this [`BooleanBuffer`] in bits
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this [`BooleanBuffer`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the boolean value at index `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i >= self.len()`
    pub fn value(&self, idx: usize) -> bool {
        assert!(idx < self.len);
        unsafe { self.value_unchecked(idx) }
    }

    /// Returns the boolean value at index `i`.
    ///
    /// # Safety
    /// This doesn't check bounds, the caller must ensure that index < self.len()
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> bool {
        unsafe { bit_util::get_bit_raw(self.buffer.as_ptr(), i + self.offset) }
    }

    /// Returns the packed values of this [`BooleanBuffer`] not including any offset
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.buffer
    }

    /// Slices this [`BooleanBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced BooleanBuffer cannot exceed the existing length"
        );
        Self {
            buffer: self.buffer.clone(),
            offset: self.offset + offset,
            len,
        }
    }

    /// Returns a [`Buffer`] containing the sliced contents of this [`BooleanBuffer`]
    ///
    /// Equivalent to `self.buffer.bit_slice(self.offset, self.len)`
    pub fn sliced(&self) -> Buffer {
        self.buffer.bit_slice(self.offset, self.len)
    }

    /// Returns true if this [`BooleanBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.buffer.as_ptr() == other.buffer.as_ptr()
            && self.offset == other.offset
            && self.len == other.len
    }

    /// Returns the inner [`Buffer`]
    #[inline]
    pub fn inner(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the inner [`Buffer`], consuming self
    pub fn into_inner(self) -> Buffer {
        self.buffer
    }

    /// Returns an iterator over the bits in this [`BooleanBuffer`]
    pub fn iter(&self) -> BitIterator<'_> {
        self.into_iter()
    }

    /// Returns an iterator over the set bit positions in this [`BooleanBuffer`]
    pub fn set_indices(&self) -> BitIndexIterator<'_> {
        BitIndexIterator::new(self.values(), self.offset, self.len)
    }

    /// Returns a [`BitSliceIterator`] yielding contiguous ranges of set bits
    pub fn set_slices(&self) -> BitSliceIterator<'_> {
        BitSliceIterator::new(self.values(), self.offset, self.len)
    }
}

impl Not for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn not(self) -> Self::Output {
        BooleanBuffer {
            buffer: buffer_unary_not(&self.buffer, self.offset, self.len),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitAnd<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitand(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_and(
                &self.buffer,
                self.offset,
                &rhs.buffer,
                rhs.offset,
                self.len,
            ),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitOr<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_or(
                &self.buffer,
                self.offset,
                &rhs.buffer,
                rhs.offset,
                self.len,
            ),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitXor<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitxor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_xor(
                &self.buffer,
                self.offset,
                &rhs.buffer,
                rhs.offset,
                self.len,
            ),
            offset: 0,
            len: self.len,
        }
    }
}

impl<'a> IntoIterator for &'a BooleanBuffer {
    type Item = bool;
    type IntoIter = BitIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitIterator::new(self.values(), self.offset, self.len)
    }
}
