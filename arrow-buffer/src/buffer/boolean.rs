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
use crate::{bit_util, Buffer};

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

        let lhs = self.bit_chunks();
        let rhs = other.bit_chunks();

        if lhs.iter().zip(rhs.iter()).any(|(a, b)| a != b) {
            return false;
        }
        lhs.remainder_bits() == rhs.remainder_bits()
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
    pub fn is_set(&self, i: usize) -> bool {
        assert!(i < self.len);
        unsafe { bit_util::get_bit_raw(self.buffer.as_ptr(), i + self.offset) }
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

    /// Returns the packed values of this [`BooleanBuffer`] not including any offset
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.buffer
    }

    /// Slices this [`BooleanBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the offset of the new BooleanBuffer cannot exceed the existing length"
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
}
