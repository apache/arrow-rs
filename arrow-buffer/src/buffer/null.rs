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

use crate::bit_iterator::BitIndexIterator;
use crate::buffer::BooleanBuffer;
use crate::{Buffer, MutableBuffer};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NullBuffer {
    buffer: BooleanBuffer,
    null_count: usize,
}

impl NullBuffer {
    /// Create a new [`NullBuffer`] computing the null count
    pub fn new(buffer: BooleanBuffer) -> Self {
        let null_count = buffer.len() - buffer.count_set_bits();
        Self { buffer, null_count }
    }

    /// Create a new [`NullBuffer`] of length `len` where all values are null
    pub fn new_null(len: usize) -> Self {
        let buffer = MutableBuffer::new_null(len).into_buffer();
        let buffer = BooleanBuffer::new(buffer, 0, len);
        Self {
            buffer,
            null_count: len,
        }
    }

    /// Create a new [`NullBuffer`] with the provided `buffer` and `null_count`
    ///
    /// # Safety
    ///
    /// `buffer` must contain `null_count` `0` bits
    pub unsafe fn new_unchecked(buffer: BooleanBuffer, null_count: usize) -> Self {
        Self { buffer, null_count }
    }

    /// Computes the union of two optional [`NullBuffer`]
    pub fn union(
        lhs: Option<&NullBuffer>,
        rhs: Option<&NullBuffer>,
    ) -> Option<NullBuffer> {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Some(Self::new(lhs.inner() & rhs.inner())),
            (Some(n), None) | (None, Some(n)) => Some(n.clone()),
            (None, None) => None,
        }
    }

    /// Returns the length of this [`NullBuffer`]
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the offset of this [`NullBuffer`] in bits
    #[inline]
    pub fn offset(&self) -> usize {
        self.buffer.offset()
    }

    /// Returns true if this [`NullBuffer`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the null count for this [`NullBuffer`]
    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }

    /// Returns `true` if the value at `idx` is not null
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.buffer.is_set(idx)
    }

    /// Returns `true` if the value at `idx` is null
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        !self.is_valid(idx)
    }

    /// Returns the packed validity of this [`NullBuffer`] not including any offset
    #[inline]
    pub fn validity(&self) -> &[u8] {
        self.buffer.values()
    }

    /// Slices this [`NullBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self::new(self.buffer.slice(offset, len))
    }

    /// Calls the provided closure for each index in this null mask that is set
    #[inline]
    pub fn try_for_each_valid_idx<E, F: FnMut(usize) -> Result<(), E>>(
        &self,
        f: F,
    ) -> Result<(), E> {
        if self.null_count == self.len() {
            return Ok(());
        }
        BitIndexIterator::new(self.validity(), self.offset(), self.len()).try_for_each(f)
    }

    /// Returns the inner [`BooleanBuffer`]
    #[inline]
    pub fn inner(&self) -> &BooleanBuffer {
        &self.buffer
    }

    /// Returns the inner [`BooleanBuffer`]
    #[inline]
    pub fn into_inner(self) -> BooleanBuffer {
        self.buffer
    }

    /// Returns the underlying [`Buffer`]
    #[inline]
    pub fn buffer(&self) -> &Buffer {
        self.buffer.inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size() {
        // This tests that the niche optimisation eliminates the overhead of an option
        assert_eq!(
            std::mem::size_of::<NullBuffer>(),
            std::mem::size_of::<Option<NullBuffer>>()
        );
    }
}
