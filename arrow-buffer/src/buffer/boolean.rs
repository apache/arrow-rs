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

use crate::{bit_util, Buffer};

#[derive(Debug, Clone)]
pub struct BooleanBuffer {
    buffer: Buffer,
    offset: usize,
    len: usize,
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
}
