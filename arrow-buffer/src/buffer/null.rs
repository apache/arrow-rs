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

use crate::buffer::BooleanBuffer;

#[derive(Debug, Clone)]
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

    /// Create a new [`NullBuffer`] with the provided `buffer` and `null_count`
    ///
    /// # Safety
    ///
    /// `buffer` must contain `null_count` `0` bits
    pub unsafe fn new_unchecked(buffer: BooleanBuffer, null_count: usize) -> Self {
        Self { buffer, null_count }
    }

    /// Returns the length of this [`NullBuffer`]
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
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

    /// Returns the inner buffer
    #[inline]
    pub fn inner(&self) -> &BooleanBuffer {
        &self.buffer
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
