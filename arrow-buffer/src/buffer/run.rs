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

use crate::buffer::ScalarBuffer;
use crate::ArrowNativeType;

/// A slice-able buffer of monotonically increasing, positive integers used to store run-ends
///
/// In order to provide zero-copy slicing, this container stores a separate offset and length
///
/// For example, a [RunEndBuffer] containing values `[3, 6, 8]` with offset and length `4` would
/// describe the value indices `1, 1, 2, 2` for a RunArray
///
/// For example, a [RunEndBuffer] containing values `[6, 8, 9]` with offset `2` and length `5`
/// would describe the value indices `0, 0, 0, 0, 1` for a RunArray
#[derive(Debug, Clone)]
pub struct RunEndBuffer<E: ArrowNativeType> {
    run_ends: ScalarBuffer<E>,
    len: usize,
    offset: usize,
}

impl<E> RunEndBuffer<E>
where
    E: ArrowNativeType,
{
    /// Create a new [`RunEndBuffer`] from a [`ScalarBuffer`], an `offset` and `len`
    pub fn new(run_ends: ScalarBuffer<E>, offset: usize, len: usize) -> Self {
        assert!(
            run_ends.windows(2).all(|w| w[0] < w[1]),
            "run-ends not strictly increasing"
        );

        if len != 0 {
            assert!(!run_ends.is_empty(), "non-empty slice but empty run-ends");
            let end = E::from_usize(offset.saturating_add(len)).unwrap();
            assert!(
                *run_ends.first().unwrap() >= E::usize_as(0),
                "run-ends not greater than 0"
            );
            assert!(
                *run_ends.last().unwrap() >= end,
                "slice beyond bounds of run-ends"
            );
        }

        Self {
            run_ends,
            offset,
            len,
        }
    }

    /// Create a new [`RunEndBuffer`] from an [`ScalarBuffer`], an `offset` and `len`
    ///
    /// # Safety
    ///
    /// - `buffer` must contain strictly increasing values greater than zero
    /// - The last value of `buffer` must be greater than or equal to `offset + len`
    pub unsafe fn new_unchecked(
        run_ends: ScalarBuffer<E>,
        offset: usize,
        len: usize,
    ) -> Self {
        Self {
            run_ends,
            offset,
            len,
        }
    }

    /// Returns the logical offset into the run-ends stored by this buffer
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the logical length of the run-ends stored by this buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the values of this [`RunEndBuffer`] not including any offset
    #[inline]
    pub fn values(&self) -> &[E] {
        &self.run_ends
    }

    /// Returns the maximum run-end encoded in the underlying buffer
    #[inline]
    pub fn max_value(&self) -> usize {
        self.values().last().copied().unwrap_or_default().as_usize()
    }

    /// Performs a binary search to find the physical index for the given logical index
    pub fn get_physical_index(&self, logical_index: usize) -> Option<usize> {
        if logical_index > self.len {
            return None;
        }
        let logical_index = E::usize_as(self.offset + logical_index);
        let cmp = |p: &E| p.partial_cmp(&logical_index).unwrap();

        match self.run_ends.binary_search_by(cmp) {
            Ok(idx) => Some(idx + 1),
            Err(idx) => Some(idx),
        }
    }

    /// Returns the physical index at which the array slice starts
    pub fn get_start_physical_index(&self) -> usize {
        if self.offset == 0 {
            return 0;
        }
        // Fallback to binary search
        self.get_physical_index(0).unwrap_or_default()
    }

    /// Returns the physical index at which the array slice ends
    pub fn get_end_physical_index(&self) -> usize {
        if self.max_value() == self.offset + self.len {
            return self.values().len() - 1;
        }
        // Fallback to binary search
        self.get_physical_index(self.len - 1).unwrap_or_default()
    }

    /// Slices this [`RunEndBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced RunEndBuffer cannot exceed the existing length"
        );
        Self {
            run_ends: self.run_ends.clone(),
            offset: self.offset + offset,
            len,
        }
    }
}
