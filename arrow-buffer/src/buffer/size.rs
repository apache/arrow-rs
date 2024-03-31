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

use crate::{ArrowNativeType, MutableBuffer, ScalarBuffer};
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct SizeBuffer<O: ArrowNativeType>(ScalarBuffer<O>);

impl<O: ArrowNativeType> SizeBuffer<O> {
    /// Create a new [`SizeBuffer`] containing `len` `0` values
    pub fn new_zeroed(len: usize) -> Self {
        let len_bytes = len.checked_mul(std::mem::size_of::<O>()).expect("overflow");
        let buffer = MutableBuffer::from_len_zeroed(len_bytes);
        Self(buffer.into_buffer().into())
    }

    /// Create a new [`SizeBuffer`] from the provided [`ScalarBuffer`]
    pub fn new(buffer: ScalarBuffer<O>) -> Self {
        assert!(!buffer.is_empty(), "offsets cannot be empty");
        assert!(
            buffer[0] >= O::usize_as(0),
            "offsets must be greater than 0"
        );
        Self(buffer)
    }

    /// Create a new [`SizeBuffer`] containing a single 0 value
    pub fn new_empty() -> Self {
        let buffer = MutableBuffer::from_len_zeroed(std::mem::size_of::<O>());
        Self(buffer.into_buffer().into())
    }

    /// Create a new [`SizeBuffer`] from the iterator of slice lengths
    ///
    /// ```
    /// # use arrow_buffer::SizeBuffer;
    /// let offsets = SizeBuffer::<i32>::from_lengths([1, 3, 5]);
    /// assert_eq!(offsets.as_ref(), &[1, 3, 5]);
    /// ```
    pub fn from_lengths<I>(lengths: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        let iter = lengths.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        for size in iter {
            out.push(O::usize_as(size))
        }
        Self(out.into())
    }

    /// Returns the inner [`ScalarBuffer`]
    pub fn inner(&self) -> &ScalarBuffer<O> {
        &self.0
    }

    /// Returns the inner [`ScalarBuffer`], consuming self
    pub fn into_inner(self) -> ScalarBuffer<O> {
        self.0
    }

    /// Returns a zero-copy slice of this buffer with length `len` and starting at `offset`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self(self.0.slice(offset, len.saturating_add(1)))
    }

    /// Returns true if this [`ScalarBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    #[inline]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

impl<T: ArrowNativeType> Deref for SizeBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for SizeBuffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self
    }
}
