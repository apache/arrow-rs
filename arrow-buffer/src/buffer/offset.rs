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
use crate::{ArrowNativeType, MutableBuffer};
use std::ops::Deref;

/// A non-empty buffer of monotonically increasing, positive integers
#[derive(Debug, Clone)]
pub struct OffsetBuffer<O: ArrowNativeType>(ScalarBuffer<O>);

impl<O: ArrowNativeType> OffsetBuffer<O> {
    /// Create a new [`OffsetBuffer`] from the provided [`ScalarBuffer`]
    ///
    /// # Safety
    ///
    /// `buffer` must be a non-empty buffer containing monotonically increasing
    /// values greater than zero
    pub unsafe fn new_unchecked(buffer: ScalarBuffer<O>) -> Self {
        Self(buffer)
    }

    /// Create a new [`OffsetBuffer`] containing a single 0 value
    pub fn new_empty() -> Self {
        let buffer = MutableBuffer::from_len_zeroed(std::mem::size_of::<O>());
        Self(buffer.into_buffer().into())
    }

    /// Returns the inner [`ScalarBuffer`]
    pub fn inner(&self) -> &ScalarBuffer<O> {
        &self.0
    }

    /// Returns the inner [`ScalarBuffer`]
    pub fn into_inner(self) -> ScalarBuffer<O> {
        self.0
    }

    /// Returns a zero-copy slice of this buffer with length `len` and starting at `offset`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self(self.0.slice(offset, len.saturating_add(1)))
    }
}

impl<T: ArrowNativeType> Deref for OffsetBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for OffsetBuffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self
    }
}
