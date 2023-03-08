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

use crate::buffer::Buffer;
use crate::native::ArrowNativeType;
use std::marker::PhantomData;
use std::ops::Deref;

/// Provides a safe API for interpreting a [`Buffer`] as a slice of [`ArrowNativeType`]
///
/// # Safety
///
/// All [`ArrowNativeType`] are valid for all possible backing byte representations, and as
/// a result they are "trivially safely transmutable".
#[derive(Debug, Clone)]
pub struct ScalarBuffer<T: ArrowNativeType> {
    /// Underlying data buffer
    buffer: Buffer,
    phantom: PhantomData<T>,
}

impl<T: ArrowNativeType> ScalarBuffer<T> {
    /// Create a new [`ScalarBuffer`] from a [`Buffer`], and an `offset`
    /// and `length` in units of `T`
    ///
    /// # Panics
    ///
    /// This method will panic if
    ///
    /// * `offset` or `len` would result in overflow
    /// * `buffer` is not aligned to a multiple of `std::mem::size_of::<T>`
    /// * `bytes` is not large enough for the requested slice
    pub fn new(buffer: Buffer, offset: usize, len: usize) -> Self {
        let size = std::mem::size_of::<T>();
        let byte_offset = offset.checked_mul(size).expect("offset overflow");
        let byte_len = len.checked_mul(size).expect("length overflow");
        buffer.slice_with_length(byte_offset, byte_len).into()
    }

    /// Returns a zero-copy slice of this buffer with length `len` and starting at `offset`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self::new(self.buffer.clone(), offset, len)
    }

    /// Returns the inner [`Buffer`]
    pub fn inner(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the inner [`Buffer`]
    pub fn into_inner(self) -> Buffer {
        self.buffer
    }
}

impl<T: ArrowNativeType> Deref for ScalarBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: Verified alignment in From<Buffer>
        unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const T,
                self.buffer.len() / std::mem::size_of::<T>(),
            )
        }
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for ScalarBuffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T: ArrowNativeType> From<Buffer> for ScalarBuffer<T> {
    fn from(buffer: Buffer) -> Self {
        let align = std::mem::align_of::<T>();
        assert_eq!(
            buffer.as_ptr().align_offset(align),
            0,
            "memory is not aligned"
        );

        Self {
            buffer,
            phantom: Default::default(),
        }
    }
}

impl<T: ArrowNativeType> From<Vec<T>> for ScalarBuffer<T> {
    fn from(value: Vec<T>) -> Self {
        Self {
            buffer: Buffer::from_vec(value),
            phantom: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let expected = [0_i32, 1, 2];
        let buffer = Buffer::from_iter(expected.iter().cloned());
        let typed = ScalarBuffer::<i32>::new(buffer.clone(), 0, 3);
        assert_eq!(*typed, expected);

        let typed = ScalarBuffer::<i32>::new(buffer.clone(), 1, 2);
        assert_eq!(*typed, expected[1..]);

        let typed = ScalarBuffer::<i32>::new(buffer.clone(), 1, 0);
        assert!(typed.is_empty());

        let typed = ScalarBuffer::<i32>::new(buffer, 3, 0);
        assert!(typed.is_empty());
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_unaligned() {
        let expected = [0_i32, 1, 2];
        let buffer = Buffer::from_iter(expected.iter().cloned());
        let buffer = buffer.slice(1);
        ScalarBuffer::<i32>::new(buffer, 0, 2);
    }

    #[test]
    #[should_panic(
        expected = "the offset of the new Buffer cannot exceed the existing length"
    )]
    fn test_length_out_of_bounds() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 1, 3);
    }

    #[test]
    #[should_panic(
        expected = "the offset of the new Buffer cannot exceed the existing length"
    )]
    fn test_offset_out_of_bounds() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 4, 0);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn test_length_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, usize::MAX, 1);
    }

    #[test]
    #[should_panic(expected = "offset overflow")]
    fn test_start_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, usize::MAX / 4 + 1, 0);
    }

    #[test]
    #[should_panic(expected = "length overflow")]
    fn test_end_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 0, usize::MAX / 4 + 1);
    }
}
