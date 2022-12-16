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
use std::ops::Deref;

/// Provides a safe API for interpreting a [`Buffer`] as a slice of [`ArrowNativeType`]
///
/// # Safety
///
/// All [`ArrowNativeType`] are valid for all possible backing byte representations, and as
/// a result they are "trivially safely transmutable".
#[derive(Debug)]
pub struct ScalarBuffer<T: ArrowNativeType> {
    #[allow(unused)]
    buffer: Buffer,
    // Borrows from `buffer` and is valid for the lifetime of `buffer`
    ptr: *const T,
    // The length of this slice
    len: usize,
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
        let offset_len = offset.checked_add(len).expect("length overflow");
        let start_bytes = offset.checked_mul(size).expect("start bytes overflow");
        let end_bytes = offset_len.checked_mul(size).expect("end bytes overflow");

        let bytes = &buffer.as_slice()[start_bytes..end_bytes];

        // SAFETY: all byte sequences correspond to a valid instance of T
        let (prefix, offsets, suffix) = unsafe { bytes.align_to::<T>() };
        assert!(
            prefix.is_empty() && suffix.is_empty(),
            "buffer is not aligned to {} byte boundary",
            size
        );

        let ptr = offsets.as_ptr();
        Self { buffer, ptr, len }
    }
}

impl<T: ArrowNativeType> Deref for ScalarBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        // SAFETY: Bounds checked in constructor and ptr is valid for the lifetime of self
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for ScalarBuffer<T> {
    fn as_ref(&self) -> &[T] {
        self
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
    #[should_panic(expected = "buffer is not aligned to 4 byte boundary")]
    fn test_unaligned() {
        let expected = [0_i32, 1, 2];
        let buffer = Buffer::from_iter(expected.iter().cloned());
        let buffer = buffer.slice(1);
        ScalarBuffer::<i32>::new(buffer, 0, 2);
    }

    #[test]
    #[should_panic(expected = "range end index 16 out of range for slice of length 12")]
    fn test_length_out_of_bounds() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 1, 3);
    }

    #[test]
    #[should_panic(expected = "range end index 16 out of range for slice of length 12")]
    fn test_offset_out_of_bounds() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 4, 0);
    }

    #[test]
    #[should_panic(expected = "length overflow")]
    fn test_length_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, usize::MAX, 1);
    }

    #[test]
    #[should_panic(expected = "start bytes overflow")]
    fn test_start_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, usize::MAX / 4 + 1, 0);
    }

    #[test]
    #[should_panic(expected = "end bytes overflow")]
    fn test_end_overflow() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 0, usize::MAX / 4 + 1);
    }
}
