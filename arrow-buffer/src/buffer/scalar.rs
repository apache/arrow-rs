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

use crate::alloc::Deallocation;
use crate::buffer::Buffer;
use crate::native::ArrowNativeType;
use crate::{BufferBuilder, MutableBuffer, OffsetBuffer};
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Deref;

/// A strongly-typed [`Buffer`] supporting zero-copy cloning and slicing
///
/// The easiest way to think about `ScalarBuffer<T>` is being equivalent to a `Arc<Vec<T>>`,
/// with the following differences:
///
/// - slicing and cloning is O(1).
/// - it supports external allocated memory
///
/// ```
/// # use arrow_buffer::ScalarBuffer;
/// // Zero-copy conversion from Vec
/// let buffer = ScalarBuffer::from(vec![1, 2, 3]);
/// assert_eq!(&buffer, &[1, 2, 3]);
///
/// // Zero-copy slicing
/// let sliced = buffer.slice(1, 2);
/// assert_eq!(&sliced, &[2, 3]);
/// ```
#[derive(Clone)]
pub struct ScalarBuffer<T: ArrowNativeType> {
    /// Underlying data buffer
    buffer: Buffer,
    phantom: PhantomData<T>,
}

impl<T: ArrowNativeType> std::fmt::Debug for ScalarBuffer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ScalarBuffer").field(&self.as_ref()).finish()
    }
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
    /// * `buffer` is not aligned to a multiple of `std::mem::align_of::<T>`
    /// * `bytes` is not large enough for the requested slice
    pub fn new(buffer: Buffer, offset: usize, len: usize) -> Self {
        let size = std::mem::size_of::<T>();
        let byte_offset = offset.checked_mul(size).expect("offset overflow");
        let byte_len = len.checked_mul(size).expect("length overflow");
        buffer.slice_with_length(byte_offset, byte_len).into()
    }

    /// Free up unused memory.
    pub fn shrink_to_fit(&mut self) {
        self.buffer.shrink_to_fit();
    }

    /// Returns a zero-copy slice of this buffer with length `len` and starting at `offset`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self::new(self.buffer.clone(), offset, len)
    }

    /// Returns the inner [`Buffer`]
    pub fn inner(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the inner [`Buffer`], consuming self
    pub fn into_inner(self) -> Buffer {
        self.buffer
    }

    /// Returns true if this [`ScalarBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    #[inline]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.buffer.ptr_eq(&other.buffer)
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

impl<T: ArrowNativeType> From<MutableBuffer> for ScalarBuffer<T> {
    fn from(value: MutableBuffer) -> Self {
        Buffer::from(value).into()
    }
}

impl<T: ArrowNativeType> From<Buffer> for ScalarBuffer<T> {
    fn from(buffer: Buffer) -> Self {
        let align = std::mem::align_of::<T>();
        let is_aligned = buffer.as_ptr().align_offset(align) == 0;

        match buffer.deallocation() {
            Deallocation::Standard(_) => assert!(
                is_aligned,
                "Memory pointer is not aligned with the specified scalar type"
            ),
            Deallocation::Custom(_, _) =>
                assert!(is_aligned, "Memory pointer from external source (e.g, FFI) is not aligned with the specified scalar type. Before importing buffer through FFI, please make sure the allocation is aligned."),
        }

        Self {
            buffer,
            phantom: Default::default(),
        }
    }
}

impl<T: ArrowNativeType> From<OffsetBuffer<T>> for ScalarBuffer<T> {
    fn from(value: OffsetBuffer<T>) -> Self {
        value.into_inner()
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

impl<T: ArrowNativeType> From<ScalarBuffer<T>> for Vec<T> {
    fn from(value: ScalarBuffer<T>) -> Self {
        value
            .buffer
            .into_vec()
            .unwrap_or_else(|buffer| buffer.typed_data::<T>().into())
    }
}

impl<T: ArrowNativeType> From<BufferBuilder<T>> for ScalarBuffer<T> {
    fn from(mut value: BufferBuilder<T>) -> Self {
        let len = value.len();
        Self::new(value.finish(), 0, len)
    }
}

impl<T: ArrowNativeType> FromIterator<T> for ScalarBuffer<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

impl<'a, T: ArrowNativeType> IntoIterator for &'a ScalarBuffer<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_ref().iter()
    }
}

impl<T: ArrowNativeType, S: AsRef<[T]> + ?Sized> PartialEq<S> for ScalarBuffer<T> {
    fn eq(&self, other: &S) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<T: ArrowNativeType, const N: usize> PartialEq<ScalarBuffer<T>> for [T; N] {
    fn eq(&self, other: &ScalarBuffer<T>) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<T: ArrowNativeType> PartialEq<ScalarBuffer<T>> for [T] {
    fn eq(&self, other: &ScalarBuffer<T>) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<T: ArrowNativeType> PartialEq<ScalarBuffer<T>> for Vec<T> {
    fn eq(&self, other: &ScalarBuffer<T>) -> bool {
        self.as_slice().eq(other.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::{ptr::NonNull, sync::Arc};

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
    fn test_debug() {
        let buffer = ScalarBuffer::from(vec![1, 2, 3]);
        assert_eq!(format!("{buffer:?}"), "ScalarBuffer([1, 2, 3])");
    }

    #[test]
    #[should_panic(expected = "Memory pointer is not aligned with the specified scalar type")]
    fn test_unaligned() {
        let expected = [0_i32, 1, 2];
        let buffer = Buffer::from_iter(expected.iter().cloned());
        let buffer = buffer.slice(1);
        ScalarBuffer::<i32>::new(buffer, 0, 2);
    }

    #[test]
    #[should_panic(expected = "the offset of the new Buffer cannot exceed the existing length")]
    fn test_length_out_of_bounds() {
        let buffer = Buffer::from_iter([0_i32, 1, 2]);
        ScalarBuffer::<i32>::new(buffer, 1, 3);
    }

    #[test]
    #[should_panic(expected = "the offset of the new Buffer cannot exceed the existing length")]
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

    #[test]
    fn convert_from_buffer_builder() {
        let input = vec![1, 2, 3, 4];
        let buffer_builder = BufferBuilder::from(input.clone());
        let scalar_buffer = ScalarBuffer::from(buffer_builder);
        assert_eq!(scalar_buffer.as_ref(), input);
    }

    #[test]
    fn into_vec() {
        let input = vec![1u8, 2, 3, 4];

        // No copy
        let input_buffer = Buffer::from_vec(input.clone());
        let input_ptr = input_buffer.as_ptr();
        let input_len = input_buffer.len();
        let scalar_buffer = ScalarBuffer::<u8>::new(input_buffer, 0, input_len);
        let vec = Vec::from(scalar_buffer);
        assert_eq!(vec.as_slice(), input.as_slice());
        assert_eq!(vec.as_ptr(), input_ptr);

        // Custom allocation - makes a copy
        let mut input_clone = input.clone();
        let input_ptr = NonNull::new(input_clone.as_mut_ptr()).unwrap();
        let dealloc = Arc::new(());
        let buffer =
            unsafe { Buffer::from_custom_allocation(input_ptr, input_clone.len(), dealloc as _) };
        let scalar_buffer = ScalarBuffer::<u8>::new(buffer, 0, input.len());
        let vec = Vec::from(scalar_buffer);
        assert_eq!(vec, input.as_slice());
        assert_ne!(vec.as_ptr(), input_ptr.as_ptr());

        // Offset - makes a copy
        let input_buffer = Buffer::from_vec(input.clone());
        let input_ptr = input_buffer.as_ptr();
        let input_len = input_buffer.len();
        let scalar_buffer = ScalarBuffer::<u8>::new(input_buffer, 1, input_len - 1);
        let vec = Vec::from(scalar_buffer);
        assert_eq!(vec.as_slice(), &input[1..]);
        assert_ne!(vec.as_ptr(), input_ptr);

        // Inner buffer Arc ref count != 0 - makes a copy
        let buffer = Buffer::from_slice_ref(input.as_slice());
        let scalar_buffer = ScalarBuffer::<u8>::new(buffer, 0, input.len());
        let vec = Vec::from(scalar_buffer);
        assert_eq!(vec, input.as_slice());
        assert_ne!(vec.as_ptr(), input.as_ptr());
    }
}
