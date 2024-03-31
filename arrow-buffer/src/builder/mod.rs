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

//! Buffer builders

mod boolean;
mod null;
mod offset;

pub use boolean::*;
pub use null::*;
pub use offset::*;

use crate::{ArrowNativeType, Buffer, MutableBuffer};
use std::{iter, marker::PhantomData};

/// Builder for creating a [Buffer] object.
///
/// A [Buffer] is the underlying data structure of Arrow's Arrays.
///
/// For all supported types, there are type definitions for the
/// generic version of `BufferBuilder<T>`, e.g. `BufferBuilder`.
///
/// # Example:
///
/// ```
/// # use arrow_buffer::builder::BufferBuilder;
///
/// let mut builder = BufferBuilder::<u8>::new(100);
/// builder.append_slice(&[42, 43, 44]);
/// builder.append(45);
/// let buffer = builder.finish();
///
/// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 43, 44, 45]);
/// ```
#[derive(Debug)]
pub struct BufferBuilder<T: ArrowNativeType> {
    buffer: MutableBuffer,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T: ArrowNativeType> BufferBuilder<T> {
    /// Creates a new builder with initial capacity for _at least_ `capacity`
    /// elements of type `T`.
    ///
    /// The capacity can later be manually adjusted with the
    /// [`reserve()`](BufferBuilder::reserve) method.
    /// Also the
    /// [`append()`](BufferBuilder::append),
    /// [`append_slice()`](BufferBuilder::append_slice) and
    /// [`advance()`](BufferBuilder::advance)
    /// methods automatically increase the capacity if needed.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    ///
    /// assert!(builder.capacity() >= 10);
    /// ```
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let buffer = MutableBuffer::new(capacity * std::mem::size_of::<T>());

        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Creates a new builder from a [`MutableBuffer`]
    pub fn new_from_buffer(buffer: MutableBuffer) -> Self {
        let buffer_len = buffer.len();
        Self {
            buffer,
            len: buffer_len / std::mem::size_of::<T>(),
            _marker: PhantomData,
        }
    }

    /// Returns the current number of array elements in the internal buffer.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the internal buffer is empty.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the actual capacity (number of elements) of the internal buffer.
    ///
    /// Note: the internal capacity returned by this method might be larger than
    /// what you'd expect after setting the capacity in the `new()` or `reserve()`
    /// functions.
    pub fn capacity(&self) -> usize {
        let byte_capacity = self.buffer.capacity();
        byte_capacity / std::mem::size_of::<T>()
    }

    /// Increases the number of elements in the internal buffer by `n`
    /// and resizes the buffer as needed.
    ///
    /// The values of the newly added elements are 0.
    /// This method is usually used when appending `NULL` values to the buffer
    /// as they still require physical memory space.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.advance(2);
    ///
    /// assert_eq!(builder.len(), 2);
    /// ```
    #[inline]
    pub fn advance(&mut self, i: usize) {
        self.buffer.extend_zeros(i * std::mem::size_of::<T>());
        self.len += i;
    }

    /// Reserves memory for _at least_ `n` more elements of type `T`.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.reserve(10);
    ///
    /// assert!(builder.capacity() >= 20);
    /// ```
    #[inline]
    pub fn reserve(&mut self, n: usize) {
        self.buffer.reserve(n * std::mem::size_of::<T>());
    }

    /// Appends a value of type `T` into the builder,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    #[inline]
    pub fn append(&mut self, v: T) {
        self.reserve(1);
        self.buffer.push(v);
        self.len += 1;
    }

    /// Appends a value of type `T` into the builder N times,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append_n(10, 42);
    ///
    /// assert_eq!(builder.len(), 10);
    /// ```
    #[inline]
    pub fn append_n(&mut self, n: usize, v: T) {
        self.reserve(n);
        self.extend(iter::repeat(v).take(n))
    }

    /// Appends `n`, zero-initialized values
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u32>::new(10);
    /// builder.append_n_zeroed(3);
    ///
    /// assert_eq!(builder.len(), 3);
    /// assert_eq!(builder.as_slice(), &[0, 0, 0])
    #[inline]
    pub fn append_n_zeroed(&mut self, n: usize) {
        self.buffer.extend_zeros(n * std::mem::size_of::<T>());
        self.len += n;
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// assert_eq!(builder.len(), 3);
    /// ```
    #[inline]
    pub fn append_slice(&mut self, slice: &[T]) {
        self.buffer.extend_from_slice(slice);
        self.len += slice.len();
    }

    /// View the contents of this buffer as a slice
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<f64>::new(10);
    /// builder.append(1.3);
    /// builder.append_n(2, 2.3);
    ///
    /// assert_eq!(builder.as_slice(), &[1.3, 2.3, 2.3]);
    /// ```
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY
        //
        // - MutableBuffer is aligned and initialized for len elements of T
        // - MutableBuffer corresponds to a single allocation
        // - MutableBuffer does not support modification whilst active immutable borrows
        unsafe { std::slice::from_raw_parts(self.buffer.as_ptr() as _, self.len) }
    }

    /// View the contents of this buffer as a mutable slice
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<f32>::new(10);
    ///
    /// builder.append_slice(&[1., 2., 3.4]);
    /// assert_eq!(builder.as_slice(), &[1., 2., 3.4]);
    ///
    /// builder.as_slice_mut()[1] = 4.2;
    /// assert_eq!(builder.as_slice(), &[1., 4.2, 3.4]);
    /// ```
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        // SAFETY
        //
        // - MutableBuffer is aligned and initialized for len elements of T
        // - MutableBuffer corresponds to a single allocation
        // - MutableBuffer does not support modification whilst active immutable borrows
        unsafe { std::slice::from_raw_parts_mut(self.buffer.as_mut_ptr() as _, self.len) }
    }

    /// Shorten this BufferBuilder to `len` items
    ///
    /// If `len` is greater than the builder's current length, this has no effect
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u16>::new(10);
    ///
    /// builder.append_slice(&[42, 44, 46]);
    /// assert_eq!(builder.as_slice(), &[42, 44, 46]);
    ///
    /// builder.truncate(2);
    /// assert_eq!(builder.as_slice(), &[42, 44]);
    ///
    /// builder.append(12);
    /// assert_eq!(builder.as_slice(), &[42, 44, 12]);
    /// ```
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.buffer.truncate(len * std::mem::size_of::<T>());
        self.len = len;
    }

    /// # Safety
    /// This requires the iterator be a trusted length. This could instead require
    /// the iterator implement `TrustedLen` once that is stabilized.
    #[inline]
    pub unsafe fn append_trusted_len_iter(&mut self, iter: impl IntoIterator<Item = T>) {
        let iter = iter.into_iter();
        let len = iter
            .size_hint()
            .1
            .expect("append_trusted_len_iter expects upper bound");
        self.reserve(len);
        self.extend(iter);
    }

    /// Resets this builder and returns an immutable [Buffer].
    ///
    /// # Example:
    ///
    /// ```
    /// # use arrow_buffer::builder::BufferBuilder;
    ///
    /// let mut builder = BufferBuilder::<u8>::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// let buffer = builder.finish();
    ///
    /// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 44, 46]);
    /// ```
    #[inline]
    pub fn finish(&mut self) -> Buffer {
        let buf = std::mem::take(&mut self.buffer);
        self.len = 0;
        buf.into()
    }
}

impl<T: ArrowNativeType> Default for BufferBuilder<T> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<T: ArrowNativeType> Extend<T> for BufferBuilder<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.buffer.extend(iter.into_iter().inspect(|_| {
            self.len += 1;
        }))
    }
}

impl<T: ArrowNativeType> From<Vec<T>> for BufferBuilder<T> {
    fn from(value: Vec<T>) -> Self {
        Self::new_from_buffer(MutableBuffer::from(value))
    }
}

impl<T: ArrowNativeType> FromIterator<T> for BufferBuilder<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut builder = Self::default();
        builder.extend(iter);
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn default() {
        let builder = BufferBuilder::<u32>::default();
        assert!(builder.is_empty());
        assert!(builder.buffer.is_empty());
        assert_eq!(builder.buffer.capacity(), 0);
    }

    #[test]
    fn from_iter() {
        let input = [1u16, 2, 3, 4];
        let builder = input.into_iter().collect::<BufferBuilder<_>>();
        assert_eq!(builder.len(), 4);
        assert_eq!(builder.buffer.len(), 4 * mem::size_of::<u16>());
    }

    #[test]
    fn extend() {
        let input = [1, 2];
        let mut builder = input.into_iter().collect::<BufferBuilder<_>>();
        assert_eq!(builder.len(), 2);
        builder.extend([3, 4]);
        assert_eq!(builder.len(), 4);
    }
}
