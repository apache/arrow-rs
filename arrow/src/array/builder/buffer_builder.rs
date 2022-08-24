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

use std::mem;

use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::ArrowNativeType;

use super::PhantomData;

/// Builder for creating a [`Buffer`](crate::buffer::Buffer) object.
///
/// A [`Buffer`](crate::buffer::Buffer) is the underlying data
/// structure of Arrow's [`Arrays`](crate::array::Array).
///
/// For all supported types, there are type definitions for the
/// generic version of `BufferBuilder<T>`, e.g. `UInt8BufferBuilder`.
///
/// # Example:
///
/// ```
/// use arrow::array::UInt8BufferBuilder;
///
/// # fn main() -> arrow::error::Result<()> {
/// let mut builder = UInt8BufferBuilder::new(100);
/// builder.append_slice(&[42, 43, 44]);
/// builder.append(45);
/// let buffer = builder.finish();
///
/// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 43, 44, 45]);
/// # Ok(())
/// # }
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
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    ///
    /// assert!(builder.capacity() >= 10);
    /// ```
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let buffer = MutableBuffer::new(capacity * mem::size_of::<T>());

        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Returns the current number of array elements in the internal buffer.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
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
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
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
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.advance(2);
    ///
    /// assert_eq!(builder.len(), 2);
    /// ```
    #[inline]
    pub fn advance(&mut self, i: usize) {
        self.buffer.extend_zeros(i * mem::size_of::<T>());
        self.len += i;
    }

    /// Reserves memory for _at least_ `n` more elements of type `T`.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.reserve(10);
    ///
    /// assert!(builder.capacity() >= 20);
    /// ```
    #[inline]
    pub fn reserve(&mut self, n: usize) {
        self.buffer.reserve(n * mem::size_of::<T>());
    }

    /// Appends a value of type `T` into the builder,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
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
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_n(10, 42);
    ///
    /// assert_eq!(builder.len(), 10);
    /// ```
    #[inline]
    pub fn append_n(&mut self, n: usize, v: T) {
        self.reserve(n);
        for _ in 0..n {
            self.buffer.push(v);
        }
        self.len += n;
    }

    /// Appends `n`, zero-initialized values
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt32BufferBuilder;
    ///
    /// let mut builder = UInt32BufferBuilder::new(10);
    /// builder.append_n_zeroed(3);
    ///
    /// assert_eq!(builder.len(), 3);
    /// assert_eq!(builder.as_slice(), &[0, 0, 0])
    #[inline]
    pub fn append_n_zeroed(&mut self, n: usize) {
        self.buffer.extend_zeros(n * mem::size_of::<T>());
        self.len += n;
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
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
    /// use arrow::array::Float64BufferBuilder;
    ///
    /// let mut builder = Float64BufferBuilder::new(10);
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
    /// use arrow::array::Float32BufferBuilder;
    ///
    /// let mut builder = Float32BufferBuilder::new(10);
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
    /// use arrow::array::UInt16BufferBuilder;
    ///
    /// let mut builder = UInt16BufferBuilder::new(10);
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
        self.buffer.truncate(len * mem::size_of::<T>());
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
        for v in iter {
            self.buffer.push(v)
        }
        self.len += len;
    }

    /// Resets this builder and returns an immutable [`Buffer`](crate::buffer::Buffer).
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// let buffer = builder.finish();
    ///
    /// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 44, 46]);
    /// ```
    #[inline]
    pub fn finish(&mut self) -> Buffer {
        let buf = std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
        self.len = 0;
        buf.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::array::Array;
    use crate::array::builder::ArrayBuilder;
    use crate::array::Int32BufferBuilder;
    use crate::array::Int8Builder;
    use crate::array::UInt8BufferBuilder;

    #[test]
    fn test_builder_i32_empty() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(0, b.len());
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b = Int32BufferBuilder::new(0);
        b.append(123);
        let a = b.finish();
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b = Int32BufferBuilder::new(5);
        for i in 0..5 {
            b.append(i);
        }
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(20, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        for i in 0..20 {
            b.append(i);
        }
        assert_eq!(32, b.capacity());
        let a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_builder_finish() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(16, b.capacity());
        for i in 0..10 {
            b.append(i);
        }
        let mut a = b.finish();
        assert_eq!(40, a.len());
        assert_eq!(0, b.len());
        assert_eq!(0, b.capacity());

        // Try build another buffer after cleaning up.
        for i in 0..20 {
            b.append(i)
        }
        assert_eq!(32, b.capacity());
        a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b = UInt8BufferBuilder::new(2);
        assert_eq!(64, b.capacity());
        b.reserve(64);
        assert_eq!(64, b.capacity());
        b.reserve(65);
        assert_eq!(128, b.capacity());

        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        b.reserve(16);
        assert_eq!(16, b.capacity());
        b.reserve(17);
        assert_eq!(32, b.capacity());
    }

    #[test]
    fn test_append_slice() {
        let mut b = UInt8BufferBuilder::new(0);
        b.append_slice(b"Hello, ");
        b.append_slice(b"World!");
        let buffer = b.finish();
        assert_eq!(13, buffer.len());

        let mut b = Int32BufferBuilder::new(0);
        b.append_slice(&[32, 54]);
        let buffer = b.finish();
        assert_eq!(8, buffer.len());
    }

    #[test]
    fn test_append_values() {
        let mut a = Int8Builder::new();
        a.append_value(1);
        a.append_null();
        a.append_value(-2);
        assert_eq!(a.len(), 3);

        // append values
        let values = &[1, 2, 3, 4];
        let is_valid = &[true, true, false, true];
        a.append_values(values, is_valid);

        assert_eq!(a.len(), 7);
        let array = a.finish();
        assert_eq!(array.value(0), 1);
        assert!(array.is_null(1));
        assert_eq!(array.value(2), -2);
        assert_eq!(array.value(3), 1);
        assert_eq!(array.value(4), 2);
        assert!(array.is_null(5));
        assert_eq!(array.value(6), 4);
    }
}
