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

use std::marker::PhantomData;
use std::ops::Range;

use arrow::buffer::{Buffer, MutableBuffer};

/// A buffer that supports writing new data to the end, and removing data from the front
///
/// Used by [RecordReader](`super::RecordReader`) to buffer up values before returning a
/// potentially smaller number of values, corresponding to a whole number of semantic records
pub trait BufferQueue: Sized {
    type Output: Sized;

    type Slice: ?Sized;

    /// Split out the first `len` items
    ///
    /// # Panics
    ///
    /// Implementations must panic if `len` is beyond the length of [`BufferQueue`]
    ///
    fn split_off(&mut self, len: usize) -> Self::Output;

    /// Returns a [`Self::Slice`] with at least `batch_size` capacity that can be used
    /// to append data to the end of this [`BufferQueue`]
    ///
    /// NB: writes to the returned slice will not update the length of [`BufferQueue`]
    /// instead a subsequent call should be made to [`BufferQueue::set_len`]
    fn spare_capacity_mut(&mut self, batch_size: usize) -> &mut Self::Slice;

    /// Sets the length of the [`BufferQueue`].
    ///
    /// Intended to be used in combination with [`BufferQueue::spare_capacity_mut`]
    ///
    /// # Panics
    ///
    /// Implementations must panic if `len` is beyond the initialized length
    ///
    /// Implementations may panic if `set_len` is called with less than what has been written
    ///
    /// This distinction is to allow for implementations that return a default initialized
    /// [BufferQueue::Slice`] which doesn't track capacity and length separately
    ///
    /// For example, [`TypedBuffer<T>`] returns a default-initialized `&mut [T]`, and does not
    /// track how much of this slice is actually written to by the caller. This is still
    /// safe as the slice is default-initialized.
    ///
    fn set_len(&mut self, len: usize);
}

/// A marker trait for [scalar] types
///
/// This means that a `[Self::default()]` of length `len` can be safely created from a
/// zero-initialized `[u8]` with length `len * std::mem::size_of::<Self>()` and
/// alignment of `std::mem::size_of::<Self>()`
///
/// [scalar]: https://doc.rust-lang.org/book/ch03-02-data-types.html#scalar-types
///
pub trait ScalarValue {}
impl ScalarValue for bool {}
impl ScalarValue for i16 {}
impl ScalarValue for i32 {}
impl ScalarValue for i64 {}
impl ScalarValue for f32 {}
impl ScalarValue for f64 {}

/// A typed buffer similar to [`Vec<T>`] but using [`MutableBuffer`] for storage
pub struct ScalarBuffer<T: ScalarValue> {
    buffer: MutableBuffer,

    /// Length in elements of size T
    len: usize,

    /// Placeholder to allow `T` as an invariant generic parameter
    _phantom: PhantomData<*mut T>,
}

impl<T: ScalarValue> Default for ScalarBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ScalarValue> ScalarBuffer<T> {
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(0),
            len: 0,
            _phantom: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn resize(&mut self, len: usize) {
        self.buffer.resize(len * std::mem::size_of::<T>(), 0);
        self.len = len;
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        let (prefix, buf, suffix) = unsafe { self.buffer.as_slice().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        let (prefix, buf, suffix) =
            unsafe { self.buffer.as_slice_mut().align_to_mut::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }
}

impl<T: ScalarValue> BufferQueue for ScalarBuffer<T> {
    type Output = Buffer;

    type Slice = [T];

    fn split_off(&mut self, len: usize) -> Self::Output {
        assert!(len <= self.len);

        let num_bytes = len * std::mem::size_of::<T>();
        let remaining_bytes = self.buffer.len() - num_bytes;
        // TODO: Optimize to reduce the copy
        // create an empty buffer, as it will be resized below
        let mut remaining = MutableBuffer::new(0);
        remaining.resize(remaining_bytes, 0);

        let new_records = remaining.as_slice_mut();

        new_records[0..remaining_bytes]
            .copy_from_slice(&self.buffer.as_slice()[num_bytes..]);

        self.buffer.resize(num_bytes, 0);
        self.len -= len;

        std::mem::replace(&mut self.buffer, remaining).into()
    }

    fn spare_capacity_mut(&mut self, batch_size: usize) -> &mut Self::Slice {
        self.buffer
            .resize((self.len + batch_size) * std::mem::size_of::<T>(), 0);

        let range = self.len..self.len + batch_size;
        &mut self.as_slice_mut()[range]
    }

    fn set_len(&mut self, len: usize) {
        self.len = len;

        let new_bytes = self.len * std::mem::size_of::<T>();
        assert!(new_bytes <= self.buffer.len());
        self.buffer.resize(new_bytes, 0);
    }
}

/// A [`BufferQueue`] capable of storing column values
pub trait ValuesBuffer: BufferQueue {
    /// Iterate through the indexes in `range` in reverse order, moving the value at each
    /// index to the next index returned by `rev_valid_position_iter`
    ///
    /// It is required that:
    ///
    /// - `rev_valid_position_iter` has at least `range.end - range.start` elements
    /// - `rev_valid_position_iter` returns strictly monotonically decreasing values
    /// - the `i`th index returned by `rev_valid_position_iter` is `>= range.end - i - 1`
    ///
    /// Implementations may panic or otherwise misbehave if this is not the case
    ///
    fn pad_nulls(
        &mut self,
        range: Range<usize>,
        rev_valid_position_iter: impl Iterator<Item = usize>,
    );
}

impl<T: ScalarValue> ValuesBuffer for ScalarBuffer<T> {
    fn pad_nulls(
        &mut self,
        range: Range<usize>,
        rev_valid_position_iter: impl Iterator<Item = usize>,
    ) {
        let slice = self.as_slice_mut();

        for (value_pos, level_pos) in range.rev().zip(rev_valid_position_iter) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            slice.swap(value_pos, level_pos)
        }
    }
}
