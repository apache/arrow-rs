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

use super::Buffer;
use crate::alloc::Deallocation;
use crate::{
    alloc,
    bytes::Bytes,
    native::{ArrowNativeType, ToByteSlice},
    util::bit_util,
};
use std::mem;
use std::ptr::NonNull;

/// A [`MutableBuffer`] is Arrow's interface to build a [`Buffer`] out of items or slices of items.
/// [`Buffer`]s created from [`MutableBuffer`] (via `into`) are guaranteed to have its pointer aligned
/// along cache lines and in multiple of 64 bytes.
/// Use [MutableBuffer::push] to insert an item, [MutableBuffer::extend_from_slice]
/// to insert many items, and `into` to convert it to [`Buffer`].
///
/// For a safe, strongly typed API consider using `arrow::array::BufferBuilder`
///
/// # Example
///
/// ```
/// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
/// let mut buffer = MutableBuffer::new(0);
/// buffer.push(256u32);
/// buffer.extend_from_slice(&[1u32]);
/// let buffer: Buffer = buffer.into();
/// assert_eq!(buffer.as_slice(), &[0u8, 1, 0, 0, 1, 0, 0, 0])
/// ```
#[derive(Debug)]
pub struct MutableBuffer {
    // dangling iff capacity = 0
    data: NonNull<u8>,
    // invariant: len <= capacity
    len: usize,
    capacity: usize,
}

impl MutableBuffer {
    /// Allocate a new [MutableBuffer] with initial capacity to be at least `capacity`.
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    /// Allocate a new [MutableBuffer] with initial capacity to be at least `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = bit_util::round_upto_multiple_of_64(capacity);
        let ptr = alloc::allocate_aligned(capacity);
        Self {
            data: ptr,
            len: 0,
            capacity,
        }
    }

    /// Allocates a new [MutableBuffer] with `len` and capacity to be at least `len` where
    /// all bytes are guaranteed to be `0u8`.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::from_len_zeroed(127);
    /// assert_eq!(buffer.len(), 127);
    /// assert!(buffer.capacity() >= 127);
    /// let data = buffer.as_slice_mut();
    /// assert_eq!(data[126], 0u8);
    /// ```
    pub fn from_len_zeroed(len: usize) -> Self {
        let new_capacity = bit_util::round_upto_multiple_of_64(len);
        let ptr = alloc::allocate_aligned_zeroed(new_capacity);
        Self {
            data: ptr,
            len,
            capacity: new_capacity,
        }
    }

    /// Allocates a new [MutableBuffer] from given `Bytes`.
    pub(crate) fn from_bytes(bytes: Bytes) -> Result<Self, Bytes> {
        if !matches!(bytes.deallocation(), Deallocation::Arrow(_)) {
            return Err(bytes);
        }

        let len = bytes.len();
        let capacity = bytes.capacity();
        let ptr = bytes.ptr();
        mem::forget(bytes);

        Ok(Self {
            data: ptr,
            len,
            capacity,
        })
    }

    /// creates a new [MutableBuffer] with capacity and length capable of holding `len` bits.
    /// This is useful to create a buffer for packed bitmaps.
    pub fn new_null(len: usize) -> Self {
        let num_bytes = bit_util::ceil(len, 8);
        MutableBuffer::from_len_zeroed(num_bytes)
    }

    /// Set the bits in the range of `[0, end)` to 0 (if `val` is false), or 1 (if `val`
    /// is true). Also extend the length of this buffer to be `end`.
    ///
    /// This is useful when one wants to clear (or set) the bits and then manipulate
    /// the buffer directly (e.g., modifying the buffer by holding a mutable reference
    /// from `data_mut()`).
    pub fn with_bitset(mut self, end: usize, val: bool) -> Self {
        assert!(end <= self.capacity);
        let v = if val { 255 } else { 0 };
        unsafe {
            std::ptr::write_bytes(self.data.as_ptr(), v, end);
            self.len = end;
        }
        self
    }

    /// Ensure that `count` bytes from `start` contain zero bits
    ///
    /// This is used to initialize the bits in a buffer, however, it has no impact on the
    /// `len` of the buffer and so can be used to initialize the memory region from
    /// `len` to `capacity`.
    pub fn set_null_bits(&mut self, start: usize, count: usize) {
        assert!(start + count <= self.capacity);
        unsafe {
            std::ptr::write_bytes(self.data.as_ptr().add(start), 0, count);
        }
    }

    /// Ensures that this buffer has at least `self.len + additional` bytes. This re-allocates iff
    /// `self.len + additional > capacity`.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::new(0);
    /// buffer.reserve(253); // allocates for the first time
    /// (0..253u8).for_each(|i| buffer.push(i)); // no reallocation
    /// let buffer: Buffer = buffer.into();
    /// assert_eq!(buffer.len(), 253);
    /// ```
    // For performance reasons, this must be inlined so that the `if` is executed inside the caller, and not as an extra call that just
    // exits.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        let required_cap = self.len + additional;
        if required_cap > self.capacity {
            // JUSTIFICATION
            //  Benefit
            //      necessity
            //  Soundness
            //      `self.data` is valid for `self.capacity`.
            let (ptr, new_capacity) =
                unsafe { reallocate(self.data, self.capacity, required_cap) };
            self.data = ptr;
            self.capacity = new_capacity;
        }
    }

    /// Truncates this buffer to `len` bytes
    ///
    /// If `len` is greater than the buffer's current length, this has no effect
    #[inline(always)]
    pub fn truncate(&mut self, len: usize) {
        if len > self.len {
            return;
        }
        self.len = len;
    }

    /// Resizes the buffer, either truncating its contents (with no change in capacity), or
    /// growing it (potentially reallocating it) and writing `value` in the newly available bytes.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::new(0);
    /// buffer.resize(253, 2); // allocates for the first time
    /// assert_eq!(buffer.as_slice()[252], 2u8);
    /// ```
    // For performance reasons, this must be inlined so that the `if` is executed inside the caller, and not as an extra call that just
    // exits.
    #[inline(always)]
    pub fn resize(&mut self, new_len: usize, value: u8) {
        if new_len > self.len {
            let diff = new_len - self.len;
            self.reserve(diff);
            // write the value
            unsafe { self.data.as_ptr().add(self.len).write_bytes(value, diff) };
        }
        // this truncates the buffer when new_len < self.len
        self.len = new_len;
    }

    /// Shrinks the capacity of the buffer as much as possible.
    /// The new capacity will aligned to the nearest 64 bit alignment.
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
    /// // 2 cache lines
    /// let mut buffer = MutableBuffer::new(128);
    /// assert_eq!(buffer.capacity(), 128);
    /// buffer.push(1);
    /// buffer.push(2);
    ///
    /// buffer.shrink_to_fit();
    /// assert!(buffer.capacity() >= 64 && buffer.capacity() < 128);
    /// ```
    pub fn shrink_to_fit(&mut self) {
        let new_capacity = bit_util::round_upto_multiple_of_64(self.len);
        if new_capacity < self.capacity {
            // JUSTIFICATION
            //  Benefit
            //      necessity
            //  Soundness
            //      `self.data` is valid for `self.capacity`.
            let ptr =
                unsafe { alloc::reallocate(self.data, self.capacity, new_capacity) };

            self.data = ptr;
            self.capacity = new_capacity;
        }
    }

    /// Returns whether this buffer is empty or not.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the length (the number of bytes written) in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the total capacity in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.len = 0
    }

    /// Returns the data stored in this buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        self
    }

    /// Returns the data stored in this buffer as a mutable slice.
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self
    }

    /// Returns a raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Returns a mutable raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    #[deprecated(
        since = "2.0.0",
        note = "This method is deprecated in favour of `into` from the trait `Into`."
    )]
    /// Freezes this buffer and return an immutable version of it.
    pub fn freeze(self) -> Buffer {
        self.into_buffer()
    }

    #[inline]
    pub(super) fn into_buffer(self) -> Buffer {
        let bytes = unsafe {
            Bytes::new(self.data, self.len, Deallocation::Arrow(self.capacity))
        };
        std::mem::forget(self);
        Buffer::from_bytes(bytes)
    }

    /// View this buffer as a mutable slice of a specific type.
    ///
    /// # Panics
    ///
    /// This function panics if the underlying buffer is not aligned
    /// correctly for type `T`.
    pub fn typed_data_mut<T: ArrowNativeType>(&mut self) -> &mut [T] {
        // SAFETY
        // ArrowNativeType is trivially transmutable, is sealed to prevent potentially incorrect
        // implementation outside this crate, and this method checks alignment
        let (prefix, offsets, suffix) =
            unsafe { self.as_slice_mut().align_to_mut::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        offsets
    }

    /// View buffer as a immutable slice of a specific type.
    ///
    /// # Panics
    ///
    /// This function panics if the underlying buffer is not aligned
    /// correctly for type `T`.
    pub fn typed_data<T: ArrowNativeType>(&self) -> &[T] {
        // SAFETY
        // ArrowNativeType is trivially transmutable, is sealed to prevent potentially incorrect
        // implementation outside this crate, and this method checks alignment
        let (prefix, offsets, suffix) = unsafe { self.as_slice().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        offsets
    }

    /// Extends this buffer from a slice of items that can be represented in bytes, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new(0);
    /// buffer.extend_from_slice(&[2u32, 0]);
    /// assert_eq!(buffer.len(), 8) // u32 has 4 bytes
    /// ```
    #[inline]
    pub fn extend_from_slice<T: ArrowNativeType>(&mut self, items: &[T]) {
        let len = items.len();
        let additional = len * std::mem::size_of::<T>();
        self.reserve(additional);
        unsafe {
            // this assumes that `[ToByteSlice]` can be copied directly
            // without calling `to_byte_slice` for each element,
            // which is correct for all ArrowNativeType implementations.
            let src = items.as_ptr() as *const u8;
            let dst = self.data.as_ptr().add(self.len);
            std::ptr::copy_nonoverlapping(src, dst, additional)
        }
        self.len += additional;
    }

    /// Extends the buffer with a new item, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new(0);
    /// buffer.push(256u32);
    /// assert_eq!(buffer.len(), 4) // u32 has 4 bytes
    /// ```
    #[inline]
    pub fn push<T: ToByteSlice>(&mut self, item: T) {
        let additional = std::mem::size_of::<T>();
        self.reserve(additional);
        unsafe {
            let src = item.to_byte_slice().as_ptr();
            let dst = self.data.as_ptr().add(self.len);
            std::ptr::copy_nonoverlapping(src, dst, additional);
        }
        self.len += additional;
    }

    /// Extends the buffer with a new item, without checking for sufficient capacity
    /// # Safety
    /// Caller must ensure that the capacity()-len()>=`size_of<T>`()
    #[inline]
    pub unsafe fn push_unchecked<T: ToByteSlice>(&mut self, item: T) {
        let additional = std::mem::size_of::<T>();
        let src = item.to_byte_slice().as_ptr();
        let dst = self.data.as_ptr().add(self.len);
        std::ptr::copy_nonoverlapping(src, dst, additional);
        self.len += additional;
    }

    /// Extends the buffer by `additional` bytes equal to `0u8`, incrementing its capacity if needed.
    #[inline]
    pub fn extend_zeros(&mut self, additional: usize) {
        self.resize(self.len + additional, 0);
    }

    /// # Safety
    /// The caller must ensure that the buffer was properly initialized up to `len`.
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }

    /// Invokes `f` with values `0..len` collecting the boolean results into a new `MutableBuffer`
    ///
    /// This is similar to `from_trusted_len_iter_bool`, however, can be significantly faster
    /// as it eliminates the conditional `Iterator::next`
    #[inline]
    pub fn collect_bool<F: FnMut(usize) -> bool>(len: usize, mut f: F) -> Self {
        let mut buffer = Self::new(bit_util::ceil(len, 64) * 8);

        let chunks = len / 64;
        let remainder = len % 64;
        for chunk in 0..chunks {
            let mut packed = 0;
            for bit_idx in 0..64 {
                let i = bit_idx + chunk * 64;
                packed |= (f(i) as u64) << bit_idx;
            }

            // SAFETY: Already allocated sufficient capacity
            unsafe { buffer.push_unchecked(packed) }
        }

        if remainder != 0 {
            let mut packed = 0;
            for bit_idx in 0..remainder {
                let i = bit_idx + chunks * 64;
                packed |= (f(i) as u64) << bit_idx;
            }

            // SAFETY: Already allocated sufficient capacity
            unsafe { buffer.push_unchecked(packed) }
        }

        buffer.truncate(bit_util::ceil(len, 8));
        buffer
    }
}

/// # Safety
/// `ptr` must be allocated for `old_capacity`.
#[cold]
unsafe fn reallocate(
    ptr: NonNull<u8>,
    old_capacity: usize,
    new_capacity: usize,
) -> (NonNull<u8>, usize) {
    let new_capacity = bit_util::round_upto_multiple_of_64(new_capacity);
    let new_capacity = std::cmp::max(new_capacity, old_capacity * 2);
    let ptr = alloc::reallocate(ptr, old_capacity, new_capacity);
    (ptr, new_capacity)
}

impl<A: ArrowNativeType> Extend<A> for MutableBuffer {
    #[inline]
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        let iterator = iter.into_iter();
        self.extend_from_iter(iterator)
    }
}

impl MutableBuffer {
    #[inline]
    pub(super) fn extend_from_iter<T: ArrowNativeType, I: Iterator<Item = T>>(
        &mut self,
        mut iterator: I,
    ) {
        let item_size = std::mem::size_of::<T>();
        let (lower, _) = iterator.size_hint();
        let additional = lower * item_size;
        self.reserve(additional);

        // this is necessary because of https://github.com/rust-lang/rust/issues/32155
        let mut len = SetLenOnDrop::new(&mut self.len);
        let mut dst = unsafe { self.data.as_ptr().add(len.local_len) };
        let capacity = self.capacity;

        while len.local_len + item_size <= capacity {
            if let Some(item) = iterator.next() {
                unsafe {
                    let src = item.to_byte_slice().as_ptr();
                    std::ptr::copy_nonoverlapping(src, dst, item_size);
                    dst = dst.add(item_size);
                }
                len.local_len += item_size;
            } else {
                break;
            }
        }
        drop(len);

        iterator.for_each(|item| self.push(item));
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { MutableBuffer::from_trusted_len_iter(iter) };
    /// assert_eq!(buffer.len(), 4) // u32 has 4 bytes
    /// ```
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This implementation is required for two reasons:
    // 1. there is no trait `TrustedLen` in stable rust and therefore
    //    we can't specialize `extend` for `TrustedLen` like `Vec` does.
    // 2. `from_trusted_len_iter` is faster.
    #[inline]
    pub unsafe fn from_trusted_len_iter<T: ArrowNativeType, I: Iterator<Item = T>>(
        iterator: I,
    ) -> Self {
        let item_size = std::mem::size_of::<T>();
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("from_trusted_len_iter requires an upper limit");
        let len = upper * item_size;

        let mut buffer = MutableBuffer::new(len);

        let mut dst = buffer.data.as_ptr();
        for item in iterator {
            // note how there is no reserve here (compared with `extend_from_iter`)
            let src = item.to_byte_slice().as_ptr();
            std::ptr::copy_nonoverlapping(src, dst, item_size);
            dst = dst.add(item_size);
        }
        assert_eq!(
            dst.offset_from(buffer.data.as_ptr()) as usize,
            len,
            "Trusted iterator length was not accurately reported"
        );
        buffer.len = len;
        buffer
    }

    /// Creates a [`MutableBuffer`] from a boolean [`Iterator`] with a trusted (upper) length.
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// let v = vec![false, true, false];
    /// let iter = v.iter().map(|x| *x || true);
    /// let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(iter) };
    /// assert_eq!(buffer.len(), 1) // 3 booleans have 1 byte
    /// ```
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This implementation is required for two reasons:
    // 1. there is no trait `TrustedLen` in stable rust and therefore
    //    we can't specialize `extend` for `TrustedLen` like `Vec` does.
    // 2. `from_trusted_len_iter_bool` is faster.
    #[inline]
    pub unsafe fn from_trusted_len_iter_bool<I: Iterator<Item = bool>>(
        mut iterator: I,
    ) -> Self {
        let (_, upper) = iterator.size_hint();
        let len = upper.expect("from_trusted_len_iter requires an upper limit");

        Self::collect_bool(len, |_| iterator.next().unwrap())
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length or errors
    /// if any of the items of the iterator is an error.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<
        E,
        T: ArrowNativeType,
        I: Iterator<Item = std::result::Result<T, E>>,
    >(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        let item_size = std::mem::size_of::<T>();
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("try_from_trusted_len_iter requires an upper limit");
        let len = upper * item_size;

        let mut buffer = MutableBuffer::new(len);

        let mut dst = buffer.data.as_ptr();
        for item in iterator {
            let item = item?;
            // note how there is no reserve here (compared with `extend_from_iter`)
            let src = item.to_byte_slice().as_ptr();
            std::ptr::copy_nonoverlapping(src, dst, item_size);
            dst = dst.add(item_size);
        }
        // try_from_trusted_len_iter is instantiated a lot, so we extract part of it into a less
        // generic method to reduce compile time
        unsafe fn finalize_buffer(dst: *mut u8, buffer: &mut MutableBuffer, len: usize) {
            assert_eq!(
                dst.offset_from(buffer.data.as_ptr()) as usize,
                len,
                "Trusted iterator length was not accurately reported"
            );
            buffer.len = len;
        }
        finalize_buffer(dst, &mut buffer, len);
        Ok(buffer)
    }
}

impl std::ops::Deref for MutableBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }
}

impl std::ops::DerefMut for MutableBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }
}

impl Drop for MutableBuffer {
    fn drop(&mut self) {
        unsafe { alloc::free_aligned(self.data, self.capacity) };
    }
}

impl PartialEq for MutableBuffer {
    fn eq(&self, other: &MutableBuffer) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.capacity != other.capacity {
            return false;
        }
        self.as_slice() == other.as_slice()
    }
}

unsafe impl Sync for MutableBuffer {}
unsafe impl Send for MutableBuffer {}

struct SetLenOnDrop<'a> {
    len: &'a mut usize,
    local_len: usize,
}

impl<'a> SetLenOnDrop<'a> {
    #[inline]
    fn new(len: &'a mut usize) -> Self {
        SetLenOnDrop {
            local_len: *len,
            len,
        }
    }
}

impl Drop for SetLenOnDrop<'_> {
    #[inline]
    fn drop(&mut self) {
        *self.len = self.local_len;
    }
}

/// Creating a `MutableBuffer` instance by setting bits according to the boolean values
impl std::iter::FromIterator<bool> for MutableBuffer {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        let mut iterator = iter.into_iter();
        let mut result = {
            let byte_capacity: usize = iterator.size_hint().0.saturating_add(7) / 8;
            MutableBuffer::new(byte_capacity)
        };

        loop {
            let mut exhausted = false;
            let mut byte_accum: u8 = 0;
            let mut mask: u8 = 1;

            //collect (up to) 8 bits into a byte
            while mask != 0 {
                if let Some(value) = iterator.next() {
                    byte_accum |= match value {
                        true => mask,
                        false => 0,
                    };
                    mask <<= 1;
                } else {
                    exhausted = true;
                    break;
                }
            }

            // break if the iterator was exhausted before it provided a bool for this byte
            if exhausted && mask == 1 {
                break;
            }

            //ensure we have capacity to write the byte
            if result.len() == result.capacity() {
                //no capacity for new byte, allocate 1 byte more (plus however many more the iterator advertises)
                let additional_byte_capacity = 1usize.saturating_add(
                    iterator.size_hint().0.saturating_add(7) / 8, //convert bit count to byte count, rounding up
                );
                result.reserve(additional_byte_capacity)
            }

            // Soundness: capacity was allocated above
            unsafe { result.push_unchecked(byte_accum) };
            if exhausted {
                break;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutable_new() {
        let buf = MutableBuffer::new(63);
        assert_eq!(64, buf.capacity());
        assert_eq!(0, buf.len());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_mutable_extend_from_slice() {
        let mut buf = MutableBuffer::new(100);
        buf.extend_from_slice(b"hello");
        assert_eq!(5, buf.len());
        assert_eq!(b"hello", buf.as_slice());

        buf.extend_from_slice(b" world");
        assert_eq!(11, buf.len());
        assert_eq!(b"hello world", buf.as_slice());

        buf.clear();
        assert_eq!(0, buf.len());
        buf.extend_from_slice(b"hello arrow");
        assert_eq!(11, buf.len());
        assert_eq!(b"hello arrow", buf.as_slice());
    }

    #[test]
    fn mutable_extend_from_iter() {
        let mut buf = MutableBuffer::new(0);
        buf.extend(vec![1u32, 2]);
        assert_eq!(8, buf.len());
        assert_eq!(&[1u8, 0, 0, 0, 2, 0, 0, 0], buf.as_slice());

        buf.extend(vec![3u32, 4]);
        assert_eq!(16, buf.len());
        assert_eq!(
            &[1u8, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0],
            buf.as_slice()
        );
    }

    #[test]
    fn mutable_extend_from_iter_unaligned_u64() {
        let mut buf = MutableBuffer::new(16);
        buf.push(1_u8);
        buf.extend([1_u64]);
        assert_eq!(9, buf.len());
        assert_eq!(&[1u8, 1u8, 0, 0, 0, 0, 0, 0, 0], buf.as_slice());
    }

    #[test]
    fn mutable_extend_from_slice_unaligned_u64() {
        let mut buf = MutableBuffer::new(16);
        buf.extend_from_slice(&[1_u8]);
        buf.extend_from_slice(&[1_u64]);
        assert_eq!(9, buf.len());
        assert_eq!(&[1u8, 1u8, 0, 0, 0, 0, 0, 0, 0], buf.as_slice());
    }

    #[test]
    fn mutable_push_unaligned_u64() {
        let mut buf = MutableBuffer::new(16);
        buf.push(1_u8);
        buf.push(1_u64);
        assert_eq!(9, buf.len());
        assert_eq!(&[1u8, 1u8, 0, 0, 0, 0, 0, 0, 0], buf.as_slice());
    }

    #[test]
    fn mutable_push_unchecked_unaligned_u64() {
        let mut buf = MutableBuffer::new(16);
        unsafe {
            buf.push_unchecked(1_u8);
            buf.push_unchecked(1_u64);
        }
        assert_eq!(9, buf.len());
        assert_eq!(&[1u8, 1u8, 0, 0, 0, 0, 0, 0, 0], buf.as_slice());
    }

    #[test]
    fn test_from_trusted_len_iter() {
        let iter = vec![1u32, 2].into_iter();
        let buf = unsafe { Buffer::from_trusted_len_iter(iter) };
        assert_eq!(8, buf.len());
        assert_eq!(&[1u8, 0, 0, 0, 2, 0, 0, 0], buf.as_slice());
    }

    #[test]
    fn test_mutable_reserve() {
        let mut buf = MutableBuffer::new(1);
        assert_eq!(64, buf.capacity());

        // Reserving a smaller capacity should have no effect.
        buf.reserve(10);
        assert_eq!(64, buf.capacity());

        buf.reserve(80);
        assert_eq!(128, buf.capacity());

        buf.reserve(129);
        assert_eq!(256, buf.capacity());
    }

    #[test]
    fn test_mutable_resize() {
        let mut buf = MutableBuffer::new(1);
        assert_eq!(64, buf.capacity());
        assert_eq!(0, buf.len());

        buf.resize(20, 0);
        assert_eq!(64, buf.capacity());
        assert_eq!(20, buf.len());

        buf.resize(10, 0);
        assert_eq!(64, buf.capacity());
        assert_eq!(10, buf.len());

        buf.resize(100, 0);
        assert_eq!(128, buf.capacity());
        assert_eq!(100, buf.len());

        buf.resize(30, 0);
        assert_eq!(128, buf.capacity());
        assert_eq!(30, buf.len());

        buf.resize(0, 0);
        assert_eq!(128, buf.capacity());
        assert_eq!(0, buf.len());
    }

    #[test]
    fn test_mutable_into() {
        let mut buf = MutableBuffer::new(1);
        buf.extend_from_slice(b"aaaa bbbb cccc dddd");
        assert_eq!(19, buf.len());
        assert_eq!(64, buf.capacity());
        assert_eq!(b"aaaa bbbb cccc dddd", buf.as_slice());

        let immutable_buf: Buffer = buf.into();
        assert_eq!(19, immutable_buf.len());
        assert_eq!(64, immutable_buf.capacity());
        assert_eq!(b"aaaa bbbb cccc dddd", immutable_buf.as_slice());
    }

    #[test]
    fn test_mutable_equal() {
        let mut buf = MutableBuffer::new(1);
        let mut buf2 = MutableBuffer::new(1);

        buf.extend_from_slice(&[0xaa]);
        buf2.extend_from_slice(&[0xaa, 0xbb]);
        assert!(buf != buf2);

        buf.extend_from_slice(&[0xbb]);
        assert_eq!(buf, buf2);

        buf2.reserve(65);
        assert!(buf != buf2);
    }

    #[test]
    fn test_mutable_shrink_to_fit() {
        let mut buffer = MutableBuffer::new(128);
        assert_eq!(buffer.capacity(), 128);
        buffer.push(1);
        buffer.push(2);

        buffer.shrink_to_fit();
        assert!(buffer.capacity() >= 64 && buffer.capacity() < 128);
    }
}
