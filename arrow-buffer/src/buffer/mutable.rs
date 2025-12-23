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

use std::alloc::{Layout, handle_alloc_error};
use std::mem;
use std::ptr::NonNull;

use crate::alloc::{ALIGNMENT, Deallocation};
use crate::{
    bytes::Bytes,
    native::{ArrowNativeType, ToByteSlice},
    util::bit_util,
};

#[cfg(feature = "pool")]
use crate::pool::{MemoryPool, MemoryReservation};
#[cfg(feature = "pool")]
use std::sync::Mutex;

use super::Buffer;

/// A [`MutableBuffer`] is a wrapper over memory regions, used to build
/// [`Buffer`]s out of items or slices of items.
///
/// [`Buffer`]s created from [`MutableBuffer`] (via `into`) are guaranteed to be
/// aligned along cache lines and in multiples of 64 bytes.
///
/// Use [MutableBuffer::push] to insert an item, [MutableBuffer::extend_from_slice]
/// to insert many items, and `into` to convert it to [`Buffer`]. For typed data,
/// it is often more efficient to use [`Vec`] and convert it to [`Buffer`] rather
/// than using [`MutableBuffer`] (see examples below).
///
/// # See Also
/// * For a safe, strongly typed API consider using [`Vec`] and [`ScalarBuffer`](crate::ScalarBuffer)
/// * To apply bitwise operations, see [`apply_bitwise_binary_op`] and [`apply_bitwise_unary_op`]
///
/// [`apply_bitwise_binary_op`]: crate::bit_util::apply_bitwise_binary_op
/// [`apply_bitwise_unary_op`]: crate::bit_util::apply_bitwise_unary_op
///
/// # Example: Creating a [`Buffer`] from a [`MutableBuffer`]
/// ```
/// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
/// let mut buffer = MutableBuffer::new(0);
/// buffer.push(256u32);
/// buffer.extend_from_slice(&[1u32]);
/// let buffer = Buffer::from(buffer);
/// assert_eq!(buffer.as_slice(), &[0u8, 1, 0, 0, 1, 0, 0, 0])
/// ```
///
/// The same can be achieved more efficiently by using a `Vec<u32>`
/// ```
/// # use arrow_buffer::buffer::Buffer;
/// let mut vec = Vec::new();
/// vec.push(256u32);
/// vec.extend_from_slice(&[1u32]);
/// let buffer = Buffer::from(vec);
/// assert_eq!(buffer.as_slice(), &[0u8, 1, 0, 0, 1, 0, 0, 0]);
/// ```
///
/// # Example: Creating a [`MutableBuffer`] from a `Vec<T>`
/// ```
/// # use arrow_buffer::buffer::MutableBuffer;
/// let vec = vec![1u32, 2, 3];
/// let mutable_buffer = MutableBuffer::from(vec); // reuses the allocation from vec
/// assert_eq!(mutable_buffer.len(), 12); // 3 * 4 bytes
/// ```
///
/// # Example: Creating a [`MutableBuffer`] from a [`Buffer`]
/// ```
/// # use arrow_buffer::buffer::{Buffer, MutableBuffer};
/// let buffer: Buffer = Buffer::from(&[1u8, 2, 3, 4][..]);
/// // Only possible to convert a Buffer into a MutableBuffer if uniquely owned
/// // (i.e., there are no other references to it).
/// let mut mutable_buffer = match buffer.into_mutable() {
///    Ok(mutable) => mutable,
///    Err(orig_buffer) => {
///      panic!("buffer was not uniquely owned");
///    }
/// };
/// mutable_buffer.push(5u8);
/// let buffer = Buffer::from(mutable_buffer);
/// assert_eq!(buffer.as_slice(), &[1u8, 2, 3, 4, 5])
/// ```
#[derive(Debug)]
pub struct MutableBuffer {
    // dangling iff capacity = 0
    data: NonNull<u8>,
    // invariant: len <= capacity
    len: usize,
    layout: Layout,

    /// Memory reservation for tracking memory usage
    #[cfg(feature = "pool")]
    reservation: Mutex<Option<Box<dyn MemoryReservation>>>,
}

impl MutableBuffer {
    /// Allocate a new [MutableBuffer] with initial capacity to be at least `capacity`.
    ///
    /// See [`MutableBuffer::with_capacity`].
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    /// Allocate a new [MutableBuffer] with initial capacity to be at least `capacity`.
    ///
    /// # Panics
    ///
    /// If `capacity`, when rounded up to the nearest multiple of [`ALIGNMENT`], is greater
    /// then `isize::MAX`, then this function will panic.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = bit_util::round_upto_multiple_of_64(capacity);
        let layout = Layout::from_size_align(capacity, ALIGNMENT)
            .expect("failed to create layout for MutableBuffer");
        let data = match layout.size() {
            0 => dangling_ptr(),
            _ => {
                // Safety: Verified size != 0
                let raw_ptr = unsafe { std::alloc::alloc(layout) };
                NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
            }
        };
        Self {
            data,
            len: 0,
            layout,
            #[cfg(feature = "pool")]
            reservation: std::sync::Mutex::new(None),
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
        let layout = Layout::from_size_align(len, ALIGNMENT).unwrap();
        let data = match layout.size() {
            0 => dangling_ptr(),
            _ => {
                // Safety: Verified size != 0
                let raw_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
                NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
            }
        };
        Self {
            data,
            len,
            layout,
            #[cfg(feature = "pool")]
            reservation: std::sync::Mutex::new(None),
        }
    }

    /// Allocates a new [MutableBuffer] from given `Bytes`.
    pub(crate) fn from_bytes(bytes: Bytes) -> Result<Self, Bytes> {
        let layout = match bytes.deallocation() {
            Deallocation::Standard(layout) => *layout,
            _ => return Err(bytes),
        };

        let len = bytes.len();
        let data = bytes.ptr();
        #[cfg(feature = "pool")]
        let reservation = bytes.reservation.lock().unwrap().take();
        mem::forget(bytes);

        Ok(Self {
            data,
            len,
            layout,
            #[cfg(feature = "pool")]
            reservation: Mutex::new(reservation),
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
        assert!(end <= self.layout.size());
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
        assert!(
            start.saturating_add(count) <= self.layout.size(),
            "range start index {start} and count {count} out of bounds for \
            buffer of length {}",
            self.layout.size(),
        );

        // Safety: `self.data[start..][..count]` is in-bounds and well-aligned for `u8`
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
        if required_cap > self.layout.size() {
            let new_capacity = bit_util::round_upto_multiple_of_64(required_cap);
            let new_capacity = std::cmp::max(new_capacity, self.layout.size() * 2);
            self.reallocate(new_capacity)
        }
    }

    /// Adding to this mutable buffer `slice_to_repeat` repeated `repeat_count` times.
    ///
    /// # Example
    ///
    /// ## Repeat the same string bytes multiple times
    /// ```
    /// # use arrow_buffer::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new(0);
    /// let bytes_to_repeat = b"ab";
    /// buffer.repeat_slice_n_times(bytes_to_repeat, 3);
    /// assert_eq!(buffer.as_slice(), b"ababab");
    /// ```
    pub fn repeat_slice_n_times<T: ArrowNativeType>(
        &mut self,
        slice_to_repeat: &[T],
        repeat_count: usize,
    ) {
        if repeat_count == 0 || slice_to_repeat.is_empty() {
            return;
        }

        let bytes_to_repeat = size_of_val(slice_to_repeat);

        // Ensure capacity
        self.reserve(repeat_count * bytes_to_repeat);

        // Save the length before we do all the copies to know where to start from
        let length_before = self.len;

        // Copy the initial slice once so we can use doubling strategy on it
        self.extend_from_slice(slice_to_repeat);

        // This tracks how much bytes we have added by repeating so far
        let added_repeats_length = bytes_to_repeat;
        assert_eq!(
            self.len - length_before,
            added_repeats_length,
            "should copy exactly the same number of bytes"
        );

        // Number of times the slice was repeated
        let mut already_repeated_times = 1;

        // We will use doubling strategy to fill the buffer in log(repeat_count) steps
        while already_repeated_times < repeat_count {
            // How many slices can we copy in this iteration
            // (either double what we have, or just the remaining ones)
            let number_of_slices_to_copy =
                already_repeated_times.min(repeat_count - already_repeated_times);
            let number_of_bytes_to_copy = number_of_slices_to_copy * bytes_to_repeat;

            unsafe {
                // Get to the start of the data before we started copying anything
                let src = self.data.as_ptr().add(length_before) as *const u8;

                // Go to the current location to copy to (end of current data)
                let dst = self.data.as_ptr().add(self.len);

                // SAFETY: the pointers are not overlapping as there is `number_of_bytes_to_copy` or less between them
                std::ptr::copy_nonoverlapping(src, dst, number_of_bytes_to_copy)
            }

            // Advance the length by the amount of data we just copied (doubled)
            self.len += number_of_bytes_to_copy;

            already_repeated_times += number_of_slices_to_copy;
        }
    }

    #[cold]
    fn reallocate(&mut self, capacity: usize) {
        let new_layout = Layout::from_size_align(capacity, self.layout.align()).unwrap();
        if new_layout.size() == 0 {
            if self.layout.size() != 0 {
                // Safety: data was allocated with layout
                unsafe { std::alloc::dealloc(self.as_mut_ptr(), self.layout) };
                self.layout = new_layout
            }
            return;
        }

        let data = match self.layout.size() {
            // Safety: new_layout is not empty
            0 => unsafe { std::alloc::alloc(new_layout) },
            // Safety: verified new layout is valid and not empty
            _ => unsafe { std::alloc::realloc(self.as_mut_ptr(), self.layout, capacity) },
        };
        self.data = NonNull::new(data).unwrap_or_else(|| handle_alloc_error(new_layout));
        self.layout = new_layout;
        #[cfg(feature = "pool")]
        {
            if let Some(reservation) = self.reservation.lock().unwrap().as_mut() {
                reservation.resize(self.layout.size());
            }
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
        #[cfg(feature = "pool")]
        {
            if let Some(reservation) = self.reservation.lock().unwrap().as_mut() {
                reservation.resize(self.len);
            }
        }
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
        #[cfg(feature = "pool")]
        {
            if let Some(reservation) = self.reservation.lock().unwrap().as_mut() {
                reservation.resize(self.len);
            }
        }
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
        if new_capacity < self.layout.size() {
            self.reallocate(new_capacity)
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

    /// Returns the total capacity in this buffer, in bytes.
    ///
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.layout.size()
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

    #[inline]
    pub(super) fn into_buffer(self) -> Buffer {
        let bytes = unsafe { Bytes::new(self.data, self.len, Deallocation::Standard(self.layout)) };
        #[cfg(feature = "pool")]
        {
            let reservation = self.reservation.lock().unwrap().take();
            *bytes.reservation.lock().unwrap() = reservation;
        }
        std::mem::forget(self);
        Buffer::from(bytes)
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
        let (prefix, offsets, suffix) = unsafe { self.as_slice_mut().align_to_mut::<T>() };
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
        let additional = mem::size_of_val(items);
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
        let dst = unsafe { self.data.as_ptr().add(self.len) };
        unsafe { std::ptr::copy_nonoverlapping(src, dst, additional) };
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
        let mut buffer: Vec<u64> = Vec::with_capacity(bit_util::ceil(len, 64));

        let chunks = len / 64;
        let remainder = len % 64;
        buffer.extend((0..chunks).map(|chunk| {
            let mut packed = 0;
            for bit_idx in 0..64 {
                let i = bit_idx + chunk * 64;
                packed |= (f(i) as u64) << bit_idx;
            }

            packed
        }));

        if remainder != 0 {
            let mut packed = 0;
            for bit_idx in 0..remainder {
                let i = bit_idx + chunks * 64;
                packed |= (f(i) as u64) << bit_idx;
            }

            buffer.push(packed)
        }

        let mut buffer: MutableBuffer = buffer.into();
        buffer.truncate(bit_util::ceil(len, 8));
        buffer
    }

    /// Register this [`MutableBuffer`] with the provided [`MemoryPool`]
    ///
    /// This claims the memory used by this buffer in the pool, allowing for
    /// accurate accounting of memory usage. Any prior reservation will be
    /// released so this works well when the buffer is being shared among
    /// multiple arrays.
    #[cfg(feature = "pool")]
    pub fn claim(&self, pool: &dyn MemoryPool) {
        *self.reservation.lock().unwrap() = Some(pool.reserve(self.capacity()));
    }
}

/// Creates a non-null pointer with alignment of [`ALIGNMENT`]
///
/// This is similar to [`NonNull::dangling`]
#[inline]
pub(crate) fn dangling_ptr() -> NonNull<u8> {
    // SAFETY: ALIGNMENT is a non-zero usize which is then cast
    // to a *mut u8. Therefore, `ptr` is not null and the conditions for
    // calling new_unchecked() are respected.
    #[cfg(miri)]
    {
        // Since miri implies a nightly rust version we can use the unstable strict_provenance feature
        unsafe { NonNull::new_unchecked(std::ptr::without_provenance_mut(ALIGNMENT)) }
    }
    #[cfg(not(miri))]
    {
        unsafe { NonNull::new_unchecked(ALIGNMENT as *mut u8) }
    }
}

impl<A: ArrowNativeType> Extend<A> for MutableBuffer {
    #[inline]
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        let iterator = iter.into_iter();
        self.extend_from_iter(iterator)
    }
}

impl<T: ArrowNativeType> From<Vec<T>> for MutableBuffer {
    fn from(value: Vec<T>) -> Self {
        // Safety
        // Vec::as_ptr guaranteed to not be null and ArrowNativeType are trivially transmutable
        let data = unsafe { NonNull::new_unchecked(value.as_ptr() as _) };
        let len = value.len() * mem::size_of::<T>();
        // Safety
        // Vec guaranteed to have a valid layout matching that of `Layout::array`
        // This is based on `RawVec::current_memory`
        let layout = unsafe { Layout::array::<T>(value.capacity()).unwrap_unchecked() };
        mem::forget(value);
        Self {
            data,
            len,
            layout,
            #[cfg(feature = "pool")]
            reservation: std::sync::Mutex::new(None),
        }
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
        let capacity = self.layout.size();

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
            unsafe { std::ptr::copy_nonoverlapping(src, dst, item_size) };
            dst = unsafe { dst.add(item_size) };
        }
        assert_eq!(
            unsafe { dst.offset_from(buffer.data.as_ptr()) } as usize,
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
    pub unsafe fn from_trusted_len_iter_bool<I: Iterator<Item = bool>>(mut iterator: I) -> Self {
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
        I: Iterator<Item = Result<T, E>>,
    >(
        iterator: I,
    ) -> Result<Self, E> {
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
            unsafe { std::ptr::copy_nonoverlapping(src, dst, item_size) };
            dst = unsafe { dst.add(item_size) };
        }
        // try_from_trusted_len_iter is instantiated a lot, so we extract part of it into a less
        // generic method to reduce compile time
        unsafe fn finalize_buffer(dst: *mut u8, buffer: &mut MutableBuffer, len: usize) {
            unsafe {
                assert_eq!(
                    dst.offset_from(buffer.data.as_ptr()) as usize,
                    len,
                    "Trusted iterator length was not accurately reported"
                );
                buffer.len = len;
            }
        }
        unsafe { finalize_buffer(dst, &mut buffer, len) };
        Ok(buffer)
    }
}

impl Default for MutableBuffer {
    fn default() -> Self {
        Self::with_capacity(0)
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

impl AsRef<[u8]> for &MutableBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for MutableBuffer {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            // Safety: data was allocated with standard allocator with given layout
            unsafe { std::alloc::dealloc(self.data.as_ptr() as _, self.layout) };
        }
    }
}

impl PartialEq for MutableBuffer {
    fn eq(&self, other: &MutableBuffer) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.layout != other.layout {
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

impl<T: ArrowNativeType> std::iter::FromIterator<T> for MutableBuffer {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut buffer = Self::default();
        buffer.extend_from_iter(iter.into_iter());
        buffer
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
    fn test_mutable_default() {
        let buf = MutableBuffer::default();
        assert_eq!(0, buf.capacity());
        assert_eq!(0, buf.len());
        assert!(buf.is_empty());

        let mut buf = MutableBuffer::default();
        buf.extend_from_slice(b"hello");
        assert_eq!(5, buf.len());
        assert_eq!(b"hello", buf.as_slice());
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
        let buf = unsafe { MutableBuffer::from_trusted_len_iter(iter) };
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

    #[test]
    fn test_mutable_set_null_bits() {
        let mut buffer = MutableBuffer::new(8).with_bitset(8, true);

        for i in 0..=buffer.capacity() {
            buffer.set_null_bits(i, 0);
            assert_eq!(buffer[..8], [255; 8][..]);
        }

        buffer.set_null_bits(1, 4);
        assert_eq!(buffer[..8], [255, 0, 0, 0, 0, 255, 255, 255][..]);
    }

    #[test]
    #[should_panic = "out of bounds for buffer of length"]
    fn test_mutable_set_null_bits_oob() {
        let mut buffer = MutableBuffer::new(64);
        buffer.set_null_bits(1, buffer.capacity());
    }

    #[test]
    #[should_panic = "out of bounds for buffer of length"]
    fn test_mutable_set_null_bits_oob_by_overflow() {
        let mut buffer = MutableBuffer::new(0);
        buffer.set_null_bits(1, usize::MAX);
    }

    #[test]
    fn from_iter() {
        let buffer = [1u16, 2, 3, 4].into_iter().collect::<MutableBuffer>();
        assert_eq!(buffer.len(), 4 * mem::size_of::<u16>());
        assert_eq!(buffer.as_slice(), &[1, 0, 2, 0, 3, 0, 4, 0]);
    }

    #[test]
    #[should_panic(expected = "failed to create layout for MutableBuffer: LayoutError")]
    fn test_with_capacity_panics_above_max_capacity() {
        let max_capacity = isize::MAX as usize - (isize::MAX as usize % ALIGNMENT);
        let _ = MutableBuffer::with_capacity(max_capacity + 1);
    }

    #[cfg(feature = "pool")]
    mod pool_tests {
        use super::*;
        use crate::pool::{MemoryPool, TrackingMemoryPool};

        #[test]
        fn test_reallocate_with_pool() {
            let pool = TrackingMemoryPool::default();
            let mut buffer = MutableBuffer::with_capacity(100);
            buffer.claim(&pool);

            // Initial capacity should be 128 (multiple of 64)
            assert_eq!(buffer.capacity(), 128);
            assert_eq!(pool.used(), 128);

            // Reallocate to a larger size
            buffer.reallocate(200);

            // The capacity is exactly the requested size, not rounded up
            assert_eq!(buffer.capacity(), 200);
            assert_eq!(pool.used(), 200);

            // Reallocate to a smaller size
            buffer.reallocate(50);

            // The capacity is exactly the requested size, not rounded up
            assert_eq!(buffer.capacity(), 50);
            assert_eq!(pool.used(), 50);
        }

        #[test]
        fn test_truncate_with_pool() {
            let pool = TrackingMemoryPool::default();
            let mut buffer = MutableBuffer::with_capacity(100);

            // Fill buffer with some data
            buffer.resize(80, 1);
            assert_eq!(buffer.len(), 80);

            buffer.claim(&pool);
            assert_eq!(pool.used(), 128);

            // Truncate buffer
            buffer.truncate(40);
            assert_eq!(buffer.len(), 40);
            assert_eq!(pool.used(), 40);

            // Truncate to zero
            buffer.truncate(0);
            assert_eq!(buffer.len(), 0);
            assert_eq!(pool.used(), 0);
        }

        #[test]
        fn test_resize_with_pool() {
            let pool = TrackingMemoryPool::default();
            let mut buffer = MutableBuffer::with_capacity(100);
            buffer.claim(&pool);

            // Initial state
            assert_eq!(buffer.len(), 0);
            assert_eq!(pool.used(), 128);

            // Resize to increase length
            buffer.resize(50, 1);
            assert_eq!(buffer.len(), 50);
            assert_eq!(pool.used(), 50);

            // Resize to increase length beyond capacity
            buffer.resize(150, 1);
            assert_eq!(buffer.len(), 150);
            assert_eq!(buffer.capacity(), 256);
            assert_eq!(pool.used(), 150);

            // Resize to decrease length
            buffer.resize(30, 1);
            assert_eq!(buffer.len(), 30);
            assert_eq!(pool.used(), 30);
        }

        #[test]
        fn test_buffer_lifecycle_with_pool() {
            let pool = TrackingMemoryPool::default();

            // Create a buffer with memory reservation
            let mut mutable = MutableBuffer::with_capacity(100);
            mutable.resize(80, 1);
            mutable.claim(&pool);

            // Memory reservation is based on capacity when using claim()
            assert_eq!(pool.used(), 128);

            // Convert to immutable Buffer
            let buffer = mutable.into_buffer();

            // Memory reservation should be preserved
            assert_eq!(pool.used(), 128);

            // Drop the buffer and the reservation should be released
            drop(buffer);
            assert_eq!(pool.used(), 0);
        }
    }

    fn create_expected_repeated_slice<T: ArrowNativeType>(
        slice_to_repeat: &[T],
        repeat_count: usize,
    ) -> Buffer {
        let mut expected = MutableBuffer::new(size_of_val(slice_to_repeat) * repeat_count);
        for _ in 0..repeat_count {
            // Not using push_slice_repeated as this is the function under test
            expected.extend_from_slice(slice_to_repeat);
        }
        expected.into()
    }

    // Helper to test a specific repeat count with various slice sizes
    fn test_repeat_count<T: ArrowNativeType + PartialEq + std::fmt::Debug>(
        repeat_count: usize,
        test_data: &[T],
    ) {
        let mut buffer = MutableBuffer::new(0);
        buffer.repeat_slice_n_times(test_data, repeat_count);

        let expected = create_expected_repeated_slice(test_data, repeat_count);
        let result: Buffer = buffer.into();

        assert_eq!(
            result,
            expected,
            "Failed for repeat_count={}, slice_len={}",
            repeat_count,
            test_data.len()
        );
    }

    #[test]
    fn test_repeat_slice_count_edge_cases() {
        // Empty slice
        test_repeat_count(100, &[] as &[i32]);

        // Zero repeats
        test_repeat_count(0, &[1i32, 2, 3]);
    }

    #[test]
    fn test_small_repeats_counts() {
        // test any special implementation for small repeat counts
        let data = &[1u8, 2, 3, 4, 5];

        for _ in 1..=10 {
            test_repeat_count(2, data);
        }
    }

    #[test]
    fn test_different_size_of_i32_repeat_slice() {
        let data: &[i32] = &[1, 2, 3];
        let data_with_single_item: &[i32] = &[42];

        for data in &[data, data_with_single_item] {
            for item in 1..=9 {
                let base_repeat_count = 2_usize.pow(item);
                test_repeat_count(base_repeat_count - 1, data);
                test_repeat_count(base_repeat_count, data);
                test_repeat_count(base_repeat_count + 1, data);
            }
        }
    }

    #[test]
    fn test_different_size_of_u8_repeat_slice() {
        let data: &[u8] = &[1, 2, 3];
        let data_with_single_item: &[u8] = &[10];

        for data in &[data, data_with_single_item] {
            for item in 1..=9 {
                let base_repeat_count = 2_usize.pow(item);
                test_repeat_count(base_repeat_count - 1, data);
                test_repeat_count(base_repeat_count, data);
                test_repeat_count(base_repeat_count + 1, data);
            }
        }
    }

    #[test]
    fn test_different_size_of_u16_repeat_slice() {
        let data: &[u16] = &[1, 2, 3];
        let data_with_single_item: &[u16] = &[10];

        for data in &[data, data_with_single_item] {
            for item in 1..=9 {
                let base_repeat_count = 2_usize.pow(item);
                test_repeat_count(base_repeat_count - 1, data);
                test_repeat_count(base_repeat_count, data);
                test_repeat_count(base_repeat_count + 1, data);
            }
        }
    }

    #[test]
    fn test_various_slice_lengths() {
        // Test different slice lengths with same repeat pattern
        let repeat_count = 37; // Arbitrary non-power-of-2

        // Single element
        test_repeat_count(repeat_count, &[42i32]);

        // Small slices
        test_repeat_count(repeat_count, &[1i32, 2]);
        test_repeat_count(repeat_count, &[1i32, 2, 3]);
        test_repeat_count(repeat_count, &[1i32, 2, 3, 4]);
        test_repeat_count(repeat_count, &[1i32, 2, 3, 4, 5]);

        // Larger slices
        let data_10: Vec<i32> = (0..10).collect();
        test_repeat_count(repeat_count, &data_10);

        let data_100: Vec<i32> = (0..100).collect();
        test_repeat_count(repeat_count, &data_100);

        let data_1000: Vec<i32> = (0..1000).collect();
        test_repeat_count(repeat_count, &data_1000);
    }
}
