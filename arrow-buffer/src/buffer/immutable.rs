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

use std::fmt::Debug;
use std::iter::FromIterator;
use std::ptr::NonNull;
use std::sync::Arc;
use std::{convert::AsRef, usize};

use crate::alloc::{Allocation, Deallocation};
use crate::util::bit_chunk_iterator::{BitChunks, UnalignedBitChunk};
use crate::{bytes::Bytes, native::ArrowNativeType};

use super::ops::bitwise_unary_op_helper;
use super::MutableBuffer;

/// Buffer represents a contiguous memory region that can be shared with other buffers and across
/// thread boundaries.
#[derive(Clone, PartialEq, Debug)]
pub struct Buffer {
    /// the internal byte buffer.
    data: Arc<Bytes>,

    /// The offset into the buffer.
    offset: usize,

    /// Byte length of the buffer.
    length: usize,
}

impl Buffer {
    /// Auxiliary method to create a new Buffer
    #[inline]
    pub fn from_bytes(bytes: Bytes) -> Self {
        let length = bytes.len();
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
            length,
        }
    }

    /// Initializes a [Buffer] from a slice of items.
    pub fn from_slice_ref<U: ArrowNativeType, T: AsRef<[U]>>(items: T) -> Self {
        let slice = items.as_ref();
        let capacity = slice.len() * std::mem::size_of::<U>();
        let mut buffer = MutableBuffer::with_capacity(capacity);
        buffer.extend_from_slice(slice);
        buffer.into()
    }

    /// Creates a buffer from an existing memory region (must already be byte-aligned), this
    /// `Buffer` will free this piece of memory when dropped.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `capacity` - Total allocated memory for the pointer `ptr`, in **bytes**
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    pub unsafe fn from_raw_parts(ptr: NonNull<u8>, len: usize, capacity: usize) -> Self {
        assert!(len <= capacity);
        Buffer::build_with_arguments(ptr, len, Deallocation::Arrow(capacity))
    }

    /// Creates a buffer from an existing memory region. Ownership of the memory is tracked via reference counting
    /// and the memory will be freed using the `drop` method of [crate::alloc::Allocation] when the reference count reaches zero.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `owner` - A [crate::alloc::Allocation] which is responsible for freeing that data
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len` bytes
    pub unsafe fn from_custom_allocation(
        ptr: NonNull<u8>,
        len: usize,
        owner: Arc<dyn Allocation>,
    ) -> Self {
        Buffer::build_with_arguments(ptr, len, Deallocation::Custom(owner))
    }

    /// Auxiliary method to create a new Buffer
    unsafe fn build_with_arguments(
        ptr: NonNull<u8>,
        len: usize,
        deallocation: Deallocation,
    ) -> Self {
        let bytes = Bytes::new(ptr, len, deallocation);
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
            length: len,
        }
    }

    /// Returns the number of bytes in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns the capacity of this buffer.
    /// For externally owned buffers, this returns zero
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns the byte slice stored in this buffer
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.offset..(self.offset + self.length)]
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `offset` is larger than `len`.
    pub fn slice(&self, offset: usize) -> Self {
        assert!(
            offset <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        Self {
            data: self.data.clone(),
            offset: self.offset + offset,
            length: self.length - offset,
        }
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`,
    /// with `length` bytes.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `(offset + length)` is larger than the existing length.
    pub fn slice_with_length(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        Self {
            data: self.data.clone(),
            offset: self.offset + offset,
            length,
        }
    }

    /// Returns a pointer to the start of this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.ptr().as_ptr().add(self.offset) }
    }

    /// View buffer as a slice of a specific type.
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

    /// Returns a slice of this buffer starting at a certain bit offset.
    /// If the offset is byte-aligned the returned buffer is a shallow clone,
    /// otherwise a new buffer is allocated and filled with a copy of the bits in the range.
    pub fn bit_slice(&self, offset: usize, len: usize) -> Self {
        if offset % 8 == 0 {
            return self.slice(offset / 8);
        }

        bitwise_unary_op_helper(self, offset, len, |a| a)
    }

    /// Returns a `BitChunks` instance which can be used to iterate over this buffers bits
    /// in larger chunks and starting at arbitrary bit offsets.
    /// Note that both `offset` and `length` are measured in bits.
    pub fn bit_chunks(&self, offset: usize, len: usize) -> BitChunks {
        BitChunks::new(self.as_slice(), offset, len)
    }

    /// Returns the number of 1-bits in this buffer.
    #[deprecated(note = "use count_set_bits_offset instead")]
    pub fn count_set_bits(&self) -> usize {
        let len_in_bits = self.len() * 8;
        // self.offset is already taken into consideration by the bit_chunks implementation
        self.count_set_bits_offset(0, len_in_bits)
    }

    /// Returns the number of 1-bits in this buffer, starting from `offset` with `length` bits
    /// inspected. Note that both `offset` and `length` are measured in bits.
    pub fn count_set_bits_offset(&self, offset: usize, len: usize) -> usize {
        UnalignedBitChunk::new(self.as_slice(), offset, len).count_ones()
    }

    /// Returns `MutableBuffer` for mutating the buffer if this buffer is not shared.
    /// Returns `Err` if this is shared or its allocation is from an external source.
    pub fn into_mutable(self) -> Result<MutableBuffer, Self> {
        let offset_ptr = self.as_ptr();
        let offset = self.offset;
        let length = self.length;
        Arc::try_unwrap(self.data)
            .and_then(|bytes| {
                // The pointer of underlying buffer should not be offset.
                assert_eq!(offset_ptr, bytes.ptr().as_ptr());
                MutableBuffer::from_bytes(bytes).map_err(Arc::new)
            })
            .map_err(|bytes| Buffer {
                data: bytes,
                offset,
                length,
            })
    }
}

/// Creating a `Buffer` instance by copying the memory from a `AsRef<[u8]>` into a newly
/// allocated memory region.
impl<T: AsRef<[u8]>> From<T> for Buffer {
    fn from(p: T) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len();
        let mut buffer = MutableBuffer::new(len);
        buffer.extend_from_slice(slice);
        buffer.into()
    }
}

/// Creating a `Buffer` instance by storing the boolean values into the buffer
impl std::iter::FromIterator<bool> for Buffer {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        MutableBuffer::from_iter(iter).into()
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

impl From<MutableBuffer> for Buffer {
    #[inline]
    fn from(buffer: MutableBuffer) -> Self {
        buffer.into_buffer()
    }
}

impl Buffer {
    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it is ~60% faster.
    /// # Example
    /// ```
    /// # use arrow_buffer::buffer::Buffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { Buffer::from_trusted_len_iter(iter) };
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
        MutableBuffer::from_trusted_len_iter(iterator).into()
    }

    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted (upper) length or errors
    /// if any of the items of the iterator is an error.
    /// Prefer this to `collect` whenever possible, as it is ~60% faster.
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
        Ok(MutableBuffer::try_from_trusted_len_iter(iterator)?.into())
    }
}

impl<T: ArrowNativeType> FromIterator<T> for Buffer {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut iterator = iter.into_iter();
        let size = std::mem::size_of::<T>();

        // first iteration, which will likely reserve sufficient space for the buffer.
        let mut buffer = match iterator.next() {
            None => MutableBuffer::new(0),
            Some(element) => {
                let (lower, _) = iterator.size_hint();
                let mut buffer = MutableBuffer::new(lower.saturating_add(1) * size);
                unsafe {
                    std::ptr::write(buffer.as_mut_ptr() as *mut T, element);
                    buffer.set_len(size);
                }
                buffer
            }
        };

        buffer.extend_from_iter(iterator);
        buffer.into()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::thread;

    use super::*;

    #[test]
    fn test_buffer_data_equality() {
        let buf1 = Buffer::from(&[0, 1, 2, 3, 4]);
        let buf2 = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(buf1, buf2);

        // slice with same offset and same length should still preserve equality
        let buf3 = buf1.slice(2);
        assert_ne!(buf1, buf3);
        let buf4 = buf2.slice_with_length(2, 3);
        assert_eq!(buf3, buf4);

        // Different capacities should still preserve equality
        let mut buf2 = MutableBuffer::new(65);
        buf2.extend_from_slice(&[0u8, 1, 2, 3, 4]);

        let buf2 = buf2.into();
        assert_eq!(buf1, buf2);

        // unequal because of different elements
        let buf2 = Buffer::from(&[0, 0, 2, 3, 4]);
        assert_ne!(buf1, buf2);

        // unequal because of different length
        let buf2 = Buffer::from(&[0, 1, 2, 3]);
        assert_ne!(buf1, buf2);
    }

    #[test]
    fn test_from_raw_parts() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.as_ptr().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf.as_slice());
    }

    #[test]
    fn test_from_vec() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.as_ptr().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf.as_slice());
    }

    #[test]
    fn test_copy() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        let buf2 = buf;
        assert_eq!(5, buf2.len());
        assert_eq!(64, buf2.capacity());
        assert!(!buf2.as_ptr().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf2.as_slice());
    }

    #[test]
    fn test_slice() {
        let buf = Buffer::from(&[2, 4, 6, 8, 10]);
        let buf2 = buf.slice(2);

        assert_eq!([6, 8, 10], buf2.as_slice());
        assert_eq!(3, buf2.len());
        assert_eq!(unsafe { buf.as_ptr().offset(2) }, buf2.as_ptr());

        let buf3 = buf2.slice_with_length(1, 2);
        assert_eq!([8, 10], buf3.as_slice());
        assert_eq!(2, buf3.len());
        assert_eq!(unsafe { buf.as_ptr().offset(3) }, buf3.as_ptr());

        let buf4 = buf.slice(5);
        let empty_slice: [u8; 0] = [];
        assert_eq!(empty_slice, buf4.as_slice());
        assert_eq!(0, buf4.len());
        assert!(buf4.is_empty());
        assert_eq!(buf2.slice_with_length(2, 1).as_slice(), &[10]);
    }

    #[test]
    #[should_panic(
        expected = "the offset of the new Buffer cannot exceed the existing length"
    )]
    fn test_slice_offset_out_of_bound() {
        let buf = Buffer::from(&[2, 4, 6, 8, 10]);
        buf.slice(6);
    }

    #[test]
    fn test_access_concurrently() {
        let buffer = Buffer::from(vec![1, 2, 3, 4, 5]);
        let buffer2 = buffer.clone();
        assert_eq!([1, 2, 3, 4, 5], buffer.as_slice());

        let buffer_copy = thread::spawn(move || {
            // access buffer in another thread.
            buffer
        })
        .join();

        assert!(buffer_copy.is_ok());
        assert_eq!(buffer2, buffer_copy.ok().unwrap());
    }

    macro_rules! check_as_typed_data {
        ($input: expr, $native_t: ty) => {{
            let buffer = Buffer::from_slice_ref($input);
            let slice: &[$native_t] = buffer.typed_data::<$native_t>();
            assert_eq!($input, slice);
        }};
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_as_typed_data() {
        check_as_typed_data!(&[1i8, 3i8, 6i8], i8);
        check_as_typed_data!(&[1u8, 3u8, 6u8], u8);
        check_as_typed_data!(&[1i16, 3i16, 6i16], i16);
        check_as_typed_data!(&[1i32, 3i32, 6i32], i32);
        check_as_typed_data!(&[1i64, 3i64, 6i64], i64);
        check_as_typed_data!(&[1u16, 3u16, 6u16], u16);
        check_as_typed_data!(&[1u32, 3u32, 6u32], u32);
        check_as_typed_data!(&[1u64, 3u64, 6u64], u64);
        check_as_typed_data!(&[1f32, 3f32, 6f32], f32);
        check_as_typed_data!(&[1f64, 3f64, 6f64], f64);
    }

    #[test]
    fn test_count_bits() {
        assert_eq!(0, Buffer::from(&[0b00000000]).count_set_bits_offset(0, 8));
        assert_eq!(8, Buffer::from(&[0b11111111]).count_set_bits_offset(0, 8));
        assert_eq!(3, Buffer::from(&[0b00001101]).count_set_bits_offset(0, 8));
        assert_eq!(
            6,
            Buffer::from(&[0b01001001, 0b01010010]).count_set_bits_offset(0, 16)
        );
        assert_eq!(
            16,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(0, 16)
        );
    }

    #[test]
    fn test_count_bits_slice() {
        assert_eq!(
            0,
            Buffer::from(&[0b11111111, 0b00000000])
                .slice(1)
                .count_set_bits_offset(0, 8)
        );
        assert_eq!(
            8,
            Buffer::from(&[0b11111111, 0b11111111])
                .slice_with_length(1, 1)
                .count_set_bits_offset(0, 8)
        );
        assert_eq!(
            3,
            Buffer::from(&[0b11111111, 0b11111111, 0b00001101])
                .slice(2)
                .count_set_bits_offset(0, 8)
        );
        assert_eq!(
            6,
            Buffer::from(&[0b11111111, 0b01001001, 0b01010010])
                .slice_with_length(1, 2)
                .count_set_bits_offset(0, 16)
        );
        assert_eq!(
            16,
            Buffer::from(&[0b11111111, 0b11111111, 0b11111111, 0b11111111])
                .slice(2)
                .count_set_bits_offset(0, 16)
        );
    }

    #[test]
    fn test_count_bits_offset_slice() {
        assert_eq!(8, Buffer::from(&[0b11111111]).count_set_bits_offset(0, 8));
        assert_eq!(3, Buffer::from(&[0b11111111]).count_set_bits_offset(0, 3));
        assert_eq!(5, Buffer::from(&[0b11111111]).count_set_bits_offset(3, 5));
        assert_eq!(1, Buffer::from(&[0b11111111]).count_set_bits_offset(3, 1));
        assert_eq!(0, Buffer::from(&[0b11111111]).count_set_bits_offset(8, 0));
        assert_eq!(2, Buffer::from(&[0b01010101]).count_set_bits_offset(0, 3));
        assert_eq!(
            16,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(0, 16)
        );
        assert_eq!(
            10,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(0, 10)
        );
        assert_eq!(
            10,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(3, 10)
        );
        assert_eq!(
            8,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(8, 8)
        );
        assert_eq!(
            5,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(11, 5)
        );
        assert_eq!(
            0,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(16, 0)
        );
        assert_eq!(
            2,
            Buffer::from(&[0b01101101, 0b10101010]).count_set_bits_offset(7, 5)
        );
        assert_eq!(
            4,
            Buffer::from(&[0b01101101, 0b10101010]).count_set_bits_offset(7, 9)
        );
    }

    #[test]
    fn test_unwind_safe() {
        fn assert_unwind_safe<T: RefUnwindSafe + UnwindSafe>() {}
        assert_unwind_safe::<Buffer>()
    }

    #[test]
    fn test_from_foreign_vec() {
        let mut vector = vec![1_i32, 2, 3, 4, 5];
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                NonNull::new_unchecked(vector.as_mut_ptr() as *mut u8),
                vector.len() * std::mem::size_of::<i32>(),
                Arc::new(vector),
            )
        };

        let slice = buffer.typed_data::<i32>();
        assert_eq!(slice, &[1, 2, 3, 4, 5]);

        let buffer = buffer.slice(std::mem::size_of::<i32>());

        let slice = buffer.typed_data::<i32>();
        assert_eq!(slice, &[2, 3, 4, 5]);
    }
}
