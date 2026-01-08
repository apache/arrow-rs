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

use crate::bit_chunk_iterator::BitChunks;
use crate::bit_iterator::{BitIndexIterator, BitIndexU32Iterator, BitIterator, BitSliceIterator};
use crate::{
    BooleanBufferBuilder, Buffer, MutableBuffer, bit_util, buffer_bin_and, buffer_bin_or,
    buffer_bin_xor, buffer_unary_not,
};

use std::ops::{BitAnd, BitOr, BitXor, Not};

/// A slice-able [`Buffer`] containing bit-packed booleans
///
/// This structure represents a sequence of boolean values packed into a
/// byte-aligned [`Buffer`]. Both the offset and length are represented in bits.
///
/// # Layout
///
/// The values are represented as little endian bit-packed values, where the
/// least significant bit of each byte represents the first boolean value and
/// then proceeding to the most significant bit.
///
/// For example, the 10 bit bitmask `0b0111001101` has length 10, and is
/// represented using 2 bytes with offset 0 like this:
///
/// ```text
///        ┌─────────────────────────────────┐    ┌─────────────────────────────────┐
///        │┌───┬───┬───┬───┬───┬───┬───┬───┐│    │┌───┬───┬───┬───┬───┬───┬───┬───┐│
///        ││ 1 │ 0 │ 1 │ 1 │ 0 │ 0 │ 1 │ 1 ││    ││ 1 │ 0 │ ? │ ? │ ? │ ? │ ? │ ? ││
///        │└───┴───┴───┴───┴───┴───┴───┴───┘│    │└───┴───┴───┴───┴───┴───┴───┴───┘│
/// bit    └─────────────────────────────────┘    └─────────────────────────────────┘
/// offset  0             Byte 0             7    0              Byte 1            7
///
///         length = 10 bits, offset = 0
/// ```
///
/// The same bitmask with length 10 and offset 3 would be represented using 2
/// bytes like this:
///
/// ```text
///       ┌─────────────────────────────────┐    ┌─────────────────────────────────┐
///       │┌───┬───┬───┬───┬───┬───┬───┬───┐│    │┌───┬───┬───┬───┬───┬───┬───┬───┐│
///       ││ ? │ ? │ ? │ 1 │ 0 │ 1 │ 1 │ 0 ││    ││ 0 │ 1 │ 1 │ 1 │ 0 │ ? │ ? │ ? ││
///       │└───┴───┴───┴───┴───┴───┴───┴───┘│    │└───┴───┴───┴───┴───┴───┴───┴───┘│
/// bit   └─────────────────────────────────┘    └─────────────────────────────────┘
/// offset 0             Byte 0             7    0              Byte 1            7
///
///        length = 10 bits, offset = 3
/// ```
///
/// Note that the bits marked `?` are not logically part of the mask and may
/// contain either `0` or `1`
///
/// # See Also
/// * [`BooleanBufferBuilder`] for building [`BooleanBuffer`] instances
/// * [`NullBuffer`] for representing null values in Arrow arrays
///
/// [`NullBuffer`]: crate::NullBuffer
#[derive(Debug, Clone, Eq)]
pub struct BooleanBuffer {
    /// Underlying buffer (byte aligned)
    buffer: Buffer,
    /// Offset in bits (not bytes)
    bit_offset: usize,
    /// Length in bits (not bytes)
    bit_len: usize,
}

impl PartialEq for BooleanBuffer {
    fn eq(&self, other: &Self) -> bool {
        if self.bit_len != other.bit_len {
            return false;
        }

        let lhs = self.bit_chunks().iter_padded();
        let rhs = other.bit_chunks().iter_padded();
        lhs.zip(rhs).all(|(a, b)| a == b)
    }
}

impl BooleanBuffer {
    /// Create a new [`BooleanBuffer`] from a [`Buffer`], `bit_offset` offset and `bit_len` length
    ///
    /// # Panics
    ///
    /// This method will panic if `buffer` is not large enough
    pub fn new(buffer: Buffer, bit_offset: usize, bit_len: usize) -> Self {
        let total_len = bit_offset.saturating_add(bit_len);
        let buffer_len = buffer.len();
        let buffer_bit_len = buffer_len.saturating_mul(8);
        assert!(
            total_len <= buffer_bit_len,
            "buffer not large enough (bit_offset: {bit_offset}, bit_len: {bit_len}, buffer_len: {buffer_len})"
        );
        Self {
            buffer,
            bit_offset,
            bit_len,
        }
    }

    /// Create a new [`BooleanBuffer`] of `length` bits (not bytes) where all values are `true`
    pub fn new_set(length: usize) -> Self {
        let mut builder = BooleanBufferBuilder::new(length);
        builder.append_n(length, true);
        builder.finish()
    }

    /// Create a new [`BooleanBuffer`] of `length` bits (not bytes) where all values are `false`
    pub fn new_unset(length: usize) -> Self {
        let buffer = MutableBuffer::new_null(length).into_buffer();
        Self {
            buffer,
            bit_offset: 0,
            bit_len: length,
        }
    }

    /// Invokes `f` with indexes `0..len` collecting the boolean results into a new `BooleanBuffer`
    pub fn collect_bool<F: FnMut(usize) -> bool>(len: usize, f: F) -> Self {
        let buffer = MutableBuffer::collect_bool(len, f);
        Self::new(buffer.into(), 0, len)
    }

    /// Create a new [`BooleanBuffer`] by copying the relevant bits from an
    /// input buffer.
    ///
    /// # Notes:
    /// * The new `BooleanBuffer` has zero offset, even if `offset_in_bits` is non-zero
    ///
    /// # Example: Create a new [`BooleanBuffer`] copying a bit slice from in input slice
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8];
    /// // // Copy bits 4..16 from input
    /// let result = BooleanBuffer::from_bits(&input, 4, 12);
    /// assert_eq!(result.values(), &[0b10101100u8, 0b00001011u8]);
    pub fn from_bits(src: impl AsRef<[u8]>, offset_in_bits: usize, len_in_bits: usize) -> Self {
        Self::from_bitwise_unary_op(src, offset_in_bits, len_in_bits, |a| a)
    }

    /// Create a new [`BooleanBuffer`] by applying the bitwise operation to `op`
    /// to an input buffer.
    ///
    /// This function is faster than applying the operation bit by bit as
    /// it processes input buffers in chunks of 64 bits (8 bytes) at a time
    ///
    /// # Notes:
    /// * `op` takes a single `u64` inputs and produces one `u64` output.
    /// * `op` must only apply bitwise operations
    ///   on the relevant bits; the input `u64` may contain irrelevant bits
    ///   and may be processed differently on different endian architectures.
    /// * `op` may be called with input bits outside the requested range
    /// * The output always has zero offset
    ///
    /// # See Also
    /// - [`apply_bitwise_unary_op`](bit_util::apply_bitwise_unary_op) for in-place unary bitwise operations
    ///
    /// # Example: Create new [`BooleanBuffer`] from bitwise `NOT` of an input [`Buffer`]
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8]; // 2 bytes = 16 bits
    /// // NOT of the first 12 bits
    /// let result = BooleanBuffer::from_bitwise_unary_op(
    ///  &input, 0, 12, |a| !a
    /// );
    /// assert_eq!(result.values(), &[0b00110011u8, 0b11110101u8]);
    /// ```
    pub fn from_bitwise_unary_op<F>(
        src: impl AsRef<[u8]>,
        offset_in_bits: usize,
        len_in_bits: usize,
        mut op: F,
    ) -> Self
    where
        F: FnMut(u64) -> u64,
    {
        // try fast path for aligned input
        if offset_in_bits & 0x7 == 0 {
            // align to byte boundary
            let aligned = &src.as_ref()[offset_in_bits / 8..];
            if let Some(result) =
                Self::try_from_aligned_bitwise_unary_op(aligned, len_in_bits, &mut op)
            {
                return result;
            }
        }

        let chunks = BitChunks::new(src.as_ref(), offset_in_bits, len_in_bits);
        let mut result = MutableBuffer::with_capacity(chunks.num_u64s() * 8);
        for chunk in chunks.iter() {
            // SAFETY: reserved enough capacity above, (exactly num_u64s()
            // items) and we assume `BitChunks` correctly reports upper bound
            unsafe {
                result.push_unchecked(op(chunk));
            }
        }
        if chunks.remainder_len() > 0 {
            debug_assert!(result.capacity() >= result.len() + 8); // should not reallocate
            // SAFETY: reserved enough capacity above, (exactly num_u64s()
            // items) and we assume `BitChunks` correctly reports upper bound
            unsafe {
                result.push_unchecked(op(chunks.remainder_bits()));
            }
            // Just pushed one u64, which may have trailing zeros
            result.truncate(chunks.num_bytes());
        }

        let buffer = Buffer::from(result);
        BooleanBuffer {
            buffer,
            bit_offset: 0,
            bit_len: len_in_bits,
        }
    }

    /// Fast path for [`Self::from_bitwise_unary_op`] when input is aligned to
    /// 8-byte (64-bit) boundaries
    ///
    /// Returns None if the fast path cannot be taken
    fn try_from_aligned_bitwise_unary_op<F>(
        src: &[u8],
        len_in_bits: usize,
        op: &mut F,
    ) -> Option<Self>
    where
        F: FnMut(u64) -> u64,
    {
        // Safety: all valid bytes are valid u64s
        let (prefix, aligned_u6us, suffix) = unsafe { src.align_to::<u64>() };
        if !(prefix.is_empty() && suffix.is_empty()) {
            // Couldn't make this case any faster than the default path, see
            // https://github.com/apache/arrow-rs/pull/8996/changes#r2620022082
            return None;
        }
        // the buffer is word (64 bit) aligned, so use optimized Vec code.
        let result_u64s: Vec<u64> = aligned_u6us.iter().map(|l| op(*l)).collect();
        let buffer = Buffer::from(result_u64s);
        Some(BooleanBuffer::new(buffer, 0, len_in_bits))
    }

    /// Returns the number of set bits in this buffer
    pub fn count_set_bits(&self) -> usize {
        self.buffer
            .count_set_bits_offset(self.bit_offset, self.bit_len)
    }

    /// Returns a [`BitChunks`] instance which can be used to iterate over
    /// this buffer's bits in `u64` chunks
    #[inline]
    pub fn bit_chunks(&self) -> BitChunks<'_> {
        BitChunks::new(self.values(), self.bit_offset, self.bit_len)
    }

    /// Returns the offset of this [`BooleanBuffer`] in bits (not bytes)
    #[inline]
    pub fn offset(&self) -> usize {
        self.bit_offset
    }

    /// Returns the length of this [`BooleanBuffer`] in bits (not bytes)
    #[inline]
    pub fn len(&self) -> usize {
        self.bit_len
    }

    /// Returns true if this [`BooleanBuffer`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Free up unused memory.
    pub fn shrink_to_fit(&mut self) {
        // TODO(emilk): we could shrink even more in the case where we are a small sub-slice of the full buffer
        self.buffer.shrink_to_fit();
    }

    /// Returns the boolean value at index `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i >= self.len()`
    #[inline]
    pub fn value(&self, idx: usize) -> bool {
        assert!(idx < self.bit_len);
        unsafe { self.value_unchecked(idx) }
    }

    /// Returns the boolean value at index `i`.
    ///
    /// # Safety
    /// This doesn't check bounds, the caller must ensure that index < self.len()
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> bool {
        unsafe { bit_util::get_bit_raw(self.buffer.as_ptr(), i + self.bit_offset) }
    }

    /// Returns the packed values of this [`BooleanBuffer`] not including any offset
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.buffer
    }

    /// Slices this [`BooleanBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.bit_len,
            "the length + offset of the sliced BooleanBuffer cannot exceed the existing length"
        );
        Self {
            buffer: self.buffer.clone(),
            bit_offset: self.bit_offset + offset,
            bit_len: len,
        }
    }

    /// Returns a [`Buffer`] containing the sliced contents of this [`BooleanBuffer`]
    ///
    /// Equivalent to `self.buffer.bit_slice(self.offset, self.len)`
    pub fn sliced(&self) -> Buffer {
        self.buffer.bit_slice(self.bit_offset, self.bit_len)
    }

    /// Returns true if this [`BooleanBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.buffer.as_ptr() == other.buffer.as_ptr()
            && self.bit_offset == other.bit_offset
            && self.bit_len == other.bit_len
    }

    /// Returns the inner [`Buffer`]
    ///
    /// Note: this does not account for offset and length of this [`BooleanBuffer`]
    #[inline]
    pub fn inner(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the inner [`Buffer`], consuming self
    ///
    /// Note: this does not account for offset and length of this [`BooleanBuffer`]
    pub fn into_inner(self) -> Buffer {
        self.buffer
    }

    /// Returns an iterator over the bits in this [`BooleanBuffer`]
    pub fn iter(&self) -> BitIterator<'_> {
        self.into_iter()
    }

    /// Returns an iterator over the set bit positions in this [`BooleanBuffer`]
    pub fn set_indices(&self) -> BitIndexIterator<'_> {
        BitIndexIterator::new(self.values(), self.bit_offset, self.bit_len)
    }

    /// Returns a `u32` iterator over set bit positions without any usize->u32 conversion
    pub fn set_indices_u32(&self) -> BitIndexU32Iterator<'_> {
        BitIndexU32Iterator::new(self.values(), self.bit_offset, self.bit_len)
    }

    /// Returns a [`BitSliceIterator`] yielding contiguous ranges of set bits
    pub fn set_slices(&self) -> BitSliceIterator<'_> {
        BitSliceIterator::new(self.values(), self.bit_offset, self.bit_len)
    }
}

impl Not for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn not(self) -> Self::Output {
        BooleanBuffer {
            buffer: buffer_unary_not(&self.buffer, self.bit_offset, self.bit_len),
            bit_offset: 0,
            bit_len: self.bit_len,
        }
    }
}

impl BitAnd<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitand(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.bit_len, rhs.bit_len);
        BooleanBuffer {
            buffer: buffer_bin_and(
                &self.buffer,
                self.bit_offset,
                &rhs.buffer,
                rhs.bit_offset,
                self.bit_len,
            ),
            bit_offset: 0,
            bit_len: self.bit_len,
        }
    }
}

impl BitOr<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.bit_len, rhs.bit_len);
        BooleanBuffer {
            buffer: buffer_bin_or(
                &self.buffer,
                self.bit_offset,
                &rhs.buffer,
                rhs.bit_offset,
                self.bit_len,
            ),
            bit_offset: 0,
            bit_len: self.bit_len,
        }
    }
}

impl BitXor<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitxor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.bit_len, rhs.bit_len);
        BooleanBuffer {
            buffer: buffer_bin_xor(
                &self.buffer,
                self.bit_offset,
                &rhs.buffer,
                rhs.bit_offset,
                self.bit_len,
            ),
            bit_offset: 0,
            bit_len: self.bit_len,
        }
    }
}

impl<'a> IntoIterator for &'a BooleanBuffer {
    type Item = bool;
    type IntoIter = BitIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitIterator::new(self.values(), self.bit_offset, self.bit_len)
    }
}

impl From<&[bool]> for BooleanBuffer {
    fn from(value: &[bool]) -> Self {
        let mut builder = BooleanBufferBuilder::new(value.len());
        builder.append_slice(value);
        builder.finish()
    }
}

impl From<Vec<bool>> for BooleanBuffer {
    fn from(value: Vec<bool>) -> Self {
        value.as_slice().into()
    }
}

impl FromIterator<bool> for BooleanBuffer {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (hint, _) = iter.size_hint();
        let mut builder = BooleanBufferBuilder::new(hint);
        iter.for_each(|b| builder.append(b));
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_new() {
        let bytes = &[0, 1, 2, 3, 4];
        let buf = Buffer::from(bytes);
        let offset = 0;
        let len = 24;

        let boolean_buf = BooleanBuffer::new(buf.clone(), offset, len);
        assert_eq!(bytes, boolean_buf.values());
        assert_eq!(offset, boolean_buf.offset());
        assert_eq!(len, boolean_buf.len());

        assert_eq!(2, boolean_buf.count_set_bits());
        assert_eq!(&buf, boolean_buf.inner());
        assert_eq!(buf, boolean_buf.clone().into_inner());

        assert!(!boolean_buf.is_empty())
    }

    #[test]
    fn test_boolean_data_equality() {
        let boolean_buf1 = BooleanBuffer::new(Buffer::from(&[0, 1, 4, 3, 5]), 0, 32);
        let boolean_buf2 = BooleanBuffer::new(Buffer::from(&[0, 1, 4, 3, 5]), 0, 32);
        assert_eq!(boolean_buf1, boolean_buf2);

        // slice with same offset and same length should still preserve equality
        let boolean_buf3 = boolean_buf1.slice(8, 16);
        assert_ne!(boolean_buf1, boolean_buf3);
        let boolean_buf4 = boolean_buf1.slice(0, 32);
        assert_eq!(boolean_buf1, boolean_buf4);

        // unequal because of different elements
        let boolean_buf2 = BooleanBuffer::new(Buffer::from(&[0, 0, 2, 3, 4]), 0, 32);
        assert_ne!(boolean_buf1, boolean_buf2);

        // unequal because of different length
        let boolean_buf2 = BooleanBuffer::new(Buffer::from(&[0, 1, 4, 3, 5]), 0, 24);
        assert_ne!(boolean_buf1, boolean_buf2);

        // ptr_eq
        assert!(boolean_buf1.ptr_eq(&boolean_buf1));
        assert!(boolean_buf2.ptr_eq(&boolean_buf2));
        assert!(!boolean_buf1.ptr_eq(&boolean_buf2));
    }

    #[test]
    fn test_boolean_slice() {
        let bytes = &[0, 3, 2, 6, 2];
        let boolean_buf1 = BooleanBuffer::new(Buffer::from(bytes), 0, 32);
        let boolean_buf2 = BooleanBuffer::new(Buffer::from(bytes), 0, 32);

        let boolean_slice1 = boolean_buf1.slice(16, 16);
        let boolean_slice2 = boolean_buf2.slice(0, 16);
        assert_eq!(boolean_slice1.values(), boolean_slice2.values());

        assert_eq!(bytes, boolean_slice1.values());
        assert_eq!(16, boolean_slice1.bit_offset);
        assert_eq!(16, boolean_slice1.bit_len);

        assert_eq!(bytes, boolean_slice2.values());
        assert_eq!(0, boolean_slice2.bit_offset);
        assert_eq!(16, boolean_slice2.bit_len);
    }

    #[test]
    fn test_boolean_bitand() {
        let offset = 0;
        let len = 40;

        let buf1 = Buffer::from(&[0, 1, 1, 0, 0]);
        let boolean_buf1 = &BooleanBuffer::new(buf1, offset, len);

        let buf2 = Buffer::from(&[0, 1, 1, 1, 0]);
        let boolean_buf2 = &BooleanBuffer::new(buf2, offset, len);

        let expected = BooleanBuffer::new(Buffer::from(&[0, 1, 1, 0, 0]), offset, len);
        assert_eq!(boolean_buf1 & boolean_buf2, expected);
    }

    #[test]
    fn test_boolean_bitor() {
        let offset = 0;
        let len = 40;

        let buf1 = Buffer::from(&[0, 1, 1, 0, 0]);
        let boolean_buf1 = &BooleanBuffer::new(buf1, offset, len);

        let buf2 = Buffer::from(&[0, 1, 1, 1, 0]);
        let boolean_buf2 = &BooleanBuffer::new(buf2, offset, len);

        let expected = BooleanBuffer::new(Buffer::from(&[0, 1, 1, 1, 0]), offset, len);
        assert_eq!(boolean_buf1 | boolean_buf2, expected);
    }

    #[test]
    fn test_boolean_bitxor() {
        let offset = 0;
        let len = 40;

        let buf1 = Buffer::from(&[0, 1, 1, 0, 0]);
        let boolean_buf1 = &BooleanBuffer::new(buf1, offset, len);

        let buf2 = Buffer::from(&[0, 1, 1, 1, 0]);
        let boolean_buf2 = &BooleanBuffer::new(buf2, offset, len);

        let expected = BooleanBuffer::new(Buffer::from(&[0, 0, 0, 1, 0]), offset, len);
        assert_eq!(boolean_buf1 ^ boolean_buf2, expected);
    }

    #[test]
    fn test_boolean_not() {
        let offset = 0;
        let len = 40;

        let buf = Buffer::from(&[0, 1, 1, 0, 0]);
        let boolean_buf = &BooleanBuffer::new(buf, offset, len);

        let expected = BooleanBuffer::new(Buffer::from(&[255, 254, 254, 255, 255]), offset, len);
        assert_eq!(!boolean_buf, expected);
    }

    #[test]
    fn test_boolean_from_slice_bool() {
        let v = [true, false, false];
        let buf = BooleanBuffer::from(&v[..]);
        assert_eq!(buf.offset(), 0);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.values().len(), 1);
        assert!(buf.value(0));
    }

    #[test]
    fn test_from_bitwise_unary_op() {
        // Use 1024 boolean values so that at least some of the tests cover multiple u64 chunks and
        // perfect alignment
        let input_bools = (0..1024)
            .map(|_| rand::random::<bool>())
            .collect::<Vec<bool>>();
        let input_buffer = BooleanBuffer::from(&input_bools[..]);

        // Note ensure we test offsets over 100 to cover multiple u64 chunks
        for offset in 0..1024 {
            let result = BooleanBuffer::from_bitwise_unary_op(
                input_buffer.values(),
                offset,
                input_buffer.len() - offset,
                |a| !a,
            );
            let expected = input_bools[offset..]
                .iter()
                .map(|b| !*b)
                .collect::<BooleanBuffer>();
            assert_eq!(result, expected);
        }

        // Also test when the input doesn't cover the entire buffer
        for offset in 0..512 {
            let len = 512 - offset; // fixed length less than total
            let result =
                BooleanBuffer::from_bitwise_unary_op(input_buffer.values(), offset, len, |a| !a);
            let expected = input_bools[offset..]
                .iter()
                .take(len)
                .map(|b| !*b)
                .collect::<BooleanBuffer>();
            assert_eq!(result, expected);
        }
    }
}
