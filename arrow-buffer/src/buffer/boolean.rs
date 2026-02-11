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
use crate::bit_util::read_u64;
use crate::{
    BooleanBufferBuilder, Buffer, MutableBuffer, bit_util, buffer_bin_and, buffer_bin_or,
    buffer_bin_xor,
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
    /// * The new `BooleanBuffer` may have non zero offset
    ///   and/or padding bits outside the logical range.
    ///
    /// # Example: Create a new [`BooleanBuffer`] copying a bit slice from in input slice
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8];
    /// // // Copy bits 4..16 from input
    /// let result = BooleanBuffer::from_bits(&input, 4, 12);
    /// // output is 12 bits long starting from bit offset 4
    /// assert_eq!(result.len(), 12);
    /// assert_eq!(result.offset(), 4);
    /// // the expected 12 bits are 0b101110101100 (bits 4..16 of the input)
    /// let expected_bits = [false, false, true, true, false, true, false, true, true, true, false, true];
    /// for (i, v) in expected_bits.into_iter().enumerate() {
    ///    assert_eq!(result.value(i), v);
    /// }
    /// // However, underlying buffer has (ignored) bits set outside the requested range
    /// assert_eq!(result.values(), &[0b11001100u8, 0b10111010, 0, 0, 0, 0, 0, 0]);
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
    /// * Returned `BooleanBuffer` may have non zero offset
    /// * Returned `BooleanBuffer` may have bits set outside the requested range
    ///
    /// # See Also
    /// - [`BooleanBuffer::from_bitwise_binary_op`] to create a new buffer from a binary operation
    /// - [`apply_bitwise_unary_op`](bit_util::apply_bitwise_unary_op) for in-place unary bitwise operations
    ///
    /// # Example: Create new [`BooleanBuffer`] from bitwise `NOT`
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8]; // 2 bytes = 16 bits
    /// // NOT of bits 4..16
    /// let result = BooleanBuffer::from_bitwise_unary_op(
    ///  &input, 4, 12, |a| !a
    /// );
    /// // output is 12 bits long starting from bit offset 4
    /// assert_eq!(result.len(), 12);
    /// assert_eq!(result.offset(), 4);
    /// // the expected 12 bits are 0b001100110101, (NOT of the requested bits)
    /// let expected_bits = [true, true, false, false, true, false, true, false, false, false, true, false];
    /// for (i, v) in expected_bits.into_iter().enumerate() {
    ///     assert_eq!(result.value(i), v);
    /// }
    /// // However, underlying buffer has (ignored) bits set outside the requested range
    /// let expected = [0b00110011u8, 0b01000101u8, 255, 255, 255, 255, 255, 255];
    /// assert_eq!(result.values(), &expected);
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
        let end = offset_in_bits + len_in_bits;
        // Align start and end to 64 bit (8 byte) boundaries if possible to allow using the
        // optimized code path as much as possible.
        let aligned_offset = offset_in_bits & !63;
        let aligned_end_bytes = bit_util::ceil(end, 64) * 8;
        let src_len = src.as_ref().len();
        let slice_end = aligned_end_bytes.min(src_len);

        let aligned_start = &src.as_ref()[aligned_offset / 8..slice_end];

        let (prefix, aligned_u64s, suffix) = unsafe { aligned_start.as_ref().align_to::<u64>() };
        match (prefix, suffix) {
            ([], []) => {
                // the buffer is word (64 bit) aligned, so use optimized Vec code.
                let result_u64s: Vec<u64> = aligned_u64s.iter().map(|l| op(*l)).collect();
                return BooleanBuffer::new(result_u64s.into(), offset_in_bits % 64, len_in_bits);
            }
            ([], suffix) => {
                let suffix = read_u64(suffix);
                let result_u64s: Vec<u64> = aligned_u64s
                    .iter()
                    .cloned()
                    .chain(std::iter::once(suffix))
                    .map(&mut op)
                    .collect();
                return BooleanBuffer::new(result_u64s.into(), offset_in_bits % 64, len_in_bits);
            }
            _ => {}
        }

        // align to byte boundaries
        // Use unaligned code path, handle remainder bytes
        let chunks = aligned_start.chunks_exact(8);
        let remainder = chunks.remainder();
        let iter = chunks.map(|c| u64::from_le_bytes(c.try_into().unwrap()));
        let vec_u64s: Vec<u64> = if remainder.is_empty() {
            iter.map(&mut op).collect()
        } else {
            iter.chain(Some(read_u64(remainder))).map(&mut op).collect()
        };

        BooleanBuffer::new(vec_u64s.into(), offset_in_bits % 64, len_in_bits)
    }

    /// Create a new [`BooleanBuffer`] by applying the bitwise operation `op` to
    /// the relevant bits from two input buffers.
    ///
    /// This function is faster than applying the operation bit by bit as
    /// it processes input buffers in chunks of 64 bits (8 bytes) at a time
    ///
    /// # Notes:
    /// * `op` takes two `u64` inputs and produces one `u64` output.
    /// * `op` must only apply bitwise operations
    ///   on the relevant bits; the input `u64` values may contain irrelevant bits
    ///   and may be processed differently on different endian architectures.
    /// * `op` may be called with input bits outside the requested range.
    /// * The returned `BooleanBuffer` always has zero offset.
    ///
    /// # See Also
    /// - [`BooleanBuffer::from_bitwise_unary_op`] for unary operations on a single input buffer.
    /// - [`apply_bitwise_binary_op`](bit_util::apply_bitwise_binary_op) for in-place binary bitwise operations
    ///
    /// # Example: Create new [`BooleanBuffer`] from bitwise `AND` of two [`Buffer`]s
    /// ```
    /// # use arrow_buffer::{Buffer, BooleanBuffer};
    /// let left = Buffer::from(vec![0b11001100u8, 0b10111010u8]); // 2 bytes = 16 bits
    /// let right = Buffer::from(vec![0b10101010u8, 0b11011100u8, 0b11110000u8]); // 3 bytes = 24 bits
    /// // AND of the first 12 bits
    /// let result = BooleanBuffer::from_bitwise_binary_op(
    ///   &left, 0, &right, 0, 12, |a, b| a & b
    /// );
    /// assert_eq!(result.inner().as_slice(), &[0b10001000u8, 0b00001000u8]);
    /// ```
    ///
    /// # Example: Create new [`BooleanBuffer`] from bitwise `OR` of two byte slices
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let left = [0b11001100u8, 0b10111010u8];
    /// let right = [0b10101010u8, 0b11011100u8];
    /// // OR of bits 4..16 from left and bits 0..12 from right
    /// let result = BooleanBuffer::from_bitwise_binary_op(
    ///  &left, 4, &right, 0, 12, |a, b| a | b
    /// );
    /// assert_eq!(result.inner().as_slice(), &[0b10101110u8, 0b00001111u8]);
    /// ```
    pub fn from_bitwise_binary_op<F>(
        left: impl AsRef<[u8]>,
        left_offset_in_bits: usize,
        right: impl AsRef<[u8]>,
        right_offset_in_bits: usize,
        len_in_bits: usize,
        mut op: F,
    ) -> Self
    where
        F: FnMut(u64, u64) -> u64,
    {
        let left = left.as_ref();
        let right = right.as_ref();
        // try fast path for aligned input
        // If the underlying buffers are aligned to u64 we can apply the operation directly on the u64 slices
        // to improve performance.
        if left_offset_in_bits & 0x7 == 0 && right_offset_in_bits & 0x7 == 0 {
            // align to byte boundary
            let left = &left[left_offset_in_bits / 8..];
            let right = &right[right_offset_in_bits / 8..];

            unsafe {
                let (left_prefix, left_u64s, left_suffix) = left.align_to::<u64>();
                let (right_prefix, right_u64s, right_suffix) = right.align_to::<u64>();
                // if there is no prefix or suffix, both buffers are aligned and
                // we can do the operation directly on u64s.
                // TODO: consider `slice::as_chunks` and `u64::from_le_bytes` when MSRV reaches 1.88.
                // https://github.com/apache/arrow-rs/pull/9022#discussion_r2639949361
                if left_prefix.is_empty()
                    && right_prefix.is_empty()
                    && left_suffix.is_empty()
                    && right_suffix.is_empty()
                {
                    let result_u64s = left_u64s
                        .iter()
                        .zip(right_u64s.iter())
                        .map(|(l, r)| op(*l, *r))
                        .collect::<Vec<u64>>();
                    return BooleanBuffer {
                        buffer: Buffer::from(result_u64s),
                        bit_offset: 0,
                        bit_len: len_in_bits,
                    };
                }
            }
        }
        let left_chunks = BitChunks::new(left, left_offset_in_bits, len_in_bits);
        let right_chunks = BitChunks::new(right, right_offset_in_bits, len_in_bits);

        let chunks = left_chunks
            .iter()
            .zip(right_chunks.iter())
            .map(|(left, right)| op(left, right));
        // Soundness: `BitChunks` is a `BitChunks` trusted length iterator which
        // correctly reports its upper bound
        let mut buffer = unsafe { MutableBuffer::from_trusted_len_iter(chunks) };

        let remainder_bytes = bit_util::ceil(left_chunks.remainder_len(), 8);
        let rem = op(left_chunks.remainder_bits(), right_chunks.remainder_bits());
        // we are counting its starting from the least significant bit, to to_le_bytes should be correct
        let rem = &rem.to_le_bytes()[0..remainder_bytes];
        buffer.extend_from_slice(rem);

        BooleanBuffer {
            buffer: Buffer::from(buffer),
            bit_offset: 0,
            bit_len: len_in_bits,
        }
    }

    /// Returns the number of set bits in this buffer
    pub fn count_set_bits(&self) -> usize {
        self.buffer
            .count_set_bits_offset(self.bit_offset, self.bit_len)
    }

    /// Finds the position of the n-th set bit (1-based) starting from `start` index.
    /// If fewer than `n` set bits are found, returns the length of the buffer.
    pub fn find_nth_set_bit_position(&self, start: usize, n: usize) -> usize {
        if n == 0 {
            return start;
        }

        self.slice(start, self.bit_len - start)
            .set_indices()
            .nth(n - 1)
            .map(|idx| start + idx + 1)
            .unwrap_or(self.bit_len)
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
        BooleanBuffer::from_bitwise_unary_op(&self.buffer, self.bit_offset, self.bit_len, |a| !a)
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

        // Demonstrate that Non-zero offsets are preserved
        let sliced = boolean_buf.slice(3, 20);
        let result = !&sliced;
        assert_eq!(result.offset(), 3);
        assert_eq!(result.len(), sliced.len());
        for i in 0..sliced.len() {
            assert_eq!(result.value(i), !sliced.value(i));
        }
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

    #[test]
    fn test_from_bitwise_unary_op_unaligned_fallback() {
        // Deterministic affine sequence over u8: b[i] = 37*i + 11 (mod 256).
        // This yields a non-trivial mix of bits (prefix: 11, 48, 85, 122, 159, 196, 233, 14, ...)
        // so unary bit operations are exercised on varied input patterns.
        let bytes = (0..80)
            .map(|i| (i as u8).wrapping_mul(37).wrapping_add(11))
            .collect::<Vec<_>>();
        let base = bytes.as_ptr() as usize;
        let shift = (0..8).find(|s| (base + s) % 8 != 0).unwrap();
        let misaligned = &bytes[shift..];

        // Case 1: fallback path with `remainder.is_empty() == true`
        let src = &misaligned[..24];
        let offset = 7;
        let len = 96;
        let result = BooleanBuffer::from_bitwise_unary_op(src, offset, len, |a| !a);
        let expected = (0..len)
            .map(|i| !bit_util::get_bit(src, offset + i))
            .collect::<BooleanBuffer>();
        assert_eq!(result, expected);
        assert_eq!(result.offset(), offset % 64);

        // Case 2: fallback path with `remainder.is_empty() == false`
        let src = &misaligned[..13];
        let offset = 3;
        let len = 100;
        let result = BooleanBuffer::from_bitwise_unary_op(src, offset, len, |a| !a);
        let expected = (0..len)
            .map(|i| !bit_util::get_bit(src, offset + i))
            .collect::<BooleanBuffer>();
        assert_eq!(result, expected);
        assert_eq!(result.offset(), offset % 64);
    }

    #[test]
    fn test_from_bitwise_binary_op() {
        // pick random boolean inputs
        let input_bools_left = (0..1024)
            .map(|_| rand::random::<bool>())
            .collect::<Vec<bool>>();
        let input_bools_right = (0..1024)
            .map(|_| rand::random::<bool>())
            .collect::<Vec<bool>>();
        let input_buffer_left = BooleanBuffer::from(&input_bools_left[..]);
        let input_buffer_right = BooleanBuffer::from(&input_bools_right[..]);

        for left_offset in 0..200 {
            for right_offset in [0, 4, 5, 17, 33, 24, 45, 64, 65, 100, 200] {
                for len_offset in [0, 1, 44, 100, 256, 300, 512] {
                    let len = 1024 - len_offset - left_offset.max(right_offset); // ensure we don't go out of bounds
                    // compute with AND
                    let result = BooleanBuffer::from_bitwise_binary_op(
                        input_buffer_left.values(),
                        left_offset,
                        input_buffer_right.values(),
                        right_offset,
                        len,
                        |a, b| a & b,
                    );
                    // compute directly from bools
                    let expected = input_bools_left[left_offset..]
                        .iter()
                        .zip(&input_bools_right[right_offset..])
                        .take(len)
                        .map(|(a, b)| *a & *b)
                        .collect::<BooleanBuffer>();
                    assert_eq!(result, expected);
                }
            }
        }
    }

    #[test]
    fn test_extend_trusted_len_sets_byte_len() {
        // Ensures extend_trusted_len keeps the underlying byte length in sync with bit length.
        let mut builder = BooleanBufferBuilder::new(0);
        let bools: Vec<_> = (0..10).map(|i| i % 2 == 0).collect();
        unsafe { builder.extend_trusted_len(bools.into_iter()) };
        assert_eq!(builder.as_slice().len(), bit_util::ceil(builder.len(), 8));
    }

    #[test]
    fn test_extend_trusted_len_then_append() {
        // Exercises append after extend_trusted_len to validate byte length and values.
        let mut builder = BooleanBufferBuilder::new(0);
        let bools: Vec<_> = (0..9).map(|i| i % 3 == 0).collect();
        unsafe { builder.extend_trusted_len(bools.clone().into_iter()) };
        builder.append(true);
        assert_eq!(builder.as_slice().len(), bit_util::ceil(builder.len(), 8));
        let finished = builder.finish();
        for (i, v) in bools.into_iter().chain(std::iter::once(true)).enumerate() {
            assert_eq!(finished.value(i), v, "at index {}", i);
        }
    }

    #[test]
    fn test_find_nth_set_bit_position() {
        let bools = vec![true, false, true, true, false, true];
        let buffer = BooleanBuffer::from(bools);

        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 1), 1);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 2), 3);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 3), 4);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 4), 6);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 5), 6);

        assert_eq!(buffer.clone().find_nth_set_bit_position(1, 1), 3);
        assert_eq!(buffer.clone().find_nth_set_bit_position(3, 1), 4);
        assert_eq!(buffer.clone().find_nth_set_bit_position(3, 2), 6);
    }

    #[test]
    fn test_find_nth_set_bit_position_large() {
        let mut bools = vec![false; 1000];
        bools[100] = true;
        bools[500] = true;
        bools[999] = true;
        let buffer = BooleanBuffer::from(bools);

        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 1), 101);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 2), 501);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 3), 1000);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 4), 1000);

        assert_eq!(buffer.clone().find_nth_set_bit_position(101, 1), 501);
    }

    #[test]
    fn test_find_nth_set_bit_position_sliced() {
        let bools = vec![false, true, false, true, true, false, true]; // [F, T, F, T, T, F, T]
        let buffer = BooleanBuffer::from(bools);
        let slice = buffer.slice(1, 6); // [T, F, T, T, F, T]

        assert_eq!(slice.len(), 6);
        // Logical indices: 0, 1, 2, 3, 4, 5
        // Logical values: T, F, T, T, F, T

        assert_eq!(slice.clone().find_nth_set_bit_position(0, 1), 1);
        assert_eq!(slice.clone().find_nth_set_bit_position(0, 2), 3);
        assert_eq!(slice.clone().find_nth_set_bit_position(0, 3), 4);
        assert_eq!(slice.clone().find_nth_set_bit_position(0, 4), 6);
    }

    #[test]
    fn test_find_nth_set_bit_position_all_set() {
        let buffer = BooleanBuffer::new_set(100);
        for i in 1..=100 {
            assert_eq!(buffer.clone().find_nth_set_bit_position(0, i), i);
        }
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 101), 100);
    }

    #[test]
    fn test_find_nth_set_bit_position_none_set() {
        let buffer = BooleanBuffer::new_unset(100);
        assert_eq!(buffer.clone().find_nth_set_bit_position(0, 1), 100);
    }
}
