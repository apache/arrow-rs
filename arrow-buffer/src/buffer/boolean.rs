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
/// `BooleanBuffer`s can be creating modified using [`BooleanBufferBuilder`]
///
/// # See Also
///
/// * [`Buffer`] for working with bytes
/// * [`NullBuffer`] for representing null values in Arrow arrays
///
/// [`NullBuffer`]: crate::NullBuffer
#[derive(Debug, Clone, Eq)]
pub struct BooleanBuffer {
    buffer: Buffer,
    // offset in bits
    offset: usize,
    // length in bits
    len: usize,
}

impl PartialEq for BooleanBuffer {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }

        let lhs = self.bit_chunks().iter_padded();
        let rhs = other.bit_chunks().iter_padded();
        lhs.zip(rhs).all(|(a, b)| a == b)
    }
}

impl BooleanBuffer {
    /// Create a new [`BooleanBuffer`] from a [`Buffer`], an `offset` and `length` in bits
    ///
    /// # Panics
    ///
    /// This method will panic if `buffer` is not large enough
    pub fn new(buffer: Buffer, offset: usize, len: usize) -> Self {
        let total_len = offset.saturating_add(len);
        let buffer_len = buffer.len();
        let bit_len = buffer_len.saturating_mul(8);
        assert!(
            total_len <= bit_len,
            "buffer not large enough (offset: {offset}, len: {len}, buffer_len: {buffer_len})"
        );
        Self {
            buffer,
            offset,
            len,
        }
    }

    /// Create a new [`BooleanBuffer`] of `length` where all values are `true`
    pub fn new_set(length: usize) -> Self {
        let mut builder = BooleanBufferBuilder::new(length);
        builder.append_n(length, true);
        builder.finish()
    }

    /// Create a new [`BooleanBuffer`] of `length` where all values are `false`
    pub fn new_unset(length: usize) -> Self {
        let buffer = MutableBuffer::new_null(length).into_buffer();
        Self {
            buffer,
            offset: 0,
            len: length,
        }
    }

    /// Create a new [`BooleanBuffer`] by applying the bitwise operation `op` to
    /// two input buffers.
    ///
    /// This function is much faster than applying the operation bit by bit as
    /// it processes input buffers in chunks of 64 bits (8 bytes) at a time
    ///
    /// # Notes:
    /// * `op` takes two `u64` inputs and produces one `u64` output,
    ///   operating on 64 bits at a time.
    /// * `op` must only apply bitwise operations on the relevant bits, as
    ///   the input `u64` may contain irrelevant bits and may be processed
    ///   differently on different endian architectures.
    /// * The inputs are treated as bitmaps, meaning that offsets and length
    ///   are specified in number of bits.
    /// * The output always has zero offset.
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
        // Fast path for aligned inputs
        if left_offset_in_bits % 8 == 0 && right_offset_in_bits % 8 == 0 {
            if let Some(result) = Self::try_from_aligned_bitwise_binary_op(
                &left.as_ref()[left_offset_in_bits / 8..], // aligned to byte boundary
                &right.as_ref()[right_offset_in_bits / 8..],
                len_in_bits,
                &mut op,
            ) {
                return result;
            }
        }

        // each chunk is 64 bits
        let left_chunks = BitChunks::new(left.as_ref(), left_offset_in_bits, len_in_bits);
        let right_chunks = BitChunks::new(right.as_ref(), right_offset_in_bits, len_in_bits);

        let mut result = MutableBuffer::with_capacity(left_chunks.num_u64s() * 8);

        for (left, right) in left_chunks.iter().zip(right_chunks.iter()) {
            // SAFETY: we have reserved enough capacity above, and we are
            // pushing exactly num_u64s() items and `BitChunks` correctly
            // reports its upper bound
            unsafe {
                result.push_unchecked(op(left, right));
            }
        }
        if left_chunks.remainder_len() > 0 {
            debug_assert!(result.capacity() >= result.len() + 8); // should not reallocate 
            result.push(op(
                left_chunks.remainder_bits(),
                right_chunks.remainder_bits(),
            ));
            // Just pushed one u64, which may have trailing zeros,
            // so truncate back to the correct length
            result.truncate(left_chunks.num_bytes());
        }

        BooleanBuffer {
            buffer: Buffer::from(result),
            offset: 0,
            len: len_in_bits,
        }
    }

    /// Like [`from_bitwise_binary_op`] but optimized for the case where the
    /// inputs are aligned to byte boundaries
    ///
    /// Returns `None` if the inputs are not fully u64 aligned
    fn try_from_aligned_bitwise_binary_op<F>(
        left: &[u8],
        right: &[u8],
        len_in_bits: usize,
        op: &mut F,
    ) -> Option<Self>
    where
        F: FnMut(u64, u64) -> u64,
    {
        unsafe {
            // safety: all  bytes are valid u64s
            let (left_prefix, left_u64s, left_suffix) = left.align_to::<u64>();
            let (right_prefix, right_u64s, right_suffix) = right.align_to::<u64>();
            // if there is no prefix or suffix, both buffers are aligned and we can do the operation directly
            // on u64s
            // TODO also handle non empty suffixes by processing them separately
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
                Some(BooleanBuffer::new(
                    Buffer::from(result_u64s),
                    0,
                    len_in_bits,
                ))
            } else {
                None
            }
        }
    }

    /// Create a new [`BooleanBuffer`] by applying the bitwise operation to `op` to an input buffer.
    ///
    /// This function is much faster than applying the operation bit by bit as
    /// it processes input buffers in chunks of 64 bits (8 bytes) at a time
    ///
    /// # Notes:
    /// * `op` takes a single `u64` inputs and produces one `u64` output
    ///   operating on 64 bits at a time.
    /// * `op` must only apply bitwise operations
    ///   on the relevant bits, as the input `u64` may contain irrelevant bits
    ///   and may be processed differently on different endian architectures.
    /// * The inputs are treated as bitmaps, meaning that offsets and length
    ///   are specified in number of bits.
    /// * The output always has zero offset
    ///
    /// # See Also
    /// - [`BooleanBuffer::from_bitwise_binary_op`] for binary operations on a single input buffer.
    /// - [`apply_bitwise_unary_op`](bit_util::apply_bitwise_unary_op) for in-place unary bitwise operations
    ///
    /// # Example: Create new [`Buffer`] from bitwise `NOT` of an input [`Buffer`]
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8]; // 2 bytes = 16 bits
    /// // NOT of the first 12 bits
    /// let result = BooleanBuffer::from_bitwise_unary_op(
    ///  &input, 0, 12, |a| !a
    /// );
    /// assert_eq!(result.inner().as_slice(), &[0b00110011u8, 0b11110101u8]);
    /// ```
    ///
    /// # Example: Create a new [`BooleanBuffer`] copying a bit slice from in input slice
    /// ```
    /// # use arrow_buffer::BooleanBuffer;
    /// let input = [0b11001100u8, 0b10111010u8];
    /// // // Copy bits 4..16 from input
    /// let result = BooleanBuffer::from_bitwise_unary_op(
    ///    &input, 4, 12, |a| a
    /// );
    /// assert_eq!(result.inner().as_slice(), &[0b10101100u8, 0b00001011u8]);
    pub fn from_bitwise_unary_op<F>(
        left: impl AsRef<[u8]>,
        offset_in_bits: usize,
        len_in_bits: usize,
        mut op: F,
    ) -> Self
    where
        F: FnMut(u64) -> u64,
    {
        // try fast path for aligned input
        if offset_in_bits % 8 == 0 {
            if let Some(result) = Self::try_from_aligned_bitwise_unary_op(
                &left.as_ref()[offset_in_bits / 8..], // align to byte boundary
                len_in_bits,
                &mut op,
            ) {
                return result;
            }
        }

        // each chunk is 64 bits
        let left_chunks = BitChunks::new(left.as_ref(), offset_in_bits, len_in_bits);
        let mut result = MutableBuffer::with_capacity(left_chunks.num_u64s() * 8);
        for left in left_chunks.iter() {
            // SAFETY: we have reserved enough capacity above, and we are
            // pushing exactly num_u64s() items and `BitChunks` correctly
            // reports its upper bound
            unsafe {
                result.push_unchecked(op(left));
            }
        }
        if left_chunks.remainder_len() > 0 {
            debug_assert!(result.capacity() >= result.len() + 8); // should not reallocate
            result.push(op(left_chunks.remainder_bits()));
            // Just pushed one u64, which may have have trailing zeros,
            result.truncate(left_chunks.num_bytes());
        }

        BooleanBuffer {
            buffer: Buffer::from(result),
            offset: 0,
            len: len_in_bits,
        }
    }

    /// Like [`from_bitwise_unary_op`] but optimized for the case where the
    /// input is aligned to byte boundaries
    fn try_from_aligned_bitwise_unary_op<F>(
        left: &[u8],
        len_in_bits: usize,
        op: &mut F,
    ) -> Option<Self>
    where
        F: FnMut(u64) -> u64,
    {
        unsafe {
            // safety: all valid bytes are valid u64s
            let (left_prefix, left_u64s, left_suffix) = left.align_to::<u64>();
            // if there is no prefix or suffix, the buffer is aligned and we can do the operation directly
            // on u64s
            // TODO also handle non empty suffixes by processing them separately
            if left_prefix.is_empty() && left_suffix.is_empty() {
                let result_u64s = left_u64s.iter().map(|l| op(*l)).collect::<Vec<u64>>();
                Some(BooleanBuffer::new(
                    Buffer::from(result_u64s),
                    0,
                    len_in_bits,
                ))
            } else {
                None
            }
        }
    }

    /// Invokes `f` with indexes `0..len` collecting the boolean results into a new `BooleanBuffer`
    pub fn collect_bool<F: FnMut(usize) -> bool>(len: usize, f: F) -> Self {
        let buffer = MutableBuffer::collect_bool(len, f);
        Self::new(buffer.into(), 0, len)
    }

    /// Returns the number of set bits in this buffer
    pub fn count_set_bits(&self) -> usize {
        self.buffer.count_set_bits_offset(self.offset, self.len)
    }

    /// Returns a `BitChunks` instance which can be used to iterate over
    /// this buffer's bits in `u64` chunks
    #[inline]
    pub fn bit_chunks(&self) -> BitChunks<'_> {
        BitChunks::new(self.values(), self.offset, self.len)
    }

    /// Returns the offset of this [`BooleanBuffer`] in bits
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the length of this [`BooleanBuffer`] in bits
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this [`BooleanBuffer`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
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
        assert!(idx < self.len);
        unsafe { self.value_unchecked(idx) }
    }

    /// Returns the boolean value at index `i`.
    ///
    /// # Safety
    /// This doesn't check bounds, the caller must ensure that index < self.len()
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> bool {
        unsafe { bit_util::get_bit_raw(self.buffer.as_ptr(), i + self.offset) }
    }

    /// Returns the packed values of this [`BooleanBuffer`] not including any offset
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.buffer
    }

    /// Slices this [`BooleanBuffer`] by the provided `offset` and `length`
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced BooleanBuffer cannot exceed the existing length"
        );
        Self {
            buffer: self.buffer.clone(),
            offset: self.offset + offset,
            len,
        }
    }

    /// Returns a [`Buffer`] containing the sliced contents of this [`BooleanBuffer`]
    ///
    /// Equivalent to `self.buffer.bit_slice(self.offset, self.len)`
    pub fn sliced(&self) -> Buffer {
        self.buffer.bit_slice(self.offset, self.len)
    }

    /// Returns true if this [`BooleanBuffer`] is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.buffer.as_ptr() == other.buffer.as_ptr()
            && self.offset == other.offset
            && self.len == other.len
    }

    /// Returns the inner [`Buffer`]
    ///
    /// Note: this does not account for offset and length of this [`BooleanBuffer`]
    #[inline]
    pub fn inner(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the inner [`Buffer`], consuming self
    pub fn into_inner(self) -> Buffer {
        self.buffer
    }

    /// Returns an iterator over the bits in this [`BooleanBuffer`]
    pub fn iter(&self) -> BitIterator<'_> {
        self.into_iter()
    }

    /// Returns an iterator over the set bit positions in this [`BooleanBuffer`]
    pub fn set_indices(&self) -> BitIndexIterator<'_> {
        BitIndexIterator::new(self.values(), self.offset, self.len)
    }

    /// Returns a `u32` iterator over set bit positions without any usize->u32 conversion
    pub fn set_indices_u32(&self) -> BitIndexU32Iterator<'_> {
        BitIndexU32Iterator::new(self.values(), self.offset, self.len)
    }

    /// Returns a [`BitSliceIterator`] yielding contiguous ranges of set bits
    pub fn set_slices(&self) -> BitSliceIterator<'_> {
        BitSliceIterator::new(self.values(), self.offset, self.len)
    }
}

impl Not for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn not(self) -> Self::Output {
        BooleanBuffer {
            buffer: buffer_unary_not(&self.buffer, self.offset, self.len),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitAnd<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitand(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_and(&self.buffer, self.offset, &rhs.buffer, rhs.offset, self.len),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitOr<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_or(&self.buffer, self.offset, &rhs.buffer, rhs.offset, self.len),
            offset: 0,
            len: self.len,
        }
    }
}

impl BitXor<&BooleanBuffer> for &BooleanBuffer {
    type Output = BooleanBuffer;

    fn bitxor(self, rhs: &BooleanBuffer) -> Self::Output {
        assert_eq!(self.len, rhs.len);
        BooleanBuffer {
            buffer: buffer_bin_xor(&self.buffer, self.offset, &rhs.buffer, rhs.offset, self.len),
            offset: 0,
            len: self.len,
        }
    }
}

impl<'a> IntoIterator for &'a BooleanBuffer {
    type Item = bool;
    type IntoIter = BitIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitIterator::new(self.values(), self.offset, self.len)
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
        assert_eq!(16, boolean_slice1.offset);
        assert_eq!(16, boolean_slice1.len);

        assert_eq!(bytes, boolean_slice2.values());
        assert_eq!(0, boolean_slice2.offset);
        assert_eq!(16, boolean_slice2.len);
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
        let input_bools = (0..1024).map(|_| rand::random::<bool>()).collect::<Vec<bool>>();
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
            let result = BooleanBuffer::from_bitwise_unary_op(
                input_buffer.values(),
                offset,
                len,
                |a| !a,
            );
            let expected = input_bools[offset..]
                .iter()
                .take(len)
                .map(|b| !*b)
                .collect::<BooleanBuffer>();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_from_bitwise_binary_op() {
        // pick random boolean inputs
        let input_bools_left = (0..1024).map(|_| rand::random::<bool>()).collect::<Vec<bool>>();
        let input_bools_right = (0..1024).map(|_| rand::random::<bool>()).collect::<Vec<bool>>();
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
}
