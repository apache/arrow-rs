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

//! Utils for working with bits

use crate::bit_chunk_iterator::BitChunks;
use std::ops::{Bound, RangeBounds};

/// Returns the nearest number that is `>=` than `num` and is a multiple of 64
///
/// # Panics
///
/// Panics if rounding `num` up overflows `usize`
#[inline]
pub fn round_upto_multiple_of_64(num: usize) -> usize {
    num.checked_next_multiple_of(64)
        .expect("failed to round upto multiple of 64")
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
///
/// # Panics
///
/// Panics if rounding `num` up overflows `usize`
pub fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && factor.is_power_of_two());
    num.checked_add(factor - 1)
        .expect("failed to round to next highest power of 2")
        & !(factor - 1)
}

/// Returns whether bit at position `i` in `data` is set or not
///
/// # Panics
///
/// Panics if `i / 8 >= data.len()`
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    data[i / 8] & (1 << (i % 8)) != 0
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    unsafe { (*data.add(i / 8) & (1 << (i % 8))) != 0 }
}

/// Sets bit at position `i` for `data` to 1
///
/// # Panics
///
/// Panics if `i / 8 >= data.len()`
#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i / 8] |= 1 << (i % 8);
}

/// Sets bit at position `i` for `data`
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    unsafe {
        *data.add(i / 8) |= 1 << (i % 8);
    }
}

/// Sets bit at position `i` for `data` to 0
///
/// # Panics
///
/// Panics if `i / 8 >= data.len()`
#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i / 8] &= !(1 << (i % 8));
}

/// Sets bit at position `i` for `data` to 0
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    unsafe {
        *data.add(i / 8) &= !(1 << (i % 8));
    }
}

/// Returns the ceil of `value`/`divisor`
#[inline]
pub fn ceil(value: usize, divisor: usize) -> usize {
    value.div_ceil(divisor)
}

/// Read a u64 from a byte slice, padding with zeros if necessary
#[inline]
pub(crate) fn read_u64(input: &[u8]) -> u64 {
    let len = input.len().min(8);
    let mut buf = [0_u8; 8];
    buf[..len].copy_from_slice(input);
    u64::from_le_bytes(buf)
}

/// Read up to 8 bits from a byte slice starting at a given bit offset.
///
/// # Arguments
///
/// * `slice` - The byte slice to read from
/// * `number_of_bits_to_read` - Number of bits to read (must be < 8)
/// * `bit_offset` - Starting bit offset within the first byte (must be < 8)
///
/// # Returns
///
/// A `u8` containing the requested bits in the least significant positions
///
/// # Panics
/// - Panics if `number_of_bits_to_read` is 0 or >= 8
/// - Panics if `bit_offset` is >= 8
/// - Panics if `slice` is empty or too small to read the requested bits
///
#[inline]
pub(crate) fn read_up_to_byte_from_offset(
    slice: &[u8],
    number_of_bits_to_read: usize,
    bit_offset: usize,
) -> u8 {
    assert!(number_of_bits_to_read < 8, "can read up to 8 bits only");
    assert!(bit_offset < 8, "bit offset must be less than 8");
    assert_ne!(
        number_of_bits_to_read, 0,
        "number of bits to read must be greater than 0"
    );
    assert_ne!(slice.len(), 0, "slice must not be empty");

    let number_of_bytes_to_read = ceil(number_of_bits_to_read + bit_offset, 8);

    // number of bytes to read
    assert!(slice.len() >= number_of_bytes_to_read, "slice is too small");

    let mut bits = slice[0] >> bit_offset;
    for (i, &byte) in slice
        .iter()
        .take(number_of_bytes_to_read)
        .enumerate()
        .skip(1)
    {
        bits |= byte << (i * 8 - bit_offset);
    }

    bits & ((1 << number_of_bits_to_read) - 1)
}

/// Applies a bitwise operation relative to another bit-packed byte slice
/// (right) in place
///
/// Note: applies the operation 64-bits (u64) at a time.
///
/// # Arguments
///
/// * `left` - The mutable buffer to be modified in-place
/// * `offset_in_bits` - Starting bit offset in Self buffer
/// * `right` - slice of bit-packed bytes in LSB order
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
/// * `op` - Binary operation to apply (e.g., `|a, b| a & b`). Applied a word at a time
///
/// Only the bits in `left_offset_in_bits..left_offset_in_bits + len_in_bits` are
/// modified. Bits of `left` outside that range are left unchanged, including the
/// bits sharing a byte with either end of the range.
///
/// # Example: Modify entire buffer
/// ```
/// # use arrow_buffer::MutableBuffer;
/// # use arrow_buffer::bit_util::apply_bitwise_binary_op;
/// let mut left = MutableBuffer::new(2);
/// left.extend_from_slice(&[0b11110000u8, 0b00110011u8]);
/// let right = &[0b10101010u8, 0b10101010u8];
/// // apply bitwise AND between left and right buffers, updating left in place
/// apply_bitwise_binary_op(left.as_slice_mut(), 0, right, 0, 16, |a, b| a & b);
/// assert_eq!(left.as_slice(), &[0b10100000u8, 0b00100010u8]);
/// ```
///
/// # Example: Modify buffer with offsets
/// ```
/// # use arrow_buffer::MutableBuffer;
/// # use arrow_buffer::bit_util::apply_bitwise_binary_op;
/// let mut left = MutableBuffer::new(2);
/// left.extend_from_slice(&[0b00000000u8, 0b00000000u8]);
/// let right = &[0b10110011u8, 0b11111110u8];
/// // apply bitwise OR between left and right buffers,
/// // Apply only 8 bits starting from bit offset 3 in left and bit offset 2 in right
/// apply_bitwise_binary_op(left.as_slice_mut(), 3, right, 2, 8, |a, b| a | b);
/// assert_eq!(left.as_slice(), &[0b01100000, 0b00000101u8]);
/// ```
///
/// # Panics
///
/// If the offset or lengths exceed the buffer or slice size.
pub fn apply_bitwise_binary_op<F>(
    left: &mut [u8],
    left_offset_in_bits: usize,
    right: impl AsRef<[u8]>,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64, u64) -> u64,
{
    if len_in_bits == 0 {
        return;
    }

    // offset inside a byte
    let bit_offset = left_offset_in_bits % 8;

    let is_mutable_buffer_byte_aligned = bit_offset == 0;

    if is_mutable_buffer_byte_aligned {
        byte_aligned_bitwise_bin_op_helper(
            left,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            op,
        );
    } else {
        // If we are not byte aligned, run `op` on the first few bits to reach byte alignment
        let bits_to_next_byte = (8 - bit_offset)
            // Minimum with the amount of bits we need to process
            // to avoid reading out of bounds
            .min(len_in_bits);

        {
            let right_byte_offset = right_offset_in_bits / 8;

            // Read the same amount of bits from the right buffer
            let right_first_byte = crate::util::bit_util::read_up_to_byte_from_offset(
                &right.as_ref()[right_byte_offset..],
                bits_to_next_byte,
                // Right bit offset
                right_offset_in_bits % 8,
            );

            align_to_byte(
                left,
                // Hope it gets inlined
                &mut |left| op(left, right_first_byte as u64),
                left_offset_in_bits,
                bits_to_next_byte,
            );
        }

        let offset_in_bits = left_offset_in_bits + bits_to_next_byte;
        let right_offset_in_bits = right_offset_in_bits + bits_to_next_byte;
        let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

        if len_in_bits == 0 {
            return;
        }

        // We are now byte aligned
        byte_aligned_bitwise_bin_op_helper(
            left,
            offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            op,
        );
    }
}

/// Apply a bitwise operation to a mutable buffer, updating it in place.
///
/// Note: applies the operation 64-bits (u64) at a time.
///
/// # Arguments
///
/// * `offset_in_bits` - Starting bit offset for the current buffer
/// * `len_in_bits` - Number of bits to process
/// * `op` - Unary operation to apply (e.g., `|a| !a`). Applied a word at a time
///
/// Only the bits in `offset_in_bits..offset_in_bits + len_in_bits` are modified.
/// Bits outside that range are left unchanged, including the bits sharing a byte
/// with either end of the range.
///
/// # Example: Modify entire buffer
/// ```
/// # use arrow_buffer::MutableBuffer;
/// # use arrow_buffer::bit_util::apply_bitwise_unary_op;
/// let mut buffer = MutableBuffer::new(2);
/// buffer.extend_from_slice(&[0b11110000u8, 0b00110011u8]);
/// // apply bitwise NOT to the buffer in place
/// apply_bitwise_unary_op(buffer.as_slice_mut(), 0, 16, |a| !a);
/// assert_eq!(buffer.as_slice(), &[0b00001111u8, 0b11001100u8]);
/// ```
///
/// # Example: Modify buffer with offsets
/// ```
/// # use arrow_buffer::MutableBuffer;
/// # use arrow_buffer::bit_util::apply_bitwise_unary_op;
/// let mut buffer = MutableBuffer::new(2);
/// buffer.extend_from_slice(&[0b00000000u8, 0b00000000u8]);
/// // apply bitwise NOT to 8 bits starting from bit offset 3
/// apply_bitwise_unary_op(buffer.as_slice_mut(), 3, 8, |a| !a);
/// assert_eq!(buffer.as_slice(), &[0b11111000u8, 0b00000111u8]);
/// ```
///
/// # Panics
///
/// If the offset and length exceed the buffer size.
pub fn apply_bitwise_unary_op<F>(
    buffer: &mut [u8],
    offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64) -> u64,
{
    if len_in_bits == 0 {
        return;
    }

    // offset inside a byte
    let left_bit_offset = offset_in_bits % 8;

    let is_mutable_buffer_byte_aligned = left_bit_offset == 0;

    if is_mutable_buffer_byte_aligned {
        byte_aligned_bitwise_unary_op_helper(buffer, offset_in_bits, len_in_bits, op);
    } else {
        align_to_byte(buffer, &mut op, offset_in_bits, len_in_bits);

        // If we are not byte aligned we will read the first few bits
        let bits_to_next_byte = 8 - left_bit_offset;

        let offset_in_bits = offset_in_bits + bits_to_next_byte;
        let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

        if len_in_bits == 0 {
            return;
        }

        // We are now byte aligned
        byte_aligned_bitwise_unary_op_helper(buffer, offset_in_bits, len_in_bits, op);
    }
}

/// Perform bitwise binary operation on byte-aligned buffers (i.e. not offsetting into a middle of a byte).
///
/// This is the optimized path for byte-aligned operations. It processes data in
/// u64 chunks for maximum efficiency, then handles any remainder bits.
///
/// # Arguments
///
/// * `left` - The left mutable buffer (must be byte-aligned)
/// * `left_offset_in_bits` - Starting bit offset in the left buffer (must be multiple of 8)
/// * `right` - The right buffer as byte slice
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
/// * `op` - Binary operation to apply
#[inline]
fn byte_aligned_bitwise_bin_op_helper<F>(
    left: &mut [u8],
    left_offset_in_bits: usize,
    right: impl AsRef<[u8]>,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64, u64) -> u64,
{
    // Must not reach here if we not byte aligned
    assert_eq!(
        left_offset_in_bits % 8,
        0,
        "offset_in_bits must be byte aligned"
    );

    // 1. Prepare the buffers
    let (complete_u64_chunks, remainder_bytes) =
        U64UnalignedSlice::split(left, left_offset_in_bits, len_in_bits);

    let right_chunks = BitChunks::new(right.as_ref(), right_offset_in_bits, len_in_bits);
    assert_eq!(
        self::ceil(right_chunks.remainder_len(), 8),
        remainder_bytes.len()
    );

    let right_chunks_iter = right_chunks.iter();
    assert_eq!(right_chunks_iter.len(), complete_u64_chunks.len());

    // 2. Process complete u64 chunks
    complete_u64_chunks.zip_modify(right_chunks_iter, &mut op);

    // Handle remainder bits if any
    if right_chunks.remainder_len() > 0 {
        handle_mutable_buffer_remainder(
            &mut op,
            remainder_bytes,
            right_chunks.remainder_bits(),
            right_chunks.remainder_len(),
        )
    }
}

/// Perform bitwise unary operation on byte-aligned buffer.
///
/// This is the optimized path for byte-aligned unary operations. It processes data in
/// u64 chunks for maximum efficiency, then handles any remainder bits.
///
/// # Arguments
///
/// * `buffer` - The mutable buffer (must be byte-aligned)
/// * `offset_in_bits` - Starting bit offset (must be multiple of 8)
/// * `len_in_bits` - Number of bits to process
/// * `op` - Unary operation to apply (e.g., `|a| !a`)
#[inline]
fn byte_aligned_bitwise_unary_op_helper<F>(
    buffer: &mut [u8],
    offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64) -> u64,
{
    // Must not reach here if we not byte aligned
    assert_eq!(offset_in_bits % 8, 0, "offset_in_bits must be byte aligned");

    let remainder_len = len_in_bits % 64;

    let (complete_u64_chunks, remainder_bytes) =
        U64UnalignedSlice::split(buffer, offset_in_bits, len_in_bits);

    assert_eq!(self::ceil(remainder_len, 8), remainder_bytes.len());

    // 2. Process complete u64 chunks
    complete_u64_chunks.apply_unary_op(&mut op);

    // Handle remainder bits if any
    if remainder_len > 0 {
        handle_mutable_buffer_remainder_unary(&mut op, remainder_bytes, remainder_len)
    }
}

/// Align to byte boundary by applying operation to bits before the next byte boundary.
///
/// This function handles non-byte-aligned operations by processing bits from the current
/// position up to the next byte boundary, while preserving all other bits in the byte.
///
/// # Arguments
///
/// * `op` - Unary operation to apply
/// * `buffer` - The mutable buffer to modify
/// * `offset_in_bits` - Starting bit offset (not byte-aligned)
/// * `remaining_len_in_bits` - Number of bits still to process starting at `offset_in_bits`.
///   When this is smaller than the number of bits left in the byte, the trailing bits of
///   the byte are left untouched.
fn align_to_byte<F>(
    buffer: &mut [u8],
    op: &mut F,
    offset_in_bits: usize,
    remaining_len_in_bits: usize,
) where
    F: FnMut(u64) -> u64,
{
    let byte_offset = offset_in_bits / 8;
    let bit_offset = offset_in_bits % 8;

    // Byte aligned offsets must take the byte aligned path instead
    debug_assert_ne!(bit_offset, 0, "offset_in_bits must not be byte aligned");

    // 1. read the first byte from the buffer
    let first_byte: u8 = buffer[byte_offset];

    // 2. Shift byte by the bit offset, keeping only the relevant bits
    let relevant_first_byte = first_byte >> bit_offset;

    // 3. run the op on the first byte only
    let result_first_byte = op(relevant_first_byte as u64) as u8;

    // 4. Shift back the result to the original position
    let result_first_byte = result_first_byte << bit_offset;

    // 5. Mask in only the bits the caller asked to process, i.e. the bits in
    //    `bit_offset..bit_offset + bits_in_this_byte`. The request may end before the
    //    byte boundary, in which case the trailing bits must be preserved as well.
    //
    //    `bit_offset` is in `1..=7` per the assert above, so `bits_in_this_byte` is at
    //    most 7 and `bits_in_this_byte + bit_offset <= 8`, keeping the mask within a `u8`.
    let bits_in_this_byte = (8 - bit_offset).min(remaining_len_in_bits);
    let write_mask = ((1u8 << bits_in_this_byte) - 1) << bit_offset;

    let result_first_byte = (first_byte & !write_mask) | (result_first_byte & write_mask);

    // 6. write back the result to the buffer
    buffer[byte_offset] = result_first_byte;
}

/// Centralized structure to handle a mutable u8 slice as a mutable u64 pointer.
///
/// Handle the following:
/// 1. the lifetime is correct
/// 2. we read/write within the bounds
/// 3. We read and write using unaligned
///
/// This does not deallocate the underlying pointer when dropped
///
/// This is the only place that uses unsafe code to read and write unaligned
///
struct U64UnalignedSlice<'a> {
    /// Pointer to the start of the u64 data
    ///
    /// We are using raw pointer as the data came from a u8 slice so we need to read and write unaligned
    ptr: *mut u64,

    /// Number of u64 elements
    len: usize,

    /// Marker to tie the lifetime of the pointer to the lifetime of the u8 slice
    _marker: std::marker::PhantomData<&'a u8>,
}

impl<'a> U64UnalignedSlice<'a> {
    /// Create a new [`U64UnalignedSlice`] from a `&mut [u8]` buffer
    ///
    /// return the [`U64UnalignedSlice`] and slice of bytes that are not part of the u64 chunks (guaranteed to be less than 8 bytes)
    ///
    fn split(
        buffer: &'a mut [u8],
        offset_in_bits: usize,
        len_in_bits: usize,
    ) -> (Self, &'a mut [u8]) {
        // 1. Prepare the buffers
        let left_buffer_mut: &mut [u8] = {
            let last_offset = self::ceil(offset_in_bits + len_in_bits, 8);
            assert!(last_offset <= buffer.len());

            let byte_offset = offset_in_bits / 8;

            &mut buffer[byte_offset..last_offset]
        };

        let number_of_u64_we_can_fit = len_in_bits / (u64::BITS as usize);

        // 2. Split
        let u64_len_in_bytes = number_of_u64_we_can_fit * size_of::<u64>();

        assert!(u64_len_in_bytes <= left_buffer_mut.len());
        let (bytes_for_u64, remainder) = left_buffer_mut.split_at_mut(u64_len_in_bytes);

        #[expect(
            clippy::cast_ptr_alignment,
            reason = "`U64UnalignedSlice` only reads and writes through the unaligned methods"
        )]
        let ptr = bytes_for_u64.as_mut_ptr().cast::<u64>();

        let this = Self {
            ptr,
            len: number_of_u64_we_can_fit,
            _marker: std::marker::PhantomData,
        };

        (this, remainder)
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Modify the underlying u64 data in place using a binary operation
    /// with another iterator.
    fn zip_modify(
        mut self,
        mut zip_iter: impl ExactSizeIterator<Item = u64>,
        mut map: impl FnMut(u64, u64) -> u64,
    ) {
        assert_eq!(self.len, zip_iter.len());

        // In order to avoid advancing the pointer at the end of the loop which will
        // make the last pointer invalid, we handle the first element outside the loop
        // and then advance the pointer at the start of the loop
        // making sure that the iterator is not empty
        if let Some(right) = zip_iter.next() {
            // SAFETY: We asserted that the iterator length and the current length are the same
            // and the iterator is not empty, so the pointer is valid
            unsafe {
                self.apply_bin_op(right, &mut map);
            }

            // Because this consumes self we don't update the length
        }

        for right in zip_iter {
            // Advance the pointer
            //
            // SAFETY: We asserted that the iterator length and the current length are the same
            self.ptr = unsafe { self.ptr.add(1) };

            // SAFETY: the pointer is valid as we are within the length
            unsafe {
                self.apply_bin_op(right, &mut map);
            }

            // Because this consumes self we don't update the length
        }
    }

    /// Centralized function to correctly read the current u64 value and write back the result
    ///
    /// # SAFETY
    /// the caller must ensure that the pointer is valid for reads and writes
    ///
    #[inline]
    unsafe fn apply_bin_op(&mut self, right: u64, mut map: impl FnMut(u64, u64) -> u64) {
        // SAFETY: The constructor ensures the pointer is valid,
        // and as to all modifications in U64UnalignedSlice
        let current_input = unsafe {
            self.ptr
                // Reading unaligned as we came from u8 slice
                .read_unaligned()
                // bit-packed buffers are stored starting with the least-significant byte first
                // so when reading as u64 on a big-endian machine, the bytes need to be swapped
                .to_le()
        };

        let combined = map(current_input, right);

        // Write the result back
        //
        // The pointer came from mutable u8 slice so the pointer is valid for writes,
        // and we need to write unaligned
        unsafe { self.ptr.write_unaligned(combined) }
    }

    /// Modify the underlying u64 data in place using a unary operation.
    fn apply_unary_op(mut self, mut map: impl FnMut(u64) -> u64) {
        if self.len == 0 {
            return;
        }

        // In order to avoid advancing the pointer at the end of the loop which will
        // make the last pointer invalid, we handle the first element outside the loop
        // and then advance the pointer at the start of the loop
        // making sure that the iterator is not empty
        // Safety: `self.len > 0` (checked above) and the pointer has not been advanced yet,
        // so it is valid for reads and writes.
        unsafe {
            // I hope the function get inlined and the compiler remove the dead right parameter
            self.apply_bin_op(0, &mut |left, _| map(left));

            // Because this consumes self we don't update the length
        }

        for _ in 1..self.len {
            // Advance the pointer
            //
            // SAFETY: we only advance the pointer within the length and not beyond
            self.ptr = unsafe { self.ptr.add(1) };

            // SAFETY: the pointer is valid as we are within the length
            unsafe {
                // I hope the function get inlined and the compiler remove the dead right parameter
                self.apply_bin_op(0, &mut |left, _| map(left));
            }

            // Because this consumes self we don't update the length
        }
    }
}

/// Handle remainder bits (< 64 bits) for binary operations.
///
/// This function processes the bits that don't form a complete u64 chunk,
/// ensuring that bits outside the operation range are preserved.
///
/// # Arguments
///
/// * `op` - Binary operation to apply
/// * `start_remainder_mut_slice` - slice to the start of remainder bytes
///   the length must be equal to `ceil(remainder_len, 8)`
/// * `right_remainder_bits` - Right operand bits
/// * `remainder_len` - Number of remainder bits
#[inline]
fn handle_mutable_buffer_remainder<F>(
    op: &mut F,
    start_remainder_mut_slice: &mut [u8],
    right_remainder_bits: u64,
    remainder_len: usize,
) where
    F: FnMut(u64, u64) -> u64,
{
    // Only read from slice the number of remainder bits
    let left_remainder_bits = get_remainder_bits(start_remainder_mut_slice, remainder_len);

    // Apply the operation
    let rem = op(left_remainder_bits, right_remainder_bits);

    // Write only the relevant bits back the result to the mutable slice
    set_remainder_bits(start_remainder_mut_slice, rem, remainder_len);
}

/// Write remainder bits back to buffer while preserving bits outside the range.
///
/// This function carefully updates only the specified bits, leaving all other
/// bits in the affected bytes unchanged.
///
/// # Arguments
///
/// * `start_remainder_mut_slice` - the slice of bytes to write the remainder bits to,
///   the length must be equal to `ceil(remainder_len, 8)`
/// * `rem` - The result bits to write
/// * `remainder_len` - Number of bits to write
#[inline]
fn set_remainder_bits(start_remainder_mut_slice: &mut [u8], rem: u64, remainder_len: usize) {
    assert_ne!(
        start_remainder_mut_slice.len(),
        0,
        "start_remainder_mut_slice must not be empty"
    );
    assert!(remainder_len < 64, "remainder_len must be less than 64");

    // This assertion is to make sure that the last byte in the slice is the boundary byte
    // (i.e., the byte that contains both remainder bits and bits outside the remainder)
    assert_eq!(
        start_remainder_mut_slice.len(),
        self::ceil(remainder_len, 8),
        "start_remainder_mut_slice length must be equal to ceil(remainder_len, 8)"
    );

    // Need to update the remainder bytes in the mutable buffer
    // but not override the bits outside the remainder

    // Update `rem` end with the current bytes in the mutable buffer
    // to preserve the bits outside the remainder
    let rem = {
        // 1. Read the byte that we will override
        //    we only read the last byte as we verified that start_remainder_mut_slice length is
        //    equal to ceil(remainder_len, 8), which means the last byte is the boundary byte
        //    containing both remainder bits and bits outside the remainder
        let current = start_remainder_mut_slice
            .last()
            // Unwrap as we already validated the slice is not empty
            .unwrap();

        // Shift the boundary byte to the position it occupies within `rem`, otherwise
        // its bits would be compared against the wrong end of the mask below
        let current = (*current as u64) << ((start_remainder_mut_slice.len() - 1) * 8);

        // Mask where the bits that are inside the remainder are 1
        // and the bits outside the remainder are 0
        let inside_remainder_mask = (1 << remainder_len) - 1;
        // Mask where the bits that are outside the remainder are 1
        // and the bits inside the remainder are 0
        let outside_remainder_mask = !inside_remainder_mask;

        // 2. Only keep the bits that are outside the remainder for the value from the mutable buffer
        let current = current & outside_remainder_mask;

        // 3. Only keep the bits that are inside the remainder for the value from the operation
        let rem = rem & inside_remainder_mask;

        // 4. Combine the two values
        current | rem
    };

    // Write back the result to the mutable slice
    {
        let remainder_bytes = start_remainder_mut_slice.len();

        // we are counting starting from the least significant bit, so to_le_bytes should be correct
        let rem = &rem.to_le_bytes()[0..remainder_bytes];

        // this assumes that `[ToByteSlice]` can be copied directly
        // without calling `to_byte_slice` for each element,
        // which is correct for all ArrowNativeType implementations including u64.
        let src = rem.as_ptr();
        // Safety: `rem` has length `remainder_bytes`, `start_remainder_mut_slice` has length
        // `remainder_bytes`, and the two slices are non-overlapping (rem is derived from a
        // local `to_le_bytes()` call; start_remainder_mut_slice is the caller's mutable buffer).
        unsafe {
            std::ptr::copy_nonoverlapping(
                src,
                start_remainder_mut_slice.as_mut_ptr(),
                remainder_bytes,
            )
        };
    }
}

/// Read remainder bits from a slice.
///
/// Reads the specified number of bits from slice and returns them as a u64.
///
/// # Arguments
///
/// * `remainder` - slice to the start of the bits
/// * `remainder_len` - Number of bits to read (must be < 64)
///
/// # Returns
///
/// A u64 containing the bits in the least significant positions
#[inline]
fn get_remainder_bits(remainder: &[u8], remainder_len: usize) -> u64 {
    assert!(remainder.len() < 64, "remainder_len must be less than 64");
    assert_eq!(
        remainder.len(),
        self::ceil(remainder_len, 8),
        "remainder and remainder len ceil must be the same"
    );

    let bits = remainder
        .iter()
        .enumerate()
        .fold(0_u64, |acc, (index, &byte)| {
            acc | ((byte as u64) << (index * 8))
        });

    bits & ((1 << remainder_len) - 1)
}

/// Handle remainder bits (< 64 bits) for unary operations.
///
/// This function processes the bits that don't form a complete u64 chunk,
/// ensuring that bits outside the operation range are preserved.
///
/// # Arguments
///
/// * `op` - Unary operation to apply
/// * `start_remainder_mut` - Slice of bytes to write the remainder bits to
/// * `remainder_len` - Number of remainder bits
#[inline]
fn handle_mutable_buffer_remainder_unary<F>(
    op: &mut F,
    start_remainder_mut: &mut [u8],
    remainder_len: usize,
) where
    F: FnMut(u64) -> u64,
{
    // Only read from the slice the number of remainder bits
    let left_remainder_bits = get_remainder_bits(start_remainder_mut, remainder_len);

    // Apply the operation
    let rem = op(left_remainder_bits);

    // Write only the relevant bits back the result to the slice
    set_remainder_bits(start_remainder_mut, rem, remainder_len);
}

/// Copies bits `src` of `data` to position `dest`
///
/// # Arguments
/// * `data` - The buffer to copy bits within
/// * `buffer_len_in_bits` - The length of the buffer in bits. must be `<= data.len() * 8`.
/// * `src` - The source range of bits to copy
/// * `dest` - The destination bit index to copy to
///
/// # Example
/// ```
/// # use arrow_buffer::MutableBuffer;
/// # use arrow_buffer::bit_util::copy_bits_within;
/// let mut buffer = MutableBuffer::new(3);
/// buffer.extend_from_slice(&[0b0101_1001_u8, 0b1100_1000_u8, 0b0010_0110_u8]);
/// let buffer_len_in_bits = buffer.len() * 8;
/// // Copy bits 14..22 to position 2
/// copy_bits_within(buffer.as_slice_mut(), buffer_len_in_bits, 14..22, 2);
/// assert_eq!(buffer.as_slice(), &[0b0100_0010_u8, 0b0100_1000_u8, 0b0010_0110_u8]);
/// ```
///
/// # Panics
///
/// If the source range or the destination range are out of bounds of `buffer_len_in_bits`
pub fn copy_bits_within<R: RangeBounds<usize>>(
    data: &mut [u8],
    buffer_len_in_bits: usize,
    src: R,
    dest: usize,
) {
    let start_bound = match src.start_bound() {
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n + 1,
        Bound::Unbounded => 0,
    };
    let end_bound = match src.end_bound() {
        Bound::Included(n) => n + 1,
        Bound::Excluded(n) => *n,
        Bound::Unbounded => buffer_len_in_bits,
    };
    assert!(
        buffer_len_in_bits <= data.len() * 8,
        "buffer length {buffer_len_in_bits} exceeds data of {} bytes",
        data.len()
    );
    assert!(
        start_bound <= end_bound,
        "start bound {start_bound} > end bound {end_bound}"
    );
    assert!(
        end_bound <= buffer_len_in_bits,
        "end bound {end_bound} is out of bounds 0..{buffer_len_in_bits}"
    );
    let len = end_bound - start_bound;
    assert!(
        dest <= buffer_len_in_bits - len,
        "dest {dest} is out of bounds for range of length {len}"
    );

    if len == 0 || dest == start_bound {
        return;
    }

    // If we can copy the bits directly and avoid shifting each byte
    if start_bound % 8 == dest % 8 {
        copy_within_same_phase(data, start_bound, dest, len);
    } else {
        fallback_copy_within(data, start_bound, dest, len);
    }
}

/// Copies `len` bits between two non overlapping slices, preserving the bits of `dst` outside the range
#[inline]
fn copy_bits(dst: &mut [u8], dst_offset: usize, src: &[u8], src_offset: usize, len: usize) {
    apply_bitwise_binary_op(dst, dst_offset, src, src_offset, len, |_, src| src);
}

/// Source and destination share the same bit phase, so the whole bytes in between are memmoved
fn copy_within_same_phase(data: &mut [u8], src: usize, dest: usize, len: usize) {
    let head = ((8 - src % 8) % 8).min(len);
    let middle_bytes = (len - head) / 8;
    let tail = (len - head) % 8;
    let tail_offset = head + middle_bytes * 8;

    // Save the partial edges first so the memmove cannot clobber them
    let mut head_bits = [0u8; 1];
    let mut tail_bits = [0u8; 1];
    copy_bits(&mut head_bits, 0, data, src, head);
    copy_bits(&mut tail_bits, 0, data, src + tail_offset, tail);

    let src_byte = (src + head) / 8;
    let dest_byte = (dest + head) / 8;
    data.copy_within(src_byte..src_byte + middle_bytes, dest_byte);

    copy_bits(data, dest, &head_bits, 0, head);
    copy_bits(data, dest + tail_offset, &tail_bits, 0, tail);
}

/// Fallback copy within when we have to do some shifting of the bits
fn fallback_copy_within(data: &mut [u8], src: usize, dest: usize, len: usize) {
    const BUFFER_SIZE_BYTES: usize = 256;
    const BUFFER_SIZE_BITS: usize = BUFFER_SIZE_BYTES * 8;

    let mut buffer = [0u8; BUFFER_SIZE_BYTES];
    let mut copy_block = |data: &mut [u8], done: usize| {
        let n = (len - done).min(BUFFER_SIZE_BITS);
        // Copy to temp buffer
        copy_bits(&mut buffer, 0, data, src + done, n);
        // Copy back to destination
        copy_bits(data, dest + done, &buffer, 0, n);
    };

    if dest < src {
        for done in (0..len).step_by(BUFFER_SIZE_BITS) {
            copy_block(data, done);
        }
    } else {
        for done in (0..len).step_by(BUFFER_SIZE_BITS).rev() {
            copy_block(data, done);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::bit_iterator::BitIterator;
    use crate::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    #[test]
    fn test_round_upto_multiple_of_64() {
        assert_eq!(0, round_upto_multiple_of_64(0));
        assert_eq!(64, round_upto_multiple_of_64(1));
        assert_eq!(64, round_upto_multiple_of_64(63));
        assert_eq!(64, round_upto_multiple_of_64(64));
        assert_eq!(128, round_upto_multiple_of_64(65));
        assert_eq!(192, round_upto_multiple_of_64(129));
    }

    #[test]
    #[should_panic(expected = "failed to round upto multiple of 64")]
    fn test_round_upto_multiple_of_64_panic() {
        let _ = round_upto_multiple_of_64(usize::MAX);
    }

    #[test]
    #[should_panic(expected = "failed to round to next highest power of 2")]
    fn test_round_upto_panic() {
        let _ = round_upto_power_of_2(usize::MAX, 2);
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert!(get_bit(&[0b00001101], 0));
        assert!(!get_bit(&[0b00001101], 1));
        assert!(get_bit(&[0b00001101], 2));
        assert!(get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert!(get_bit(&[0b01001001, 0b01010010], 0));
        assert!(!get_bit(&[0b01001001, 0b01010010], 1));
        assert!(!get_bit(&[0b01001001, 0b01010010], 2));
        assert!(get_bit(&[0b01001001, 0b01010010], 3));
        assert!(!get_bit(&[0b01001001, 0b01010010], 4));
        assert!(!get_bit(&[0b01001001, 0b01010010], 5));
        assert!(get_bit(&[0b01001001, 0b01010010], 6));
        assert!(!get_bit(&[0b01001001, 0b01010010], 7));
        assert!(!get_bit(&[0b01001001, 0b01010010], 8));
        assert!(get_bit(&[0b01001001, 0b01010010], 9));
        assert!(!get_bit(&[0b01001001, 0b01010010], 10));
        assert!(!get_bit(&[0b01001001, 0b01010010], 11));
        assert!(get_bit(&[0b01001001, 0b01010010], 12));
        assert!(!get_bit(&[0b01001001, 0b01010010], 13));
        assert!(get_bit(&[0b01001001, 0b01010010], 14));
        assert!(!get_bit(&[0b01001001, 0b01010010], 15));
    }

    pub fn seedable_rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn test_get_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = [0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if b {
                set_bit(&mut buf[..], i)
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_set_bit() {
        let mut b = [0b00000010];
        set_bit(&mut b, 0);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 1);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 7);
        assert_eq!([0b10000011], b);
    }

    #[test]
    fn test_unset_bit() {
        let mut b = [0b11111101];
        unset_bit(&mut b, 0);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 1);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 7);
        assert_eq!([0b01111100], b);
    }

    #[test]
    fn test_set_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if b {
                unsafe {
                    set_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_unset_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![255; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.random_bool(0.5);
            expected.push(b);
            if !b {
                unsafe {
                    unset_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_get_set_bit_roundtrip() {
        const NUM_BYTES: usize = 10;
        const NUM_SETS: usize = 10;

        let mut buffer: [u8; NUM_BYTES * 8] = [0; NUM_BYTES * 8];
        let mut v = HashSet::new();
        let mut rng = seedable_rng();
        for _ in 0..NUM_SETS {
            let offset = rng.random_range(0..8 * NUM_BYTES);
            v.insert(offset);
            set_bit(&mut buffer[..], offset);
        }
        for i in 0..NUM_BYTES * 8 {
            assert_eq!(v.contains(&i), get_bit(&buffer[..], i));
        }
    }

    #[test]
    fn test_ceil() {
        assert_eq!(ceil(0, 1), 0);
        assert_eq!(ceil(1, 1), 1);
        assert_eq!(ceil(1, 2), 1);
        assert_eq!(ceil(1, 8), 1);
        assert_eq!(ceil(7, 8), 1);
        assert_eq!(ceil(8, 8), 1);
        assert_eq!(ceil(9, 8), 2);
        assert_eq!(ceil(9, 9), 1);
        assert_eq!(ceil(10000000000, 10), 1000000000);
        assert_eq!(ceil(10, 10000000000), 1);
        assert_eq!(ceil(10000000000, 1000000000), 10);
    }

    #[test]
    fn test_read_up_to() {
        let all_ones = &[0b10111001, 0b10001100];

        for (bit_offset, expected) in [
            (0, 0b00000001),
            (1, 0b00000000),
            (2, 0b00000000),
            (3, 0b00000001),
            (4, 0b00000001),
            (5, 0b00000001),
            (6, 0b00000000),
            (7, 0b00000001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 1, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00000001),
            (1, 0b00000000),
            (2, 0b00000010),
            (3, 0b00000011),
            (4, 0b00000011),
            (5, 0b00000001),
            (6, 0b00000010),
            (7, 0b00000001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 2, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00111001),
            (1, 0b00011100),
            (2, 0b00101110),
            (3, 0b00010111),
            (4, 0b00001011),
            (5, 0b00100101),
            (6, 0b00110010),
            (7, 0b00011001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 6, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }

        for (bit_offset, expected) in [
            (0, 0b00111001),
            (1, 0b01011100),
            (2, 0b00101110),
            (3, 0b00010111),
            (4, 0b01001011),
            (5, 0b01100101),
            (6, 0b00110010),
            (7, 0b00011001),
        ] {
            let result = read_up_to_byte_from_offset(all_ones, 7, bit_offset);
            assert_eq!(
                result, expected,
                "failed at bit_offset {bit_offset}. result, expected:\n{result:08b}\n{expected:08b}"
            );
        }
    }

    /// Verifies that a unary operation applied to a buffer using u64 chunks
    /// is the same as applying the operation bit by bit.
    fn test_mutable_buffer_bin_op_helper<F, G>(
        left_data: &[bool],
        right_data: &[bool],
        left_offset_in_bits: usize,
        right_offset_in_bits: usize,
        len_in_bits: usize,
        op: F,
        mut expected_op: G,
    ) where
        F: FnMut(u64, u64) -> u64,
        G: FnMut(bool, bool) -> bool,
    {
        let mut left_buffer = BooleanBufferBuilder::new(len_in_bits);
        left_buffer.append_slice(left_data);
        let right_buffer = BooleanBuffer::from(right_data);

        let expected: Vec<bool> = left_data
            .iter()
            .skip(left_offset_in_bits)
            .zip(right_data.iter().skip(right_offset_in_bits))
            .take(len_in_bits)
            .map(|(l, r)| expected_op(*l, *r))
            .collect();

        let before = left_buffer.as_slice().to_vec();

        apply_bitwise_binary_op(
            left_buffer.as_slice_mut(),
            left_offset_in_bits,
            right_buffer.inner(),
            right_offset_in_bits,
            len_in_bits,
            op,
        );

        let result: Vec<bool> =
            BitIterator::new(left_buffer.as_slice(), left_offset_in_bits, len_in_bits).collect();

        assert_eq!(
            result, expected,
            "Failed with left_offset={left_offset_in_bits}, right_offset={right_offset_in_bits}, len={len_in_bits}"
        );

        assert_bits_outside_range_preserved(
            &before,
            left_buffer.as_slice(),
            left_offset_in_bits,
            len_in_bits,
            &format!(
                "left_offset={left_offset_in_bits}, right_offset={right_offset_in_bits}, len={len_in_bits}"
            ),
        );
    }

    /// Asserts that every bit outside `offset_in_bits..offset_in_bits + len_in_bits`
    /// is identical in `before` and `after`.
    fn assert_bits_outside_range_preserved(
        before: &[u8],
        after: &[u8],
        offset_in_bits: usize,
        len_in_bits: usize,
        context: &str,
    ) {
        assert_eq!(before.len(), after.len());
        for i in 0..before.len() * 8 {
            if i >= offset_in_bits && i < offset_in_bits + len_in_bits {
                continue;
            }
            assert_eq!(
                get_bit(before, i),
                get_bit(after, i),
                "bit {i} outside the requested range was modified ({context})"
            );
        }
    }

    /// Verifies that a unary operation applied to a buffer using u64 chunks
    /// is the same as applying the operation bit by bit.
    fn test_mutable_buffer_unary_op_helper<F, G>(
        data: &[bool],
        offset_in_bits: usize,
        len_in_bits: usize,
        op: F,
        mut expected_op: G,
    ) where
        F: FnMut(u64) -> u64,
        G: FnMut(bool) -> bool,
    {
        let mut buffer = BooleanBufferBuilder::new(len_in_bits);
        buffer.append_slice(data);

        let expected: Vec<bool> = data
            .iter()
            .skip(offset_in_bits)
            .take(len_in_bits)
            .map(|b| expected_op(*b))
            .collect();

        let before = buffer.as_slice().to_vec();

        apply_bitwise_unary_op(buffer.as_slice_mut(), offset_in_bits, len_in_bits, op);

        let result: Vec<bool> =
            BitIterator::new(buffer.as_slice(), offset_in_bits, len_in_bits).collect();

        assert_eq!(
            result, expected,
            "Failed with offset={offset_in_bits}, len={len_in_bits}"
        );

        assert_bits_outside_range_preserved(
            &before,
            buffer.as_slice(),
            offset_in_bits,
            len_in_bits,
            &format!("offset={offset_in_bits}, len={len_in_bits}"),
        );
    }

    // Helper to create test data of specific length
    fn create_test_data(len: usize) -> (Vec<bool>, Vec<bool>) {
        let mut rng = rand::rng();
        let left: Vec<bool> = (0..len).map(|_| rng.random_bool(0.5)).collect();
        let right: Vec<bool> = (0..len).map(|_| rng.random_bool(0.5)).collect();
        (left, right)
    }

    /// Test all binary operations (AND, OR, XOR) with the given parameters
    fn test_all_binary_ops(
        left_data: &[bool],
        right_data: &[bool],
        left_offset_in_bits: usize,
        right_offset_in_bits: usize,
        len_in_bits: usize,
    ) {
        // Test AND
        test_mutable_buffer_bin_op_helper(
            left_data,
            right_data,
            left_offset_in_bits,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a & b,
            |a, b| a & b,
        );

        // Test OR
        test_mutable_buffer_bin_op_helper(
            left_data,
            right_data,
            left_offset_in_bits,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a | b,
            |a, b| a | b,
        );

        // Test XOR
        test_mutable_buffer_bin_op_helper(
            left_data,
            right_data,
            left_offset_in_bits,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a ^ b,
            |a, b| a ^ b,
        );
    }

    // ===== Combined Binary Operation Tests =====

    #[test]
    fn test_binary_ops_less_than_byte() {
        let (left, right) = create_test_data(4);
        test_all_binary_ops(&left, &right, 0, 0, 4);
    }

    #[test]
    fn test_binary_ops_less_than_byte_across_boundary() {
        let (left, right) = create_test_data(16);
        test_all_binary_ops(&left, &right, 6, 6, 4);
    }

    #[test]
    fn test_binary_ops_exactly_byte() {
        let (left, right) = create_test_data(16);
        test_all_binary_ops(&left, &right, 0, 0, 8);
    }

    #[test]
    fn test_binary_ops_more_than_byte_less_than_u64() {
        let (left, right) = create_test_data(64);
        test_all_binary_ops(&left, &right, 0, 0, 32);
    }

    #[test]
    fn test_binary_ops_exactly_u64() {
        let (left, right) = create_test_data(180);
        test_all_binary_ops(&left, &right, 0, 0, 64);
        test_all_binary_ops(&left, &right, 64, 9, 64);
        test_all_binary_ops(&left, &right, 8, 100, 64);
        test_all_binary_ops(&left, &right, 1, 15, 64);
        test_all_binary_ops(&left, &right, 12, 10, 64);
        test_all_binary_ops(&left, &right, 180 - 64, 2, 64);
    }

    #[test]
    fn test_binary_ops_more_than_u64_not_multiple() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 0, 0, 100);
    }

    #[test]
    fn test_binary_ops_exactly_multiple_u64() {
        let (left, right) = create_test_data(256);
        test_all_binary_ops(&left, &right, 0, 0, 128);
    }

    #[test]
    fn test_binary_ops_more_than_multiple_u64() {
        let (left, right) = create_test_data(300);
        test_all_binary_ops(&left, &right, 0, 0, 200);
    }

    #[test]
    fn test_binary_ops_byte_aligned_no_remainder() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 0, 0, 128);
    }

    #[test]
    fn test_binary_ops_byte_aligned_with_remainder() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 0, 0, 100);
    }

    #[test]
    fn test_binary_ops_not_byte_aligned_no_remainder() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 3, 3, 128);
    }

    #[test]
    fn test_binary_ops_not_byte_aligned_with_remainder() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 5, 5, 100);
    }

    #[test]
    fn test_binary_ops_different_offsets() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 3, 7, 50);
    }

    #[test]
    fn test_binary_ops_offsets_greater_than_8_less_than_64() {
        let (left, right) = create_test_data(200);
        test_all_binary_ops(&left, &right, 13, 27, 100);
    }

    // ===== NOT (Unary) Operation Tests =====

    #[test]
    fn test_not_less_than_byte() {
        let data = vec![true, false, true, false];
        test_mutable_buffer_unary_op_helper(&data, 0, 4, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_less_than_byte_across_boundary() {
        let data: Vec<bool> = (0..16).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 6, 4, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_exactly_byte() {
        let data: Vec<bool> = (0..16).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 8, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_more_than_byte_less_than_u64() {
        let data: Vec<bool> = (0..64).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 32, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_exactly_u64() {
        let data: Vec<bool> = (0..128).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 64, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_more_than_u64_not_multiple() {
        let data: Vec<bool> = (0..200).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 100, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_exactly_multiple_u64() {
        let data: Vec<bool> = (0..256).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 128, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_more_than_multiple_u64() {
        let data: Vec<bool> = (0..300).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 200, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_byte_aligned_no_remainder() {
        let data: Vec<bool> = (0..200).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 128, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_byte_aligned_with_remainder() {
        let data: Vec<bool> = (0..200).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 0, 100, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_not_byte_aligned_no_remainder() {
        let data: Vec<bool> = (0..200).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 3, 128, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_not_byte_aligned_with_remainder() {
        let data: Vec<bool> = (0..200).map(|i| i % 2 == 0).collect();
        test_mutable_buffer_unary_op_helper(&data, 5, 100, |a| !a, |a| !a);
    }

    // ===== Edge Cases =====

    #[test]
    fn test_empty_length() {
        let (left, right) = create_test_data(16);
        test_all_binary_ops(&left, &right, 0, 0, 0);
    }

    #[test]
    fn test_single_bit() {
        let (left, right) = create_test_data(16);
        test_all_binary_ops(&left, &right, 0, 0, 1);
    }

    #[test]
    fn test_single_bit_at_offset() {
        let (left, right) = create_test_data(16);
        test_all_binary_ops(&left, &right, 7, 7, 1);
    }

    #[test]
    fn test_not_single_bit() {
        let data = vec![true, false, true, false];
        test_mutable_buffer_unary_op_helper(&data, 0, 1, |a| !a, |a| !a);
    }

    #[test]
    fn test_not_empty_length() {
        let data = vec![true, false, true, false];
        test_mutable_buffer_unary_op_helper(&data, 0, 0, |a| !a, |a| !a);
    }

    #[test]
    fn test_less_than_byte_unaligned_and_not_enough_bits() {
        let left_offset_in_bits = 2;
        let right_offset_in_bits = 4;
        let len_in_bits = 1;

        // Single byte
        let right = (0..8).map(|i| (i / 2) % 2 == 0).collect::<Vec<_>>();
        // less than a byte
        let left = (0..3).map(|i| i % 2 == 0).collect::<Vec<_>>();
        test_all_binary_ops(
            &left,
            &right,
            left_offset_in_bits,
            right_offset_in_bits,
            len_in_bits,
        );
    }

    /// Ranges that start and end inside the same non-byte-aligned byte must not
    /// touch the trailing bits of that byte.
    #[test]
    fn test_ops_ending_inside_the_first_partial_byte() {
        let (left, right) = create_test_data(32);
        for offset in 1..8 {
            // Inclusive so the range ending exactly on the byte boundary is covered too
            for len in 1..=(8 - offset) {
                test_all_binary_ops(&left, &right, offset, offset, len);
                test_all_binary_ops(&left, &right, offset, (offset + 3) % 8, len);
                test_mutable_buffer_unary_op_helper(&left, offset, len, |a| !a, |a| !a);
            }
        }
    }

    #[test]
    fn test_and_within_first_partial_byte_preserves_trailing_bits() {
        let mut left = vec![0b11111111u8, 0b11111111u8];
        let right = vec![0b00000000u8, 0b00000000u8];
        // AND a single bit at bit offset 1: only bit 1 may be cleared
        apply_bitwise_binary_op(&mut left, 1, &right, 0, 1, |a, b| a & b);
        assert_eq!(left, vec![0b11111101u8, 0b11111111u8]);
    }

    #[test]
    fn test_not_within_first_partial_byte_preserves_trailing_bits() {
        let mut buffer = vec![0b00000000u8];
        // NOT two bits at bit offset 3: only bits 3 and 4 may be flipped
        apply_bitwise_unary_op(&mut buffer, 3, 2, |a| !a);
        assert_eq!(buffer, vec![0b00011000u8]);
    }

    /// When the remainder spans more than one byte, the byte holding the end of the
    /// range is the *last* byte of the remainder, not the first. Its bits above the
    /// remainder must survive.
    #[test]
    fn test_or_with_multi_byte_remainder_preserves_boundary_bits() {
        let mut left = vec![0b00000000u8, 0b00000000u8, 0b11110000u8];
        let right = vec![0b11111111u8, 0b11111111u8, 0b11111111u8];
        // OR over 20 bits: bits 20..24 of `left` are outside the range and must stay set
        apply_bitwise_binary_op(&mut left, 0, &right, 0, 20, |a, b| a | b);
        assert_eq!(
            left,
            vec![0b11111111u8, 0b11111111u8, 0b11111111u8],
            "the boundary byte lost its out-of-range bits"
        );
    }

    #[test]
    fn test_not_with_multi_byte_remainder_preserves_boundary_bits() {
        let mut buffer = vec![0b00000000u8, 0b00000000u8, 0b11111111u8];
        // NOT over 20 bits: only bits 16..20 of the last byte may be flipped
        apply_bitwise_unary_op(&mut buffer, 0, 20, |a| !a);
        assert_eq!(
            buffer,
            vec![0b11111111u8, 0b11111111u8, 0b11110000u8],
            "the boundary byte lost its out-of-range bits"
        );
    }

    #[test]
    fn test_bitwise_binary_op_offset_out_of_bounds() {
        let input = vec![0b10101010u8, 0b01010101u8];
        let mut buffer = MutableBuffer::new(2); // space for 16 bits
        buffer.extend_from_slice(&input); // only 2 bytes
        apply_bitwise_binary_op(
            buffer.as_slice_mut(),
            100, // exceeds buffer length, becomes a noop
            [0b11110000u8, 0b00001111u8],
            0,
            0,
            |a, b| a & b,
        );
        assert_eq!(buffer.as_slice(), &input);
    }

    #[test]
    #[should_panic(expected = "assertion failed: last_offset <= buffer.len()")]
    fn test_bitwise_binary_op_length_out_of_bounds() {
        let mut buffer = MutableBuffer::new(2); // space for 16 bits
        buffer.extend_from_slice(&[0b10101010u8, 0b01010101u8]); // only 2 bytes
        apply_bitwise_binary_op(
            buffer.as_slice_mut(),
            0, // exceeds buffer length
            [0b11110000u8, 0b00001111u8],
            0,
            100,
            |a, b| a & b,
        );
        assert_eq!(buffer.as_slice(), &[0b10101010u8, 0b01010101u8]);
    }

    #[test]
    #[should_panic(expected = "offset + len out of bounds")]
    fn test_bitwise_binary_op_right_len_out_of_bounds() {
        let mut buffer = MutableBuffer::new(2); // space for 16 bits
        buffer.extend_from_slice(&[0b10101010u8, 0b01010101u8]); // only 2 bytes
        apply_bitwise_binary_op(
            buffer.as_slice_mut(),
            0, // exceeds buffer length
            [0b11110000u8, 0b00001111u8],
            1000,
            16,
            |a, b| a & b,
        );
        assert_eq!(buffer.as_slice(), &[0b10101010u8, 0b01010101u8]);
    }

    #[test]
    #[should_panic(expected = "the len is 2 but the index is 12")]
    fn test_bitwise_unary_op_offset_out_of_bounds() {
        let input = vec![0b10101010u8, 0b01010101u8];
        let mut buffer = MutableBuffer::new(2); // space for 16 bits
        buffer.extend_from_slice(&input); // only 2 bytes
        apply_bitwise_unary_op(
            buffer.as_slice_mut(),
            100, // exceeds buffer length, becomes a noop
            8,
            |a| !a,
        );
        assert_eq!(buffer.as_slice(), &input);
    }

    #[test]
    #[should_panic(expected = "assertion failed: last_offset <= buffer.len()")]
    fn test_bitwise_unary_op_length_out_of_bounds2() {
        let input = vec![0b10101010u8, 0b01010101u8];
        let mut buffer = MutableBuffer::new(2); // space for 16 bits
        buffer.extend_from_slice(&input); // only 2 bytes
        apply_bitwise_unary_op(
            buffer.as_slice_mut(),
            3,   // start at bit 3, to exercise different path
            100, // exceeds buffer length
            |a| !a,
        );
        assert_eq!(buffer.as_slice(), &input);
    }

    fn assert_copy_bits_within(bits: Vec<bool>, src: impl RangeBounds<usize> + Clone, dest: usize) {
        let expected_bytes = {
            let mut expected = bits.clone();
            // copy_within using the standard library to get the expected result
            expected.copy_within(src.clone(), dest);

            let expected = BooleanBuffer::from(expected);
            expected.into_inner().as_slice().to_vec()
        };
        let bits_len = bits.len();

        let mut input_bite_packed = BooleanBuffer::from(bits).into_inner().as_slice().to_vec();

        copy_bits_within(&mut input_bite_packed, bits_len, src, dest);

        assert_eq!(input_bite_packed, expected_bytes);
    }

    fn pattern(len: usize) -> Vec<bool> {
        let mut rng = seedable_rng();
        (0..len).map(|_| rng.random_bool(0.321)).collect()
    }

    #[test]
    fn test_copy_bits_within_noop() {
        assert_copy_bits_within(pattern(20), 5..5, 0);
        assert_copy_bits_within(pattern(20), 3..15, 3);
        assert_copy_bits_within(pattern(20), .., 0);
        assert_copy_bits_within(vec![], .., 0);
    }

    #[test]
    fn test_copy_bits_within_range_bounds() {
        assert_copy_bits_within(pattern(40), 3..=17, 20);
        assert_copy_bits_within(pattern(40), ..10, 25);
        assert_copy_bits_within(pattern(40), 30.., 1);
        assert_copy_bits_within(pattern(40), (Bound::Excluded(2), Bound::Included(12)), 27);
    }

    #[test]
    fn test_copy_bits_within_dest_and_src_share_same_bit_offset() {
        // Whole bytes only
        assert_copy_bits_within(pattern(128), 0..64, 64);
        assert_copy_bits_within(pattern(128), 64..128, 0);
        // Head only, tail only, head and tail
        assert_copy_bits_within(pattern(128), 3..64, 67);
        assert_copy_bits_within(pattern(128), 8..45, 72);
        assert_copy_bits_within(pattern(128), 3..45, 75);
        // Inside a single byte
        assert_copy_bits_within(pattern(32), 9..12, 25);
        // Overlapping in both directions
        assert_copy_bits_within(pattern(200), 13..190, 5);
        assert_copy_bits_within(pattern(200), 5..182, 13);
        // Range ending at the last bit of a partial byte
        assert_copy_bits_within(pattern(100), 50..100, 2);
    }

    #[test]
    fn test_copy_bits_within_different_phase() {
        assert_copy_bits_within(pattern(32), 1..4, 20);
        assert_copy_bits_within(pattern(32), 3..11, 14);
        assert_copy_bits_within(pattern(200), 0..64, 65);
        assert_copy_bits_within(pattern(200), 7..72, 100);
        // Overlapping by all but one bit
        assert_copy_bits_within(pattern(200), 1..200, 0);
        assert_copy_bits_within(pattern(200), 0..199, 1);
        // Range ending at the last bit of a partial byte
        assert_copy_bits_within(pattern(99), 40..99, 3);

        // Large input, overlapping in same byte in both directions
        assert_copy_bits_within(pattern(5000), 3..4500, 0);
        assert_copy_bits_within(pattern(5000), 0..4500, 3);
        assert_copy_bits_within(pattern(5000), 5..2053, 2054);
        // Non overlapping and far apart
        assert_copy_bits_within(pattern(5000), 4000..4990, 1);
        assert_copy_bits_within(pattern(5000), 1..990, 4003);
    }

    #[test]
    fn test_copy_bits_within_exhaustive_small() {
        for len in 1..=24 {
            for start in 0..=len {
                for end in start..=len {
                    for dest in 0..=len - (end - start) {
                        assert_copy_bits_within(pattern(len), start..end, dest);
                    }
                }
            }
        }
    }

    #[test]
    fn test_copy_bits_within_preserves_bits_past_buffer_len() {
        let mut data = vec![0xFFu8; 3];
        copy_bits_within(&mut data, 20, 0..4, 16);
        assert_eq!(data, vec![0xFF; 3]);

        let mut data = vec![0b1111_0000u8, 0xFF];
        copy_bits_within(&mut data, 12, 0..4, 8);
        assert_eq!(data, vec![0b1111_0000, 0b1111_0000]);
    }

    #[test]
    #[should_panic(expected = "start bound 5 > end bound 3")]
    fn test_copy_bits_within_start_after_end() {
        #[expect(clippy::reversed_empty_ranges)]
        copy_bits_within(&mut [0u8; 2], 16, 5..3, 0);
    }

    #[test]
    #[should_panic(expected = "end bound 17 is out of bounds")]
    fn test_copy_bits_within_end_out_of_bounds() {
        copy_bits_within(&mut [0u8; 2], 16, 0..17, 0);
    }

    #[test]
    #[should_panic(expected = "dest 7 is out of bounds")]
    fn test_copy_bits_within_dest_out_of_bounds() {
        copy_bits_within(&mut [0u8; 2], 16, 0..10, 7);
    }

    #[test]
    #[should_panic(expected = "buffer length 17 exceeds data")]
    fn test_copy_bits_within_buffer_len_exceeds_data() {
        copy_bits_within(&mut [0u8; 2], 17, 0..1, 0);
    }
}
