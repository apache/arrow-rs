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

use super::MutableBuffer;
use crate::bit_chunk_iterator::BitChunks;
use crate::util::bit_util;

impl MutableBuffer {
    /// Apply a binary bitwise operation on self (mutate) with respect to another buffer (right).
    ///
    /// # Arguments
    ///
    /// * `self` - The mutable buffer to be modified in-place
    /// * `offset_in_bits` - Starting bit offset in Self buffer
    /// * `right` - slice of bit-packed bytes in LSB order
    /// * `right_offset_in_bits` - Starting bit offset in the right buffer
    /// * `len_in_bits` - Number of bits to process
    /// * `op` - Binary operation to apply (e.g., `|a, b| a & b`)
    ///
    pub fn bitwise_binary_op<F>(
        &mut self,
        offset_in_bits: usize,
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

        let mutable_buffer_len = self.len();
        let mutable_buffer_cap = self.capacity();

        // offset inside a byte
        let bit_offset = offset_in_bits % 8;

        let is_mutable_buffer_byte_aligned = bit_offset == 0;

        if is_mutable_buffer_byte_aligned {
            byte_aligned_bitwise_bin_op_helper(
                self,
                offset_in_bits,
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
                let right_first_byte: u8 = crate::util::bit_util::read_up_to_byte_from_offset(
                    &right.as_ref()[right_byte_offset..],
                    bits_to_next_byte,
                    // Right bit offset
                    right_offset_in_bits % 8,
                );

                align_to_byte(
                    self,
                    // Hope it gets inlined
                    &mut |left| op(left, right_first_byte as u64),
                    offset_in_bits,
                );
            }

            let offset_in_bits = offset_in_bits + bits_to_next_byte;
            let right_offset_in_bits = right_offset_in_bits + bits_to_next_byte;
            let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

            if len_in_bits == 0 {
                // Making sure that our guarantee that the length and capacity of the mutable buffer
                // will not change is upheld
                assert_eq!(
                    self.len(),
                    mutable_buffer_len,
                    "The length of the mutable buffer must not change"
                );
                assert_eq!(
                    self.capacity(),
                    mutable_buffer_cap,
                    "The capacity of the mutable buffer must not change"
                );

                return;
            }

            // We are now byte aligned
            byte_aligned_bitwise_bin_op_helper(
                self,
                offset_in_bits,
                right,
                right_offset_in_bits,
                len_in_bits,
                op,
            );
        }

        // Making sure that our guarantee that the length and capacity of the mutable buffer
        // will not change is upheld
        assert_eq!(
            self.len(),
            mutable_buffer_len,
            "The length of the mutable buffer must not change"
        );
        assert_eq!(
            self.capacity(),
            mutable_buffer_cap,
            "The capacity of the mutable buffer must not change"
        );
    }

    /// Apply a bitwise operation to a mutable buffer and update it in-place.
    ///
    /// # Arguments
    ///
    /// * `offset_in_bits` - Starting bit offset for the current buffer
    /// * `len_in_bits` - Number of bits to process
    /// * `op` - Unary operation to apply (e.g., `|a| !a`)
    ///
    pub fn bitwise_unary_op<F>(&mut self, offset_in_bits: usize, len_in_bits: usize, mut op: F)
    where
        F: FnMut(u64) -> u64,
    {
        if len_in_bits == 0 {
            return;
        }

        let mutable_buffer_len = self.len();
        let mutable_buffer_cap = self.capacity();

        // offset inside a byte
        let left_bit_offset = offset_in_bits % 8;

        let is_mutable_buffer_byte_aligned = left_bit_offset == 0;

        if is_mutable_buffer_byte_aligned {
            byte_aligned_bitwise_unary_op_helper(self, offset_in_bits, len_in_bits, op);
        } else {
            align_to_byte(self, &mut op, offset_in_bits);

            // If we are not byte aligned we will read the first few bits
            let bits_to_next_byte = 8 - left_bit_offset;

            let offset_in_bits = offset_in_bits + bits_to_next_byte;
            let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

            if len_in_bits == 0 {
                // Making sure that our guarantee that the length and capacity of the mutable buffer
                // will not change is upheld
                assert_eq!(
                    self.len(),
                    mutable_buffer_len,
                    "The length of the mutable buffer must not change"
                );
                assert_eq!(
                    self.capacity(),
                    mutable_buffer_cap,
                    "The capacity of the mutable buffer must not change"
                );

                return;
            }

            // We are now byte aligned
            byte_aligned_bitwise_unary_op_helper(self, offset_in_bits, len_in_bits, op);
        }

        // Making sure that our guarantee that the length and capacity of the mutable buffer
        // will not change is upheld
        assert_eq!(
            self.len(),
            mutable_buffer_len,
            "The length of the mutable buffer must not change"
        );
        assert_eq!(
            self.capacity(),
            mutable_buffer_cap,
            "The capacity of the mutable buffer must not change"
        );
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
    left: &mut MutableBuffer,
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
        bit_util::ceil(right_chunks.remainder_len(), 8),
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
    buffer: &mut MutableBuffer,
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

    assert_eq!(bit_util::ceil(remainder_len, 8), remainder_bytes.len());

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
fn align_to_byte<F>(buffer: &mut MutableBuffer, op: &mut F, offset_in_bits: usize)
where
    F: FnMut(u64) -> u64,
{
    let byte_offset = offset_in_bits / 8;
    let bit_offset = offset_in_bits % 8;

    // 1. read the first byte from the buffer
    let first_byte: u8 = buffer.as_slice()[byte_offset];

    // 2. Shift byte by the bit offset, keeping only the relevant bits
    let relevant_first_byte = first_byte >> bit_offset;

    // 3. run the op on the first byte only
    let result_first_byte = op(relevant_first_byte as u64) as u8;

    // 4. Shift back the result to the original position
    let result_first_byte = result_first_byte << bit_offset;

    // 5. Mask the bits that are outside the relevant bits in the byte
    //    so the bits until bit_offset are 1 and the rest are 0
    let mask_for_first_bit_offset = (1 << bit_offset) - 1;

    let result_first_byte =
        (first_byte & mask_for_first_bit_offset) | (result_first_byte & !mask_for_first_bit_offset);

    // 6. write back the result to the buffer
    buffer.as_slice_mut()[byte_offset] = result_first_byte;
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
    /// Create a new [`U64UnalignedSlice`] from a [`MutableBuffer`]
    ///
    /// return the [`U64UnalignedSlice`] and slice of bytes that are not part of the u64 chunks (guaranteed to be less than 8 bytes)
    ///
    fn split(
        mutable_buffer: &'a mut MutableBuffer,
        offset_in_bits: usize,
        len_in_bits: usize,
    ) -> (Self, &'a mut [u8]) {
        // 1. Prepare the buffers
        let left_buffer_mut: &mut [u8] = {
            let last_offset = bit_util::ceil(offset_in_bits + len_in_bits, 8);
            assert!(last_offset <= mutable_buffer.len());

            let byte_offset = offset_in_bits / 8;

            &mut mutable_buffer.as_slice_mut()[byte_offset..last_offset]
        };

        let number_of_u64_we_can_fit = len_in_bits / (u64::BITS as usize);

        // 2. Split
        let u64_len_in_bytes = number_of_u64_we_can_fit * size_of::<u64>();

        assert!(u64_len_in_bytes <= left_buffer_mut.len());
        let (bytes_for_u64, remainder) = left_buffer_mut.split_at_mut(u64_len_in_bytes);

        let ptr = bytes_for_u64.as_mut_ptr() as *mut u64;

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
        bit_util::ceil(remainder_len, 8),
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

        let current = *current as u64;

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
        let remainder_bytes = bit_util::ceil(remainder_len, 8);

        // we are counting starting from the least significant bit, so to_le_bytes should be correct
        let rem = &rem.to_le_bytes()[0..remainder_bytes];

        // this assumes that `[ToByteSlice]` can be copied directly
        // without calling `to_byte_slice` for each element,
        // which is correct for all ArrowNativeType implementations including u64.
        let src = rem.as_ptr();
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
        bit_util::ceil(remainder_len, 8),
        "remainder and remainder len ceil must be the same"
    );

    let bits = remainder
        .iter()
        .enumerate()
        .fold(0_u64, |acc, (index, &byte)| {
            acc | (byte as u64) << (index * 8)
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

#[cfg(test)]
mod tests {
    use crate::bit_iterator::BitIterator;
    use crate::{BooleanBuffer, BooleanBufferBuilder};
    use rand::Rng;

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
        let mut left_buffer = BooleanBufferBuilder::from(left_data);
        let right_buffer = BooleanBuffer::from(right_data);

        let expected: Vec<bool> = left_data
            .iter()
            .skip(left_offset_in_bits)
            .zip(right_data.iter().skip(right_offset_in_bits))
            .take(len_in_bits)
            .map(|(l, r)| expected_op(*l, *r))
            .collect();

        let mutable_buffer = unsafe { left_buffer.mutable_buffer() };

        mutable_buffer.bitwise_binary_op(
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
            "Failed with left_offset={}, right_offset={}, len={}",
            left_offset_in_bits, right_offset_in_bits, len_in_bits
        );
    }

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
        let mut buffer = BooleanBufferBuilder::from(data);

        let expected: Vec<bool> = data
            .iter()
            .skip(offset_in_bits)
            .take(len_in_bits)
            .map(|b| expected_op(*b))
            .collect();

        let mutable_buffer = unsafe { buffer.mutable_buffer() };

        mutable_buffer.bitwise_unary_op(offset_in_bits, len_in_bits, op);

        let result: Vec<bool> =
            BitIterator::new(buffer.as_slice(), offset_in_bits, len_in_bits).collect();

        assert_eq!(
            result, expected,
            "Failed with offset={}, len={}",
            offset_in_bits, len_in_bits
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
}
