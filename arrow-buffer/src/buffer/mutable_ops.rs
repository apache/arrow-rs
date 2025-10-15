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

use super::{Buffer, MutableBuffer};
use crate::BooleanBufferBuilder;
use crate::bit_chunk_iterator::BitChunks;
use crate::util::bit_util::ceil;

/// What can be used as the right-hand side (RHS) buffer in mutable operations.
///
/// this is not mutated.
///
/// # Implementation notes
///
/// ## Why `pub(crate)`?
/// This is because we don't want this trait to expose the inner buffer to the public.
/// this is the trait implementor choice.
///
pub(crate) trait BufferSupportedRhs {
    fn as_slice(&self) -> &[u8];
}

impl BufferSupportedRhs for Buffer {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl BufferSupportedRhs for MutableBuffer {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl BufferSupportedRhs for BooleanBufferBuilder {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Trait that will be operated on as the left-hand side (LHS) buffer in mutable operations.
///
/// This consumer of the trait must satisfies the following guarantees:
/// 1. It will not change the length of the buffer.
///
/// # Implementation notes
///
/// ## Why is this trait `pub(crate)`?
/// Because we don't wanna expose the inner mutable buffer to the public.
/// as this is the choice of the implementor of the trait and sometimes it is not desirable
/// (e.g. `BooleanBufferBuilder`).
///
/// ## Why this trait is needed, can't we just use `MutableBuffer` directly?
/// Sometimes we don't want to expose the inner `MutableBuffer`
/// so it can't be misused.
///
/// For example, [`BooleanBufferBuilder`] does not expose the inner `MutableBuffer`
/// as exposing it will allow the user to change the length of the buffer that will make the
/// `BooleanBufferBuilder` invalid.
///
pub(crate) trait MutableOpsBufferSupportedLhs {
    /// Get a mutable reference to the inner `MutableBuffer`.
    ///
    /// This is used to perform in-place operations on the buffer.
    ///
    /// the caller must ensure that the length of the buffer is not changed.
    fn inner_mutable_buffer(&mut self) -> &mut MutableBuffer;
}

impl MutableOpsBufferSupportedLhs for MutableBuffer {
    fn inner_mutable_buffer(&mut self) -> &mut MutableBuffer {
        self
    }
}

/// Apply a binary bitwise operation to two bit-packed buffers.
///
/// This is the main entry point for binary operations. It handles both byte-aligned
/// and non-byte-aligned cases by delegating to specialized helper functions.
///
/// # Arguments
///
/// * `left` - The left mutable buffer to be modified in-place
/// * `left_offset_in_bits` - Starting bit offset in the left buffer
/// * `right` - The right buffer (as byte slice)
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
/// * `op` - Binary operation to apply (e.g., `|a, b| a & b`)
///
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs and BufferSupportedRhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_bitwise_bin_op_helper<F>(
    left: &mut impl MutableOpsBufferSupportedLhs,
    left_offset_in_bits: usize,
    right: &impl BufferSupportedRhs,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64, u64) -> u64,
{
    if len_in_bits == 0 {
        return;
    }

    let mutable_buffer = left.inner_mutable_buffer();

    let mutable_buffer_len = mutable_buffer.len();
    let mutable_buffer_cap = mutable_buffer.capacity();

    // offset inside a byte
    let left_bit_offset = left_offset_in_bits % 8;

    let is_mutable_buffer_byte_aligned = left_bit_offset == 0;

    if is_mutable_buffer_byte_aligned {
        mutable_buffer_byte_aligned_bitwise_bin_op_helper(
            mutable_buffer,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            op,
        );
    } else {
        // If we are not byte aligned, run `op` on the first few bits to reach byte alignment
        let bits_to_next_byte = 8 - left_bit_offset;

        {
            let right_byte_offset = right_offset_in_bits / 8;

            // Read the same amount of bits from the right buffer
            let right_first_byte: u8 = read_up_to_byte_from_offset(
                &right.as_slice()[right_byte_offset..],
                bits_to_next_byte,
                // Right bit offset
                right_offset_in_bits % 8,
            );

            align_to_byte(
                // Hope it gets inlined
                &mut |left| op(left, right_first_byte as u64),
                mutable_buffer,
                left_offset_in_bits,
            );
        }

        let left_offset_in_bits = left_offset_in_bits + bits_to_next_byte;
        let right_offset_in_bits = right_offset_in_bits + bits_to_next_byte;
        let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

        if len_in_bits == 0 {
            // Making sure that our guarantee that the length and capacity of the mutable buffer
            // will not change is upheld
            assert_eq!(
                mutable_buffer.len(),
                mutable_buffer_len,
                "The length of the mutable buffer must not change"
            );
            assert_eq!(
                mutable_buffer.capacity(),
                mutable_buffer_cap,
                "The capacity of the mutable buffer must not change"
            );

            return;
        }

        // We are now byte aligned
        mutable_buffer_byte_aligned_bitwise_bin_op_helper(
            mutable_buffer,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            op,
        );
    }

    // Making sure that our guarantee that the length and capacity of the mutable buffer
    // will not change is upheld
    assert_eq!(
        mutable_buffer.len(),
        mutable_buffer_len,
        "The length of the mutable buffer must not change"
    );
    assert_eq!(
        mutable_buffer.capacity(),
        mutable_buffer_cap,
        "The capacity of the mutable buffer must not change"
    );
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
fn align_to_byte<F>(op: &mut F, buffer: &mut MutableBuffer, offset_in_bits: usize)
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

/// Read up to 8 bits from a byte slice starting at a given bit offset.
///
/// This is similar to `get_8_bits_from_offset` but works with raw byte slices
/// and can read fewer than 8 bits.
///
/// # Arguments
///
/// * `slice` - The byte slice to read from
/// * `number_of_bits_to_read` - Number of bits to read (must be ≤ 8)
/// * `bit_offset` - Starting bit offset within the first byte
///
/// # Returns
///
/// A u8 containing the requested bits in the least significant positions
#[inline]
fn read_up_to_byte_from_offset(
    slice: &[u8],
    number_of_bits_to_read: usize,
    bit_offset: usize,
) -> u8 {
    assert!(number_of_bits_to_read <= 8);

    let bit_len = number_of_bits_to_read;
    if bit_len == 0 {
        0
    } else {
        // number of bytes to read
        // might be one more than sizeof(u64) if the offset is in the middle of a byte
        let byte_len = ceil(bit_len + bit_offset, 8);
        // pointer to remainder bytes after all complete chunks
        let base = slice.as_ptr();

        let mut bits = unsafe { std::ptr::read(base) } >> bit_offset;
        for i in 1..byte_len {
            let byte = unsafe { std::ptr::read(base.add(i)) };
            bits |= (byte) << (i * 8 - bit_offset);
        }

        bits & ((1 << bit_len) - 1)
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
fn mutable_buffer_byte_aligned_bitwise_bin_op_helper<F>(
    left: &mut MutableBuffer,
    left_offset_in_bits: usize,
    right: &impl BufferSupportedRhs,
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
        "left_offset_in_bits must be byte aligned"
    );

    // 1. Prepare the buffers
    let right_chunks = BitChunks::new(right.as_slice(), right_offset_in_bits, len_in_bits);
    let left_buffer_mut: &mut [u8] = {
        assert!(ceil(left_offset_in_bits + len_in_bits, 8) <= left.len() * 8);

        let byte_offset = left_offset_in_bits / 8;

        // number of complete u64 chunks
        let chunk_len = len_in_bits / 64;

        assert_eq!(right_chunks.chunk_len(), chunk_len);

        &mut left.as_slice_mut()[byte_offset..]
    };

    // cast to *const u64 should be fine since we are using read_unaligned below
    #[allow(clippy::cast_ptr_alignment)]
    let mut left_buffer_mut_u64_ptr = left_buffer_mut.as_mut_ptr() as *mut u64;

    let mut right_chunks_iter = right_chunks.iter();

    // If not only remainder bytes
    let had_any_chunks = right_chunks_iter.len() > 0;

    // 2. Process complete u64 chunks
    {
        // Process the first chunk outside the loop to avoid incrementing
        // the pointer after the last read
        if let Some(right) = right_chunks_iter.next() {
            unsafe {
                run_op_on_mutable_pointer_and_single_value(&mut op, left_buffer_mut_u64_ptr, right);
            }
        }

        for right in right_chunks_iter {
            // Increase the pointer for the next iteration
            // we are increasing the pointer before reading because we already read the first chunk above
            left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(1) };

            unsafe {
                run_op_on_mutable_pointer_and_single_value(&mut op, left_buffer_mut_u64_ptr, right);
            }
        }
    }

    // Handle remainder bits if any
    if right_chunks.remainder_len() > 0 {
        {
            // If we had any chunks we only advance the pointer at the start.
            // so we need to advance it again if we have a remainder
            let advance_pointer_count = if had_any_chunks { 1 } else { 0 };
            left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(advance_pointer_count) }
        }
        let left_buffer_mut_u8_ptr = left_buffer_mut_u64_ptr as *mut u8;

        handle_mutable_buffer_remainder(
            &mut op,
            left_buffer_mut_u8_ptr,
            right_chunks.remainder_bits(),
            right_chunks.remainder_len(),
        )
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
/// * `start_remainder_mut_ptr` - Pointer to the start of remainder bytes
/// * `right_remainder_bits` - Right operand bits
/// * `remainder_len` - Number of remainder bits
#[inline]
fn handle_mutable_buffer_remainder<F>(
    op: &mut F,
    start_remainder_mut_ptr: *mut u8,
    right_remainder_bits: u64,
    remainder_len: usize,
) where
    F: FnMut(u64, u64) -> u64,
{
    // Only read from mut pointer the number of remainder bits
    let left_remainder_bits = get_remainder_bits(start_remainder_mut_ptr, remainder_len);

    // Apply the operation
    let rem = op(left_remainder_bits, right_remainder_bits);

    // Write only the relevant bits back the result to the mutable pointer
    set_remainder_bits(start_remainder_mut_ptr, rem, remainder_len);
}

/// Write remainder bits back to buffer while preserving bits outside the range.
///
/// This function carefully updates only the specified bits, leaving all other
/// bits in the affected bytes unchanged.
///
/// # Arguments
///
/// * `start_remainder_mut_ptr` - Pointer to the start of remainder bytes
/// * `rem` - The result bits to write
/// * `remainder_len` - Number of bits to write
#[inline]
fn set_remainder_bits(start_remainder_mut_ptr: *mut u8, rem: u64, remainder_len: usize) {
    // Need to update the remainder bytes in the mutable buffer
    // but not override the bits outside the remainder

    // Update `rem` end with the current bytes in the mutable buffer
    // to preserve the bits outside the remainder
    let rem = {
        // 1. Read the byte that we will override
        let current = {
            let last_byte_position = remainder_len / 8;
            let last_byte_ptr = unsafe { start_remainder_mut_ptr.add(last_byte_position) };

            unsafe { std::ptr::read(last_byte_ptr) as u64 }
        };

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

    // Write back the result to the mutable pointer
    {
        let remainder_bytes = ceil(remainder_len, 8);

        // we are counting starting from the least significant bit, so to_le_bytes should be correct
        let rem = &rem.to_le_bytes()[0..remainder_bytes];

        // this assumes that `[ToByteSlice]` can be copied directly
        // without calling `to_byte_slice` for each element,
        // which is correct for all ArrowNativeType implementations.
        let src = rem.as_ptr();
        unsafe { std::ptr::copy_nonoverlapping(src, start_remainder_mut_ptr, remainder_bytes) };
    }
}

/// Read remainder bits from a pointer.
///
/// Reads the specified number of bits from memory and returns them as a u64.
///
/// # Arguments
///
/// * `remainder_ptr` - Pointer to the start of the bits
/// * `remainder_len` - Number of bits to read (must be < 64)
///
/// # Returns
///
/// A u64 containing the bits in the least significant positions
#[inline]
fn get_remainder_bits(remainder_ptr: *const u8, remainder_len: usize) -> u64 {
    let bit_len = remainder_len;
    // number of bytes to read
    let byte_len = ceil(bit_len, 8);
    // pointer to remainder bytes after all complete chunks
    let base = remainder_ptr;

    let mut bits = unsafe { std::ptr::read(base) } as u64;
    for i in 1..byte_len {
        let byte = unsafe { std::ptr::read(base.add(i)) };
        bits |= (byte as u64) << (i * 8);
    }

    bits & ((1 << bit_len) - 1)
}

/// Apply a binary operation to a u64 in memory.
///
/// Reads a u64 from the pointer, applies the operation with the right operand,
/// and writes the result back. Handles endianness correctly for bit-packed buffers.
///
/// # Safety
///
/// The pointer must be valid for reads and writes of u64 values.
/// Unaligned access is handled correctly via `read_unaligned` and `write_unaligned`.
///
/// # Arguments
///
/// * `op` - Binary operation to apply
/// * `left_buffer_mut_ptr` - Pointer to the left operand u64
/// * `right` - Right operand value
#[inline]
unsafe fn run_op_on_mutable_pointer_and_single_value<F>(
    op: &mut F,
    left_buffer_mut_ptr: *mut u64,
    right: u64,
) where
    F: FnMut(u64, u64) -> u64,
{
    // 1. Read the current value from the mutable buffer
    //
    // bit-packed buffers are stored starting with the least-significant byte first
    // so when reading as u64 on a big-endian machine, the bytes need to be swapped
    let current = unsafe { std::ptr::read_unaligned(left_buffer_mut_ptr).to_le() };

    // 2. Get the new value by applying the operation
    let combined = op(current, right);

    // 3. Write the new value back to the mutable buffer
    unsafe { std::ptr::write_unaligned(left_buffer_mut_ptr, combined) };
}

/// Perform bitwise unary operation on byte-aligned buffer.
///
/// This is the optimized path for byte-aligned unary operations. It processes data in
/// u64 chunks for maximum efficiency, then handles any remainder bits.
///
/// # Arguments
///
/// * `left` - The mutable buffer (must be byte-aligned)
/// * `left_offset_in_bits` - Starting bit offset (must be multiple of 8)
/// * `len_in_bits` - Number of bits to process
/// * `op` - Unary operation to apply (e.g., `|a| !a`)
#[inline]
fn mutable_byte_aligned_bitwise_unary_op_helper<F>(
    left: &mut MutableBuffer,
    left_offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64) -> u64,
{
    // Must not reach here if we not byte aligned
    assert_eq!(
        left_offset_in_bits % 8,
        0,
        "left_offset_in_bits must be byte aligned"
    );

    let number_of_u64_chunks = len_in_bits / 64;
    let remainder_len = len_in_bits % 64;

    let left_buffer_mut: &mut [u8] = {
        assert!(ceil(left_offset_in_bits + len_in_bits, 8) <= left.len() * 8);

        let byte_offset = left_offset_in_bits / 8;

        &mut left.as_slice_mut()[byte_offset..]
    };

    // cast to *const u64 should be fine since we are using read_unaligned below
    #[allow(clippy::cast_ptr_alignment)]
    let mut left_buffer_mut_u64_ptr = left_buffer_mut.as_mut_ptr() as *mut u64;

    // If not only remainder bytes
    let had_any_chunks = number_of_u64_chunks > 0;

    // Process the first chunk outside the loop
    if had_any_chunks {
        unsafe {
            run_op_on_mutable_pointer(&mut op, left_buffer_mut_u64_ptr);
        }
    }

    for _ in 1..number_of_u64_chunks {
        // Increase the pointer for the next iteration
        left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(1) };

        unsafe {
            run_op_on_mutable_pointer(&mut op, left_buffer_mut_u64_ptr);
        }
    }

    // Handle remainder bits if any
    if remainder_len > 0 {
        {
            // If we had any chunks we only advance the pointer at the start,
            // so we need to advance it again if we have a remainder
            let advance_pointer_count = if had_any_chunks { 1 } else { 0 };
            left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(advance_pointer_count) }
        }
        let left_buffer_mut_u8_ptr = left_buffer_mut_u64_ptr as *mut u8;

        handle_mutable_buffer_remainder_unary(&mut op, left_buffer_mut_u8_ptr, remainder_len);
    }
}

/// Apply a unary operation to a u64 in memory.
///
/// Reads a u64 from the pointer, applies the operation, and writes the result back.
///
/// # Safety
///
/// The pointer must be valid for reads and writes of u64 values.
///
/// # Arguments
///
/// * `op` - Unary operation to apply
/// * `buffer_mut_ptr` - Pointer to the u64
#[inline]
unsafe fn run_op_on_mutable_pointer<F>(op: &mut F, buffer_mut_ptr: *mut u64)
where
    F: FnMut(u64) -> u64,
{
    // 1. Read the current value from the mutable buffer
    //
    // bit-packed buffers are stored starting with the least-significant byte first
    // so when reading as u64 on a big-endian machine, the bytes need to be swapped
    let current = unsafe { std::ptr::read_unaligned(buffer_mut_ptr).to_le() };

    // 2. Get the new value by applying the operation
    let result = op(current);

    // 3. Write the new value back to the mutable buffer
    unsafe { std::ptr::write_unaligned(buffer_mut_ptr, result) };
}

/// Handle remainder bits (< 64 bits) for unary operations.
///
/// This function processes the bits that don't form a complete u64 chunk,
/// ensuring that bits outside the operation range are preserved.
///
/// # Arguments
///
/// * `op` - Unary operation to apply
/// * `start_remainder_mut_ptr` - Pointer to the start of remainder bytes
/// * `remainder_len` - Number of remainder bits
#[inline]
fn handle_mutable_buffer_remainder_unary<F>(
    op: &mut F,
    start_remainder_mut_ptr: *mut u8,
    remainder_len: usize,
) where
    F: FnMut(u64) -> u64,
{
    // Only read from mut pointer the number of remainder bits
    let left_remainder_bits = get_remainder_bits(start_remainder_mut_ptr, remainder_len);

    // Apply the operation
    let rem = op(left_remainder_bits);

    // Write only the relevant bits back the result to the mutable pointer
    set_remainder_bits(start_remainder_mut_ptr, rem, remainder_len);
}

/// Apply a bitwise operation to a mutable buffer and update it in-place.
///
/// This is the main entry point for unary operations. It handles both byte-aligned
/// and non-byte-aligned cases.
///
/// The input is treated as a bitmap, meaning that offset and length are specified
/// in number of bits.
///
/// # Arguments
///
/// * `buffer` - The mutable buffer to modify
/// * `offset_in_bits` - Starting bit offset
/// * `len_in_bits` - Number of bits to process
/// * `op` - Unary operation to apply (e.g., `|a| !a`)
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_bitwise_unary_op_helper<F>(
    buffer: &mut impl MutableOpsBufferSupportedLhs,
    offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) where
    F: FnMut(u64) -> u64,
{
    if len_in_bits == 0 {
        return;
    }

    let mutable_buffer = buffer.inner_mutable_buffer();

    let mutable_buffer_len = mutable_buffer.len();
    let mutable_buffer_cap = mutable_buffer.capacity();

    // offset inside a byte
    let left_bit_offset = offset_in_bits % 8;

    let is_mutable_buffer_byte_aligned = left_bit_offset == 0;

    if is_mutable_buffer_byte_aligned {
        mutable_byte_aligned_bitwise_unary_op_helper(
            mutable_buffer,
            offset_in_bits,
            len_in_bits,
            op,
        );
    } else {
        // If we are not byte aligned we will read the first few bits
        let bits_to_next_byte = 8 - left_bit_offset;

        align_to_byte(&mut op, mutable_buffer, offset_in_bits);

        let offset_in_bits = offset_in_bits + bits_to_next_byte;
        let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

        if len_in_bits == 0 {
            // Making sure that our guarantee that the length and capacity of the mutable buffer
            // will not change is upheld
            assert_eq!(
                mutable_buffer.len(),
                mutable_buffer_len,
                "The length of the mutable buffer must not change"
            );
            assert_eq!(
                mutable_buffer.capacity(),
                mutable_buffer_cap,
                "The capacity of the mutable buffer must not change"
            );

            return;
        }

        // We are now byte aligned
        mutable_byte_aligned_bitwise_unary_op_helper(
            mutable_buffer,
            offset_in_bits,
            len_in_bits,
            op,
        );
    }

    // Making sure that our guarantee that the length and capacity of the mutable buffer
    // will not change is upheld
    assert_eq!(
        mutable_buffer.len(),
        mutable_buffer_len,
        "The length of the mutable buffer must not change"
    );
    assert_eq!(
        mutable_buffer.capacity(),
        mutable_buffer_cap,
        "The capacity of the mutable buffer must not change"
    );
}

/// Apply a bitwise AND operation to two buffers.
///
/// The left buffer (mutable) is modified in-place to contain the result.
/// The inputs are treated as bitmaps, meaning that offsets and length are
/// specified in number of bits.
///
/// # Arguments
///
/// * `left` - The left mutable buffer (will be modified)
/// * `left_offset_in_bits` - Starting bit offset in the left buffer
/// * `right` - The right buffer
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs and BufferSupportedRhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_buffer_bin_and(
    left: &mut impl MutableOpsBufferSupportedLhs,
    left_offset_in_bits: usize,
    right: &impl BufferSupportedRhs,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) {
    mutable_bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & b,
    )
}

/// Apply a bitwise OR operation to two buffers.
///
/// The left buffer (mutable) is modified in-place to contain the result.
/// The inputs are treated as bitmaps, meaning that offsets and length are
/// specified in number of bits.
///
/// # Arguments
///
/// * `left` - The left mutable buffer (will be modified)
/// * `left_offset_in_bits` - Starting bit offset in the left buffer
/// * `right` - The right buffer
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs and BufferSupportedRhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_buffer_bin_or(
    left: &mut impl MutableOpsBufferSupportedLhs,
    left_offset_in_bits: usize,
    right: &impl BufferSupportedRhs,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) {
    mutable_bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a | b,
    )
}

/// Apply a bitwise XOR operation to two buffers.
///
/// The left buffer (mutable) is modified in-place to contain the result.
/// The inputs are treated as bitmaps, meaning that offsets and length are
/// specified in number of bits.
///
/// # Arguments
///
/// * `left` - The left mutable buffer (will be modified)
/// * `left_offset_in_bits` - Starting bit offset in the left buffer
/// * `right` - The right buffer
/// * `right_offset_in_bits` - Starting bit offset in the right buffer
/// * `len_in_bits` - Number of bits to process
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs and BufferSupportedRhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_buffer_bin_xor(
    left: &mut impl MutableOpsBufferSupportedLhs,
    left_offset_in_bits: usize,
    right: &impl BufferSupportedRhs,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) {
    mutable_bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a ^ b,
    )
}

/// Apply a bitwise NOT operation to the passed buffer.
///
/// The buffer (mutable) is modified in-place to contain the result.
/// The input is treated as bitmap, meaning that offsets and length are
/// specified in number of bits.
///
/// # Arguments
///
/// * `buffer` - The mutable buffer (will be modified)
/// * `offset_in_bits` - Starting bit offset in the buffer
/// * `len_in_bits` - Number of bits to process
#[allow(
    private_bounds,
    reason = "MutableOpsBufferSupportedLhs exposes the inner internals which is the implementor choice and we dont want to leak internals"
)]
pub fn mutable_buffer_unary_not(
    buffer: &mut impl MutableOpsBufferSupportedLhs,
    offset_in_bits: usize,
    len_in_bits: usize,
) {
    mutable_bitwise_unary_op_helper(buffer, offset_in_bits, len_in_bits, |a| !a)
}

#[cfg(test)]
mod tests {
    use crate::bit_iterator::BitIterator;
    use crate::{BooleanBuffer, BooleanBufferBuilder};

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

        super::mutable_bitwise_bin_op_helper(
            &mut left_buffer,
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

        super::mutable_bitwise_unary_op_helper(&mut buffer, offset_in_bits, len_in_bits, op);

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
        let left: Vec<bool> = (0..len).map(|i| i % 2 == 0).collect();
        let right: Vec<bool> = (0..len).map(|i| (i / 2) % 2 == 0).collect();
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
}
