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
use crate::bit_chunk_iterator::BitChunks;
use crate::util::bit_util::ceil;

fn left_mutable_bitwise_bin_op_helper<F>(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &[u8],
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
  let left_bit_offset = left_offset_in_bits % 8;

  let is_mutable_buffer_byte_aligned = left_bit_offset == 0;

  if is_mutable_buffer_byte_aligned {
    mutable_left_byte_aligned_bitwise_bin_op_helper(
      left,
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

      // 3. read the same amount of bits from the right buffer
      let right_first_byte: u8 = read_up_to_byte_from_offset(
        &right[right_byte_offset..],
        bits_to_next_byte,
        // Right bit offset
        right_offset_in_bits % 8,
      );

      align_to_byte(
        // Hope it gets inlined
        &mut |left| op(left, right_first_byte as u64),
        left,
        left_offset_in_bits,
      );
    }

    let left_offset_in_bits = left_offset_in_bits + bits_to_next_byte;
    let right_offset_in_bits = right_offset_in_bits + bits_to_next_byte;
    let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

    if len_in_bits == 0 {
      return;
    }

    // We are now byte aligned
    mutable_left_byte_aligned_bitwise_bin_op_helper(
      left,
      left_offset_in_bits,
      right,
      right_offset_in_bits,
      len_in_bits,
      op,
    );
  }
}

fn align_to_byte<F>(
  op: &mut F,
  buffer: &mut MutableBuffer,
  offset_in_bits: usize,
)
where
  F: FnMut(u64) -> u64
{
    let byte_offset = offset_in_bits / 8;
    let bit_offset = offset_in_bits % 8;

    // 1. read the first byte from the buffer
    let first_byte: u8 = buffer.as_slice()[byte_offset];

    // 2. Shift byte by the bit offset, keeping only the relevant bits
    let relevant_first_byte = first_byte >> bit_offset;

    // 4. run the op on the first byte only
    let result_first_byte =
      op(relevant_first_byte as u64) as u8;

    // 5. Shift back the result to the original position
    let result_first_byte = result_first_byte << bit_offset;

    // 6. Mask the bits that are outside the relevant bits in the byte
    //    so the bits until bit_offset are 1 and the rest are 0
    let mask_for_first_bit_offset = (1 << bit_offset) - 1;

    let result_first_byte = (first_byte & mask_for_first_bit_offset)
      | (result_first_byte & !mask_for_first_bit_offset);

    // 7. write back the result to the buffer
    buffer.as_slice_mut()[byte_offset] = result_first_byte;
}

/// Read 8 bits from a buffer starting at a given bit offset
#[inline]
fn get_8_bits_from_offset(buffer: &Buffer, offset_in_bits: usize) -> u8 {
  let byte_offset = offset_in_bits / 8;
  let bit_offset = offset_in_bits % 8;

  let first_byte = buffer.as_slice()[byte_offset] as u16;
  let second_byte = if byte_offset + 1 < buffer.len() {
    buffer.as_slice()[byte_offset + 1] as u16
  } else {
    0
  };

  // Combine the two bytes into a single u16
  let combined = (second_byte << 8) | first_byte;

  // Shift right by the bit offset and mask to get the relevant 8 bits
  ((combined >> bit_offset) & 0xFF) as u8
}

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
    let base = unsafe { slice.as_ptr() };

    let mut bits = unsafe { std::ptr::read(base) } >> bit_offset;
    for i in 1..byte_len {
      let byte = unsafe { std::ptr::read(base.add(i)) };
      bits |= (byte) << (i * 8 - bit_offset);
    }

    bits & ((1 << bit_len) - 1)
  }
}

/// Helper function to run the bitwise operation when we know that the left offset is byte-aligned.
/// This is the easiest case as we can do the operation directly on u64 chunks and then handle the remainder bits if any.
#[inline]
fn mutable_left_byte_aligned_bitwise_bin_op_helper<F>(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &[u8],
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

  let right_chunks = BitChunks::new(right, right_offset_in_bits, len_in_bits);
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

  // Trying to read the first chunk
  // we do this outside the loop so in the loop we can increment the pointer first
  // and then read the value
  // avoiding incrementing the pointer after the last read
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
  set_remainder_bits(
    start_remainder_mut_ptr,
    rem,
    remainder_len,
  );
}


#[inline]
fn set_remainder_bits(
  start_remainder_mut_ptr: *mut u8,
  rem: u64,
  remainder_len: usize,
) {
  // Need to update the remainder bytes in the mutable buffer
  // but not override the bits outside the remainder

  // Update `rem` end with the current bytes in the mutable buffer
  // to preserve the bits outside the remainder
  let rem = {
    // 1. Read the byte that we will override
    let mut current = {
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

    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];

    // this assumes that `[ToByteSlice]` can be copied directly
    // without calling `to_byte_slice` for each element,
    // which is correct for all ArrowNativeType implementations.
    let src = rem.as_ptr() as *const u8;
    unsafe { std::ptr::copy_nonoverlapping(src, start_remainder_mut_ptr, remainder_bytes) };
  }
}

// Get remainder bits from a pointer and length in bits
#[inline]
fn get_remainder_bits(remainder_ptr: *const u8, remainder_len: usize) -> u64 {
  let bit_len = remainder_len;
  // number of bytes to read
  // might be one more than sizeof(u64) if the offset is in the middle of a byte
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

// TODO - find a better name
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
  // TODO - should write with to_le()? because we already read as to_le()
  unsafe { std::ptr::write_unaligned(left_buffer_mut_ptr, combined) };
}


/// Helper function to run the bitwise operation when we know that the left offset is byte-aligned.
/// This is the easiest case as we can do the operation directly on u64 chunks and then handle the remainder bits if any.
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

  // Trying to read the first chunk
  // we do this outside the loop so in the loop we can increment the pointer first
  // and then read the value
  // avoiding incrementing the pointer after the last read
  if had_any_chunks {
    unsafe {
      run_op_on_mutable_pointer(&mut op, left_buffer_mut_u64_ptr);
    }
  }

  for _ in 1..number_of_u64_chunks {
    // Increase the pointer for the next iteration
    // we are increasing the pointer before reading because we already read the first chunk above
    left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(1) };

    unsafe {
      run_op_on_mutable_pointer(&mut op, left_buffer_mut_u64_ptr);
    }
  }

  // Handle remainder bits if any
  if remainder_len > 0 {
    {
      // If we had any chunks we only advance the pointer at the start.
      // so we need to advance it again if we have a remainder
      let advance_pointer_count = if had_any_chunks { 1 } else { 0 };
      left_buffer_mut_u64_ptr = unsafe { left_buffer_mut_u64_ptr.add(advance_pointer_count) }
    }
    let left_buffer_mut_u8_ptr = left_buffer_mut_u64_ptr as *mut u8;

    handle_mutable_buffer_remainder_unary(
      &mut op,
      left_buffer_mut_u8_ptr,
      remainder_len,
    );
  }
}

// TODO - find a better name
#[inline]
unsafe fn run_op_on_mutable_pointer<F>(
  op: &mut F,
  left_buffer_mut_ptr: *mut u64,
) where
  F: FnMut(u64) -> u64,
{
  // 1. Read the current value from the mutable buffer
  //
  // bit-packed buffers are stored starting with the least-significant byte first
  // so when reading as u64 on a big-endian machine, the bytes need to be swapped
  let current = unsafe { std::ptr::read_unaligned(left_buffer_mut_ptr).to_le() };

  // 2. Get the new value by applying the operation
  let result = op(current);

  // 3. Write the new value back to the mutable buffer
  // TODO - should write with to_le()? because we already read as to_le()
  unsafe { std::ptr::write_unaligned(left_buffer_mut_ptr, result) };
}

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
  set_remainder_bits(
    start_remainder_mut_ptr,
    rem,
    remainder_len,
  );
}

/// Apply a bitwise operation `op` to the passed [`MutableBuffer`] and update it
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
pub fn mutable_bitwise_unary_op_helper<F>(
  buffer: &mut MutableBuffer,
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
    mutable_byte_aligned_bitwise_unary_op_helper(
      buffer,
      offset_in_bits,
      len_in_bits,
      op,
    );
  } else {
    // If we are not byte aligned we will read the first few bits
    let bits_to_next_byte = 8 - left_bit_offset;

    align_to_byte(
      &mut op,
      buffer,
      offset_in_bits
    );

    let offset_in_bits = offset_in_bits + bits_to_next_byte;
    let len_in_bits = len_in_bits.saturating_sub(bits_to_next_byte);

    if len_in_bits == 0 {
      return;
    }

    // We are now byte aligned
    mutable_byte_aligned_bitwise_unary_op_helper(
      buffer,
      offset_in_bits,
      len_in_bits,
      op,
    );
  }
}

/// Apply a bitwise and to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn left_mutable_buffer_bin_and(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &Buffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a & b,
  )
}

/// Apply a bitwise and to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn both_mutable_buffer_bin_and(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &MutableBuffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a & b,
  )
}

/// Apply a bitwise or to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn left_mutable_buffer_bin_or(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &Buffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a | b,
  )
}

/// Apply a bitwise or to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn both_mutable_buffer_bin_or(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &MutableBuffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a | b,
  )
}

/// Apply a bitwise xor to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn left_mutable_buffer_bin_xor(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &Buffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a ^ b,
  )
}

/// Apply a bitwise xor to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn both_mutable_buffer_bin_xor(
  left: &mut MutableBuffer,
  left_offset_in_bits: usize,
  right: &MutableBuffer,
  right_offset_in_bits: usize,
  len_in_bits: usize,
) {
  left_mutable_bitwise_bin_op_helper(
    left,
    left_offset_in_bits,
    right.as_slice(),
    right_offset_in_bits,
    len_in_bits,
    |a, b| a ^ b,
  )
}

/// Apply a bitwise not to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
pub fn mutable_buffer_unary_not(
  left: &mut MutableBuffer,
  offset_in_bits: usize,
  len_in_bits: usize,
) {
  mutable_bitwise_unary_op_helper(left, offset_in_bits, len_in_bits, |a| !a)
}
