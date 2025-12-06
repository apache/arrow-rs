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
use crate::util::bit_util::ceil;

/// Apply a bitwise operation `op` to four inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn bitwise_quaternary_op_helper<F>(
    buffers: [&Buffer; 4],
    offsets: [usize; 4],
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: Fn(u64, u64, u64, u64) -> u64,
{
    let first_chunks = buffers[0].bit_chunks(offsets[0], len_in_bits);
    let second_chunks = buffers[1].bit_chunks(offsets[1], len_in_bits);
    let third_chunks = buffers[2].bit_chunks(offsets[2], len_in_bits);
    let fourth_chunks = buffers[3].bit_chunks(offsets[3], len_in_bits);

    let chunks = first_chunks
        .iter()
        .zip(second_chunks.iter())
        .zip(third_chunks.iter())
        .zip(fourth_chunks.iter())
        .map(|(((first, second), third), fourth)| op(first, second, third, fourth));
    // Soundness: `BitChunks` is a `BitChunks` iterator which
    // correctly reports its upper bound
    let mut buffer = unsafe { MutableBuffer::from_trusted_len_iter(chunks) };

    let remainder_bytes = ceil(first_chunks.remainder_len(), 8);
    let rem = op(
        first_chunks.remainder_bits(),
        second_chunks.remainder_bits(),
        third_chunks.remainder_bits(),
        fourth_chunks.remainder_bits(),
    );
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    buffer.into()
}

/// Apply a bitwise operation `op` to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_binary instead")]
pub fn bitwise_bin_op_helper<F>(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: Fn(u64, u64) -> u64 + Copy,
{
    left.bitwise_binary(right, left_offset_in_bits, right_offset_in_bits, len_in_bits, op)
}

/// Apply a bitwise operation `op` to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_unary instead")]
pub fn bitwise_unary_op_helper<F>(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: Fn(u64) -> u64 + Copy,
{
    left.bitwise_unary(offset_in_bits, len_in_bits, op)
}

/// Apply a bitwise and to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_binary with |a, b| a & b or BooleanArray::binary instead")]
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    left.bitwise_binary(right, left_offset_in_bits, right_offset_in_bits, len_in_bits, |a, b| a & b)
}

/// Apply a bitwise or to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_binary with |a, b| a | b or BooleanArray::binary instead")]
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    left.bitwise_binary(right, left_offset_in_bits, right_offset_in_bits, len_in_bits, |a, b| a | b)
}

/// Apply a bitwise xor to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_binary with |a, b| a ^ b or BooleanArray::binary instead")]
pub fn buffer_bin_xor(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    left.bitwise_binary(right, left_offset_in_bits, right_offset_in_bits, len_in_bits, |a, b| a ^ b)
}

/// Apply a bitwise and_not to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_binary with |a, b| a & !b or BooleanArray::binary instead")]
pub fn buffer_bin_and_not(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    left.bitwise_binary(right, left_offset_in_bits, right_offset_in_bits, len_in_bits, |a, b| a & !b)
}

/// Apply a bitwise not to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
#[deprecated(note = "use Buffer::bitwise_unary with |a| !a or BooleanArray::unary instead")]
pub fn buffer_unary_not(left: &Buffer, offset_in_bits: usize, len_in_bits: usize) -> Buffer {
    left.bitwise_unary(offset_in_bits, len_in_bits, |a| !a)
}
