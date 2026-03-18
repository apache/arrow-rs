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
use crate::BooleanBuffer;
use crate::util::bit_util::ceil;

/// Apply a bitwise operation `op` to four inputs and return the result as a Buffer.
///
/// The inputs are treated as bitmaps, meaning that offsets and length are
/// specified in number of bits.
///
/// NOTE: The operation `op` is applied to chunks of 64 bits (u64) and any bits
/// outside the offsets and len are set to zero out before calling `op`.
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
///
/// The inputs are treated as bitmaps, meaning that offsets and length are
/// specified in number of bits.
///
/// NOTE: The operation `op` is applied to chunks of 64 bits (u64) and any bits
/// outside the offsets and len are set to zero out before calling `op`.
pub fn bitwise_bin_op_helper<F>(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) -> Buffer
where
    F: FnMut(u64, u64) -> u64,
{
    let left_chunks = left.bit_chunks(left_offset_in_bits, len_in_bits);
    let right_chunks = right.bit_chunks(right_offset_in_bits, len_in_bits);

    let chunks = left_chunks
        .iter()
        .zip(right_chunks.iter())
        .map(|(left, right)| op(left, right));
    // Soundness: `BitChunks` is a `BitChunks` iterator which
    // correctly reports its upper bound
    let mut buffer = unsafe { MutableBuffer::from_trusted_len_iter(chunks) };

    let remainder_bytes = ceil(left_chunks.remainder_len(), 8);
    let rem = op(left_chunks.remainder_bits(), right_chunks.remainder_bits());
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    buffer.into()
}

/// Apply a bitwise operation `op` to one input and return the result as a Buffer.
///
/// The input is treated as a bitmap, meaning that offset and length are
/// specified in number of bits.
///
/// NOTE: The operation `op` is applied to chunks of 64 bits (u64) and any bits
/// outside the offsets and len are set to zero out before calling `op`.
pub fn bitwise_unary_op_helper<F>(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) -> Buffer
where
    F: FnMut(u64) -> u64,
{
    // reserve capacity and set length so we can get a typed view of u64 chunks
    let mut result =
        MutableBuffer::new(ceil(len_in_bits, 8)).with_bitset(len_in_bits / 64 * 8, false);

    let left_chunks = left.bit_chunks(offset_in_bits, len_in_bits);

    let result_chunks = result.typed_data_mut::<u64>().iter_mut();

    result_chunks
        .zip(left_chunks.iter())
        .for_each(|(res, left)| {
            *res = op(left);
        });

    let remainder_bytes = ceil(left_chunks.remainder_len(), 8);
    let rem = op(left_chunks.remainder_bits());
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    result.extend_from_slice(rem);

    result.into()
}

/// Apply a bitwise and to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
///
/// # See Also
/// * [`BooleanBuffer::from_bitwise_binary_op`] for creating `BooleanBuffer`s directly
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    let result = BooleanBuffer::from_bitwise_binary_op(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & b,
    );
    // Normalize non-zero BooleanBuffer offsets back to a zero-offset Buffer.
    if result.offset() == 0 {
        result.into_inner()
    } else {
        result.sliced()
    }
}

/// Apply a bitwise or to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
///
/// # See Also
/// * [`BooleanBuffer::from_bitwise_binary_op`] for creating `BooleanBuffer`s directly
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    let result = BooleanBuffer::from_bitwise_binary_op(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a | b,
    );
    // Normalize non-zero BooleanBuffer offsets back to a zero-offset Buffer.
    if result.offset() == 0 {
        result.into_inner()
    } else {
        result.sliced()
    }
}

/// Apply a bitwise xor to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
///
/// # See Also
/// * [`BooleanBuffer::from_bitwise_binary_op`] for creating `BooleanBuffer`s directly
pub fn buffer_bin_xor(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    let result = BooleanBuffer::from_bitwise_binary_op(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a ^ b,
    );
    // Normalize non-zero BooleanBuffer offsets back to a zero-offset Buffer.
    if result.offset() == 0 {
        result.into_inner()
    } else {
        result.sliced()
    }
}

/// Apply a bitwise and_not to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
///
/// # See Also
/// * [`BooleanBuffer::from_bitwise_binary_op`] for creating `BooleanBuffer`s directly
pub fn buffer_bin_and_not(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    let result = BooleanBuffer::from_bitwise_binary_op(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & !b,
    );
    // Normalize non-zero BooleanBuffer offsets back to a zero-offset Buffer.
    if result.offset() == 0 {
        result.into_inner()
    } else {
        result.sliced()
    }
}

/// Apply a bitwise not to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
///
/// # See Also
/// * [`BooleanBuffer::from_bitwise_unary_op`] for creating `BooleanBuffer`s directly
pub fn buffer_unary_not(left: &Buffer, offset_in_bits: usize, len_in_bits: usize) -> Buffer {
    BooleanBuffer::from_bitwise_unary_op(left, offset_in_bits, len_in_bits, |a| !a).into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_bin_ops_return_zero_offset_buffers() {
        let left = Buffer::from(vec![0b1010_1100, 0b0110_1001]);
        let right = Buffer::from(vec![
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0b1110_0101,
            0b0101_1000,
        ]);

        let left_offset = 1;
        let right_offset = 65; // same mod 64 as left_offset, so from_bitwise_binary_op returns non-zero offset
        let len = 7;

        // Reuse the same offset scenario for all four binary wrappers:
        // each wrapper should return the logically equivalent offset-0 Buffer,
        // even though the underlying BooleanBuffer result has offset 1.
        for (op, wrapper) in [
            (
                (|a, b| a & b) as fn(u64, u64) -> u64,
                buffer_bin_and as fn(&Buffer, usize, &Buffer, usize, usize) -> Buffer,
            ),
            (((|a, b| a | b) as fn(u64, u64) -> u64), buffer_bin_or),
            (((|a, b| a ^ b) as fn(u64, u64) -> u64), buffer_bin_xor),
            (((|a, b| a & !b) as fn(u64, u64) -> u64), buffer_bin_and_not),
        ] {
            let unsliced =
                BooleanBuffer::from_bitwise_binary_op(&left, left_offset, &right, right_offset, len, op);
            assert_eq!(unsliced.offset(), 1);

            let result = wrapper(&left, left_offset, &right, right_offset, len);

            assert_eq!(result, unsliced.sliced());
            assert_eq!(result.len(), 1);
        }
    }
}
