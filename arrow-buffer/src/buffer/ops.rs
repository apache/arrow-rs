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
use crate::bit_util::{bitwise_binary_op, bitwise_unary_op};
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
///
/// The output is guaranteed to have
/// 1. all bits outside the specified range set to zero
/// 2. start at offset zero
pub fn bitwise_bin_op_helper<F>(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: FnMut(u64, u64) -> u64,
{
    if len_in_bits == 0 {
        return Buffer::default();
    }

    // figure out the starting byte for left buffer
    let start_byte = left_offset_in_bits / 8;
    let starting_bit_in_byte = left_offset_in_bits % 8;

    let len_bytes = ceil(starting_bit_in_byte + len_in_bits, 8);
    let mut result = left[start_byte..len_bytes].to_vec();
    bitwise_binary_op(
        &mut result,
        starting_bit_in_byte,
        right,
        right_offset_in_bits,
        len_in_bits,
        op,
    );

    // shift result to the left so that that it starts at offset zero (TODO do this a word at a time)
    shift_left_by(&mut result, starting_bit_in_byte);
    result.into()
}

/// Shift the bits in the buffer to the left by `shift` bits.
/// `shift` must be less than 8.
fn shift_left_by(buffer: &mut [u8], starting_bit_in_byte: usize) {
    if starting_bit_in_byte == 0 {
        return;
    }
    assert!(starting_bit_in_byte < 8);
    let shift = 8 - starting_bit_in_byte;
    let carry_mask = ((1u8 << starting_bit_in_byte) - 1) << shift;

    let mut carry = 0;
    // shift from right to left
    for b in buffer.iter_mut().rev() {
        let new_carry = (*b & carry_mask) >> shift;
        *b = (*b << starting_bit_in_byte) | carry;
        carry = new_carry;
    }
}

/// Apply a bitwise operation `op` to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
///
/// The output is guaranteed to have
/// 1. all bits outside the specified range set to zero
/// 2. start at offset zero
pub fn bitwise_unary_op_helper<F>(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
    mut op: F,
) -> Buffer
where
    F: FnMut(u64) -> u64,
{
    if len_in_bits == 0 {
        return Buffer::default();
    }
    // already byte aligned, copy over directly
    let len_in_bytes = ceil(len_in_bits, 8);
    let mut result;
    if offset_in_bits == 0 {
        result = left.as_slice()[0..len_in_bytes].to_vec();
        bitwise_unary_op(&mut result, 0, len_in_bits, op);
    } else {
        // need to align bits
        result = vec![0u8; len_in_bytes];
        bitwise_binary_op(&mut result, 0, left, offset_in_bits, len_in_bits, |_, b| {
            op(b)
        });
    }
    result.into()
}

/// Apply a bitwise and to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & b,
    )
}

/// Apply a bitwise or to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a | b,
    )
}

/// Apply a bitwise xor to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn buffer_bin_xor(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a ^ b,
    )
}

/// Apply a bitwise and_not to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn buffer_bin_and_not(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & !b,
    )
}

/// Apply a bitwise not to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
pub fn buffer_unary_not(left: &Buffer, offset_in_bits: usize, len_in_bits: usize) -> Buffer {
    bitwise_unary_op_helper(left, offset_in_bits, len_in_bits, |a| !a)
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_shift_left_by() {
        let input = vec![0b10110011, 0b00011100, 0b11111111];
        do_shift_left_by(&input, 0, &input);
        do_shift_left_by(&input, 1, &[0b01100110, 0b00111001, 0b11111110]);
        do_shift_left_by(&input, 2, &[0b11001100, 0b01110011, 0b11111100]);
        do_shift_left_by(&input, 3, &[0b10011000, 0b11100111, 0b11111000]);
        do_shift_left_by(&input, 4, &[0b00110001, 0b11001111, 0b11110000]);
        do_shift_left_by(&input, 5, &[0b01100011, 0b10011111, 0b11100000]);
        do_shift_left_by(&input, 6, &[0b11000111, 0b00111111, 0b11000000]);
        do_shift_left_by(&input, 7, &[0b10001110, 0b01111111, 0b10000000]);

    }
    fn do_shift_left_by(input: &[u8], shift: usize, expected: &[u8])  {
        let mut buffer = input.to_vec();
        super::shift_left_by(&mut buffer, shift);
        assert_eq!(buffer, expected,
                   "\nshift_left_by({}, {})\nactual:   {}\nexpected: {}",
                   buffer_string(input), shift,
                   buffer_string(&buffer),
                   buffer_string(expected)
        );
    }
    fn buffer_string(buffer: &[u8]) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for b in buffer {
            write!(&mut s, "{:08b} ", b).unwrap();
        }
        s
    }
}