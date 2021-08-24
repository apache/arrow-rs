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

use crate::{array::OffsetSizeTrait, buffer::MutableBuffer, util::bit_util};

/// extends the `buffer` to be able to hold `len` bits, setting all bits of the new size to zero.
#[inline]
pub(super) fn resize_for_bits(buffer: &mut MutableBuffer, len: usize) {
    let needed_bytes = bit_util::ceil(len, 8);
    if buffer.len() < needed_bytes {
        buffer.resize(needed_bytes, 0);
    }
}

/// sets all bits on `write_data` on the range `[offset_write..offset_write+len]` to be equal to the
/// bits on `data` on the range `[offset_read..offset_read+len]`
pub(super) fn set_bits(
    write_data: &mut [u8],
    data: &[u8],
    offset_write: usize,
    offset_read: usize,
    len: usize,
) -> usize {
    // write to write_data until it is at a whole-byte offset
    // create bit chunk iterator for data
    // write whole bytes from iterator to write_data

    let mut count = 0;
    (0..len).for_each(|i| {
        if bit_util::get_bit(data, offset_read + i) {
            bit_util::set_bit(write_data, offset_write + i);
        } else {
            count += 1;
        }
    });
    count
}

pub(super) fn extend_offsets<T: OffsetSizeTrait>(
    buffer: &mut MutableBuffer,
    mut last_offset: T,
    offsets: &[T],
) {
    buffer.reserve(offsets.len() * std::mem::size_of::<T>());
    offsets.windows(2).for_each(|offsets| {
        // compute the new offset
        let length = offsets[1] - offsets[0];
        last_offset += length;
        buffer.push(last_offset);
    });
}

#[inline]
pub(super) unsafe fn get_last_offset<T: OffsetSizeTrait>(
    offset_buffer: &MutableBuffer,
) -> T {
    // JUSTIFICATION
    //  Benefit
    //      20% performance improvement extend of variable sized arrays (see bench `mutable_array`)
    //  Soundness
    //      * offset buffer is always extended in slices of T and aligned accordingly.
    //      * Buffer[0] is initialized with one element, 0, and thus `mutable_offsets.len() - 1` is always valid.
    let (prefix, offsets, suffix) = offset_buffer.as_slice().align_to::<T>();
    debug_assert!(prefix.is_empty() && suffix.is_empty());
    *offsets.get_unchecked(offsets.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_bits_aligned() {
        let mut destination: Vec<u8> = vec![0b01010101, 0b00000000];
        let source: &[u8] = &[0b00110011];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 8;

        let expected_data: &[u8] = &[0b01010101, 0b00110011];
        let expected_null_count = 4;
        let result = set_bits(
            destination.as_mut_slice(),
            source,
            destination_offset,
            source_offset,
            len,
        );

        assert_eq!(destination, expected_data);
        assert_eq!(result, expected_null_count);
    }

    #[test]
    fn test_set_bits_unaligned() {
        let mut destination: Vec<u8> = vec![0, 0];
        let source: &[u8] = &[0b11111111];

        let destination_offset = 4;
        let source_offset = 2;

        let len = 6;

        let expected_data: &[u8] = &[0b11110000, 0b00000011];
        let expected_null_count = 0;
        let result = set_bits(
            destination.as_mut_slice(),
            source,
            destination_offset,
            source_offset,
            len,
        );

        assert_eq!(destination, expected_data);
        assert_eq!(result, expected_null_count);
    }
}
