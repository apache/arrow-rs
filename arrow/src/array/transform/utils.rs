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

use crate::{
    array::OffsetSizeTrait,
    buffer::MutableBuffer,
    util::{
        bit_chunk_iterator::BitChunks,
        bit_util::{self, ceil},
    },
};

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
    let mut null_count = 0;

    let mut bits_to_align = offset_write % 8;
    if bits_to_align > 0 {
        bits_to_align = std::cmp::min(len, 8 - bits_to_align);
    }
    let bytes_to_copy = ceil(offset_write + bits_to_align, 8)
        ..ceil(offset_write + len - bits_to_align, 8);

    let chunks = BitChunks::new(data, offset_read + bits_to_align, len - bits_to_align);
    let mut bytes: Vec<u8> = Vec::with_capacity(bytes_to_copy.len());
    chunks.iter().for_each(|chunk| {
        null_count += chunk.count_zeros();
        bytes.extend_from_slice(&chunk.to_ne_bytes());
    });

    let remainder_bits = &chunks.remainder_len();
    (0..bits_to_align)
        .chain(len - remainder_bits..len)
        .for_each(|i| {
            if bit_util::get_bit(data, offset_read + i) {
                bit_util::set_bit(write_data, offset_write + i);
            } else {
                null_count += 1;
            }
        });
    bytes_to_copy.zip(bytes.iter()).for_each(|(i, b)| {
        write_data[i] = *b;
    });
    null_count as usize
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
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111,
            0b11100111, 0b11100111,
        ];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 64;

        let expected_data: &[u8] = &[
            0, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111,
            0b11100111, 0b11100111, 0,
        ];
        let expected_null_count = 16;
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
    fn test_set_bits_unaligned_unset() {
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111,
            0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111,
        ];

        let destination_offset = 4;
        let source_offset = 4;

        let len = 88;

        let expected_data: &[u8] = &[
            0b11100000, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111,
            0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b11100111, 0b0000111,
        ];
        let expected_null_count = 22;
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
