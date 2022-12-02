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

//! Utils for working with packed bit masks

use crate::ArrayData;
use arrow_buffer::bit_chunk_iterator::BitChunks;
use arrow_buffer::bit_util::{ceil, get_bit, set_bit};
use arrow_buffer::buffer::buffer_bin_and;
use arrow_buffer::Buffer;

/// Sets all bits on `write_data` in the range `[offset_write..offset_write+len]` to be equal to the
/// bits in `data` in the range `[offset_read..offset_read+len]`
/// returns the number of `0` bits `data[offset_read..offset_read+len]`
pub fn set_bits(
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
    let mut write_byte_index = ceil(offset_write + bits_to_align, 8);

    // Set full bytes provided by bit chunk iterator (which iterates in 64 bits at a time)
    let chunks = BitChunks::new(data, offset_read + bits_to_align, len - bits_to_align);
    chunks.iter().for_each(|chunk| {
        null_count += chunk.count_zeros();
        write_data[write_byte_index..write_byte_index + 8]
            .copy_from_slice(&chunk.to_le_bytes());
        write_byte_index += 8;
    });

    // Set individual bits both to align write_data to a byte offset and the remainder bits not covered by the bit chunk iterator
    let remainder_offset = len - chunks.remainder_len();
    (0..bits_to_align)
        .chain(remainder_offset..len)
        .for_each(|i| {
            if get_bit(data, offset_read + i) {
                set_bit(write_data, offset_write + i);
            } else {
                null_count += 1;
            }
        });

    null_count as usize
}

/// Combines the null bitmaps of multiple arrays using a bitwise `and` operation.
///
/// This function is useful when implementing operations on higher level arrays.
pub fn combine_option_bitmap(
    arrays: &[&ArrayData],
    len_in_bits: usize,
) -> Option<Buffer> {
    let (buffer, offset) = arrays
        .iter()
        .map(|array| (array.null_buffer().cloned(), array.offset()))
        .reduce(|acc, buffer_and_offset| match (acc, buffer_and_offset) {
            ((None, _), (None, _)) => (None, 0),
            ((Some(buffer), offset), (None, _)) | ((None, _), (Some(buffer), offset)) => {
                (Some(buffer), offset)
            }
            ((Some(buffer_left), offset_left), (Some(buffer_right), offset_right)) => (
                Some(buffer_bin_and(
                    &buffer_left,
                    offset_left,
                    &buffer_right,
                    offset_right,
                    len_in_bits,
                )),
                0,
            ),
        })?;

    Some(buffer?.bit_slice(offset, len_in_bits))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_set_bits_aligned() {
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101,
        ];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 64;

        let expected_data: &[u8] = &[
            0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101, 0,
        ];
        let expected_null_count = 24;
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
    fn test_set_bits_unaligned_destination_start() {
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101,
        ];

        let destination_offset = 3;
        let source_offset = 0;

        let len = 64;

        let expected_data: &[u8] = &[
            0b00111000, 0b00101111, 0b11001101, 0b11011100, 0b01011110, 0b00011111,
            0b00111110, 0b00101111, 0b00000101, 0b00000000,
        ];
        let expected_null_count = 24;
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
    fn test_set_bits_unaligned_destination_end() {
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101,
        ];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 62;

        let expected_data: &[u8] = &[
            0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b00100101, 0,
        ];
        let expected_null_count = 23;
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
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
        ];

        let destination_offset = 3;
        let source_offset = 5;

        let len = 95;

        let expected_data: &[u8] = &[
            0b01111000, 0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000,
            0b01111001, 0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000,
            0b00000001,
        ];
        let expected_null_count = 35;
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

    fn make_data_with_null_bit_buffer(
        len: usize,
        offset: usize,
        null_bit_buffer: Option<Buffer>,
    ) -> Arc<ArrayData> {
        let buffer = Buffer::from(&vec![11; len + offset]);

        Arc::new(
            ArrayData::try_new(
                DataType::UInt8,
                len,
                null_bit_buffer,
                offset,
                vec![buffer],
                vec![],
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_combine_option_bitmap() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let some_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b01001010])));
        let inverse_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10110101])));
        let some_other_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b11010111])));
        assert_eq!(None, combine_option_bitmap(&[], 8));
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap], 8)
        );
        assert_eq!(
            None,
            combine_option_bitmap(&[&none_bitmap, &none_bitmap], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap, &none_bitmap], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b11010111])),
            combine_option_bitmap(&[&none_bitmap, &some_other_bitmap], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap, &some_bitmap], 8,)
        );
        assert_eq!(
            Some(Buffer::from([0b0])),
            combine_option_bitmap(&[&some_bitmap, &inverse_bitmap], 8,)
        );
        assert_eq!(
            Some(Buffer::from([0b01000010])),
            combine_option_bitmap(&[&some_bitmap, &some_other_bitmap, &none_bitmap], 8,)
        );
        assert_eq!(
            Some(Buffer::from([0b00001001])),
            combine_option_bitmap(
                &[
                    &some_bitmap.slice(3, 5),
                    &inverse_bitmap.slice(2, 5),
                    &some_other_bitmap.slice(1, 5)
                ],
                5,
            )
        );
    }

    #[test]
    fn test_combine_option_bitmap_with_offsets() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let bitmap0 =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10101010])));
        let bitmap1 =
            make_data_with_null_bit_buffer(8, 1, Some(Buffer::from([0b01010100, 0b1])));
        let bitmap2 =
            make_data_with_null_bit_buffer(8, 2, Some(Buffer::from([0b10101000, 0b10])));
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap2], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1, &none_bitmap], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&none_bitmap, &bitmap2], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap0, &bitmap1], 8)
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1, &bitmap2], 8)
        );
    }
}
