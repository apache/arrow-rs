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

use crate::bit_util::ceil;

/// Sets all bits on `write_data` in the range `[offset_write..offset_write+len]` to be equal to the
/// bits in `data` in the range `[offset_read..offset_read+len]`
/// returns the number of `0` bits `data[offset_read..offset_read+len]`
/// `offset_write`, `offset_read`, and `len` are in terms of bits
pub fn set_bits(
    write_data: &mut [u8],
    data: &[u8],
    offset_write: usize,
    offset_read: usize,
    len: usize,
) -> usize {
    assert!(offset_write + len <= write_data.len() * 8);
    assert!(offset_read + len <= data.len() * 8);
    let mut null_count = 0;
    let mut acc = 0;
    while len > acc {
        // SAFETY: the arguments to `set_upto_64bits` are within the valid range because
        // (offset_write + acc) + (len - acc) == offset_write + len <= write_data.len() * 8
        // (offset_read + acc) + (len - acc) == offset_read + len <= data.len() * 8
        let (n, len_set) = unsafe {
            set_upto_64bits(
                write_data,
                data,
                offset_write + acc,
                offset_read + acc,
                len - acc,
            )
        };
        null_count += n;
        acc += len_set;
    }

    null_count
}

/// Similar to `set_bits` but sets only upto 64 bits, actual number of bits set may vary.
/// Returns a pair of the number of `0` bits and the number of bits set
///
/// # Safety
/// The caller must ensure all arguments are within the valid range.
#[inline]
unsafe fn set_upto_64bits(
    write_data: &mut [u8],
    data: &[u8],
    offset_write: usize,
    offset_read: usize,
    len: usize,
) -> (usize, usize) {
    let read_byte = offset_read / 8;
    let read_shift = offset_read % 8;
    let write_byte = offset_write / 8;
    let write_shift = offset_write % 8;

    if len >= 64 {
        let chunk = unsafe { (data.as_ptr().add(read_byte) as *const u64).read_unaligned() };
        if read_shift == 0 {
            if write_shift == 0 {
                // no shifting necessary
                let len = 64;
                let null_count = chunk.count_zeros() as usize;
                unsafe { write_u64_bytes(write_data, write_byte, chunk) };
                (null_count, len)
            } else {
                // only write shifting necessary
                let len = 64 - write_shift;
                let chunk = chunk << write_shift;
                let null_count = len - chunk.count_ones() as usize;
                unsafe { or_write_u64_bytes(write_data, write_byte, chunk) };
                (null_count, len)
            }
        } else if write_shift == 0 {
            // only read shifting necessary
            let len = 64 - 8; // 56 bits so the next set_upto_64bits call will see write_shift == 0
            let chunk = (chunk >> read_shift) & 0x00FFFFFFFFFFFFFF; // 56 bits mask
            let null_count = len - chunk.count_ones() as usize;
            unsafe { write_u64_bytes(write_data, write_byte, chunk) };
            (null_count, len)
        } else {
            let len = 64 - std::cmp::max(read_shift, write_shift);
            let chunk = (chunk >> read_shift) << write_shift;
            let null_count = len - chunk.count_ones() as usize;
            unsafe { or_write_u64_bytes(write_data, write_byte, chunk) };
            (null_count, len)
        }
    } else if len == 1 {
        let byte_chunk = (unsafe { data.get_unchecked(read_byte) } >> read_shift) & 1;
        unsafe { *write_data.get_unchecked_mut(write_byte) |= byte_chunk << write_shift };
        ((byte_chunk ^ 1) as usize, 1)
    } else {
        let len = std::cmp::min(len, 64 - std::cmp::max(read_shift, write_shift));
        let bytes = ceil(len + read_shift, 8);
        // SAFETY: the args of `read_bytes_to_u64` are valid as read_byte + bytes <= data.len()
        let chunk = unsafe { read_bytes_to_u64(data, read_byte, bytes) };
        let mask = u64::MAX >> (64 - len);
        let chunk = (chunk >> read_shift) & mask; // masking to read `len` bits only
        let chunk = chunk << write_shift; // shifting back to align with `write_data`
        let null_count = len - chunk.count_ones() as usize;
        let bytes = ceil(len + write_shift, 8);
        for (i, c) in chunk.to_le_bytes().iter().enumerate().take(bytes) {
            unsafe { *write_data.get_unchecked_mut(write_byte + i) |= c };
        }
        (null_count, len)
    }
}

/// # Safety
/// The caller must ensure all arguments are within the valid range.
#[inline]
unsafe fn read_bytes_to_u64(data: &[u8], offset: usize, count: usize) -> u64 {
    debug_assert!(count <= 8);
    let mut tmp = std::mem::MaybeUninit::<u64>::new(0);
    let src = data.as_ptr().add(offset);
    unsafe {
        std::ptr::copy_nonoverlapping(src, tmp.as_mut_ptr() as *mut u8, count);
        tmp.assume_init()
    }
}

/// # Safety
/// The caller must ensure `data` has `offset..(offset + 8)` range
#[inline]
unsafe fn write_u64_bytes(data: &mut [u8], offset: usize, chunk: u64) {
    let ptr = data.as_mut_ptr().add(offset) as *mut u64;
    ptr.write_unaligned(chunk);
}

/// Similar to `write_u64_bytes`, but this method ORs the offset addressed `data` and `chunk`
/// instead of overwriting
///
/// # Safety
/// The caller must ensure `data` has `offset..(offset + 8)` range
#[inline]
unsafe fn or_write_u64_bytes(data: &mut [u8], offset: usize, chunk: u64) {
    let ptr = data.as_mut_ptr().add(offset);
    let chunk = chunk | (*ptr) as u64;
    (ptr as *mut u64).write_unaligned(chunk);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_bits_aligned() {
        let mut destination: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let source: &[u8] = &[
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b10100101,
        ];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 64;

        let expected_data: &[u8] = &[
            0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b10100101, 0,
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
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b10100101,
        ];

        let destination_offset = 3;
        let source_offset = 0;

        let len = 64;

        let expected_data: &[u8] = &[
            0b00111000, 0b00101111, 0b11001101, 0b11011100, 0b01011110, 0b00011111, 0b00111110,
            0b00101111, 0b00000101, 0b00000000,
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
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b10100101,
        ];

        let destination_offset = 8;
        let source_offset = 0;

        let len = 62;

        let expected_data: &[u8] = &[
            0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b00100101, 0,
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
            0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
            0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111, 0b10100101,
            0b10011001, 0b11011011, 0b11101011, 0b11000011,
        ];

        let destination_offset = 3;
        let source_offset = 5;

        let len = 95;

        let expected_data: &[u8] = &[
            0b01111000, 0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000, 0b01111001,
            0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000, 0b00000001,
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

    #[test]
    fn test_set_upto_64bits() {
        // len >= 64
        let write_data: &mut [u8] = &mut [0; 9];
        let data: &[u8] = &[
            0b00000001, 0b00000001, 0b00000001, 0b00000001, 0b00000001, 0b00000001, 0b00000001,
            0b00000001, 0b00000001,
        ];
        let offset_write = 1;
        let offset_read = 0;
        let len = 65;
        let (n, len_set) =
            unsafe { set_upto_64bits(write_data, data, offset_write, offset_read, len) };
        assert_eq!(n, 55);
        assert_eq!(len_set, 63);
        assert_eq!(
            write_data,
            &[
                0b00000010, 0b00000010, 0b00000010, 0b00000010, 0b00000010, 0b00000010, 0b00000010,
                0b00000010, 0b00000000
            ]
        );

        // len = 1
        let write_data: &mut [u8] = &mut [0b00000000];
        let data: &[u8] = &[0b00000001];
        let offset_write = 1;
        let offset_read = 0;
        let len = 1;
        let (n, len_set) =
            unsafe { set_upto_64bits(write_data, data, offset_write, offset_read, len) };
        assert_eq!(n, 0);
        assert_eq!(len_set, 1);
        assert_eq!(write_data, &[0b00000010]);
    }
}
