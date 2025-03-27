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

/// Util function to set bits in a slice of bytes.
///
/// This will sets all bits on `write_data` in the range `[offset_write..offset_write+len]`
/// to be equal to the bits in `data` in the range `[offset_read..offset_read+len]`
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
/// The caller must ensure `data` has `offset..(offset + 8)` range, and `count <= 8`.
#[inline]
unsafe fn read_bytes_to_u64(data: &[u8], offset: usize, count: usize) -> u64 {
    debug_assert!(count <= 8);
    let mut tmp: u64 = 0;
    let src = data.as_ptr().add(offset);
    unsafe {
        std::ptr::copy_nonoverlapping(src, &mut tmp as *mut _ as *mut u8, count);
    }
    tmp
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
    use crate::bit_util::{get_bit, set_bit, unset_bit};
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng, TryRngCore};
    use std::fmt::Display;

    #[test]
    fn test_set_bits_aligned() {
        SetBitsTest {
            write_data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            data: vec![
                0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
                0b10100101,
            ],
            offset_write: 8,
            offset_read: 0,
            len: 64,
            expected_data: vec![
                0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
                0b11100111, 0b10100101, 0,
            ],
            expected_null_count: 24,
        }
        .verify();
    }

    #[test]
    fn test_set_bits_unaligned_destination_start() {
        SetBitsTest {
            write_data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            data: vec![
                0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
                0b10100101,
            ],
            offset_write: 3,
            offset_read: 0,
            len: 64,
            expected_data: vec![
                0b00111000, 0b00101111, 0b11001101, 0b11011100, 0b01011110, 0b00011111, 0b00111110,
                0b00101111, 0b00000101, 0b00000000,
            ],
            expected_null_count: 24,
        }
        .verify();
    }

    #[test]
    fn test_set_bits_unaligned_destination_end() {
        SetBitsTest {
            write_data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            data: vec![
                0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
                0b10100101,
            ],
            offset_write: 8,
            offset_read: 0,
            len: 62,
            expected_data: vec![
                0, 0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011,
                0b11100111, 0b00100101, 0,
            ],
            expected_null_count: 23,
        }
        .verify();
    }

    #[test]
    fn test_set_bits_unaligned() {
        SetBitsTest {
            write_data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            data: vec![
                0b11100111, 0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111,
                0b10100101, 0b10011001, 0b11011011, 0b11101011, 0b11000011, 0b11100111, 0b10100101,
                0b10011001, 0b11011011, 0b11101011, 0b11000011,
            ],
            offset_write: 3,
            offset_read: 5,
            len: 95,
            expected_data: vec![
                0b01111000, 0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000, 0b01111001,
                0b01101001, 0b11100110, 0b11110110, 0b11111010, 0b11110000, 0b00000001,
            ],
            expected_null_count: 35,
        }
        .verify();
    }

    #[test]
    fn set_bits_fuzz() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = SetBitsTest::new();
        for _ in 0..100 {
            data.regen(&mut rng);
            data.verify();
        }
    }

    #[derive(Debug, Default)]
    struct SetBitsTest {
        /// target write data
        write_data: Vec<u8>,
        /// source data
        data: Vec<u8>,
        offset_write: usize,
        offset_read: usize,
        len: usize,
        /// the expected contents of write_data after the test
        expected_data: Vec<u8>,
        /// the expected number of nulls copied at the end of the test
        expected_null_count: usize,
    }

    /// prints a byte slice as a binary string like "01010101 10101010"
    struct BinaryFormatter<'a>(&'a [u8]);
    impl Display for BinaryFormatter<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for byte in self.0 {
                write!(f, "{:08b} ", byte)?;
            }
            write!(f, " ")?;
            Ok(())
        }
    }

    impl Display for SetBitsTest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "SetBitsTest {{")?;
            writeln!(f, "  write_data:    {}", BinaryFormatter(&self.write_data))?;
            writeln!(f, "  data:          {}", BinaryFormatter(&self.data))?;
            writeln!(
                f,
                "  expected_data: {}",
                BinaryFormatter(&self.expected_data)
            )?;
            writeln!(f, "  offset_write: {}", self.offset_write)?;
            writeln!(f, "  offset_read: {}", self.offset_read)?;
            writeln!(f, "  len: {}", self.len)?;
            writeln!(f, "  expected_null_count: {}", self.expected_null_count)?;
            writeln!(f, "}}")
        }
    }

    impl SetBitsTest {
        /// create a new instance of FuzzData
        fn new() -> Self {
            Self::default()
        }

        /// Update this instance's fields with randomly selected values and expected data
        fn regen(&mut self, rng: &mut StdRng) {
            //  (read) data
            // ------------------+-----------------+-------
            // .. offset_read .. | data            | ...
            // ------------------+-----------------+-------

            // Write data
            // -------------------+-----------------+-------
            // .. offset_write .. | (data to write) | ...
            // -------------------+-----------------+-------

            // length of data to copy
            let len = rng.random_range(0..=200);

            // randomly pick where we will write to
            let offset_write_bits = rng.random_range(0..=200);
            let offset_write_bytes = if offset_write_bits % 8 == 0 {
                offset_write_bits / 8
            } else {
                (offset_write_bits / 8) + 1
            };
            let extra_write_data_bytes = rng.random_range(0..=5); // ensure 0 shows up often

            // randomly decide where we will read from
            let extra_read_data_bytes = rng.random_range(0..=5); // make sure 0 shows up often
            let offset_read_bits = rng.random_range(0..=200);
            let offset_read_bytes = if offset_read_bits % 8 != 0 {
                (offset_read_bits / 8) + 1
            } else {
                offset_read_bits / 8
            };

            // create space for writing
            self.write_data.clear();
            self.write_data
                .resize(offset_write_bytes + len + extra_write_data_bytes, 0);

            // interestingly set_bits seems to assume the output is already zeroed
            // the fuzz tests fail when this is uncommented
            //self.write_data.try_fill(rng).unwrap();
            self.offset_write = offset_write_bits;

            // make source data
            self.data
                .resize(offset_read_bytes + len + extra_read_data_bytes, 0);
            // fill source data with random bytes
            rng.try_fill_bytes(self.data.as_mut_slice()).unwrap();
            self.offset_read = offset_read_bits;

            self.len = len;

            // generated expectated output (not efficient)
            self.expected_data.resize(self.write_data.len(), 0);
            self.expected_data.copy_from_slice(&self.write_data);

            self.expected_null_count = 0;
            for i in 0..self.len {
                let bit = get_bit(&self.data, self.offset_read + i);
                if bit {
                    set_bit(&mut self.expected_data, self.offset_write + i);
                } else {
                    unset_bit(&mut self.expected_data, self.offset_write + i);
                    self.expected_null_count += 1;
                }
            }
        }

        /// call set_bits with the given parameters and compare with the expected output
        fn verify(&self) {
            // call set_bits and compare
            let mut actual = self.write_data.to_vec();
            let null_count = set_bits(
                &mut actual,
                &self.data,
                self.offset_write,
                self.offset_read,
                self.len,
            );

            assert_eq!(actual, self.expected_data, "self: {}", self);
            assert_eq!(null_count, self.expected_null_count, "self: {}", self);
        }
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
