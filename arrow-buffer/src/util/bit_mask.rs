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

use crate::bit_chunk_iterator::BitChunks;
use crate::bit_util::{ceil, get_bit, set_bit};

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
        write_data[write_byte_index..write_byte_index + 8].copy_from_slice(&chunk.to_le_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bit_util::unset_bit;
    use rand::prelude::StdRng;
    use rand::{Fill, Rng, SeedableRng};
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
    fn set_bits_fuz() {
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
    impl<'a> Display for BinaryFormatter<'a> {
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
            let len = rng.gen_range(0..=200);

            // randomly pick where we will write to
            let offset_write_bits = rng.gen_range(0..=200);
            let offset_write_bytes = if offset_write_bits % 8 == 0 {
                offset_write_bits / 8
            } else {
                (offset_write_bits / 8) + 1
            };
            let extra_write_data_bytes = rng.gen_range(0..=5); // ensure 0 shows up often

            // randomly decide where we will read from
            let extra_read_data_bytes = rng.gen_range(0..=5); // make sure 0 shows up often
            let offset_read_bits = rng.gen_range(0..=200);
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
            self.data.try_fill(rng).unwrap();
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
}
