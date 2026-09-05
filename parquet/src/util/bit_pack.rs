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

//! Vectorised bit-packing utilities

/// Macro that generates an unpack function taking the number of bits as a const generic
macro_rules! unpack_impl {
    ($t:ty, $bytes:literal, $bits:tt) => {
        pub fn unpack<const NUM_BITS: usize>(input: &[u8], output: &mut [$t; $bits]) {
            if NUM_BITS == 0 {
                for out in output {
                    *out = 0;
                }
                return;
            }

            assert!(NUM_BITS <= $bytes * 8);

            let mask = match NUM_BITS {
                $bits => <$t>::MAX,
                _ => ((1 << NUM_BITS) - 1),
            };

            assert!(input.len() >= NUM_BITS * $bytes);

            let r = |output_idx: usize| {
                <$t>::from_le_bytes(
                    input[output_idx * $bytes..output_idx * $bytes + $bytes]
                        .try_into()
                        .unwrap(),
                )
            };

            seq_macro::seq!(i in 0..$bits {
                let start_bit = i * NUM_BITS;
                let end_bit = start_bit + NUM_BITS;

                let start_bit_offset = start_bit % $bits;
                let end_bit_offset = end_bit % $bits;
                let start_byte = start_bit / $bits;
                let end_byte = end_bit / $bits;
                if start_byte != end_byte && end_bit_offset != 0 {
                    let val = r(start_byte);
                    let a = val >> start_bit_offset;
                    let val = r(end_byte);
                    let b = val << (NUM_BITS - end_bit_offset);

                    output[i] = a | (b & mask);
                } else {
                    let val = r(start_byte);
                    output[i] = (val >> start_bit_offset) & mask;
                }
            });
        }
    };
}

/// Macro that generates unpack functions that accept num_bits as a parameter
macro_rules! unpack {
    ($name:ident, $t:ty, $bytes:literal, $bits:tt) => {
        mod $name {
            unpack_impl!($t, $bytes, $bits);
        }

        /// Unpack packed `input` into `output` with a bit width of `num_bits`
        pub fn $name(input: &[u8], output: &mut [$t; $bits], num_bits: usize) {
            // This will get optimised into a jump table
            seq_macro::seq!(i in 0..=$bits {
                if i == num_bits {
                    return $name::unpack::<i>(input, output);
                }
            });
            unreachable!("invalid num_bits {}", num_bits);
        }
    };
}

unpack!(unpack8, u8, 1, 8);
unpack!(unpack16, u16, 2, 16);
unpack!(unpack32, u32, 4, 32);
unpack!(unpack64, u64, 8, 64);

/// Macro that generates a pack function taking the number of bits as a const generic
macro_rules! pack_impl {
    ($t:ty, $bytes:literal, $bits:tt) => {
        pub fn pack<const NUM_BITS: usize>(input: &[$t; $bits], output: &mut [u8]) {
            if NUM_BITS == 0 {
                return;
            }

            assert!(NUM_BITS <= $bytes * 8);
            assert!(output.len() >= NUM_BITS * $bytes);

            let mask = match NUM_BITS {
                $bits => <$t>::MAX,
                _ => ((1 << NUM_BITS) - 1),
            };

            // Accumulate into locals so the packed words stay in registers. Only the
            // first NUM_BITS entries are used, `[$t; NUM_BITS]` needs generic_const_exprs
            let mut words = [0; $bits];

            seq_macro::seq!(i in 0..$bits {
                let value = input[i] & mask;

                let start_bit = i * NUM_BITS;
                let end_bit = start_bit + NUM_BITS;

                let start_bit_offset = start_bit % $bits;
                let end_bit_offset = end_bit % $bits;
                let start_word = start_bit / $bits;
                let end_word = end_bit / $bits;

                words[start_word] |= value << start_bit_offset;
                if start_word != end_word && end_bit_offset != 0 {
                    words[end_word] |= value >> (NUM_BITS - end_bit_offset);
                }
            });

            seq_macro::seq!(w in 0..$bits {
                if w < NUM_BITS {
                    output[w * $bytes..(w + 1) * $bytes].copy_from_slice(&words[w].to_le_bytes());
                }
            });
        }
    };
}

/// Macro that generates pack functions that accept num_bits as a parameter
macro_rules! pack {
    ($name:ident, $t:ty, $bytes:literal, $bits:tt) => {
        mod $name {
            pack_impl!($t, $bytes, $bits);
        }

        /// Pack `input` into `output` with a bit width of `num_bits`
        ///
        /// Only the `num_bits` least significant bits of each value are written,
        /// and `output` must contain at least `num_bits * size_of::<T>()` bytes
        pub fn $name(input: &[$t; $bits], output: &mut [u8], num_bits: usize) {
            // This will get optimised into a jump table
            seq_macro::seq!(i in 0..=$bits {
                if i == num_bits {
                    return $name::pack::<i>(input, output);
                }
            });
            unreachable!("invalid num_bits {}", num_bits);
        }
    };
}

pack!(pack8, u8, 1, 8);
pack!(pack16, u16, 2, 16);
pack!(pack32, u32, 4, 32);
pack!(pack64, u64, 8, 64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let input = [0xFF; 4096];

        for i in 0..=8 {
            let mut output = [0; 8];
            unpack8(&input, &mut output, i);
            for (idx, out) in output.iter().enumerate() {
                assert_eq!(out.trailing_ones() as usize, i, "out[{idx}] = {out}");
            }
        }

        for i in 0..=16 {
            let mut output = [0; 16];
            unpack16(&input, &mut output, i);
            for (idx, out) in output.iter().enumerate() {
                assert_eq!(out.trailing_ones() as usize, i, "out[{idx}] = {out}");
            }
        }

        for i in 0..=32 {
            let mut output = [0; 32];
            unpack32(&input, &mut output, i);
            for (idx, out) in output.iter().enumerate() {
                assert_eq!(out.trailing_ones() as usize, i, "out[{idx}] = {out}");
            }
        }

        for i in 0..=64 {
            let mut output = [0; 64];
            unpack64(&input, &mut output, i);
            for (idx, out) in output.iter().enumerate() {
                assert_eq!(out.trailing_ones() as usize, i, "out[{idx}] = {out}");
            }
        }
    }

    #[test]
    fn test_pack_all_ones() {
        // Packing all-ones values must set every bit of the packed block and
        // touch nothing beyond it
        let mut output = [0u8; 4096];

        for i in 0..=8 {
            output.fill(0);
            pack8(&[u8::MAX; 8], &mut output, i);
            assert!(output[..i].iter().all(|&b| b == u8::MAX), "num_bits = {i}");
            assert!(output[i..].iter().all(|&b| b == 0), "num_bits = {i}");
        }

        for i in 0..=16 {
            output.fill(0);
            pack16(&[u16::MAX; 16], &mut output, i);
            assert!(
                output[..2 * i].iter().all(|&b| b == u8::MAX),
                "num_bits = {i}"
            );
            assert!(output[2 * i..].iter().all(|&b| b == 0), "num_bits = {i}");
        }

        for i in 0..=32 {
            output.fill(0);
            pack32(&[u32::MAX; 32], &mut output, i);
            assert!(
                output[..4 * i].iter().all(|&b| b == u8::MAX),
                "num_bits = {i}"
            );
            assert!(output[4 * i..].iter().all(|&b| b == 0), "num_bits = {i}");
        }

        for i in 0..=64 {
            output.fill(0);
            pack64(&[u64::MAX; 64], &mut output, i);
            assert!(
                output[..8 * i].iter().all(|&b| b == u8::MAX),
                "num_bits = {i}"
            );
            assert!(output[8 * i..].iter().all(|&b| b == 0), "num_bits = {i}");
        }
    }

    #[test]
    fn test_pack_round_trip() {
        use crate::util::test_common::rand_gen::random_numbers;

        // Values are deliberately not masked, pack must ignore the high bits
        for i in 0..=8 {
            let input: [u8; 8] = random_numbers(8).try_into().unwrap();
            let mut packed = vec![0u8; i];
            pack8(&input, &mut packed, i);
            let mut output = [0; 8];
            unpack8(&packed, &mut output, i);
            let mask = ((1u16 << i) - 1) as u8;
            for (idx, (&v, &out)) in input.iter().zip(output.iter()).enumerate() {
                assert_eq!(v & mask, out, "num_bits = {i}, index = {idx}");
            }
        }

        for i in 0..=16 {
            let input: [u16; 16] = random_numbers(16).try_into().unwrap();
            let mut packed = vec![0u8; 2 * i];
            pack16(&input, &mut packed, i);
            let mut output = [0; 16];
            unpack16(&packed, &mut output, i);
            let mask = ((1u32 << i) - 1) as u16;
            for (idx, (&v, &out)) in input.iter().zip(output.iter()).enumerate() {
                assert_eq!(v & mask, out, "num_bits = {i}, index = {idx}");
            }
        }

        for i in 0..=32 {
            let input: [u32; 32] = random_numbers(32).try_into().unwrap();
            let mut packed = vec![0u8; 4 * i];
            pack32(&input, &mut packed, i);
            let mut output = [0; 32];
            unpack32(&packed, &mut output, i);
            let mask = ((1u64 << i) - 1) as u32;
            for (idx, (&v, &out)) in input.iter().zip(output.iter()).enumerate() {
                assert_eq!(v & mask, out, "num_bits = {i}, index = {idx}");
            }
        }

        for i in 0..=64 {
            let input: [u64; 64] = random_numbers(64).try_into().unwrap();
            let mut packed = vec![0u8; 8 * i];
            pack64(&input, &mut packed, i);
            let mut output = [0; 64];
            unpack64(&packed, &mut output, i);
            let mask = ((1u128 << i) - 1) as u64;
            for (idx, (&v, &out)) in input.iter().zip(output.iter()).enumerate() {
                assert_eq!(v & mask, out, "num_bits = {i}, index = {idx}");
            }
        }
    }
}
