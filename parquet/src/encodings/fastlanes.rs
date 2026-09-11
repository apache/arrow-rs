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

//! Byte-oriented FastLanes bit packing.
//!
//! The upstream FastLanes API exposes packed data as typed words to ensure
//! alignment. PFOR puts variable-sized vectors after a 7-byte page header and
//! a byte-offset array, with no alignment padding. Packed values therefore have
//! no general `u32`/`u64` alignment guarantee. These kernels preserve the FastLanes
//! transposed wire order while loading and storing little-endian words directly
//! from potentially unaligned bytes.
//!
//! Adapted from the ALP experiment on `alp-benchmark-fastlanes`, commit
//! e97ae0dbb74dfcd4d4109afbe3dec78598de0900. Frame addition is fused into
//! unpacking here; the packing order is unchanged.

const FL_ORDER: [usize; 8] = [0, 4, 2, 6, 1, 5, 3, 7];
const VECTOR_SIZE: usize = 1024;

pub(crate) trait FastLanesBitPacking: Copy + Default {
    /// Pack one 1024-value vector and append its transposed bytes to `out`.
    fn pack_bytes(width: usize, input: &[Self], out: &mut Vec<u8>);

    /// Unpack one complete vector and add the frame with wrapping arithmetic.
    fn unpack_for_bytes(width: usize, input: &[u8], frame: Self, output: &mut [Self]);

    #[cfg(test)]
    fn unpack_bytes(width: usize, input: &[u8], output: &mut [Self]) {
        Self::unpack_for_bytes(width, input, Self::default(), output);
    }

    /// Read one value without unpacking the rest of its vector.
    #[cfg(test)]
    fn unpack_single_bytes(width: usize, input: &[u8], index: usize) -> Self;
}

macro_rules! impl_fastlanes_bitpacking {
    ($ty:ty, $bits:literal, $load:ident, $store:ident, $pack:ident, $unpack:ident) => {
        const _: () = assert!(VECTOR_SIZE % $bits == 0);

        impl FastLanesBitPacking for $ty {
            fn pack_bytes(width: usize, input: &[Self], out: &mut Vec<u8>) {
                assert!(width <= $bits);
                assert_eq!(input.len(), VECTOR_SIZE);

                seq_macro::seq!(W in 0..=$bits {
                    match width {
                        #(W => $pack::<W>(input, out),)*
                        _ => unreachable!("invalid FastLanes bit width {width}"),
                    }
                })
            }

            fn unpack_for_bytes(width: usize, input: &[u8], frame: Self, output: &mut [Self]) {
                assert!(width <= $bits);
                assert_eq!(input.len(), VECTOR_SIZE * width / 8);
                assert_eq!(output.len(), VECTOR_SIZE);

                seq_macro::seq!(W in 0..=$bits {
                    match width {
                        #(W => $unpack::<W>(input, frame, output),)*
                        _ => unreachable!("invalid FastLanes bit width {width}"),
                    }
                })
            }

            #[cfg(test)]
            fn unpack_single_bytes(width: usize, input: &[u8], index: usize) -> Self {
                assert!(width <= $bits);
                assert_eq!(input.len(), VECTOR_SIZE * width / 8);
                assert!(index < VECTOR_SIZE);
                if width == 0 {
                    return 0;
                }

                const LANES: usize = VECTOR_SIZE / $bits;
                let lane = index % LANES;
                let sub_row = index / 128;
                let order = (index - sub_row * 128 - lane) / 16;
                let row = FL_ORDER[order] * 8 + sub_row;

                if width == $bits {
                    return $load(input, LANES * row + lane);
                }

                let mask = (<$ty>::from(1u8) << width) - 1;
                let start_bit = row * width;
                let start_word = start_bit / $bits;
                let lo_shift = start_bit % $bits;
                let remaining_bits = $bits - lo_shift;
                let lo = $load(input, LANES * start_word + lane) >> lo_shift;
                if remaining_bits >= width {
                    lo & mask
                } else {
                    let hi = $load(input, LANES * (start_word + 1) + lane)
                        << remaining_bits;
                    (lo | hi) & mask
                }
            }
        }

        #[inline(always)]
        fn $load(input: &[u8], word: usize) -> $ty {
            let offset = word * std::mem::size_of::<$ty>();
            debug_assert!(offset + std::mem::size_of::<$ty>() <= input.len());
            // SAFETY: the caller validates the complete packed byte length. The
            // pointer may be unaligned, which is exactly why `read_unaligned`
            // is used. Every bit pattern is valid for an unsigned integer.
            unsafe {
                input.as_ptr().add(offset).cast::<$ty>()
                    .read_unaligned()
                    .to_le()
            }
        }

        #[inline(always)]
        fn $store(output: &mut [u8], word: usize, value: $ty) {
            let offset = word * std::mem::size_of::<$ty>();
            debug_assert!(offset + std::mem::size_of::<$ty>() <= output.len());
            // SAFETY: the caller sized the packed byte output in advance. The
            // destination may be unaligned, so use `write_unaligned`.
            unsafe {
                output.as_mut_ptr().add(offset).cast::<$ty>().write_unaligned(value.to_le());
            }
        }

        #[inline(never)]
        #[expect(unused_assignments, reason = "The final unrolled row does not consume the next accumulator")]
        fn $pack<const W: usize>(input: &[$ty], out: &mut Vec<u8>) {
            const LANES: usize = VECTOR_SIZE / $bits;
            let start = out.len();
            out.resize(start + VECTOR_SIZE * W / 8, 0);
            let packed = &mut out[start..];

            if W == 0 {
                return;
            }

            for lane in 0..LANES {
                if W == $bits {
                    seq_macro::seq!(ROW in 0..$bits {
                        let order = ROW / 8;
                        let sub_row = ROW % 8;
                        let index = FL_ORDER[order] * 16 + sub_row * 128 + lane;
                        $store(packed, LANES * ROW + lane, input[index]);
                    });
                } else {
                    let mask: $ty = (<$ty>::from(1u8) << W) - 1;
                    let mut tmp: $ty = 0;
                    seq_macro::seq!(ROW in 0..$bits {
                        let order = ROW / 8;
                        let sub_row = ROW % 8;
                        let index = FL_ORDER[order] * 16 + sub_row * 128 + lane;
                        let src = input[index] & mask;
                        if ROW == 0 {
                            tmp = src;
                        } else {
                            tmp |= src << ((ROW * W) % $bits);
                        }

                        let current_word = ROW * W / $bits;
                        let next_word = (ROW + 1) * W / $bits;
                        if next_word > current_word {
                            $store(packed, LANES * current_word + lane, tmp);
                            let remaining_bits = ((ROW + 1) * W) % $bits;
                            tmp = src >> (W - remaining_bits);
                        }
                    });
                }
            }
        }

        #[inline(never)]
        fn $unpack<const W: usize>(input: &[u8], frame: $ty, output: &mut [$ty]) {
            const LANES: usize = VECTOR_SIZE / $bits;
            if W == 0 {
                output.fill(frame);
                return;
            }

            for lane in 0..LANES {
                if W == $bits {
                    seq_macro::seq!(ROW in 0..$bits {
                        let order = ROW / 8;
                        let sub_row = ROW % 8;
                        let index = FL_ORDER[order] * 16 + sub_row * 128 + lane;
                        output[index] = $load(input, LANES * ROW + lane).wrapping_add(frame);
                    });
                } else {
                    let mask = |width: usize| (<$ty>::from(1u8) << width) - 1;
                    let mut src = $load(input, lane);
                    seq_macro::seq!(ROW in 0..$bits {
                        let current_word = ROW * W / $bits;
                        let next_word = (ROW + 1) * W / $bits;
                        let shift = ROW * W % $bits;
                        let value = if next_word > current_word {
                            let remaining_bits = (ROW + 1) * W % $bits;
                            let current_bits = W - remaining_bits;
                            let mut tmp = (src >> shift) & mask(current_bits);
                            if next_word < W {
                                src = $load(input, LANES * next_word + lane);
                                tmp |= (src & mask(remaining_bits)) << current_bits;
                            }
                            tmp
                        } else {
                            (src >> shift) & mask(W)
                        };

                        let order = ROW / 8;
                        let sub_row = ROW % 8;
                        let index = FL_ORDER[order] * 16 + sub_row * 128 + lane;
                        output[index] = value.wrapping_add(frame);
                    });
                }
            }
        }
    };
}

impl_fastlanes_bitpacking!(u32, 32, load_u32, store_u32, pack_impl_u32, unpack_impl_u32);
impl_fastlanes_bitpacking!(u64, 64, load_u64, store_u64, pack_impl_u64, unpack_impl_u64);

#[cfg(test)]
mod tests {
    use super::*;
    use fastlanes::BitPacking;

    fn check_u32(width: usize) {
        let input: Vec<u32> = (0..VECTOR_SIZE)
            .map(|i| (i as u32).wrapping_mul(0x9e37_79b9))
            .collect();
        let mut expected_words = vec![0u32; VECTOR_SIZE * width / 32];
        unsafe { <u32 as BitPacking>::unchecked_pack(width, &input, &mut expected_words) };
        let expected: Vec<u8> = expected_words
            .iter()
            .flat_map(|word| word.to_le_bytes())
            .collect();

        let mut packed = Vec::new();
        u32::pack_bytes(width, &input, &mut packed);
        assert_eq!(packed, expected, "u32 pack width {width}");

        let mut output = vec![0; VECTOR_SIZE];
        u32::unpack_bytes(width, &packed, &mut output);
        let mask = if width == 32 {
            u32::MAX
        } else {
            ((1u64 << width) - 1) as u32
        };
        for (index, (&actual, &original)) in output.iter().zip(&input).enumerate() {
            assert_eq!(actual, original & mask, "u32 width {width}, index {index}");
            assert_eq!(
                u32::unpack_single_bytes(width, &packed, index),
                actual,
                "u32 point width {width}, index {index}"
            );
        }
        // Exercise every byte alignment and modular addition, including the
        // zero- and full-width specializations. Compare with the source values,
        // not with a second invocation of our decoder.
        for offset in 0..8 {
            let mut unaligned = vec![0x5a; offset];
            u32::pack_bytes(width, &input, &mut unaligned);
            let frame = u32::MAX - 7;
            u32::unpack_for_bytes(width, &unaligned[offset..], frame, &mut output);
            for (actual, original) in output.iter().zip(&input) {
                assert_eq!(*actual, (original & mask).wrapping_add(frame));
            }
        }
    }

    fn check_u64(width: usize) {
        let input: Vec<u64> = (0..VECTOR_SIZE)
            .map(|i| (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .collect();
        let mut expected_words = vec![0u64; VECTOR_SIZE * width / 64];
        unsafe { <u64 as BitPacking>::unchecked_pack(width, &input, &mut expected_words) };
        let expected: Vec<u8> = expected_words
            .iter()
            .flat_map(|word| word.to_le_bytes())
            .collect();

        let mut packed = Vec::new();
        u64::pack_bytes(width, &input, &mut packed);
        assert_eq!(packed, expected, "u64 pack width {width}");

        let mut output = vec![0; VECTOR_SIZE];
        u64::unpack_bytes(width, &packed, &mut output);
        let mask = if width == 64 {
            u64::MAX
        } else {
            ((1u128 << width) - 1) as u64
        };
        for (index, (&actual, &original)) in output.iter().zip(&input).enumerate() {
            assert_eq!(actual, original & mask, "u64 width {width}, index {index}");
            assert_eq!(
                u64::unpack_single_bytes(width, &packed, index),
                actual,
                "u64 point width {width}, index {index}"
            );
        }
        for offset in 0..8 {
            let mut unaligned = vec![0x5a; offset];
            u64::pack_bytes(width, &input, &mut unaligned);
            let frame = u64::MAX - 7;
            u64::unpack_for_bytes(width, &unaligned[offset..], frame, &mut output);
            for (actual, original) in output.iter().zip(&input) {
                assert_eq!(*actual, (original & mask).wrapping_add(frame));
            }
        }
    }

    #[test]
    fn byte_kernels_match_fastlanes_wire_format() {
        for width in 0..=32 {
            check_u32(width);
        }
        for width in 0..=64 {
            check_u64(width);
        }
    }

    #[test]
    fn decoding_does_not_require_alignment() {
        let input: Vec<u64> = (0..VECTOR_SIZE).map(|i| i as u64).collect();
        let mut packed = vec![0xff];
        u64::pack_bytes(11, &input, &mut packed);
        let mut output = vec![0; VECTOR_SIZE];
        u64::unpack_bytes(11, &packed[1..], &mut output);
        assert_eq!(output, input);
    }
}
