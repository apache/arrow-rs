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
//! unpacking here; the packing order is unchanged. UTL delta kernels sum lanes
//! in transposed order, then restore original order with a fully unrolled
//! permutation, following FastLanes and Vortex's separate untranspose kernels.

const FL_ORDER: [usize; 8] = [0, 4, 2, 6, 1, 5, 3, 7];
const VECTOR_SIZE: usize = 1024;

/// Position of a row/lane in the universal transposed layout. A delta lane
/// contains 32 consecutive original i32 values or 64 consecutive i64 values.
pub(crate) const fn utl_index(row: usize, lane: usize) -> usize {
    FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane
}

const fn original_index(bits: usize, index: usize) -> usize {
    let lane = index % (VECTOR_SIZE / bits);
    let sub_row = index / 128;
    let order = (index - sub_row * 128 - lane) / 16;
    lane * bits + FL_ORDER[order] * 8 + sub_row
}

/// Generate the entire permutation, not just its rows. This follows
/// vortex-fastlanes/src/transpose.rs and FastLanes' generated untranspose_i:
/// constant source/destination indices let LLVM use contiguous loads/stores
/// and register shuffles rather than a loop of gathers or scatters.
///
/// Mode 2's i32 lane numbering differs from the reference's universal
/// permutation. Keep its published ordering here; the i64 mapping is identical.
#[inline(never)]
fn untranspose<T: Copy, const BITS: usize>(input: &[T; VECTOR_SIZE], output: &mut [T]) {
    assert_eq!(output.len(), VECTOR_SIZE);
    seq_macro::seq!(I in 0..1024 {
        output[original_index(BITS, I)] = input[I];
    });
}

pub(crate) trait FastLanesBitPacking: Copy + Default {
    /// Pack one 1024-value vector and append its transposed bytes to `out`.
    fn pack_bytes(width: usize, input: &[Self], out: &mut Vec<u8>);

    /// Unpack one complete vector and add the frame with wrapping arithmetic.
    fn unpack_for_bytes(width: usize, input: &[u8], frame: Self, output: &mut [Self]);

    /// Unpack and sum independent UTL lanes, then restore original value order.
    fn unpack_delta_bytes(
        width: usize,
        input: &[u8],
        frame: Self,
        starts: &[u8],
        output: &mut [Self],
    );

    /// Sum already unpacked and patched UTL differences and restore value order.
    fn restore_delta(starts: &[u8], output: &mut [Self]);

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
                        #(W => $unpack::<W, false>(input, frame, &[], output),)*
                        _ => unreachable!("invalid FastLanes bit width {width}"),
                    }
                })
            }

            fn unpack_delta_bytes(width: usize, input: &[u8], frame: Self, starts: &[u8], output: &mut [Self]) {
                assert!(width <= $bits);
                assert_eq!(input.len(), VECTOR_SIZE * width / 8);
                assert_eq!(starts.len(), 128);
                assert_eq!(output.len(), VECTOR_SIZE);
                let mut transposed = [0; VECTOR_SIZE];
                seq_macro::seq!(W in 0..=$bits {
                    match width {
                        #(W => $unpack::<W, true>(input, frame, starts, &mut transposed),)*
                        _ => unreachable!("invalid FastLanes bit width {width}"),
                    }
                });
                untranspose::<$ty, $bits>(&transposed, output);
            }

            fn restore_delta(starts: &[u8], output: &mut [Self]) {
                assert_eq!(starts.len(), 128);
                assert_eq!(output.len(), VECTOR_SIZE);
                // Unroll each chain so the independent outer lane loop can be
                // vectorized. Patch values have already replaced differences.
                let mut transposed = [0; VECTOR_SIZE];
                for lane in 0..VECTOR_SIZE / $bits {
                    let mut acc = $load(starts, lane);
                    seq_macro::seq!(ROW in 0..$bits {
                        let index = utl_index(ROW, lane);
                        acc = acc.wrapping_add(output[index]);
                        transposed[index] = acc;
                    });
                }
                untranspose::<$ty, $bits>(&transposed, output);
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
        fn $unpack<const W: usize, const DELTA: bool>(input: &[u8], frame: $ty, starts: &[u8], output: &mut [$ty]) {
            const LANES: usize = VECTOR_SIZE / $bits;
            if W == 0 {
                if DELTA {
                    for lane in 0..LANES {
                        let mut acc = $load(starts, lane);
                        seq_macro::seq!(ROW in 0..$bits {
                            acc = acc.wrapping_add(frame);
                            output[utl_index(ROW, lane)] = acc;
                        });
                    }
                } else {
                    output.fill(frame);
                }
                return;
            }

            for lane in 0..LANES {
                let mut acc = if DELTA { $load(starts, lane) } else { 0 };
                if W == $bits {
                    seq_macro::seq!(ROW in 0..$bits {
                        let order = ROW / 8;
                        let sub_row = ROW % 8;
                        let index = FL_ORDER[order] * 16 + sub_row * 128 + lane;
                        let value = $load(input, LANES * ROW + lane).wrapping_add(frame);
                        if DELTA {
                            acc = acc.wrapping_add(value);
                            output[index] = acc;
                        } else {
                            output[index] = value;
                        }
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
                        let value = value.wrapping_add(frame);
                        if DELTA {
                            acc = acc.wrapping_add(value);
                            output[index] = acc;
                        } else {
                            output[index] = value;
                        }
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
    use fastlanes::{BitPacking, Delta, Transpose};

    #[test]
    fn utl_delta_kernels_match_independent_lane_sums_at_every_width() {
        macro_rules! check {
            ($ty:ty, $bits:literal) => {
                for width in 0..=$bits {
                    let mask = <$ty>::MAX >> ($bits - width).min($bits - 1);
                    let mask = if width == 0 { 0 } else { mask };
                    let frame = <$ty>::MAX - 7;
                    let mut residuals: [$ty; VECTOR_SIZE] = [0; VECTOR_SIZE];
                    let mut expected: [$ty; VECTOR_SIZE] = [0; VECTOR_SIZE];
                    let mut starts = Vec::new();
                    for lane in 0..VECTOR_SIZE / $bits {
                        let mut acc = (<$ty>::MAX - lane as $ty).wrapping_mul(1009);
                        starts.extend_from_slice(&acc.to_le_bytes());
                        for row in 0..$bits {
                            let residual =
                                ((lane * $bits + row) as $ty).wrapping_mul(0x9e37_79b9) & mask;
                            residuals[utl_index(row, lane)] = residual;
                            acc = acc.wrapping_add(residual).wrapping_add(frame);
                            expected[lane * $bits + row] = acc;
                        }
                    }
                    // Also check the running sums and permutation against the
                    // actual FastLanes implementation used by Vortex. Mode 2
                    // numbers i32 chains consecutively; the universal reference
                    // puts the two halves of each 64-value run 16 lanes apart.
                    let deltas = std::array::from_fn(|i| residuals[i].wrapping_add(frame));
                    let bases = std::array::from_fn(|i| {
                        let at = i * std::mem::size_of::<$ty>();
                        <$ty>::from_le_bytes(
                            starts[at..at + std::mem::size_of::<$ty>()]
                                .try_into()
                                .unwrap(),
                        )
                    });
                    let mut reference_transposed = [0; VECTOR_SIZE];
                    <$ty as Delta>::undelta::<{ VECTOR_SIZE / $bits }>(
                        &deltas,
                        &bases,
                        &mut reference_transposed,
                    );
                    let mut reference = [0; VECTOR_SIZE];
                    <$ty as Transpose>::untranspose(&reference_transposed, &mut reference);
                    for lane in 0..VECTOR_SIZE / $bits {
                        for row in 0..$bits {
                            assert_eq!(
                                expected[lane * $bits + row],
                                reference[(lane % 16) * 64 + (lane / 16) * $bits + row]
                            );
                        }
                    }
                    // The packed stream is produced by the independent upstream
                    // bitpacking oracle, not our packer.
                    let mut words: Vec<$ty> = vec![0; VECTOR_SIZE * width / $bits];
                    unsafe { <$ty as BitPacking>::unchecked_pack(width, &residuals, &mut words) };
                    let packed: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
                    for offset in 0..8 {
                        let mut unaligned = vec![0xa5; offset];
                        unaligned.extend_from_slice(&packed);
                        let mut unaligned_starts = vec![0xa5; offset];
                        unaligned_starts.extend_from_slice(&starts);
                        let mut out: [$ty; VECTOR_SIZE] = [0; VECTOR_SIZE];
                        <$ty>::unpack_delta_bytes(
                            width,
                            &unaligned[offset..],
                            frame,
                            &unaligned_starts[offset..],
                            &mut out,
                        );
                        assert_eq!(out, expected, "fused width {width}, offset {offset}");
                        <$ty>::unpack_for_bytes(width, &unaligned[offset..], frame, &mut out);
                        <$ty>::restore_delta(&unaligned_starts[offset..], &mut out);
                        assert_eq!(out, expected, "separate width {width}, offset {offset}");
                    }
                }
            };
        }
        check!(u32, 32);
        check!(u64, 64);
    }

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
