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

mod unpack32 {
    unpack_impl!(u32, 4, 32);
}

/// Unpack packed `input` into `output` with a bit width of `num_bits`
///
/// On x86_64 with AVX2, bit widths 1..=24 use a hand-vectorised path
/// (gather + variable shift + mask). All other bit widths — and all
/// non-x86_64 targets — fall through to the autovectorised scalar
/// implementation. Bit widths 1..=24 cover dictionaries up to ~16M
/// entries, which is the whole range we care about for low-cardinality
/// parquet dictionary reads.
pub fn unpack32(input: &[u8], output: &mut [u32; 32], num_bits: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if (1..=24).contains(&num_bits) && x86::avx2_supported() {
            // SAFETY: AVX2 availability confirmed above.
            unsafe {
                seq_macro::seq!(i in 1..=24 {
                    if i == num_bits {
                        return x86::unpack32_avx2::<i>(input, output);
                    }
                });
            }
        }
    }

    seq_macro::seq!(i in 0..=32 {
        if i == num_bits {
            return unpack32::unpack::<i>(input, output);
        }
    });
    unreachable!("invalid num_bits {}", num_bits);
}

unpack!(unpack64, u64, 8, 64);

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;
    use std::sync::atomic::{AtomicU8, Ordering};

    /// Cached result of `is_x86_feature_detected!("avx2")`.
    /// 2 = unknown, 1 = yes, 0 = no.
    static AVX2_CACHED: AtomicU8 = AtomicU8::new(2);

    pub(super) fn avx2_supported() -> bool {
        match AVX2_CACHED.load(Ordering::Relaxed) {
            1 => true,
            0 => false,
            _ => {
                let v = std::arch::is_x86_feature_detected!("avx2");
                AVX2_CACHED.store(v as u8, Ordering::Relaxed);
                v
            }
        }
    }

    /// AVX2 bit-unpack for 32 x u32 outputs at bit widths 1..=24.
    ///
    /// Treats `input` as a sequence of little-endian u32 words (matching the
    /// scalar implementation). For each group of 8 output lanes we gather
    /// the containing lo-word per lane, variable-shift right, and OR in the
    /// hi-word only for lanes that straddle a 32-bit boundary. The hi gather
    /// is masked so non-straddling lanes don't touch (potentially OOB) memory.
    ///
    /// NB: `vpgatherdd` has notoriously poor throughput on recent Intel
    /// (Skylake-X onward even worse post-mitigations); benchmark before
    /// shipping. May regress vs. the autovectorised scalar on some CPUs.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn unpack32_avx2<const NUM_BITS: usize>(
        input: &[u8],
        output: &mut [u32; 32],
    ) {
        debug_assert!((1..=24).contains(&NUM_BITS));
        debug_assert!(input.len() >= NUM_BITS * 4);

        unsafe {
            let mask_val: u32 = (1u32 << NUM_BITS) - 1;
            let vmask = _mm256_set1_epi32(mask_val as i32);
            let base = input.as_ptr() as *const i32;

            for group in 0..4usize {
                let mut offs = [0i32; 8];
                let mut shs = [0i32; 8];
                let mut hi_m = [0i32; 8];
                let mut any_hi = false;
                for lane in 0..8usize {
                    let bit = (group * 8 + lane) * NUM_BITS;
                    offs[lane] = (bit >> 5) as i32;
                    let s = (bit & 31) as i32;
                    shs[lane] = s;
                    if (s as usize) + NUM_BITS > 32 {
                        hi_m[lane] = i32::MIN;
                        any_hi = true;
                    }
                }

                let voffsets = _mm256_setr_epi32(
                    offs[0], offs[1], offs[2], offs[3], offs[4], offs[5], offs[6], offs[7],
                );
                let vshifts = _mm256_setr_epi32(
                    shs[0], shs[1], shs[2], shs[3], shs[4], shs[5], shs[6], shs[7],
                );

                let lo = _mm256_i32gather_epi32::<4>(base, voffsets);
                let lo_shifted = _mm256_srlv_epi32(lo, vshifts);

                let combined = if any_hi {
                    let voffsets_hi = _mm256_add_epi32(voffsets, _mm256_set1_epi32(1));
                    let shifts_hi = _mm256_sub_epi32(_mm256_set1_epi32(32), vshifts);
                    let vhi_mask = _mm256_setr_epi32(
                        hi_m[0], hi_m[1], hi_m[2], hi_m[3], hi_m[4], hi_m[5], hi_m[6],
                        hi_m[7],
                    );
                    // Masked gather: non-straddling lanes skip the load (no fault
                    // even if the offset is OOB per Intel manual).
                    let hi = _mm256_mask_i32gather_epi32::<4>(
                        _mm256_setzero_si256(),
                        base,
                        voffsets_hi,
                        vhi_mask,
                    );
                    let hi_shifted = _mm256_sllv_epi32(hi, shifts_hi);
                    _mm256_or_si256(lo_shifted, hi_shifted)
                } else {
                    lo_shifted
                };

                let masked = _mm256_and_si256(combined, vmask);
                let out_ptr = output.as_mut_ptr().add(group * 8) as *mut __m256i;
                _mm256_storeu_si256(out_ptr, masked);
            }
        }
    }
}

/// Macro that generates a pack function taking the number of bits as a const generic
macro_rules! pack_impl {
    ($t:ty, $bytes:literal, $bits:tt) => {
        #[inline(never)]
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

        pub fn pack_blocks<const NUM_BITS: usize>(input: &[$t], output: &mut [u8]) {
            if NUM_BITS == 0 {
                return;
            }
            let block_bytes = NUM_BITS * $bytes;
            let blocks = input.len() / $bits;
            assert!(output.len() >= blocks * block_bytes);
            for (input, output) in input
                .chunks_exact($bits)
                .zip(output.chunks_exact_mut(block_bytes))
            {
                pack::<NUM_BITS>(input.try_into().unwrap(), output);
            }
        }
    };
}

/// Macro that generates pack functions that accept num_bits as a parameter
macro_rules! pack {
    ($name:ident, $blocks:ident, $t:ty, $bytes:literal, $bits:tt) => {
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

        #[inline(never)]
        pub(crate) fn $blocks(input: &[$t], output: &mut [u8], num_bits: usize) {
            seq_macro::seq!(i in 0..=$bits {
                if i == num_bits {
                    return $name::pack_blocks::<i>(input, output);
                }
            });
            unreachable!("invalid num_bits {}", num_bits);
        }
    };
}

pack!(pack8, pack8_blocks, u8, 1, 8);
pack!(pack16, pack16_blocks, u16, 2, 16);
pack!(pack32, pack32_blocks, u32, 4, 32);
pack!(pack64, pack64_blocks, u64, 8, 64);

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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_unpack32_avx2_matches_scalar() {
        use crate::util::test_common::rand_gen::random_numbers;
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }
        for num_bits in 1..=24 {
            for _ in 0..16 {
                let input: Vec<u8> = random_numbers(num_bits * 4);
                let mut avx_out = [0u32; 32];
                let mut scalar_out = [0u32; 32];
                unsafe {
                    seq_macro::seq!(i in 1..=24 {
                        if i == num_bits {
                            super::x86::unpack32_avx2::<i>(&input, &mut avx_out);
                        }
                    });
                }
                seq_macro::seq!(i in 0..=32 {
                    if i == num_bits {
                        super::unpack32::unpack::<i>(&input, &mut scalar_out);
                    }
                });
                assert_eq!(avx_out, scalar_out, "mismatch at num_bits={num_bits}");
            }
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
