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

//! Vectorized min / max / NaN count kernels for page statistics of fixed
//! width primitive columns.
//!
//! Every value written to a column with statistics enabled (the default) goes
//! through these kernels, so they are on the hot path of the writer.
//!
//! The kernels fold values into `LANES` independent accumulators, one
//! fixed size chunk at a time. With no dependency between lanes and no
//! data dependent branches, LLVM lowers each chunk to SIMD min / max / select
//! instructions on every target (NEON, SSE4.1, AVX2, AVX-512) without
//! `unsafe` or target specific intrinsics.
//!
//! The results match the scalar comparison in [`compare_greater`] exactly:
//!
//! * `INT32` / `INT64` compare as signed, or as unsigned for
//!   [`SortOrder::UNSIGNED`].
//! * `FLOAT` / `DOUBLE` compare by IEEE 754 total order (as
//!   [`f32::total_cmp`]) and skip NaN, which are only counted.
//!
//! [`compare_greater`]: super::compare_greater

use crate::basic::SortOrder;
use crate::schema::types::BasicTypeInfo;

/// Accumulators for 64 bytes of values, i.e. a cache line and one AVX-512
/// register, or four NEON / two AVX2 registers.
const LANES_32: usize = 16;
const LANES_64: usize = 8;

/// Returns the minimum and maximum of `values` after mapping them with `key`,
/// or `None` if `values` is empty.
#[inline(always)]
fn min_max_by_key<T: Copy, K: Copy + Ord, const LANES: usize>(
    values: &[T],
    key: impl Fn(T) -> K,
    key_min: K,
    key_max: K,
) -> Option<(K, K)> {
    if values.is_empty() {
        return None;
    }

    let mut mins = [key_max; LANES];
    let mut maxs = [key_min; LANES];

    let (chunks, remainder) = values.as_chunks::<LANES>();
    for chunk in chunks {
        for i in 0..LANES {
            let k = key(chunk[i]);
            mins[i] = mins[i].min(k);
            maxs[i] = maxs[i].max(k);
        }
    }
    for (i, v) in remainder.iter().enumerate() {
        let k = key(*v);
        mins[i] = mins[i].min(k);
        maxs[i] = maxs[i].max(k);
    }

    let min = mins.into_iter().min().unwrap();
    let max = maxs.into_iter().max().unwrap();
    Some((min, max))
}

#[inline]
fn is_unsigned(basic_type_info: &BasicTypeInfo) -> bool {
    matches!(basic_type_info.sort_order(), SortOrder::UNSIGNED)
}

/// Returns the min and max of an `INT32` column, or `None` if `values` is empty.
pub(crate) fn min_max_i32(basic_type_info: &BasicTypeInfo, values: &[i32]) -> Option<(i32, i32)> {
    if is_unsigned(basic_type_info) {
        min_max_by_key::<_, _, LANES_32>(values, |v| v as u32, u32::MIN, u32::MAX)
            .map(|(min, max)| (min as i32, max as i32))
    } else {
        min_max_by_key::<_, _, LANES_32>(values, |v| v, i32::MIN, i32::MAX)
    }
}

/// Returns the min and max of an `INT64` column, or `None` if `values` is empty.
pub(crate) fn min_max_i64(basic_type_info: &BasicTypeInfo, values: &[i64]) -> Option<(i64, i64)> {
    if is_unsigned(basic_type_info) {
        min_max_by_key::<_, _, LANES_64>(values, |v| v as u64, u64::MIN, u64::MAX)
            .map(|(min, max)| (min as i64, max as i64))
    } else {
        min_max_by_key::<_, _, LANES_64>(values, |v| v, i64::MIN, i64::MAX)
    }
}

macro_rules! float_min_max {
    ($name:ident, $float:ty, $int:ty, $uint:ty, $lanes:expr) => {
        /// Returns the min, max and NaN count of a floating point column,
        /// comparing by IEEE 754 total order and skipping NaN.
        ///
        /// Returns `None` if `values` is empty or contains only NaN, in which
        /// case the min and max are NaN themselves and the caller falls back
        /// to the scalar implementation.
        pub(crate) fn $name(values: &[$float]) -> Option<($float, $float, u64)> {
            const SIGN_SHIFT: u32 = <$int>::BITS - 1;
            const ABS_MASK: $uint = <$uint>::MAX >> 1;
            const INF_BITS: $uint = <$float>::INFINITY.to_bits();

            // Maps the bits of a float to an integer with the same ordering as
            // `total_cmp`: negative values have their magnitude bits flipped
            // so that they order in reverse. The mapping is its own inverse.
            #[inline(always)]
            fn total_order_key(bits: $int) -> $int {
                bits ^ ((((bits >> SIGN_SHIFT) as $uint) >> 1) as $int)
            }

            // Bounds the per lane NaN counters well below overflow on every
            // platform, including 32-bit.
            const BLOCK: usize = 1 << 30;

            let mut min = <$int>::MAX;
            let mut max = <$int>::MIN;
            let mut nan_count = 0u64;

            for block in values.chunks(BLOCK) {
                let mut mins = [<$int>::MAX; $lanes];
                let mut maxs = [<$int>::MIN; $lanes];
                let mut nans = [<$uint>::MIN; $lanes];

                // NaN lanes are replaced by the identity of each accumulator, so
                // they never win, without a branch.
                let mut accumulate = |i: usize, v: $float| {
                    let bits = v.to_bits();
                    let is_nan = (bits & ABS_MASK) > INF_BITS;
                    let key = total_order_key(bits as $int);
                    mins[i] = mins[i].min(if is_nan { <$int>::MAX } else { key });
                    maxs[i] = maxs[i].max(if is_nan { <$int>::MIN } else { key });
                    nans[i] += is_nan as $uint;
                };

                let (chunks, remainder) = block.as_chunks::<$lanes>();
                for chunk in chunks {
                    for i in 0..$lanes {
                        accumulate(i, chunk[i]);
                    }
                }
                for (i, v) in remainder.iter().enumerate() {
                    accumulate(i, *v);
                }

                min = min.min(mins.into_iter().min().unwrap());
                max = max.max(maxs.into_iter().max().unwrap());
                nan_count += nans.into_iter().map(u64::from).sum::<u64>();
            }

            if nan_count as usize == values.len() {
                return None;
            }

            let min = <$float>::from_bits(total_order_key(min) as $uint);
            let max = <$float>::from_bits(total_order_key(max) as $uint);
            Some((min, max, nan_count))
        }
    };
}

float_min_max!(min_max_f32, f32, i32, u32, LANES_32);
float_min_max!(min_max_f64, f64, i64, u64, LANES_64);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::{ConvertedType, IntType, LogicalType, Type as PhysicalType};
    use crate::column::writer::encoder::get_min_max;
    use crate::schema::types::Type;
    use rand::prelude::*;

    fn type_info(physical: PhysicalType, logical: Option<LogicalType>) -> BasicTypeInfo {
        Type::primitive_type_builder("col", physical)
            .with_logical_type(logical)
            .build()
            .unwrap()
            .get_basic_info()
            .clone()
    }

    fn signed_int(bits: u8) -> Option<LogicalType> {
        Some(LogicalType::Integer(IntType {
            bit_width: bits as i8,
            is_signed: true,
        }))
    }

    fn unsigned_int(bits: u8) -> Option<LogicalType> {
        Some(LogicalType::Integer(IntType {
            bit_width: bits as i8,
            is_signed: false,
        }))
    }

    /// Lengths around every lane count and remainder boundary
    fn lengths() -> impl Iterator<Item = usize> {
        (0..=70).chain([127, 128, 129, 1000, 4097])
    }

    fn check_ints<T, F>(
        rng: &mut StdRng,
        info: &BasicTypeInfo,
        kernel: F,
        gen_value: impl Fn(&mut StdRng) -> T,
    ) where
        T: crate::data_type::private::ParquetValueType + Copy,
        F: Fn(&BasicTypeInfo, &[T]) -> Option<(T, T)>,
    {
        for len in lengths() {
            let values: Vec<T> = (0..len).map(|_| gen_value(rng)).collect();
            let expected = get_min_max(info, values.iter());
            let actual = kernel(info, &values).map(|(min, max)| (min, max, 0));
            assert_eq!(actual, expected, "len {len}: {values:?}");
        }
    }

    #[test]
    fn test_min_max_i32() {
        let mut rng = StdRng::seed_from_u64(42);
        let edges = [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
        for info in [
            type_info(PhysicalType::INT32, None),
            type_info(PhysicalType::INT32, signed_int(32)),
            type_info(PhysicalType::INT32, unsigned_int(32)),
            type_info(PhysicalType::INT32, unsigned_int(8)),
        ] {
            check_ints(&mut rng, &info, min_max_i32, |r| r.random());
            check_ints(&mut rng, &info, min_max_i32, |r| *edges.choose(r).unwrap());
            check_ints(&mut rng, &info, min_max_i32, |r| r.random_range(-3..3));
        }
    }

    #[test]
    fn test_min_max_i64() {
        let mut rng = StdRng::seed_from_u64(42);
        let edges = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
        for info in [
            type_info(PhysicalType::INT64, None),
            type_info(PhysicalType::INT64, signed_int(64)),
            type_info(PhysicalType::INT64, unsigned_int(64)),
        ] {
            check_ints(&mut rng, &info, min_max_i64, |r| r.random());
            check_ints(&mut rng, &info, min_max_i64, |r| *edges.choose(r).unwrap());
            check_ints(&mut rng, &info, min_max_i64, |r| r.random_range(-3..3));
        }
    }

    #[test]
    fn test_unsigned_int32_converted_type() {
        let info = Type::primitive_type_builder("col", PhysicalType::INT32)
            .with_converted_type(ConvertedType::UINT_32)
            .build()
            .unwrap()
            .get_basic_info()
            .clone();
        assert_eq!(min_max_i32(&info, &[-1, 0, 5]), Some((0, -1)));
    }

    macro_rules! float_tests {
        ($test:ident, $float:ty, $uint:ty, $kernel:ident, $physical:expr) => {
            #[test]
            fn $test() {
                let info = type_info($physical, None);
                let mut rng = StdRng::seed_from_u64(42);

                let quiet_nan = <$float>::NAN;
                let neg_nan = -<$float>::NAN;
                let payload_nan = <$float>::from_bits(<$float>::NAN.to_bits() | 1);
                let max_nan = <$float>::from_bits(<$uint>::MAX >> 1);
                let edges = [
                    <$float>::NEG_INFINITY,
                    <$float>::MIN,
                    -1.0,
                    -<$float>::MIN_POSITIVE,
                    -0.0,
                    0.0,
                    <$float>::MIN_POSITIVE,
                    <$float>::from_bits(1),
                    1.0,
                    <$float>::MAX,
                    <$float>::INFINITY,
                    quiet_nan,
                    neg_nan,
                    payload_nan,
                    max_nan,
                ];
                let nans = [quiet_nan, neg_nan, payload_nan, max_nan];

                let generators: [&dyn Fn(&mut StdRng) -> $float; 5] = [
                    &|r| r.random::<$float>() * 2.0 - 1.0,
                    &|r| <$float>::from_bits(r.random()),
                    &|r| *edges.choose(r).unwrap(),
                    &|r| *nans.choose(r).unwrap(),
                    &|r| {
                        if r.random_bool(0.95) {
                            quiet_nan
                        } else {
                            r.random()
                        }
                    },
                ];

                for generator in generators {
                    for len in lengths() {
                        let values: Vec<$float> = (0..len).map(|_| generator(&mut rng)).collect();
                        let expected = get_min_max(&info, values.iter());
                        match $kernel(&values) {
                            Some(actual) => {
                                // Compare bits to tell apart -0.0 / 0.0
                                let to_bits = |(min, max, nans): ($float, $float, u64)| {
                                    (min.to_bits(), max.to_bits(), nans)
                                };
                                assert_eq!(
                                    to_bits(actual),
                                    to_bits(expected.unwrap()),
                                    "len {len}: {values:?}"
                                );
                            }
                            // Only all NaN (or empty) input defers to the scalar path
                            None => assert!(values.iter().all(|v| v.is_nan()), "{values:?}"),
                        }
                    }
                }
            }
        };
    }

    float_tests!(test_min_max_f32, f32, u32, min_max_f32, PhysicalType::FLOAT);
    float_tests!(
        test_min_max_f64,
        f64,
        u64,
        min_max_f64,
        PhysicalType::DOUBLE
    );
}
