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

use std::hint;

use arrow_arith::arithmetic::multiply_fixed_point;
use arrow_arith::numeric::{div, rem};
use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_array::{Array, ArrayRef, ArrowNativeTypeOp, Datum, PrimitiveArray, Scalar};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::ArrowError;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const SIZE: usize = 1024;

type Kernel = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;

/// Creates an array of `len` random values with `digits` significant digits
/// (no leading zero, half of them negative) for the given `scale`
fn random_decimal<T: DecimalType>(
    len: usize,
    digits: u32,
    scale: i8,
    seed: u64,
) -> PrimitiveArray<T> {
    let mut rng = StdRng::seed_from_u64(seed);
    let ten = T::Native::usize_as(10);
    let values = (0..len).map(|_| {
        let mut value = T::Native::usize_as(rng.random_range(1..10));
        for _ in 1..digits {
            value = value
                .mul_wrapping(ten)
                .add_wrapping(T::Native::usize_as(rng.random_range(0..10)));
        }
        if rng.random::<bool>() {
            value.neg_wrapping()
        } else {
            value
        }
    });
    PrimitiveArray::<T>::from_iter_values(values)
        .with_precision_and_scale(T::MAX_PRECISION, scale)
        .unwrap()
}

/// Returns `array` with a fifth of its values null, at random positions
fn with_random_nulls<T: DecimalType>(array: &PrimitiveArray<T>) -> PrimitiveArray<T> {
    let mut rng = StdRng::seed_from_u64(7);
    let nulls = NullBuffer::from_iter((0..array.len()).map(|_| rng.random_ratio(4, 5)));
    PrimitiveArray::<T>::new(array.values().clone(), Some(nulls))
        .with_data_type(array.data_type().clone())
}

/// Benchmarks `div` and `rem` of values with scale `scale` by a scalar, for each pair of value
/// and divisor digit counts in `div_cases` and `rem_cases`, and `div` of the first `div` case
/// on arrays of each length in `small_lens`
///
/// For `div` the divisor has scale 2, so `div` scales the values by 10^6. The last `div` case
/// overflows when scaled, which `div` handles one digit at a time. For `rem` the divisor has
/// scale `scale`, because `rem` scales the divisor to the larger scale, which would change its
/// digit count.
fn benchmark_scalar<T: DecimalType>(
    c: &mut Criterion,
    name: &str,
    scale: i8,
    div_cases: &[(u32, u32)],
    rem_cases: &[(u32, u32)],
    small_lens: &[usize],
) {
    let mut group = c.benchmark_group(format!("{name}_scalar"));
    group.throughput(Throughput::Elements(SIZE as u64));
    let scale_factor = T::Native::usize_as(1_000_000);
    let ops = [
        ("div", div as Kernel, 2, div_cases),
        ("rem", rem as Kernel, scale, rem_cases),
    ];
    let mut small = None;
    for (op, kernel, divisor_scale, cases) in ops {
        for (i, &(digits, divisor_digits)) in cases.iter().enumerate() {
            let array = random_decimal::<T>(SIZE, digits, scale, 42);
            let divisor = Scalar::new(random_decimal::<T>(1, divisor_digits, divisor_scale, 42));
            let id = format!("{op}_{digits}_digits_by_{divisor_digits}_digits");
            if op == "div" {
                // Fail if a case no longer takes the path it's meant to
                let overflowing = array
                    .values()
                    .iter()
                    .filter(|v| v.mul_checked(scale_factor).is_err())
                    .count();
                let expected = if i + 1 == cases.len() { SIZE } else { 0 };
                assert_eq!(overflowing, expected, "{name} {id}");
            }
            group.bench_function(format!("{id}/no_nulls"), |b| {
                b.iter(|| hint::black_box(kernel(&array, &divisor).unwrap()))
            });

            if i == 0 {
                // `try_unary` visits only the valid values when the array has nulls
                let with_nulls = with_random_nulls(&array);
                group.bench_function(format!("{id}/mixed_nulls"), |b| {
                    b.iter(|| hint::black_box(kernel(&with_nulls, &divisor).unwrap()))
                });
                if op == "div" {
                    small = Some((id, array, divisor));
                }
            }
        }
    }

    // Small arrays, where the cost of building a divisor can outweigh the per-value savings
    let (id, array, divisor) = small.unwrap();
    for &len in small_lens {
        let array = array.slice(0, len);
        group.throughput(Throughput::Elements(len as u64));
        group.bench_function(format!("{id}/{len}_values"), |b| {
            b.iter(|| hint::black_box(div(&array, &divisor).unwrap()))
        });
    }
    group.finish();
}

fn decimal_scalar_division(c: &mut Criterion) {
    // Values that fit after scaling and values that overflow. For each overflowing value, `div`
    // also builds an `ArrowError` and discards it before falling back.
    benchmark_scalar::<Decimal32Type>(c, "decimal32", 2, &[(3, 4), (6, 4)], &[(9, 4)], &[128]);
    // A value that fits in 32 bits after scaling, a wider one, and one that overflows
    benchmark_scalar::<Decimal64Type>(
        c,
        "decimal64",
        2,
        &[(3, 4), (12, 4), (15, 4)],
        &[(18, 4)],
        &[128],
    );
    // A value that fits in 64 bits after scaling, a wider one, a divisor wider than 64 bits, and a
    // value that overflows. `rem` doesn't scale the values, so it has no overflow case. Building
    // a divisor for these types costs more, so they also run on 1 and 16 values.
    benchmark_scalar::<Decimal128Type>(
        c,
        "decimal128",
        10,
        &[(10, 4), (30, 4), (30, 21), (34, 4)],
        &[(10, 4), (30, 4), (30, 21)],
        &[1, 16, 128],
    );
    // The same cases for 128 bits
    benchmark_scalar::<Decimal256Type>(
        c,
        "decimal256",
        10,
        &[(30, 4), (60, 4), (60, 40), (72, 4)],
        &[(30, 4), (60, 4), (60, 40)],
        &[1, 16, 128],
    );
}

/// Benchmarks `multiply_fixed_point`, which computes each product in `i256` and divides it by
/// 10^(product scale - required scale)
fn decimal_multiply_fixed_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("decimal128_multiply_fixed_point");
    group.throughput(Throughput::Elements(SIZE as u64));
    // Products that fit in 128 bits, wider products, and a divisor wider than 64 bits
    for (digits, scale) in [(15, 10), (24, 10), (21, 20)] {
        let left = random_decimal::<Decimal128Type>(SIZE, digits, scale, 1);
        let right = random_decimal::<Decimal128Type>(SIZE, digits, scale, 2);
        group.bench_function(format!("{digits}_digits_scale_{scale}_to_scale_6"), |b| {
            b.iter(|| hint::black_box(multiply_fixed_point(&left, &right, 6).unwrap()))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    decimal_scalar_division,
    decimal_multiply_fixed_point
);
criterion_main!(benches);
