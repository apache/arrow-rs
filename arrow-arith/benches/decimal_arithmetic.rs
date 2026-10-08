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
use arrow_arith::numeric::{add, div, rem, sub};
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

fn decimal<T: DecimalType>(
    values: impl Iterator<Item = T::Native>,
    scale: i8,
) -> PrimitiveArray<T> {
    PrimitiveArray::<T>::new(values.collect::<Vec<_>>().into(), None)
        .with_precision_and_scale(T::MAX_PRECISION, scale)
        .unwrap()
}

fn benchmark<T: DecimalType>(c: &mut Criterion, name: &str) {
    for (scale, right_scale) in [("equal", 0), ("different", 1)] {
        let left = decimal::<T>((0..SIZE).map(T::Native::usize_as), 0);
        let right = decimal::<T>(
            (0..SIZE).map(|i| T::Native::usize_as(SIZE - i)),
            right_scale,
        );
        let mut group = c.benchmark_group(format!("{name}_{scale}_scale"));
        group.throughput(Throughput::Elements(SIZE as u64));
        group.bench_function("add", |b| {
            b.iter(|| hint::black_box(add(&left, &right).unwrap()))
        });
        group.bench_function("sub", |b| {
            b.iter(|| hint::black_box(sub(&left, &right).unwrap()))
        });
        group.finish();
    }
}

fn decimal_arithmetic(c: &mut Criterion) {
    benchmark::<Decimal32Type>(c, "decimal32");
    benchmark::<Decimal64Type>(c, "decimal64");
    benchmark::<Decimal128Type>(c, "decimal128");
    benchmark::<Decimal256Type>(c, "decimal256");
}

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
    decimal::<T>(values, scale)
}

/// Benchmarks dividing values with scale 10 by a scalar with scale 2, for each
/// pair of value and divisor digit counts in `cases`
fn benchmark_scalar<T: DecimalType>(c: &mut Criterion, name: &str, cases: [(u32, u32); 4]) {
    let mut group = c.benchmark_group(format!("{name}_scalar"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (digits, divisor_digits) in cases {
        let array = random_decimal::<T>(SIZE, digits, 10, 42);
        let divisor = Scalar::new(random_decimal::<T>(1, divisor_digits, 2, 42));
        for (op, kernel) in [("div", div as Kernel), ("rem", rem)] {
            group.bench_function(
                format!("{op}_{digits}_digits_by_{divisor_digits}_digits/no_nulls"),
                |b| b.iter(|| hint::black_box(kernel(&array, &divisor).unwrap())),
            );
        }
    }

    // `try_unary` visits only the valid values when the array has nulls
    let (digits, divisor_digits) = cases[0];
    let array = random_decimal::<T>(SIZE, digits, 10, 42);
    let nulls = NullBuffer::from_iter((0..SIZE).map(|i| i % 5 != 0));
    let with_nulls = PrimitiveArray::<T>::new(array.values().clone(), Some(nulls))
        .with_data_type(array.data_type().clone());
    let divisor = Scalar::new(random_decimal::<T>(1, divisor_digits, 2, 42));
    for (op, kernel) in [("div", div as Kernel), ("rem", rem)] {
        group.bench_function(
            format!("{op}_{digits}_digits_by_{divisor_digits}_digits/mixed_nulls"),
            |b| b.iter(|| hint::black_box(kernel(&with_nulls, &divisor).unwrap())),
        );
    }

    // Small arrays, where the cost of setting up a division can outweigh the per-value work
    let (digits, divisor_digits) = cases[1];
    let divisor = Scalar::new(random_decimal::<T>(1, divisor_digits, 2, 42));
    for len in [1, 16, 128] {
        let array = random_decimal::<T>(len, digits, 10, 42);
        group.throughput(Throughput::Elements(len as u64));
        group.bench_function(
            format!("div_{digits}_digits_by_{divisor_digits}_digits/{len}_values"),
            |b| b.iter(|| hint::black_box(div(&array, &divisor).unwrap())),
        );
    }
    group.finish();
}

/// Decimal32 and Decimal64 divide with the same instructions as Int32 and Int64,
/// which the `integer_division` benchmark covers
fn decimal_scalar_division(c: &mut Criterion) {
    // `div` scales the values by 10^6. The cases are a value that fits in 64 bits after scaling,
    // a wider value, a divisor wider than 64 bits, and a value that overflows when scaled, which
    // `div` handles one digit at a time
    benchmark_scalar::<Decimal128Type>(c, "decimal128", [(10, 4), (30, 4), (30, 21), (34, 4)]);
    // The same cases for 128 bits
    benchmark_scalar::<Decimal256Type>(c, "decimal256", [(30, 4), (60, 4), (60, 40), (72, 4)]);
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
    decimal_arithmetic,
    decimal_scalar_division,
    decimal_multiply_fixed_point
);
criterion_main!(benches);
