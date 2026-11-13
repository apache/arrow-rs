// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::*;

use arrow::compute::kernels::numeric::*;
use arrow::datatypes::{
    ArrowPrimitiveType, Float32Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use arrow::util::bench_util::*;
use arrow_array::{Array, ArrowNativeTypeOp, PrimitiveArray, Scalar};
use rand::distr::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;
use std::ops::Range;

fn add_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 64 * 1024;
    for null_density in [0., 0.1, 0.5, 0.9, 1.0] {
        let arr_a = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let arr_b = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let scalar_a = create_primitive_array::<Float32Type>(1, 0.);
        let scalar = Scalar::new(&scalar_a);

        c.bench_function(&format!("add({null_density})"), |b| {
            b.iter(|| hint::black_box(add_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(add(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(add_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("subtract({null_density})"), |b| {
            b.iter(|| hint::black_box(sub_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(sub(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(sub_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("multiply({null_density})"), |b| {
            b.iter(|| hint::black_box(mul_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(mul(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(mul_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("divide({null_density})"), |b| {
            b.iter(|| hint::black_box(div(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("divide_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(div(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("modulo({null_density})"), |b| {
            b.iter(|| hint::black_box(rem(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("modulo_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(rem(&arr_a, &scalar).unwrap()))
        });
    }
}

/// Like `create_primitive_array_range`, but with its own `seed`, so that two
/// arrays hold different values and nulls.
fn seeded_array<T>(
    size: usize,
    null_density: f32,
    range: Range<T::Native>,
    seed: u64,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: SampleUniform,
{
    let mut rng = StdRng::seed_from_u64(seed);
    (0..size)
        .map(|_| (rng.random::<f32>() >= null_density).then(|| rng.random_range(range.clone())))
        .collect()
}

/// Checked and wrapping integer arithmetic at DataFusion's default batch size.
///
/// The left operands come from `a_range` and the right ones from `b_range`, so
/// the checked cases never overflow in a valid row unless the case name says
/// so. Null slots hold 0, as a builder writes them, so with unsigned types
/// `subtract_checked_scalar` overflows in null slots.
fn integer_benchmark<T>(
    c: &mut Criterion,
    name: &str,
    a_range: Range<T::Native>,
    b_range: Range<T::Native>,
    max: T::Native,
) where
    T: ArrowPrimitiveType,
    T::Native: SampleUniform,
{
    const BATCH_SIZE: usize = 8192;
    let one = T::Native::ONE;
    for null_density in [0., 0.1] {
        let a = seeded_array::<T>(BATCH_SIZE, null_density, a_range.clone(), 1);
        let b = seeded_array::<T>(BATCH_SIZE, null_density, b_range.clone(), 2);
        let scalar = Scalar::new(seeded_array::<T>(1, 0., one..b_range.end, 3));

        c.bench_function(&format!("{name}_add({null_density})"), |bench| {
            bench.iter(|| hint::black_box(add_wrapping(&a, &b).unwrap()))
        });
        c.bench_function(&format!("{name}_add_checked({null_density})"), |bench| {
            bench.iter(|| hint::black_box(add(&a, &b).unwrap()))
        });
        c.bench_function(
            &format!("{name}_add_checked_scalar({null_density})"),
            |bench| bench.iter(|| hint::black_box(add(&a, &scalar).unwrap())),
        );
        c.bench_function(&format!("{name}_subtract({null_density})"), |bench| {
            bench.iter(|| hint::black_box(sub_wrapping(&a, &b).unwrap()))
        });
        c.bench_function(
            &format!("{name}_subtract_checked({null_density})"),
            |bench| bench.iter(|| hint::black_box(sub(&a, &b).unwrap())),
        );
        c.bench_function(
            &format!("{name}_subtract_checked_scalar({null_density})"),
            |bench| bench.iter(|| hint::black_box(sub(&a, &scalar).unwrap())),
        );
        c.bench_function(&format!("{name}_multiply({null_density})"), |bench| {
            bench.iter(|| hint::black_box(mul_wrapping(&a, &b).unwrap()))
        });
        c.bench_function(
            &format!("{name}_multiply_checked({null_density})"),
            |bench| bench.iter(|| hint::black_box(mul(&a, &b).unwrap())),
        );
    }

    // Overflow in the last row, so the whole array is processed before the error.
    let mut values = seeded_array::<T>(BATCH_SIZE, 0., a_range.clone(), 4)
        .values()
        .to_vec();
    values[BATCH_SIZE - 1] = max;
    let overflow = PrimitiveArray::<T>::from_iter_values(values);
    let one = Scalar::new(PrimitiveArray::<T>::from_iter_values([one]));
    c.bench_function(&format!("{name}_add_checked_overflow"), |bench| {
        bench.iter(|| hint::black_box(add(&overflow, &one).unwrap_err()))
    });

    // Null slots hold `max`, which overflows, but no valid row does.
    let a = seeded_array::<T>(BATCH_SIZE, 0.1, a_range, 5);
    let b = seeded_array::<T>(BATCH_SIZE, 0., T::Native::ONE..b_range.end, 6);
    let values = (0..BATCH_SIZE).map(|i| if a.is_null(i) { max } else { a.value(i) });
    let null_overflow = PrimitiveArray::<T>::new(values.collect(), a.nulls().cloned());
    c.bench_function(&format!("{name}_add_checked_null_slot_overflow"), |bench| {
        bench.iter(|| hint::black_box(add(&null_overflow, &b).unwrap()))
    });
}

fn integer_benchmarks(c: &mut Criterion) {
    integer_benchmark::<Int32Type>(c, "int32", -1000..1000, -1000..1000, i32::MAX);
    integer_benchmark::<Int64Type>(c, "int64", -1000..1000, -1000..1000, i64::MAX);
    // The left operands are larger, so that subtraction doesn't overflow.
    integer_benchmark::<UInt32Type>(c, "uint32", 1000..2000, 0..1000, u32::MAX);
    integer_benchmark::<UInt64Type>(c, "uint64", 1000..2000, 0..1000, u64::MAX);
}

criterion_group!(benches, add_benchmark, integer_benchmarks);
criterion_main!(benches);
