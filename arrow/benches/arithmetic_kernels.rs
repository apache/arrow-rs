// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::*;

use arrow::compute::kernels::numeric::*;
use arrow::datatypes::{Decimal64Type, Decimal128Type, Decimal256Type, Float32Type};
use arrow::util::bench_util::*;
use arrow_array::{Array, PrimitiveArray, Scalar};
use arrow_buffer::i256;
use std::hint;

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

/// Decimal arithmetic at DataFusion's default batch size.
///
/// Values stay within six digits, so the cases never overflow unless the case
/// name says so. `add` and `add_wrapping` run the same checked kernel for
/// decimals, so only `add` is measured.
fn decimal_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 8192;
    let decimal128 = |null_density, scale| {
        create_primitive_array_range::<Decimal128Type>(
            BATCH_SIZE,
            null_density,
            -999_999..1_000_000,
        )
        .with_precision_and_scale(18, scale)
        .unwrap()
    };
    for null_density in [0., 0.1] {
        let a = decimal128(null_density, 2);
        let b = decimal128(null_density, 2);
        let b_scale4 = decimal128(null_density, 4);
        let a64 = create_primitive_array_range::<Decimal64Type>(
            BATCH_SIZE,
            null_density,
            -999_999..1_000_000,
        )
        .with_precision_and_scale(18, 2)
        .unwrap();
        let b64 = a64.clone();
        let a256: PrimitiveArray<Decimal256Type> = a.iter().map(|v| v.map(i256::from)).collect();
        let a256 = a256.with_precision_and_scale(38, 2).unwrap();
        let b256 = a256.clone();

        c.bench_function(&format!("decimal128_add({null_density})"), |bench| {
            bench.iter(|| hint::black_box(add(&a, &b).unwrap()))
        });
        c.bench_function(
            &format!("decimal128_add_rescale({null_density})"),
            |bench| bench.iter(|| hint::black_box(add(&a, &b_scale4).unwrap())),
        );
        c.bench_function(&format!("decimal128_subtract({null_density})"), |bench| {
            bench.iter(|| hint::black_box(sub(&a, &b).unwrap()))
        });
        c.bench_function(&format!("decimal128_multiply({null_density})"), |bench| {
            bench.iter(|| hint::black_box(mul(&a, &b).unwrap()))
        });
        c.bench_function(&format!("decimal128_divide({null_density})"), |bench| {
            bench.iter(|| hint::black_box(div(&a, &b).unwrap()))
        });
        c.bench_function(&format!("decimal64_add({null_density})"), |bench| {
            bench.iter(|| hint::black_box(add(&a64, &b64).unwrap()))
        });
        c.bench_function(&format!("decimal64_multiply({null_density})"), |bench| {
            bench.iter(|| hint::black_box(mul(&a64, &b64).unwrap()))
        });
        c.bench_function(&format!("decimal256_add({null_density})"), |bench| {
            bench.iter(|| hint::black_box(add(&a256, &b256).unwrap()))
        });
        c.bench_function(&format!("decimal256_multiply({null_density})"), |bench| {
            bench.iter(|| hint::black_box(mul(&a256, &b256).unwrap()))
        });
    }

    // Overflow in the last row, so the whole array is processed before the error.
    let mut values = decimal128(0., 2).values().to_vec();
    values[BATCH_SIZE - 1] = i128::MAX;
    let overflow = PrimitiveArray::<Decimal128Type>::from_iter_values(values)
        .with_precision_and_scale(38, 2)
        .unwrap();
    let one = Scalar::new(
        PrimitiveArray::<Decimal128Type>::from_iter_values([1])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    c.bench_function("decimal128_add_overflow", |bench| {
        bench.iter(|| hint::black_box(add(&overflow, &one).unwrap_err()))
    });

    // Null slots hold `i128::MAX`, which overflows, but no valid row does.
    let a = decimal128(0.1, 2);
    let values = (0..BATCH_SIZE).map(|i| if a.is_null(i) { i128::MAX } else { a.value(i) });
    let null_overflow = PrimitiveArray::<Decimal128Type>::new(values.collect(), a.nulls().cloned())
        .with_precision_and_scale(18, 2)
        .unwrap();
    let b = create_primitive_array_range::<Decimal128Type>(BATCH_SIZE, 0., 1..1_000_000)
        .with_precision_and_scale(18, 2)
        .unwrap();
    c.bench_function("decimal128_add_null_slot_overflow", |bench| {
        bench.iter(|| hint::black_box(add(&null_overflow, &b).unwrap()))
    });
}

criterion_group!(benches, add_benchmark, decimal_benchmark);
criterion_main!(benches);
