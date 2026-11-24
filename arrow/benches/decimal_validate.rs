// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;

use arrow::array::{
    Array, Decimal32Array, Decimal32Builder, Decimal64Array, Decimal64Builder, Decimal128Array,
    Decimal128Builder, Decimal256Array, Decimal256Builder,
};
use criterion::Criterion;
use rand::RngExt;

use arrow_buffer::i256;

fn validate_decimal32_array(array: Decimal32Array) {
    array.with_precision_and_scale(8, 0).unwrap();
}

fn validate_decimal64_array(array: Decimal64Array) {
    array.with_precision_and_scale(16, 0).unwrap();
}

fn validate_decimal128_array(array: Decimal128Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal256_array(array: Decimal256Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal32_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    let size: i32 = 20000;
    let mut decimal_builder = Decimal32Builder::with_capacity(size as usize);
    for _ in 0..size {
        decimal_builder.append_value(rng.random_range::<i32, _>(0..99999999));
    }
    let decimal_array = decimal_builder
        .finish()
        .with_precision_and_scale(9, 0)
        .unwrap();
    let data = decimal_array.into_data();
    c.bench_function("validate_decimal32_array 20000", |b| {
        b.iter(|| {
            let array = Decimal32Array::from(data.clone());
            validate_decimal32_array(array);
        })
    });
}

fn validate_decimal64_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    let size: i64 = 20000;
    let mut decimal_builder = Decimal64Builder::with_capacity(size as usize);
    for _ in 0..size {
        decimal_builder.append_value(rng.random_range::<i64, _>(0..999999999999));
    }
    let decimal_array = decimal_builder
        .finish()
        .with_precision_and_scale(18, 0)
        .unwrap();
    let data = decimal_array.into_data();
    c.bench_function("validate_decimal64_array 20000", |b| {
        b.iter(|| {
            let array = Decimal64Array::from(data.clone());
            validate_decimal64_array(array);
        })
    });
}

fn validate_decimal128_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    let size: i128 = 20000;
    let mut decimal_builder = Decimal128Builder::with_capacity(size as usize);
    for _ in 0..size {
        decimal_builder.append_value(rng.random_range::<i128, _>(0..999999999999));
    }
    let decimal_array = decimal_builder
        .finish()
        .with_precision_and_scale(38, 0)
        .unwrap();
    let data = decimal_array.into_data();
    c.bench_function("validate_decimal128_array 20000", |b| {
        b.iter(|| {
            let array = Decimal128Array::from(data.clone());
            validate_decimal128_array(array);
        })
    });
}

fn validate_decimal256_benchmark(c: &mut Criterion) {
    let mut rng = rand::rng();
    let size: i128 = 20000;
    let mut decimal_builder = Decimal256Builder::with_capacity(size as usize);
    for _ in 0..size {
        let v = rng.random_range::<i128, _>(0..999999999999999);
        let decimal = i256::from_i128(v);
        decimal_builder.append_value(decimal);
    }
    let decimal_array256_data = decimal_builder
        .finish()
        .with_precision_and_scale(76, 0)
        .unwrap();
    let data = decimal_array256_data.into_data();
    c.bench_function("validate_decimal256_array 20000", |b| {
        b.iter(|| {
            let array = Decimal256Array::from(data.clone());
            validate_decimal256_array(array);
        })
    });
}

criterion_group!(
    benches,
    validate_decimal32_benchmark,
    validate_decimal64_benchmark,
    validate_decimal128_benchmark,
    validate_decimal256_benchmark,
);
criterion_main!(benches);
