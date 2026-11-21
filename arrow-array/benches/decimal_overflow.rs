// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::builder::{Decimal128Builder, Decimal256Builder};
use arrow_array::{Decimal128Array, Decimal256Array};
use arrow_buffer::{NullBuffer, i256};
use criterion::*;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    let len = 8192;
    let mut builder_128 = Decimal128Builder::with_capacity(len);
    let mut builder_256 = Decimal256Builder::with_capacity(len);
    for i in 0..len {
        if i % 10 == 0 {
            builder_128.append_value(i128::MAX);
            builder_256.append_value(i256::from_i128(i128::MAX));
        } else {
            builder_128.append_value(i as i128);
            builder_256.append_value(i256::from_i128(i as i128));
        }
    }
    let array_128 = builder_128.finish();
    let array_256 = builder_256.finish();

    c.bench_function("validate_decimal_precision_128", |b| {
        b.iter(|| hint::black_box(array_128.validate_decimal_precision(8)));
    });
    c.bench_function("null_if_overflow_precision_128", |b| {
        b.iter(|| hint::black_box(array_128.null_if_overflow_precision(8)));
    });
    c.bench_function("validate_decimal_precision_256", |b| {
        b.iter(|| hint::black_box(array_256.validate_decimal_precision(8)));
    });
    c.bench_function("null_if_overflow_precision_256", |b| {
        b.iter(|| hint::black_box(array_256.null_if_overflow_precision(8)));
    });
}

/// Arrays at scale 2 whose values fit precision 10, except where the name says otherwise, so that
/// `validate_decimal_precision(10)` reads every row. The shapes with nulls have 10% random nulls.
fn validate_fitting_benchmark(c: &mut Criterion) {
    const PRECISION: u8 = 10;
    const OVERFLOW: i128 = 10_i128.pow(12);
    let len = 8192;
    let mut rng = StdRng::seed_from_u64(42);
    let fits: Vec<i128> = (0..len as i128).map(|i| (i % 100_000) * 100).collect();
    let nulls: NullBuffer = (0..len).map(|_| !rng.random_bool(0.1)).collect();
    let null_slots_overflow = nulls
        .iter()
        .zip(&fits)
        .map(|(valid, &v)| if valid { v } else { OVERFLOW })
        .collect();
    let mut last_row_overflow = fits.clone();
    last_row_overflow[len - 1] = OVERFLOW;

    let shapes = [
        ("fits", fits.clone(), None),
        ("fits nulls", fits, Some(nulls.clone())),
        ("null slots overflow", null_slots_overflow, Some(nulls)),
        ("last row overflow", last_row_overflow, None),
    ];
    for (name, values, nulls) in shapes {
        let values_256 = values.iter().map(|&v| i256::from_i128(v)).collect();
        let array_128 = Decimal128Array::new(values.into(), nulls.clone())
            .with_precision_and_scale(38, 2)
            .unwrap();
        let array_256 = Decimal256Array::new(values_256, nulls)
            .with_precision_and_scale(76, 2)
            .unwrap();

        c.bench_function(&format!("validate_decimal_precision_128 {name}"), |b| {
            b.iter(|| hint::black_box(&array_128).validate_decimal_precision(PRECISION));
        });
        c.bench_function(&format!("validate_decimal_precision_256 {name}"), |b| {
            b.iter(|| hint::black_box(&array_256).validate_decimal_precision(PRECISION));
        });
    }
}

criterion_group!(benches, criterion_benchmark, validate_fitting_benchmark);
criterion_main!(benches);
