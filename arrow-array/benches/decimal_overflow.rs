// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::builder::{Decimal128Builder, Decimal256Builder};
use arrow_buffer::i256;
use criterion::*;
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
