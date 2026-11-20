// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow::array::*;
use std::sync::Arc;

fn create_array_slice(array: &ArrayRef, length: usize) -> ArrayRef {
    array.slice(0, length)
}

fn create_array_with_nulls(size: usize) -> ArrayRef {
    let array: Float64Array = (0..size)
        .map(|i| if i % 2 == 0 { Some(1.0) } else { None })
        .collect();
    Arc::new(array)
}

fn array_slice_benchmark(c: &mut Criterion) {
    let array = create_array_with_nulls(4096);
    c.bench_function("array_slice 128", |b| {
        b.iter(|| create_array_slice(&array, 128))
    });
    c.bench_function("array_slice 512", |b| {
        b.iter(|| create_array_slice(&array, 512))
    });
    c.bench_function("array_slice 2048", |b| {
        b.iter(|| create_array_slice(&array, 2048))
    });
}

criterion_group!(benches, array_slice_benchmark);
criterion_main!(benches);
