// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow::{array::*, compute::kernels::length::bit_length};
use std::hint;

fn bench_bit_length(array: &StringArray) {
    hint::black_box(bit_length(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
        [&v[..], &v[..]].concat()
    }

    // double ["hello", " ", "world", "!"] 10 times
    let mut values = vec!["one", "on", "o", ""];
    for _ in 0..10 {
        values = double_vec(values);
    }
    let array = StringArray::from(values);

    c.bench_function("bit_length", |b| b.iter(|| bench_bit_length(&array)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
