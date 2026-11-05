// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::StringDictionaryBuilder;
use arrow::datatypes::Int32Type;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::{RngExt, rng};

/// Note: this is best effort, not all keys are necessarily present or unique
fn build_strings(dict_size: usize, total_size: usize, key_len: usize) -> Vec<String> {
    let mut rng = rng();
    let values: Vec<String> = (0..dict_size)
        .map(|_| (0..key_len).map(|_| rng.random::<char>()).collect())
        .collect();

    (0..total_size)
        .map(|_| values[rng.random_range(0..dict_size)].clone())
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_dictionary_builder");

    let mut do_bench = |dict_size: usize, total_size: usize, key_len: usize| {
        group.bench_function(
            format!("(dict_size:{dict_size}, len:{total_size}, key_len: {key_len})"),
            |b| {
                let strings = build_strings(dict_size, total_size, key_len);
                b.iter(|| {
                    let mut builder = StringDictionaryBuilder::<Int32Type>::with_capacity(
                        strings.len(),
                        key_len + 1,
                        (key_len + 1) * dict_size,
                    );

                    for val in &strings {
                        builder.append(val).unwrap();
                    }

                    builder.finish();
                })
            },
        );
    };

    do_bench(20, 1000, 5);
    do_bench(100, 1000, 5);
    do_bench(100, 1000, 10);
    do_bench(100, 10000, 10);
    do_bench(100, 10000, 100);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
