// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::BooleanArray;
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    for len in [64, 448, 960, 1024, 65536] {
        // All true (no nulls)
        let all_true = BooleanArray::from(vec![true; len]);
        c.bench_function(&format!("true_count(all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&all_true).true_count());
        });
        c.bench_function(&format!("has_true(all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&all_true).has_true());
        });
        c.bench_function(&format!("has_false(all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&all_true).has_false());
        });

        // All false (no nulls)
        let all_false = BooleanArray::from(vec![false; len]);
        c.bench_function(&format!("true_count(all_false, {len})"), |b| {
            b.iter(|| hint::black_box(&all_false).true_count());
        });
        c.bench_function(&format!("has_true(all_false, {len})"), |b| {
            b.iter(|| hint::black_box(&all_false).has_true());
        });
        c.bench_function(&format!("has_false(all_false, {len})"), |b| {
            b.iter(|| hint::black_box(&all_false).has_false());
        });

        // Mixed: first element differs (best-case short-circuit)
        let mut mixed_early: Vec<bool> = vec![true; len];
        mixed_early[0] = false;
        let mixed_early = BooleanArray::from(mixed_early);
        c.bench_function(&format!("true_count(mixed_early, {len})"), |b| {
            b.iter(|| hint::black_box(&mixed_early).true_count());
        });
        c.bench_function(&format!("has_false(mixed_early, {len})"), |b| {
            b.iter(|| hint::black_box(&mixed_early).has_false());
        });

        // With nulls: all valid values true
        let with_nulls: Vec<Option<bool>> = (0..len)
            .map(|i| if i % 10 == 0 { None } else { Some(true) })
            .collect();
        let with_nulls = BooleanArray::from(with_nulls);
        c.bench_function(&format!("true_count(nulls_all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&with_nulls).true_count());
        });
        c.bench_function(&format!("has_true(nulls_all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&with_nulls).has_true());
        });
        c.bench_function(&format!("has_false(nulls_all_true, {len})"), |b| {
            b.iter(|| hint::black_box(&with_nulls).has_false());
        });

        // take_n_true: mixed true/false
        let bool_vec: Vec<bool> = (0..len).map(|idx| idx % 3 != 0).collect();
        c.bench_function(&format!("take_n_true_unique({})", len / 2), |b| {
            b.iter_batched(
                || BooleanArray::from(bool_vec.clone()), // fresh buffer, strong_count == 1
                |arr| hint::black_box(arr).take_n_true(len / 2),
                BatchSize::SmallInput,
            );
        });
        // underlying buffer is shared
        let mixed_boolean_array_shared = BooleanArray::from(bool_vec);
        let shared_array = mixed_boolean_array_shared.clone();
        c.bench_function(&format!("take_n_true_shared({})", len / 2), |b| {
            b.iter_batched(
                || shared_array.clone(), // strong_count >= 2, forces copy path
                |arr| hint::black_box(arr).take_n_true(len / 2),
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
