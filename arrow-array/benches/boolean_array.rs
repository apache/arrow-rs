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

use arrow_array::BooleanArray;
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    for len in [64, 1024, 65536] {
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
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
