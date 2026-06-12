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

//! Benchmarks for the internal `filter_bits` kernel.
//!
//! `filter_bits` is private, so it is exercised through
//! [`FilterPredicate::filter`] on a [`BooleanArray`] without nulls, which
//! dispatches directly to `filter_bits` on the array's value buffer.
//!
//! The filter selectivity determines which `IterationStrategy` is used:
//! selectivity above 0.8 selects `SlicesIterator`, below selects
//! `IndexIterator`, and [`FilterBuilder::optimize`] converts these into their
//! precomputed `Slices` / `Indices` counterparts.

use arrow_array::BooleanArray;
use arrow_select::filter::{FilterBuilder, FilterPredicate};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint;

fn create_boolean_array(size: usize, true_density: f64, rng: &mut StdRng) -> BooleanArray {
    (0..size)
        .map(|_| Some(rng.random_bool(true_density)))
        .collect()
}

fn bench_filter_bits(predicate: &FilterPredicate, array: &BooleanArray) {
    hint::black_box(predicate.filter(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    const SIZE: usize = 65536;
    let mut rng = StdRng::seed_from_u64(42);

    let data = create_boolean_array(SIZE, 0.5, &mut rng);

    // Slice off a non-byte-aligned prefix to exercise the bit offset handling
    // in `filter_bits`
    let padded = create_boolean_array(SIZE + 3, 0.5, &mut rng);
    let sliced = padded.slice(3, SIZE);

    // (label, true_density): densities above the 0.8 selectivity threshold use
    // the slices strategies, those below use the index strategies
    let cases = [
        ("slices, kept 1023/1024", 1.0 - 1.0 / 1024.0),
        ("slices, kept 9/10", 0.9),
        ("indices, kept 1/2", 0.5),
        ("indices, kept 1/10", 0.1),
        ("indices, kept 1/1024", 1.0 / 1024.0),
    ];

    for (label, true_density) in cases {
        let filter_array = create_boolean_array(SIZE, true_density, &mut rng);

        // Lazy strategies: SlicesIterator / IndexIterator
        let lazy = FilterBuilder::new(&filter_array).build();
        // Precomputed strategies: Slices / Indices
        let optimized = FilterBuilder::new(&filter_array).optimize().build();

        c.bench_function(&format!("filter_bits ({label})"), |b| {
            b.iter(|| bench_filter_bits(&lazy, &data))
        });

        c.bench_function(&format!("filter_bits optimized ({label})"), |b| {
            b.iter(|| bench_filter_bits(&optimized, &data))
        });

        c.bench_function(&format!("filter_bits sliced ({label})"), |b| {
            b.iter(|| bench_filter_bits(&lazy, &sliced))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
