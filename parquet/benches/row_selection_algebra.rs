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

//! Equal-length intersection/union benchmarks without a precomputed RLE cache.
//! Timings include result allocation and destruction, but exclude input construction.

use std::hint;

use arrow_buffer::BooleanBuffer;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use rand::{RngExt, SeedableRng, rngs::StdRng};

fn mask(rows: usize, selected: impl FnMut(usize) -> bool) -> RowSelection {
    RowSelection::from_boolean_buffer(BooleanBuffer::from_iter((0..rows).map(selected)))
}

fn selectors(rows: usize, run_rows: usize) -> RowSelection {
    RowSelection::from(
        (0..rows)
            .step_by(run_rows)
            .map(|start| RowSelector {
                row_count: run_rows.min(rows - start),
                skip: (start / run_rows).is_multiple_of(2),
            })
            .collect::<Vec<_>>(),
    )
}

fn page_selectors(rows: usize, selectivity: f64, rng: &mut StdRng) -> RowSelection {
    let mut remaining = rows;
    let mut pages = Vec::new();
    while remaining > 0 {
        let row_count = rng.random_range(512..=8192).min(remaining);
        pages.push(RowSelector {
            row_count,
            skip: !rng.random_bool(selectivity),
        });
        remaining -= row_count;
    }
    RowSelection::from(pages)
}

fn bench_selection_algebra(c: &mut Criterion) {
    const ROWS: usize = 1_000_000;
    let coarse = selectors(ROWS, 4096);
    let mut rng = StdRng::seed_from_u64(42);

    let cases = [
        ("alternating_1m", mask(ROWS, |i| i % 2 == 0), coarse.clone()),
        (
            "alternating_3m",
            mask(3_000_000, |i| i % 2 == 0),
            selectors(3_000_000, 4096),
        ),
        (
            "random50",
            mask(ROWS, |_| rng.random_bool(0.5)),
            coarse.clone(),
        ),
        (
            "contiguous50",
            mask(ROWS, |i| (ROWS / 4..ROWS * 3 / 4).contains(&i)),
            coarse.clone(),
        ),
        (
            "coarse50",
            mask(ROWS, |i| (i / 4096) % 2 == 0),
            coarse.clone(),
        ),
        (
            "random10_variable_pages10",
            mask(ROWS, |_| rng.random_bool(0.1)),
            page_selectors(ROWS, 0.1, &mut rng),
        ),
        (
            "random50_variable_pages50",
            mask(ROWS, |_| rng.random_bool(0.5)),
            page_selectors(ROWS, 0.5, &mut rng),
        ),
        (
            "random90_variable_pages90",
            mask(ROWS, |_| rng.random_bool(0.9)),
            page_selectors(ROWS, 0.9, &mut rng),
        ),
        (
            "compact_1m",
            mask(ROWS, |i| i < ROWS / 100),
            RowSelection::from(vec![RowSelector::select(ROWS)]),
        ),
        (
            "sparse_prefix64_1m",
            mask(ROWS, |i| i < 64),
            RowSelection::from(vec![RowSelector::select(ROWS)]),
        ),
        (
            "fragmented_selectors",
            RowSelection::from_boolean_buffer(BooleanBuffer::new_set(ROWS)),
            selectors(ROWS, 8),
        ),
        (
            "offset7_equal",
            RowSelection::from_boolean_buffer(
                BooleanBuffer::from_iter((0..ROWS + 7).map(|i| i % 2 == 0)).slice(7, ROWS),
            ),
            coarse.clone(),
        ),
        (
            "offset63_equal",
            RowSelection::from_boolean_buffer(
                BooleanBuffer::from_iter((0..ROWS + 63).map(|i| i % 2 == 0)).slice(63, ROWS),
            ),
            coarse.clone(),
        ),
    ];

    let mut group = c.benchmark_group("row_selection_algebra");
    for (name, mask, selectors) in cases {
        let rows = mask.total_row_count();
        assert_eq!(rows, selectors.total_row_count(), "{name}: unequal lengths");
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new("intersection", name), |b| {
            b.iter(|| hint::black_box(&mask).intersection(hint::black_box(&selectors)));
        });
        group.bench_function(BenchmarkId::new("union", name), |b| {
            b.iter(|| hint::black_box(&mask).union(hint::black_box(&selectors)));
        });
    }

    let initial = mask(ROWS, |_| rng.random_bool(0.5));
    let conditions = [
        page_selectors(ROWS, 0.5, &mut rng),
        mask(ROWS, |_| rng.random_bool(0.9)),
        page_selectors(ROWS, 0.9, &mut rng),
    ];
    for condition in &conditions {
        assert_eq!(initial.total_row_count(), condition.total_row_count());
    }
    // Each iteration measures all three operations.
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function(BenchmarkId::new("intersection", "chain_mixed_3"), |b| {
        b.iter(|| {
            let mut result =
                hint::black_box(&initial).intersection(hint::black_box(&conditions[0]));
            for condition in &conditions[1..] {
                result = result.intersection(hint::black_box(condition));
            }
            result
        });
    });
    group.bench_function(BenchmarkId::new("union", "chain_mixed_3"), |b| {
        b.iter(|| {
            let mut result = hint::black_box(&initial).union(hint::black_box(&conditions[0]));
            for condition in &conditions[1..] {
                result = result.union(hint::black_box(condition));
            }
            result
        });
    });
    group.finish();
}

criterion_group!(benches, bench_selection_algebra);
criterion_main!(benches);
