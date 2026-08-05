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
use arrow_buffer::BooleanBuffer;
use criterion::*;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use rand::Rng;
use std::hint;

/// Run lengths for the mask conversion benchmarks. Shorter runs mean more
/// [`RowSelector`]s per row, so the RLE encoding dominates.
const MASK_RUN_LENGTHS: &[usize] = &[1, 4, 16, 32, 48, 64, 96, 128];

/// Generates a random RowSelection with a specified selection ratio.
///
/// # Arguments
///
/// * `total_rows` - The total number of rows in the selection.
/// * `selection_ratio` - The ratio of rows to select (e.g., 1/3 for ~33% selection).
///
/// # Returns
///
/// * A `BooleanArray` instance with randomly selected rows based on the provided ratio.
fn generate_random_row_selection(total_rows: usize, selection_ratio: f64) -> BooleanArray {
    let mut rng = rand::rng();
    let bools: Vec<bool> = (0..total_rows)
        .map(|_| rng.random_bool(selection_ratio))
        .collect();
    BooleanArray::from(bools)
}

/// Generates a mask alternating between selected and skipped runs of `run_len` rows.
fn generate_run_length_mask(total_rows: usize, run_len: usize) -> BooleanBuffer {
    BooleanBuffer::from_iter((0..total_rows).map(|row| (row / run_len).is_multiple_of(2)))
}

/// Benchmarks converting a mask-backed [`RowSelection`] into [`RowSelector`]s.
///
/// `RowSelection::iter` caches the RLE form, so a caller that iterates before
/// consuming should reuse that cache rather than encode the bitmap twice.
/// `mask_consume` is the same conversion with a cold cache.
fn bench_mask_backed_conversion(c: &mut Criterion, total_rows: usize, selection_ratio: f64) {
    let mut cases: Vec<(String, BooleanBuffer)> = MASK_RUN_LENGTHS
        .iter()
        .map(|&run_len| {
            (
                format!("run{run_len:02}"),
                generate_run_length_mask(total_rows, run_len),
            )
        })
        .collect();
    cases.push((
        "random".to_string(),
        generate_random_row_selection(total_rows, selection_ratio)
            .values()
            .clone(),
    ));

    for (label, mask) in cases {
        let selection = RowSelection::from_boolean_buffer(mask);

        c.bench_with_input(
            BenchmarkId::new("mask_iterate_then_consume", &label),
            &selection,
            |b, selection| {
                b.iter(|| {
                    // `clone` drops the selector cache, so each iteration
                    // starts from an unconverted selection.
                    let selection = selection.clone();
                    let rows: usize = selection.iter().map(|s| s.row_count).sum();
                    hint::black_box(rows);
                    let selectors: Vec<RowSelector> = selection.into();
                    hint::black_box(selectors);
                })
            },
        );

        c.bench_with_input(
            BenchmarkId::new("mask_consume", &label),
            &selection,
            |b, selection| {
                b.iter(|| {
                    let selectors: Vec<RowSelector> = selection.clone().into();
                    hint::black_box(selectors);
                })
            },
        );
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let total_rows = 300_000;
    let selection_ratio = 1.0 / 3.0;

    // Generate two random RowSelections with approximately 1/3 of the rows selected.
    let row_selection_a =
        RowSelection::from_filters(&[generate_random_row_selection(total_rows, selection_ratio)]);
    let row_selection_b =
        RowSelection::from_filters(&[generate_random_row_selection(total_rows, selection_ratio)]);

    // Benchmark the intersection of the two RowSelections.
    c.bench_function("intersection", |b| {
        b.iter(|| {
            let intersection = row_selection_a.intersection(&row_selection_b);
            hint::black_box(intersection);
        })
    });

    c.bench_function("union", |b| {
        b.iter(|| {
            let union = row_selection_a.union(&row_selection_b);
            hint::black_box(union);
        })
    });

    c.bench_function("from_filters", |b| {
        let boolean_array = generate_random_row_selection(total_rows, selection_ratio);
        b.iter(|| {
            let array = boolean_array.clone();
            let selection = RowSelection::from_filters(&[array]);
            hint::black_box(selection);
        })
    });

    c.bench_function("and_then", |b| {
        let selected = row_selection_a.row_count();
        let sub_selection =
            RowSelection::from_filters(&[generate_random_row_selection(selected, selection_ratio)]);
        b.iter(|| {
            let result = row_selection_a.and_then(&sub_selection);
            hint::black_box(result);
        })
    });

    bench_mask_backed_conversion(c, total_rows, selection_ratio);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
