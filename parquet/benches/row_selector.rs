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
use parquet::arrow::arrow_reader::{BooleanRowSelection, RowSelection};
use rand::Rng;

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
    let mut rng = rand::thread_rng();
    let bools: Vec<bool> = (0..total_rows)
        .map(|_| rng.gen_bool(selection_ratio))
        .collect();
    BooleanArray::from(bools)
}

fn criterion_benchmark(c: &mut Criterion) {
    let total_rows = 300_000;
    let selection_ratios = [0.000_01, 0.001, 0.1, 0.3];

    for ratio in selection_ratios {
        let slice_selection_a =
            RowSelection::from_filters(&[generate_random_row_selection(total_rows, ratio)]);
        let slice_selection_b =
            RowSelection::from_filters(&[generate_random_row_selection(total_rows, ratio)]);

        let boolean_selection_a = BooleanRowSelection::from(slice_selection_a.clone());
        let boolean_selection_b = BooleanRowSelection::from(slice_selection_b.clone());

        // Benchmark the intersection of the two RowSelections.
        c.bench_function(&format!("slice intersection {}", ratio), |b| {
            b.iter(|| {
                let intersection = slice_selection_a.intersection(&slice_selection_b);
                criterion::black_box(intersection);
            })
        });

        c.bench_function(&format!("boolean intersection {}", ratio), |b| {
            b.iter(|| {
                let intersection = boolean_selection_a.intersection(&boolean_selection_b);
                criterion::black_box(intersection);
            })
        });

        c.bench_function(&format!("slice union {}", ratio), |b| {
            b.iter(|| {
                let union = slice_selection_a.union(&slice_selection_b);
                criterion::black_box(union);
            })
        });

        c.bench_function(&format!("boolean union {}", ratio), |b| {
            b.iter(|| {
                let union = boolean_selection_a.union(&boolean_selection_b);
                criterion::black_box(union);
            })
        });

        c.bench_function(&format!("slice from_filters {}", ratio), |b| {
            let boolean_array = generate_random_row_selection(total_rows, ratio);
            b.iter(|| {
                let array = boolean_array.clone();
                let selection = RowSelection::from_filters(&[array]);
                criterion::black_box(selection);
            })
        });

        c.bench_function(&format!("boolean from_filters {}", ratio), |b| {
            let boolean_array = generate_random_row_selection(total_rows, ratio);
            b.iter(|| {
                let array = boolean_array.clone();
                let selection = BooleanRowSelection::from_filters(&[array]);
                criterion::black_box(selection);
            })
        });

        c.bench_function(&format!("slice and_then {}", ratio), |b| {
            let selected = slice_selection_a.row_count();
            let sub_selection =
                RowSelection::from_filters(&[generate_random_row_selection(selected, ratio)]);
            b.iter(|| {
                let result = slice_selection_a.and_then(&sub_selection);
                criterion::black_box(result);
            })
        });

        c.bench_function(&format!("boolean and_then {}", ratio), |b| {
            let selected = boolean_selection_a.row_count();
            let sub_selection =
                BooleanRowSelection::from_filters(&[generate_random_row_selection(
                    selected, ratio,
                )]);
            b.iter(|| {
                let result = boolean_selection_a.and_then(&sub_selection);
                criterion::black_box(result);
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
