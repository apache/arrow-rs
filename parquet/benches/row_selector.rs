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
use parquet::arrow::arrow_reader::RowSelection;
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
/// * A `RowSelection` instance with randomly selected rows based on the provided ratio.
fn generate_random_row_selection(total_rows: usize, selection_ratio: f64) -> RowSelection {
    let mut rng = rand::thread_rng();
    let bools: Vec<bool> = (0..total_rows)
        .map(|_| rng.gen_bool(selection_ratio))
        .collect();
    let boolean_array = BooleanArray::from(bools);
    RowSelection::from_filters(&[boolean_array])
}

fn criterion_benchmark(c: &mut Criterion) {
    let total_rows = 300_000;
    let selection_ratio = 1.0 / 3.0;

    // Generate two random RowSelections with approximately 1/3 of the rows selected.
    let row_selection_a = generate_random_row_selection(total_rows, selection_ratio);
    let row_selection_b = generate_random_row_selection(total_rows, selection_ratio);

    // Benchmark the intersection of the two RowSelections.
    c.bench_function("intersection", |b| {
        b.iter(|| {
            let intersection = row_selection_a.intersection(&row_selection_b);
            criterion::black_box(intersection);
        })
    });

    c.bench_function("union", |b| {
        b.iter(|| {
            let union = row_selection_a.union(&row_selection_b);
            criterion::black_box(union);
        })
    });

    c.bench_function("from_filters", |b| {
        let mut rng = rand::thread_rng();
        let bools: Vec<bool> = (0..total_rows)
            .map(|_| rng.gen_bool(selection_ratio))
            .collect();
        let boolean_array = BooleanArray::from(bools);
        b.iter(|| {
            let array = boolean_array.clone();
            let selection = RowSelection::from_filters(&[array]);
            criterion::black_box(selection);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
