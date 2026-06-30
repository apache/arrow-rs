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

//! Benchmarks for the `cleanup_non_empty_nulls` kernel.
//!
//! The kernel rewrites a variable-length array so that its null entries point
//! to empty offset ranges. The interesting workload is therefore an array
//! whose null entries still reference real data in the values buffer - the
//! state these benches construct via `inject_nulls`.

use std::hint;
use std::sync::Arc;

use arrow::array::*;
use arrow::util::bench_util::*;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::cleanup_non_empty_nulls::{cleanup_non_empty_nulls, has_non_empty_nulls};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const SIZE: usize = 8192;
const NULL_DENSITIES: &[f32] = &[0.1, 0.5];
const SEED: u64 = 42;

/// Build a random `NullBuffer` of the given length and null density.
fn random_nulls(len: usize, null_density: f32, seed: u64) -> NullBuffer {
    let mut rng = StdRng::seed_from_u64(seed);
    NullBuffer::from_iter((0..len).map(|_| rng.random::<f32>() >= null_density))
}

/// Overlay a fresh null mask on an array's existing offsets/values. The new
/// nulls land on top of positions whose offset ranges are non-empty - exactly
/// the case `cleanup_non_empty_nulls` exists to fix.
fn inject_nulls_string(array: StringArray, null_density: f32) -> StringArray {
    let (offsets, values, _) = array.into_parts();
    let nulls = random_nulls(offsets.len() - 1, null_density, SEED + 1);
    StringArray::new(offsets, values, Some(nulls))
}

fn inject_nulls_list(array: ListArray, null_density: f32) -> ListArray {
    let (field, offsets, values, _) = array.into_parts();
    let nulls = random_nulls(offsets.len() - 1, null_density, SEED + 1);
    ListArray::new(field, offsets, values, Some(nulls))
}

/// Build a `ListArray` of `size` entries with random lengths in `length_range`.
/// Set the range's lower bound to 1 to forbid empty entries; use 0 to allow them.
fn build_list_array(size: usize, length_range: std::ops::Range<usize>) -> ListArray {
    let mut rng = StdRng::seed_from_u64(SEED);
    let lengths: Vec<usize> = (0..size)
        .map(|_| rng.random_range(length_range.clone()))
        .collect();
    let total: usize = lengths.iter().sum();
    let values: arrow_array::Int32Array = (0..total as i32).collect();
    let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    ListArray::new(field, offsets, Arc::new(values), None)
}

/// Build a string array containing no zero-length entries.
fn build_string_array_no_empty(size: usize) -> StringArray {
    create_string_array_with_len_range_and_prefix_and_seed::<i32>(size, 0.0, 1, 16, "", SEED)
}

fn bench_pair(c: &mut Criterion, name: &str, array: ArrayRef) {
    c.bench_function(&format!("has_non_empty_nulls {name}"), |b| {
        b.iter(|| hint::black_box(has_non_empty_nulls(array.as_ref()).unwrap()))
    });
    c.bench_function(&format!("cleanup_non_empty_nulls {name}"), |b| {
        b.iter(|| hint::black_box(cleanup_non_empty_nulls(Arc::clone(&array)).unwrap()))
    });
}

fn add_benchmark(c: &mut Criterion) {
    // For each type and null density, bench two input shapes:
    //   * "N% nulls"                   - array also contains some empty entries
    //   * "N% nulls, no empty entries" - every entry has length >= 1

    for &density in NULL_DENSITIES {
        let pct = (density * 100.0).round() as u32;

        // ----- String -----
        let string_mixed = inject_nulls_string(create_string_array::<i32>(SIZE, 0.0), density);
        let string_no_empty = inject_nulls_string(build_string_array_no_empty(SIZE), density);
        bench_pair(c, &format!("string {pct}% nulls"), Arc::new(string_mixed));
        bench_pair(
            c,
            &format!("string {pct}% nulls, no empty entries"),
            Arc::new(string_no_empty),
        );

        // ----- List -----
        let list_mixed = inject_nulls_list(build_list_array(SIZE, 0..8), density);
        let list_no_empty = inject_nulls_list(build_list_array(SIZE, 1..8), density);
        bench_pair(c, &format!("list {pct}% nulls"), Arc::new(list_mixed));
        bench_pair(
            c,
            &format!("list {pct}% nulls, no empty entries"),
            Arc::new(list_no_empty),
        );
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
