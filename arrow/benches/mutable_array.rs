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

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use arrow::array::*;
use arrow::datatypes::Int32Type;
use arrow::util::bench_util::{
    create_string_array_with_len_range_and_prefix_and_seed,
    create_string_view_array_with_len_range_and_seed,
};

/// Each benchmark cycles through this many arrays and lists of slices, one per
/// iteration.
const NUM_INPUTS: usize = 64;

/// `NUM_INPUTS` values made by `make` from the seeds `0..NUM_INPUTS`
fn per_seed<T>(make: impl FnMut(u64) -> T) -> Vec<T> {
    (0..NUM_INPUTS as u64).map(make).collect()
}

/// `size` random strings of 0 to 399 bytes
fn create_strings(size: usize, null_density: f32, seed: u64) -> StringArray {
    create_string_array_with_len_range_and_prefix_and_seed(size, null_density, 0, 399, "", seed)
}

fn create_slices(size: usize, seed: u64) -> Vec<(usize, usize)> {
    let rng = &mut StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            let start = rng.random_range(0..size / 2);
            let end = rng.random_range(start + 1..size);
            (start, end)
        })
        .collect()
}

/// Slices of 1 to 8 rows, like the runs that `zip` copies for a `CASE`
/// expression whose condition changes often.
fn create_short_slices(size: usize, seed: u64) -> Vec<(usize, usize)> {
    let rng = &mut StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            let start = rng.random_range(0..size - 8);
            let len = rng.random_range(1..=8);
            (start, start + len)
        })
        .collect()
}

/// A dictionary array of `size` random keys into 16 random strings
fn create_dict_array(size: usize, seed: u64) -> DictionaryArray<Int32Type> {
    let rng = &mut StdRng::seed_from_u64(seed);
    let keys = Int32Array::from_iter_values((0..size).map(|_| rng.random_range(0..16)));
    DictionaryArray::new(keys, Arc::new(create_strings(16, 0.0, seed)))
}

/// Total number of rows in `slices`
fn total_len(slices: &[(usize, usize)]) -> usize {
    slices.iter().map(|(start, end)| end - start).sum()
}

/// Copies `slices` of `v1`, which hold `len` rows in all, into a
/// `MutableArrayData` that starts with room for all of them.
fn bench<T: Array>(v1: &T, slices: &[(usize, usize)], len: usize) {
    let data = v1.to_data();
    let mut mutable = MutableArrayData::new(vec![&data], false, len);
    for (start, end) in slices {
        mutable.try_extend(0, *start, *end).unwrap();
    }
    mutable.freeze();
}

/// Benchmarks [`bench`], where iteration `i` copies list `i % NUM_INPUTS` of
/// `slices` from array `i % NUM_INPUTS` of `arrays`.
fn bench_inputs<T: Array>(
    c: &mut Criterion,
    name: &str,
    arrays: &[T],
    slices: &[Vec<(usize, usize)>],
) {
    let lens: Vec<usize> = slices.iter().map(|slices| total_len(slices)).collect();
    let mut inputs = arrays.iter().zip(slices).zip(&lens).cycle();
    c.bench_function(name, |b| {
        b.iter(|| {
            let ((array, slices), len) = inputs.next().unwrap();
            bench(array, slices, *len)
        })
    });
}

fn add_benchmark(c: &mut Criterion) {
    let long_slices = per_seed(|seed| create_slices(1024, seed));
    let short_slices = per_seed(|seed| create_short_slices(1024, seed));

    let strings = per_seed(|seed| create_strings(1024, 0.0, seed));
    bench_inputs(c, "mutable str 1024", &strings, &long_slices);

    let strings = per_seed(|seed| create_strings(1024, 0.5, seed));
    bench_inputs(c, "mutable str nulls 1024", &strings, &long_slices);

    let views =
        per_seed(|seed| create_string_view_array_with_len_range_and_seed(1024, 0.0, 0..400, seed));
    bench_inputs(c, "mutable str_view 1024", &views, &long_slices);
    bench_inputs(
        c,
        "mutable str_view short slices 1024",
        &views,
        &short_slices,
    );

    let dicts = per_seed(|seed| create_dict_array(1024, seed));
    bench_inputs(c, "mutable dict 1024", &dicts, &long_slices);
    bench_inputs(c, "mutable dict short slices 1024", &dicts, &short_slices);
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
