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
use criterion::{BatchSize, Criterion};

use arrow::util::bench_util::create_boolean_array;

extern crate arrow;

use arrow::array::*;
use arrow::compute::kernels::boolean as boolean_kernels;
use std::hint;

fn bench_and(lhs: &BooleanArray, rhs: &BooleanArray) {
    hint::black_box(boolean_kernels::and(lhs, rhs).unwrap());
}

fn bench_or(lhs: &BooleanArray, rhs: &BooleanArray) {
    hint::black_box(boolean_kernels::or(lhs, rhs).unwrap());
}

fn bench_not(array: &BooleanArray) {
    hint::black_box(boolean_kernels::not(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    // allocate arrays of 32K elements
    let size = 2usize.pow(15);

    // Note we allocate all arrays before the benchmark to ensure the allocation of the arrays
    // is not affected by allocations that happen during the benchmarked operation.
    let array1 = create_boolean_array(size, 0.0, 0.5);
    let array2 = create_boolean_array(size, 0.0, 0.5);

    // Slice by 1 (not aligned to byte (8 bit) or word (64 bit) boundaries)
    let offset = 1;
    let array1_sliced_1 = array1.slice(offset, size - offset);
    let array2_sliced_1 = array2.slice(offset, size - offset);

    // Slice by 24 (aligned on byte (8 bit) but not word (64 bit) boundaries)
    let offset = 24;
    let array1_sliced_24 = array1.slice(offset, size - offset);
    let array2_sliced_24 = array2.slice(offset, size - offset);

    c.bench_function("and", |b| b.iter(|| bench_and(&array1, &array2)));
    c.bench_function("or", |b| b.iter(|| bench_or(&array1, &array2)));
    c.bench_function("not", |b| b.iter(|| bench_not(&array1)));

    c.bench_function("and_sliced_1", |b| {
        b.iter(|| bench_and(&array1_sliced_1, &array2_sliced_1))
    });
    c.bench_function("or_sliced_1", |b| {
        b.iter(|| bench_or(&array1_sliced_1, &array2_sliced_1))
    });
    c.bench_function("not_sliced_1", |b| b.iter(|| bench_not(&array1_sliced_1)));

    c.bench_function("and_sliced_24", |b| {
        b.iter(|| bench_and(&array1_sliced_24, &array2_sliced_24))
    });
    c.bench_function("or_sliced_24", |b| {
        b.iter(|| bench_or(&array1_sliced_24, &array2_sliced_24))
    });
    c.bench_function("not_slice_24", |b| b.iter(|| bench_not(&array1_sliced_24)));
}

fn add_take_n_true_benchmark(c: &mut Criterion) {
    let size = 2usize.pow(13); // 8K
    // keep roughly the first half of the set bits
    let n = size / 4;

    // the clone in each iteration shares the values buffer, so `take_n_true`
    // cannot reuse the allocation and must copy the retained prefix
    let array = create_boolean_array(size, 0.0, 0.5);
    c.bench_function("take_n_true shared", |b| {
        b.iter(|| hint::black_box(array.clone().take_n_true(n)))
    });

    // a fresh array per iteration (built in setup, excluded from timing) has
    // an unshared values buffer, allowing the allocation to be reused
    let values: Vec<bool> = array.values().iter().collect();
    c.bench_function("take_n_true unshared", |b| {
        b.iter_batched(
            || BooleanArray::from(values.clone()),
            |array| array.take_n_true(n),
            BatchSize::SmallInput,
        )
    });

    let array_nulls = create_boolean_array(size, 0.1, 0.5);
    c.bench_function("take_n_true nulls shared", |b| {
        b.iter(|| hint::black_box(array_nulls.clone().take_n_true(n)))
    });

    // fewer than n set bits: scans but returns self unchanged
    c.bench_function("take_n_true no truncation", |b| {
        b.iter(|| hint::black_box(array.clone().take_n_true(size)))
    });
}

criterion_group!(benches, add_benchmark, add_take_n_true_benchmark);
criterion_main!(benches);
