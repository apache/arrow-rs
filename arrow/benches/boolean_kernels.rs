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

use arrow::util::bench_util::create_boolean_array;

extern crate arrow;

use arrow::array::*;
use arrow::compute::kernels::boolean as boolean_kernels;
use std::hint;

const ARRAY_LEN: usize = 8_192;
// These bitwise kernels are very cheap, so run them many times per Criterion iteration to
// try and reduce noise in the benchmarks.
const RUNS_PER_SAMPLE: usize = 100;

fn bench_and(lhs: &BooleanArray, rhs: &BooleanArray) {
    for _ in 0..RUNS_PER_SAMPLE {
        hint::black_box(boolean_kernels::and(lhs, rhs).unwrap());
    }
}

fn bench_or(lhs: &BooleanArray, rhs: &BooleanArray) {
    for _ in 0..RUNS_PER_SAMPLE {
        hint::black_box(boolean_kernels::or(lhs, rhs).unwrap());
    }
}

fn bench_not(array: &BooleanArray) {
    for _ in 0..RUNS_PER_SAMPLE {
        hint::black_box(boolean_kernels::not(array).unwrap());
    }
}

fn add_benchmark(c: &mut Criterion) {
    let array1_full = create_boolean_array(ARRAY_LEN + 1, 0.0, 0.5);
    let array2_full = create_boolean_array(ARRAY_LEN + 1, 0.0, 0.5);
    let array1 = array1_full.slice(0, ARRAY_LEN);
    let array2 = array2_full.slice(0, ARRAY_LEN);
    c.bench_function("and", |b| b.iter(|| bench_and(&array1, &array2)));
    c.bench_function("or", |b| b.iter(|| bench_or(&array1, &array2)));
    c.bench_function("not", |b| b.iter(|| bench_not(&array1)));

    let array1_slice = array1_full.slice(1, ARRAY_LEN);
    let array2_slice = array2_full.slice(1, ARRAY_LEN);

    c.bench_function("and_sliced", |b| {
        b.iter(|| bench_and(&array1_slice, &array2_slice))
    });
    c.bench_function("or_sliced", |b| {
        b.iter(|| bench_or(&array1_slice, &array2_slice))
    });
    c.bench_function("not_sliced", |b| b.iter(|| bench_not(&array1_slice)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
