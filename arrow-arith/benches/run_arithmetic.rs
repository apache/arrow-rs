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

use std::hint;

use arrow_arith::numeric::add;
use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, Int64Array, RunArray};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const SIZE: usize = 1_000_000;

fn input(run_length: usize, first_run_length: usize) -> (RunArray<Int32Type>, Int64Array) {
    let mut run_ends = Vec::new();
    let mut values = Vec::new();
    let mut dense = Vec::with_capacity(SIZE);
    let mut previous = 0;
    let mut end = first_run_length.min(run_length).clamp(1, SIZE);

    loop {
        let value = 1 + (run_ends.len() % 100) as i64;
        run_ends.push(end as i32);
        values.push(value);
        dense.extend(std::iter::repeat_n(value, end - previous));
        if end == SIZE {
            break;
        }
        previous = end;
        end = (end + run_length).min(SIZE);
    }

    let run_array =
        RunArray::try_new(&Int32Array::from(run_ends), &Int64Array::from(values)).unwrap();
    (run_array, Int64Array::from(dense))
}

fn run_arithmetic(c: &mut Criterion) {
    for run_length in [8, 128, 4096] {
        let (left, left_dense) = input(run_length, run_length);
        let (right, right_dense) = input(run_length, run_length.div_ceil(2));
        let scalar = Int64Array::new_scalar(2);
        let mut group = c.benchmark_group(format!("run_arithmetic/{run_length}"));
        group.throughput(Throughput::Elements(SIZE as u64));

        group.bench_function("ree_scalar", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left), &scalar).unwrap()))
        });
        group.bench_function("dense_scalar", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left_dense), &scalar).unwrap()))
        });
        group.bench_function("ree_aligned", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left), &left).unwrap()))
        });
        group.bench_function("dense_aligned", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left_dense), &left_dense).unwrap()))
        });
        group.bench_function("ree_staggered", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left), &right).unwrap()))
        });
        group.bench_function("dense_staggered", |b| {
            b.iter(|| hint::black_box(add(hint::black_box(&left_dense), &right_dense).unwrap()))
        });
        group.finish();
    }
}

criterion_group!(benches, run_arithmetic);
criterion_main!(benches);
