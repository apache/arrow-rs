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

use criterion::{black_box, Criterion};
use rand::RngCore;
use arrow::compute::kernels::bitwise::{bitwise_and, bitwise_and_scalar};
use arrow::datatypes::Int64Type;

extern crate arrow;

use arrow::util::bench_util::create_primitive_array;
use arrow::util::test_util::seedable_rng;


fn bitwise_array_benchmark(c: &mut Criterion) {
    let size = 64 * 1024_usize;
    // array and
    let left = create_primitive_array::<Int64Type>(size, 0 as f32);
    let right = create_primitive_array::<Int64Type>(size, 0 as f32);
    let mut group = c.benchmark_group("bench bitwise array: and");
    group.bench_function("bitwise array and, no nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and(&left, &right).unwrap())
        })
    });
    let left = create_primitive_array::<Int64Type>(size, 0.2 as f32);
    let right = create_primitive_array::<Int64Type>(size, 0.2 as f32);
    group.bench_function("bitwise array and, 20% nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and(&left, &right).unwrap())
        })
    });
    group.finish();
    // array or
    let mut group = c.benchmark_group("bench bitwise: or");
    group.bench_function("bitwise array or, no nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and(&left, &right).unwrap())
        })
    });
    let left = create_primitive_array::<Int64Type>(size, 0.2 as f32);
    let right = create_primitive_array::<Int64Type>(size, 0.2 as f32);
    group.bench_function("bitwise array or, 20% nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and(&left, &right).unwrap())
        })
    });
    group.finish();
    // xor
    let mut group = c.benchmark_group("bench bitwise: xor");
    // not
    let mut group = c.benchmark_group("bench bitwise: not");
}

fn bitwise_array_scalar_benchmark(c: &mut Criterion) {
    let size = 64 * 1024_usize;
    // array scalar and
    let array = create_primitive_array::<Int64Type>(size, 0 as f32);
    let scalar = seedable_rng().next_u64() as i64;
    let mut group = c.benchmark_group("bench bitwise array scalar: and");
    group.bench_function("bitwise array scalar and, no nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and_scalar(&array, scalar).unwrap())
        })
    });
    let array = create_primitive_array::<Int64Type>(size, 0.2 as f32);
    group.bench_function("bitwise array and, 20% nulls", |b| {
        b.iter(|| {
            black_box(bitwise_and_scalar(&array, scalar).unwrap())
        })
    });
    group.finish();
    // array scalar or

    // array scalar xor
}

criterion_group!(benches, bitwise_benchmark);
criterion_main!(benches);