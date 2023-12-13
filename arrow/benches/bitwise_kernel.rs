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

use arrow::compute::kernels::bitwise::{
    bitwise_and, bitwise_and_scalar, bitwise_not, bitwise_or, bitwise_or_scalar, bitwise_xor,
    bitwise_xor_scalar,
};
use arrow::datatypes::Int64Type;
use criterion::{black_box, Criterion};
use rand::RngCore;

extern crate arrow;

use arrow::util::bench_util::create_primitive_array;
use arrow::util::test_util::seedable_rng;

fn bitwise_array_benchmark(c: &mut Criterion) {
    let size = 64 * 1024_usize;
    let left_without_null = create_primitive_array::<Int64Type>(size, 0 as f32);
    let right_without_null = create_primitive_array::<Int64Type>(size, 0 as f32);
    let left_with_null = create_primitive_array::<Int64Type>(size, 0.2_f32);
    let right_with_null = create_primitive_array::<Int64Type>(size, 0.2_f32);
    // array and
    let mut group = c.benchmark_group("bench bitwise array: and");
    group.bench_function("bitwise array and, no nulls", |b| {
        b.iter(|| black_box(bitwise_and(&left_without_null, &right_without_null).unwrap()))
    });
    group.bench_function("bitwise array and, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_and(&left_with_null, &right_with_null).unwrap()))
    });
    group.finish();
    // array or
    let mut group = c.benchmark_group("bench bitwise: or");
    group.bench_function("bitwise array or, no nulls", |b| {
        b.iter(|| black_box(bitwise_or(&left_without_null, &right_without_null).unwrap()))
    });
    group.bench_function("bitwise array or, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_or(&left_with_null, &right_with_null).unwrap()))
    });
    group.finish();
    // xor
    let mut group = c.benchmark_group("bench bitwise: xor");
    group.bench_function("bitwise array xor, no nulls", |b| {
        b.iter(|| black_box(bitwise_xor(&left_without_null, &right_without_null).unwrap()))
    });
    group.bench_function("bitwise array xor, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_xor(&left_with_null, &right_with_null).unwrap()))
    });
    group.finish();
    // not
    let mut group = c.benchmark_group("bench bitwise: not");
    group.bench_function("bitwise array not, no nulls", |b| {
        b.iter(|| black_box(bitwise_not(&left_without_null).unwrap()))
    });
    group.bench_function("bitwise array not, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_not(&left_with_null).unwrap()))
    });
    group.finish();
}

fn bitwise_array_scalar_benchmark(c: &mut Criterion) {
    let size = 64 * 1024_usize;
    let array_without_null = create_primitive_array::<Int64Type>(size, 0 as f32);
    let array_with_null = create_primitive_array::<Int64Type>(size, 0.2_f32);
    let scalar = seedable_rng().next_u64() as i64;
    // array scalar and
    let mut group = c.benchmark_group("bench bitwise array scalar: and");
    group.bench_function("bitwise array scalar and, no nulls", |b| {
        b.iter(|| black_box(bitwise_and_scalar(&array_without_null, scalar).unwrap()))
    });
    group.bench_function("bitwise array and, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_and_scalar(&array_with_null, scalar).unwrap()))
    });
    group.finish();
    // array scalar or
    let mut group = c.benchmark_group("bench bitwise array scalar: or");
    group.bench_function("bitwise array scalar or, no nulls", |b| {
        b.iter(|| black_box(bitwise_or_scalar(&array_without_null, scalar).unwrap()))
    });
    group.bench_function("bitwise array scalar or, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_or_scalar(&array_with_null, scalar).unwrap()))
    });
    group.finish();
    // array scalar xor
    let mut group = c.benchmark_group("bench bitwise array scalar: xor");
    group.bench_function("bitwise array scalar xor, no nulls", |b| {
        b.iter(|| black_box(bitwise_xor_scalar(&array_without_null, scalar).unwrap()))
    });
    group.bench_function("bitwise array scalar xor, 20% nulls", |b| {
        b.iter(|| black_box(bitwise_xor_scalar(&array_with_null, scalar).unwrap()))
    });
    group.finish();
}

criterion_group!(
    benches,
    bitwise_array_benchmark,
    bitwise_array_scalar_benchmark
);
criterion_main!(benches);
