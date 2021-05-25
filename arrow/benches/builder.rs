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

extern crate arrow;
extern crate criterion;
extern crate rand;

use std::mem::size_of;

use criterion::*;
use rand::distributions::Standard;

use arrow::array::*;
use arrow::util::test_util::seedable_rng;
use rand::Rng;

// Build arrays with 512k elements.
const BATCH_SIZE: usize = 8 << 10;
const NUM_BATCHES: usize = 64;

fn bench_primitive(c: &mut Criterion) {
    let data: [i64; BATCH_SIZE] = [100; BATCH_SIZE];

    let mut group = c.benchmark_group("bench_primitive");
    group.throughput(Throughput::Bytes(
        ((data.len() * NUM_BATCHES * size_of::<i64>()) as u32).into(),
    ));
    group.bench_function("bench_primitive", |b| {
        b.iter(|| {
            let mut builder = Int64Builder::new(64);
            for _ in 0..NUM_BATCHES {
                let _ = black_box(builder.append_slice(&data[..]));
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_primitive_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_primitive_nulls");
    group.bench_function("bench_primitive_nulls", |b| {
        b.iter(|| {
            let mut builder = UInt8Builder::new(64);
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                let _ = black_box(builder.append_null());
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_bool(c: &mut Criterion) {
    let data: Vec<bool> = seedable_rng()
        .sample_iter(&Standard)
        .take(BATCH_SIZE)
        .collect();
    let data_len = data.len();

    let mut group = c.benchmark_group("bench_bool");
    group.throughput(Throughput::Bytes(
        ((data_len * NUM_BATCHES * size_of::<bool>()) as u32).into(),
    ));
    group.bench_function("bench_bool", |b| {
        b.iter(|| {
            let mut builder = BooleanBuilder::new(64);
            for _ in 0..NUM_BATCHES {
                let _ = black_box(builder.append_slice(&data[..]));
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_string(c: &mut Criterion) {
    const SAMPLE_STRING: &str = "🎈🎨👗🎱🎯🏆";
    let mut group = c.benchmark_group("bench_string");
    group.throughput(Throughput::Bytes(
        ((BATCH_SIZE * NUM_BATCHES * SAMPLE_STRING.len()) as u32).into(),
    ));
    group.bench_function("bench_string", |b| {
        b.iter(|| {
            let mut builder = StringBuilder::new(64);
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                let _ = black_box(builder.append_value(SAMPLE_STRING));
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_string_slice(c: &mut Criterion) {
    // "🎈🎨👗🎱🎯🏆"
    const SAMPLE_STRING: &'static [u8] = &[
        240, 159, 142, 136, 240, 159, 142, 168, 240, 159, 145, 151, 240, 159, 142, 177,
        240, 159, 142, 175, 240, 159, 143, 134,
    ];
    let mut group = c.benchmark_group("bench_string");
    group.throughput(Throughput::Bytes(
        ((BATCH_SIZE * NUM_BATCHES * SAMPLE_STRING.len()) as u32).into(),
    ));
    group.bench_function("bench_string_slice", |b| {
        b.iter(|| {
            let mut builder = StringBuilder::new(64);
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                let _ = black_box(builder.append_slice(SAMPLE_STRING));
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_string_slice_unchecked(c: &mut Criterion) {
    // "🎈🎨👗🎱🎯🏆"
    const SAMPLE_STRING: &'static [u8] = &[
        240, 159, 142, 136, 240, 159, 142, 168, 240, 159, 145, 151, 240, 159, 142, 177,
        240, 159, 142, 175, 240, 159, 143, 134,
    ];
    let mut group = c.benchmark_group("bench_string");
    group.throughput(Throughput::Bytes(
        ((BATCH_SIZE * NUM_BATCHES * SAMPLE_STRING.len()) as u32).into(),
    ));
    group.bench_function("bench_string_slice_unchecked", |b| {
        b.iter(|| {
            let mut builder = StringBuilder::new(64);
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                let _ =
                    black_box(unsafe { builder.append_slice_unchecked(SAMPLE_STRING) });
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_primitive,
    bench_primitive_nulls,
    bench_bool,
    bench_string,
    bench_string_slice,
    bench_string_slice_unchecked
);
criterion_main!(benches);
