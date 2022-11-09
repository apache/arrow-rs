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
use arrow_buffer::i256;
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
            let mut builder = Int64Builder::with_capacity(64);
            for _ in 0..NUM_BATCHES {
                builder.append_slice(&data[..]);
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
            let mut builder = UInt8Builder::with_capacity(64);
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                builder.append_null();
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
            let mut builder = BooleanBuilder::with_capacity(64);
            for _ in 0..NUM_BATCHES {
                builder.append_slice(&data[..]);
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_string(c: &mut Criterion) {
    const SAMPLE_STRING: &str = "sample string";
    let mut group = c.benchmark_group("bench_primitive");
    group.throughput(Throughput::Bytes(
        ((BATCH_SIZE * NUM_BATCHES * SAMPLE_STRING.len()) as u32).into(),
    ));
    group.bench_function("bench_string", |b| {
        b.iter(|| {
            let mut builder = StringBuilder::new();
            for _ in 0..NUM_BATCHES * BATCH_SIZE {
                builder.append_value(SAMPLE_STRING);
            }
            black_box(builder.finish());
        })
    });
    group.finish();
}

fn bench_decimal128(c: &mut Criterion) {
    c.bench_function("bench_decimal128_builder", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut decimal_builder = Decimal128Builder::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                decimal_builder.append_value(rng.gen_range::<i128, _>(0..9999999999));
            }
            black_box(
                decimal_builder
                    .finish()
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            );
        })
    });
}

fn bench_decimal256(c: &mut Criterion) {
    c.bench_function("bench_decimal128_builder", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut decimal_builder = Decimal256Builder::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                decimal_builder.append_value(i256::from_i128(
                    rng.gen_range::<i128, _>(0..99999999999),
                ));
            }
            black_box(
                decimal_builder
                    .finish()
                    .with_precision_and_scale(76, 10)
                    .unwrap(),
            );
        })
    });
}

criterion_group!(
    benches,
    bench_primitive,
    bench_primitive_nulls,
    bench_bool,
    bench_string,
    bench_decimal128,
    bench_decimal256,
);
criterion_main!(benches);
