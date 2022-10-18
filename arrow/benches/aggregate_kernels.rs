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
use criterion::{Criterion, Throughput};

extern crate arrow;

use arrow::compute::kernels::aggregate::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};

fn bench_sum(arr_a: &Float32Array) {
    criterion::black_box(sum(arr_a).unwrap());
}

fn bench_min(arr_a: &Float32Array) {
    criterion::black_box(min(arr_a).unwrap());
}

fn bench_max(arr_a: &Float32Array) {
    criterion::black_box(max(arr_a).unwrap());
}

fn bench_min_string(arr_a: &StringArray) {
    criterion::black_box(min_string(arr_a).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_primitive_array::<Float32Type>(4096, 0.0);
    {
        let mut g = c.benchmark_group("float");
        g.throughput(Throughput::Bytes(4096 * std::mem::size_of::<f32>() as u64));

        g.bench_function("sum 4096", |b| b.iter(|| bench_sum(&arr_a)));
        g.bench_function("min 4096", |b| b.iter(|| bench_min(&arr_a)));
        g.bench_function("max 4096", |b| b.iter(|| bench_max(&arr_a)));

        let arr_a = create_primitive_array::<Float32Type>(4096, 0.5);

        g.bench_function("sum nulls 4096", |b| b.iter(|| bench_sum(&arr_a)));
        g.bench_function("min nulls 4096", |b| b.iter(|| bench_min(&arr_a)));
        g.bench_function("max nulls 4096", |b| b.iter(|| bench_max(&arr_a)));
    }

    {
        const STRING_LEN: usize = 4;

        let mut g = c.benchmark_group("string");
        g.throughput(Throughput::Bytes(
            4096 * (std::mem::size_of::<i32>() + STRING_LEN) as u64,
        ));

        let arr_b = create_string_array_with_len::<i32>(4096, 0.0, STRING_LEN);
        g.bench_function("min string 4096", |b| b.iter(|| bench_min_string(&arr_b)));

        let arr_b = create_string_array::<i32>(4096, 0.5);
        g.bench_function("min nulls string 4096", |b| {
            b.iter(|| bench_min_string(&arr_b))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
