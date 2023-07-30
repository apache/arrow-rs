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
use arrow_array::types::{
    Float64Type, TimestampMillisecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::ArrowNumericType;
use criterion::Criterion;

extern crate arrow;

use arrow::compute::kernels::aggregate::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use rand::distributions::Standard;
use rand::prelude::Distribution;

fn bench_sum<T: ArrowNumericType>(arr_a: &PrimitiveArray<T>) {
    criterion::black_box(sum(arr_a).unwrap());
}

fn bench_min<T: ArrowNumericType>(arr_a: &PrimitiveArray<T>) {
    criterion::black_box(min(arr_a).unwrap());
}

fn bench_max<T: ArrowNumericType>(arr_a: &PrimitiveArray<T>) {
    criterion::black_box(max(arr_a).unwrap());
}

fn bench_min_string(arr_a: &StringArray) {
    criterion::black_box(min_string(arr_a).unwrap());
}

fn sum_min_max_bench<T>(
    c: &mut Criterion,
    size: usize,
    null_density: f32,
    description: &str,
) where
    T: ArrowNumericType,
    Standard: Distribution<T::Native>,
{
    let arr_a = create_primitive_array::<T>(size, null_density);

    c.bench_function(&format!("sum {size} {description}"), |b| {
        b.iter(|| bench_sum(&arr_a))
    });
    c.bench_function(&format!("min {size} {description}"), |b| {
        b.iter(|| bench_min(&arr_a))
    });
    c.bench_function(&format!("max {size} {description}"), |b| {
        b.iter(|| bench_max(&arr_a))
    });
}

fn add_benchmark(c: &mut Criterion) {
    sum_min_max_bench::<UInt8Type>(c, 512, 0.0, "u8 no nulls");
    sum_min_max_bench::<UInt8Type>(c, 512, 0.5, "u8 50% nulls");

    sum_min_max_bench::<UInt16Type>(c, 512, 0.0, "u16 no nulls");
    sum_min_max_bench::<UInt16Type>(c, 512, 0.5, "u16 50% nulls");

    sum_min_max_bench::<UInt32Type>(c, 512, 0.0, "u32 no nulls");
    sum_min_max_bench::<UInt32Type>(c, 512, 0.5, "u32 50% nulls");

    sum_min_max_bench::<UInt64Type>(c, 512, 0.0, "u64 no nulls");
    sum_min_max_bench::<UInt64Type>(c, 512, 0.5, "u64 50% nulls");

    sum_min_max_bench::<TimestampMillisecondType>(c, 512, 0.0, "ts_millis no nulls");
    sum_min_max_bench::<TimestampMillisecondType>(c, 512, 0.5, "ts_millis 50% nulls");

    sum_min_max_bench::<Float32Type>(c, 512, 0.0, "f32 no nulls");
    sum_min_max_bench::<Float32Type>(c, 512, 0.5, "f32 50% nulls");

    sum_min_max_bench::<Float64Type>(c, 512, 0.0, "f64 no nulls");
    sum_min_max_bench::<Float64Type>(c, 512, 0.5, "f64 50% nulls");

    let arr_b = create_string_array::<i32>(512, 0.0);
    c.bench_function("min string 512", |b| b.iter(|| bench_min_string(&arr_b)));

    let arr_b = create_string_array::<i32>(512, 0.5);
    c.bench_function("min nulls string 512", |b| {
        b.iter(|| bench_min_string(&arr_b))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
