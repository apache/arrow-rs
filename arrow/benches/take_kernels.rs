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

use rand::Rng;

extern crate arrow;

use arrow::compute::{take, TakeOptions};
use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};

fn create_random_index(size: usize, null_density: f32) -> UInt32Array {
    let mut rng = seedable_rng();
    let mut builder = UInt32Builder::with_capacity(size);
    for _ in 0..size {
        if rng.gen::<f32>() < null_density {
            builder.append_null();
        } else {
            let value = rng.gen_range::<u32, _>(0u32..size as u32);
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn bench_take(values: &dyn Array, indices: &UInt32Array) {
    criterion::black_box(take(values, indices, None).unwrap());
}

fn bench_take_bounds_check(values: &dyn Array, indices: &UInt32Array) {
    criterion::black_box(take(values, indices, Some(TakeOptions { check_bounds: true })).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take i32 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_array::<Int32Type>(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_array::<Int32Type>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take check bounds i32 512", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });
    let values = create_primitive_array::<Int32Type>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take check bounds i32 1024", |b| {
        b.iter(|| bench_take_bounds_check(&values, &indices))
    });

    let values = create_boolean_array(512, 0.0, 0.5);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take bool 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.0, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean_array(1024, 0.5, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take str 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_string_array::<i32>(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take str null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take stringview 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take stringview null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take stringview null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_view_array(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take stringview null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_primitive_run_array::<Int32Type, Int32Type>(1024, 512);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take primitive run logical len: 1024, physical len: 512, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
