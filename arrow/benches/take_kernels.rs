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

use arrow::compute::{TakeOptions, take};
use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};
use std::hint;

fn create_random_index(size: usize, null_density: f32) -> UInt32Array {
    let mut rng = seedable_rng();
    let mut builder = UInt32Builder::with_capacity(size);
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let value = rng.random_range::<u32, _>(0u32..size as u32);
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn bench_take(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(take(values, indices, None).unwrap());
}

fn bench_take_bounds_check(values: &dyn Array, indices: &UInt32Array) {
    hint::black_box(take(values, indices, Some(TakeOptions { check_bounds: true })).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_fsb_array(1024, 0.0, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take fsb value len: 12, indices: 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_fsb_array(1024, 0.5, 12);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take fsb value len: 12, null values, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_fsb_array(1024, 0.0, 16);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take fsb value optimized len: 16, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );

    let values = create_fsb_array(1024, 0.5, 16);
    let indices = create_random_index(1024, 0.0);
    c.bench_function(
        "take fsb value optimized len: 16, null values, indices: 1024",
        |b| b.iter(|| bench_take(&values, &indices)),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
