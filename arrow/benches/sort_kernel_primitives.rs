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
use arrow_ord::sort::sort;
use criterion::Criterion;

use std::sync::Arc;

extern crate arrow;

use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Int64Type};

fn create_i64_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let array = create_primitive_array::<Int64Type>(size, null_density);
    Arc::new(array)
}

fn bench_sort(array: &ArrayRef) {
    criterion::black_box(sort(criterion::black_box(array), None).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_i64_array(2u64.pow(10) as usize, false);

    c.bench_function("sort 2^10", |b| b.iter(|| bench_sort(&arr_a)));

    let arr_a = create_i64_array(2u64.pow(12) as usize, false);

    c.bench_function("sort 2^12", |b| b.iter(|| bench_sort(&arr_a)));

    let arr_a = create_i64_array(2u64.pow(10) as usize, true);

    c.bench_function("sort nulls 2^10", |b| b.iter(|| bench_sort(&arr_a)));

    let arr_a = create_i64_array(2u64.pow(12) as usize, true);

    c.bench_function("sort nulls 2^12", |b| b.iter(|| bench_sort(&arr_a)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
