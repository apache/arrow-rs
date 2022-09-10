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

use std::sync::Arc;

extern crate arrow;

use arrow::compute::kernels::sort::{lexsort, SortColumn};
use arrow::compute::sort_to_indices;
use arrow::datatypes::Int32Type;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};

fn create_f32_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let array = create_primitive_array::<Float32Type>(size, null_density);
    Arc::new(array)
}

fn create_bool_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let true_density = 0.5;
    let array = create_boolean_array(size, null_density, true_density);
    Arc::new(array)
}

fn bench_sort(array_a: &ArrayRef, array_b: &ArrayRef, limit: Option<usize>) {
    let columns = vec![
        SortColumn {
            values: array_a.clone(),
            options: None,
        },
        SortColumn {
            values: array_b.clone(),
            options: None,
        },
    ];

    criterion::black_box(lexsort(&columns, limit).unwrap());
}

fn bench_sort_to_indices(array: &ArrayRef, limit: Option<usize>) {
    criterion::black_box(sort_to_indices(array, None, limit).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_f32_array(2u64.pow(10) as usize, false);
    let arr_b = create_f32_array(2u64.pow(10) as usize, false);

    c.bench_function("sort 2^10", |b| b.iter(|| bench_sort(&arr_a, &arr_b, None)));

    let arr_a = create_f32_array(2u64.pow(12) as usize, false);
    let arr_b = create_f32_array(2u64.pow(12) as usize, false);

    c.bench_function("sort 2^12", |b| b.iter(|| bench_sort(&arr_a, &arr_b, None)));

    let arr_a = create_f32_array(2u64.pow(10) as usize, true);
    let arr_b = create_f32_array(2u64.pow(10) as usize, true);

    c.bench_function("sort nulls 2^10", |b| {
        b.iter(|| bench_sort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2u64.pow(12) as usize, true);
    let arr_b = create_f32_array(2u64.pow(12) as usize, true);

    c.bench_function("sort nulls 2^12", |b| {
        b.iter(|| bench_sort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2u64.pow(12) as usize, false);
    let arr_b = create_bool_array(2u64.pow(12) as usize, false);
    c.bench_function("bool sort 2^12", |b| {
        b.iter(|| bench_sort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2u64.pow(12) as usize, true);
    let arr_b = create_bool_array(2u64.pow(12) as usize, true);
    c.bench_function("bool sort nulls 2^12", |b| {
        b.iter(|| bench_sort(&arr_a, &arr_b, None))
    });

    let dict_arr = Arc::new(create_string_dict_array::<Int32Type>(
        2u64.pow(12) as usize,
        0.0,
        1,
    )) as ArrayRef;
    c.bench_function("dict string 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&dict_arr, None))
    });

    // with limit
    {
        let arr_a = create_f32_array(2u64.pow(12) as usize, false);
        let arr_b = create_f32_array(2u64.pow(12) as usize, false);
        c.bench_function("sort 2^12 limit 10", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(10)))
        });

        let arr_a = create_f32_array(2u64.pow(12) as usize, false);
        let arr_b = create_f32_array(2u64.pow(12) as usize, false);
        c.bench_function("sort 2^12 limit 100", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(100)))
        });

        let arr_a = create_f32_array(2u64.pow(12) as usize, false);
        let arr_b = create_f32_array(2u64.pow(12) as usize, false);
        c.bench_function("sort 2^12 limit 1000", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(1000)))
        });

        let arr_a = create_f32_array(2u64.pow(12) as usize, false);
        let arr_b = create_f32_array(2u64.pow(12) as usize, false);
        c.bench_function("sort 2^12 limit 2^12", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(2u64.pow(12) as usize)))
        });

        let arr_a = create_f32_array(2u64.pow(12) as usize, true);
        let arr_b = create_f32_array(2u64.pow(12) as usize, true);

        c.bench_function("sort nulls 2^12 limit 10", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(10)))
        });
        c.bench_function("sort nulls 2^12 limit 100", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(100)))
        });
        c.bench_function("sort nulls 2^12 limit 1000", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(1000)))
        });
        c.bench_function("sort nulls 2^12 limit 2^12", |b| {
            b.iter(|| bench_sort(&arr_a, &arr_b, Some(2u64.pow(12) as usize)))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
