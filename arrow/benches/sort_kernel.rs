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

use std::sync::Arc;

extern crate arrow;

use arrow::compute::{lexsort, sort, sort_to_indices, SortColumn};
use arrow::datatypes::{Int16Type, Int32Type};
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use arrow_ord::rank::rank;

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

fn bench_sort(array: &dyn Array) {
    black_box(sort(array, None).unwrap());
}

fn bench_lexsort(array_a: &ArrayRef, array_b: &ArrayRef, limit: Option<usize>) {
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

    black_box(lexsort(&columns, limit).unwrap());
}

fn bench_sort_to_indices(array: &dyn Array, limit: Option<usize>) {
    black_box(sort_to_indices(array, None, limit).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr = create_primitive_array::<Int32Type>(2usize.pow(10), 0.0);
    c.bench_function("sort i32 2^10", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 to indices 2^10", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(12), 0.0);
    c.bench_function("sort i32 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(10), 0.5);
    c.bench_function("sort i32 nulls 2^10", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 nulls to indices 2^10", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(12), 0.5);
    c.bench_function("sort i32 nulls 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_f32_array(2_usize.pow(12), false);
    c.bench_function("sort f32 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort f32 to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_f32_array(2usize.pow(12), true);
    c.bench_function("sort f32 nulls 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort f32 nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string[10] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string[10] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_dict_array::<Int32Type>(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string[10] dict to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_dict_array::<Int32Type>(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string[10] dict nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let run_encoded_array =
        create_primitive_run_array::<Int16Type, Int32Type>(2usize.pow(12), 2usize.pow(10));

    c.bench_function("sort primitive run 2^12", |b| {
        b.iter(|| bench_sort(&run_encoded_array))
    });

    c.bench_function("sort primitive run to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&run_encoded_array, None))
    });

    let arr_a = create_f32_array(2usize.pow(10), false);
    let arr_b = create_f32_array(2usize.pow(10), false);

    c.bench_function("lexsort (f32, f32) 2^10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);

    c.bench_function("lexsort (f32, f32) 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(10), true);
    let arr_b = create_f32_array(2usize.pow(10), true);

    c.bench_function("lexsort (f32, f32) nulls 2^10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), true);
    let arr_b = create_f32_array(2usize.pow(12), true);

    c.bench_function("lexsort (f32, f32) nulls 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2usize.pow(12), false);
    let arr_b = create_bool_array(2usize.pow(12), false);
    c.bench_function("lexsort (bool, bool) 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2usize.pow(12), true);
    let arr_b = create_bool_array(2usize.pow(12), true);
    c.bench_function("lexsort (bool, bool) nulls 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(10)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 100", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(100)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 1000", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(1000)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(2usize.pow(12))))
    });

    let arr_a = create_f32_array(2usize.pow(12), true);
    let arr_b = create_f32_array(2usize.pow(12), true);

    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(10)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 100", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(100)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 1000", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(1000)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(2usize.pow(12))))
    });

    let arr = create_f32_array(2usize.pow(12), false);
    c.bench_function("rank f32 2^12", |b| {
        b.iter(|| black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_f32_array(2usize.pow(12), true);
    c.bench_function("rank f32 nulls 2^12", |b| {
        b.iter(|| black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 10);
    c.bench_function("rank string[10] 2^12", |b| {
        b.iter(|| black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 10);
    c.bench_function("rank string[10] nulls 2^12", |b| {
        b.iter(|| black_box(rank(&arr, None).unwrap()))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
