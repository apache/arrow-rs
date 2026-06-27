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
#[macro_use]
extern crate criterion;

use criterion::Criterion;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::util::bench_util::*;
use arrow_string::concat_elements::concat_elements_dyn;
use std::hint;

fn bench_concat(v1: &dyn Array, v2: &dyn Array) {
    hint::black_box(concat_elements_dyn(v1, v2).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let v1 = create_string_array::<i32>(1024, 0.0);
    let v2 = create_string_array::<i32>(1024, 0.0);
    c.bench_function("concat str 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_string_array::<i32>(1024, 0.5);
    let v2 = create_string_array::<i32>(1024, 0.5);
    c.bench_function("concat str nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    {
        let input1 = create_string_array::<i32>(8192, 0.0);
        let input2 = create_string_array::<i32>(8192, 0.0);
        c.bench_function("concat str 8192", |b| {
            b.iter(|| bench_concat(&input1, &input2))
        });
    }

    {
        let input1 = create_string_array::<i32>(8192, 0.5);
        let input2 = create_string_array::<i32>(8192, 0.5);
        c.bench_function("concat str nulls 8192 over 100 arrays", |b| {
            b.iter(|| bench_concat(&input1, &input2))
        });
    }

    // String view arrays
    for null_density in [0.0, 0.2] {
        // Any strings less than 12 characters are stored as prefix only, so specially
        // benchmark cases that have different mixes of lengths.
        for (name, str_len) in [("all_inline", 12), ("", 20), ("", 128)] {
            let array = create_string_view_array_with_len(8192, null_density, str_len, false);
            let id = format!(
                "concat utf8_view {name} max_str_len={str_len} null_density={null_density}"
            );
            c.bench_function(&id, |b| b.iter(|| bench_concat(&array, &array)));
        }
    }

    let v1 = create_string_array_with_len::<i32>(10, 0.0, 20);
    let v1 = create_dict_from_values::<Int32Type>(1024, 0.0, &v1);
    let v2 = create_string_array_with_len::<i32>(10, 0.0, 20);
    let v2 = create_dict_from_values::<Int32Type>(1024, 0.0, &v2);
    c.bench_function("concat str_dict 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let v1 = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let v1 = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &v1, 10..20);
    let v2 = create_string_array_with_len::<i32>(1024, 0.0, 20);
    let v2 = create_sparse_dict_from_values::<Int32Type>(1024, 0.0, &v2, 30..40);
    c.bench_function("concat str_dict_sparse 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
