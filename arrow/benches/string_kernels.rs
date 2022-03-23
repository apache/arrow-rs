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

extern crate arrow;

use arrow::array::*;
use arrow::compute::kernels::substring::substring;
use arrow::util::bench_util::*;

fn bench_substring(arr: &StringArray, start: i64, length: usize) {
    substring(criterion::black_box(arr), start, &Some(length as u64)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let str_len = 1000;

    let arr_string = create_string_array_with_len::<i32>(size, 0.0, str_len);
    let start = 0;

    c.bench_function("substring", |b| {
        b.iter(|| bench_substring(&arr_string, start, str_len))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
