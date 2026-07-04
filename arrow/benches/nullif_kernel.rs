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

use arrow::util::bench_util::{create_boolean_array, create_primitive_array};

use arrow::array::*;
use arrow_array::types::Int64Type;
use arrow_select::nullif::nullif;
use std::hint;

fn bench_nullif(left: &dyn Array, right: &BooleanArray) {
    hint::black_box(nullif(left, right).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let size = 8192usize;

    // create input before benchmark to ensure allocations are consistent
    let int64_no_nulls = create_primitive_array::<Int64Type>(size, 0.0);
    let int64_nulls = create_primitive_array::<Int64Type>(size, 0.1);

    let mask_10 = create_boolean_array(size, 0.0, 0.1);
    let mask_10_sliced = create_boolean_array(size + 7, 0.0, 0.1).slice(7, size);
    let mask_1 = create_boolean_array(size, 0.0, 0.01);

    c.bench_function("nullif no-nulls mask(10%)", |b| {
        b.iter(|| bench_nullif(&int64_no_nulls, &mask_10))
    });
    c.bench_function("nullif no-nulls mask(10%, sliced)", |b| {
        b.iter(|| bench_nullif(&int64_no_nulls, &mask_10_sliced))
    });
    c.bench_function("nullif no-nulls mask(1%)", |b| {
        b.iter(|| bench_nullif(&int64_no_nulls, &mask_1))
    });

    c.bench_function("nullif nulls mask(10%)", |b| {
        b.iter(|| bench_nullif(&int64_nulls, &mask_10))
    });
    c.bench_function("nullif nulls mask(10%, sliced)", |b| {
        b.iter(|| bench_nullif(&int64_nulls, &mask_10_sliced))
    });
    c.bench_function("nullif nulls mask(1%)", |b| {
        b.iter(|| bench_nullif(&int64_nulls, &mask_1))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
