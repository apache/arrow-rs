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
use criterion::{BenchmarkId, Criterion, Throughput};

use arrow::array::*;
use arrow::compute::kernels::regexp::*;
use arrow::util::bench_util::*;
use std::hint;

fn bench_regexp(arr: &dyn Array, regex_array: &dyn Datum) {
    hint::black_box(
        regexp_match(hint::black_box(arr), hint::black_box(regex_array), None).unwrap(),
    );
}

fn bench_scalar_captures(c: &mut Criterion) {
    let size = 8192;
    let matches = StringArray::from_iter_values(
        (0..size).map(|i| format!("customer-{i:08}-region-{}-purchase", i % 8)),
    );
    let no_matches = StringArray::from_iter_values(
        (0..size).map(|i| format!("visitor-{i:08}-region-{}-purchase", i % 8)),
    );
    let mixed = StringArray::from_iter((0..size).map(|i| match i % 4 {
        0 => None,
        2 => Some(no_matches.value(i)),
        _ => Some(matches.value(i)),
    }));
    let pattern = r"customer-([0-9]+)-region-([0-7])";
    let utf8_pattern = Scalar::new(StringArray::from(vec![pattern]));
    let view_pattern = Scalar::new(StringViewArray::from(vec![pattern]));

    let mut group = c.benchmark_group("regexp_scalar_captures");
    group.throughput(Throughput::Elements(size as u64));
    for (name, array) in [
        ("matches", matches),
        ("mixed", mixed),
        ("no_matches", no_matches),
    ] {
        let view = StringViewArray::from(&array);
        group.bench_function(BenchmarkId::new("utf8", name), |b| {
            b.iter(|| bench_regexp(&array, &utf8_pattern))
        });
        group.bench_function(BenchmarkId::new("utf8view", name), |b| {
            b.iter(|| bench_regexp(&view, &view_pattern))
        });
    }
    group.finish();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let val_len = 1000;

    let arr_string = create_string_array_with_len::<i32>(size, 0.0, val_len);
    let pattern_values = vec![r".*-(\d*)-.*"; size];
    let pattern = GenericStringArray::<i32>::from(pattern_values);

    c.bench_function("regexp", |b| b.iter(|| bench_regexp(&arr_string, &pattern)));

    let pattern_values = vec![r".*-(\d*)-.*"];
    let pattern = Scalar::new(GenericStringArray::<i32>::from(pattern_values));

    c.bench_function("regexp scalar", |b| {
        b.iter(|| bench_regexp(&arr_string, &pattern))
    });
}

criterion_group!(benches, add_benchmark, bench_scalar_captures);
criterion_main!(benches);
