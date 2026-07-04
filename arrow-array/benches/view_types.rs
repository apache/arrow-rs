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

use arrow_array::StringViewArray;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn gen_view_array(size: usize) -> StringViewArray {
    StringViewArray::from_iter((0..size).map(|v| match v % 3 {
        0 => Some("small"),
        1 => Some("larger than 12 bytes array"),
        2 => None,
        _ => unreachable!("unreachable"),
    }))
}

fn gen_view_array_without_nulls(size: usize) -> StringViewArray {
    StringViewArray::from_iter((0..size).map(|v| {
        let s = match v % 3 {
            0 => "small".to_string(),                      // < 12 bytes
            1 => "larger than 12 bytes array".to_string(), // >12 bytes
            2 => "x".repeat(300),                          // 300 bytes (>256)
            _ => unreachable!(),
        };
        Some(s)
    }))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array = gen_view_array(100_000);

    c.bench_function("view types slice", |b| {
        b.iter(|| {
            black_box(array.slice(0, 100_000 / 2));
        });
    });

    c.bench_function("gc view types all[100000]", |b| {
        b.iter(|| {
            black_box(array.gc());
        });
    });

    let sliced = array.slice(0, 100_000 / 2);
    c.bench_function("gc view types slice half[100000]", |b| {
        b.iter(|| {
            black_box(sliced.gc());
        });
    });

    let array = gen_view_array_without_nulls(100_000);

    c.bench_function("gc view types all without nulls[100000]", |b| {
        b.iter(|| {
            black_box(array.gc());
        });
    });

    let sliced = array.slice(0, 100_000 / 2);
    c.bench_function("gc view types slice half without nulls[100000]", |b| {
        b.iter(|| {
            black_box(sliced.gc());
        });
    });

    let array = gen_view_array(8000);

    c.bench_function("gc view types all[8000]", |b| {
        b.iter(|| {
            black_box(array.gc());
        });
    });

    let sliced = array.slice(0, 8000 / 2);
    c.bench_function("gc view types slice half[8000]", |b| {
        b.iter(|| {
            black_box(sliced.gc());
        });
    });

    let array = gen_view_array_without_nulls(8000);

    c.bench_function("gc view types all without nulls[8000]", |b| {
        b.iter(|| {
            black_box(array.gc());
        });
    });

    let sliced = array.slice(0, 8000 / 2);
    c.bench_function("gc view types slice half without nulls[8000]", |b| {
        b.iter(|| {
            black_box(sliced.gc());
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
