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
use arrow_buffer::i256;
use rand::Rng;
use std::{convert::TryFrom, sync::Arc};

fn array_from_vec(n: usize) {
    let v: Vec<i32> = (0..n as i32).collect();
    criterion::black_box(Int32Array::from(v));
}

fn array_string_from_vec(n: usize) {
    let mut v: Vec<Option<&str>> = Vec::with_capacity(n);
    for i in 0..n {
        if i % 2 == 0 {
            v.push(Some("hello world"));
        } else {
            v.push(None);
        }
    }
    criterion::black_box(StringArray::from(v));
}

fn struct_array_values(
    n: usize,
) -> (
    &'static str,
    Vec<Option<&'static str>>,
    &'static str,
    Vec<Option<i32>>,
) {
    let mut strings: Vec<Option<&str>> = Vec::with_capacity(n);
    let mut ints: Vec<Option<i32>> = Vec::with_capacity(n);
    for _ in 0..n / 4 {
        strings.extend_from_slice(&[Some("joe"), None, None, Some("mark")]);
        ints.extend_from_slice(&[Some(1), Some(2), None, Some(4)]);
    }
    ("f1", strings, "f2", ints)
}

fn struct_array_from_vec(
    field1: &str,
    strings: &[Option<&str>],
    field2: &str,
    ints: &[Option<i32>],
) {
    let strings: ArrayRef = Arc::new(StringArray::from(strings.to_owned()));
    let ints: ArrayRef = Arc::new(Int32Array::from(ints.to_owned()));

    criterion::black_box(
        StructArray::try_from(vec![(field1, strings), (field2, ints)]).unwrap(),
    );
}

fn decimal128_array_from_vec(array: &[Option<i128>]) {
    criterion::black_box(
        array
            .iter()
            .copied()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(34, 2)
            .unwrap(),
    );
}

fn decimal256_array_from_vec(array: &[Option<i256>]) {
    criterion::black_box(
        array
            .iter()
            .copied()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(70, 2)
            .unwrap(),
    );
}

fn decimal_benchmark(c: &mut Criterion) {
    // bench decimal128 array
    // create option<i128> array
    let size: usize = 1 << 15;
    let mut rng = rand::thread_rng();
    let mut array = vec![];
    for _ in 0..size {
        array.push(Some(rng.gen_range::<i128, _>(0..9999999999)));
    }
    c.bench_function("decimal128_array_from_vec 32768", |b| {
        b.iter(|| decimal128_array_from_vec(array.as_slice()))
    });

    // bench decimal256array
    // create option<into<decimal256>> array
    let size = 1 << 10;
    let mut array = vec![];
    let mut rng = rand::thread_rng();
    for _ in 0..size {
        let decimal = i256::from_i128(rng.gen_range::<i128, _>(0..9999999999999));
        array.push(Some(decimal));
    }

    // bench decimal256 array
    c.bench_function("decimal256_array_from_vec 32768", |b| {
        b.iter(|| decimal256_array_from_vec(array.as_slice()))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("array_from_vec 128", |b| b.iter(|| array_from_vec(128)));
    c.bench_function("array_from_vec 256", |b| b.iter(|| array_from_vec(256)));
    c.bench_function("array_from_vec 512", |b| b.iter(|| array_from_vec(512)));

    c.bench_function("array_string_from_vec 128", |b| {
        b.iter(|| array_string_from_vec(128))
    });
    c.bench_function("array_string_from_vec 256", |b| {
        b.iter(|| array_string_from_vec(256))
    });
    c.bench_function("array_string_from_vec 512", |b| {
        b.iter(|| array_string_from_vec(512))
    });

    let (field1, strings, field2, ints) = struct_array_values(128);
    c.bench_function("struct_array_from_vec 128", |b| {
        b.iter(|| struct_array_from_vec(field1, &strings, field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(256);
    c.bench_function("struct_array_from_vec 256", |b| {
        b.iter(|| struct_array_from_vec(field1, &strings, field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(512);
    c.bench_function("struct_array_from_vec 512", |b| {
        b.iter(|| struct_array_from_vec(field1, &strings, field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(1024);
    c.bench_function("struct_array_from_vec 1024", |b| {
        b.iter(|| struct_array_from_vec(field1, &strings, field2, &ints))
    });
}

criterion_group!(benches, criterion_benchmark, decimal_benchmark);
criterion_main!(benches);
