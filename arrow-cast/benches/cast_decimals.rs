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

use arrow_array::{
    ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float32Array,
    Float64Array, StringArray,
};
use arrow_buffer::i256;
use arrow_schema::DataType::{
    Decimal32, Decimal64, Decimal128, Decimal256, Float32, Float64, Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
};
use criterion::*;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

fn cast_string_from_decimals(c: &mut Criterion) {
    let total_records = 10_000;
    let mut rng = StdRng::seed_from_u64(42);
    let str_array = StringArray::from_iter((0..total_records).map(|x| match x % 20 {
        0 => None,
        1 => Some("".to_string()),
        2 => Some(" ".to_string()),
        3 => Some("-1.-23499999".to_string()),
        4 => Some("--1.23456789".to_string()),
        5 => Some("1.-23456789".to_string()),
        6 => Some("000.123".to_string()),
        7 => Some("+123".to_string()),
        8 => Some("+123.12345000".to_string()),
        9 => Some("0".to_string()),
        10 => Some("000.000".to_string()),
        11 => Some("0000000000000000012345.000".to_string()),
        _ => Some(format!("{:.6}", f64::from(x) * rng.random::<f64>())),
    }));
    let array = Arc::new(str_array) as ArrayRef;

    let bench_suite = [
        ("string2decimal32(9, 2)", Decimal32(9, 2)),
        ("string2decimal64(18, 2)", Decimal64(18, 2)),
        ("string2decimal128(38, 3)", Decimal128(38, 3)),
        ("string2decimal256(76, 4)", Decimal256(76, 4)),
    ];
    for bench in bench_suite {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }
}

fn cast_float_to_decimals(c: &mut Criterion) {
    let total_records = 50_000;
    let mut rng = StdRng::seed_from_u64(42);
    let bench_suite_float32 = [
        ("float32_to_decimal32(9, 2)", Decimal32(9, 2)),
        ("float32_to_decimal64(18, 2)", Decimal64(18, 2)),
        ("float32_to_decimal128(38, 3)", Decimal128(38, 3)),
        ("float32_to_decimal256(76, 4)", Decimal256(76, 4)),
    ];

    let float32_array = Float32Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(f32::MIN),
        2 => Some(f32::MAX),
        _ => match x % 2 {
            0 => Some((x as f32) * rng.random::<f32>()),
            _ => Some(-(x as f32) * rng.random::<f32>()),
        },
    }));
    for bench in &bench_suite_float32 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&float32_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    let bench_suite_float64 = [
        ("float64_to_decimal32(9, 2)", Decimal32(9, 2)),
        ("float64_to_decimal64(18, 4)", Decimal64(18, 2)),
        ("float64_to_decimal128(38, 3)", Decimal128(38, 3)),
        ("float64_to_decimal256(76, 4)", Decimal256(76, 4)),
    ];

    rng = StdRng::seed_from_u64(42);
    let float64_array = Float64Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(f64::MIN),
        2 => Some(f64::MAX),
        _ => match x % 2 {
            0 => Some(f64::from(x) * rng.random::<f64>()),
            _ => Some(-f64::from(x) * rng.random::<f64>()),
        },
    }));
    for bench in &bench_suite_float64 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&float64_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }
}

fn cast_decimal_to_float(c: &mut Criterion) {
    let total_records = 100_000;
    let mut rng = StdRng::seed_from_u64(42);
    let decimal32_array = Decimal32Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i32::MIN),
        2 => Some(i32::MAX),
        _ => Some(rng.random::<i32>()),
    }))
    .with_precision_and_scale(9, 2)
    .unwrap();

    let bench_suite_decimal32 = [
        ("decimal32(9, 2)_to_float32", Float32),
        ("decimal32(9, 2)_to_float64", Float64),
    ];

    for bench in &bench_suite_decimal32 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal32_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal64_array = Decimal64Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i64::MIN),
        2 => Some(i64::MAX),
        _ => Some(rng.random::<i64>()),
    }))
    .with_precision_and_scale(18, 2)
    .unwrap();

    let bench_suite_decimal64 = [
        ("decimal64(18, 2)_to_float32", Float32),
        ("decimal64(18, 2)_to_float64", Float64),
    ];
    for bench in &bench_suite_decimal64 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal64_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal128_array = Decimal128Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i128::MIN),
        2 => Some(i128::MAX),
        _ => Some(rng.random::<i128>()),
    }))
    .with_precision_and_scale(38, 3)
    .unwrap();
    let bench_suite_decimal128 = [
        ("decimal128(38, 3)_to_float32", Float32),
        ("decimal128(38, 3)_to_float64", Float64),
    ];
    for bench in &bench_suite_decimal128 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal128_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal256_array = Decimal256Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i256::MIN),
        2 => Some(i256::MAX),
        _ => Some(i256::from(rng.random::<i64>())),
    }))
    .with_precision_and_scale(76, 4)
    .unwrap();
    let bench_suite_decimal256 = [
        ("decimal256(76, 4)_to_float32", Float32),
        ("decimal256(76, 4)_to_float64", Float64),
    ];
    for bench in &bench_suite_decimal256 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal256_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }
}

fn cast_decimal_to_integer(c: &mut Criterion) {
    let total_records = 30_000;
    let mut rng = StdRng::seed_from_u64(42);
    let decimal32_array = Decimal32Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i32::MIN),
        2 => Some(i32::MAX),
        _ => Some(rng.random::<i32>()),
    }))
    .with_precision_and_scale(9, 2)
    .unwrap();

    let bench_suite_decimal32 = [
        ("decimal32(9, 2)_to_int8", Int8),
        ("decimal32(9, 2)_to_int16", Int16),
        ("decimal32(9, 2)_to_int32", Int32),
        ("decimal32(9, 2)_to_int64", Int64),
        ("decimal32(9, 2)_to_uint8", UInt8),
        ("decimal32(9, 2)_to_uint16", UInt16),
        ("decimal32(9, 2)_to_uint32", UInt32),
        ("decimal32(9, 2)_to_uint64", UInt64),
    ];

    for bench in &bench_suite_decimal32 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal32_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal64_array = Decimal64Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i64::MIN),
        2 => Some(i64::MAX),
        _ => Some(rng.random::<i64>()),
    }))
    .with_precision_and_scale(18, 2)
    .unwrap();

    let bench_suite_decimal64 = [
        ("decimal64(18, 2)_to_int8", Int8),
        ("decimal64(18, 2)_to_int16", Int16),
        ("decimal64(18, 2)_to_int32", Int32),
        ("decimal64(18, 2)_to_int64", Int64),
        ("decimal64(18, 2)_to_uint8", UInt8),
        ("decimal64(18, 2)_to_uint16", UInt16),
        ("decimal64(18, 2)_to_uint32", UInt32),
        ("decimal64(18, 2)_to_uint64", UInt64),
    ];

    for bench in &bench_suite_decimal64 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal64_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal128_array = Decimal128Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i128::MIN),
        2 => Some(i128::MAX),
        _ => Some(rng.random::<i128>()),
    }))
    .with_precision_and_scale(38, 3)
    .unwrap();

    let bench_suite_decimal128 = [
        ("decimal128(38, 3)_to_int8", Int8),
        ("decimal128(38, 3)_to_int16", Int16),
        ("decimal128(38, 3)_to_int32", Int32),
        ("decimal128(38, 3)_to_int64", Int64),
        ("decimal128(38, 3)_to_uint8", UInt8),
        ("decimal128(38, 3)_to_uint16", UInt16),
        ("decimal128(38, 3)_to_uint32", UInt32),
        ("decimal128(38, 3)_to_uint64", UInt64),
    ];

    for bench in &bench_suite_decimal128 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal128_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }

    rng = StdRng::seed_from_u64(42);
    let decimal256_array = Decimal256Array::from_iter((0..total_records).map(|x| match x % 10 {
        0 => None,
        1 => Some(i256::MIN),
        2 => Some(i256::MAX),
        _ => Some(i256::from(rng.random::<i64>())),
    }))
    .with_precision_and_scale(76, 4)
    .unwrap();

    let bench_suite_decimal256 = [
        ("decimal256(76, 4)_to_int8", Int8),
        ("decimal256(76, 4)_to_int16", Int16),
        ("decimal256(76, 4)_to_int32", Int32),
        ("decimal256(76, 4)_to_int64", Int64),
        ("decimal256(76, 4)_to_uint8", UInt8),
        ("decimal256(76, 4)_to_uint16", UInt16),
        ("decimal256(76, 4)_to_uint32", UInt32),
        ("decimal256(76, 4)_to_uint64", UInt64),
    ];

    for bench in &bench_suite_decimal256 {
        c.bench_function(bench.0, |b| {
            b.iter(|| {
                let r = arrow_cast::cast(&decimal256_array, &bench.1);
                std::hint::black_box(r)
            })
        });
    }
}
criterion_group!(
    benches,
    cast_string_from_decimals,
    cast_float_to_decimals,
    cast_decimal_to_float,
    cast_decimal_to_integer
);
criterion_main!(benches);
