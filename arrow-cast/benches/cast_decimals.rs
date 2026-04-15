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
use std::sync::Arc;

fn cast_string_from_or_to_decimals(c: &mut Criterion) {
    let str_array = StringArray::from(vec![
        Some("123.45"),
        Some("1.2345"),
        Some("0.12345"),
        Some("0.1267"),
        Some("1.263"),
        Some("12345.0"),
        Some("12345"),
        Some("000.123"),
        Some("12.234000"),
        None,
        Some(""),
        Some(" "),
        None,
        Some("-1.23499999"),
        Some("-1.23599999"),
        Some("-0.00001"),
        Some("-123"),
        Some("-123.234000"),
        Some("-000.123"),
        Some("+1.23499999"),
        Some("+1.23599999"),
        Some("+0.00001"),
        Some("+123"),
        Some("+123.234000"),
        Some("+000.123"),
        Some("1.-23499999"),
        Some("-1.-23499999"),
        Some("--1.23499999"),
        Some("0"),
        Some("000.000"),
        Some("0000000000000000012345.000"),
    ]);
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
    let bench_suite_float32 = [
        ("float32_to_decimal32(9, 2)", Decimal32(9, 2)),
        ("float32_to_decimal64(18, 2)", Decimal64(18, 2)),
        ("float32_to_decimal128(38, 3)", Decimal128(38, 3)),
        ("float32_to_decimal256(76, 4)", Decimal256(76, 4)),
    ];
    let float32_array = Float32Array::from(vec![-123.45f32, f32::MIN, 0f32, 123.45f32, f32::MAX]);
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
    let float64_array = Float64Array::from(vec![-123.45f64, f64::MIN, 0f64, 123.45f64, f64::MAX]);
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
    let decimal32_array = Decimal32Array::from(vec![Some(12345), Some(23400), None, Some(-12342)])
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

    let decimal64_array = Decimal64Array::from(vec![
        Some(123451),
        Some(234000),
        None,
        Some(1234100),
        Some(-12342),
    ])
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

    let decimal128_array = Decimal128Array::from(vec![
        Some(123451),
        Some(234000),
        None,
        Some(1234100),
        Some(-12342),
    ])
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

    let decimal256_array = Decimal256Array::from(vec![
        Some(i256::from_i128(2000)),
        Some(i256::from_i128(234000)),
        None,
        Some(i256::from_i128(1234100)),
        Some(i256::from_i128(-12342)),
    ])
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
    let decimal32_array = Decimal32Array::from(vec![Some(12345), Some(23400), None, Some(-12342)])
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

    let decimal64_array = Decimal64Array::from(vec![Some(12345), Some(23400), None, Some(-12342)])
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

    let decimal128_array =
        Decimal128Array::from(vec![Some(12345), Some(23400), None, Some(-12342)])
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
    let decimal256_array = Decimal256Array::from(vec![
        Some(i256::from(12345)),
        Some(i256::from(23400)),
        None,
        Some(i256::from(-12342)),
    ])
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
    cast_string_from_or_to_decimals,
    cast_float_to_decimals,
    cast_decimal_to_float,
    cast_decimal_to_integer
);
criterion_main!(benches);
