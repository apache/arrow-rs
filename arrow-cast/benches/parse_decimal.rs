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

use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_cast::parse::parse_decimal;
use criterion::*;
use std::hint;

fn bench_parse<T: DecimalType>(
    c: &mut Criterion,
    name: &str,
    decimal: &str,
    precision: u8,
    scale: i8,
) {
    let d = hint::black_box(decimal);
    c.bench_function(name, |b| {
        b.iter(|| parse_decimal::<T>(d, precision, scale).unwrap());
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let decimals = [
        "123.123",
        "123.1234",
        "123.1",
        "123",
        "-123.123",
        "-123.1234",
        "-123.1",
        "-123",
        "0.0000123",
        "12.",
        "-12.",
        "00.1",
        "-00.1",
        "12345678912345678.1234",
        "-12345678912345678.1234",
        "99999999999999999.999",
        "-99999999999999999.999",
        ".123",
        "-.123",
        "123.",
        "-123.",
    ];

    for decimal in decimals {
        bench_parse::<Decimal256Type>(c, decimal, decimal, 20, 3);
    }

    let decimal128 = [
        ("string decimal128 integer", "12345678912345678", 3),
        ("string decimal128 exact scale", "12345678912345.123", 3),
        ("string decimal128 padded scale", "12345678912345.1", 6),
        ("string decimal128 rounded scale", "12345678912345.1235", 3),
        ("string decimal128 signed", "-12345678912345.1235", 3),
        (
            "string decimal128 38 digits",
            "99999999999999999999999999999999999999",
            0,
        ),
        ("string decimal128 exponent", "1.2345678912345e13", 3),
        (
            "string decimal128 negative exponent",
            "12345678912345678e-3",
            3,
        ),
        ("string decimal128 negative scale", "12345678912345678", -3),
        (
            "string decimal128 long fraction",
            "1.2345678912345678912345678912345678912345",
            3,
        ),
    ];
    for (name, decimal, scale) in decimal128 {
        bench_parse::<Decimal128Type>(c, name, decimal, 38, scale);
    }

    let decimal256 = [
        (
            "string decimal256 76 digits",
            "9999999999999999999999999999999999999999999999999999999999999999999999999999",
            0,
        ),
        (
            "string decimal256 rounded scale",
            "999999999999999999999999999999999999999999999999999999999999999999999.9995",
            3,
        ),
    ];
    for (name, decimal, scale) in decimal256 {
        bench_parse::<Decimal256Type>(c, name, decimal, 76, scale);
    }

    let decimal32 = [
        ("string decimal32 short", "1234.56", 2),
        ("string decimal32 9 digits", "9999999.99", 2),
    ];
    for (name, decimal, scale) in decimal32 {
        bench_parse::<Decimal32Type>(c, name, decimal, 9, scale);
    }

    let decimal64 = [
        ("string decimal64 short", "1234.56", 2),
        ("string decimal64 18 digits", "9999999999999999.99", 2),
    ];
    for (name, decimal, scale) in decimal64 {
        bench_parse::<Decimal64Type>(c, name, decimal, 18, scale);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
