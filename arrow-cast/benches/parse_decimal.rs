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

use arrow_array::types::{Decimal128Type, Decimal256Type};
use arrow_cast::cast::parse_string_to_decimal_native;
use arrow_cast::parse::parse_decimal;
use criterion::*;
use std::hint;

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
        let d = hint::black_box(decimal);
        c.bench_function(d, |b| {
            b.iter(|| parse_decimal::<Decimal256Type>(d, 20, 3).unwrap());
        });
    }

    let string_decimals = [
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

    for (name, decimal, scale) in string_decimals {
        let d = hint::black_box(decimal);
        let scale = hint::black_box(scale);
        if name.contains("decimal256") {
            c.bench_function(name, |b| {
                b.iter(|| parse_string_to_decimal_native::<Decimal256Type>(d, scale).unwrap());
            });
        } else {
            c.bench_function(name, |b| {
                b.iter(|| parse_string_to_decimal_native::<Decimal128Type>(d, scale).unwrap());
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
