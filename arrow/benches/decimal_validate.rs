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

use arrow::array::{
    BasicDecimalArray, Decimal128Array, Decimal256Array, Decimal256Builder,
};
use criterion::Criterion;
use arrow::datatypes::{DataType, validate_decimal_precision, validate_decimal_precision_with_bytes};

extern crate arrow;

use arrow::util::decimal::{BasicDecimal, Decimal128, Decimal256};

fn validate_decimal128_array_slow(array: &Decimal128Array) {
    array.validate_decimal_precision(35).unwrap();
}

// fn validate_decimal128_array_fast(array: &Decimal128Array) {
//     array.validate_decimal_with_bytes(35).unwrap();
// }

fn validate_decimal256_array_slow(array: &Decimal256Array) {
    array.validate_decimal_precision(35).unwrap();
}

// fn validate_decimal256_array_fast(array: &Decimal256Array) {
//     array.validate_decimal_with_bytes(35).unwrap();
// }

fn validate_decimal128_bytes(array: &[[u8; 16]], precision: usize) {
    for value in array {
        validate_decimal_precision_with_bytes(value, precision).unwrap();
    }
}

fn validate_decimal128_i128(array: &[[u8; 16]], precision: usize) {
    // convert the the element to decimal128
    for v in array {
        let decimal = Decimal128::new(precision, 0, v);
        validate_decimal_precision(decimal.as_i128(), precision).unwrap();
    }
}

fn validate_decimal128_or_bytes_benchmark(c: &mut Criterion) {
    // create the [u8;16] array
    let mut array: Vec<[u8; 16]> = vec![];
    for i in 1..20000 {
        array.push((i as i128).to_le_bytes());
    }
    c.bench_function("validate_decimal128_bytes 2000", |b| {
        b.iter(|| validate_decimal128_bytes(&array, 35))
    });

    c.bench_function("validate_decimal128_i128 2000", |b| {
        b.iter(|| validate_decimal128_i128(&array, 35))
    });
}

fn validate_decimal128_benchmark(c: &mut Criterion) {
    // decimal array slow
    let decimal_array = Decimal128Array::from_iter_values(vec![12324; 20000]);
    c.bench_function("validate_decimal128_array_slow 2000", |b| {
        b.iter(|| validate_decimal128_array_slow(&decimal_array))
    });

    // decimal array fast
    // c.bench_function("validate_decimal128_array_fast 2000", |b| {
    //     b.iter(|| validate_decimal128_array_fast(&decimal_array))
    // });
}

fn validate_decimal256_benchmark(c: &mut Criterion) {
    // decimal array slow
    let mut decimal_builder = Decimal256Builder::new(2000, 76, 0);
    let mut bytes = vec![0; 32];
    bytes[0..16].clone_from_slice(&12324_i128.to_le_bytes());
    for _ in 0..200000 {
        decimal_builder
            .append_value(&Decimal256::new(76, 0, &bytes))
            .unwrap();
    }
    let decimal_array256 = decimal_builder.finish();
    c.bench_function("validate_decimal256_array_slow 2000", |b| {
        b.iter(|| validate_decimal256_array_slow(&decimal_array256))
    });

    // decimal array fast
    // c.bench_function("validate_decimal256_array_fast 2000", |b| {
    //     b.iter(|| validate_decimal256_array_fast(&decimal_array256))
    // });
}

criterion_group!(
    benches,
    validate_decimal128_or_bytes_benchmark,
    // validate_decimal128_benchmark,
    // validate_decimal256_benchmark
);
// criterion_group!(benches, validate_decimal256_benchmark);
criterion_main!(benches);
