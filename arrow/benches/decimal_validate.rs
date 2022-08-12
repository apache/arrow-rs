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
    Array, Decimal128Array, Decimal128Builder, Decimal256Array, Decimal256Builder,
};
use arrow::datatypes::{
    validate_decimal_precision, validate_decimal_precision_with_bytes,
};
use criterion::Criterion;
use rand::Rng;

extern crate arrow;

use arrow::util::decimal::{Decimal128, Decimal256};

// fn validate_decimal128_array(array: Decimal128Array) {
//     array.with_precision_and_scale(35, 0).unwrap();
// }

fn validate_decimal128_array_fast(array: &Decimal128Array) {
    array.validate_decimal_with_bytes(35).unwrap();
}

fn validate_decimal128_array_slow(array: &Decimal128Array) {
    array.validate_decimal_precision(35).unwrap();
}

fn validate_decimal256_array(array: Decimal256Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

// remove from the commit
// https://github.com/apache/arrow-rs/pull/2360/commits/364929e918bdb0e96bc931424de615cbc18af8cb
fn validate_decimal128_using_bytes(array: &[[u8; 16]], precision: usize) {
    for value in array {
        validate_decimal_precision_with_bytes(value, precision).unwrap();
    }
}

fn validate_decimal128_using_i128(array: &[[u8; 16]], precision: usize) {
    // convert the the element to decimal128
    for v in array {
        let decimal = Decimal128::new(precision, 0, v);
        validate_decimal_precision(decimal.as_i128(), precision).unwrap();
    }
}

fn validate_decimal128_cmp_i128_with_bytes_benchmark(c: &mut Criterion) {
    // create the [u8;16] array
    let mut array: Vec<[u8; 16]> = vec![];
    let mut rng = rand::thread_rng();
    for i in 0..200000 {
        // array.push((i as i128).to_le_bytes());
        array.push((rng.gen_range::<i128, _>(0..999999999999)).to_le_bytes());
    }

    c.bench_function("validate_decimal128_bytes 20000", |b| {
        b.iter(|| validate_decimal128_using_bytes(&array, 35))
    });

    c.bench_function("validate_decimal128_i128 20000", |b| {
        b.iter(|| validate_decimal128_using_i128(&array, 35))
    });
}

fn validate_decimal128_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let size: i128 = 200000;
    let mut decimal_builder = Decimal128Builder::new(size as usize, 38, 0);
    for i in 0..size {
        decimal_builder
            .append_value(rng.gen_range::<i128, _>(0..999999999999))
            .unwrap();
    }
    let decimal_array = decimal_builder.finish();
    // let decimal_array = Decimal128Array::from_iter_values(vec![12324; size as usize]);
    c.bench_function("validate_decimal128_array_fast 20000", |b| {
        b.iter(|| {
            // let array = Decimal128Array::from(data.clone());
            validate_decimal128_array_fast(&decimal_array);
        })
    });

    c.bench_function("validate_decimal256_array_slow 20000", |b| {
        b.iter(|| {
            // let array = Decimal128Array::from(data.clone());
            validate_decimal128_array_slow(&decimal_array);
        })
    });
}

fn validate_decimal256_benchmark(c: &mut Criterion) {
    let mut decimal_builder = Decimal256Builder::new(20000, 76, 0);
    let mut bytes = vec![0; 32];
    bytes[0..16].clone_from_slice(&12324_i128.to_le_bytes());
    for _ in 0..20000 {
        decimal_builder
            .append_value(&Decimal256::new(76, 0, &bytes))
            .unwrap();
    }
    let decimal_array256_data = decimal_builder.finish();
    let data = decimal_array256_data.into_data();
    c.bench_function("validate_decimal256_array 20000", |b| {
        b.iter(|| {
            let array = Decimal256Array::from(data.clone());
            validate_decimal256_array(array);
        })
    });
}

criterion_group!(
    benches,
    validate_decimal128_benchmark,
    // validate_decimal256_benchmark,
    validate_decimal128_cmp_i128_with_bytes_benchmark,
);
criterion_main!(benches);
