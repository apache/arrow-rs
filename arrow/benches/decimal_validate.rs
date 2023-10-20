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

use arrow::array::{Array, Decimal128Array, Decimal128Builder, Decimal256Array, Decimal256Builder};
use criterion::Criterion;
use rand::Rng;

extern crate arrow;

use arrow_buffer::i256;

fn validate_decimal128_array(array: Decimal128Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal256_array(array: Decimal256Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal128_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let size: i128 = 20000;
    let mut decimal_builder = Decimal128Builder::with_capacity(size as usize);
    for _ in 0..size {
        decimal_builder.append_value(rng.gen_range::<i128, _>(0..999999999999));
    }
    let decimal_array = decimal_builder
        .finish()
        .with_precision_and_scale(38, 0)
        .unwrap();
    let data = decimal_array.into_data();
    c.bench_function("validate_decimal128_array 20000", |b| {
        b.iter(|| {
            let array = Decimal128Array::from(data.clone());
            validate_decimal128_array(array);
        })
    });
}

fn validate_decimal256_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let size: i128 = 20000;
    let mut decimal_builder = Decimal256Builder::with_capacity(size as usize);
    for _ in 0..size {
        let v = rng.gen_range::<i128, _>(0..999999999999999);
        let decimal = i256::from_i128(v);
        decimal_builder.append_value(decimal);
    }
    let decimal_array256_data = decimal_builder
        .finish()
        .with_precision_and_scale(76, 0)
        .unwrap();
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
    validate_decimal256_benchmark,
);
criterion_main!(benches);
