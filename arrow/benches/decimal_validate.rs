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

use arrow::array::{Array, Decimal128Array, Decimal256Array, Decimal256Builder};
use criterion::Criterion;

extern crate arrow;

use arrow::util::decimal::Decimal256;

fn validate_decimal128_array(array: Decimal128Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal256_array(array: Decimal256Array) {
    array.with_precision_and_scale(35, 0).unwrap();
}

fn validate_decimal128_benchmark(c: &mut Criterion) {
    let decimal_array = Decimal128Array::from_iter_values(vec![12324; 20000]);
    let data = decimal_array.into_data();
    c.bench_function("validate_decimal128_array 20000", |b| {
        b.iter(|| {
            let array = Decimal128Array::from(data.clone());
            validate_decimal128_array(array);
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
    validate_decimal256_benchmark,
);
criterion_main!(benches);
