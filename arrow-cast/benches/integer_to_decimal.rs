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

use std::hint::black_box;

use arrow_array::{Int64Array, UInt64Array};
use arrow_cast::cast;
use arrow_schema::DataType;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const ARRAY_LEN: usize = 8_192;

fn integer_to_decimal(c: &mut Criterion) {
    let int64_small = Int64Array::from_iter_values((0..ARRAY_LEN).map(|i| i as i64));
    let int64_scaled = Int64Array::from_value(5_000_000_000, ARRAY_LEN);
    let uint64_small = UInt64Array::from_iter_values((0..ARRAY_LEN).map(|i| i as u64));

    let mut group = c.benchmark_group("integer_to_decimal");
    group.throughput(Throughput::Elements(ARRAY_LEN as u64));

    group.bench_function("int64_to_decimal32_scale_0", |b| {
        b.iter(|| black_box(cast(&int64_small, &DataType::Decimal32(9, 0)).unwrap()))
    });
    group.bench_function("int64_to_decimal32_scale_negative", |b| {
        b.iter(|| black_box(cast(&int64_scaled, &DataType::Decimal32(9, -1)).unwrap()))
    });
    group.bench_function("int64_to_decimal64_scale_0", |b| {
        b.iter(|| black_box(cast(&int64_small, &DataType::Decimal64(18, 0)).unwrap()))
    });
    group.bench_function("uint64_to_decimal64_scale_0", |b| {
        b.iter(|| black_box(cast(&uint64_small, &DataType::Decimal64(18, 0)).unwrap()))
    });

    group.finish();
}

criterion_group!(benches, integer_to_decimal);
criterion_main!(benches);
