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

use std::hint;

use arrow_arith::numeric::{add, sub};
use arrow_array::PrimitiveArray;
use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_buffer::ArrowNativeType;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const SIZE: usize = 1024;

fn decimal<T: DecimalType>(
    values: impl Iterator<Item = T::Native>,
    scale: i8,
) -> PrimitiveArray<T> {
    PrimitiveArray::<T>::new(values.collect::<Vec<_>>().into(), None)
        .with_precision_and_scale(T::MAX_PRECISION, scale)
        .unwrap()
}

fn benchmark<T: DecimalType>(c: &mut Criterion, name: &str) {
    for (scale, right_scale) in [("equal", 0), ("different", 1)] {
        let left = decimal::<T>((0..SIZE).map(T::Native::usize_as), 0);
        let right = decimal::<T>(
            (0..SIZE).map(|i| T::Native::usize_as(SIZE - i)),
            right_scale,
        );
        let mut group = c.benchmark_group(format!("{name}_{scale}_scale"));
        group.throughput(Throughput::Elements(SIZE as u64));
        group.bench_function("add", |b| {
            b.iter(|| hint::black_box(add(&left, &right).unwrap()))
        });
        group.bench_function("sub", |b| {
            b.iter(|| hint::black_box(sub(&left, &right).unwrap()))
        });
        group.finish();
    }
}

fn decimal_arithmetic(c: &mut Criterion) {
    benchmark::<Decimal32Type>(c, "decimal32");
    benchmark::<Decimal64Type>(c, "decimal64");
    benchmark::<Decimal128Type>(c, "decimal128");
    benchmark::<Decimal256Type>(c, "decimal256");
}

criterion_group!(benches, decimal_arithmetic);
criterion_main!(benches);
