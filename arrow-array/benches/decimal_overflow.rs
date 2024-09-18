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

use arrow_array::builder::Decimal128Builder;
use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let len = 8192;
    let mut b = Decimal128Builder::with_capacity(len);
    for i in 0..len {
        if i % 10 == 0 {
            b.append_value(i128::max_value());
        } else {
            b.append_value(i as i128);
        }
    }
    let array = b.finish();

    c.bench_function("validate_decimal_precision", |b| {
        b.iter(|| black_box(array.validate_decimal_precision(8).unwrap_or(())));
    });
    c.bench_function("null_if_overflow_precision", |b| {
        b.iter(|| black_box(array.null_if_overflow_precision(8)));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
