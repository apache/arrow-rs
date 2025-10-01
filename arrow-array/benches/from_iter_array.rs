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

use arrow_array::{BooleanArray, Int64Array};
use criterion::*;
use std::hint;
use std::iter::repeat_n;

fn gen_vector<TItem: Copy>(item: TItem, len: usize) -> Vec<Option<TItem>> {
    repeat_n(item, len)
        .enumerate()
        .map(|(idx, item)| if idx % 3 == 0 { None } else { Some(item) })
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    // All ArrowPrimitiveType use the same implementation
    c.bench_function("Int64Array::from_iter", |b| {
        let values = gen_vector(1, 81920);
        b.iter(|| hint::black_box(Int64Array::from_iter(values.iter())));
    });
    c.bench_function("Int64Array::from_trusted_len_iter", |b| unsafe {
        let values = gen_vector(1, 81920);
        b.iter(|| hint::black_box(Int64Array::from_trusted_len_iter(values.iter())));
    });

    c.bench_function("BooleanArray::from_iter", |b| {
        let values = gen_vector(true, 81920);
        b.iter(|| hint::black_box(BooleanArray::from_iter(values.iter())));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
