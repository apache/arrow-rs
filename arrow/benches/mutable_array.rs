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
use criterion::Criterion;

use rand::Rng;

extern crate arrow;

use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::create_string_array};

fn create_slices(size: usize) -> Vec<(usize, usize)> {
    let rng = &mut seedable_rng();

    (0..size)
        .map(|_| {
            let start = rng.gen_range(0..size / 2);
            let end = rng.gen_range(start + 1..size);
            (start, end)
        })
        .collect()
}

fn bench<T: Array>(v1: &T, slices: &[(usize, usize)]) {
    let mut mutable = MutableArrayData::new(vec![v1.data_ref()], false, 5);
    for (start, end) in slices {
        mutable.extend(0, *start, *end)
    }
    mutable.freeze();
}

fn add_benchmark(c: &mut Criterion) {
    let v1 = create_string_array::<i32>(1024, 0.0);
    let v2 = create_slices(1024);
    c.bench_function("mutable str 1024", |b| b.iter(|| bench(&v1, &v2)));

    let v1 = create_string_array::<i32>(1024, 0.5);
    let v2 = create_slices(1024);
    c.bench_function("mutable str nulls 1024", |b| b.iter(|| bench(&v1, &v2)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
