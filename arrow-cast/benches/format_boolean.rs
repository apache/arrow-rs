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

use arrow_array::BooleanArray;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const ARRAY_LEN: usize = 8192;

fn format_array(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let array = BooleanArray::from((0..ARRAY_LEN).map(|_| rng.random()).collect::<Vec<bool>>());
    let formatter = ArrayFormatter::try_new(&array, &FormatOptions::new()).unwrap();
    let mut output = String::with_capacity(8);

    let mut group = c.benchmark_group("format_boolean");
    group.throughput(Throughput::Elements(ARRAY_LEN as u64));
    group.bench_function("random", |b| {
        b.iter(|| {
            for idx in 0..array.len() {
                output.clear();
                formatter.value(idx).write(&mut output).unwrap();
            }
            black_box(&output);
        })
    });
    group.finish();
}

criterion_group!(benches, format_array);
criterion_main!(benches);
