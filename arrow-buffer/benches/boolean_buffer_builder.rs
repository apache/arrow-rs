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

use arrow_buffer::BooleanBufferBuilder;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const LEN: usize = 64 * 1024;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let inputs = [
        ("random", (0..LEN).map(|_| rng.random_bool(0.5)).collect()),
        (
            "mostly_true",
            (0..LEN).map(|_| rng.random_bool(0.9)).collect(),
        ),
        ("all_true", vec![true; LEN]),
    ];

    let mut group = c.benchmark_group("BooleanBufferBuilder append_slice");
    group.throughput(Throughput::Elements(LEN as u64));

    for (name, values) in &inputs {
        // An offset of 3 bits makes the slice start in the middle of a byte
        for offset in [0, 3] {
            let id = BenchmarkId::new(format!("offset_{offset}"), name);
            group.bench_with_input(id, values, |b, values: &Vec<bool>| {
                b.iter(|| {
                    let mut builder = BooleanBufferBuilder::new(offset + LEN);
                    builder.append_n(offset, true);
                    builder.append_slice(hint::black_box(values));
                    hint::black_box(builder.finish())
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
