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

    // Many appends of 1 to 7 values each, starting in the middle of a byte.
    // Lengths cycle through 1 to 7, which adds up to `CALLS * 4` values.
    const CALLS: usize = 7 * 1024;
    let short_inputs = [
        (
            "random",
            (0..CALLS * 4).map(|_| rng.random_bool(0.5)).collect(),
        ),
        ("all_false", vec![false; CALLS * 4]),
    ];

    let mut group = c.benchmark_group("BooleanBufferBuilder append_slice short");
    group.throughput(Throughput::Elements(CALLS as u64));

    for (name, values) in &short_inputs {
        group.bench_with_input(*name, values, |b, values: &Vec<bool>| {
            b.iter(|| {
                let mut builder = BooleanBufferBuilder::new(3 + values.len());
                builder.append_n(3, true);
                let mut start = 0;
                for i in 0..CALLS {
                    let end = start + i % 7 + 1;
                    builder.append_slice(hint::black_box(&values[start..end]));
                    start = end;
                }
                hint::black_box(builder.finish())
            })
        });
    }

    group.finish();

    // Many appends of 8 values each. At offset 3 every value goes to the head
    // or the remainder, and none is packed.
    let eight_inputs = [
        (
            "random",
            (0..CALLS * 8).map(|_| rng.random_bool(0.5)).collect(),
        ),
        ("all_false", vec![false; CALLS * 8]),
    ];

    let mut group = c.benchmark_group("BooleanBufferBuilder append_slice eight");
    group.throughput(Throughput::Elements(CALLS as u64));

    for (name, values) in &eight_inputs {
        for offset in [0, 3] {
            let id = BenchmarkId::new(format!("offset_{offset}"), name);
            group.bench_with_input(id, values, |b, values: &Vec<bool>| {
                b.iter(|| {
                    let mut builder = BooleanBufferBuilder::new(offset + values.len());
                    builder.append_n(offset, true);
                    for chunk in values.as_chunks::<8>().0 {
                        builder.append_slice(hint::black_box(chunk));
                    }
                    hint::black_box(builder.finish())
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
