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

use arrow_buffer::NullBufferBuilder;
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const SIZE: usize = 8192;

/// A builder with `SIZE` bits of capacity whose bitmap is already materialized,
/// so that the benchmarks below measure appending rather than the one-off
/// `#[cold]` materialization.
fn materialized() -> NullBufferBuilder {
    let mut builder = NullBufferBuilder::new(SIZE);
    builder.append_null();
    builder
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    // 10% nulls, which is enough to materialize on the first few values
    let validity: Vec<bool> = hint::black_box((0..SIZE).map(|_| rng.random_ratio(9, 10)).collect());

    c.bench_function("NullBufferBuilder::append_null", |b| {
        b.iter_batched_ref(
            materialized,
            |builder| {
                for _ in 1..SIZE {
                    builder.append_null();
                }
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("NullBufferBuilder::append_n_nulls", |b| {
        b.iter_batched_ref(
            materialized,
            |builder| {
                for _ in 1..SIZE / 8 {
                    builder.append_n_nulls(8);
                }
            },
            BatchSize::SmallInput,
        );
    });

    // The all-valid case never materializes a bitmap: a control for the two above
    c.bench_function("NullBufferBuilder::append_non_null", |b| {
        b.iter_batched_ref(
            || NullBufferBuilder::new(SIZE),
            |builder| {
                for _ in 0..SIZE {
                    builder.append_non_null();
                }
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("NullBufferBuilder::append", |b| {
        b.iter_batched_ref(
            || NullBufferBuilder::new(SIZE),
            |builder| {
                for v in &validity {
                    builder.append(*v);
                }
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("NullBufferBuilder::append_slice", |b| {
        b.iter_batched_ref(
            || NullBufferBuilder::new(SIZE),
            |builder| {
                for chunk in validity.chunks(64) {
                    builder.append_slice(chunk);
                }
            },
            BatchSize::SmallInput,
        );
    });

    // Materializing is `#[cold]`, but it happens once per builder that sees a null
    c.bench_function("NullBufferBuilder::materialize", |b| {
        b.iter_batched_ref(
            || {
                let mut builder = NullBufferBuilder::new(SIZE);
                builder.append_n_non_nulls(SIZE);
                builder
            },
            |builder| builder.append_null(),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
