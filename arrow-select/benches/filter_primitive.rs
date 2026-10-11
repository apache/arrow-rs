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
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, UInt8Array};
use arrow_select::filter::FilterBuilder;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

fn benchmark(c: &mut Criterion) {
    const ROWS: usize = 8192;
    const BATCHES: usize = 64;
    let arrays: [(&str, ArrayRef); 5] = [
        (
            "u8",
            Arc::new((0..ROWS).map(|i| (i % 128) as u8).collect::<UInt8Array>()),
        ),
        (
            "i32",
            Arc::new((0..ROWS).map(|i| i as i32).collect::<Int32Array>()),
        ),
        (
            "i64",
            Arc::new((0..ROWS).map(|i| i as i64).collect::<Int64Array>()),
        ),
        (
            "i128",
            Arc::new((0..ROWS).map(|i| i as i128).collect::<Decimal128Array>()),
        ),
        (
            "i64_nulls",
            Arc::new(
                (0..ROWS)
                    .map(|i| (i % 10 != 0).then_some(i as i64))
                    .collect::<Int64Array>(),
            ),
        ),
    ];
    let mut group = c.benchmark_group("filter_primitive");
    group.throughput(Throughput::Elements((ROWS * BATCHES) as u64));

    for (density_name, density) in [
        ("1_1024", 1.0 / 1024.0),
        ("1_16", 1.0 / 16.0),
        ("1_4", 0.25),
        ("1_2", 0.5),
        ("3_4", 0.75),
        ("9_10", 0.9),
    ] {
        for clustered in [false, true] {
            let mut rng = StdRng::seed_from_u64(42);
            let masks: Vec<BooleanArray> = (0..BATCHES)
                .map(|_| {
                    let mut keep = rng.random_bool(density);
                    (0..ROWS)
                        .map(|_| {
                            if clustered {
                                let bit = keep;
                                keep = if keep {
                                    !rng.random_bool(1.0 / 32.0)
                                } else {
                                    rng.random_bool(density / (32.0 * (1.0 - density)))
                                };
                                bit
                            } else {
                                rng.random_bool(density)
                            }
                        })
                        .collect()
                })
                .collect();
            let pattern = if clustered { "clustered" } else { "random" };
            for optimized in [false, true] {
                let predicates: Vec<_> = masks
                    .iter()
                    .map(|mask| {
                        let builder = FilterBuilder::new(mask);
                        if optimized {
                            builder.optimize()
                        } else {
                            builder
                        }
                        .build()
                    })
                    .collect();
                let strategy = if optimized { "optimized" } else { "lazy" };
                for (name, array) in &arrays {
                    group.bench_with_input(
                        BenchmarkId::new(format!("{name}/{pattern}/{strategy}"), density_name),
                        array,
                        |b, array| {
                            b.iter(|| {
                                for predicate in &predicates {
                                    black_box(predicate.filter(black_box(array.as_ref())).unwrap());
                                }
                            })
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
