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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use parquet::bloom_filter::Sbbf;

/// Build a bloom filter sized for `initial_ndv` at `fpp`, insert `num_values` distinct values,
/// and return it ready for folding.
fn build_filter(initial_ndv: u64, fpp: f64, num_values: u64) -> Sbbf {
    let mut sbbf = Sbbf::new_with_ndv_fpp(initial_ndv, fpp).unwrap();
    for i in 0..num_values {
        sbbf.insert(&i);
    }
    sbbf
}

fn bench_fold_to_target_fpp(c: &mut Criterion) {
    let mut group = c.benchmark_group("fold_to_target_fpp");

    // Realistic scenario: filter sized for 1M NDV, varying actual distinct values
    let initial_ndv = 1_000_000u64;
    let fpp = 0.05;

    for num_values in [1_000u64, 10_000, 100_000] {
        let filter = build_filter(initial_ndv, fpp, num_values);
        let num_blocks = filter.num_blocks();
        group.throughput(Throughput::Elements(num_blocks as u64));
        group.bench_with_input(
            BenchmarkId::new("ndv", num_values),
            &filter,
            |b, filter| {
                b.iter_batched(
                    || filter.clone(),
                    |mut f| {
                        f.fold_to_target_fpp(fpp);
                        f
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_insert_and_fold(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_and_fold");

    let initial_ndv = 1_000_000u64;
    let fpp = 0.05;

    for num_values in [1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_values));
        group.bench_with_input(
            BenchmarkId::new("values", num_values),
            &num_values,
            |b, &num_values| {
                b.iter(|| {
                    let mut sbbf = Sbbf::new_with_ndv_fpp(initial_ndv, fpp).unwrap();
                    for i in 0..num_values {
                        sbbf.insert(&i);
                    }
                    sbbf.fold_to_target_fpp(fpp);
                    sbbf
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_fold_to_target_fpp, bench_insert_and_fold);
criterion_main!(benches);
