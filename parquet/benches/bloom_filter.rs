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

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::bloom_filter::Sbbf;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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
        group.bench_with_input(BenchmarkId::new("ndv", num_values), &filter, |b, filter| {
            b.iter_batched(
                || filter.clone(),
                |mut f| {
                    f.fold_to_target_fpp(fpp);
                    f
                },
                BatchSize::SmallInput,
            );
        });
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

fn bench_insert_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_only");

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
                    sbbf
                });
            },
        );
    }
    group.finish();
}

/// Benchmark `Sbbf::insert` across the same three cache regimes as
/// `bench_check`. The filter is allocated once per regime and reused
/// across iterations — bloom inserts are idempotent on identical
/// input, so this measures the pure insert kernel cost (hash + mask +
/// load + OR + store) without per-iteration allocation noise.
fn bench_insert(c: &mut Criterion) {
    let regimes: [(&str, usize); 3] = [
        ("s_128KiB", 128 * 1024),
        ("m_2MiB", 2 * 1024 * 1024),
        ("l_32MiB", 32 * 1024 * 1024),
    ];
    const NUM_INSERTS: usize = 50_000;

    let mut group = c.benchmark_group("insert");

    for (label, num_bytes) in regimes {
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let keys: Vec<[u8; 16]> = (0..NUM_INSERTS)
            .map(|_| {
                let mut k = [0u8; 16];
                rng.fill(&mut k);
                k
            })
            .collect();

        let mut filter = Sbbf::new_with_num_of_bytes(num_bytes);

        group.throughput(Throughput::Elements(NUM_INSERTS as u64));
        group.bench_with_input(BenchmarkId::new("ins", label), &keys, |b, keys| {
            b.iter(|| {
                for k in keys {
                    filter.insert(hint::black_box(k.as_slice()));
                }
            });
        });
    }
    group.finish();
}

/// Benchmark `Sbbf::check` across three cache regimes and both miss-heavy
/// (the common case for Parquet row-group skipping) and hit-heavy workloads.
///
/// The three filter sizes span the cache hierarchy on a typical server:
/// 128 KB fits in L2, 2 MB lives in L3, 32 MB spills out of L3.
fn bench_check(c: &mut Criterion) {
    let regimes: [(&str, usize, usize); 3] = [
        ("s_128KiB", 128 * 1024, 10_000),
        ("m_2MiB", 2 * 1024 * 1024, 200_000),
        ("l_32MiB", 32 * 1024 * 1024, 3_000_000),
    ];
    const NUM_QUERIES: usize = 50_000;

    let mut group = c.benchmark_group("check");

    for (label, num_bytes, ndv) in regimes {
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);

        // Clear the high bit of byte 0 on inserted keys so the miss set below
        // — which forces the same bit to 1 — is genuinely disjoint by
        // construction (not just disjoint at birthday-paradox probability).
        let mut filter = Sbbf::new_with_num_of_bytes(num_bytes);
        let mut inserted: Vec<[u8; 16]> = Vec::with_capacity(ndv);
        for _ in 0..ndv {
            let mut k = [0u8; 16];
            rng.fill(&mut k);
            k[0] &= 0x7f;
            filter.insert(k.as_slice());
            inserted.push(k);
        }

        // Disjoint miss set: high bit of byte 0 is 1 here and 0 in `inserted`,
        // so no miss key can equal an inserted key.
        let miss_keys: Vec<[u8; 16]> = (0..NUM_QUERIES)
            .map(|_| {
                let mut k = [0u8; 16];
                rng.fill(&mut k);
                k[0] |= 0x80;
                k
            })
            .collect();
        let hit_keys: Vec<[u8; 16]> = (0..NUM_QUERIES)
            .map(|_| inserted[rng.random_range(0..inserted.len())])
            .collect();

        group.throughput(Throughput::Elements(NUM_QUERIES as u64));
        group.bench_with_input(BenchmarkId::new("miss", label), &miss_keys, |b, keys| {
            b.iter(|| {
                let mut found = 0u64;
                for k in keys {
                    if hint::black_box(filter.check(k.as_slice())) {
                        found += 1;
                    }
                }
                hint::black_box(found)
            });
        });
        group.bench_with_input(BenchmarkId::new("hit", label), &hit_keys, |b, keys| {
            b.iter(|| {
                let mut found = 0u64;
                for k in keys {
                    if hint::black_box(filter.check(k.as_slice())) {
                        found += 1;
                    }
                }
                hint::black_box(found)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_fold_to_target_fpp,
    bench_insert_and_fold,
    bench_insert_only,
    bench_insert,
    bench_check,
);
criterion_main!(benches);
