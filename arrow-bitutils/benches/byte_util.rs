// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_bitutils::byte_util::{cmp_bytes, common_prefix_len};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::cmp::Ordering;
use std::hint::black_box;

const VALUES: usize = 8 * 1024;

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.random()).collect()
}

/// Inputs for `cmp_bytes`, each a column of values
fn cmp_inputs(rng: &mut StdRng) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let prefixed = |rng: &mut StdRng, prefix: usize| -> Vec<Vec<u8>> {
        let shared = random_bytes(rng, prefix);
        (0..VALUES)
            .map(|_| {
                let suffix_len = rng.random_range(4..12);
                [shared.clone(), random_bytes(rng, suffix_len)].concat()
            })
            .collect()
    };
    vec![
        // Poorly predicted: random lengths, most pairs differ in the first byte
        (
            "random_0_to_32",
            (0..VALUES)
                .map(|_| {
                    let len = rng.random_range(0..32);
                    random_bytes(rng, len)
                })
                .collect(),
        ),
        (
            "random_1_to_7",
            (0..VALUES)
                .map(|_| {
                    let len = rng.random_range(1..8);
                    random_bytes(rng, len)
                })
                .collect(),
        ),
        // Well predicted: constant length, sequential, differ only after the first 8 bytes
        (
            "sequential_14",
            (0..VALUES)
                .map(|i| format!("value_{i:08}").into_bytes())
                .collect(),
        ),
        ("prefix_8_random_suffix", prefixed(rng, 8)),
        ("prefix_32_random_suffix", prefixed(rng, 32)),
        ("prefix_100_random_suffix", prefixed(rng, 100)),
        ("prefix_1000_random_suffix", prefixed(rng, 1000)),
    ]
}

/// The per-row statistics loop of the parquet writers (`compute_min_max`)
fn min_max(values: &[Vec<u8>], cmp: impl Fn(&[u8], &[u8]) -> Ordering) -> (&[u8], &[u8]) {
    let mut min = values[0].as_slice();
    let mut max = min;
    for v in &values[1..] {
        if cmp(min, v).is_gt() {
            min = v;
        } else if cmp(v, max).is_gt() {
            max = v;
        }
    }
    (min, max)
}

/// The scan parquet used for `DELTA_BYTE_ARRAY` before `common_prefix_len`, kept to compare
/// against in one run
#[inline]
fn block32_bytewise_tail(a: &[u8], b: &[u8]) -> usize {
    const BLOCK: usize = 32;
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);
    let mut matched = 0;
    for (x, y) in a
        .as_chunks::<BLOCK>()
        .0
        .iter()
        .zip(b.as_chunks::<BLOCK>().0)
    {
        if x != y {
            break;
        }
        matched += BLOCK;
    }
    matched
        + a[matched..]
            .iter()
            .zip(&b[matched..])
            .take_while(|(x, y)| x == y)
            .count()
}

/// Inputs for `common_prefix_len`, as consecutive values of a `DELTA_BYTE_ARRAY` column
fn prefix_inputs(rng: &mut StdRng) -> Vec<(String, Vec<Vec<u8>>)> {
    let mut inputs = vec![];
    for shared in [0, 4, 14, 31, 100, 1000] {
        let prefix = random_bytes(rng, shared);
        let values = (0..VALUES)
            .map(|_| [prefix.clone(), random_bytes(rng, 8)].concat())
            .collect();
        inputs.push((format!("shared_{shared}"), values));
    }
    // Poorly predicted: the shared length varies from 0 to 40 bytes
    let base = random_bytes(rng, 48);
    let values = (0..VALUES)
        .map(|_| {
            let shared = rng.random_range(0..40);
            [base[..shared].to_vec(), random_bytes(rng, 8)].concat()
        })
        .collect();
    inputs.push(("shared_0_to_40".to_string(), values));
    inputs
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut group = c.benchmark_group("min_max");
    group.throughput(Throughput::Elements(VALUES as u64));
    for (name, values) in cmp_inputs(&mut rng) {
        group.bench_function(format!("slice_cmp/{name}"), |b| {
            b.iter(|| black_box(min_max(black_box(&values), |a, b| a.cmp(b))))
        });
        group.bench_function(format!("cmp_bytes/{name}"), |b| {
            b.iter(|| black_box(min_max(black_box(&values), cmp_bytes)))
        });
    }
    group.finish();

    // One long shared prefix, the case `memcmp` is tuned for
    let mut group = c.benchmark_group("cmp_long");
    for len in [64, 1024, 256 * 1024] {
        let a = random_bytes(&mut rng, len);
        let mut b = a.clone();
        b[len - 1] ^= 1;
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_function(format!("slice_cmp/{len}"), |bench| {
            bench.iter(|| black_box(black_box(&a[..]).cmp(black_box(&b[..]))))
        });
        group.bench_function(format!("cmp_bytes/{len}"), |bench| {
            bench.iter(|| black_box(cmp_bytes(black_box(&a), black_box(&b))))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("common_prefix_len");
    group.throughput(Throughput::Elements(VALUES as u64));
    for (name, values) in prefix_inputs(&mut rng) {
        group.bench_function(format!("block32_bytewise_tail/{name}"), |b| {
            b.iter(|| {
                let values = black_box(&values);
                values
                    .windows(2)
                    .map(|w| block32_bytewise_tail(&w[0], &w[1]))
                    .sum::<usize>()
            })
        });
        group.bench_function(format!("common_prefix_len/{name}"), |b| {
            b.iter(|| {
                let values = black_box(&values);
                values
                    .windows(2)
                    .map(|w| common_prefix_len(&w[0], &w[1]))
                    .sum::<usize>()
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
