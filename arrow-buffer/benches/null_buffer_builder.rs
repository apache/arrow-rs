// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_buffer::NullBufferBuilder;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const WORDS: usize = 512;
const LEN: usize = WORDS * 64;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let very_sparse_nulls = (0..WORDS)
        .map(|i| if i % 32 == 0 { u64::MAX - 1 } else { u64::MAX })
        .collect();
    let sparse_nulls = (0..WORDS)
        .map(|i| if i % 8 == 0 { u64::MAX - 1 } else { u64::MAX })
        .collect();
    let inputs = [
        ("all_valid", vec![u64::MAX; WORDS]),
        ("very_sparse_nulls", very_sparse_nulls),
        ("sparse_nulls", sparse_nulls),
        ("dense_nulls", vec![0x0000_0000_0000_FFFF; WORDS]),
        ("alternating", vec![0xAAAA_AAAA_AAAA_AAAA; WORDS]),
        ("all_null", vec![0; WORDS]),
        ("random", (0..WORDS).map(|_| rng.random()).collect()),
    ];

    let mut group = c.benchmark_group("NullBufferBuilder append");
    group.throughput(Throughput::Elements(LEN as u64));

    for (name, words) in &inputs {
        group.bench_with_input(BenchmarkId::new("word", name), words, |b, words| {
            b.iter(|| {
                let mut builder = NullBufferBuilder::new(LEN);
                for &word in words {
                    builder.append_word(hint::black_box(word), 64);
                }
                hint::black_box(builder.finish())
            })
        });

        group.bench_with_input(BenchmarkId::new("bool", name), words, |b, words| {
            b.iter(|| {
                let mut builder = NullBufferBuilder::new(LEN);
                for &word in hint::black_box(words) {
                    for i in 0..64 {
                        builder.append(word & (1 << i) != 0);
                    }
                }
                hint::black_box(builder.finish())
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
