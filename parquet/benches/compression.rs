// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::*;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::compression::create_codec;
use rand::distr::Alphanumeric;
use rand::prelude::*;
use std::hint;

fn do_bench(c: &mut Criterion, name: &str, uncompressed: &[u8]) {
    let codecs = [
        Compression::BROTLI(BrotliLevel::default()),
        Compression::GZIP(GzipLevel::default()),
        Compression::LZ4,
        Compression::LZ4_RAW,
        Compression::SNAPPY,
        Compression::GZIP(GzipLevel::default()),
        Compression::ZSTD(ZstdLevel::default()),
    ];

    for compression in codecs {
        let mut codec = create_codec(compression, &Default::default())
            .unwrap()
            .unwrap();

        c.bench_function(&format!("compress {compression} - {name}"), |b| {
            b.iter(|| {
                let mut out = Vec::new();
                codec.compress(uncompressed, &mut out).unwrap();
                out
            });
        });

        let mut compressed = Vec::new();
        codec.compress(uncompressed, &mut compressed).unwrap();
        println!(
            "{compression} compressed {} bytes of {name} to {} bytes",
            uncompressed.len(),
            compressed.len()
        );

        c.bench_function(&format!("decompress {compression} - {name}"), |b| {
            b.iter(|| {
                let mut out = Vec::new();
                codec
                    .decompress(
                        hint::black_box(&compressed),
                        &mut out,
                        Some(uncompressed.len()),
                    )
                    .unwrap();
                out
            });
        });
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let rng = &mut rng;
    const DATA_SIZE: usize = 1024 * 1024;

    let uncompressed: Vec<_> = rng.sample_iter(&Alphanumeric).take(DATA_SIZE).collect();
    do_bench(c, "alphanumeric", &uncompressed);

    // Create a collection of 64 words
    let words: Vec<Vec<_>> = (0..64)
        .map(|_| {
            let len = rng.random_range(1..12);
            rng.sample_iter(&Alphanumeric).take(len).collect()
        })
        .collect();

    // Build data by concatenating these words randomly together
    let mut uncompressed = Vec::with_capacity(DATA_SIZE);
    while uncompressed.len() < DATA_SIZE {
        let word = &words[rng.random_range(0..words.len())];
        uncompressed.extend_from_slice(&word[..word.len().min(DATA_SIZE - uncompressed.len())])
    }
    assert_eq!(uncompressed.len(), DATA_SIZE);

    do_bench(c, "words", &uncompressed);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
