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

use criterion::*;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::compression::create_codec;
use rand::distributions::Alphanumeric;
use rand::prelude::*;

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
                    .decompress(black_box(&compressed), &mut out, Some(uncompressed.len()))
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
            let len = rng.gen_range(1..12);
            rng.sample_iter(&Alphanumeric).take(len).collect()
        })
        .collect();

    // Build data by concatenating these words randomly together
    let mut uncompressed = Vec::with_capacity(DATA_SIZE);
    while uncompressed.len() < DATA_SIZE {
        let word = &words[rng.gen_range(0..words.len())];
        uncompressed.extend_from_slice(&word[..word.len().min(DATA_SIZE - uncompressed.len())])
    }
    assert_eq!(uncompressed.len(), DATA_SIZE);

    do_bench(c, "words", &uncompressed);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
