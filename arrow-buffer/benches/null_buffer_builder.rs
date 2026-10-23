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
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint;

const WORDS: usize = 512;
const LEN: usize = WORDS * 64;

fn criterion_benchmark(c: &mut Criterion) {
    let very_sparse_nulls = (0..WORDS)
        .map(|i| if i % 64 == 0 { u64::MAX - 1 } else { u64::MAX })
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
    ];

    let mut group = c.benchmark_group("NullBufferBuilder append");
    group.throughput(Throughput::Elements(LEN as u64));

    for (name, words) in inputs {
        group.bench_with_input(BenchmarkId::new("word", name), &words, |b, words| {
            b.iter(|| {
                let mut builder = NullBufferBuilder::new(LEN);
                for &word in words {
                    builder.append_word(hint::black_box(word), 64);
                }
                hint::black_box(builder.finish())
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
