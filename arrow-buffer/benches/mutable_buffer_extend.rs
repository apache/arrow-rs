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

//! Benchmarks for appending iterators to a [`MutableBuffer`] with `extend`.
//!
//! Each case appends `LEN` items to a buffer that already has enough
//! capacity, with one `extend` call per chunk of items. The chunks have random
//! lengths that average `chunk` items.

use arrow_buffer::{ArrowNativeType, MutableBuffer};
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;
use std::ops::Range;

const LEN: usize = 1 << 15;

/// Each case cycles through this many inputs, one per iteration. An input is a
/// split of `0..LEN` into chunks, plus an array of items for cases that read
/// them.
const NUM_INPUTS: usize = 16;

/// `NUM_INPUTS` arrays of `LEN` items made by `item`
fn random_inputs<T>(mut item: impl FnMut() -> T) -> Vec<Vec<T>> {
    (0..NUM_INPUTS)
        .map(|_| (0..LEN).map(|_| item()).collect())
        .collect()
}

/// For each input, `0..LEN` split into chunks whose lengths are chosen at random
/// from 1 to `2 * chunk - 1` items
fn random_chunks(rng: &mut StdRng, chunk: usize) -> Vec<Vec<Range<usize>>> {
    (0..NUM_INPUTS)
        .map(|_| {
            let mut chunks = vec![];
            let mut start = 0;
            while start < LEN {
                let end = LEN.min(start + rng.random_range(1..2 * chunk));
                chunks.push(start..end);
                start = end;
            }
            chunks
        })
        .collect()
}

/// Benchmarks `extend` with the iterators that `make` returns. Each iteration
/// takes the next input, clears the buffer, and appends `make(input, range)`
/// for each `range` in `chunks[input]`.
fn bench_extend<I: Iterator<Item: ArrowNativeType>>(
    group: &mut BenchmarkGroup<'_, measurement::WallTime>,
    name: &str,
    chunk: usize,
    chunks: &[Vec<Range<usize>>],
    make: impl Fn(usize, Range<usize>) -> I,
) {
    let mut buffer = MutableBuffer::new(LEN * size_of::<I::Item>());
    let mut inputs = (0..NUM_INPUTS).cycle();
    group.bench_function(BenchmarkId::new(name, chunk), |b| {
        b.iter(|| {
            let input = inputs.next().unwrap();
            buffer.clear();
            for range in &chunks[input] {
                buffer.extend(make(input, range.clone()));
            }
            hint::black_box(buffer.as_slice());
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let bytes = random_inputs(|| rng.random_range(b'a'..=b'z'));
    let keys: Vec<Vec<i32>> = random_inputs(|| rng.random_range(0..1000));
    let wide: Vec<Vec<u128>> = random_inputs(|| rng.random_range(0..1 << 100));

    // Read at run time so the compiler can't specialize the maps for them.
    let (from, to) = hint::black_box((b'a', b'z'));
    let key_offset: i32 = hint::black_box(1000);
    let wide_offset: u128 = hint::black_box(1000);

    let mut group = c.benchmark_group("MutableBuffer extend");
    for chunk in [16, 1024] {
        let chunks = random_chunks(&mut rng, chunk);
        bench_extend(&mut group, "copied u8", chunk, &chunks, |input, r| {
            bytes[input][r].iter().copied()
        });
        // Like replacing one byte with another in a string.
        bench_extend(&mut group, "map u8", chunk, &chunks, |input, r| {
            bytes[input][r]
                .iter()
                .map(move |&b| if b == from { to } else { b })
        });
        bench_extend(&mut group, "map i32", chunk, &chunks, |input, r| {
            keys[input][r].iter().map(move |k| k + key_offset)
        });
        bench_extend(&mut group, "map u128", chunk, &chunks, |input, r| {
            wide[input][r].iter().map(move |v| v + wide_offset)
        });
        bench_extend(&mut group, "range i32", chunk, &chunks, |_, r| {
            r.start as i32..r.end as i32
        });
        bench_extend(&mut group, "repeat_n u32", chunk, &chunks, |_, r| {
            std::iter::repeat_n(r.start as u32, r.len())
        });
        bench_extend(&mut group, "chain i32", chunk, &chunks, |input, r| {
            let mid = r.len() / 2;
            let (lo, hi) = keys[input][r].split_at(mid);
            lo.iter().chain(hi).copied()
        });
        // The size hint is not exact. The predicate always passes, so the
        // branch is predictable.
        bench_extend(&mut group, "filter i32", chunk, &chunks, |input, r| {
            keys[input][r].iter().copied().filter(|&k| k >= 0)
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
