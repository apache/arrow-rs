// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Counts the set bits in each row of a list's element bitmap, as a kernel
//! does to count the valid elements of each row.

use arrow_buffer::{BooleanBuffer, OffsetBuffer};
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

/// DataFusion's default batch size
const ROWS: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bit_counter");
    for width in [4, 32, 256] {
        // Constant row lengths, and random lengths with the same mean.
        for (lengths_name, random_lengths) in [("constant", false), ("random", true)] {
            // A byte-aligned bitmap, and one sliced to an unaligned offset.
            for offset in [0, 3] {
                let mut rng = StdRng::seed_from_u64(42);
                let offsets = OffsetBuffer::<i32>::from_lengths((0..ROWS).map(|_| {
                    if random_lengths {
                        rng.random_range(0..=2 * width)
                    } else {
                        width
                    }
                }));
                let len = offsets.last() as usize;
                let bits = (0..offset + len)
                    .map(|_| rng.random_bool(0.5))
                    .collect::<BooleanBuffer>()
                    .slice(offset, len);
                let name = format!("width_{width}/{lengths_name}/offset_{offset}");

                group.bench_function(format!("count_set_bits_offset/{name}"), |b| {
                    b.iter(|| {
                        let buffer = hint::black_box(&bits);
                        offsets
                            .windows(2)
                            .map(|w| {
                                let (start, end) = (w[0] as usize, w[1] as usize);
                                buffer
                                    .inner()
                                    .count_set_bits_offset(buffer.offset() + start, end - start)
                            })
                            .sum::<usize>()
                    })
                });

                group.bench_function(format!("set_bit_counter/{name}"), |b| {
                    b.iter(|| {
                        let mut counter = hint::black_box(&bits).set_bit_counter();
                        offsets
                            .windows(2)
                            .map(|w| counter.count_range(w[0] as usize..w[1] as usize))
                            .sum::<usize>()
                    })
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
