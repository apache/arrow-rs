// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Definition / repetition level encoding in isolation, fed the way the column
//! writer feeds it: one `put_with_observer` call per mini-batch, with an
//! observer that counts values and updates a level histogram.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use parquet::encodings::levels::LevelEncoder;
use rand::prelude::*;

const NUM_LEVELS: usize = 64 * 1024;
const MINI_BATCH: usize = 1024;

/// Levels for `List<T>` with nullable list and element: lists of `0..=max_len`
/// elements, `null_pct` percent null lists and null elements.
/// Returns `(def, rep)` with max def 3 and max rep 1.
fn list_levels(rng: &mut StdRng, max_len: usize, null_pct: u32) -> (Vec<i16>, Vec<i16>) {
    let mut def = Vec::with_capacity(NUM_LEVELS);
    let mut rep = Vec::with_capacity(NUM_LEVELS);
    while def.len() < NUM_LEVELS {
        if rng.random_range(0..100) < null_pct {
            def.push(0);
            rep.push(0);
            continue;
        }
        let len = rng.random_range(0..=max_len);
        if len == 0 {
            def.push(1);
            rep.push(0);
            continue;
        }
        for i in 0..len {
            def.push(if rng.random_range(0..100) < null_pct {
                2
            } else {
                3
            });
            rep.push((i != 0) as i16);
        }
    }
    def.truncate(NUM_LEVELS);
    rep.truncate(NUM_LEVELS);
    (def, rep)
}

/// Levels for `List<List<T>>`, all nullable: max def 5, max rep 2.
fn nested_list_levels(rng: &mut StdRng) -> (Vec<i16>, Vec<i16>) {
    let mut def = Vec::with_capacity(NUM_LEVELS);
    let mut rep = Vec::with_capacity(NUM_LEVELS);
    while def.len() < NUM_LEVELS {
        let outer = rng.random_range(0..=4);
        if outer == 0 {
            def.push(rng.random_range(0..=1));
            rep.push(0);
            continue;
        }
        for o in 0..outer {
            let first_rep = i16::from(o != 0);
            let inner = rng.random_range(0..=4);
            if inner == 0 {
                def.push(rng.random_range(2..=3));
                rep.push(first_rep);
                continue;
            }
            for i in 0..inner {
                def.push(rng.random_range(4..=5));
                rep.push(if i == 0 { first_rep } else { 2 });
            }
        }
    }
    def.truncate(NUM_LEVELS);
    rep.truncate(NUM_LEVELS);
    (def, rep)
}

/// Runs of random length in `min_run..=max_run`, each a random level in `0..=max_level`.
fn runs(rng: &mut StdRng, max_level: i16, min_run: usize, max_run: usize) -> Vec<i16> {
    let mut out = Vec::with_capacity(NUM_LEVELS);
    while out.len() < NUM_LEVELS {
        let v = rng.random_range(0..=max_level);
        let n = rng.random_range(min_run..=max_run);
        out.extend(std::iter::repeat_n(v, n));
    }
    out.truncate(NUM_LEVELS);
    out
}

fn shapes() -> Vec<(String, i16, Vec<i16>)> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut shapes = vec![];
    for max_level in [1, 3, 7] {
        let levels = (0..NUM_LEVELS)
            .map(|_| rng.random_range(0..=max_level))
            .collect();
        shapes.push((format!("random/max{max_level}"), max_level, levels));
    }
    shapes.push((
        "alternating/max1".to_string(),
        1,
        (0..NUM_LEVELS).map(|i| (i % 2) as i16).collect(),
    ));
    shapes.push(("mixed_runs/max3".to_string(), 3, runs(&mut rng, 3, 1, 40)));
    shapes.push((
        "long_runs/max3".to_string(),
        3,
        runs(&mut rng, 3, 1000, 5000),
    ));
    shapes.push(("uniform/max1".to_string(), 1, vec![1; NUM_LEVELS]));
    // 75% valid: mostly short runs of 1 with occasional nulls
    shapes.push((
        "nullable_25pct/max1".to_string(),
        1,
        (0..NUM_LEVELS)
            .map(|_| (rng.random_range(0..4) != 0) as i16)
            .collect(),
    ));
    shapes.push((
        "sparse_99pct_null/max1".to_string(),
        1,
        (0..NUM_LEVELS)
            .map(|_| (rng.random_range(0..100) == 0) as i16)
            .collect(),
    ));
    let (def, rep) = list_levels(&mut rng, 9, 10);
    shapes.push(("list_0_9/def_max3".to_string(), 3, def));
    shapes.push(("list_0_9/rep_max1".to_string(), 1, rep));
    let (def, rep) = list_levels(&mut rng, 4, 25);
    shapes.push(("list_0_4_25pct_null/def_max3".to_string(), 3, def));
    shapes.push(("list_0_4_25pct_null/rep_max1".to_string(), 1, rep));
    let (def, rep) = nested_list_levels(&mut rng);
    shapes.push(("list_list/def_max5".to_string(), 5, def));
    shapes.push(("list_list/rep_max2".to_string(), 2, rep));
    shapes
}

fn bench_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("level_encoding");
    group.throughput(Throughput::Elements(NUM_LEVELS as u64));
    for (name, max_level, levels) in shapes() {
        // What the column writer does per mini-batch: count values at max level
        // and update the page's level histogram
        group.bench_function(&name, |b| {
            let mut encoder = LevelEncoder::v2_streaming(max_level);
            let mut histogram = vec![0i64; max_level as usize + 1];
            b.iter(|| {
                let mut at_max = 0usize;
                for chunk in black_box(levels.as_slice()).chunks(MINI_BATCH) {
                    encoder.put_with_observer(chunk, |level, count| {
                        at_max += count * (level == max_level) as usize;
                        histogram[level as usize] += count as i64;
                    });
                }
                black_box(at_max);
                encoder.flush_to(|bytes| black_box(bytes.len()))
            });
            black_box(&histogram);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_levels);
criterion_main!(benches);
