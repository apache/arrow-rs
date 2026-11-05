// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_buffer::i256;
use criterion::*;
use num_traits::cast::ToPrimitive;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::{hint, str::FromStr};

const SIZE: usize = 1024;

fn criterion_benchmark(c: &mut Criterion) {
    let numbers = vec![
        i256::ZERO,
        i256::ONE,
        i256::MINUS_ONE,
        i256::from_i128(1233456789),
        i256::from_i128(-1233456789),
        i256::from_i128(i128::MAX),
        i256::from_i128(i128::MIN),
        i256::MIN,
        i256::MAX,
    ];

    for number in &numbers {
        let t = hint::black_box(number.to_string());
        c.bench_function(&format!("i256_parse({t})"), |b| {
            b.iter(|| i256::from_str(&t).unwrap());
        });
    }

    for number in &numbers {
        c.bench_function(&format!("i256_to_f64({number})"), |b| {
            b.iter(|| (*number).to_f64().unwrap())
        });
    }

    let mut rng = StdRng::seed_from_u64(42);

    let numerators: Vec<_> = (0..SIZE)
        .map(|_| {
            let high = rng.random_range(1000..i128::MAX);
            let low = rng.random();
            i256::from_parts(low, high)
        })
        .collect();

    let divisors: Vec<_> = numerators
        .iter()
        .map(|n| {
            let quotient = rng.random_range(1..100_i32);
            n.wrapping_div(i256::from(quotient))
        })
        .collect();

    c.bench_function("i256_div_rem small quotient", |b| {
        b.iter(|| {
            for (n, d) in numerators.iter().zip(&divisors) {
                hint::black_box(n.wrapping_div(*d));
            }
        });
    });

    let divisors: Vec<_> = (0..SIZE)
        .map(|_| i256::from(rng.random_range(1..100_i32)))
        .collect();

    c.bench_function("i256_div_rem small divisor", |b| {
        b.iter(|| {
            for (n, d) in numerators.iter().zip(&divisors) {
                hint::black_box(n.wrapping_div(*d));
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
