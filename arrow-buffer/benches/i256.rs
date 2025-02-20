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

use arrow_buffer::i256;
use criterion::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::str::FromStr;

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

    for number in numbers {
        let t = black_box(number.to_string());
        c.bench_function(&format!("i256_parse({t})"), |b| {
            b.iter(|| i256::from_str(&t).unwrap());
        });
    }

    let mut rng = StdRng::seed_from_u64(42);

    let numerators: Vec<_> = (0..SIZE)
        .map(|_| {
            let high = rng.gen_range(1000..i128::MAX);
            let low = rng.gen();
            i256::from_parts(low, high)
        })
        .collect();

    let divisors: Vec<_> = numerators
        .iter()
        .map(|n| {
            let quotient = rng.gen_range(1..100_i32);
            n.wrapping_div(i256::from(quotient))
        })
        .collect();

    c.bench_function("i256_div_rem small quotient", |b| {
        b.iter(|| {
            for (n, d) in numerators.iter().zip(&divisors) {
                black_box(n.wrapping_div(*d));
            }
        });
    });

    let divisors: Vec<_> = (0..SIZE)
        .map(|_| i256::from(rng.gen_range(1..100_i32)))
        .collect();

    c.bench_function("i256_div_rem small divisor", |b| {
        b.iter(|| {
            for (n, d) in numerators.iter().zip(&divisors) {
                black_box(n.wrapping_div(*d));
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
