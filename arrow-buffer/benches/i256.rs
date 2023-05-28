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

/// Returns fixed seedable RNG
fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn create_i256_vec(size: usize) -> Vec<i256> {
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| i256::from_i128(rng.gen::<i128>()))
        .collect()
}

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

    c.bench_function(&format!("i256_div"), |b| {
        b.iter(|| {
            for number_a in create_i256_vec(10) {
                for number_b in create_i256_vec(5) {
                    number_a.checked_div(number_b);
                    number_a.wrapping_div(number_b);
                }
            }
        });
    });

    c.bench_function(&format!("i256_rem"), |b| {
        b.iter(|| {
            for number_a in create_i256_vec(10) {
                for number_b in create_i256_vec(5) {
                    number_a.checked_rem(number_b);
                    number_a.wrapping_rem(number_b);
                }
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
