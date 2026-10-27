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

use std::hint::black_box;
use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{DictionaryArray, Int32Array, StringArray};
use arrow_cmp::make_comparator;
use arrow_schema::SortOptions;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const BATCH_SIZE: usize = 8_192;
const CALLS: usize = 100;
const CARDINALITIES: [usize; 8] = [10, 50, 100, 500, 750, 1_000, 4_000, 8_192];
const SEED: u64 = 0xC0FFEE;

fn build_dict(rows: usize, cardinality: usize, seed: u64) -> DictionaryArray<Int32Type> {
    let values: Vec<String> = (0..cardinality).map(|i| format!("v{i:07}")).collect();
    let values = StringArray::from(values);

    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Int32Array = (0..rows)
        .map(|_| rng.random_range(0..cardinality) as i32)
        .collect();

    DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap()
}

fn index_pairs(bound: usize, count: usize) -> Vec<(usize, usize)> {
    let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(0x1234));
    (0..count)
        .map(|_| (rng.random_range(0..bound), rng.random_range(0..bound)))
        .collect()
}

fn bench_dict_cmp(c: &mut Criterion) {
    let pairs = index_pairs(BATCH_SIZE, CALLS);

    let mut group = c.benchmark_group("dict_cmp_utf8/100_calls_8k_batch");
    group.throughput(Throughput::Elements(CALLS as u64));
    group.sample_size(10);

    for &card in &CARDINALITIES {
        let left = build_dict(BATCH_SIZE, card, SEED);
        let right = build_dict(BATCH_SIZE, card, SEED ^ 0xA5A5_A5A5);

        group.bench_with_input(BenchmarkId::from_parameter(card), &card, |b, _| {
            b.iter(|| {
                let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();
                let mut acc = 0i64;
                for &(i, j) in &pairs {
                    acc += cmp(i, j) as i64;
                }
                black_box(acc)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_dict_cmp);
criterion_main!(benches);
