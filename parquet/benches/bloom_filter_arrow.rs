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

//! Benchmarks for the Arrow batch APIs of [`Sbbf`].

use std::hint;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::bloom_filter::Sbbf;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const NUM_ROWS: usize = 1 << 20;

fn arrays() -> Vec<(&'static str, ArrayRef)> {
    let mut rng = StdRng::seed_from_u64(42);
    let ints: Vec<i64> = (0..NUM_ROWS)
        .map(|_| rng.random_range(0..4_000_000))
        .collect();
    let strings: Vec<String> = ints.iter().map(|v| format!("key-{v:012}")).collect();
    vec![
        ("i64", Arc::new(Int64Array::from(ints)) as ArrayRef),
        ("utf8", Arc::new(StringArray::from(strings)) as ArrayRef),
    ]
}

fn bench_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_check");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    for (name, array) in arrays() {
        let mut filter = Sbbf::new_with_ndv_fpp(1_000_000, 0.01).unwrap();
        filter.insert_array(&array.slice(0, NUM_ROWS / 4)).unwrap();
        group.bench_with_input(BenchmarkId::new("check_array", name), &array, |b, a| {
            b.iter(|| filter.check_array(hint::black_box(a.as_ref())).unwrap())
        });
        // Baseline: compute the hashes, then check each with `check_hash`
        group.bench_with_input(
            BenchmarkId::new("hash_array_then_check_hash", name),
            &array,
            |b, a| {
                let mut hashes = vec![];
                b.iter(|| {
                    Sbbf::hash_array(hint::black_box(a.as_ref()), &mut hashes).unwrap();
                    hashes.iter().filter(|h| filter.check_hash(**h)).count()
                })
            },
        );
    }
    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    // 16M distinct values: a 32 MiB filter that does not fit in the CPU caches
    const NUM_KEYS: usize = 16 << 20;
    let mut group = c.benchmark_group("arrow_insert");
    group.sample_size(10);
    group.throughput(Throughput::Elements(NUM_KEYS as u64));
    let array: ArrayRef = Arc::new(Int64Array::from_iter_values(0..NUM_KEYS as i64));
    let mut hashes = vec![];
    Sbbf::hash_array(array.as_ref(), &mut hashes).unwrap();

    group.bench_function("insert_array", |b| {
        b.iter(|| {
            let mut f = Sbbf::new_with_ndv_fpp(NUM_KEYS as u64, 0.01).unwrap();
            f.insert_array(hint::black_box(array.as_ref())).unwrap();
            f
        })
    });
    group.bench_function("insert_hash_serial", |b| {
        b.iter(|| {
            let mut f = Sbbf::new_with_ndv_fpp(NUM_KEYS as u64, 0.01).unwrap();
            hashes.iter().for_each(|h| f.insert_hash(*h));
            f
        })
    });
    // Hashes sorted by block: inserts touch memory in order
    let mut sorted = hashes.clone();
    sorted.sort_unstable();
    group.bench_function("insert_hash_sorted", |b| {
        b.iter(|| {
            let mut f = Sbbf::new_with_ndv_fpp(NUM_KEYS as u64, 0.01).unwrap();
            sorted.iter().for_each(|h| f.insert_hash(*h));
            f
        })
    });
    group.finish();
}

criterion_group!(benches, bench_check, bench_insert);
criterion_main!(benches);
