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

//! Benchmarks for row-format encoding.
//!
//! Covers the key performance comparison between dictionary and UTF-8 encoding.
//! At large NDV the dictionary encoder re-encodes O(NDV) values per batch and
//! then does N random accesses into a ~400 KB buffer, causing L3 cache misses.
//! UTF-8 encoding reads the data sequentially, staying in L1/L2.

use std::sync::Arc;

use arrow_array::{ArrayRef, DictionaryArray, Int32Array, StringArray};
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Build a `Dictionary<Int32, Utf8>` with `num_rows` rows, `ndv` distinct values,
/// each value being an 11-character string like `"k1_00000042"`.
fn make_dict_array(num_rows: usize, ndv: usize) -> DictionaryArray<arrow_array::types::Int32Type> {
    let mut rng = StdRng::seed_from_u64(42);
    let values: StringArray = (0..ndv)
        .map(|i| Some(format!("k1_{i:08}")))
        .collect();
    let keys: Int32Array = (0..num_rows)
        .map(|_| Some(rng.random_range(0..ndv as i32)))
        .collect();
    DictionaryArray::try_new(keys, Arc::new(values)).unwrap()
}

/// Build a plain `Utf8` array with `num_rows` rows where each value is chosen
/// from `ndv` distinct strings. Produces the same logical data as `make_dict_array`
/// but without the dictionary encoding.
fn make_utf8_array(num_rows: usize, ndv: usize) -> StringArray {
    let mut rng = StdRng::seed_from_u64(42);
    let values: Vec<String> = (0..ndv).map(|i| format!("k1_{i:08}")).collect();
    (0..num_rows)
        .map(|_| Some(values[rng.random_range(0..ndv)].as_str()))
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmark: dictionary encoding with a fresh values Arc each call
// ---------------------------------------------------------------------------
// Simulates the worst case: every batch has a distinct values Arc, so the
// codec's Arc-ptr cache always misses and O(NDV) values are re-encoded.
fn bench_dict_fresh_values(c: &mut Criterion) {
    let num_rows = 4096;
    let mut group = c.benchmark_group("dict_encode/fresh_values_arc");

    for ndv in [100usize, 1_000, 10_000, 100_000, 200_000, 500_000, 1_000_000, 5_000_000] {
        let dict = make_dict_array(num_rows, ndv);

        let converter = RowConverter::new(vec![SortField::new(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ))])
        .unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(ndv),
            &ndv,
            |b, _| {
                b.iter(|| {
                    // Clone the dict so each iteration has a fresh Arc for values,
                    // bypassing the codec's Arc-ptr cache.
                    let fresh: ArrayRef = Arc::new(dict.clone());
                    converter.convert_columns(&[fresh]).unwrap()
                })
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: dictionary encoding with a shared values Arc across calls
// ---------------------------------------------------------------------------
// Simulates the common "global dictionary" workload where every batch shares
// the same values Arc. The codec caches the encoded Rows on the first call and
// skips O(NDV) re-encoding for every subsequent call.
fn bench_dict_shared_values(c: &mut Criterion) {
    let num_rows = 4096;
    let mut group = c.benchmark_group("dict_encode/shared_values_arc");

    for ndv in [100usize, 1_000, 10_000, 100_000, 200_000, 500_000, 1_000_000, 5_000_000] {
        let dict = make_dict_array(num_rows, ndv);
        // Use the dict directly (same Arc across iterations).
        let dict_ref: ArrayRef = Arc::new(dict);

        let converter = RowConverter::new(vec![SortField::new(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ))])
        .unwrap();

        // Warm the cache with one call before timing.
        converter.convert_columns(&[Arc::clone(&dict_ref)]).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(ndv),
            &ndv,
            |b, _| b.iter(|| converter.convert_columns(&[Arc::clone(&dict_ref)]).unwrap()),
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: plain UTF-8 encoding (baseline for comparison)
// ---------------------------------------------------------------------------
// Reads data sequentially → hardware prefetcher keeps L1/L2 warm.
// This is the lower bound we are trying to approach with the dict encoder.
fn bench_utf8(c: &mut Criterion) {
    let num_rows = 4096;
    let mut group = c.benchmark_group("dict_encode/utf8_baseline");

    for ndv in [100usize, 1_000, 10_000, 100_000, 200_000, 500_000, 1_000_000, 5_000_000] {
        let arr: ArrayRef = Arc::new(make_utf8_array(num_rows, ndv));
        let converter =
            RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(ndv),
            &ndv,
            |b, _| b.iter(|| converter.convert_columns(&[Arc::clone(&arr)]).unwrap()),
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: dictionary re-use across identical sorted batches
// ---------------------------------------------------------------------------
// The full sort pipeline: encode → (sort happens externally) → decode.
// Measures round-trip cost including convert_rows.
fn bench_dict_roundtrip(c: &mut Criterion) {
    let num_rows = 1024;
    let mut group = c.benchmark_group("dict_encode/roundtrip");

    for ndv in [100usize, 1_000, 10_000] {
        let dict: ArrayRef = Arc::new(make_dict_array(num_rows, ndv));
        let converter = RowConverter::new(vec![SortField::new(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ))])
        .unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(ndv),
            &ndv,
            |b, _| {
                b.iter(|| {
                    let rows = converter.convert_columns(&[Arc::clone(&dict)]).unwrap();
                    converter.convert_rows(&rows).unwrap()
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_dict_fresh_values,
    bench_dict_shared_values,
    bench_utf8,
    bench_dict_roundtrip,
);
criterion_main!(benches);
