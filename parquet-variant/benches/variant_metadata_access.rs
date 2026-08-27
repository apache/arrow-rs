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

//! Benchmarks for reading field names out of an already-validated variant
//! metadata dictionary.
//!
//! The shape modelled here is a write-heavy workload that stores semi-structured
//! attributes as Variant: a modest dictionary of short, ASCII field names that is
//! validated once and then read many times, once per object field access.

use criterion::*;
use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
use std::hint::black_box;

/// Short, ASCII field names, in the style of telemetry / log attribute keys.
fn field_names(n: usize) -> Vec<String> {
    const SUFFIXES: [&str; 8] = [
        "id",
        "name",
        "count",
        "status",
        "region",
        "duration_ms",
        "user_agent",
        "retry",
    ];
    (0..n)
        .map(|i| format!("attr_{}_{}", SUFFIXES[i % SUFFIXES.len()], i))
        .collect()
}

/// One object whose keys are `names`, plus small integer values.
fn build_object(names: &[String]) -> (Vec<u8>, Vec<u8>) {
    let mut builder = VariantBuilder::new();
    let mut object = builder.new_object();
    for (i, name) in names.iter().enumerate() {
        object.insert(name.as_str(), Variant::Int32(i as i32));
    }
    object.finish();
    builder.finish()
}

fn bench_metadata_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_iter");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, _value) = build_object(&names);
        // Pay full validation once, outside the measured loop.
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_fully_validated());
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for name in metadata.iter() {
                    acc += black_box(name).len();
                }
                black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_field_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_field_name");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = build_object(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata.clone(), &value).unwrap();
        let object = variant.as_object().unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for i in 0..n {
                    acc += black_box(object.field_name(i).unwrap()).len();
                }
                black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_iter");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = build_object(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata.clone(), &value).unwrap();
        let object = variant.as_object().unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for (name, _value) in object.iter() {
                    acc += black_box(name).len();
                }
                black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_get_by_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_get_by_name");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = build_object(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata.clone(), &value).unwrap();
        let object = variant.as_object().unwrap();
        // Always look up the last key, the worst case for a linear scan.
        let needle = names.last().unwrap().as_str();
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| black_box(object.get(black_box(needle))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_metadata_iter,
    bench_object_field_name,
    bench_object_iter,
    bench_object_get_by_name,
);
criterion_main!(benches);
