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

use criterion::*;

use parquet_variant::{Variant, VariantBuilder, VariantMetadata};

fn generate_large_object() -> (Vec<u8>, Vec<u8>) {
    // 256 elements (keys: 000-255) - each element is an object of 256 elements (240-495) - each
    // element a list of numbers from 0-127
    let mut variant_builder = VariantBuilder::new();
    let mut outer_object = variant_builder.new_object();

    for i in 0..=125 {
        let key = format!("{i:03}");
        let mut inner_object = outer_object.new_object(&key);

        for j in 125..=250 {
            let inner_key = format!("{j}");
            let mut list_builder = inner_object.new_list(&inner_key);

            for k in 0..=127 {
                list_builder.append_value(Variant::Int8(k));
            }
            list_builder.finish();
        }
        inner_object.finish();
    }
    outer_object.finish();

    variant_builder.finish()
}

fn generate_complex_object() -> (Vec<u8>, Vec<u8>) {
    let mut variant_builder = VariantBuilder::new();
    let mut object_builder = variant_builder.new_object();
    let mut inner_list_builder = object_builder.new_list("booleans");

    for _ in 0..1024 {
        inner_list_builder.append_value(Variant::BooleanTrue);
    }

    inner_list_builder.finish();
    object_builder.insert("null", Variant::Null);
    let mut inner_list_builder = object_builder.new_list("numbers");
    for _ in 0..1024 {
        inner_list_builder.append_value(Variant::Int8(4));
        inner_list_builder.append_value(Variant::Double(-3e0));
        inner_list_builder.append_value(Variant::Double(1001e-3));
    }
    inner_list_builder.finish();

    let mut inner_object_builder = object_builder.new_object("nested");

    for i in 0..2048 {
        let key = format!("{}", 1024 - i);
        inner_object_builder.insert(&key, i);
    }
    inner_object_builder.finish();

    object_builder.finish();

    variant_builder.finish()
}

fn generate_large_nested_list() -> (Vec<u8>, Vec<u8>) {
    let mut variant_builder = VariantBuilder::new();
    let mut list_builder = variant_builder.new_list();
    for _ in 0..255 {
        let mut list_builder_inner = list_builder.new_list();
        for _ in 0..120 {
            list_builder_inner.append_value(Variant::Null);

            let mut list_builder_inner_inner = list_builder_inner.new_list();
            for _ in 0..20 {
                list_builder_inner_inner.append_value(Variant::Double(-3e0));
            }

            list_builder_inner_inner.finish();
        }
        list_builder_inner.finish();
    }
    list_builder.finish();
    variant_builder.finish()
}

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
fn generate_object_with_field_names(names: &[String]) -> (Vec<u8>, Vec<u8>) {
    let mut variant_builder = VariantBuilder::new();
    let mut object_builder = variant_builder.new_object();
    for (i, name) in names.iter().enumerate() {
        object_builder.insert(name.as_str(), Variant::Int32(i as i32));
    }
    object_builder.finish();
    variant_builder.finish()
}

// Generates a large object and performs full validation
fn bench_validate_large_object(c: &mut Criterion) {
    let (metadata, value) = generate_large_object();
    c.bench_function("bench_validate_large_object", |b| {
        b.iter(|| {
            std::hint::black_box(Variant::try_new(&metadata, &value).unwrap());
        })
    });
}

fn bench_validate_complex_object(c: &mut Criterion) {
    let (metadata, value) = generate_complex_object();
    c.bench_function("bench_validate_complex_object", |b| {
        b.iter(|| {
            std::hint::black_box(Variant::try_new(&metadata, &value).unwrap());
        })
    });
}

fn bench_validate_large_nested_list(c: &mut Criterion) {
    let (metadata, value) = generate_large_nested_list();
    c.bench_function("bench_validate_large_nested_list", |b| {
        b.iter(|| {
            std::hint::black_box(Variant::try_new(&metadata, &value).unwrap());
        })
    });
}

// The benchmarks below read field names back out of already-validated metadata, the path that
// runs once per object field access. Validation is paid once, outside the measured loop.

fn bench_metadata_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_iter");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, _value) = generate_object_with_field_names(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for name in metadata.iter() {
                    acc += std::hint::black_box(name).len();
                }
                std::hint::black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_field_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_field_name");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = generate_object_with_field_names(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let object = variant.as_object().unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for i in 0..n {
                    acc += std::hint::black_box(object.field_name(i).unwrap()).len();
                }
                std::hint::black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_iter");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = generate_object_with_field_names(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let object = variant.as_object().unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for (name, _value) in object.iter() {
                    acc += std::hint::black_box(name).len();
                }
                std::hint::black_box(acc)
            });
        });
    }
    group.finish();
}

fn bench_object_get_by_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_get_by_name");
    for n in [8usize, 32, 128] {
        let names = field_names(n);
        let (metadata, value) = generate_object_with_field_names(&names);
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let object = variant.as_object().unwrap();
// Always look up the last generated key.
        let needle = names.last().unwrap().as_str();
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| std::hint::black_box(object.get(std::hint::black_box(needle))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_validate_large_object,
    bench_validate_complex_object,
    bench_validate_large_nested_list,
    bench_metadata_iter,
    bench_object_field_name,
    bench_object_iter,
    bench_object_get_by_name,
);

criterion_main!(benches);
