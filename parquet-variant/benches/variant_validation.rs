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

extern crate parquet_variant;

use criterion::*;

use parquet_variant::{json_to_variant, Variant, VariantBuilder};

fn generate_large_object() -> (Vec<u8>, Vec<u8>) {
    // 256 elements (keys: 000-255) - each element is an object of 256 elements (240-495) - each
    // element a list of numbers from 0-127
    let keys: Vec<String> = (0..=255).map(|n| format!("{n:03}")).collect();
    let innermost_list: String = format!(
        "[{}]",
        (0..=127)
            .map(|n| format!("{n}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let inner_keys: Vec<String> = (240..=495).map(|n| format!("{n}")).collect();
    let inner_object = format!(
        "{{{}:{}}}",
        inner_keys
            .iter()
            .map(|k| format!("\"{k}\""))
            .collect::<Vec<String>>()
            .join(format!(":{innermost_list},").as_str()),
        innermost_list
    );
    let json = format!(
        "{{{}:{}}}",
        keys.iter()
            .map(|k| format!("\"{k}\""))
            .collect::<Vec<String>>()
            .join(format!(":{inner_object},").as_str()),
        inner_object
    );
    // Manually verify raw JSON value size
    let mut variant_builder = VariantBuilder::new();
    json_to_variant(&json, &mut variant_builder).unwrap();
    variant_builder.finish()
}

fn generate_complex_object() -> (Vec<u8>, Vec<u8>) {
    let mut variant_builder = VariantBuilder::new();
    let mut object_builder = variant_builder.new_object();
    let mut inner_list_builder = object_builder.new_list("booleans");
    inner_list_builder.append_value(Variant::BooleanTrue);
    inner_list_builder.append_value(Variant::BooleanFalse);
    inner_list_builder.finish();
    object_builder.insert("null", Variant::Null);
    let mut inner_list_builder = object_builder.new_list("numbers");
    inner_list_builder.append_value(Variant::Int8(4));
    inner_list_builder.append_value(Variant::Double(-3e0));
    inner_list_builder.append_value(Variant::Double(1001e-3));
    inner_list_builder.finish();
    object_builder.finish().unwrap();

    variant_builder.finish()
}

fn generate_large_nested_list() -> (Vec<u8>, Vec<u8>) {
    let mut variant_builder = VariantBuilder::new();
    let mut list_builder = variant_builder.new_list();
    for _ in 0..256 {
        let mut list_builder_inner = list_builder.new_list();
        for _ in 0..255 {
            list_builder_inner.append_value(Variant::Null);
        }
        list_builder_inner.finish();
    }
    list_builder.finish();
    variant_builder.finish()
}

// Generates a large object and performs full validation
fn bench_validate_large_object(c: &mut Criterion) {
    c.bench_function("bench_validate_large_object", |b| {
        b.iter_batched(
            generate_large_object,
            |(m, v)| {
                std::hint::black_box(Variant::try_new(&m, &v).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_validate_complex_object(c: &mut Criterion) {
    c.bench_function("bench_validate_complex_object", |b| {
        b.iter_batched(
            generate_complex_object,
            |(m, v)| {
                std::hint::black_box(Variant::try_new(&m, &v).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_validate_large_nested_list(c: &mut Criterion) {
    c.bench_function("bench_validate_large_nested_list", |b| {
        b.iter_batched(
            generate_large_nested_list,
            |(m, v)| {
                std::hint::black_box(Variant::try_new(&m, &v).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_validate_large_object,
    bench_validate_complex_object,
    bench_validate_large_nested_list
);

criterion_main!(benches);
