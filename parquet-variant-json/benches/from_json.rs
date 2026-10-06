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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet_variant::VariantBuilder;
use parquet_variant_json::{JsonToVariant, append_json};
use serde_json::Value;
use std::hint::black_box;

fn bench_from_json(c: &mut Criterion) {
    let large_array = format!(
        "[{}]",
        (0..1024)
            .map(|number| number.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let large_string = format!(r#"{{"payload":"{}","id":42}}"#, "x".repeat(8192));
    let duplicate_large = format!(r#"{{"a":"{}","a":0}}"#, "x".repeat(100_000));
    let inputs = [
        (
            "integers",
            r#"{"id":123456789,"values":[1,2,3,4,5],"active":true}"#,
        ),
        (
            "decimals",
            r#"{"small":1.23,"medium":999999999.0,"large":0.9999999999999999999}"#,
        ),
        (
            "strings",
            r#"{"first":"alpha","second":"beta","third":"gamma"}"#,
        ),
        (
            "escaped_strings",
            r#"{"line":"one\ntwo","quote":"\"value\"","unicode":"\u2764"}"#,
        ),
        (
            "nested",
            r#"{"outer":[{"id":1,"values":[1.25,2.50]},{"id":2,"values":[3.75,4.00]}]}"#,
        ),
        ("large_array_1024", large_array.as_str()),
        ("large_string_8k", large_string.as_str()),
        ("duplicate_key_100k", duplicate_large.as_str()),
    ];

    let mut parse_group = c.benchmark_group("json_parse");
    for (name, json) in &inputs {
        parse_group.throughput(Throughput::Bytes(
            u64::try_from(json.len()).expect("input size fits in u64"),
        ));
        parse_group.bench_with_input(BenchmarkId::from_parameter(name), json, |b, json| {
            b.iter(|| serde_json::from_str::<Value>(black_box(json)).expect("valid JSON"));
        });
    }
    parse_group.finish();

    let mut value_group = c.benchmark_group("variant_from_json_value");
    for (name, json) in &inputs {
        let parsed: Value = serde_json::from_str(json).expect("valid JSON input");
        value_group.bench_with_input(BenchmarkId::from_parameter(name), &parsed, |b, value| {
            b.iter(|| {
                let mut builder = VariantBuilder::new();
                append_json(black_box(value), &mut builder).expect("valid Variant");
                black_box(builder.finish())
            });
        });
    }
    value_group.finish();

    let mut full_group = c.benchmark_group("variant_from_json_string");
    for (name, json) in &inputs {
        full_group.throughput(Throughput::Bytes(
            u64::try_from(json.len()).expect("input size fits in u64"),
        ));
        full_group.bench_with_input(BenchmarkId::from_parameter(name), json, |b, json| {
            b.iter(|| {
                let mut builder = VariantBuilder::new();
                builder.append_json(black_box(json)).expect("valid JSON");
                black_box(builder.finish())
            });
        });
    }
    full_group.finish();
}

criterion_group!(benches, bench_from_json);
criterion_main!(benches);
