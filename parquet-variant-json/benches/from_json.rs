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
    let inputs = [
        (
            "integers",
            r#"{"id":123456789,"values":[1,2,3,4,5],"active":true}"#,
        ),
        (
            "mixed",
            r#"{"id":123456789,"name":"alice","values":[1,2.5,3e2,null],"active":true}"#,
        ),
        (
            "decimals",
            r#"{"small":1.23,"scale10":0.0000000001,"scale19":0.0000000000000000001,"decimal4_max":99999999.9,"decimal8_max":99999999999999999.9,"large":0.9999999999999999999}"#,
        ),
        (
            "strings",
            r#"{"first":"unescaped alpha","second":"unescaped beta","third":"unescaped gamma"}"#,
        ),
        (
            "escaped_strings",
            r#"{"first":"line one\nline two","second":"quote: \"value\"","third":"unicode: \u2764"}"#,
        ),
        (
            "nested",
            r#"{"outer":[{"id":1,"values":[1.25,2.50]},{"id":2,"values":[3.75,4.00]}]}"#,
        ),
        (
            "descending_keys",
            r#"{"z":1,"y":2,"x":3,"w":4,"v":5,"u":6,"t":7,"s":8,"r":9,"q":10,"p":11,"o":12}"#,
        ),
        (
            "duplicate_keys",
            r#"{"value":{"discarded":[1,2,3,4,5]},"other":1,"value":{"kept":1.25}}"#,
        ),
    ];

    let mut group = c.benchmark_group("variant_from_json");
    for (name, json) in inputs {
        group.throughput(Throughput::Bytes(json.len() as u64));
        group.bench_with_input(BenchmarkId::new("direct", name), json, |b, json| {
            b.iter(|| {
                let mut builder = VariantBuilder::new();
                builder.append_json(black_box(json)).unwrap();
                black_box(builder.finish())
            });
        });
        group.bench_with_input(BenchmarkId::new("value_tree", name), json, |b, json| {
            b.iter(|| {
                let value: Value = serde_json::from_str(black_box(json)).unwrap();
                let mut builder = VariantBuilder::new();
                append_json(&value, &mut builder).unwrap();
                black_box(builder.finish())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_from_json);
criterion_main!(benches);
