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

use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::{Map, Number, Value};
use std::fmt::Write;
use std::sync::Arc;

fn build_schema(field_count: usize) -> Arc<Schema> {
    let fields: Vec<Field> = (0..field_count)
        .map(|i| Field::new(format!("f{i}"), DataType::Int64, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_wide_json(rows: usize, fields: usize) -> Vec<u8> {
    let mut out = String::with_capacity(rows * fields * 12);
    for row in 0..rows {
        out.push('{');
        for field in 0..fields {
            if field > 0 {
                out.push(',');
            }
            let value = row as i64 + field as i64;
            write!(&mut out, "\"f{field}\":{value}").unwrap();
        }
        out.push('}');
        out.push('\n');
    }
    out.into_bytes()
}

fn build_wide_values(rows: usize, fields: usize) -> Vec<Value> {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut map = Map::with_capacity(fields);
        for field in 0..fields {
            let key = format!("f{field}");
            let value = Number::from((row + field) as i64);
            map.insert(key, Value::Number(value));
        }
        out.push(Value::Object(map));
    }
    out
}

fn bench_decode_wide_object(c: &mut Criterion) {
    let rows = 4096;
    let fields = 64;
    let data = build_wide_json(rows, fields);
    let schema = build_schema(fields);

    c.bench_function("decode_wide_object_i64_json", |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(1024)
                .build_decoder()
                .unwrap();

            let mut offset = 0;
            while offset < data.len() {
                let read = decoder.decode(&data[offset..]).unwrap();
                if read == 0 {
                    break;
                }
                offset += read;
                while let Some(_batch) = decoder.flush().unwrap() {}
            }
        })
    });
}

fn bench_serialize_wide_object(c: &mut Criterion) {
    let rows = 4096;
    let fields = 64;
    let values = build_wide_values(rows, fields);
    let schema = build_schema(fields);

    c.bench_function("decode_wide_object_i64_serialize", |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(1024)
                .build_decoder()
                .unwrap();

            decoder.serialize(&values).unwrap();
            while let Some(_batch) = decoder.flush().unwrap() {}
        })
    });
}

criterion_group!(
    benches,
    bench_decode_wide_object,
    bench_serialize_wide_object
);
criterion_main!(benches);
