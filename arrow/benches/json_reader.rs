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

use arrow::datatypes::*;
use arrow::util::bench_util::{create_primitive_array, create_string_array};
use arrow_array::RecordBatch;
use arrow_json::raw::RawReaderBuilder;
use arrow_json::{LineDelimitedWriter, ReaderBuilder};
use std::io::Cursor;
use std::sync::Arc;

fn do_bench(c: &mut Criterion, name: &str, json: &str, schema: SchemaRef) {
    c.bench_function(&format!("{name} (basic)"), |b| {
        b.iter(|| {
            let cursor = Cursor::new(black_box(json));
            let builder = ReaderBuilder::new()
                .with_schema(schema.clone())
                .with_batch_size(64);

            let mut reader = builder.build(cursor).unwrap();
            while let Some(next) = reader.next().transpose() {
                next.unwrap();
            }
        })
    });

    c.bench_function(&format!("{name} (raw)"), |b| {
        b.iter(|| {
            let cursor = Cursor::new(black_box(json));
            let builder = RawReaderBuilder::new(schema.clone()).with_batch_size(64);
            let reader = builder.build(cursor).unwrap();
            for next in reader {
                next.unwrap();
            }
        })
    });
}

fn small_bench_primitive(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, true),
        Field::new("c4", DataType::Boolean, true),
    ]));

    let json_content = r#"
        {"c1": "eleven", "c2": 6.2222222225, "c3": 5.0, "c4": false}
        {"c1": "twelve", "c2": -55555555555555.2, "c3": 3}
        {"c1": null, "c2": 3, "c3": 125, "c4": null}
        {"c2": -35, "c3": 100.0, "c4": true}
        {"c1": "fifteen", "c2": null, "c4": true}
        {"c1": "eleven", "c2": 6.2222222225, "c3": 5.0, "c4": false}
        {"c1": "twelve", "c2": -55555555555555.2, "c3": 3}
        {"c1": null, "c2": 3, "c3": 125, "c4": null}
        {"c2": -35, "c3": 100.0, "c4": true}
        {"c1": "fifteen", "c2": null, "c4": true}
        "#;

    do_bench(c, "small_bench_primitive", json_content, schema)
}

fn large_bench_primitive(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::UInt32, true),
        Field::new("c4", DataType::Utf8, true),
        Field::new("c5", DataType::Utf8, true),
        Field::new("c6", DataType::Float32, true),
    ]));

    let c1 = Arc::new(create_string_array::<i32>(4096, 0.));
    let c2 = Arc::new(create_primitive_array::<Int32Type>(4096, 0.));
    let c3 = Arc::new(create_primitive_array::<UInt32Type>(4096, 0.));
    let c4 = Arc::new(create_string_array::<i32>(4096, 0.2));
    let c5 = Arc::new(create_string_array::<i32>(4096, 0.2));
    let c6 = Arc::new(create_primitive_array::<Float32Type>(4096, 0.2));

    let batch = RecordBatch::try_from_iter([
        ("c1", c1 as _),
        ("c2", c2 as _),
        ("c3", c3 as _),
        ("c4", c4 as _),
        ("c5", c5 as _),
        ("c6", c6 as _),
    ])
    .unwrap();

    let mut out = Vec::with_capacity(1024);
    LineDelimitedWriter::new(&mut out).write(batch).unwrap();

    let json = std::str::from_utf8(&out).unwrap();
    do_bench(c, "large_bench_primitive", json, schema)
}

fn small_bench_list() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "c1",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "c2",
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        Field::new(
            "c3",
            DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
            true,
        ),
        Field::new(
            "c4",
            DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
    ]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let json_content = r#"
        {"c1": ["eleven"], "c2": [6.2222222225, -3.2, null], "c3": [5.0, 6], "c4": [false, true]}
        {"c1": ["twelve"], "c2": [-55555555555555.2, 12500000.0], "c3": [3, 4, 5]}
        {"c1": null, "c2": [3], "c3": [125, 127, 129], "c4": [null, false, true]}
        {"c2": [-35], "c3": [100.0, 200.0], "c4": null}
        {"c1": ["fifteen"], "c2": [null, 2.1, 1.5, -3], "c4": [true, false, null]}
        {"c1": ["fifteen"], "c2": [], "c4": [true, false, null]}
        {"c1": ["eleven"], "c2": [6.2222222225, -3.2, null], "c3": [5.0, 6], "c4": [false, true]}
        {"c1": ["twelve"], "c2": [-55555555555555.2, 12500000.0], "c3": [3, 4, 5]}
        {"c1": null, "c2": [3], "c3": [125, 127, 129], "c4": [null, false, true]}
        {"c2": [-35], "c3": [100.0, 200.0], "c4": null}
        {"c1": ["fifteen"], "c2": [null, 2.1, 1.5, -3], "c4": [true, false, null]}
        {"c1": ["fifteen"], "c2": [], "c4": [true, false, null]}
        "#;
    let cursor = Cursor::new(black_box(json_content));
    let mut reader = builder.build(cursor).unwrap();
    reader.next().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("small_bench_list", |b| b.iter(small_bench_list));

    small_bench_primitive(c);
    large_bench_primitive(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
