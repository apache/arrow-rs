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

use arrow_array::builder::{Date32Builder, Decimal128Builder, Int32Builder};
use arrow_array::{builder::StringBuilder, RecordBatch};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow_ipc::CompressionType;
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_ipc_stream_writer");

    group.bench_function("StreamWriter/write_10", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    group.bench_function("StreamWriter/write_10/zstd", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                    .unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    group.bench_function("FileWriter/write_10", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let mut writer = FileWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });
}

fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();
    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }
    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    )
    .unwrap()
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
