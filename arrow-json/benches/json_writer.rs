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

use arrow_array::builder::{FixedSizeListBuilder, Int64Builder, ListBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_json::LineDelimitedWriter;
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;

const ROWS: usize = 1 << 17; // 128K rows
const LIST_SHORT_ELEMENTS: usize = 5;
const LIST_LONG_ELEMENTS: usize = 100;

fn build_list_batch(rows: usize, elements: usize) -> RecordBatch {
    let mut list_builder = ListBuilder::new(Int64Builder::new());
    for row in 0..rows {
        for i in 0..elements {
            list_builder.values().append_value((row + i) as i64);
        }
        list_builder.append(true);
    }
    let list_array = list_builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false))),
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap()
}

fn bench_write_list(c: &mut Criterion) {
    let short_batch = build_list_batch(ROWS, LIST_SHORT_ELEMENTS);
    let long_batch = build_list_batch(ROWS, LIST_LONG_ELEMENTS);

    let mut group = c.benchmark_group("write_list_i64");
    // Short lists: tests per-list overhead (few elements per row)
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("short", |b| {
        let mut buf = Vec::with_capacity(ROWS * LIST_SHORT_ELEMENTS * 8);
        b.iter(|| {
            buf.clear();
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write(&short_batch).unwrap();
            writer.finish().unwrap();
        })
    });

    // Long lists: tests child element encode throughput (many elements per row)
    group.bench_function("long", |b| {
        let mut buf = Vec::with_capacity(ROWS * LIST_LONG_ELEMENTS * 8);
        b.iter(|| {
            buf.clear();
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write(&long_batch).unwrap();
            writer.finish().unwrap();
        })
    });

    group.finish();
}

fn build_fixed_size_list_batch(rows: usize, elements: usize) -> RecordBatch {
    let mut builder = FixedSizeListBuilder::new(Int64Builder::new(), elements as i32);
    for row in 0..rows {
        for i in 0..elements {
            builder.values().append_value((row + i) as i64);
        }
        builder.append(true);
    }
    let fsl_array = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "fixed_size_list",
        fsl_array.data_type().clone(),
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(fsl_array)]).unwrap()
}

fn bench_write_fixed_size_list(c: &mut Criterion) {
    let short_batch = build_fixed_size_list_batch(ROWS, LIST_SHORT_ELEMENTS);
    let long_batch = build_fixed_size_list_batch(ROWS, LIST_LONG_ELEMENTS);

    let mut group = c.benchmark_group("write_fixed_size_list_i64");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("short", |b| {
        let mut buf = Vec::with_capacity(ROWS * LIST_SHORT_ELEMENTS * 8);
        b.iter(|| {
            buf.clear();
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write(&short_batch).unwrap();
            writer.finish().unwrap();
        })
    });

    group.bench_function("long", |b| {
        let mut buf = Vec::with_capacity(ROWS * LIST_LONG_ELEMENTS * 8);
        b.iter(|| {
            buf.clear();
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write(&long_batch).unwrap();
            writer.finish().unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_write_list, bench_write_fixed_size_list);
criterion_main!(benches);
