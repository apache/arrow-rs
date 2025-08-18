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

//! Benchmarks for `arrow‑avro` **Decoder**
//!

extern crate arrow_avro;
extern crate criterion;
extern crate once_cell;

use arrow_array::{
    types::{Int32Type, Int64Type, TimestampMicrosecondType},
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, PrimitiveArray, RecordBatch,
};
use arrow_avro::writer::AvroWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use once_cell::sync::Lazy;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempfile;

const SIZES: [usize; 3] = [100, 10_000, 1_000_000];

fn make_bool_array(n: usize) -> BooleanArray {
    BooleanArray::from_iter((0..n).map(|i| Some(i % 2 == 0)))
}

fn make_i32_array(n: usize) -> PrimitiveArray<Int32Type> {
    PrimitiveArray::<Int32Type>::from_iter_values((0..n).map(|i| i as i32))
}

fn make_i64_array(n: usize) -> PrimitiveArray<Int64Type> {
    PrimitiveArray::<Int64Type>::from_iter_values((0..n).map(|i| i as i64))
}

fn make_f32_array(n: usize) -> Float32Array {
    Float32Array::from_iter_values((0..n).map(|i| i as f32 + 0.5678))
}

fn make_f64_array(n: usize) -> Float64Array {
    Float64Array::from_iter_values((0..n).map(|i| i as f64 + 0.1234))
}

fn make_binary_array(n: usize) -> BinaryArray {
    let payloads: Vec<Vec<u8>> = (0..n).map(|i| vec![(i & 0xFF) as u8; 16]).collect();
    let views: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
    BinaryArray::from_vec(views)
}

fn make_ts_micros_array(n: usize) -> PrimitiveArray<TimestampMicrosecondType> {
    let base: i64 = 1_600_000_000_000_000;
    PrimitiveArray::<TimestampMicrosecondType>::from_iter_values((0..n).map(|i| base + i as i64))
}

fn schema_single(name: &str, dt: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, dt, false)]))
}

fn schema_mixed() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("f1", DataType::Int32, false),
        Field::new("f2", DataType::Int64, false),
        Field::new("f3", DataType::Binary, false),
        Field::new("f4", DataType::Float64, false),
    ]))
}

static BOOLEAN_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Boolean);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_bool_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static INT32_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Int32);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_i32_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static INT64_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Int64);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_i64_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static FLOAT32_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Float32);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_f32_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static FLOAT64_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Float64);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_f64_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static BINARY_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Binary);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_binary_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static TIMESTAMP_US_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Timestamp(TimeUnit::Microsecond, None));
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_ts_micros_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static MIXED_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_mixed();
    SIZES
        .iter()
        .map(|&n| {
            let f1: ArrayRef = Arc::new(make_i32_array(n));
            let f2: ArrayRef = Arc::new(make_i64_array(n));
            let f3: ArrayRef = Arc::new(make_binary_array(n));
            let f4: ArrayRef = Arc::new(make_f64_array(n));
            RecordBatch::try_new(schema.clone(), vec![f1, f2, f3, f4]).unwrap()
        })
        .collect()
});

fn ocf_size_for_batch(batch: &RecordBatch) -> usize {
    let schema_owned: Schema = (*batch.schema()).clone();
    let cursor = Cursor::new(Vec::<u8>::with_capacity(1024));
    let mut writer = AvroWriter::new(cursor, schema_owned).expect("create writer");
    writer.write(batch).expect("write batch");
    writer.finish().expect("finish writer");
    let inner = writer.into_inner();
    inner.into_inner().len()
}

fn bench_writer_scenario(c: &mut Criterion, name: &str, data_sets: &[RecordBatch]) {
    let mut group = c.benchmark_group(name);
    let schema_owned: Schema = (*data_sets[0].schema()).clone();
    for (idx, &rows) in SIZES.iter().enumerate() {
        let batch = &data_sets[idx];
        let bytes = ocf_size_for_batch(batch);
        group.throughput(Throughput::Bytes(bytes as u64));
        match rows {
            10_000 => {
                group
                    .sample_size(25)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            1_000_000 => {
                group
                    .sample_size(10)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            _ => {}
        }
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter_batched_ref(
                || {
                    let file = tempfile().expect("create temp file");
                    AvroWriter::new(file, schema_owned.clone()).expect("create writer")
                },
                |writer| {
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    bench_writer_scenario(c, "write-Boolean", &BOOLEAN_DATA);
    bench_writer_scenario(c, "write-Int32", &INT32_DATA);
    bench_writer_scenario(c, "write-Int64", &INT64_DATA);
    bench_writer_scenario(c, "write-Float32", &FLOAT32_DATA);
    bench_writer_scenario(c, "write-Float64", &FLOAT64_DATA);
    bench_writer_scenario(c, "write-Binary(Bytes)", &BINARY_DATA);
    bench_writer_scenario(c, "write-TimestampMicros", &TIMESTAMP_US_DATA);
    bench_writer_scenario(c, "write-Mixed", &MIXED_DATA);
}

criterion_group! {
    name = avro_writer;
    config = Criterion::default().configure_from_args();
    targets = criterion_benches
}
criterion_main!(avro_writer);
