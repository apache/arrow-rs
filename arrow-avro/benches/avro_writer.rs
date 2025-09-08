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

//! Benchmarks for `arrow‑avro` **Writer** (Avro Object Container Files)
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
use rand::{
    distr::uniform::{SampleRange, SampleUniform},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempfile;

const SIZES: [usize; 4] = [4_096, 8_192, 100_000, 1_000_000];
const BASE_SEED: u64 = 0x5EED_1234_ABCD_EF01;
const MIX_CONST_1: u64 = 0x9E37_79B1_85EB_CA87;
const MIX_CONST_2: u64 = 0xC2B2_AE3D_27D4_EB4F;

#[inline]
fn rng_for(tag: u64, n: usize) -> StdRng {
    let seed = BASE_SEED ^ tag.wrapping_mul(MIX_CONST_1) ^ (n as u64).wrapping_mul(MIX_CONST_2);
    StdRng::seed_from_u64(seed)
}

#[inline]
fn sample_in<T, Rg>(rng: &mut StdRng, range: Rg) -> T
where
    T: SampleUniform,
    Rg: SampleRange<T>,
{
    rng.random_range(range)
}

#[inline]
fn make_bool_array_with_tag(n: usize, tag: u64) -> BooleanArray {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random_bool(0.5));
    BooleanArray::from_iter(values.map(Some))
}

#[inline]
fn make_i32_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<Int32Type> {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<i32>());
    PrimitiveArray::<Int32Type>::from_iter_values(values)
}

#[inline]
fn make_i64_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<Int64Type> {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<i64>());
    PrimitiveArray::<Int64Type>::from_iter_values(values)
}

#[inline]
fn make_f32_array_with_tag(n: usize, tag: u64) -> Float32Array {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<f32>());
    Float32Array::from_iter_values(values)
}

#[inline]
fn make_f64_array_with_tag(n: usize, tag: u64) -> Float64Array {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<f64>());
    Float64Array::from_iter_values(values)
}

#[inline]
fn make_binary_array_with_tag(n: usize, tag: u64) -> BinaryArray {
    let mut rng = rng_for(tag, n);
    let mut payloads: Vec<[u8; 16]> = vec![[0; 16]; n];
    for p in payloads.iter_mut() {
        rng.fill(&mut p[..]);
    }
    let views: Vec<&[u8]> = payloads.iter().map(|p| &p[..]).collect();
    BinaryArray::from_vec(views)
}

#[inline]
fn make_ts_micros_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<TimestampMicrosecondType> {
    let mut rng = rng_for(tag, n);
    let base: i64 = 1_600_000_000_000_000;
    let year_us: i64 = 31_536_000_000_000;
    let values = (0..n).map(|_| base + sample_in::<i64, _>(&mut rng, 0..year_us));
    PrimitiveArray::<TimestampMicrosecondType>::from_iter_values(values)
}

#[inline]
fn make_bool_array(n: usize) -> BooleanArray {
    make_bool_array_with_tag(n, 0xB001)
}
#[inline]
fn make_i32_array(n: usize) -> PrimitiveArray<Int32Type> {
    make_i32_array_with_tag(n, 0x1337_0032)
}
#[inline]
fn make_i64_array(n: usize) -> PrimitiveArray<Int64Type> {
    make_i64_array_with_tag(n, 0x1337_0064)
}
#[inline]
fn make_f32_array(n: usize) -> Float32Array {
    make_f32_array_with_tag(n, 0xF0_0032)
}
#[inline]
fn make_f64_array(n: usize) -> Float64Array {
    make_f64_array_with_tag(n, 0xF0_0064)
}
#[inline]
fn make_binary_array(n: usize) -> BinaryArray {
    make_binary_array_with_tag(n, 0xB1_0001)
}
#[inline]
fn make_ts_micros_array(n: usize) -> PrimitiveArray<TimestampMicrosecondType> {
    make_ts_micros_array_with_tag(n, 0x7157_0001)
}

#[inline]
fn schema_single(name: &str, dt: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, dt, false)]))
}

#[inline]
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
            let f1: ArrayRef = Arc::new(make_i32_array_with_tag(n, 0xA1));
            let f2: ArrayRef = Arc::new(make_i64_array_with_tag(n, 0xA2));
            let f3: ArrayRef = Arc::new(make_binary_array_with_tag(n, 0xA3));
            let f4: ArrayRef = Arc::new(make_f64_array_with_tag(n, 0xA4));
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
            4_096 | 8_192 => {
                group
                    .sample_size(40)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            100_000 => {
                group
                    .sample_size(20)
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