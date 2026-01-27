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

//! Benchmarks for the `arrow-avro` Encoder

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_avro::writer::format::AvroSoeFormat;
use arrow_avro::writer::{EncodedRows, WriterBuilder};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use once_cell::sync::Lazy;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const SIZES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

/// Pre-generate EncodedRows for each size to avoid setup overhead in benchmarks.
static ENCODED_DATA: Lazy<Vec<EncodedRows>> =
    Lazy::new(|| SIZES.iter().map(|&n| make_encoded_rows(n)).collect());

/// Create an EncodedRows with `n` rows of Int32 data.
fn make_encoded_rows(n: usize) -> EncodedRows {
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    let values: Vec<i32> = (0..n as i32).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(values)) as ArrayRef],
    )
    .unwrap();
    let mut encoder = WriterBuilder::new(schema)
        .build_encoder::<AvroSoeFormat>()
        .unwrap();
    encoder.encode(&batch).unwrap();
    encoder.flush()
}

fn bench_row_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_access");
    for (idx, &size) in SIZES.iter().enumerate() {
        let encoded = &ENCODED_DATA[idx];
        let num_rows = encoded.len();
        // Configure sampling based on data size
        match size {
            100_000 | 1_000_000 => {
                group
                    .sample_size(20)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            _ => {
                group.sample_size(100);
            }
        }
        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_function(BenchmarkId::from_parameter(size), |b| {
            b.iter(|| {
                for i in 0..num_rows {
                    black_box(encoded.row(i).unwrap());
                }
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = encoder;
    config = Criterion::default().configure_from_args();
    targets = bench_row_access
}

criterion_main!(encoder);
