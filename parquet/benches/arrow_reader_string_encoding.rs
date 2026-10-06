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

//! Benchmark comparing reading the SAME dictionary-encoded parquet string column
//! back as three different Arrow logical types:
//!   1. `Dictionary(Int32, Utf8)`
//!   2. `Utf8`
//!   3. `Utf8View`
//!
//! Sweeps cardinality in {10, 50, 100, 500, 1000, 8192} and row count
//! in {1M, 2M, 5M}, writing a dictionary-encoded parquet file in-memory
//! and measuring only the read time via `ArrowReaderOptions::with_schema`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::{RngExt, SeedableRng, rngs::StdRng};

const STRING_LEN: usize = 16;
const SEED: u64 = 0xC0FFEE_u64;
const CARDINALITIES: &[usize] = &[10, 50, 100, 500, 1000, 8192, 16384];
const ROW_COUNTS: &[usize] = &[1_000_000, 2_000_000, 5_000_000];

fn make_dictionary(cardinality: usize) -> Vec<String> {
    (0..cardinality)
        .map(|idx| format!("value_{:010}", idx))
        .collect()
}

fn make_string_array(cardinality: usize, num_rows: usize) -> StringArray {
    let dictionary = make_dictionary(cardinality);
    debug_assert_eq!(dictionary[0].len(), STRING_LEN);
    let mut rng = StdRng::seed_from_u64(SEED ^ cardinality as u64 ^ num_rows as u64);
    let values: Vec<&str> = (0..num_rows)
        .map(|_| {
            let pick = rng.random_range(0..cardinality);
            dictionary[pick].as_str()
        })
        .collect();
    StringArray::from(values)
}

fn write_parquet(column: StringArray) -> Bytes {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column) as ArrayRef]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    let mut buffer: Vec<u8> = Vec::with_capacity(64 * 1024 * 1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
}

fn override_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", data_type, false)]))
}

fn bench_batch_size() -> usize {
    std::env::var("BENCH_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8192)
}

fn read_as(file_bytes: &Bytes, schema: SchemaRef) -> usize {
    let options = ArrowReaderOptions::new().with_schema(schema);
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file_bytes.clone(), options)
        .unwrap()
        .with_batch_size(bench_batch_size())
        .build()
        .unwrap();
    let mut total_rows = 0;
    for maybe_batch in reader {
        let batch = maybe_batch.unwrap();
        total_rows += batch.num_rows();
    }
    total_rows
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let dict_schema = override_schema(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    ));
    let utf8_schema = override_schema(DataType::Utf8);
    let utf8_view_schema = override_schema(DataType::Utf8View);

    for &num_rows in ROW_COUNTS {
        for &cardinality in CARDINALITIES {
            let column = make_string_array(cardinality, num_rows);
            let file_bytes = write_parquet(column);

            let mut group = criterion.benchmark_group("string_encoding_read");
            group.sample_size(10);
            group.throughput(criterion::Throughput::Elements(num_rows as u64));

            let param = format!("rows={}/card={}", num_rows, cardinality);

            group.bench_with_input(
                BenchmarkId::new("Dictionary(Int32,Utf8)", &param),
                &param,
                |bencher, _| {
                    bencher.iter(|| {
                        let rows = read_as(&file_bytes, dict_schema.clone());
                        assert_eq!(rows, num_rows);
                    });
                },
            );
            group.bench_with_input(BenchmarkId::new("Utf8", &param), &param, |bencher, _| {
                bencher.iter(|| {
                    let rows = read_as(&file_bytes, utf8_schema.clone());
                    assert_eq!(rows, num_rows);
                });
            });
            group.bench_with_input(
                BenchmarkId::new("Utf8View", &param),
                &param,
                |bencher, _| {
                    bencher.iter(|| {
                        let rows = read_as(&file_bytes, utf8_view_schema.clone());
                        assert_eq!(rows, num_rows);
                    });
                },
            );

            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
