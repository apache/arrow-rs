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

//! Benchmarks for the push-based decoder measuring PushBuffers overhead.
//!
//! Uses `try_next_reader` to build row group readers without decoding any
//! pages, isolating PushBuffers operations (has_range, get_bytes, clearing).

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{Float32Array, RecordBatch};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::DecodeResult;
use parquet::arrow::ArrowWriter;
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::metadata::ParquetMetaDataPushDecoder;
use parquet::file::properties::WriterProperties;

fn make_wide_schema(num_columns: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..num_columns)
        .map(|i| Field::new(format!("c{i}"), DataType::Float32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Write a Parquet file with `num_columns` columns, 10 row groups of 100 rows.
fn make_test_file(num_columns: usize) -> Bytes {
    let num_rows = 1_000;
    let rows_per_rg = 100;
    let schema = make_wide_schema(num_columns);
    let columns: Vec<Arc<dyn arrow_array::Array>> = (0..num_columns)
        .map(|_| Arc::new(Float32Array::from(vec![0.0f32; num_rows])) as _)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_rg))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

fn decode_metadata(file_data: &Bytes) -> Arc<parquet::file::metadata::ParquetMetaData> {
    let file_len = file_data.len() as u64;
    let mut dec = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
    dec.push_range(0..file_len, file_data.clone()).unwrap();
    match dec.try_decode().unwrap() {
        DecodeResult::Data(m) => Arc::new(m),
        other => panic!("expected metadata, got {other:?}"),
    }
}

/// Push the entire file as one buffer, then build all row group readers.
fn build_readers_single_buffer(
    file_data: &Bytes,
    metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
) {
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(metadata.clone())
        .unwrap()
        .build()
        .unwrap();

    decoder
        .push_range(0..file_data.len() as u64, file_data.clone())
        .unwrap();

    loop {
        match decoder.try_next_reader().unwrap() {
            DecodeResult::Data(reader) => {
                black_box(reader);
            }
            DecodeResult::Finished => break,
            DecodeResult::NeedsData(r) => panic!("unexpected NeedsData: {r:?}"),
        }
    }
}

/// Push one buffer per requested range, then build all row group readers.
fn build_readers_exact_ranges(
    file_data: &Bytes,
    metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
) {
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(metadata.clone())
        .unwrap()
        .build()
        .unwrap();

    loop {
        match decoder.try_next_reader().unwrap() {
            DecodeResult::Data(reader) => {
                black_box(reader);
            }
            DecodeResult::Finished => break,
            DecodeResult::NeedsData(ranges) => {
                let buffers: Vec<Bytes> = ranges
                    .iter()
                    .map(|r| file_data.slice(r.start as usize..r.end as usize))
                    .collect();
                decoder.push_ranges(ranges, buffers).unwrap();
            }
        }
    }
}

fn bench_1buf(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_decoder/1buf");

    for num_cols in [100, 1_000, 10_000, 50_000] {
        let file_data = make_test_file(num_cols);
        let metadata = decode_metadata(&file_data);
        let num_ranges: usize = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.columns().len())
            .sum();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_ranges}ranges")),
            &(&file_data, &metadata),
            |b, &(data, meta)| b.iter(|| build_readers_single_buffer(data, meta)),
        );
    }

    group.finish();
}

fn bench_nbuf(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_decoder/Nbuf");

    for num_cols in [100, 1_000, 10_000] {
        let file_data = make_test_file(num_cols);
        let metadata = decode_metadata(&file_data);
        let num_ranges: usize = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.columns().len())
            .sum();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_ranges}ranges")),
            &(&file_data, &metadata),
            |b, &(data, meta)| b.iter(|| build_readers_exact_ranges(data, meta)),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_1buf, bench_nbuf);
criterion_main!(benches);
