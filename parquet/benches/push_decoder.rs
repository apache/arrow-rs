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
//!
//! The `scan_plan` group measures `ParquetPushDecoder::scan_plan` for wide
//! schemas with a page index.

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{Float32Array, RecordBatch};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::DecodeResult;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataPushDecoder};
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

/// Write a Parquet file with `num_columns` columns and one row group of
/// 1,000 rows, with `pages_per_column` data pages per column chunk.
fn make_paged_test_file(num_columns: usize, pages_per_column: usize) -> Bytes {
    let num_rows = 1_000;
    let schema = make_wide_schema(num_columns);
    let columns: Vec<Arc<dyn arrow_array::Array>> = (0..num_columns)
        .map(|_| {
            Arc::new(Float32Array::from_iter_values(
                (0..num_rows).map(|v| v as f32),
            )) as _
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let page_rows = num_rows / pages_per_column;
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(num_rows))
        .set_data_page_row_count_limit(page_rows)
        // Page limits are checked between write batches.
        .set_write_batch_size(page_rows)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

type BuilderFn<'a> = Box<dyn Fn() -> ParquetPushDecoderBuilder + 'a>;

/// Plan a wide file with a page index: the first range (what a read-ahead
/// caller waits for) and the whole scan, which is one row group. For
/// comparison, `first_reader` builds a decoder, pushes the whole file and
/// builds the first row group reader, which includes the decoder's own
/// per-row-group setup.
///
/// `all` reads every column and row. `selection` keeps 10 rows of every 100,
/// so each column chunk reads every other page. `narrow` reads 10 columns.
fn bench_scan_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_decoder/scan_plan");

    for (num_cols, pages) in [(100, 10), (1_000, 10), (10_000, 10), (1_000, 100)] {
        let file_data = make_paged_test_file(num_cols, pages);
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = ArrowReaderMetadata::load(&file_data, options).unwrap();
        let selection = RowSelection::from(
            (0..10)
                .flat_map(|_| [RowSelector::select(10), RowSelector::skip(90)])
                .collect::<Vec<_>>(),
        );
        let narrow = ProjectionMask::leaves(metadata.parquet_schema(), 0..10);
        let variants: [(&str, BuilderFn); 3] = [
            (
                "all",
                Box::new(|| ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())),
            ),
            (
                "selection",
                Box::new(|| {
                    ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())
                        .with_row_selection(selection.clone())
                }),
            ),
            (
                "narrow",
                Box::new(|| {
                    ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())
                        .with_projection(narrow.clone())
                }),
            ),
        ];

        for (variant, builder) in &variants {
            let decoder = builder().build().unwrap();
            let id = format!("{variant}/{num_cols}cols_{pages}pages");

            group.bench_function(BenchmarkId::new("first_range", &id), |b| {
                b.iter(|| black_box(decoder.scan_plan().next()))
            });
            group.bench_function(BenchmarkId::new("whole_scan", &id), |b| {
                b.iter(|| black_box(decoder.scan_plan().count()))
            });
            group.bench_function(BenchmarkId::new("first_reader", &id), |b| {
                b.iter(|| {
                    let mut decoder = builder().build().unwrap();
                    decoder
                        .push_range(0..file_data.len() as u64, file_data.clone())
                        .unwrap();
                    match decoder.try_next_reader().unwrap() {
                        DecodeResult::Data(reader) => black_box(reader),
                        other => panic!("expected a reader, got {other:?}"),
                    };
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_1buf, bench_nbuf, bench_scan_plan);
criterion_main!(benches);
