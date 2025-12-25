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

//! Benchmarks comparing FlatBuffers vs Thrift metadata serialization.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;

use arrow::util::test_util::seedable_rng;
use parquet::basic::{Compression, Encoding, PageType, Type as PhysicalType};
use parquet::file::metadata::flatbuf::{flatbuf_to_parquet_metadata, parquet_metadata_to_flatbuf};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, PageEncodingStats, ParquetMetaData, ParquetMetaDataReader,
    ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use parquet::file::writer::TrackedWrite;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescriptor};

/// Create test metadata with configurable number of columns and row groups
fn create_test_metadata(num_columns: usize, num_row_groups: usize) -> ParquetMetaData {
    let mut rng = seedable_rng();

    let mut column_desc_ptrs: Vec<ColumnDescPtr> = Vec::with_capacity(num_columns);
    let mut message_type = "message test_schema {".to_string();
    for i in 0..num_columns {
        message_type.push_str(&format!("REQUIRED FLOAT col_{};", i));
        column_desc_ptrs.push(ColumnDescPtr::new(ColumnDescriptor::new(
            Arc::new(
                parquet::schema::types::Type::primitive_type_builder(
                    &format!("col_{}", i),
                    PhysicalType::FLOAT,
                )
                .build()
                .unwrap(),
            ),
            0,
            0,
            ColumnPath::new(vec![format!("col_{}", i)]),
        )));
    }
    message_type.push('}');

    let schema_descr = parse_message_type(&message_type)
        .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
        .unwrap();

    let stats = Statistics::float(Some(rng.random()), Some(rng.random()), None, Some(0), false);

    let row_groups = (0..num_row_groups)
        .map(|i| {
            let columns = (0..num_columns)
                .map(|j| {
                    ColumnChunkMetaData::builder(column_desc_ptrs[j].clone())
                        .set_encodings(vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY])
                        .set_compression(Compression::SNAPPY)
                        .set_num_values(rng.random_range(1..1000000))
                        .set_total_compressed_size(rng.random_range(50000..5000000))
                        .set_data_page_offset(rng.random_range(4..2000000000))
                        .set_dictionary_page_offset(Some(rng.random_range(4..2000000000)))
                        .set_statistics(stats.clone())
                        .set_page_encoding_stats(vec![
                            PageEncodingStats {
                                page_type: PageType::DICTIONARY_PAGE,
                                encoding: Encoding::PLAIN,
                                count: 1,
                            },
                            PageEncodingStats {
                                page_type: PageType::DATA_PAGE,
                                encoding: Encoding::RLE_DICTIONARY,
                                count: 10,
                            },
                        ])
                        .build()
                        .unwrap()
                })
                .collect();

            RowGroupMetaData::builder(schema_descr.clone())
                .set_column_metadata(columns)
                .set_total_byte_size(rng.random_range(1..2000000000))
                .set_num_rows(rng.random_range(1..10000000000))
                .set_ordinal(i as i16)
                .build()
                .unwrap()
        })
        .collect();

    let file_metadata = FileMetaData::new(
        2,
        rng.random_range(1..2000000000),
        Some("parquet-rs benchmark".into()),
        None,
        schema_descr,
        None,
    );

    ParquetMetaData::new(file_metadata, row_groups)
}

/// Encode metadata using Thrift (current format)
fn encode_thrift(metadata: &ParquetMetaData) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(64 * 1024);
    {
        let buf = TrackedWrite::new(&mut buffer);
        let writer = ParquetMetaDataWriter::new_with_tracked(buf, metadata);
        writer.finish().unwrap();
    }
    buffer
}

/// Encode metadata using FlatBuffers
fn encode_flatbuf(metadata: &ParquetMetaData) -> Vec<u8> {
    parquet_metadata_to_flatbuf(metadata)
}

fn benchmark_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_serialization");

    // Test different sizes: small (10 cols, 1 rg), medium (100 cols, 10 rgs), large (1000 cols, 10 rgs)
    let configs = [
        ("small", 10, 1),
        ("medium", 100, 10),
        ("large", 1000, 10),
        ("wide", 10000, 1),
    ];

    for (name, num_cols, num_rgs) in configs {
        let metadata = create_test_metadata(num_cols, num_rgs);

        // Thrift encoding benchmark
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("thrift_encode", name),
            &metadata,
            |b, meta| {
                b.iter(|| {
                    black_box(encode_thrift(meta));
                });
            },
        );

        // FlatBuffers encoding benchmark
        group.bench_with_input(
            BenchmarkId::new("flatbuf_encode", name),
            &metadata,
            |b, meta| {
                b.iter(|| {
                    black_box(encode_flatbuf(meta));
                });
            },
        );

        // Pre-encode for decoding benchmarks
        let thrift_bytes = encode_thrift(&metadata);
        let flatbuf_bytes = encode_flatbuf(&metadata);

        // Report sizes
        println!(
            "{}: Thrift size = {} bytes, FlatBuf size = {} bytes, ratio = {:.2}x",
            name,
            thrift_bytes.len(),
            flatbuf_bytes.len(),
            thrift_bytes.len() as f64 / flatbuf_bytes.len() as f64
        );

        // Thrift decoding benchmark
        group.bench_with_input(
            BenchmarkId::new("thrift_decode", name),
            &thrift_bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(ParquetMetaDataReader::decode_metadata(bytes).unwrap());
                });
            },
        );

        // FlatBuffers decoding benchmark
        let schema_descr = metadata.file_metadata().schema_descr_ptr();
        group.bench_with_input(
            BenchmarkId::new("flatbuf_decode", name),
            &(&flatbuf_bytes, schema_descr.clone()),
            |b, (bytes, schema)| {
                b.iter(|| {
                    black_box(flatbuf_to_parquet_metadata(bytes, schema.clone()).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn benchmark_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_roundtrip");

    let configs = [("medium", 100, 10), ("large", 1000, 10)];

    for (name, num_cols, num_rgs) in configs {
        let metadata = create_test_metadata(num_cols, num_rgs);
        let schema_descr = metadata.file_metadata().schema_descr_ptr();

        // Thrift roundtrip
        group.bench_with_input(
            BenchmarkId::new("thrift_roundtrip", name),
            &metadata,
            |b, meta| {
                b.iter(|| {
                    let encoded = encode_thrift(meta);
                    black_box(ParquetMetaDataReader::decode_metadata(&encoded).unwrap());
                });
            },
        );

        // FlatBuffers roundtrip
        group.bench_with_input(
            BenchmarkId::new("flatbuf_roundtrip", name),
            &(&metadata, schema_descr.clone()),
            |b, (meta, schema)| {
                b.iter(|| {
                    let encoded = encode_flatbuf(meta);
                    black_box(flatbuf_to_parquet_metadata(&encoded, schema.clone()).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn benchmark_size_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_size");

    // Benchmark with increasing column counts to show scaling
    let column_counts = [10, 50, 100, 500, 1000, 5000];

    for num_cols in column_counts {
        let metadata = create_test_metadata(num_cols, 5);

        let thrift_bytes = encode_thrift(&metadata);
        let flatbuf_bytes = encode_flatbuf(&metadata);

        println!(
            "Columns: {}, Thrift: {} bytes, FlatBuf: {} bytes, Ratio: {:.2}x",
            num_cols,
            thrift_bytes.len(),
            flatbuf_bytes.len(),
            thrift_bytes.len() as f64 / flatbuf_bytes.len() as f64
        );

        // Just measure encoding time as a function of column count
        group.throughput(Throughput::Elements(num_cols as u64));
        group.bench_with_input(
            BenchmarkId::new("thrift", num_cols),
            &metadata,
            |b, meta| {
                b.iter(|| black_box(encode_thrift(meta)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("flatbuf", num_cols),
            &metadata,
            |b, meta| {
                b.iter(|| black_box(encode_flatbuf(meta)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_serialization,
    benchmark_roundtrip,
    benchmark_size_comparison,
);
criterion_main!(benches);
