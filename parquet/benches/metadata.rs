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

use std::hint::black_box;
use std::sync::Arc;

use parquet::basic::{Encoding, PageType, Type as PhysicalType};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, PageEncodingStats, ParquetMetaData, ParquetMetaDataOptions,
    ParquetMetaDataReader, ParquetMetaDataWriter, ParquetStatisticsPolicy, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use parquet::file::writer::TrackedWrite;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescriptor, Type as SchemaType,
};
use rand::Rng;

use arrow::util::test_util::seedable_rng;
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;

const NUM_COLUMNS: usize = 10_000;
const NUM_ROW_GROUPS: usize = 10;

fn encoded_meta() -> Vec<u8> {
    let mut rng = seedable_rng();

    let mut column_desc_ptrs: Vec<ColumnDescPtr> = Vec::with_capacity(NUM_COLUMNS);
    let mut message_type = "message test_schema {".to_string();
    for i in 0..NUM_COLUMNS {
        message_type.push_str(&format!("REQUIRED FLOAT {};", i));
        column_desc_ptrs.push(ColumnDescPtr::new(ColumnDescriptor::new(
            Arc::new(
                SchemaType::primitive_type_builder(&i.to_string(), PhysicalType::FLOAT)
                    .build()
                    .unwrap(),
            ),
            0,
            0,
            ColumnPath::new(vec![]),
        )));
    }
    message_type.push('}');

    let schema_descr = parse_message_type(&message_type)
        .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
        .unwrap();

    let stats = Statistics::float(Some(rng.random()), Some(rng.random()), None, Some(0), false);

    let row_groups = (0..NUM_ROW_GROUPS)
        .map(|i| {
            let columns = (0..NUM_COLUMNS)
                .map(|j| {
                    ColumnChunkMetaData::builder(column_desc_ptrs[j].clone())
                        .set_encodings(vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY])
                        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
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
                        .set_offset_index_offset(Some(rng.random_range(0..2000000000)))
                        .set_offset_index_length(Some(rng.random_range(1..100000)))
                        .set_column_index_offset(Some(rng.random_range(0..2000000000)))
                        .set_column_index_length(Some(rng.random_range(1..100000)))
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
        1,
        rng.random_range(1..2000000000),
        Some("parquet-rs".into()),
        None,
        schema_descr,
        None,
    );

    let metadata = ParquetMetaData::new(file_metadata, row_groups);
    let mut buffer = Vec::with_capacity(1024);
    {
        let buf = TrackedWrite::new(&mut buffer);
        let writer = ParquetMetaDataWriter::new_with_tracked(buf, &metadata);
        writer.finish().unwrap();
    }

    buffer
}

fn get_footer_bytes(data: Bytes) -> Bytes {
    let footer_bytes = data.slice(data.len() - 8..);
    let footer_len = footer_bytes[0] as u32
        | (footer_bytes[1] as u32) << 8
        | (footer_bytes[2] as u32) << 16
        | (footer_bytes[3] as u32) << 24;
    let meta_start = data.len() - footer_len as usize - 8;
    let meta_end = data.len() - 8;
    data.slice(meta_start..meta_end)
}

fn criterion_benchmark(c: &mut Criterion) {
    // Read file into memory to isolate filesystem performance
    let file = "../parquet-testing/data/alltypes_tiny_pages.parquet";
    let data = std::fs::read(file).unwrap();
    let data = Bytes::from(data);

    c.bench_function("open(default)", |b| {
        b.iter(|| SerializedFileReader::new(data.clone()).unwrap())
    });

    c.bench_function("open(page index)", |b| {
        b.iter(|| {
            let options = ReadOptionsBuilder::new().with_page_index().build();
            SerializedFileReader::new_with_options(data.clone(), options).unwrap()
        })
    });

    let meta_data = get_footer_bytes(data.clone());
    c.bench_function("decode parquet metadata", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata(&meta_data).unwrap();
        })
    });

    let schema = ParquetMetaDataReader::decode_schema(&meta_data).unwrap();
    let options = ParquetMetaDataOptions::new().with_schema(schema);
    c.bench_function("decode metadata with schema", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&meta_data, Some(&options))
                .unwrap();
        })
    });

    let options = ParquetMetaDataOptions::new().with_encoding_stats_as_mask(true);
    c.bench_function("decode metadata with stats mask", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&meta_data, Some(&options))
                .unwrap();
        })
    });

    let options =
        ParquetMetaDataOptions::new().with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll);
    c.bench_function("decode metadata with skip PES", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&meta_data, Some(&options))
                .unwrap();
        })
    });

    let buf: Bytes = black_box(encoded_meta()).into();
    c.bench_function("decode parquet metadata (wide)", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata(&buf).unwrap();
        })
    });

    let schema = ParquetMetaDataReader::decode_schema(&buf).unwrap();
    let options = ParquetMetaDataOptions::new().with_schema(schema);
    c.bench_function("decode metadata (wide) with schema", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&buf, Some(&options)).unwrap();
        })
    });

    let options = ParquetMetaDataOptions::new().with_encoding_stats_as_mask(true);
    c.bench_function("decode metadata (wide) with stats mask", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&buf, Some(&options)).unwrap();
        })
    });

    let options =
        ParquetMetaDataOptions::new().with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll);
    c.bench_function("decode metadata (wide) with skip PES", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata_with_options(&buf, Some(&options)).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
