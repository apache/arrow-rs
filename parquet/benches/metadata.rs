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

use parquet::basic::{Encoding, FieldRepetitionType, PageType, Type};
use parquet::file::metadata::thrift_gen::{
    ColumnChunk, ColumnMetaData, CompressionCodec, FileMetaData, RowGroup, SchemaElement,
    Statistics,
};
use parquet::file::metadata::{PageEncodingStats, ParquetMetaDataReader};
use rand::Rng;

use parquet::parquet_thrift::{ThriftCompactOutputProtocol, WriteThrift};

use arrow::util::test_util::seedable_rng;
use bytes::Bytes;
use criterion::*;
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;

const NUM_COLUMNS: usize = 10_000;
const NUM_ROW_GROUPS: usize = 10;

fn encoded_meta() -> Vec<u8> {
    let mut rng = seedable_rng();

    let mut schema = Vec::with_capacity(NUM_COLUMNS + 1);
    schema.push(SchemaElement {
        type_: None,
        type_length: None,
        repetition_type: None,
        name: Default::default(),
        num_children: Some(NUM_COLUMNS as _),
        converted_type: None,
        scale: None,
        precision: None,
        field_id: None,
        logical_type: None,
    });
    for i in 0..NUM_COLUMNS {
        schema.push(SchemaElement {
            type_: Some(Type::FLOAT),
            type_length: None,
            repetition_type: Some(FieldRepetitionType::REQUIRED),
            name: i.to_string(),
            num_children: None,
            converted_type: None,
            scale: None,
            precision: None,
            field_id: None,
            logical_type: None,
        })
    }

    let max_value_binding = vec![rng.random(); 8];
    let min_value_binding = vec![rng.random(); 8];
    let stats = Statistics {
        min: None,
        max: None,
        null_count: Some(0),
        distinct_count: None,
        max_value: Some(&max_value_binding),
        min_value: Some(&min_value_binding),
        is_max_value_exact: Some(true),
        is_min_value_exact: Some(true),
    };

    let row_groups = (0..NUM_ROW_GROUPS)
        .map(|i| {
            let columns = (0..NUM_COLUMNS)
                .map(|_| ColumnChunk {
                    file_path: None,
                    file_offset: 0,
                    meta_data: Some(ColumnMetaData {
                        type_: Type::FLOAT,
                        encodings: vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY],
                        codec: CompressionCodec::UNCOMPRESSED,
                        num_values: rng.random_range(1..1000000),
                        total_uncompressed_size: rng.random_range(100000..100000000),
                        total_compressed_size: rng.random_range(50000..5000000),
                        data_page_offset: rng.random_range(4..2000000000),
                        index_page_offset: None,
                        dictionary_page_offset: Some(rng.random_range(4..2000000000)),
                        statistics: Some(stats.clone()),
                        encoding_stats: Some(vec![
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
                        ]),
                        bloom_filter_offset: None,
                        bloom_filter_length: None,
                        size_statistics: None,
                        geospatial_statistics: None,
                    }),
                    offset_index_offset: Some(rng.random_range(0..2000000000)),
                    offset_index_length: Some(rng.random_range(1..100000)),
                    column_index_offset: Some(rng.random_range(0..2000000000)),
                    column_index_length: Some(rng.random_range(1..100000)),
                    crypto_metadata: None,
                    encrypted_column_metadata: None,
                })
                .collect();

            RowGroup {
                columns,
                total_byte_size: rng.random_range(1..2000000000),
                num_rows: rng.random_range(1..10000000000),
                sorting_columns: None,
                file_offset: None,
                total_compressed_size: Some(rng.random_range(1..1000000000)),
                ordinal: Some(i as _),
            }
        })
        .collect();

    let file = FileMetaData {
        schema,
        row_groups,
        version: 1,
        num_rows: rng.random_range(1..2000000000),
        key_value_metadata: None,
        created_by: Some("parquet-rs".into()),
        column_orders: None,
        encryption_algorithm: None,
        footer_signing_key_metadata: None,
    };

    let mut buf = Vec::with_capacity(1024);
    {
        let mut out = ThriftCompactOutputProtocol::new(&mut buf);
        file.write_thrift(&mut out).unwrap();
    }
    buf
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

    let buf: Bytes = black_box(encoded_meta()).into();
    c.bench_function("decode parquet metadata (wide)", |b| {
        b.iter(|| {
            ParquetMetaDataReader::decode_metadata(&buf).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
