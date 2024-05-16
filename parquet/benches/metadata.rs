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

use bytes::Bytes;
use criterion::*;
use rand::Rng;
use thrift::protocol::TCompactOutputProtocol;

use arrow::util::test_util::seedable_rng;
use arrow_ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::format::{
    ColumnChunk, FieldRepetitionType, FileMetaData, RowGroup, SchemaElement, Type,
};
use parquet::thrift::{TCompactSliceInputProtocol, TSerializable};

const NUM_COLUMNS: usize = 10_000;
const NUM_ROW_GROUPS: usize = 1;

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
            name: i.to_string().into(),
            num_children: None,
            converted_type: None,
            scale: None,
            precision: None,
            field_id: None,
            logical_type: None,
        })
    }

    let mut row_groups = (0..NUM_ROW_GROUPS)
        .map(|i| {
            let columns = (0..NUM_COLUMNS)
                .map(|_| ColumnChunk {
                    file_path: None,
                    file_offset: 0,
                    meta_data: None,
                    offset_index_offset: Some(rng.gen()),
                    offset_index_length: Some(rng.gen()),
                    column_index_offset: Some(rng.gen()),
                    column_index_length: Some(rng.gen()),
                    crypto_metadata: None,
                    encrypted_column_metadata: None,
                })
                .collect();

            RowGroup {
                columns,
                total_byte_size: rng.gen(),
                num_rows: rng.gen(),
                sorting_columns: None,
                file_offset: None,
                total_compressed_size: Some(rng.gen()),
                ordinal: Some(i as _),
            }
        })
        .collect();

    let mut file = FileMetaData {
        schema,
        row_groups,
        version: 1,
        num_rows: rng.gen(),
        key_value_metadata: None,
        created_by: Some("parquet-rs".into()),
        column_orders: None,
        encryption_algorithm: None,
        footer_signing_key_metadata: None,
    };

    let mut buf = Vec::with_capacity(1024);
    {
        let mut out = TCompactOutputProtocol::new(&mut buf);
        file.write_to_out_protocol(&mut out).unwrap();
    }
    buf
}

fn encoded_ipc_schema() -> Vec<u8> {
    let schema = Schema::new(Fields::from_iter(
        (0..NUM_COLUMNS).map(|i| Field::new(i.to_string(), DataType::Float64, true)),
    ));
    let data = IpcDataGenerator::default();
    let r = data.schema_to_bytes(&schema, &IpcWriteOptions::default());
    assert_eq!(r.arrow_data.len(), 0);
    r.ipc_message
}

fn criterion_benchmark(c: &mut Criterion) {
    let buf = black_box(encoded_meta());
    println!("Parquet metadata {}", buf.len());

    c.bench_function("decode metadata", |b| {
        b.iter(|| {
            let mut input = TCompactSliceInputProtocol::new(&buf);
            FileMetaData::read_from_in_protocol(&mut input).unwrap()
        })
    });

    let buf = black_box(encoded_ipc_schema());
    println!("Arrow IPC metadata {}", buf.len());

    c.bench_function("decode ipc metadata", |b| {
        b.iter(|| arrow_ipc::root_as_message(&buf).unwrap())
    });

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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
