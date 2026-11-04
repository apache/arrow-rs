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

//! Skipping records in a dictionary-encoded column chunk.
//!
//! The chunk is shaped like a real serving file: a zstd-compressed BYTE_ARRAY
//! column whose dictionary holds 40k distinct strings. Skipping the whole
//! chunk never needs the dictionary; skipping part of it and then reading
//! does.

use std::hint;
use std::sync::Arc;

use bytes::Bytes;
use criterion::*;
use parquet::basic::Compression;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

const NUM_ROWS: usize = 80_000;

fn write_chunk() -> Bytes {
    let schema =
        Arc::new(parse_message_type("message test_schema { REQUIRED BYTE_ARRAY v; }").unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build(),
    );
    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();
    let mut column = row_group.next_column().unwrap().unwrap();
    let values: Vec<ByteArray> = (0..NUM_ROWS as u32)
        .map(|i| ByteArray::from(format!("ticker-{:016}", i / 2).into_bytes()))
        .collect();
    column
        .typed::<ByteArrayType>()
        .write_batch(&values, None, None)
        .unwrap();
    column.close().unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    Bytes::from(out)
}

fn dictionary_skip_benchmark(c: &mut Criterion) {
    let bytes = write_chunk();
    let reader = SerializedFileReader::new(bytes.clone()).unwrap();
    let metadata = reader.metadata();
    let descr = metadata.file_metadata().schema_descr().column(0);
    let column_metadata = metadata.row_group(0).column(0).clone();

    let new_reader = || {
        let pages =
            SerializedPageReader::new(Arc::new(bytes.clone()), &column_metadata, NUM_ROWS, None)
                .unwrap();
        ColumnReaderImpl::<ByteArrayType>::new(descr.clone(), Box::new(pages))
    };

    let mut group = c.benchmark_group("dictionary_skip");
    group.bench_function("skip whole chunk", |b| {
        b.iter(|| {
            let mut reader = new_reader();
            hint::black_box(reader.skip_records(NUM_ROWS).unwrap())
        })
    });
    group.bench_function("skip one record then read", |b| {
        b.iter(|| {
            let mut reader = new_reader();
            reader.skip_records(1).unwrap();
            let mut values = Vec::new();
            hint::black_box(reader.read_records(1, None, None, &mut values).unwrap())
        })
    });
    group.finish();
}

criterion_group!(benches, dictionary_skip_benchmark);
criterion_main!(benches);
