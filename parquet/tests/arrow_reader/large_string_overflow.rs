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

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;

use tempfile::tempfile;

const ROWS: usize = 500;
const VALUE_SIZE: usize = 5_068_563; // ~5MB per row → triggers >2GB total

fn make_large_binary_array() -> ArrayRef {
    let value = vec![b'a'; VALUE_SIZE];
    let values: Vec<Vec<u8>> = std::iter::repeat(value)
        .take(ROWS)
        .collect();

    Arc::new(BinaryArray::from(values)) as ArrayRef
}

fn write_parquet_with_encoding(
    array: ArrayRef,
    encoding: Encoding,
) -> std::fs::File {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col", DataType::Binary, false),
    ]));

    let batch =
        RecordBatch::try_new(schema.clone(), vec![array])
            .unwrap();

    let file = tempfile().unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_encoding(encoding)
        .build();

    let mut writer =
        ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(props))
            .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    file
}

#[test]
#[ignore = "regression test for >2GB binary offset overflow"]
fn large_binary_plain_encoding_overflow() {
    let array = make_large_binary_array();
    let file = write_parquet_with_encoding(array, Encoding::PLAIN);

    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

    assert!(matches!(reader.next(), Some(Ok(_))));
}

#[test]
#[ignore = "regression test for >2GB binary offset overflow"]
fn large_binary_delta_length_encoding_overflow() {
    let array = make_large_binary_array();
    let file = write_parquet_with_encoding(
        array,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
    );

    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

    assert!(matches!(reader.next(), Some(Ok(_))));
}

#[test]
#[ignore = "regression test for >2GB binary offset overflow"]
fn large_binary_delta_byte_array_encoding_overflow() {
    let array = make_large_binary_array();
    let file =
        write_parquet_with_encoding(array, Encoding::DELTA_BYTE_ARRAY);

    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

    assert!(matches!(reader.next(), Some(Ok(_))));
}

#[test]
#[ignore = "regression test for >2GB binary offset overflow"]
fn large_binary_rle_dictionary_encoding_overflow() {
    let array = make_large_binary_array();
    let file =
        write_parquet_with_encoding(array, Encoding::RLE_DICTIONARY);

    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

    assert!(matches!(reader.next(), Some(Ok(_))));
}
