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

//! Regression tests for <https://github.com/apache/arrow-rs/issues/7973>
//!
//! A row group whose byte array column holds more than `i32::MAX` bytes cannot
//! be decoded into a single `BinaryArray`, because the 32 bit offsets overflow.
//! The reader must emit shorter batches instead of failing.
//!
//! Note the input arrays are written in chunks that each stay under the 2GB
//! limit, so that the writer side never has to build an oversized
//! `BinaryArray`. The overflow being tested is on the read side.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;

use tempfile::tempfile;

/// Number of rows written, all in a single row group
const ROWS: usize = 700;

/// Size of each binary value (3MB)
///
/// 700 * 3MB = 2.1GB total, so the i32 offsets overflow at row 683
const VALUE_SIZE: usize = 3 * 1024 * 1024;

/// Rows written per `RecordBatch`, small enough that no single input array
/// exceeds the 2GB limit
const WRITE_CHUNK: usize = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Binary,
        false,
    )]))
}

/// The `i`th value, distinct in its first bytes so that mixed up rows are caught
fn value(i: usize) -> Vec<u8> {
    let mut data = vec![b'a'; VALUE_SIZE];
    data[..8].copy_from_slice(&(i as u64).to_le_bytes());
    data
}

fn write_parquet_with_encoding(encoding: Encoding) -> std::fs::File {
    let schema = schema();
    let file = tempfile().unwrap();

    let builder = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(ROWS))
        .set_max_row_group_bytes(None);
    let builder = match encoding {
        Encoding::RLE_DICTIONARY => builder.set_dictionary_enabled(true),
        _ => builder.set_dictionary_enabled(false).set_encoding(encoding),
    };

    let mut writer = ArrowWriter::try_new(
        file.try_clone().unwrap(),
        schema.clone(),
        Some(builder.build()),
    )
    .unwrap();

    for chunk in (0..ROWS).step_by(WRITE_CHUNK) {
        let values: Vec<Vec<u8>> = (chunk..(chunk + WRITE_CHUNK).min(ROWS))
            .map(value)
            .collect();
        let array: ArrayRef = Arc::new(BinaryArray::from_iter_values(&values));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    file
}

/// Read the whole file back and check every value survives, in order, split
/// across however many batches the reader needs
fn assert_reads_all_rows(encoding: Encoding) {
    let file = write_parquet_with_encoding(encoding);

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(ROWS)
        .build()
        .unwrap();

    let mut batches = 0;
    let mut row = 0;
    for batch in reader {
        let batch = batch.unwrap();
        batches += 1;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for i in 0..column.len() {
            let v = column.value(i);
            assert_eq!(v.len(), VALUE_SIZE, "row {row} has the wrong length");
            assert_eq!(
                u64::from_le_bytes(v[..8].try_into().unwrap()),
                row as u64,
                "row {row} is out of order"
            );
            row += 1;
        }
    }

    assert_eq!(row, ROWS, "{encoding}: lost rows while splitting batches");
    assert!(
        batches > 1,
        "{encoding}: expected the reader to split the row group into more than one batch"
    );
}

/// All four byte array encodings, in one test so the encodings run one after
/// another. Each one holds a little over 2GB while it decodes, and running them
/// as four separate tests lets the harness run them in parallel, which needs
/// four times the memory.
#[test]
fn large_binary_offset_overflow() {
    assert_reads_all_rows(Encoding::PLAIN);
    assert_reads_all_rows(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    assert_reads_all_rows(Encoding::DELTA_BYTE_ARRAY);
    assert_reads_all_rows(Encoding::RLE_DICTIONARY);
}
