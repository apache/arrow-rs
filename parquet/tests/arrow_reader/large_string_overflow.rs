// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reader regressions for a logical byte-array window above i32::MAX.
//! Build legal input batches, stream each output, and never concatenate them.
//! Run these memory-intensive tests explicitly with one test thread.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tempfile::tempfile;

const ROWS: usize = 700;
const WRITE_ROWS: usize = 10;
const VALUE_SIZE: usize = 3 * 1024 * 1024;

fn roundtrip_large_binary(encoding: Encoding) {
    assert!(ROWS * VALUE_SIZE > i32::MAX as usize);
    let schema = Arc::new(Schema::new(vec![
        Field::new("col", DataType::Binary, false),
        Field::new("row_id", DataType::Int64, false),
    ]));
    let value = vec![b'a'; VALUE_SIZE];
    // Reuse a legal 30 MiB array: an oversized input builder would panic
    // before reaching the Parquet reader and would not test this regression.
    let values: ArrayRef = Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
        value.as_slice(),
        WRITE_ROWS,
    )));
    let column = ColumnPath::from("col");
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_dictionary_page_size_limit(8 * 1024 * 1024)
        .set_max_row_group_row_count(Some(ROWS))
        .set_write_batch_size(WRITE_ROWS)
        .set_data_page_row_count_limit(WRITE_ROWS);
    let properties = if encoding == Encoding::RLE_DICTIONARY {
        properties.set_column_dictionary_enabled(column, true)
    } else {
        properties.set_column_encoding(column, encoding)
    };
    // Compression keeps the fixture small; decoding must still materialize
    // the complete >2 GiB logical window.
    #[cfg(feature = "zstd")]
    let properties =
        properties.set_compression(parquet::basic::Compression::ZSTD(Default::default()));
    let file = tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(
        file.try_clone().unwrap(),
        schema.clone(),
        Some(properties.build()),
    )
    .unwrap();
    for start in (0..ROWS).step_by(WRITE_ROWS) {
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
            start as i64..(start + WRITE_ROWS) as i64,
        ));
        writer
            .write(&RecordBatch::try_new(schema.clone(), vec![values.clone(), ids]).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
    drop(values);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(builder.metadata().num_row_groups(), 1);
    assert!(
        builder
            .metadata()
            .row_group(0)
            .column(0)
            .encodings()
            .any(|actual| actual == encoding),
        "fixture must exercise {encoding:?}"
    );
    let mut rows = 0;
    let mut output_rows = vec![];
    for batch in builder.with_batch_size(ROWS).build().unwrap() {
        let batch = batch.expect("the reader must split a wide logical window");
        assert_eq!(batch.schema(), schema);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(i32::try_from(values.values().len()).is_ok());
        for index in 0..batch.num_rows() {
            assert_eq!(ids.value(index), (rows + index) as i64);
            assert!(
                values.value(index) == value.as_slice(),
                "{encoding:?}: payload differs at row {}",
                rows + index
            );
        }
        output_rows.push(batch.num_rows());
        rows += batch.num_rows();
        // Drop this output before requesting the next. The pending window
        // and a near-2 GiB output still coexist; this remains a heavy test.
    }
    assert_eq!(rows, ROWS);
    let first_rows = i32::MAX as usize / VALUE_SIZE;
    assert_eq!(output_rows, vec![first_rows, ROWS - first_rows]);
    println!(
        "{encoding:?}: streamed {rows} rows / {} bytes as {output_rows:?}",
        ROWS * VALUE_SIZE
    );
}

#[test]
#[ignore = "requires several GiB of peak memory; run --ignored --test-threads=1"]
fn large_binary_plain_encoding_overflow() {
    roundtrip_large_binary(Encoding::PLAIN);
}

#[test]
#[ignore = "requires several GiB of peak memory; run --ignored --test-threads=1"]
fn large_binary_delta_length_encoding_overflow() {
    roundtrip_large_binary(Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

#[test]
#[ignore = "requires several GiB of peak memory; run --ignored --test-threads=1"]
fn large_binary_delta_byte_array_encoding_overflow() {
    roundtrip_large_binary(Encoding::DELTA_BYTE_ARRAY);
}

#[test]
#[ignore = "requires several GiB of peak memory; run --ignored --test-threads=1"]
fn large_binary_rle_dictionary_encoding_overflow() {
    roundtrip_large_binary(Encoding::RLE_DICTIONARY);
}
