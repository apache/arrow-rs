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

use arrow::compute::concat_batches;
use arrow::util::test_util::parquet_test_data;
use arrow_array::cast::as_primitive_array;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, Float32Array, Float64Array, RecordBatch};
use arrow_csv::ReaderBuilder as CsvReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::basic::Encoding;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[test]
fn test_read_f32_alp() {
    let data_dir = PathBuf::from(parquet_test_data());
    let parquet_path = data_dir.join("alp_float_arade.parquet");
    let expected_csv_path = data_dir.join("alp_arade_expect.csv");

    let expected = read_expected_csv_batch(&expected_csv_path);
    let actual = read_parquet_batch(&parquet_path);

    assert_eq!(actual.schema(), expected.schema(), "schema mismatch");
    assert_eq!(
        actual.num_columns(),
        expected.num_columns(),
        "column mismatch"
    );
    assert_eq!(actual.num_rows(), expected.num_rows(), "row count mismatch");

    for col_idx in 0..actual.num_columns() {
        let col_name = actual.schema().field(col_idx).name().clone();
        let actual_col = as_primitive_array::<Float32Type>(actual.column(col_idx).as_ref());
        let expected_col = as_primitive_array::<Float32Type>(expected.column(col_idx).as_ref());

        for row_idx in 0..actual.num_rows() {
            assert_eq!(
                actual_col.is_valid(row_idx),
                expected_col.is_valid(row_idx),
                "null mismatch at column {col_name} row {row_idx}"
            );
            if actual_col.is_valid(row_idx) {
                let actual_value = actual_col.value(row_idx);
                let expected_value = expected_col.value(row_idx);
                assert!(
                    actual_value.to_bits() == expected_value.to_bits(),
                    "bit mismatch at column {col_name} row {row_idx}: expected={expected_value} actual={actual_value}"
                );
            }
        }
    }
}

/// Write the arade values with the ALP encoder and read them back, over real
/// float data rather than synthetic decimals.
///
/// This checks losslessness only, not compression: these are `f32` values whose
/// encoded integers need 31 bits and which except 6.4% of the time, so ALP is
/// larger than PLAIN here. The arrow-cpp implementation produces a byte-identical
/// page from the same values (see `test_matches_cpp_reference_page`), so that is
/// a property of the data, not of the encoder. ALP's win on arade in the paper is
/// on the `f64` version of the dataset.
#[test]
fn test_write_f32_alp_roundtrip() {
    let data_dir = PathBuf::from(parquet_test_data());
    let expected = read_expected_csv_batch(&data_dir.join("alp_arade_expect.csv"));

    let alp_bytes = write_batch(&expected, Encoding::ALP);
    let actual = read_parquet_bytes(alp_bytes);
    assert_eq!(actual.num_rows(), expected.num_rows(), "row count mismatch");

    for col_idx in 0..expected.num_columns() {
        let col_name = expected.schema().field(col_idx).name().clone();
        let actual_col = as_primitive_array::<Float32Type>(actual.column(col_idx).as_ref());
        let expected_col = as_primitive_array::<Float32Type>(expected.column(col_idx).as_ref());

        for row_idx in 0..expected.num_rows() {
            assert_eq!(
                actual_col.is_valid(row_idx),
                expected_col.is_valid(row_idx),
                "null mismatch at column {col_name} row {row_idx}"
            );
            if expected_col.is_valid(row_idx) {
                // Bitwise, so that NaN and -0.0 are held to the same standard.
                assert_eq!(
                    actual_col.value(row_idx).to_bits(),
                    expected_col.value(row_idx).to_bits(),
                    "bit mismatch at column {col_name} row {row_idx}"
                );
            }
        }
    }
}

/// Round-trip a nullable column under both data page versions.
///
/// The versions differ in what the reader can tell the value decoder. A v2 page
/// header carries `num_nulls`, so the decoder is handed the exact count of
/// encoded values; a v1 header carries only `num_values`, which counts nulls, so
/// the decoder receives the level count instead. Only non-null values are
/// encoded either way, which is why the ALP header's element count - not the
/// count the reader passes in - is what governs the decode.
///
/// The exception values are here deliberately: they make the encoded and
/// unencoded value counts differ from the level count in two different ways at
/// once.
#[test]
fn test_alp_roundtrip_page_versions_with_nulls() {
    let f32_values: Vec<Option<f32>> = (0..3000)
        .map(|i| match i % 100 {
            0 => None,
            7 => Some(f32::NAN),
            13 => Some(-0.0),
            _ => Some(i as f32 * 0.01),
        })
        .collect();
    let f64_values: Vec<Option<f64>> = (0..3000)
        .map(|i| match i % 100 {
            5 => None,
            11 => Some(f64::INFINITY),
            _ => Some(i as f64 * 0.001),
        })
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float32Array::from(f32_values.clone())),
            Arc::new(Float64Array::from(f64_values.clone())),
        ],
    )
    .unwrap();

    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::ALP)
            .set_writer_version(version)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let actual = read_parquet_bytes(buf.into());
        assert_eq!(
            actual.num_rows(),
            batch.num_rows(),
            "{version:?}: row count"
        );

        let actual_f32 = as_primitive_array::<Float32Type>(actual.column(0).as_ref());
        for (row, expected) in f32_values.iter().enumerate() {
            match expected {
                None => assert!(
                    actual_f32.is_null(row),
                    "{version:?}: f32 row {row} not null"
                ),
                // Bitwise, so NaN and -0.0 are held to the same standard.
                Some(value) => assert_eq!(
                    actual_f32.value(row).to_bits(),
                    value.to_bits(),
                    "{version:?}: f32 row {row}"
                ),
            }
        }

        let actual_f64 = as_primitive_array::<Float64Type>(actual.column(1).as_ref());
        for (row, expected) in f64_values.iter().enumerate() {
            match expected {
                None => assert!(
                    actual_f64.is_null(row),
                    "{version:?}: f64 row {row} not null"
                ),
                Some(value) => assert_eq!(
                    actual_f64.value(row).to_bits(),
                    value.to_bits(),
                    "{version:?}: f64 row {row}"
                ),
            }
        }
    }
}

fn write_batch(batch: &RecordBatch, encoding: Encoding) -> Bytes {
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(encoding)
        .build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    buf.into()
}

fn read_parquet_bytes(bytes: Bytes) -> RecordBatch {
    let reader = ArrowReaderBuilder::try_new(bytes).unwrap().build().unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    assert!(!batches.is_empty(), "expected non-empty parquet batch set");
    concat_batches(batches[0].schema_ref(), &batches).unwrap()
}

fn alp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("value1", DataType::Float32, true),
        Field::new("value2", DataType::Float32, true),
        Field::new("value3", DataType::Float32, true),
        Field::new("value4", DataType::Float32, true),
    ]))
}

fn read_parquet_batch(path: &PathBuf) -> RecordBatch {
    let file = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.unwrap());
    }
    assert!(!batches.is_empty(), "expected non-empty parquet batch set");
    concat_batches(batches[0].schema_ref(), &batches).unwrap()
}

fn read_expected_csv_batch(path: &PathBuf) -> RecordBatch {
    let file = File::open(path).unwrap();
    let schema = alp_schema();
    let reader = CsvReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    assert!(!batches.is_empty(), "expected non-empty csv batch set");
    concat_batches(&schema, &batches).unwrap()
}
