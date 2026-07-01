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
use arrow_array::types::Float32Type;
use arrow_array::{Array, RecordBatch};
use arrow_csv::ReaderBuilder as CsvReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
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
