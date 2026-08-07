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

//! Tests with interoperability files in [parquet-testing]
//!
//! [parquet-testing]: https://github.com/apache/parquet-testing

use arrow::util::test_util::parquet_test_data;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::ArrowError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

/// The ALP test data file has 8 columns, each containing the same 9032 values.
///
/// The float_plain and double_plain columns are encoded with `PLAIN` + zstd
/// compression, and serve as a reference for the other columns which are
/// encoded with `ALP` encoding.
///
/// Ensure the values in the other column come back the same as the reference columns
///
/// | Column                              | Encoding                  | Rationale / coverage                                                              |
/// |-------------------------------------|---------------------------|-----------------------------------------------------------------------------------|
/// | `float_plain`, `double_plain`       | `PLAIN` + zstd            | In-file reference: readers can bit-compare the ALP columns against these          |
/// | `float_alp_1024`, `double_alp_1024` | `ALP`, 1024-value vectors | The default vector size of 1024 values                                            |
/// | `float_alp_4096`, `double_alp_4096` | `ALP`, 4096-value vectors | Readers must honor `log_vector_size` from the page header rather than assume 1024 |
/// | `float_alp_32`, `double_alp_32`     | `ALP`, 32-value vectors   | Many vectors per page, stresses the per-vector metadata loop                      |
#[test]
fn test_alp_extended() {
    let alp_extended = PathBuf::from(parquet_test_data()).join("alp_extended.zstd.parquet");
    let file = File::open(alp_extended).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 9032);
    batches
        .iter()
        .for_each(|batch| assert_eq!(batch.num_columns(), 8));

    // compare float values to the reference
    let float_plain = column(&batches, "float_plain").unwrap();
    assert_eq!(float_plain, column(&batches, "float_alp_1024").unwrap());
    assert_eq!(float_plain, column(&batches, "float_alp_4096").unwrap());
    assert_eq!(float_plain, column(&batches, "float_alp_32").unwrap());

    // compare double values to the reference
    let double_plain = column(&batches, "double_plain").unwrap();
    assert_eq!(double_plain, column(&batches, "double_alp_1024").unwrap());
    assert_eq!(double_plain, column(&batches, "double_alp_4096").unwrap());
    assert_eq!(double_plain, column(&batches, "double_alp_32").unwrap());
}

fn column(batches: &[RecordBatch], column_name: &str) -> Result<Vec<ArrayRef>, ArrowError> {
    let mut columns = Vec::new();
    for batch in batches {
        let array = batch.column(batch.schema().index_of(column_name)?);
        columns.push(array.clone());
    }
    Ok(columns)
}
