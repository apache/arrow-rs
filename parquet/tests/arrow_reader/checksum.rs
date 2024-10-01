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

//! This file contains an end to end test for verifying checksums when reading parquet files.

use std::path::PathBuf;

use arrow::util::test_util::parquet_test_data;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;

#[test]
fn test_datapage_v1_corrupt_checksum() {
    let errors = read_file_batch_errors("datapage_v1-corrupt-checksum.parquet");
    assert_eq!(errors, [
        Err("Parquet argument error: Parquet error: Page CRC checksum mismatch".to_string()), 
        Ok(()),
        Ok(()),
        Err("Parquet argument error: Parquet error: Page CRC checksum mismatch".to_string()), 
        Err("Parquet argument error: Parquet error: Not all children array length are the same!".to_string())
    ]);
}

#[test]
fn test_datapage_v1_uncompressed_checksum() {
    let errors = read_file_batch_errors("datapage_v1-uncompressed-checksum.parquet");
    assert_eq!(errors, [Ok(()), Ok(()), Ok(()), Ok(()), Ok(())]);
}

#[test]
fn test_datapage_v1_snappy_compressed_checksum() {
    let errors = read_file_batch_errors("datapage_v1-snappy-compressed-checksum.parquet");
    assert_eq!(errors, [Ok(()), Ok(()), Ok(()), Ok(()), Ok(())]);
}

#[test]
fn test_plain_dict_uncompressed_checksum() {
    let errors = read_file_batch_errors("plain-dict-uncompressed-checksum.parquet");
    assert_eq!(errors, [Ok(())]);
}
#[test]
fn test_rle_dict_snappy_checksum() {
    let errors = read_file_batch_errors("rle-dict-snappy-checksum.parquet");
    assert_eq!(errors, [Ok(())]);
}

/// Reads a file and returns a vector with one element per record batch.
/// The record batch data is replaced with () and errors are stringified.
fn read_file_batch_errors(name: &str) -> Vec<Result<(), String>> {
    let path = PathBuf::from(parquet_test_data()).join(name);
    println!("Reading file: {:?}", path);
    let file = std::fs::File::open(&path).unwrap();
    let reader = ArrowReaderBuilder::try_new(file).unwrap().build().unwrap();
    reader
        .map(|x| match x {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        })
        .collect()
}
