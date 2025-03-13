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

//! This module contains tests for reading encrypted Parquet files with the Arrow API

use crate::encryption_util::verify_encryption_test_data;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::encryption::decrypt::FileDecryptionProperties;
use std::fs::File;

#[test]
fn test_non_uniform_encryption_plaintext_footer() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let file = File::open(path).unwrap();

    // There is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    let footer_key = "0123456789012345".as_bytes(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read(file, decryption_properties);
}

#[test]
fn test_non_uniform_encryption_disabled_aad_storage() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path =
        format!("{test_data}/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted");
    let file = File::open(path.clone()).unwrap();

    let footer_key = "0123456789012345".as_bytes(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    // Can read successfully when providing the correct AAD prefix
    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .with_aad_prefix("tester".as_bytes().to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read(file, decryption_properties);

    // Using wrong AAD prefix should fail
    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .with_aad_prefix("wrong_aad_prefix".as_bytes().to_vec())
        .build()
        .unwrap();

    let file = File::open(path.clone()).unwrap();
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let result = ArrowReaderMetadata::load(&file, options.clone());
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Provided footer key and AAD were unable to decrypt parquet footer"
    );

    // Not providing any AAD prefix should fail as it isn't stored in the file
    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .build()
        .unwrap();

    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let result = ArrowReaderMetadata::load(&file, options.clone());
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Provided footer key and AAD were unable to decrypt parquet footer"
    );
}

#[test]
#[cfg(feature = "snap")]
fn test_plaintext_footer_read_without_decryption() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties();
}

#[test]
fn test_non_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");
    let file = File::open(path).unwrap();

    let footer_key = "0123456789012345".as_bytes(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read(file, decryption_properties);
}

#[test]
fn test_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let file = File::open(path).unwrap();

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read(file, decryption_properties);
}

#[test]
fn test_decrypting_without_decryption_properties_fails() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let file = File::open(path).unwrap();

    let options = ArrowReaderOptions::default();
    let result = ArrowReaderMetadata::load(&file, options.clone());
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but no decryption properties were provided"
    );
}

#[test]
fn test_aes_ctr_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer_ctr.parquet.encrypted");
    let file = File::open(path).unwrap();

    let footer_key = "0123456789012345".as_bytes();
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key.to_vec())
        .with_column_key("float_field", column_2_key.to_vec())
        .build()
        .unwrap();

    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
    let metadata = ArrowReaderMetadata::load(&file, options);

    match metadata {
        Err(parquet::errors::ParquetError::NYI(s)) => {
            assert!(s.contains("AES_GCM_CTR_V1"));
        }
        _ => {
            panic!("Expected ParquetError::NYI");
        }
    };
}

fn verify_encryption_test_file_read(file: File, decryption_properties: FileDecryptionProperties) {
    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
    let reader_metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
    let metadata = reader_metadata.metadata();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let record_reader = builder.build().unwrap();
    let record_batches = record_reader
        .map(|x| x.unwrap())
        .collect::<Vec<RecordBatch>>();

    verify_encryption_test_data(record_batches, metadata);
}
