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

//! This module contains tests for reading encrypted Parquet files with the async Arrow API

use crate::encryption_util::verify_encryption_test_data;
use arrow_array::cast::AsArray;
use arrow_array::types;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::errors::ParquetError;
use tokio::fs::File;

#[tokio::test]
async fn test_non_uniform_encryption_plaintext_footer() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    // There is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    let footer_key = "0123456789012345".as_bytes().to_vec(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key)
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_misspecified_encryption_keys() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");

    // There is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    let footer_key = "0123456789012345".as_bytes(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    // read file with keys and check for expected error message
    async fn check_for_error(
        expected_message: &str,
        path: &String,
        footer_key: &[u8],
        column_1_key: &[u8],
        column_2_key: &[u8],
    ) {
        let mut file = File::open(&path).await.unwrap();

        let mut decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec());

        if !column_1_key.is_empty() {
            decryption_properties =
                decryption_properties.with_column_key("double_field", column_1_key.to_vec());
        }

        if !column_2_key.is_empty() {
            decryption_properties =
                decryption_properties.with_column_key("float_field", column_2_key.to_vec());
        }

        let decryption_properties = decryption_properties.build().unwrap();

        match verify_encryption_test_file_read_async(&mut file, decryption_properties).await {
            Ok(_) => {
                panic!("did not get expected error")
            }
            Err(e) => {
                assert_eq!(e.to_string(), expected_message);
            }
        }
    }

    // Too short footer key
    check_for_error(
        "Parquet error: Invalid footer key. Failed to create AES key",
        &path,
        "bad_pwd".as_bytes(),
        column_1_key,
        column_2_key,
    )
    .await;

    // Wrong footer key
    check_for_error(
        "Parquet error: Provided footer key and AAD were unable to decrypt parquet footer",
        &path,
        "1123456789012345".as_bytes(),
        column_1_key,
        column_2_key,
    )
    .await;

    // Missing column key
    check_for_error("Parquet error: Unable to decrypt column 'double_field', perhaps the column key is wrong or missing?",
                    &path, footer_key, "".as_bytes(), column_2_key).await;

    // Too short column key
    check_for_error(
        "Parquet error: Failed to create AES key",
        &path,
        footer_key,
        "abc".as_bytes(),
        column_2_key,
    )
    .await;

    // Wrong column key
    check_for_error("Parquet error: Unable to decrypt column 'double_field', perhaps the column key is wrong or missing?",
                    &path, footer_key, "1123456789012345".as_bytes(), column_2_key).await;

    // Mixed up keys
    check_for_error("Parquet error: Unable to decrypt column 'float_field', perhaps the column key is wrong or missing?",
                    &path, footer_key, column_2_key, column_1_key).await;
}

#[tokio::test]
async fn test_non_uniform_encryption_plaintext_footer_without_decryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default())
        .await
        .unwrap();
    let file_metadata = metadata.metadata().file_metadata();

    assert_eq!(file_metadata.num_rows(), 50);
    assert_eq!(file_metadata.schema_descr().num_columns(), 8);
    assert_eq!(
        file_metadata.created_by().unwrap(),
        "parquet-cpp-arrow version 19.0.0-SNAPSHOT"
    );

    metadata.metadata().row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 8);
        assert_eq!(rg.num_rows(), 50);
    });

    // Should be able to read unencrypted columns. Test reading one column.
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let record_reader = builder.with_projection(mask).build().unwrap();
    let record_batches = record_reader.try_collect::<Vec<_>>().await.unwrap();

    let mut row_count = 0;
    for batch in record_batches {
        let batch = batch;
        row_count += batch.num_rows();

        let time_col = batch
            .column(0)
            .as_primitive::<types::Time32MillisecondType>();
        for (i, x) in time_col.iter().enumerate() {
            assert_eq!(x.unwrap(), i as i32);
        }
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);

    // Reading an encrypted column should fail
    let file = File::open(&path).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [4]);
    let mut record_reader = builder.with_projection(mask).build().unwrap();

    match record_reader.next().await {
        Some(Err(ParquetError::ArrowError(s))) => {
            assert!(s.contains("protocol error"));
        }
        _ => {
            panic!("Expected ArrowError::ParquetError");
        }
    };
}

#[tokio::test]
async fn test_non_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let footer_key = "0123456789012345".as_bytes().to_vec(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_aes_ctr_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer_ctr.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let footer_key = "0123456789012345".as_bytes().to_vec();
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    //let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key)
        .with_column_key("double_field", column_1_key.clone())
        .with_column_key("float_field", column_1_key)
        .build()
        .unwrap();

    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
    let metadata = ArrowReaderMetadata::load_async(&mut file, options).await;

    match metadata {
        Err(ParquetError::NYI(s)) => {
            assert!(s.contains("AES_GCM_CTR_V1"));
        }
        _ => {
            panic!("Expected ParquetError::NYI");
        }
    };
}

#[tokio::test]
async fn test_decrypting_without_decryption_properties_fails() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let options = ArrowReaderOptions::new();
    let result = ArrowReaderMetadata::load_async(&mut file, options).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but no decryption properties were provided"
    );
}

async fn verify_encryption_test_file_read_async(
    file: &mut tokio::fs::File,
    decryption_properties: FileDecryptionProperties,
) -> Result<(), ParquetError> {
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);

    let metadata = ArrowReaderMetadata::load_async(file, options.clone()).await?;
    let arrow_reader_metadata = ArrowReaderMetadata::load_async(file, options).await?;
    let file_metadata = metadata.metadata().file_metadata();

    let record_reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        arrow_reader_metadata.clone(),
    )
    .build()?;
    let record_batches = record_reader.try_collect::<Vec<_>>().await?;

    verify_encryption_test_data(record_batches, file_metadata.clone(), metadata);
    Ok(())
}
