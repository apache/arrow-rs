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

use crate::encryption_util;
use crate::encryption_util::{
    AES_128_COLUMN_KEYS, AES_128_COLUMN_NAME_KEYS, AES_128_COLUMN_NAMES, AES_128_FOOTER_KEY,
    AES_128_FOOTER_KEY_NAME, AES_128_KEY_NAME_KEY, AES_128_KEY_NAMES, AES_256_COLUMN_KEYS,
    AES_256_COLUMN_NAME_KEYS, AES_256_COLUMN_NAMES, AES_256_FOOTER_KEY, AES_256_FOOTER_KEY_NAME,
    AES_256_KEY_NAME_KEY, AES_256_KEY_NAMES, BAD_AES_128_FOOTER_KEY, BAD_AES_256_FOOTER_KEY,
    TestKeyRetriever, read_and_roundtrip_to_encrypted_file, verify_column_indexes,
    verify_encryption_test_file_read,
};
use arrow::array::*;
use arrow::error::Result as ArrowResult;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::sync::Arc;

#[test]
fn test_non_uniform_encryption_plaintext_footer() {
    fn non_uniform_encryption_plaintext_footer(footer_key: &[u8], column_keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_plaintext_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();
        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = builder.build().unwrap();
        verify_encryption_test_file_read(file, decryption_properties);
    }

    // AES-128: there is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    non_uniform_encryption_plaintext_footer(AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);

    // AES-256
    non_uniform_encryption_plaintext_footer(AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS);
}

#[test]
fn test_plaintext_footer_signature_verification() {
    fn plaintext_footer_signature_verification(footer_key: &[u8], column_keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_plaintext_footer.parquet.encrypted",
        );
        let file = File::open(path.clone()).unwrap();
        let mut disable_footer_signature_verification_builder =
            FileDecryptionProperties::builder(footer_key.to_vec())
                .disable_footer_signature_verification();
        for (column_name, key) in column_keys {
            disable_footer_signature_verification_builder =
                disable_footer_signature_verification_builder
                    .with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = disable_footer_signature_verification_builder
            .build()
            .unwrap();
        verify_encryption_test_file_read(file, decryption_properties);

        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }

        let file = File::open(path.clone()).unwrap();
        let decryption_properties = builder.build().unwrap();
        let options =
            ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
        let result = ArrowReaderMetadata::load(&file, options.clone());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .starts_with("Parquet error: Footer signature verification failed. Computed: [")
        );
    }

    plaintext_footer_signature_verification(BAD_AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);
    plaintext_footer_signature_verification(BAD_AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS)
}

#[test]
fn test_non_uniform_encryption_disabled_aad_storage() {
    fn non_uniform_encryption_disabled_aad_storage(
        footer_key: &[u8],
        column_keys: &[(&str, &[u8])],
    ) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted",
        );

        // Can read successfully when providing the correct AAD prefix
        let file = File::open(path.clone()).unwrap();
        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec())
            .with_aad_prefix(b"tester".to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = builder.build().unwrap();
        verify_encryption_test_file_read(file, decryption_properties);

        // Using wrong AAD prefix should fail
        let file = File::open(path.clone()).unwrap();
        let mut wrong_aad_builder = FileDecryptionProperties::builder(footer_key.to_vec())
            .with_aad_prefix(b"wrong_aad_prefix".to_vec());
        for (column_name, key) in column_keys {
            wrong_aad_builder = wrong_aad_builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = wrong_aad_builder.build().unwrap();
        let options =
            ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
        let result = ArrowReaderMetadata::load(&file, options.clone());
        assert!(result.is_err());
        std::assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet error: Provided footer key and AAD were unable to decrypt parquet footer"
        );

        // Not providing any AAD prefix should fail as it isn't stored in the file
        let mut no_aad_builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            no_aad_builder = no_aad_builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = no_aad_builder.build().unwrap();

        let file = File::open(path).unwrap();
        let options =
            ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
        let result = ArrowReaderMetadata::load(&file, options.clone());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet error: Parquet file was encrypted with an AAD prefix that is not stored in the file, \
        but no AAD prefix was provided in the file decryption properties"
        );
    }

    non_uniform_encryption_disabled_aad_storage(AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);
    non_uniform_encryption_disabled_aad_storage(AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS);
}

#[test]
#[cfg(feature = "snap")]
fn test_plaintext_footer_read_without_decryption() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties();
}

#[test]
fn test_non_uniform_encryption() {
    fn non_uniform_encryption(footer_key: &[u8], column_keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = builder.build().unwrap();

        verify_encryption_test_file_read(file, decryption_properties);
    }

    non_uniform_encryption(AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);
    non_uniform_encryption(AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS);
}

#[test]
fn test_uniform_encryption() {
    fn uniform_encryption(footer_key: &[u8]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "uniform_encryption.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
            .build()
            .unwrap();

        verify_encryption_test_file_read(file, decryption_properties);
    }

    uniform_encryption(AES_128_FOOTER_KEY);
    uniform_encryption(AES_256_FOOTER_KEY);
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
        "Parquet error: Parquet file has an encrypted footer but decryption properties were not provided"
    );
}

#[test]
fn test_aes_ctr_encryption() {
    fn aes_ctr_encryption(footer_key: &[u8], column_keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer_ctr.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = builder.build().unwrap();

        let options =
            ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
        let metadata = ArrowReaderMetadata::load(&file, options);

        match metadata {
            Err(ParquetError::NYI(s)) => {
                assert!(s.contains("AES_GCM_CTR_V1"));
            }
            _ => {
                panic!("Expected ParquetError::NYI");
            }
        };
    }

    aes_ctr_encryption(AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);
    aes_ctr_encryption(AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS);
}

#[test]
fn test_non_uniform_encryption_plaintext_footer_with_key_retriever() {
    fn non_uniform_encryption_plaintext_footer_with_key_retriever(
        footer_key: &[u8],
        keys: &[(&str, &[u8])],
    ) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_plaintext_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let mut key_retriever = TestKeyRetriever::new();
        for (key_name, key) in keys {
            key_retriever = key_retriever.with_key((*key_name).to_owned(), (*key).to_vec());
        }

        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
                .build()
                .unwrap();

        verify_encryption_test_file_read(file, decryption_properties);
    }

    non_uniform_encryption_plaintext_footer_with_key_retriever(
        AES_128_FOOTER_KEY,
        AES_128_KEY_NAME_KEY,
    );

    non_uniform_encryption_plaintext_footer_with_key_retriever(
        AES_256_FOOTER_KEY,
        AES_256_KEY_NAME_KEY,
    );
}

#[test]
fn test_roundtrip_non_uniform_encryption_plaintext_footer_with_key_retriever() {
    fn roundtrip_non_uniform_encryption_plaintext_footer_with_key_retriever(
        footer_key: &[u8],
        footer_key_metadata: &str,
        wrong_footer_key: &[u8],
        dec_column_keys: &[(&str, &[u8])],
        enc_column_keys: &[(&str, &[u8], &str)],
    ) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_plaintext_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let mut key_retriever =
            TestKeyRetriever::new().with_key(footer_key_metadata.to_owned(), footer_key.to_vec());

        for (key_name, key) in dec_column_keys {
            key_retriever = key_retriever.with_key((*key_name).to_owned(), (*key).to_vec());
        }

        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
                .build()
                .unwrap();

        let options = ArrowReaderOptions::default()
            .with_file_decryption_properties(decryption_properties.clone());
        let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();

        // Write data into temporary file with plaintext footer and footer key metadata
        let temp_file = tempfile::tempfile().unwrap();
        let mut encryption_properties_builder =
            FileEncryptionProperties::builder(footer_key.to_vec())
                .with_footer_key_metadata(footer_key_metadata.into());
        for (column_name, key, metadata) in enc_column_keys {
            encryption_properties_builder = encryption_properties_builder
                .with_column_key_and_metadata(column_name, key.to_vec(), (*metadata).into());
        }
        let encryption_properties = encryption_properties_builder
            .with_plaintext_footer(true)
            .build()
            .unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
        let batch_reader = builder.build().unwrap();
        let batches = batch_reader
            .collect::<parquet::errors::Result<Vec<RecordBatch>, _>>()
            .unwrap();

        let props = WriterProperties::builder()
            .with_file_encryption_properties(encryption_properties)
            .build();

        let mut writer = ArrowWriter::try_new(
            temp_file.try_clone().unwrap(),
            metadata.schema().clone(),
            Some(props),
        )
        .unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }

        writer.close().unwrap();

        // Read temporary file with plaintext metadata using key retriever
        let options = ArrowReaderOptions::default()
            .with_file_decryption_properties(decryption_properties.clone());
        let _ = ArrowReaderMetadata::load(&temp_file, options.clone()).unwrap();

        // Read temporary file with plaintext metadata using key retriever with invalid key
        let mut key_retriever = TestKeyRetriever::new()
            .with_key(footer_key_metadata.to_owned(), wrong_footer_key.to_vec());

        for (key_name, key) in dec_column_keys {
            key_retriever = key_retriever.with_key((*key_name).to_owned(), (*key).to_vec());
        }
        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
                .build()
                .unwrap();
        let options =
            ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);
        let result = ArrowReaderMetadata::load(&temp_file, options.clone());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .starts_with("Parquet error: Footer signature verification failed. Computed: [")
        );
    }

    roundtrip_non_uniform_encryption_plaintext_footer_with_key_retriever(
        AES_128_FOOTER_KEY,
        AES_128_FOOTER_KEY_NAME,
        BAD_AES_128_FOOTER_KEY,
        &[
            (AES_128_KEY_NAMES[0], AES_128_COLUMN_KEYS[0]),
            (AES_128_KEY_NAMES[1], AES_128_COLUMN_KEYS[1]),
        ],
        &[
            (
                AES_128_COLUMN_NAMES[0],
                AES_128_COLUMN_KEYS[0],
                AES_128_KEY_NAMES[0],
            ),
            (
                AES_128_COLUMN_NAMES[1],
                AES_128_COLUMN_KEYS[1],
                AES_128_KEY_NAMES[1],
            ),
        ],
    );

    roundtrip_non_uniform_encryption_plaintext_footer_with_key_retriever(
        AES_256_FOOTER_KEY,
        AES_256_FOOTER_KEY_NAME,
        BAD_AES_256_FOOTER_KEY,
        &[
            (AES_256_KEY_NAMES[0], AES_256_COLUMN_KEYS[0]),
            (AES_256_KEY_NAMES[1], AES_256_COLUMN_KEYS[1]),
            (AES_256_KEY_NAMES[2], AES_256_COLUMN_KEYS[2]),
            (AES_256_KEY_NAMES[3], AES_256_COLUMN_KEYS[3]),
            (AES_256_KEY_NAMES[4], AES_256_COLUMN_KEYS[4]),
            (AES_256_KEY_NAMES[5], AES_256_COLUMN_KEYS[5]),
            (AES_256_KEY_NAMES[6], AES_256_COLUMN_KEYS[6]),
            (AES_256_KEY_NAMES[7], AES_256_COLUMN_KEYS[7]),
        ],
        &[
            (
                AES_256_COLUMN_NAMES[0],
                AES_256_COLUMN_KEYS[0],
                AES_256_KEY_NAMES[0],
            ),
            (
                AES_256_COLUMN_NAMES[1],
                AES_256_COLUMN_KEYS[1],
                AES_256_KEY_NAMES[1],
            ),
            (
                AES_256_COLUMN_NAMES[2],
                AES_256_COLUMN_KEYS[2],
                AES_256_KEY_NAMES[2],
            ),
            (
                AES_256_COLUMN_NAMES[3],
                AES_256_COLUMN_KEYS[3],
                AES_256_KEY_NAMES[3],
            ),
            (
                AES_256_COLUMN_NAMES[4],
                AES_256_COLUMN_KEYS[4],
                AES_256_KEY_NAMES[4],
            ),
            (
                AES_256_COLUMN_NAMES[5],
                AES_256_COLUMN_KEYS[5],
                AES_256_KEY_NAMES[5],
            ),
            (
                AES_256_COLUMN_NAMES[6],
                AES_256_COLUMN_KEYS[6],
                AES_256_KEY_NAMES[6],
            ),
            (
                AES_256_COLUMN_NAMES[7],
                AES_256_COLUMN_KEYS[7],
                AES_256_KEY_NAMES[7],
            ),
        ],
    );
}

#[test]
fn test_non_uniform_encryption_with_key_retriever() {
    fn non_uniform_encryption_with_key_retriever(footer_key: &[u8], keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let mut key_retriever = TestKeyRetriever::new();
        for (key_name, key) in keys {
            key_retriever = key_retriever.with_key((*key_name).to_owned(), (*key).to_vec());
        }

        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
                .build()
                .unwrap();

        verify_encryption_test_file_read(file, decryption_properties);
    }

    non_uniform_encryption_with_key_retriever(AES_128_FOOTER_KEY, AES_128_KEY_NAME_KEY);

    non_uniform_encryption_with_key_retriever(AES_256_FOOTER_KEY, AES_256_KEY_NAME_KEY);
}

#[test]
fn test_uniform_encryption_with_key_retriever() {
    fn uniform_encryption_with_key_retriever(key_name: &str, footer_key: &[u8]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "uniform_encryption.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let key_retriever =
            TestKeyRetriever::new().with_key(key_name.to_owned(), footer_key.to_vec());

        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
                .build()
                .unwrap();

        verify_encryption_test_file_read(file, decryption_properties);
    }

    uniform_encryption_with_key_retriever(AES_128_FOOTER_KEY_NAME, AES_128_FOOTER_KEY);
    uniform_encryption_with_key_retriever(AES_256_FOOTER_KEY_NAME, AES_256_FOOTER_KEY);
}

fn row_group_sizes(metadata: &ParquetMetaData) -> Vec<i64> {
    metadata.row_groups().iter().map(|x| x.num_rows()).collect()
}

#[test]
fn test_uniform_encryption_roundtrip() {
    uniform_encryption_roundtrip(false, false).unwrap();
}

#[test]
fn test_uniform_encryption_roundtrip_with_dictionary() {
    uniform_encryption_roundtrip(false, true).unwrap();
}

#[test]
fn test_uniform_encryption_roundtrip_with_page_index() {
    uniform_encryption_roundtrip(true, false).unwrap();
}

#[test]
fn test_uniform_encryption_roundtrip_with_page_index_and_dictionary() {
    uniform_encryption_roundtrip(true, true).unwrap();
}

fn uniform_encryption_roundtrip(
    page_index: bool,
    dictionary_encoding: bool,
) -> parquet::errors::Result<()> {
    let x0_arrays = [
        Int32Array::from((0..100).collect::<Vec<_>>()),
        Int32Array::from((100..150).collect::<Vec<_>>()),
    ];
    let x1_arrays = [
        Int32Array::from((100..200).collect::<Vec<_>>()),
        Int32Array::from((200..250).collect::<Vec<_>>()),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("x0", ArrowDataType::Int32, false),
        Field::new("x1", ArrowDataType::Int32, false),
    ]));

    let file = tempfile::tempfile()?;

    let footer_key = AES_128_FOOTER_KEY;
    let file_encryption_properties =
        FileEncryptionProperties::builder(footer_key.to_vec()).build()?;

    let props = WriterProperties::builder()
        // Ensure multiple row groups
        .set_max_row_group_row_count(Some(50))
        // Ensure multiple pages per row group
        .set_write_batch_size(20)
        .set_data_page_row_count_limit(20)
        .set_dictionary_enabled(dictionary_encoding)
        .with_file_encryption_properties(file_encryption_properties)
        .build();

    let mut writer = ArrowWriter::try_new(file.try_clone()?, schema.clone(), Some(props))?;

    for (x0, x1) in x0_arrays.into_iter().zip(x1_arrays) {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(x0), Arc::new(x1)])?;
        writer.write(&batch)?;
    }

    writer.close()?;

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec()).build()?;

    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(decryption_properties)
        .with_page_index_policy(PageIndexPolicy::from(page_index));

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    assert_eq!(&row_group_sizes(builder.metadata()), &[50, 50, 50]);

    let batches = builder
        .with_batch_size(100)
        .build()?
        .collect::<ArrowResult<Vec<_>>>()?;

    assert_eq!(batches.len(), 2);
    assert!(batches.iter().all(|x| x.num_columns() == 2));

    let batch_sizes: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();

    assert_eq!(&batch_sizes, &[100, 50]);

    let x0_values: Vec<_> = batches
        .iter()
        .flat_map(|x| {
            x.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .cloned()
        })
        .collect();

    let x1_values: Vec<_> = batches
        .iter()
        .flat_map(|x| {
            x.column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .cloned()
        })
        .collect();

    let expected_x0_values: Vec<_> = (0..150).collect();
    assert_eq!(&x0_values, &expected_x0_values);

    let expected_x1_values: Vec<_> = (100..250).collect();
    assert_eq!(&x1_values, &expected_x1_values);
    Ok(())
}

#[test]
fn test_uniform_encryption_page_skipping() {
    uniform_encryption_page_skipping(false).unwrap();
}

#[test]
fn test_uniform_encryption_page_skipping_with_page_index() {
    uniform_encryption_page_skipping(true).unwrap();
}

fn uniform_encryption_page_skipping(page_index: bool) -> parquet::errors::Result<()> {
    let x0_arrays = [
        Int32Array::from((0..100).collect::<Vec<_>>()),
        Int32Array::from((100..150).collect::<Vec<_>>()),
    ];
    let x1_arrays = [
        Int32Array::from((100..200).collect::<Vec<_>>()),
        Int32Array::from((200..250).collect::<Vec<_>>()),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("x0", ArrowDataType::Int32, false),
        Field::new("x1", ArrowDataType::Int32, false),
    ]));

    let file = tempfile::tempfile()?;

    let footer_key = AES_128_FOOTER_KEY;
    let file_encryption_properties =
        FileEncryptionProperties::builder(footer_key.to_vec()).build()?;

    let props = WriterProperties::builder()
        // Ensure multiple row groups
        .set_max_row_group_row_count(Some(50))
        // Ensure multiple pages per row group
        .set_write_batch_size(20)
        .set_data_page_row_count_limit(20)
        .with_file_encryption_properties(file_encryption_properties)
        .build();

    let mut writer = ArrowWriter::try_new(file.try_clone()?, schema.clone(), Some(props))?;

    for (x0, x1) in x0_arrays.into_iter().zip(x1_arrays) {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(x0), Arc::new(x1)])?;
        writer.write(&batch)?;
    }

    writer.close()?;

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec()).build()?;

    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(decryption_properties)
        .with_page_index_policy(PageIndexPolicy::from(page_index));

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;

    let selection = RowSelection::from(vec![
        RowSelector::skip(25),
        RowSelector::select(50),
        RowSelector::skip(25),
        RowSelector::select(25),
        RowSelector::skip(25),
    ]);

    let batches = builder
        .with_row_selection(selection)
        .with_batch_size(100)
        .build()?
        .collect::<ArrowResult<Vec<_>>>()?;

    assert_eq!(batches.len(), 1);
    assert!(batches.iter().all(|x| x.num_columns() == 2));

    let batch_sizes: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();

    assert_eq!(&batch_sizes, &[75]);

    let x0_values: Vec<_> = batches
        .iter()
        .flat_map(|x| {
            x.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .cloned()
        })
        .collect();

    let x1_values: Vec<_> = batches
        .iter()
        .flat_map(|x| {
            x.column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .cloned()
        })
        .collect();

    let expected_x0_values: Vec<_> = [25..75, 100..125].into_iter().flatten().collect();
    assert_eq!(&x0_values, &expected_x0_values);

    let expected_x1_values: Vec<_> = [125..175, 200..225].into_iter().flatten().collect();
    assert_eq!(&x1_values, &expected_x1_values);
    Ok(())
}

#[test]
fn test_write_non_uniform_encryption() {
    fn write_non_uniform_encryption(
        footer_key: &[u8],
        column_names: Vec<&str>,
        column_keys: Vec<Vec<u8>>,
        encryption_column_keys: &[(&str, &[u8])],
    ) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
            .with_column_keys(column_names.to_vec(), column_keys.clone())
            .unwrap()
            .build()
            .unwrap();

        let mut builder = FileEncryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in encryption_column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let file_encryption_properties = builder.build().unwrap();

        read_and_roundtrip_to_encrypted_file(
            &file,
            decryption_properties,
            file_encryption_properties,
        );
    }

    write_non_uniform_encryption(
        AES_128_FOOTER_KEY,
        AES_128_COLUMN_NAMES.to_vec(),
        AES_128_COLUMN_KEYS.iter().map(|&s| s.to_vec()).collect(),
        AES_128_COLUMN_NAME_KEYS,
    );

    write_non_uniform_encryption(
        AES_256_FOOTER_KEY,
        AES_256_COLUMN_NAMES.to_vec(),
        AES_256_COLUMN_KEYS.iter().map(|&s| s.to_vec()).collect(),
        AES_256_COLUMN_NAME_KEYS,
    );
}

#[test]
fn test_write_uniform_encryption_plaintext_footer() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let file = File::open(path).unwrap();

    let footer_key = AES_128_FOOTER_KEY.to_vec();
    let wrong_footer_key = BAD_AES_128_FOOTER_KEY.to_vec();
    let column_1_key = AES_128_COLUMN_KEYS[0].to_vec();
    let column_2_key = AES_128_COLUMN_KEYS[1].to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .with_column_key(AES_128_COLUMN_NAMES[0], column_1_key.clone())
        .with_column_key(AES_128_COLUMN_NAMES[1], column_2_key.clone())
        .build()
        .unwrap();

    let wrong_decryption_properties = FileDecryptionProperties::builder(wrong_footer_key)
        .with_column_key(AES_128_COLUMN_NAMES[0], column_1_key)
        .with_column_key(AES_128_COLUMN_NAMES[1], column_2_key)
        .build()
        .unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(footer_key)
        .with_plaintext_footer(true)
        .build()
        .unwrap();

    // Try writing plaintext footer and then reading it with the correct footer key
    read_and_roundtrip_to_encrypted_file(
        &file,
        Arc::clone(&decryption_properties),
        file_encryption_properties.clone(),
    );

    // Try writing plaintext footer and then reading it with the wrong footer key
    let temp_file = tempfile::tempfile().unwrap();

    // read example data
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let batch_reader = builder.build().unwrap();
    let batches = batch_reader
        .collect::<parquet::errors::Result<Vec<RecordBatch>, _>>()
        .unwrap();

    // write example data
    let props = WriterProperties::builder()
        .with_file_encryption_properties(file_encryption_properties)
        .build();

    let mut writer = ArrowWriter::try_new(
        temp_file.try_clone().unwrap(),
        metadata.schema().clone(),
        Some(props),
    )
    .unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    // Try reading plaintext footer and with the wrong footer key
    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(wrong_decryption_properties);
    let result = ArrowReaderMetadata::load(&temp_file, options.clone());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .starts_with("Parquet error: Footer signature verification failed. Computed: [")
    );
}

#[test]
pub fn test_column_statistics_with_plaintext_footer() {
    let footer_key = AES_128_FOOTER_KEY.to_vec();
    let column_key = AES_128_COLUMN_KEYS[0].to_vec();

    // Encrypt with a plaintext footer and column-specific keys
    let encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_plaintext_footer(true)
        .with_column_key("x", column_key.clone())
        .with_column_key("y", column_key.clone())
        .with_column_key("s", column_key.clone())
        .build()
        .unwrap();

    // Read with only the footer key and the key for one column
    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .with_column_key("x", column_key.clone())
        .build()
        .unwrap();

    // Reader can read plaintext stats from the unencrypted column z
    // and column x for which the key is provided, but not columns y and s
    // for which no key is provided.
    write_and_read_stats(
        Arc::clone(&encryption_properties),
        Some(decryption_properties),
        &[true, false, true, false],
    );

    // Read without any decryption properties.
    // Reader can only read plaintext stats from the unencrypted column z.
    write_and_read_stats(encryption_properties, None, &[false, false, true, false]);
}

#[test]
pub fn test_column_statistics_with_plaintext_footer_and_uniform_encryption() {
    let footer_key = AES_128_FOOTER_KEY.to_vec();

    // Write with uniform encryption and a plaintext footer.
    let encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_plaintext_footer(true)
        .build()
        .unwrap();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .build()
        .unwrap();

    // Reader can read stats from plaintext footer metadata if a footer key is provided
    write_and_read_stats(
        Arc::clone(&encryption_properties),
        Some(decryption_properties),
        &[true, true, true, true],
    );

    // Reader can not read stats from plaintext footer metadata if no key is provided
    write_and_read_stats(encryption_properties, None, &[false, false, false, false]);
}

#[test]
pub fn test_column_statistics_with_encrypted_footer() {
    let footer_key = AES_128_FOOTER_KEY.to_vec();
    let column_key = AES_128_COLUMN_KEYS[0].to_vec();

    // Encrypt with an encrypted footer and column-specific keys
    let encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_plaintext_footer(false)
        .with_column_key("x", column_key.clone())
        .with_column_key("y", column_key.clone())
        .with_column_key("s", column_key.clone())
        .build()
        .unwrap();

    // Read with only the footer key and the key for one column
    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .with_column_key("x", column_key.clone())
        .build()
        .unwrap();

    // Reader can read plaintext stats from the unencrypted column z
    // and column x for which the key is provided, but not columns y and s
    // for which no key is provided.
    write_and_read_stats(
        encryption_properties,
        Some(decryption_properties),
        &[true, false, true, false],
    );
}

#[test]
pub fn test_column_statistics_with_encrypted_footer_and_uniform_encryption() {
    let footer_key = AES_128_FOOTER_KEY.to_vec();

    // Encrypt with an encrypted footer and uniform encryption
    let encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_plaintext_footer(false)
        .build()
        .unwrap();

    // Read with the footer key
    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .build()
        .unwrap();

    // Reader can read stats for all columns.
    write_and_read_stats(
        encryption_properties,
        Some(decryption_properties),
        &[true, true, true, true],
    );
}

/// Write a file with encryption and then verify whether statistics are readable with the provided decryption properties.
fn write_and_read_stats(
    encryption_properties: Arc<FileEncryptionProperties>,
    decryption_properties: Option<Arc<FileDecryptionProperties>>,
    expect_stats: &[bool],
) {
    use parquet::basic::Type;

    let int_values = Int32Array::from(vec![8, 3, 4, 19, 5]);
    let int_values = Arc::new(int_values);
    let string_values: StringArray = vec![
        None,
        Some("parquet"),
        Some("encryption"),
        Some("test"),
        None,
    ]
    .into();
    let string_values = Arc::new(string_values);

    let schema = Arc::new(Schema::new(vec![
        Field::new("x", int_values.data_type().clone(), true),
        Field::new("y", int_values.data_type().clone(), true),
        Field::new("z", int_values.data_type().clone(), true),
        Field::new("s", string_values.data_type().clone(), true),
    ]));
    let record_batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                int_values.clone(),
                int_values.clone(),
                int_values.clone(),
                string_values.clone(),
            ],
        )
        .unwrap(),
    ];

    let props = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .set_bloom_filter_enabled(true)
        .build();

    let temp_file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&temp_file, schema.clone(), Some(props)).unwrap();
    for batch in record_batches.clone() {
        writer.write(&batch).unwrap();
    }
    let metadata = writer.close().unwrap();

    let expected_min = 3i32.to_le_bytes();
    let expected_max = 19i32.to_le_bytes();

    let check_column_stats = |column: &ColumnChunkMetaData, expect_stats: bool| {
        let is_byte_array = column.column_type() == Type::BYTE_ARRAY;
        if expect_stats {
            assert!(column.page_encoding_stats().is_some());
            assert!(column.statistics().is_some());
            if is_byte_array {
                // Size statistics for BYTE_ARRAY columns
                assert!(column.unencoded_byte_array_data_bytes().is_some());
            } else {
                let column_stats = column.statistics().unwrap();
                assert_eq!(column_stats.min_bytes_opt(), Some(expected_min.as_slice()));
                assert_eq!(column_stats.max_bytes_opt(), Some(expected_max.as_slice()));
            }
            assert!(column.bloom_filter_offset().is_some());
            assert!(column.bloom_filter_length().is_some());
        } else {
            assert!(column.statistics().is_none());
            assert!(column.page_encoding_stats().is_none());
            assert!(column.bloom_filter_offset().is_none());
            assert!(column.bloom_filter_length().is_none());
            // Size statistics should also be stripped
            if is_byte_array {
                assert!(column.unencoded_byte_array_data_bytes().is_none());
            }
        }
    };

    // Check column statistics produced at write time are available in full
    let row_group = metadata.row_group(0);
    for column in row_group.columns().iter() {
        check_column_stats(column, true);
    }

    // Verify the presence or not of statistics per-column when reading with the provided decryption properties
    let mut options = ArrowReaderOptions::default().with_encoding_stats_as_mask(false);
    if let Some(decryption_properties) = decryption_properties {
        options = options.with_file_decryption_properties(decryption_properties);
    }
    let reader_metadata = ArrowReaderMetadata::load(&temp_file, options).unwrap();
    let metadata = reader_metadata.metadata();
    let row_group = metadata.row_group(0);

    for (column, stats_expected) in row_group.columns().iter().zip(expect_stats) {
        check_column_stats(column, *stats_expected);
    }
}

#[test]
fn test_write_uniform_encryption() {
    fn write_uniform_encryption(footer_key: &[u8]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "uniform_encryption.parquet.encrypted",
        );
        let file = File::open(path).unwrap();

        let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
            .build()
            .unwrap();

        let file_encryption_properties = FileEncryptionProperties::builder(footer_key.to_vec())
            .build()
            .unwrap();

        read_and_roundtrip_to_encrypted_file(
            &file,
            decryption_properties,
            file_encryption_properties,
        );
    }

    write_uniform_encryption(AES_128_FOOTER_KEY);
    write_uniform_encryption(AES_256_FOOTER_KEY);
}

#[test]
fn test_write_non_uniform_encryption_column_missmatch() {
    fn write_non_uniform_encryption_column_missmatch(
        footer_key: &[u8],
        column_keys: &[(&str, &[u8])],
        encryption_column_keys: &[(&str, &[u8])],
    ) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer.parquet.encrypted",
        );

        let mut decryption_builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            decryption_builder = decryption_builder.with_column_key(column_name, key.to_vec());
        }

        let decryption_properties = decryption_builder.build().unwrap();

        let mut file_encryption_builder = FileEncryptionProperties::builder(footer_key.to_vec())
            .with_column_key("other_field", encryption_column_keys[0].1.to_vec())
            .with_column_key("yet_another_field", encryption_column_keys[1].1.to_vec());

        for (column_name, key) in encryption_column_keys {
            file_encryption_builder =
                file_encryption_builder.with_column_key(column_name, key.to_vec());
        }

        let file_encryption_properties = file_encryption_builder.build().unwrap();

        let temp_file = tempfile::tempfile().unwrap();

        // read example data
        let file = File::open(path).unwrap();
        let options = ArrowReaderOptions::default()
            .with_file_decryption_properties(decryption_properties.clone());
        let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
        let props = WriterProperties::builder()
            .with_file_encryption_properties(file_encryption_properties)
            .build();

        let result = ArrowWriter::try_new(
            temp_file.try_clone().unwrap(),
            metadata.schema().clone(),
            Some(props),
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet error: The following columns with encryption keys specified were not found in the schema: other_field, yet_another_field"
        );
    }

    write_non_uniform_encryption_column_missmatch(
        AES_128_FOOTER_KEY,
        AES_128_COLUMN_NAME_KEYS,
        AES_128_COLUMN_NAME_KEYS,
    );

    write_non_uniform_encryption_column_missmatch(
        AES_256_FOOTER_KEY,
        AES_256_COLUMN_NAME_KEYS,
        AES_256_COLUMN_NAME_KEYS,
    );
}

#[test]
fn test_write_encrypted_column() {
    let message_type = "
            message test_schema {
                OPTIONAL BYTE_ARRAY a (UTF8);
            }
        ";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let data = vec![ByteArray::from(b"parquet".to_vec()); 7];
    let def_levels = [1, 1, 1, 1, 0, 1, 0, 1, 0, 1];

    let num_row_groups = 3;
    let num_batches = 3;
    let rows_per_batch = def_levels.len();
    let valid_rows_per_batch = def_levels.iter().filter(|&level| *level > 0).count();

    let file: File = tempfile::tempfile().unwrap();

    let builder = WriterProperties::builder();
    let footer_key: &[u8] = AES_128_FOOTER_KEY;
    let file_encryption_properties = FileEncryptionProperties::builder(footer_key.to_vec())
        .build()
        .unwrap();

    let props = Arc::new(
        builder
            .with_file_encryption_properties(file_encryption_properties)
            .set_data_page_row_count_limit(rows_per_batch)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(&file, schema, props).unwrap();
    for _ in 0..num_row_groups {
        let mut row_group_writer = writer.next_row_group().unwrap();
        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();

        for _ in 0..num_batches {
            col_writer
                .typed::<ByteArrayType>()
                .write_batch(&data, Some(&def_levels), None)
                .unwrap();
        }

        col_writer.close().unwrap();
        row_group_writer.close().unwrap();
    }

    let _file_metadata = writer.close().unwrap();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .build()
        .unwrap();
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties.clone());
    let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
    let file_metadata = metadata.metadata().file_metadata();

    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    let record_reader = builder.build().unwrap();

    assert_eq!(
        file_metadata.num_rows(),
        (num_row_groups * num_batches * rows_per_batch) as i64
    );
    assert_eq!(file_metadata.schema_descr().num_columns(), 1);

    assert_eq!(metadata.metadata().num_row_groups(), num_row_groups);
    metadata.metadata().row_groups().iter().for_each(|rg| {
        assert_eq!(rg.num_columns(), 1);
        assert_eq!(rg.num_rows(), (num_batches * rows_per_batch) as i64);
    });

    let mut row_count = 0;
    for batch in record_reader {
        let batch = batch.unwrap();
        row_count += batch.num_rows();

        let string_col = batch.column(0).as_string_opt::<i32>().unwrap();

        let mut valid_count = 0;
        for x in string_col.iter().flatten() {
            valid_count += 1;
            assert_eq!(x, "parquet");
        }
        assert_eq!(
            valid_count,
            valid_rows_per_batch * num_batches * num_row_groups
        );
    }

    assert_eq!(row_count, file_metadata.num_rows() as usize);
}

#[test]
fn test_write_encrypted_struct_field() {
    let int_32: Int32Array = [Some(1), Some(6)].iter().collect();
    let float_64: Float64Array = [None, Some(8.5)].iter().collect();
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("int64_col", DataType::Int32, true)),
            Arc::new(int_32) as ArrayRef,
        ),
        (
            Arc::new(Field::new("float64_col", DataType::Float64, true)),
            Arc::new(float_64) as ArrayRef,
        ),
    ]);
    let struct_array_data = Arc::new(struct_array);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "struct_col",
        struct_array_data.data_type().clone(),
        true,
    )]));
    let record_batches =
        vec![RecordBatch::try_new(schema.clone(), vec![struct_array_data]).unwrap()];

    let temp_file = tempfile::tempfile().unwrap();

    // When configuring encryption keys for struct columns,
    // keys need to be specified for each leaf-level Parquet column using the full "." separated
    // column path.
    let builder = WriterProperties::builder();
    let footer_key = AES_128_FOOTER_KEY.to_vec();
    let column_key_1 = AES_128_COLUMN_KEYS[0].to_vec();
    let column_key_2 = AES_128_COLUMN_KEYS[1].to_vec();
    let file_encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_column_key("struct_col.int64_col", column_key_1.clone())
        .with_column_key("struct_col.float64_col", column_key_2.clone())
        .build()
        .unwrap();

    let props = builder
        .with_file_encryption_properties(file_encryption_properties)
        .build();
    let mut writer =
        ArrowWriter::try_new(temp_file.try_clone().unwrap(), schema, Some(props)).unwrap();
    for batch in record_batches.clone() {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    let decryption_properties = FileDecryptionProperties::builder(footer_key)
        .with_column_key("struct_col.int64_col", column_key_1)
        .with_column_key("struct_col.float64_col", column_key_2)
        .build()
        .unwrap();
    let options =
        ArrowReaderOptions::default().with_file_decryption_properties(decryption_properties);

    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(temp_file, options).unwrap();
    let record_reader = builder.build().unwrap();

    let read_record_reader = record_reader
        .map(|x| x.unwrap())
        .collect::<Vec<RecordBatch>>();

    // show read batches are equal to written batches
    assert_eq!(read_record_reader.len(), record_batches.len());
    for (read_batch, written_batch) in read_record_reader.iter().zip(record_batches.iter()) {
        assert_eq!(read_batch.num_columns(), written_batch.num_columns());
        assert_eq!(read_batch.num_rows(), written_batch.num_rows());
        for (read_column, written_column) in read_batch
            .columns()
            .iter()
            .zip(written_batch.columns().iter())
        {
            assert_eq!(read_column, written_column);
        }
    }
}

/// Test that when per-column encryption is used,
/// unencrypted row group metadata are returned when the writer is closed
/// and statistics can be used.
#[test]
pub fn test_retrieve_row_group_statistics_after_encrypted_write() {
    let values = Int32Array::from(vec![8, 3, 4, 19, 5]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        values.data_type().clone(),
        true,
    )]));
    let values = Arc::new(values);
    let record_batches = vec![RecordBatch::try_new(schema.clone(), vec![values]).unwrap()];

    let temp_file = tempfile::tempfile().unwrap();

    let footer_key = AES_128_FOOTER_KEY.to_vec();
    let column_key = AES_128_COLUMN_KEYS[0].to_vec();
    let file_encryption_properties = FileEncryptionProperties::builder(footer_key.clone())
        .with_column_key("x", column_key.clone())
        .build()
        .unwrap();

    let props = WriterProperties::builder()
        .with_file_encryption_properties(file_encryption_properties)
        .build();
    let mut writer = ArrowWriter::try_new(temp_file, schema, Some(props)).unwrap();

    for batch in record_batches.clone() {
        writer.write(&batch).unwrap();
    }
    let file_metadata = writer.close().unwrap();

    assert_eq!(file_metadata.num_row_groups(), 1);
    let row_group = file_metadata.row_group(0);
    assert_eq!(row_group.num_columns(), 1);
    let column = row_group.column(0);
    let column_stats = column.statistics().unwrap();
    assert_eq!(
        column_stats.min_bytes_opt(),
        Some(3i32.to_le_bytes().as_slice())
    );
    assert_eq!(
        column_stats.max_bytes_opt(),
        Some(19i32.to_le_bytes().as_slice())
    );
}

#[test]
fn test_decrypt_page_index_uniform() {
    fn decrypt_page_index_uniform(footer_key: &[u8]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "uniform_encryption.parquet.encrypted",
        );
        let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
            .build()
            .unwrap();

        test_decrypt_page_index(&path, decryption_properties).unwrap();
    }

    decrypt_page_index_uniform(AES_128_FOOTER_KEY);
    decrypt_page_index_uniform(AES_256_FOOTER_KEY);
}

#[test]
fn test_decrypt_page_index_non_uniform() {
    fn decrypt_page_index_non_uniform(footer_key: &[u8], column_keys: &[(&str, &[u8])]) {
        let path = encryption_util::encrypted_data_path(
            footer_key,
            "encrypt_columns_and_footer.parquet.encrypted",
        );
        let mut builder = FileDecryptionProperties::builder(footer_key.to_vec());
        for (column_name, key) in column_keys {
            builder = builder.with_column_key(column_name, key.to_vec());
        }
        let decryption_properties = builder.build().unwrap();
        test_decrypt_page_index(&path, decryption_properties).unwrap();
    }

    decrypt_page_index_non_uniform(AES_128_FOOTER_KEY, AES_128_COLUMN_NAME_KEYS);

    decrypt_page_index_non_uniform(AES_256_FOOTER_KEY, AES_256_COLUMN_NAME_KEYS);
}

fn test_decrypt_page_index(
    path: &str,
    decryption_properties: Arc<FileDecryptionProperties>,
) -> Result<(), ParquetError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::default()
        .with_file_decryption_properties(decryption_properties)
        .with_page_index_policy(PageIndexPolicy::from(true));

    let arrow_metadata = ArrowReaderMetadata::load(&file, options)?;

    verify_column_indexes(arrow_metadata.metadata());

    Ok(())
}

#[test]
fn test_decryption_properties_uses_key_retriever() {
    let key_retriever = TestKeyRetriever::new()
        .with_key(
            AES_128_FOOTER_KEY_NAME.to_owned(),
            AES_128_FOOTER_KEY.to_vec(),
        )
        .with_key(
            AES_128_KEY_NAMES[0].to_owned(),
            AES_128_COLUMN_KEYS[0].to_vec(),
        );

    let properties_with_retriever =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    assert!(properties_with_retriever.uses_key_retriever());

    let properties_with_keys = FileDecryptionProperties::builder(AES_128_FOOTER_KEY.to_vec())
        .with_column_key(AES_128_COLUMN_NAMES[0], AES_128_COLUMN_KEYS[0].to_vec())
        .build()
        .unwrap();

    assert!(!properties_with_keys.uses_key_retriever());

    let uniform_properties = FileDecryptionProperties::builder(AES_128_FOOTER_KEY.to_vec())
        .build()
        .unwrap();

    assert!(!uniform_properties.uses_key_retriever());
}
