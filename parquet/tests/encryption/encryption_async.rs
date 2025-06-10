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

use crate::encryption_util::{
    verify_column_indexes, verify_encryption_test_data, TestKeyRetriever,
};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
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
    check_for_error(
        "Parquet error: No column decryption key set for encrypted column 'double_field'",
        &path,
        footer_key,
        "".as_bytes(),
        column_2_key,
    )
    .await;

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
    check_for_error(
        "Parquet error: Unable to decrypt column 'double_field', perhaps the column key is wrong?",
        &path,
        footer_key,
        "1123456789012345".as_bytes(),
        column_2_key,
    )
    .await;

    // Mixed up keys
    check_for_error(
        "Parquet error: Unable to decrypt column 'float_field', perhaps the column key is wrong?",
        &path,
        footer_key,
        column_2_key,
        column_1_key,
    )
    .await;
}

#[tokio::test]
#[cfg(feature = "snap")]
async fn test_plaintext_footer_read_without_decryption() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties_async()
        .await;
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
        "Parquet error: Parquet file has an encrypted footer but decryption properties were not provided"
    );
}

#[tokio::test]
async fn test_write_non_uniform_encryption() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");

    let footer_key = b"0123456789012345".to_vec(); // 128bit/16
    let column_names = vec!["double_field", "float_field"];
    let column_keys = vec![b"1234567890123450".to_vec(), b"1234567890123451".to_vec()];

    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .with_column_keys(column_names.clone(), column_keys.clone())
        .unwrap()
        .build()
        .unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(footer_key)
        .with_column_keys(column_names, column_keys)
        .unwrap()
        .build()
        .unwrap();

    read_and_roundtrip_to_encrypted_file_async(
        &path,
        decryption_properties,
        file_encryption_properties,
    )
    .await
    .unwrap();
}

#[cfg(feature = "object_store")]
async fn get_encrypted_meta_store() -> (
    object_store::ObjectMeta,
    std::sync::Arc<dyn object_store::ObjectStore>,
) {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use std::sync::Arc;
    let test_data = arrow::util::test_util::parquet_test_data();
    let store = LocalFileSystem::new_with_prefix(test_data).unwrap();

    let meta = store
        .head(&Path::from("uniform_encryption.parquet.encrypted"))
        .await
        .unwrap();

    (meta, Arc::new(store) as Arc<dyn ObjectStore>)
}

#[tokio::test]
#[cfg(feature = "object_store")]
async fn test_read_encrypted_file_from_object_store() {
    use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
    let (meta, store) = get_encrypted_meta_store().await;

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);

    let mut reader = ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
    let metadata = reader.get_metadata(Some(&options)).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap();
    let batch_stream = builder.build().unwrap();
    let record_batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    verify_encryption_test_data(record_batches, &metadata);
}

#[tokio::test]
async fn test_non_uniform_encryption_plaintext_footer_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever = TestKeyRetriever::new()
        .with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec())
        .with_key("kc1".to_owned(), "1234567890123450".as_bytes().to_vec())
        .with_key("kc2".to_owned(), "1234567890123451".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_non_uniform_encryption_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever = TestKeyRetriever::new()
        .with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec())
        .with_key("kc1".to_owned(), "1234567890123450".as_bytes().to_vec())
        .with_key("kc2".to_owned(), "1234567890123451".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_uniform_encryption_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever =
        TestKeyRetriever::new().with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_decrypt_page_index_uniform() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();

    test_decrypt_page_index(&path, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_decrypt_page_index_non_uniform() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");

    let footer_key = "0123456789012345".as_bytes().to_vec();
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    test_decrypt_page_index(&path, decryption_properties)
        .await
        .unwrap();
}

async fn test_decrypt_page_index(
    path: &str,
    decryption_properties: FileDecryptionProperties,
) -> Result<(), ParquetError> {
    let mut file = File::open(&path).await?;

    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(decryption_properties)
        .with_page_index(true);

    let arrow_metadata = ArrowReaderMetadata::load_async(&mut file, options).await?;

    verify_column_indexes(arrow_metadata.metadata());

    Ok(())
}

async fn verify_encryption_test_file_read_async(
    file: &mut tokio::fs::File,
    decryption_properties: FileDecryptionProperties,
) -> Result<(), ParquetError> {
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);

    let arrow_metadata = ArrowReaderMetadata::load_async(file, options).await?;
    let metadata = arrow_metadata.metadata();

    let record_reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        arrow_metadata.clone(),
    )
    .build()?;
    let record_batches = record_reader.try_collect::<Vec<_>>().await?;

    verify_encryption_test_data(record_batches, metadata);
    Ok(())
}

async fn read_and_roundtrip_to_encrypted_file_async(
    path: &str,
    decryption_properties: FileDecryptionProperties,
    encryption_properties: FileEncryptionProperties,
) -> Result<(), ParquetError> {
    let temp_file = tempfile::tempfile().unwrap();
    let mut file = File::open(&path).await.unwrap();

    let options =
        ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties.clone());
    let arrow_metadata = ArrowReaderMetadata::load_async(&mut file, options).await?;
    let record_reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        arrow_metadata.clone(),
    )
    .build()?;
    let record_batches = record_reader.try_collect::<Vec<_>>().await?;

    let props = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .build();
    let options = ArrowWriterOptions::new().with_properties(props);

    let file = tokio::fs::File::from_std(temp_file.try_clone().unwrap());
    let mut writer =
        AsyncArrowWriter::try_new_with_options(file, arrow_metadata.schema().clone(), options)
            .unwrap();
    for batch in record_batches {
        writer.write(&batch).await.unwrap();
    }
    writer.close().await.unwrap();

    let mut file = tokio::fs::File::from_std(temp_file.try_clone().unwrap());
    verify_encryption_test_file_read_async(&mut file, decryption_properties).await
}
