// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use std::fs::File;

#[test]
fn test_read_without_encryption_enabled_fails() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let file = File::open(path).unwrap();

    let options = ArrowReaderOptions::default();
    let result = ArrowReaderMetadata::load(&file, options.clone());
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but the encryption feature is disabled"
    );
}

#[tokio::test]
#[cfg(feature = "async")]
async fn test_async_read_without_encryption_enabled_fails() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let mut file = tokio::fs::File::open(&path).await.unwrap();

    let options = ArrowReaderOptions::new();
    let result = ArrowReaderMetadata::load_async(&mut file, options).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but the encryption feature is disabled"
    );
}

#[test]
#[cfg(feature = "snap")]
fn test_plaintext_footer_read_without_decryption() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties();
}

#[tokio::test]
#[cfg(all(feature = "async", feature = "snap"))]
async fn test_plaintext_footer_read_without_decryption_async() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties_async()
        .await;
}
