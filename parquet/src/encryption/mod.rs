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

//! This module implements Parquet Modular Encryption, as described in the
//! [specification](https://github.com/apache/parquet-format/blob/master/Encryption.md).
//!
//! # Example of writing and reading an encrypted Parquet file
//!
//! ```
//! use arrow::array::{ArrayRef, Float32Array, Int32Array, RecordBatch};
//! use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
//! use parquet::arrow::ArrowWriter;
//! use parquet::encryption::decrypt::FileDecryptionProperties;
//! use parquet::encryption::encrypt::FileEncryptionProperties;
//! use parquet::errors::Result;
//! use parquet::file::properties::WriterProperties;
//! use std::fs::File;
//! use std::sync::Arc;
//! use tempfile::TempDir;
//!
//! // Define 16 byte AES encryption keys to use.
//! static FOOTER_KEY: &[u8; 16] = b"0123456789012345";
//! static COLUMN_KEY_1: &[u8; 16] = b"1234567890123450";
//! static COLUMN_KEY_2: &[u8; 16] = b"1234567890123451";
//!
//! let temp_dir = TempDir::new()?;
//! let file_path = temp_dir.path().join("encrypted_example.parquet");
//!
//! // Create file encryption properties, which define how the file is encrypted.
//! // We will specify a key to encrypt the footer metadata,
//! // then separate keys for different columns.
//! // This allows fine-grained control of access to different columns within a Parquet file.
//! // Note that any columns without an encryption key specified will be left un-encrypted.
//! // If only a footer key is specified, then all columns are encrypted with the footer key.
//! let encryption_properties = FileEncryptionProperties::builder(FOOTER_KEY.into())
//!     .with_column_key("x", COLUMN_KEY_1.into())
//!     .with_column_key("y", COLUMN_KEY_2.into())
//!     // We also set an AAD prefix, which is optional.
//!     // This contributes to the "additional authenticated data" that is used to verify file
//!     // integrity and prevents data being swapped with data encrypted with the same key.
//!     .with_aad_prefix(b"example_aad".into())
//!     // Specify that the AAD prefix is stored in the file, so readers don't need
//!     // to provide it to read the data, but can optionally provide it if they want to
//!     // verify file integrity.
//!     .with_aad_prefix_storage(true)
//!     .build()?;
//!
//! let writer_properties = WriterProperties::builder()
//!     .with_file_encryption_properties(encryption_properties)
//!     .build();
//!
//! // Write the encrypted Parquet file
//! {
//!     let file = File::create(&file_path)?;
//!
//!     let ids = Int32Array::from(vec![0, 1, 2, 3, 4, 5]);
//!     let x_vals = Float32Array::from(vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5]);
//!     let y_vals = Float32Array::from(vec![1.0, 1.1, 1.2, 1.3, 1.4, 1.5]);
//!     let batch = RecordBatch::try_from_iter(vec![
//!       ("id", Arc::new(ids) as ArrayRef),
//!       ("x", Arc::new(x_vals) as ArrayRef),
//!       ("y", Arc::new(y_vals) as ArrayRef),
//!     ])?;
//!
//!     let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_properties))?;
//!
//!     writer.write(&batch)?;
//!     writer.close()?;
//! }
//!
//! // In order to read the encrypted Parquet file, we need to know the encryption
//! // keys used to encrypt it.
//! // We don't need to provide the AAD prefix as it was stored in the file metadata,
//! // but we could specify it here if we wanted to verify the file hasn't been tampered with:
//! let decryption_properties = FileDecryptionProperties::builder(FOOTER_KEY.into())
//!     .with_column_key("x", COLUMN_KEY_1.into())
//!     .with_column_key("y", COLUMN_KEY_2.into())
//!     .build()?;
//!
//! let reader_options =
//!     ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
//!
//! // Read the file using the configured decryption properties
//! let file = File::open(&file_path)?;
//!
//! let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, reader_options)?;
//! let record_reader = builder.build()?;
//! for batch in record_reader {
//!     let batch = batch?;
//!     println!("Read batch: {batch:?}");
//! }
//! # Ok::<(), parquet::errors::ParquetError>(())
//! ```

pub(crate) mod ciphers;
pub mod decrypt;
pub mod encrypt;
pub(crate) mod modules;
