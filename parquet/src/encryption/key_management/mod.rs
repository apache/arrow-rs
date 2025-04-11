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

//! Encryption Key Management Tools for Parquet
//!
//! This module provides tools for integrating with a Key Management Server (KMS)
//! to read and write encrypted Parquet files.
//!
//! Envelope encryption is used, where the Parquet file is encrypted with data encryption keys
//! (DEKs) that are randomly generated per file,
//! and the DEKs are encrypted with master encryption keys (MEKs) that are managed by a KMS.
//! Double wrapping is used by default, where the DEKs are first encrypted with key encryption
//! keys (KEKs) that are then encrypted with MEKs, to reduce KMS interactions.
//!
//! Using this module requires defining your own type that implements the
//! [`KmsClient`](kms::KmsClient) trait and interacts with your organization's KMS.
//!
//! This `KmsClient` can then be used by the
//! [`CryptoFactory`](crypto_factory::CryptoFactory) type to generate
//! [`FileEncryptionProperties`](crate::encryption::encrypt::FileEncryptionProperties)
//! for writing encrypted Parquet files and
//! [`FileDecryptionProperties`](crate::encryption::decrypt::FileDecryptionProperties)
//! for reading files.
//!
//! The encryption key metadata that is stored in the Parquet file is compatible with other Parquet
//! implementations (PyArrow and parquet-java for example), so that files encrypted with this
//! module may be decrypted by those implementations, and vice versa, as long as the
//! `KmsClient` implementations are compatible.
//!
//! # Example of writing then reading an encrypted Parquet file
//! ```
//! use arrow_array::{ArrayRef, Float32Array, Int32Array, RecordBatch};
//! use base64::prelude::BASE64_STANDARD;
//! use base64::Engine;
//! use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
//! use parquet::arrow::ArrowWriter;
//! use parquet::encryption::key_management::crypto_factory::{
//!     CryptoFactory, DecryptionConfiguration, EncryptionConfigurationBuilder,
//! };
//! use parquet::encryption::key_management::kms::{KmsClient, KmsConnectionConfig};
//! use parquet::errors::{ParquetError, Result};
//! use parquet::file::properties::WriterProperties;
//! use ring::aead::{Aad, LessSafeKey, UnboundKey, AES_128_GCM, NONCE_LEN};
//! use ring::rand::{SecureRandom, SystemRandom};
//! use std::collections::HashMap;
//! use std::fs::File;
//! use std::sync::Arc;
//! use tempfile::TempDir;
//!
//! let temp_dir = TempDir::new()?;
//! let file_path = temp_dir.path().join("encrypted_example.parquet");
//!
//! // Create a CryptoFactory, providing a factory function
//! // that will create an example KMS client
//! let crypto_factory = CryptoFactory::new(DemoKmsClient::create);
//!
//! // Specify any options required to connect to our KMS.
//! // These are ignored by the DemoKmsClient but shown here for illustration.
//! // The KMS instance ID and URL will be stored in the Parquet encryption metadata
//! // so don't need to be specified if you are only reading files.
//! let connection_config = Arc::new(
//!     KmsConnectionConfig::builder()
//!         .set_kms_instance_id("kms1".into())
//!         .set_kms_instance_url("https://example.com/kms".into())
//!         .set_key_access_token("secret_token".into())
//!         .set_custom_kms_conf_option("custom_option".into(), "some_value".into())
//!         .build(),
//! );
//!
//! // Create an encryption configuration that will encrypt the footer with the "kf" key,
//! // the "x" column with the "kc1" key, and the "y" column with the "kc2" key,
//! // while leaving the "id" column unencrypted.
//! let encryption_config = EncryptionConfigurationBuilder::new("kf".into())
//!     .add_column_key("kc1".into(), vec!["x".into()])
//!     .add_column_key("kc2".into(), vec!["y".into()])
//!     .build();
//!
//! // Use the CryptoFactory to generate file encryption properties using the configuration
//! let encryption_properties =
//!     crypto_factory.file_encryption_properties(connection_config.clone(), &encryption_config)?;
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
//!         ("id", Arc::new(ids) as ArrayRef),
//!         ("x", Arc::new(x_vals) as ArrayRef),
//!         ("y", Arc::new(y_vals) as ArrayRef),
//!     ])?;
//!
//!     let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_properties))?;
//!
//!     writer.write(&batch)?;
//!     writer.close()?;
//! }
//!
//! // Use the CryptoFactory to generate file decryption properties.
//! // We don't need to specify which columns are encrypted and which keys are used,
//! // that information is stored in the file metadata.
//! let decryption_config = DecryptionConfiguration::default();
//! let decryption_properties =
//!     crypto_factory.file_decryption_properties(connection_config, decryption_config)?;
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
//!
//! /// Example KMS client that uses in-memory AES keys.
//! /// A real KMS client should interact with a Key Management Server to encrypt and decrypt keys.
//! pub struct DemoKmsClient {
//!     key_map: HashMap<String, Vec<u8>>,
//! }
//!
//! impl DemoKmsClient {
//!     pub fn create(_config: &KmsConnectionConfig) -> Result<Arc<dyn KmsClient>> {
//!         let mut key_map = HashMap::default();
//!         key_map.insert("kf".into(), "0123456789012345".into());
//!         key_map.insert("kc1".into(), "1234567890123450".into());
//!         key_map.insert("kc2".into(), "1234567890123451".into());
//!
//!         Ok(Arc::new(Self { key_map }))
//!     }
//!
//!     /// Get the AES key corresponding to a key identifier
//!     fn get_key(&self, master_key_identifier: &str) -> Result<LessSafeKey> {
//!         let key = self.key_map.get(master_key_identifier).ok_or_else(|| {
//!             ParquetError::General(format!("Invalid master key '{master_key_identifier}'"))
//!         })?;
//!         let key = UnboundKey::new(&AES_128_GCM, key)
//!             .map_err(|e| ParquetError::General(format!("Error creating AES key '{e}'")))?;
//!         Ok(LessSafeKey::new(key))
//!     }
//! }
//!
//! impl KmsClient for DemoKmsClient {
//!     /// Take a randomly generated key and encrypt it using the specified master key
//!     fn wrap_key(&self, key_bytes: &[u8], master_key_identifier: &str) -> Result<String> {
//!         let master_key = self.get_key(master_key_identifier)?;
//!         let aad = master_key_identifier.as_bytes();
//!         let rng = SystemRandom::new();
//!
//!         let mut nonce = [0u8; NONCE_LEN];
//!         rng.fill(&mut nonce)?;
//!         let nonce = ring::aead::Nonce::assume_unique_for_key(nonce);
//!
//!         let tag_len = master_key.algorithm().tag_len();
//!         let mut ciphertext = Vec::with_capacity(NONCE_LEN + key_bytes.len() + tag_len);
//!         ciphertext.extend_from_slice(nonce.as_ref());
//!         ciphertext.extend_from_slice(key_bytes);
//!         let tag = master_key.seal_in_place_separate_tag(
//!             nonce,
//!             Aad::from(aad),
//!             &mut ciphertext[NONCE_LEN..],
//!         )?;
//!         ciphertext.extend_from_slice(tag.as_ref());
//!         let encoded = BASE64_STANDARD.encode(&ciphertext);
//!
//!         Ok(encoded)
//!     }
//!
//!     /// Take an encrypted key and decrypt it using the specified master key identifier
//!     fn unwrap_key(&self, wrapped_key: &str, master_key_identifier: &str) -> Result<Vec<u8>> {
//!         let wrapped_key = BASE64_STANDARD.decode(wrapped_key).map_err(|e| {
//!             ParquetError::General(format!("Error base64 decoding wrapped key: {e}"))
//!         })?;
//!         let master_key = self.get_key(master_key_identifier)?;
//!         let aad = master_key_identifier.as_bytes();
//!         let nonce = ring::aead::Nonce::try_assume_unique_for_key(&wrapped_key[..NONCE_LEN])?;
//!
//!         let mut plaintext = Vec::with_capacity(wrapped_key.len() - NONCE_LEN);
//!         plaintext.extend_from_slice(&wrapped_key[NONCE_LEN..]);
//!
//!         master_key.open_in_place(nonce, Aad::from(aad), &mut plaintext)?;
//!         plaintext.resize(plaintext.len() - master_key.algorithm().tag_len(), 0u8);
//!
//!         Ok(plaintext)
//!     }
//! }
//!
//! # Ok::<(), parquet::errors::ParquetError>(())
//! ```

pub mod crypto_factory;
mod key_encryption;
mod key_material;
mod key_unwrapper;
mod key_wrapper;
pub mod kms;
mod kms_manager;
#[cfg(test)]
mod test_kms;
