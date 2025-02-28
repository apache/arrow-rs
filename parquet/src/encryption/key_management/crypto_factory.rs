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

use crate::encryption::decrypt::FileDecryptionProperties;
use crate::encryption::key_management::key_unwrapper::KeyUnwrapper;
use crate::encryption::key_management::key_wrapper::KeyWrapper;
use crate::encryption::key_management::kms::{KmsClientFactory, KmsConnectionConfig};
use crate::encryption::key_management::kms_manager::KmsManager;
use crate::errors::{ParquetError, Result};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Parquet encryption algorithms
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncryptionAlgorithm {
    /// AES-GCM version 1, where all metadata and data pages are encrypted with AES-GCM
    AesGcmV1,
    /// AES-GCM-CTR version 1, where metadata is encrypted with AES-GCM, and data pages
    /// are encrypted with AES-CTR
    AesGcmCtrV1,
}

/// Configuration for encrypting a Parquet file
#[derive(Debug)]
pub struct EncryptionConfiguration {
    footer_key: String,
    column_keys: HashMap<String, Vec<String>>,
    encryption_algorithm: EncryptionAlgorithm,
    plaintext_footer: bool,
    double_wrapping: bool,
    cache_lifetime: Option<Duration>,
    internal_key_material: bool,
    data_key_length_bits: u32,
}

impl EncryptionConfiguration {
    /// Master key identifier for footer key encryption or signing
    pub fn footer_key(&self) -> &str {
        &self.footer_key
    }

    /// Map from master key identifiers to the column paths encrypted with a column
    pub fn column_keys(&self) -> &HashMap<String, Vec<String>> {
        &self.column_keys
    }

    /// The encryption algorithm to use
    pub fn encryption_algorithm(&self) -> EncryptionAlgorithm {
        self.encryption_algorithm
    }

    /// Whether to write the footer in plaintext.
    pub fn plaintext_footer(&self) -> bool {
        self.plaintext_footer
    }

    /// Whether to use double wrapping, where data encryption keys (DEKs) are wrapped
    /// with key encryption keys (KEKs), which are then wrapped with the KMS.
    /// This allows reducing interactions with the KMS.
    pub fn double_wrapping(&self) -> bool {
        self.double_wrapping
    }

    /// How long to cache objects for, including decrypted key encryption keys
    /// and KMS clients. When None, clients are cached indefinitely.
    pub fn cache_lifetime(&self) -> Option<Duration> {
        self.cache_lifetime
    }

    /// Whether to store encryption key material inside Parquet file metadata,
    /// rather than in external JSON files.
    /// Using external key material allows for rotation of master keys.
    pub fn internal_key_material(&self) -> bool {
        self.internal_key_material
    }

    /// Number of bits for randomly generated data encryption keys.
    /// May be 128, 192 or 256.
    pub fn data_key_length_bits(&self) -> u32 {
        self.data_key_length_bits
    }
}

/// Builder for Parquet [`EncryptionConfiguration`].
pub struct EncryptionConfigurationBuilder {
    footer_key: String,
    column_keys: HashMap<String, Vec<String>>,
    encryption_algorithm: EncryptionAlgorithm,
    plaintext_footer: bool,
    double_wrapping: bool,
    cache_lifetime: Option<Duration>,
    internal_key_material: bool,
    data_key_length_bits: u32,
}

impl EncryptionConfigurationBuilder {
    /// Create a new `EncryptionConfigurationBuilder` with default options
    pub fn new(footer_key: String) -> Self {
        Self {
            footer_key,
            column_keys: Default::default(),
            encryption_algorithm: EncryptionAlgorithm::AesGcmV1,
            plaintext_footer: false,
            double_wrapping: false,
            cache_lifetime: Some(Duration::from_secs(600)),
            internal_key_material: true,
            data_key_length_bits: 128,
        }
    }

    /// Finalizes the encryption configuration to be used
    pub fn build(self) -> EncryptionConfiguration {
        EncryptionConfiguration {
            footer_key: self.footer_key,
            column_keys: self.column_keys,
            encryption_algorithm: self.encryption_algorithm,
            plaintext_footer: self.plaintext_footer,
            double_wrapping: self.double_wrapping,
            cache_lifetime: self.cache_lifetime,
            internal_key_material: self.internal_key_material,
            data_key_length_bits: self.data_key_length_bits,
        }
    }

    /// Specify a column master key identifier and the column names to be encrypted with this key.
    /// Note that if no column keys are specified, uniform encryption is used where all columns
    /// are encrypted with the footer key.
    pub fn add_column_key(mut self, master_key: String, column_paths: Vec<String>) -> Self {
        self.column_keys
            .entry(master_key)
            .or_default()
            .extend(column_paths);
        self
    }

    /// Set the encryption algorithm to use
    pub fn set_encryption_algorithm(mut self, algorithm: EncryptionAlgorithm) -> Self {
        self.encryption_algorithm = algorithm;
        self
    }

    /// Set whether to write the footer in plaintext.
    /// Defaults to false.
    pub fn set_plaintext_footer(mut self, plaintext_footer: bool) -> Self {
        self.plaintext_footer = plaintext_footer;
        self
    }

    /// Set whether to use double wrapping, where data encryption keys (DEKs) are wrapped
    /// with key encryption keys (KEKs), which are then wrapped with the KMS.
    /// This allows reducing interactions with the KMS.
    /// Defaults to True.
    pub fn set_double_wrapping(mut self, double_wrapping: bool) -> Self {
        self.double_wrapping = double_wrapping;
        self
    }

    /// Set how long to cache objects for, including decrypted key encryption keys
    /// and KMS clients. When None, clients are cached indefinitely.
    /// Defaults to 10 minutes.
    pub fn set_cache_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.cache_lifetime = lifetime;
        self
    }

    /// Set whether to store encryption key material inside Parquet file metadata,
    /// rather than in external JSON files.
    /// Using external key material allows for rotation of master keys.
    /// Defaults to False.
    pub fn set_internal_key_material(mut self, internal_key_material: bool) -> Self {
        self.internal_key_material = internal_key_material;
        self
    }

    /// Set the number of bits for randomly generated data encryption keys.
    /// May be 128, 192 or 256.
    pub fn set_data_key_length_bits(mut self, key_length: u32) -> Result<Self> {
        if ![128, 192, 256].contains(&key_length) {
            return Err(general_err!(
                "Invalid key length: {}. Expected 128, 192 or 256.",
                key_length
            ));
        }
        self.data_key_length_bits = key_length;
        Ok(self)
    }
}

// Temporary FileEncryptionProperties struct until low-level encryption is added
// TODO: Delete this
#[derive(Debug, Clone, PartialEq)]
pub struct FileEncryptionProperties {
    encrypt_footer: bool,
    footer_key: EncryptionKey,
    column_keys: Option<HashMap<String, EncryptionKey>>,
    aad_prefix: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncryptionKey {
    pub key: Vec<u8>,
    pub key_metadata: Vec<u8>,
}

/// Configuration for decrypting a Parquet file
#[derive(Debug)]
pub struct DecryptionConfiguration {
    cache_lifetime: Option<Duration>,
}

impl DecryptionConfiguration {
    /// How long to cache objects for, including decrypted key encryption keys
    /// and KMS clients. When None, no caching is used.
    pub fn cache_lifetime(&self) -> Option<Duration> {
        self.cache_lifetime
    }
}

impl Default for DecryptionConfiguration {
    fn default() -> Self {
        DecryptionConfigurationBuilder::new().build()
    }
}

/// Builder for Parquet [`EncryptionConfiguration`].
pub struct DecryptionConfigurationBuilder {
    cache_lifetime: Option<Duration>,
}

impl DecryptionConfigurationBuilder {
    /// Create a new `EncryptionConfigurationBuilder` with default options
    pub fn new() -> Self {
        Self {
            cache_lifetime: Some(Duration::from_secs(600)),
        }
    }

    /// Finalizes the decryption configuration to be used
    pub fn build(self) -> DecryptionConfiguration {
        DecryptionConfiguration {
            cache_lifetime: self.cache_lifetime,
        }
    }

    /// Set how long to cache objects for, including decrypted key encryption keys
    /// and KMS clients. When None, no caching is used.
    pub fn set_cache_lifetime(mut self, cache_lifetime: Option<Duration>) -> Self {
        self.cache_lifetime = cache_lifetime;
        self
    }
}

/// A factory that produces file decryption and encryption properties using
/// configuration options and a KMS client
pub struct CryptoFactory {
    kms_manager: Arc<KmsManager>,
}

impl CryptoFactory {
    /// Create a new CryptoFactory, providing a factory function for creating KMS clients
    pub fn new<T>(kms_client_factory: T) -> Self
    where
        T: KmsClientFactory + 'static,
    {
        CryptoFactory {
            kms_manager: Arc::new(KmsManager::new(kms_client_factory)),
        }
    }

    /// Create file decryption properties for a Parquet file
    pub fn file_decryption_properties(
        &self,
        kms_connection_config: Arc<RwLock<KmsConnectionConfig>>,
        decryption_configuration: DecryptionConfiguration,
    ) -> Result<FileDecryptionProperties> {
        let key_retriever = Arc::new(KeyUnwrapper::new(
            self.kms_manager.clone(),
            kms_connection_config,
            decryption_configuration,
        ));
        FileDecryptionProperties::with_key_retriever(key_retriever).build()
    }

    pub fn file_encryption_properties(
        &self,
        kms_connection_config: Arc<RwLock<KmsConnectionConfig>>,
        encryption_configuration: &EncryptionConfiguration,
    ) -> Result<FileEncryptionProperties> {
        if !encryption_configuration.internal_key_material {
            return Err(nyi_err!("External key material is not yet implemented"));
        }
        if encryption_configuration.encryption_algorithm != EncryptionAlgorithm::AesGcmV1 {
            return Err(nyi_err!(
                "Only the AES-GCM-V1 encryption algorithm is implemented"
            ));
        }
        if encryption_configuration.data_key_length_bits != 128 {
            return Err(nyi_err!("Only 128 bit data keys are currently implemented"));
        }

        let encrypt_footer = !encryption_configuration.plaintext_footer;
        let mut key_wrapper = KeyWrapper::new(
            self.kms_manager.clone(),
            kms_connection_config,
            encryption_configuration,
        );

        let footer_key = self.generate_key(
            encryption_configuration.footer_key(),
            true,
            &mut key_wrapper,
        )?;

        let column_keys = if encryption_configuration.column_keys.is_empty() {
            None
        } else {
            let mut column_keys = HashMap::default();
            for (master_key_id, column_paths) in &encryption_configuration.column_keys {
                for column_path in column_paths {
                    let column_key = self.generate_key(master_key_id, false, &mut key_wrapper)?;
                    column_keys.insert(column_path.clone(), column_key);
                }
            }
            Some(column_keys)
        };

        Ok(FileEncryptionProperties {
            encrypt_footer,
            footer_key,
            column_keys,
            aad_prefix: None,
        })
    }

    fn generate_key<'a>(
        &self,
        master_key_identifier: &str,
        is_footer_key: bool,
        key_wrapper: &'a mut KeyWrapper,
    ) -> Result<EncryptionKey> {
        let rng = SystemRandom::new();
        let mut key = vec![0u8; 16];
        rng.fill(&mut key)?;

        let key_metadata =
            key_wrapper.get_key_metadata(&key, master_key_identifier, is_footer_key)?;

        Ok(EncryptionKey { key, key_metadata })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::key_management::key_material::KeyMaterialBuilder;
    use crate::encryption::key_management::test_kms::TestKmsClientFactory;
    use std::sync::RwLock;

    #[test]
    fn test_file_decryption_properties() {
        let kms_config = Arc::new(RwLock::new(KmsConnectionConfig::new()));
        let config = Default::default();

        let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());
        let decryption_props = crypto_factory
            .file_decryption_properties(kms_config, config)
            .unwrap();

        assert!(decryption_props.key_retriever.is_some());
        let key_retriever = decryption_props.key_retriever.unwrap();

        let expected_dek = "1234567890123450".as_bytes().to_vec();
        let kms = TestKmsClientFactory::with_default_keys()
            .create_client(&Default::default())
            .unwrap();

        let wrapped_key = kms.wrap_key(&expected_dek, "kc1").unwrap();
        let key_material = KeyMaterialBuilder::for_column_key()
            .with_single_wrapped_key("kc1".to_owned(), wrapped_key)
            .build()
            .unwrap();
        let serialized_key_material = key_material.serialize().unwrap();

        let dek = key_retriever
            .retrieve_key(serialized_key_material.as_bytes())
            .unwrap();

        assert_eq!(dek, expected_dek);
    }

    #[test]
    fn test_kms_client_caching() {
        let kms_config = Arc::new(RwLock::new(KmsConnectionConfig::new()));
        let config = Default::default();

        let kms_factory = Arc::new(TestKmsClientFactory::with_default_keys());
        let crypto_factory = CryptoFactory::new(kms_factory.clone());
        let decryption_props = crypto_factory
            .file_decryption_properties(kms_config.clone(), config)
            .unwrap();

        assert!(decryption_props.key_retriever.is_some());
        let key_retriever = decryption_props.key_retriever.unwrap();

        let dek = "1234567890123450".as_bytes().to_vec();
        let kms = TestKmsClientFactory::with_default_keys()
            .create_client(&Default::default())
            .unwrap();

        let wrapped_key = kms.wrap_key(&dek, "kc1").unwrap();
        let key_material = KeyMaterialBuilder::for_column_key()
            .with_single_wrapped_key("kc1".to_owned(), wrapped_key)
            .build()
            .unwrap();
        let serialized_key_material = key_material.serialize().unwrap();

        assert_eq!(0, kms_factory.invocations().len());

        key_retriever
            .retrieve_key(serialized_key_material.as_bytes())
            .unwrap();
        assert_eq!(vec!["DEFAULT"], kms_factory.invocations());

        key_retriever
            .retrieve_key(serialized_key_material.as_bytes())
            .unwrap();
        // Same client should have been reused
        assert_eq!(vec!["DEFAULT"], kms_factory.invocations());

        {
            let mut config = kms_config.write().unwrap();
            config.refresh_key_access_token("super_secret".to_owned());
        }

        key_retriever
            .retrieve_key(serialized_key_material.as_bytes())
            .unwrap();
        // New key access token should have been used
        assert_eq!(vec!["DEFAULT", "super_secret"], kms_factory.invocations());

        key_retriever
            .retrieve_key(serialized_key_material.as_bytes())
            .unwrap();
        assert_eq!(vec!["DEFAULT", "super_secret"], kms_factory.invocations());
    }

    #[test]
    fn test_round_trip_double_wrapping_properties() {
        round_trip_encryption_properties(true);
    }

    #[test]
    fn test_round_trip_single_wrapping_properties() {
        round_trip_encryption_properties(false);
    }

    #[test]
    fn test_uniform_encryption() {
        let kms_config = Arc::new(RwLock::new(KmsConnectionConfig::new()));
        let encryption_config = EncryptionConfigurationBuilder::new("kf".to_owned())
            .set_double_wrapping(true)
            .build();

        let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());

        let file_encryption_properties = crypto_factory
            .file_encryption_properties(kms_config.clone(), &encryption_config)
            .unwrap();

        assert!(file_encryption_properties.column_keys.is_none());
    }

    fn round_trip_encryption_properties(double_wrapping: bool) {
        let kms_config = Arc::new(RwLock::new(KmsConnectionConfig::new()));
        let encryption_config = EncryptionConfigurationBuilder::new("kf".to_owned())
            .set_double_wrapping(double_wrapping)
            .add_column_key("kc1".to_owned(), vec!["x0".to_owned(), "x1".to_owned()])
            .add_column_key("kc2".to_owned(), vec!["x2".to_owned(), "x3".to_owned()])
            .build();

        let kms_factory = Arc::new(TestKmsClientFactory::with_default_keys());
        let crypto_factory = CryptoFactory::new(kms_factory.clone());

        let file_encryption_properties = crypto_factory
            .file_encryption_properties(kms_config.clone(), &encryption_config)
            .unwrap();

        let decryption_properties = crypto_factory
            .file_decryption_properties(kms_config.clone(), Default::default())
            .unwrap();
        let key_retriever = decryption_properties.key_retriever.unwrap();

        assert!(file_encryption_properties.encrypt_footer);
        assert!(file_encryption_properties.aad_prefix.is_none());
        assert_eq!(16, file_encryption_properties.footer_key.key.len());

        let retrieved_footer_key = key_retriever
            .retrieve_key(&file_encryption_properties.footer_key.key_metadata)
            .unwrap();
        assert_eq!(
            &file_encryption_properties.footer_key.key,
            &retrieved_footer_key
        );

        let column_keys = file_encryption_properties.column_keys.unwrap();
        let mut all_columns: Vec<String> = column_keys.iter().map(|(k, _)| k.clone()).collect();
        all_columns.sort();
        assert_eq!(vec!["x0", "x1", "x2", "x3"], all_columns);
        for (_, column_key) in column_keys.iter() {
            assert_eq!(16, column_key.key.len());

            let retrieved_key = key_retriever
                .retrieve_key(&column_key.key_metadata)
                .unwrap();
            assert_eq!(&column_key.key, &retrieved_key);
        }

        assert_eq!(1, kms_factory.invocations().len());
        if double_wrapping {
            // With double wrapping, only need to wrap one KEK per master key id used
            assert_eq!(3, kms_factory.keys_wrapped());
            assert_eq!(3, kms_factory.keys_unwrapped());
        } else {
            // With single wrapping, need to wrap the footer key and a DEK per column
            assert_eq!(5, kms_factory.keys_wrapped());
            assert_eq!(5, kms_factory.keys_unwrapped());
        }
    }
}
