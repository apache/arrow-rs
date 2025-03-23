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

//! The key-management tools API for building file encryption and decryption properties
//! that work with a Key Management Server.

use crate::encryption::decrypt::FileDecryptionProperties;
use crate::encryption::encrypt::FileEncryptionProperties;
use crate::encryption::key_management::key_unwrapper::KeyUnwrapper;
use crate::encryption::key_management::key_wrapper::KeyWrapper;
use crate::encryption::key_management::kms::{KmsClientFactory, KmsConnectionConfig};
use crate::encryption::key_management::kms_manager::KmsManager;
use crate::errors::{ParquetError, Result};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::sync::Arc;
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
    /// Create a new builder for an [`EncryptionConfiguration`]
    pub fn builder(footer_key: String) -> EncryptionConfigurationBuilder {
        EncryptionConfigurationBuilder::new(footer_key)
    }

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
    /// Currently only internal key material is implemented.
    pub fn internal_key_material(&self) -> bool {
        self.internal_key_material
    }

    /// Number of bits for randomly generated data encryption keys.
    /// Currently only 128-bit keys are implemented.
    pub fn data_key_length_bits(&self) -> u32 {
        self.data_key_length_bits
    }
}

/// Builder for a Parquet [`EncryptionConfiguration`].
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
    /// Create a new [`EncryptionConfigurationBuilder`] with default options
    pub fn new(footer_key: String) -> Self {
        Self {
            footer_key,
            column_keys: Default::default(),
            encryption_algorithm: EncryptionAlgorithm::AesGcmV1,
            plaintext_footer: false,
            double_wrapping: true,
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

    /// Set the encryption algorithm to use.
    /// Currently only [`EncryptionAlgorithm::AesGcmV1`] is supported.
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
}

/// Configuration for decrypting a Parquet file
#[derive(Debug)]
pub struct DecryptionConfiguration {
    cache_lifetime: Option<Duration>,
}

impl DecryptionConfiguration {
    /// Create a new builder for a [`DecryptionConfiguration`]
    pub fn builder() -> DecryptionConfigurationBuilder {
        DecryptionConfigurationBuilder::default()
    }

    /// How long to cache objects for, including decrypted key encryption keys
    /// and KMS clients. When None, no caching is used.
    pub fn cache_lifetime(&self) -> Option<Duration> {
        self.cache_lifetime
    }
}

impl Default for DecryptionConfiguration {
    fn default() -> Self {
        DecryptionConfigurationBuilder::default().build()
    }
}

/// Builder for Parquet [`DecryptionConfiguration`].
pub struct DecryptionConfigurationBuilder {
    cache_lifetime: Option<Duration>,
}

impl DecryptionConfigurationBuilder {
    /// Create a new [`DecryptionConfigurationBuilder`] with default options
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
    /// and KMS clients. When None, objects are cached indefinitely.
    pub fn set_cache_lifetime(mut self, cache_lifetime: Option<Duration>) -> Self {
        self.cache_lifetime = cache_lifetime;
        self
    }
}

impl Default for DecryptionConfigurationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A factory that produces file decryption and encryption properties using
/// configuration options and a KMS client
pub struct CryptoFactory {
    kms_manager: Arc<KmsManager>,
}

impl CryptoFactory {
    /// Create a new [`CryptoFactory`], providing a factory function for creating KMS clients
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
        kms_connection_config: Arc<KmsConnectionConfig>,
        decryption_configuration: DecryptionConfiguration,
    ) -> Result<FileDecryptionProperties> {
        let key_retriever = Arc::new(KeyUnwrapper::new(
            self.kms_manager.clone(),
            kms_connection_config,
            decryption_configuration,
        ));
        FileDecryptionProperties::with_key_retriever(key_retriever).build()
    }

    /// Create file encryption properties for a Parquet file
    pub fn file_encryption_properties(
        &self,
        kms_connection_config: Arc<KmsConnectionConfig>,
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

        let mut builder = FileEncryptionProperties::builder(footer_key.key.clone())
            .with_footer_key_metadata(footer_key.metadata.clone())
            .with_plaintext_footer(encryption_configuration.plaintext_footer);

        for (master_key_id, column_paths) in &encryption_configuration.column_keys {
            for column_path in column_paths {
                let column_key = self.generate_key(master_key_id, false, &mut key_wrapper)?;
                builder = builder.with_column_key_and_metadata(
                    column_path,
                    column_key.key,
                    column_key.metadata,
                );
            }
        }

        Ok(builder.build())
    }

    fn generate_key(
        &self,
        master_key_identifier: &str,
        is_footer_key: bool,
        key_wrapper: &mut KeyWrapper,
    ) -> Result<EncryptionKey> {
        let rng = SystemRandom::new();
        let mut key = vec![0u8; 16];
        rng.fill(&mut key)?;

        let key_metadata =
            key_wrapper.get_key_metadata(&key, master_key_identifier, is_footer_key)?;

        Ok(EncryptionKey::new(key, key_metadata))
    }
}

struct EncryptionKey {
    key: Vec<u8>,
    metadata: Vec<u8>,
}

impl EncryptionKey {
    pub fn new(key: Vec<u8>, metadata: Vec<u8>) -> Self {
        Self { key, metadata }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::AsBytes;
    use crate::encryption::key_management::key_material::KeyMaterialBuilder;
    use crate::encryption::key_management::test_kms::TestKmsClientFactory;

    #[test]
    fn test_file_decryption_properties() {
        let kms_config = Arc::new(KmsConnectionConfig::default());
        let config = Default::default();

        let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());
        let decryption_props = crypto_factory
            .file_decryption_properties(kms_config, config)
            .unwrap();

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

        let dek = decryption_props
            .footer_key(Some(serialized_key_material.as_bytes()))
            .unwrap()
            .into_owned();

        assert_eq!(dek, expected_dek);
    }

    #[test]
    fn test_kms_client_caching_with_lifetime() {
        test_kms_client_caching(Some(Duration::from_secs(6000)));
    }

    #[test]
    fn test_kms_client_caching_no_lifetime() {
        test_kms_client_caching(None);
    }

    fn test_kms_client_caching(cache_lifetime: Option<Duration>) {
        let kms_config = Arc::new(KmsConnectionConfig::default());
        let config = DecryptionConfiguration::builder()
            .set_cache_lifetime(cache_lifetime)
            .build();

        let kms_factory = Arc::new(TestKmsClientFactory::with_default_keys());
        let crypto_factory = CryptoFactory::new(kms_factory.clone());
        let decryption_props = crypto_factory
            .file_decryption_properties(kms_config.clone(), config)
            .unwrap();

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

        decryption_props
            .footer_key(Some(serialized_key_material.as_bytes()))
            .unwrap()
            .into_owned();
        assert_eq!(vec!["DEFAULT"], kms_factory.invocations());

        decryption_props
            .footer_key(Some(serialized_key_material.as_bytes()))
            .unwrap()
            .into_owned();
        // Same client should have been reused
        assert_eq!(vec!["DEFAULT"], kms_factory.invocations());

        kms_config.refresh_key_access_token("super_secret".to_owned());

        decryption_props
            .footer_key(Some(serialized_key_material.as_bytes()))
            .unwrap()
            .into_owned();
        // New key access token should have been used
        assert_eq!(vec!["DEFAULT", "super_secret"], kms_factory.invocations());

        decryption_props
            .footer_key(Some(serialized_key_material.as_bytes()))
            .unwrap()
            .into_owned();
        assert_eq!(vec!["DEFAULT", "super_secret"], kms_factory.invocations());
    }

    #[test]
    fn test_kms_client_expiration() {
        let kms_config = Arc::new(KmsConnectionConfig::default());
        let config = DecryptionConfiguration::builder()
            .set_cache_lifetime(Some(Duration::from_secs(600)))
            .build();

        let kms_factory = Arc::new(TestKmsClientFactory::with_default_keys());
        let crypto_factory = CryptoFactory::new(kms_factory.clone());
        let decryption_props = crypto_factory
            .file_decryption_properties(kms_config.clone(), config)
            .unwrap();

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

        let time_controller =
            crate::encryption::key_management::kms_manager::mock_time::time_controller();

        assert_eq!(0, kms_factory.invocations().len());

        let do_key_retrieval = || {
            decryption_props
                .footer_key(Some(serialized_key_material.as_bytes()))
                .unwrap()
                .into_owned();
        };

        do_key_retrieval();
        assert_eq!(1, kms_factory.invocations().len());

        time_controller.advance(Duration::from_secs(599));

        do_key_retrieval();
        assert_eq!(1, kms_factory.invocations().len());

        time_controller.advance(Duration::from_secs(1));

        do_key_retrieval();
        assert_eq!(2, kms_factory.invocations().len());

        time_controller.advance(Duration::from_secs(599));

        do_key_retrieval();
        assert_eq!(2, kms_factory.invocations().len());

        time_controller.advance(Duration::from_secs(1));

        do_key_retrieval();
        assert_eq!(3, kms_factory.invocations().len());
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
        let kms_config = Arc::new(KmsConnectionConfig::default());
        let encryption_config = EncryptionConfigurationBuilder::new("kf".to_owned())
            .set_double_wrapping(true)
            .build();

        let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());

        let file_encryption_properties = crypto_factory
            .file_encryption_properties(kms_config.clone(), &encryption_config)
            .unwrap();

        assert!(file_encryption_properties.column_keys().is_empty());
    }

    fn round_trip_encryption_properties(double_wrapping: bool) {
        let kms_config = Arc::new(KmsConnectionConfig::default());
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

        assert!(file_encryption_properties.encrypt_footer());
        assert!(file_encryption_properties.aad_prefix().is_none());
        assert_eq!(16, file_encryption_properties.footer_key().len());

        let retrieved_footer_key = decryption_properties
            .footer_key(
                file_encryption_properties
                    .footer_key_metadata()
                    .map(|k| k.as_bytes()),
            )
            .unwrap();
        assert_eq!(
            file_encryption_properties.footer_key(),
            retrieved_footer_key.as_slice()
        );

        let column_keys = file_encryption_properties.column_keys();
        let mut all_columns: Vec<String> = column_keys.keys().cloned().collect();
        all_columns.sort();
        assert_eq!(vec!["x0", "x1", "x2", "x3"], all_columns);
        for (column_name, column_key) in column_keys.iter() {
            assert_eq!(16, column_key.key().len());

            let retrieved_key = decryption_properties
                .column_key(column_name, column_key.key_metadata().map(|k| k.as_bytes()))
                .unwrap();
            assert_eq!(column_key.key(), retrieved_key.as_slice());
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

    #[test]
    fn test_key_encryption_key_caching() {
        let kms_config = Arc::new(KmsConnectionConfig::default());
        let encryption_config = EncryptionConfigurationBuilder::new("kf".to_owned())
            .set_double_wrapping(true)
            .add_column_key("kc1".to_owned(), vec!["x0".to_owned(), "x1".to_owned()])
            .add_column_key("kc2".to_owned(), vec!["x2".to_owned(), "x3".to_owned()])
            .build();

        let kms_factory = Arc::new(TestKmsClientFactory::with_default_keys());
        let crypto_factory = CryptoFactory::new(kms_factory.clone());

        let file_encryption_properties = crypto_factory
            .file_encryption_properties(kms_config.clone(), &encryption_config)
            .unwrap();

        let footer_key_metadata = file_encryption_properties.footer_key_metadata().cloned();

        // Key-encryption keys are cached for the lifetime of file decryption properties,
        // and when creating new file decryption properties, a previous key-encryption key cache
        // may be reused if the cache lifetime hasn't expired and the KMS access token is the same.

        let get_new_decryption_properties = || {
            let decryption_config = DecryptionConfiguration::builder()
                .set_cache_lifetime(Some(Duration::from_secs(600)))
                .build();
            crypto_factory
                .file_decryption_properties(kms_config.clone(), decryption_config)
                .unwrap()
        };

        let retrieve_key = |props: &FileDecryptionProperties| {
            props.footer_key(footer_key_metadata.as_deref()).unwrap();
        };

        let time_controller =
            crate::encryption::key_management::kms_manager::mock_time::time_controller();

        assert_eq!(0, kms_factory.keys_unwrapped());

        {
            let props = get_new_decryption_properties();
            retrieve_key(&props);
            time_controller.advance(Duration::from_secs(599));
            retrieve_key(&props);
            assert_eq!(1, kms_factory.keys_unwrapped());
        }
        {
            let props = get_new_decryption_properties();
            retrieve_key(&props);
            assert_eq!(1, kms_factory.keys_unwrapped());
            time_controller.advance(Duration::from_secs(1));
            retrieve_key(&props);
            // Cache lifetime has expired but the key unwrapper still holds the
            // key encryption key cache.
            assert_eq!(1, kms_factory.keys_unwrapped());
        }
        {
            let props = get_new_decryption_properties();
            retrieve_key(&props);
            // Newly created decryption properties use a new key encryption key cache
            assert_eq!(2, kms_factory.keys_unwrapped());
        }
        {
            time_controller.advance(Duration::from_secs(599));
            // Creating new decryption properties should re-use the more recent cache
            let props1 = get_new_decryption_properties();
            retrieve_key(&props1);
            assert_eq!(2, kms_factory.keys_unwrapped());

            kms_config.refresh_key_access_token("new_secret".to_owned());
            // Creating decryption properties with a different access key should require
            // creating a new key encryption key cache.
            let props2 = get_new_decryption_properties();
            retrieve_key(&props2);
            assert_eq!(3, kms_factory.keys_unwrapped());

            // But the cache used by older file encryption properties is still usable.
            retrieve_key(&props1);
            assert_eq!(3, kms_factory.keys_unwrapped());
        }
    }
}
