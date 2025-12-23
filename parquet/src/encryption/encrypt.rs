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

//! Configuration and utilities for Parquet Modular Encryption

use crate::encryption::ciphers::{
    BlockEncryptor, NONCE_LEN, RingGcmBlockEncryptor, SIZE_LEN, TAG_LEN,
};
use crate::errors::{ParquetError, Result};
use crate::file::column_crypto_metadata::{ColumnCryptoMetaData, EncryptionWithColumnKey};
use crate::parquet_thrift::{ThriftCompactOutputProtocol, WriteThrift};
use crate::schema::types::{ColumnDescPtr, SchemaDescriptor};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
struct EncryptionKey {
    key: Vec<u8>,
    key_metadata: Option<Vec<u8>>,
}

impl EncryptionKey {
    fn new(key: Vec<u8>) -> EncryptionKey {
        Self {
            key,
            key_metadata: None,
        }
    }

    fn with_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(metadata);
        self
    }

    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Defines how data in a Parquet file should be encrypted
///
/// The `FileEncryptionProperties` should be included in the [`WriterProperties`](crate::file::properties::WriterProperties)
/// used to write a file by using [`WriterPropertiesBuilder::with_file_encryption_properties`](crate::file::properties::WriterPropertiesBuilder::with_file_encryption_properties).
///
/// # Examples
///
/// Create `FileEncryptionProperties` for a file encrypted with uniform encryption,
/// where all metadata and data are encrypted with the footer key:
/// ```
/// # use parquet::encryption::encrypt::FileEncryptionProperties;
/// let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
///     .build()?;
/// # Ok::<(), parquet::errors::ParquetError>(())
/// ```
///
/// Create properties for a file where columns are encrypted with different keys.
/// Any columns without a key specified will be unencrypted:
/// ```
/// # use parquet::encryption::encrypt::FileEncryptionProperties;
/// let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
///     .with_column_key("x", b"1234567890123450".into())
///     .with_column_key("y", b"1234567890123451".into())
///     .build()?;
/// # Ok::<(), parquet::errors::ParquetError>(())
/// ```
///
/// Specify additional authenticated data, used to protect against data replacement.
/// This should represent the file identity:
/// ```
/// # use parquet::encryption::encrypt::FileEncryptionProperties;
/// let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
///     .with_aad_prefix("example_file".into())
///     .build()?;
/// # Ok::<(), parquet::errors::ParquetError>(())
/// ```
pub struct FileEncryptionProperties {
    encrypt_footer: bool,
    footer_key: EncryptionKey,
    column_keys: HashMap<String, EncryptionKey>,
    aad_prefix: Option<Vec<u8>>,
    store_aad_prefix: bool,
}

impl FileEncryptionProperties {
    /// Create a new builder for encryption properties with the given footer encryption key
    pub fn builder(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        EncryptionPropertiesBuilder::new(footer_key)
    }

    /// Should the footer be encrypted
    pub fn encrypt_footer(&self) -> bool {
        self.encrypt_footer
    }

    /// Retrieval metadata of key used for encryption of footer and (possibly) columns
    pub fn footer_key_metadata(&self) -> Option<&Vec<u8>> {
        self.footer_key.key_metadata.as_ref()
    }

    /// Retrieval of key used for encryption of footer and (possibly) columns
    pub fn footer_key(&self) -> &Vec<u8> {
        &self.footer_key.key
    }

    /// Get the column names, keys, and metadata for columns to be encrypted
    pub fn column_keys(&self) -> (Vec<String>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut column_names: Vec<String> = Vec::with_capacity(self.column_keys.len());
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(self.column_keys.len());
        let mut meta: Vec<Vec<u8>> = Vec::with_capacity(self.column_keys.len());
        for (key, value) in self.column_keys.iter() {
            column_names.push(key.clone());
            keys.push(value.key.clone());
            if let Some(metadata) = value.key_metadata.as_ref() {
                meta.push(metadata.clone());
            }
        }
        (column_names, keys, meta)
    }

    /// AAD prefix string uniquely identifies the file and prevents file swapping
    pub fn aad_prefix(&self) -> Option<&Vec<u8>> {
        self.aad_prefix.as_ref()
    }

    /// Should the AAD prefix be stored in the file
    pub fn store_aad_prefix(&self) -> bool {
        self.store_aad_prefix && self.aad_prefix.is_some()
    }

    /// Checks if columns that are to be encrypted are present in schema
    pub(crate) fn validate_encrypted_column_names(
        &self,
        schema: &SchemaDescriptor,
    ) -> std::result::Result<(), ParquetError> {
        let column_paths = schema
            .columns()
            .iter()
            .map(|c| c.path().string())
            .collect::<HashSet<_>>();
        let encryption_columns = self
            .column_keys
            .keys()
            .cloned()
            .collect::<HashSet<String>>();
        if !encryption_columns.is_subset(&column_paths) {
            let mut columns_missing_in_schema = encryption_columns
                .difference(&column_paths)
                .cloned()
                .collect::<Vec<String>>();
            columns_missing_in_schema.sort();
            return Err(ParquetError::General(
                format!(
                    "The following columns with encryption keys specified were not found in the schema: {}",
                    columns_missing_in_schema.join(", ")
                )
                .to_string(),
            ));
        }
        Ok(())
    }
}

/// Builder for [`FileEncryptionProperties`]
///
/// See [`FileEncryptionProperties`] for example usage.
pub struct EncryptionPropertiesBuilder {
    encrypt_footer: bool,
    footer_key: EncryptionKey,
    column_keys: HashMap<String, EncryptionKey>,
    aad_prefix: Option<Vec<u8>>,
    store_aad_prefix: bool,
}

impl EncryptionPropertiesBuilder {
    /// Create a new [`EncryptionPropertiesBuilder`] with the given footer encryption key
    pub fn new(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        Self {
            footer_key: EncryptionKey::new(footer_key),
            column_keys: HashMap::default(),
            aad_prefix: None,
            encrypt_footer: true,
            store_aad_prefix: false,
        }
    }

    /// Set if the footer should be stored in plaintext (not encrypted). Defaults to false.
    pub fn with_plaintext_footer(mut self, plaintext_footer: bool) -> Self {
        self.encrypt_footer = !plaintext_footer;
        self
    }

    /// Set retrieval metadata of key used for encryption of footer and (possibly) columns
    pub fn with_footer_key_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.footer_key = self.footer_key.with_metadata(metadata);
        self
    }

    /// Set the key used for encryption of a column. Note that if no column keys are configured then
    /// all columns will be encrypted with the footer key.
    /// If any column keys are configured then only the columns with a key will be encrypted.
    pub fn with_column_key(mut self, column_name: &str, key: Vec<u8>) -> Self {
        self.column_keys
            .insert(column_name.to_string(), EncryptionKey::new(key));
        self
    }

    /// Set the key used for encryption of a column and its metadata. The Key's metadata field is to
    /// enable file readers to recover the key. For example, the metadata can keep a serialized
    /// ID of a data key. Note that if no column keys are configured then all columns
    /// will be encrypted with the footer key. If any column keys are configured then only the
    /// columns with a key will be encrypted.
    pub fn with_column_key_and_metadata(
        mut self,
        column_name: &str,
        key: Vec<u8>,
        metadata: Vec<u8>,
    ) -> Self {
        self.column_keys.insert(
            column_name.to_string(),
            EncryptionKey::new(key).with_metadata(metadata),
        );
        self
    }

    /// Set the keys used for encryption of columns. Analogous to
    /// with_column_key but for multiple columns. This will add column keys provided to the
    /// existing column keys. If column keys were already provided for some columns, the new keys
    /// will overwrite the old ones.
    pub fn with_column_keys(mut self, column_names: Vec<&str>, keys: Vec<Vec<u8>>) -> Result<Self> {
        if column_names.len() != keys.len() {
            return Err(general_err!(
                "The number of column names ({}) does not match the number of keys ({})",
                column_names.len(),
                keys.len()
            ));
        }
        for (i, column_name) in column_names.into_iter().enumerate() {
            self.column_keys
                .insert(column_name.to_string(), EncryptionKey::new(keys[i].clone()));
        }
        Ok(self)
    }

    /// The AAD prefix uniquely identifies the file and allows to differentiate it e.g. from
    /// older versions of the file or from other partition files in the same data set (table).
    /// These bytes are optionally passed by a writer upon file creation. When not specified, no
    /// AAD prefix is used.
    pub fn with_aad_prefix(mut self, aad_prefix: Vec<u8>) -> Self {
        self.aad_prefix = Some(aad_prefix);
        self
    }

    /// Should the AAD prefix be stored in the file. If false, readers will need to provide the
    /// AAD prefix to be able to decrypt data. Defaults to false.
    pub fn with_aad_prefix_storage(mut self, store_aad_prefix: bool) -> Self {
        self.store_aad_prefix = store_aad_prefix;
        self
    }

    /// Build the encryption properties
    pub fn build(self) -> Result<Arc<FileEncryptionProperties>> {
        Ok(Arc::new(FileEncryptionProperties {
            encrypt_footer: self.encrypt_footer,
            footer_key: self.footer_key,
            column_keys: self.column_keys,
            aad_prefix: self.aad_prefix,
            store_aad_prefix: self.store_aad_prefix,
        }))
    }
}

#[derive(Debug)]
/// The encryption configuration for a single Parquet file
pub(crate) struct FileEncryptor {
    properties: Arc<FileEncryptionProperties>,
    aad_file_unique: Vec<u8>,
    file_aad: Vec<u8>,
}

impl FileEncryptor {
    pub(crate) fn new(properties: Arc<FileEncryptionProperties>) -> Result<Self> {
        // Generate unique AAD for file
        let rng = SystemRandom::new();
        let mut aad_file_unique = vec![0u8; 8];
        rng.fill(&mut aad_file_unique)?;

        let file_aad = match properties.aad_prefix.as_ref() {
            None => aad_file_unique.clone(),
            Some(aad_prefix) => [aad_prefix.clone(), aad_file_unique.clone()].concat(),
        };

        Ok(Self {
            properties,
            aad_file_unique,
            file_aad,
        })
    }

    /// Get the encryptor's file encryption properties
    pub fn properties(&self) -> &Arc<FileEncryptionProperties> {
        &self.properties
    }

    /// Combined AAD prefix and suffix for the file generated
    pub fn file_aad(&self) -> &[u8] {
        &self.file_aad
    }

    /// Unique file identifier part of AAD suffix. The full AAD suffix is generated per module by
    /// concatenating aad_file_unique, module type, row group ordinal (all except
    /// footer), column ordinal (all except footer) and page ordinal (data page and
    /// header only).
    pub fn aad_file_unique(&self) -> &Vec<u8> {
        &self.aad_file_unique
    }

    /// Returns whether data for the specified column should be encrypted
    pub fn is_column_encrypted(&self, column_path: &str) -> bool {
        if self.properties.column_keys.is_empty() {
            // Uniform encryption
            true
        } else {
            self.properties.column_keys.contains_key(column_path)
        }
    }

    /// Get the BlockEncryptor for the footer
    pub(crate) fn get_footer_encryptor(&self) -> Result<Box<dyn BlockEncryptor>> {
        Ok(Box::new(RingGcmBlockEncryptor::new(
            &self.properties.footer_key.key,
        )?))
    }

    /// Get the encryptor for a column.
    /// Will return an error if the column is not an encrypted column.
    pub(crate) fn get_column_encryptor(
        &self,
        column_path: &str,
    ) -> Result<Box<dyn BlockEncryptor>> {
        if self.properties.column_keys.is_empty() {
            return self.get_footer_encryptor();
        }
        match self.properties.column_keys.get(column_path) {
            None => Err(general_err!("Column '{}' is not encrypted", column_path)),
            Some(column_key) => Ok(Box::new(RingGcmBlockEncryptor::new(column_key.key())?)),
        }
    }
}

/// Write an encrypted Thrift serializable object
pub(crate) fn encrypt_thrift_object<T: WriteThrift, W: Write>(
    object: &T,
    encryptor: &mut Box<dyn BlockEncryptor>,
    sink: &mut W,
    module_aad: &[u8],
) -> Result<()> {
    let encrypted_buffer = encrypt_thrift_object_to_vec(object, encryptor, module_aad)?;
    sink.write_all(&encrypted_buffer)?;
    Ok(())
}

pub(crate) fn write_signed_plaintext_thrift_object<T: WriteThrift, W: Write>(
    object: &T,
    encryptor: &mut Box<dyn BlockEncryptor>,
    sink: &mut W,
    module_aad: &[u8],
) -> Result<()> {
    let mut buffer: Vec<u8> = vec![];
    {
        let mut protocol = ThriftCompactOutputProtocol::new(&mut buffer);
        object.write_thrift(&mut protocol)?;
    }
    sink.write_all(&buffer)?;
    buffer = encryptor.encrypt(buffer.as_ref(), module_aad)?;

    // Format of encrypted buffer is: [ciphertext size, nonce, ciphertext, authentication tag]
    let nonce = &buffer[SIZE_LEN..SIZE_LEN + NONCE_LEN];
    let tag = &buffer[buffer.len() - TAG_LEN..];
    sink.write_all(nonce)?;
    sink.write_all(tag)?;

    Ok(())
}

/// Encrypt a Thrift serializable object to a byte vector
pub(crate) fn encrypt_thrift_object_to_vec<T: WriteThrift>(
    object: &T,
    encryptor: &mut Box<dyn BlockEncryptor>,
    module_aad: &[u8],
) -> Result<Vec<u8>> {
    let mut buffer: Vec<u8> = vec![];
    {
        let mut unencrypted_protocol = ThriftCompactOutputProtocol::new(&mut buffer);
        object.write_thrift(&mut unencrypted_protocol)?;
    }

    encryptor.encrypt(buffer.as_ref(), module_aad)
}

/// Get the crypto metadata for a column from the file encryption properties
pub(crate) fn get_column_crypto_metadata(
    properties: &Arc<FileEncryptionProperties>,
    column: &ColumnDescPtr,
) -> Option<ColumnCryptoMetaData> {
    if properties.column_keys.is_empty() {
        // Uniform encryption
        Some(ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY)
    } else {
        properties
            .column_keys
            .get(&column.path().string())
            .map(|encryption_key| {
                // Column is encrypted with a column specific key
                ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(EncryptionWithColumnKey {
                    path_in_schema: column.path().parts().to_vec(),
                    key_metadata: encryption_key.key_metadata.clone(),
                })
            })
    }
}
