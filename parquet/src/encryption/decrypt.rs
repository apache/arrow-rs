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

use crate::encryption::ciphers::{BlockDecryptor, RingGcmBlockDecryptor};
use crate::encryption::modules::{create_module_aad, ModuleType};
use crate::errors::{ParquetError, Result};
use crate::file::column_crypto_metadata::ColumnCryptoMetaData;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::io::Read;
use std::sync::Arc;

/// Trait for retrieving an encryption key using the key's metadata
pub trait KeyRetriever: Send + Sync {
    fn retrieve_key(&self, key_metadata: &[u8]) -> Result<Vec<u8>>;
}

pub fn read_and_decrypt<T: Read>(
    decryptor: &Arc<dyn BlockDecryptor>,
    input: &mut T,
    aad: &[u8],
) -> Result<Vec<u8>> {
    let mut len_bytes = [0; 4];
    input.read_exact(&mut len_bytes)?;
    let ciphertext_len = u32::from_le_bytes(len_bytes) as usize;
    let mut ciphertext = vec![0; 4 + ciphertext_len];
    input.read_exact(&mut ciphertext[4..])?;

    decryptor.decrypt(&ciphertext, aad.as_ref())
}

// CryptoContext is a data structure that holds the context required to
// decrypt parquet modules (data pages, dictionary pages, etc.).
#[derive(Debug, Clone)]
pub(crate) struct CryptoContext {
    pub(crate) row_group_idx: usize,
    pub(crate) column_ordinal: usize,
    pub(crate) page_ordinal: Option<usize>,
    pub(crate) dictionary_page: bool,
    // We have separate data and metadata decryptors because
    // in GCM CTR mode, the metadata and data pages use
    // different algorithms.
    data_decryptor: Arc<dyn BlockDecryptor>,
    metadata_decryptor: Arc<dyn BlockDecryptor>,
    file_aad: Vec<u8>,
}

impl CryptoContext {
    pub(crate) fn for_column(
        file_decryptor: &FileDecryptor,
        column_crypto_metadata: &ColumnCryptoMetaData,
        row_group_idx: usize,
        column_ordinal: usize,
    ) -> Result<Self> {
        let (data_decryptor, metadata_decryptor) = match column_crypto_metadata {
            ColumnCryptoMetaData::EncryptionWithFooterKey => {
                // TODO: In GCM-CTR mode will this need to be a non-GCM decryptor?
                let data_decryptor = file_decryptor.get_footer_decryptor()?;
                let metadata_decryptor = file_decryptor.get_footer_decryptor()?;
                (data_decryptor, metadata_decryptor)
            }
            ColumnCryptoMetaData::EncryptionWithColumnKey(column_key_encryption) => {
                let key_metadata = &column_key_encryption.key_metadata;
                let full_column_name;
                let column_name = if column_key_encryption.path_in_schema.len() == 1 {
                    &column_key_encryption.path_in_schema[0]
                } else {
                    full_column_name = column_key_encryption.path_in_schema.join(".");
                    &full_column_name
                };
                let data_decryptor = file_decryptor
                    .get_column_data_decryptor(column_name, key_metadata.as_deref())?;
                let metadata_decryptor = file_decryptor
                    .get_column_metadata_decryptor(column_name, key_metadata.as_deref())?;
                (data_decryptor, metadata_decryptor)
            }
        };

        Ok(CryptoContext {
            row_group_idx,
            column_ordinal,
            page_ordinal: None,
            dictionary_page: false,
            data_decryptor,
            metadata_decryptor,
            file_aad: file_decryptor.file_aad().clone(),
        })
    }

    pub(crate) fn with_page_ordinal(&self, page_ordinal: usize) -> Self {
        Self {
            row_group_idx: self.row_group_idx,
            column_ordinal: self.column_ordinal,
            page_ordinal: Some(page_ordinal),
            dictionary_page: false,
            data_decryptor: self.data_decryptor.clone(),
            metadata_decryptor: self.metadata_decryptor.clone(),
            file_aad: self.file_aad.clone(),
        }
    }

    pub(crate) fn create_page_header_aad(&self) -> Result<Vec<u8>> {
        let module_type = if self.dictionary_page {
            ModuleType::DictionaryPageHeader
        } else {
            ModuleType::DataPageHeader
        };

        create_module_aad(
            self.file_aad(),
            module_type,
            self.row_group_idx,
            self.column_ordinal,
            self.page_ordinal,
        )
    }

    pub(crate) fn create_page_aad(&self) -> Result<Vec<u8>> {
        let module_type = if self.dictionary_page {
            ModuleType::DictionaryPage
        } else {
            ModuleType::DataPage
        };

        create_module_aad(
            self.file_aad(),
            module_type,
            self.row_group_idx,
            self.column_ordinal,
            self.page_ordinal,
        )
    }

    pub(crate) fn for_dictionary_page(&self) -> Self {
        Self {
            row_group_idx: self.row_group_idx,
            column_ordinal: self.column_ordinal,
            page_ordinal: self.page_ordinal,
            dictionary_page: true,
            data_decryptor: self.data_decryptor.clone(),
            metadata_decryptor: self.metadata_decryptor.clone(),
            file_aad: self.file_aad.clone(),
        }
    }

    pub(crate) fn data_decryptor(&self) -> &Arc<dyn BlockDecryptor> {
        &self.data_decryptor
    }

    pub(crate) fn file_aad(&self) -> &Vec<u8> {
        &self.file_aad
    }
}

#[derive(Clone, PartialEq)]
struct ExplicitDecryptionKeys {
    footer_key: Vec<u8>,
    column_keys: HashMap<String, Vec<u8>>,
}

#[derive(Clone)]
enum DecryptionKeys {
    Explicit(ExplicitDecryptionKeys),
    ViaRetriever(Arc<dyn KeyRetriever>),
}

impl PartialEq for DecryptionKeys {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DecryptionKeys::Explicit(keys), DecryptionKeys::Explicit(other_keys)) => {
                keys.footer_key == other_keys.footer_key
                    && keys.column_keys == other_keys.column_keys
            }
            (DecryptionKeys::ViaRetriever(_), DecryptionKeys::ViaRetriever(_)) => true,
            _ => false,
        }
    }
}

/// FileDecryptionProperties hold keys and AAD data required to decrypt a Parquet file.
#[derive(Clone, PartialEq)]
pub struct FileDecryptionProperties {
    keys: DecryptionKeys,
    pub(crate) aad_prefix: Option<Vec<u8>>,
}

impl FileDecryptionProperties {
    /// Returns a new [`FileDecryptionProperties`] builder that will use the provided key to
    /// decrypt footer metadata.
    pub fn builder(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        DecryptionPropertiesBuilder::new(footer_key)
    }

    /// Returns a new [`FileDecryptionProperties`] builder that uses a [`KeyRetriever`]
    /// to get decryption keys based on key metadata.
    pub fn with_key_retriever(key_retriever: Arc<dyn KeyRetriever>) -> DecryptionPropertiesBuilder {
        DecryptionPropertiesBuilder::new_with_key_retriever(key_retriever)
    }

    /// Get the encryption key for decrypting a file's footer,
    /// and also column data if uniform encryption is used.
    pub(crate) fn footer_key(&self, key_metadata: Option<&[u8]>) -> Result<Cow<Vec<u8>>> {
        match &self.keys {
            DecryptionKeys::Explicit(keys) => Ok(Cow::Borrowed(&keys.footer_key)),
            DecryptionKeys::ViaRetriever(retriever) => {
                let key = retriever.retrieve_key(key_metadata.unwrap_or_default())?;
                Ok(Cow::Owned(key))
            }
        }
    }

    /// Get the column-specific encryption key for decrypting column data and metadata within a file
    pub(crate) fn column_key(
        &self,
        column_name: &str,
        key_metadata: Option<&[u8]>,
    ) -> Result<Cow<Vec<u8>>> {
        match &self.keys {
            DecryptionKeys::Explicit(keys) => match keys.column_keys.get(column_name) {
                None => Err(general_err!(
                    "No column decryption key set for column '{}'",
                    column_name
                )),
                Some(key) => Ok(Cow::Borrowed(key)),
            },
            DecryptionKeys::ViaRetriever(retriever) => {
                let key = retriever.retrieve_key(key_metadata.unwrap_or_default())?;
                Ok(Cow::Owned(key))
            }
        }
    }
}

impl std::fmt::Debug for FileDecryptionProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileDecryptionProperties {{ }}")
    }
}

/// Builder for [`FileDecryptionProperties`]
pub struct DecryptionPropertiesBuilder {
    footer_key: Option<Vec<u8>>,
    key_retriever: Option<Arc<dyn KeyRetriever>>,
    column_keys: HashMap<String, Vec<u8>>,
    aad_prefix: Option<Vec<u8>>,
}

impl DecryptionPropertiesBuilder {
    /// Create a new [`DecryptionPropertiesBuilder`] builder that will use the provided key to
    /// decrypt footer metadata.
    pub fn new(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        Self {
            footer_key: Some(footer_key),
            key_retriever: None,
            column_keys: HashMap::default(),
            aad_prefix: None,
        }
    }

    /// Create a new [`DecryptionPropertiesBuilder`] by providing a [`KeyRetriever`] that
    /// can be used to get decryption keys based on key metadata.
    pub fn new_with_key_retriever(
        key_retriever: Arc<dyn KeyRetriever>,
    ) -> DecryptionPropertiesBuilder {
        Self {
            footer_key: None,
            key_retriever: Some(key_retriever),
            column_keys: HashMap::default(),
            aad_prefix: None,
        }
    }

    /// Finalize the builder and return created [`FileDecryptionProperties`]
    pub fn build(self) -> Result<FileDecryptionProperties> {
        let keys = match (self.footer_key, self.key_retriever) {
            (Some(footer_key), None) => DecryptionKeys::Explicit(ExplicitDecryptionKeys {
                footer_key,
                column_keys: self.column_keys,
            }),
            (None, Some(key_retriever)) => {
                if !self.column_keys.is_empty() {
                    return Err(general_err!(
                        "Cannot specify column keys directly when using a key retriever"
                    ));
                }
                DecryptionKeys::ViaRetriever(key_retriever)
            }
            _ => {
                unreachable!()
            }
        };
        Ok(FileDecryptionProperties {
            keys,
            aad_prefix: self.aad_prefix,
        })
    }

    /// Specify the expected AAD prefix to be used for decryption.
    /// This must be set if the file was written with an AAD prefix and the
    /// prefix is not stored in the file metadata.
    pub fn with_aad_prefix(mut self, value: Vec<u8>) -> Self {
        self.aad_prefix = Some(value);
        self
    }

    /// Specify the decryption key to use for a column
    pub fn with_column_key(mut self, column_name: &str, decryption_key: Vec<u8>) -> Self {
        self.column_keys
            .insert(column_name.to_string(), decryption_key);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileDecryptor {
    decryption_properties: FileDecryptionProperties,
    footer_decryptor: Arc<dyn BlockDecryptor>,
    file_aad: Vec<u8>,
}

impl PartialEq for FileDecryptor {
    fn eq(&self, other: &Self) -> bool {
        self.decryption_properties == other.decryption_properties && self.file_aad == other.file_aad
    }
}

impl FileDecryptor {
    pub(crate) fn new(
        decryption_properties: &FileDecryptionProperties,
        footer_key_metadata: Option<&[u8]>,
        aad_file_unique: Vec<u8>,
        aad_prefix: Vec<u8>,
    ) -> Result<Self> {
        let file_aad = [aad_prefix.as_slice(), aad_file_unique.as_slice()].concat();
        let footer_key = decryption_properties.footer_key(footer_key_metadata)?;
        let footer_decryptor = RingGcmBlockDecryptor::new(&footer_key).map_err(|e| {
            general_err!(
                "Invalid footer key. {}",
                e.to_string().replace("Parquet error: ", "")
            )
        })?;

        Ok(Self {
            footer_decryptor: Arc::new(footer_decryptor),
            decryption_properties: decryption_properties.clone(),
            file_aad,
        })
    }

    pub(crate) fn get_footer_decryptor(&self) -> Result<Arc<dyn BlockDecryptor>> {
        Ok(self.footer_decryptor.clone())
    }

    pub(crate) fn get_column_data_decryptor(
        &self,
        column_name: &str,
        key_metadata: Option<&[u8]>,
    ) -> Result<Arc<dyn BlockDecryptor>> {
        let column_key = self
            .decryption_properties
            .column_key(column_name, key_metadata)?;
        Ok(Arc::new(RingGcmBlockDecryptor::new(&column_key)?))
    }

    pub(crate) fn get_column_metadata_decryptor(
        &self,
        column_name: &str,
        key_metadata: Option<&[u8]>,
    ) -> Result<Arc<dyn BlockDecryptor>> {
        // Once GCM CTR mode is implemented, data and metadata decryptors may be different
        self.get_column_data_decryptor(column_name, key_metadata)
    }

    pub(crate) fn file_aad(&self) -> &Vec<u8> {
        &self.file_aad
    }
}
