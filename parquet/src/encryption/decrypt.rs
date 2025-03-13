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
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

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
    pub(crate) fn new(
        row_group_idx: usize,
        column_ordinal: usize,
        data_decryptor: Arc<dyn BlockDecryptor>,
        metadata_decryptor: Arc<dyn BlockDecryptor>,
        file_aad: Vec<u8>,
    ) -> Self {
        Self {
            row_group_idx,
            column_ordinal,
            page_ordinal: None,
            dictionary_page: false,
            data_decryptor,
            metadata_decryptor,
            file_aad,
        }
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

/// FileDecryptionProperties hold keys and AAD data required to decrypt a Parquet file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileDecryptionProperties {
    footer_key: Vec<u8>,
    column_keys: HashMap<String, Vec<u8>>,
    pub(crate) aad_prefix: Option<Vec<u8>>,
}

impl FileDecryptionProperties {
    /// Returns a new FileDecryptionProperties builder
    pub fn builder(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        DecryptionPropertiesBuilder::new(footer_key)
    }
}

pub struct DecryptionPropertiesBuilder {
    footer_key: Vec<u8>,
    column_keys: HashMap<String, Vec<u8>>,
    aad_prefix: Option<Vec<u8>>,
}

impl DecryptionPropertiesBuilder {
    pub fn new(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        Self {
            footer_key,
            column_keys: HashMap::default(),
            aad_prefix: None,
        }
    }

    pub fn build(self) -> Result<FileDecryptionProperties> {
        Ok(FileDecryptionProperties {
            footer_key: self.footer_key,
            column_keys: self.column_keys,
            aad_prefix: self.aad_prefix,
        })
    }

    pub fn with_aad_prefix(mut self, value: Vec<u8>) -> Self {
        self.aad_prefix = Some(value);
        self
    }

    pub fn with_column_key(mut self, column_name: &str, decryption_key: Vec<u8>) -> Self {
        self.column_keys
            .insert(column_name.to_string(), decryption_key);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileDecryptor {
    decryption_properties: FileDecryptionProperties,
    footer_decryptor: Option<Arc<dyn BlockDecryptor>>,
    file_aad: Vec<u8>,
}

impl PartialEq for FileDecryptor {
    fn eq(&self, other: &Self) -> bool {
        self.decryption_properties == other.decryption_properties
    }
}

impl FileDecryptor {
    pub(crate) fn new(
        decryption_properties: &FileDecryptionProperties,
        aad_file_unique: Vec<u8>,
        aad_prefix: Vec<u8>,
    ) -> Result<Self, ParquetError> {
        let file_aad = [aad_prefix.as_slice(), aad_file_unique.as_slice()].concat();
        // todo decr: if no key available yet (not set in properties, should be retrieved from metadata)
        let footer_decryptor = RingGcmBlockDecryptor::new(&decryption_properties.footer_key)
            .map_err(|e| {
                general_err!(
                    "Invalid footer key. {}",
                    e.to_string().replace("Parquet error: ", "")
                )
            })?;
        Ok(Self {
            footer_decryptor: Some(Arc::new(footer_decryptor)),
            decryption_properties: decryption_properties.clone(),
            file_aad,
        })
    }

    pub(crate) fn get_footer_decryptor(&self) -> Result<Arc<dyn BlockDecryptor>, ParquetError> {
        Ok(self.footer_decryptor.clone().unwrap())
    }

    pub(crate) fn get_column_data_decryptor(
        &self,
        column_name: &str,
    ) -> Result<Arc<dyn BlockDecryptor>, ParquetError> {
        match self.decryption_properties.column_keys.get(column_name) {
            Some(column_key) => Ok(Arc::new(RingGcmBlockDecryptor::new(column_key)?)),
            None => self.get_footer_decryptor(),
        }
    }

    pub(crate) fn get_column_metadata_decryptor(
        &self,
        column_name: &str,
    ) -> Result<Arc<dyn BlockDecryptor>, ParquetError> {
        // Once GCM CTR mode is implemented, data and metadata decryptors may be different
        self.get_column_data_decryptor(column_name)
    }

    pub(crate) fn file_aad(&self) -> &Vec<u8> {
        &self.file_aad
    }

    pub(crate) fn is_column_encrypted(&self, column_name: &str) -> bool {
        // Column is encrypted if either uniform encryption is used or an encryption key is set for the column
        match self.decryption_properties.column_keys.is_empty() {
            false => self
                .decryption_properties
                .column_keys
                .contains_key(column_name),
            true => true,
        }
    }
}
