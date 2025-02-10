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
use crate::errors::Result;
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

#[derive(Debug, Clone)]
pub struct CryptoContext {
    pub(crate) row_group_ordinal: usize,
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
    pub fn new(
        row_group_ordinal: usize,
        column_ordinal: usize,
        data_decryptor: Arc<dyn BlockDecryptor>,
        metadata_decryptor: Arc<dyn BlockDecryptor>,
        file_aad: Vec<u8>,
    ) -> Self {
        Self {
            row_group_ordinal,
            column_ordinal,
            page_ordinal: None,
            dictionary_page: false,
            data_decryptor,
            metadata_decryptor,
            file_aad,
        }
    }

    pub fn with_page_ordinal(&self, page_ordinal: usize) -> Self {
        Self {
            row_group_ordinal: self.row_group_ordinal,
            column_ordinal: self.column_ordinal,
            page_ordinal: Some(page_ordinal),
            dictionary_page: false,
            data_decryptor: self.data_decryptor.clone(),
            metadata_decryptor: self.metadata_decryptor.clone(),
            file_aad: self.file_aad.clone(),
        }
    }

    pub(crate) fn create_page_aad(
        &self,
        module_type: ModuleType,
    ) -> crate::errors::Result<Vec<u8>> {
        create_module_aad(
            self.file_aad(),
            module_type,
            self.row_group_ordinal,
            self.column_ordinal,
            self.page_ordinal,
        )
    }

    pub fn for_dictionary_page(&self) -> Self {
        Self {
            row_group_ordinal: self.row_group_ordinal,
            column_ordinal: self.column_ordinal,
            page_ordinal: self.page_ordinal,
            dictionary_page: true,
            data_decryptor: self.data_decryptor.clone(),
            metadata_decryptor: self.metadata_decryptor.clone(),
            file_aad: self.file_aad.clone(),
        }
    }

    pub fn data_decryptor(&self) -> &Arc<dyn BlockDecryptor> {
        &self.data_decryptor
    }

    pub fn metadata_decryptor(&self) -> &Arc<dyn BlockDecryptor> {
        &self.metadata_decryptor
    }

    pub fn file_aad(&self) -> &Vec<u8> {
        &self.file_aad
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileDecryptionProperties {
    footer_key: Vec<u8>,
    column_keys: Option<HashMap<Vec<u8>, Vec<u8>>>,
    aad_prefix: Option<Vec<u8>>,
}

impl FileDecryptionProperties {
    pub fn builder(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        DecryptionPropertiesBuilder::new(footer_key)
    }
}

pub struct DecryptionPropertiesBuilder {
    footer_key: Vec<u8>,
    column_keys: Option<HashMap<Vec<u8>, Vec<u8>>>,
    aad_prefix: Option<Vec<u8>>,
}

impl DecryptionPropertiesBuilder {
    pub fn new(footer_key: Vec<u8>) -> DecryptionPropertiesBuilder {
        Self {
            footer_key,
            column_keys: None,
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

    pub fn with_column_key(mut self, column_name: Vec<u8>, decryption_key: Vec<u8>) -> Self {
        let mut column_keys = self.column_keys.unwrap_or_default();
        column_keys.insert(column_name, decryption_key);
        self.column_keys = Some(column_keys);
        self
    }
}

#[derive(Clone, Debug)]
pub struct FileDecryptor {
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
    ) -> Self {
        let file_aad = [aad_prefix.as_slice(), aad_file_unique.as_slice()].concat();
        let footer_decryptor = RingGcmBlockDecryptor::new(&decryption_properties.footer_key);

        Self {
            // todo decr: if no key available yet (not set in properties, will be retrieved from metadata)
            footer_decryptor: Some(Arc::new(footer_decryptor)),
            decryption_properties: decryption_properties.clone(),
            file_aad,
        }
    }

    pub(crate) fn get_footer_decryptor(&self) -> Arc<dyn BlockDecryptor> {
        self.footer_decryptor.clone().unwrap()
    }

    pub(crate) fn get_column_data_decryptor(&self, column_name: &[u8]) -> Arc<dyn BlockDecryptor> {
        match self.decryption_properties.column_keys.as_ref() {
            None => self.get_footer_decryptor(),
            Some(column_keys) => match column_keys.get(column_name) {
                None => self.get_footer_decryptor(),
                Some(column_key) => Arc::new(RingGcmBlockDecryptor::new(column_key)),
            },
        }
    }

    pub(crate) fn get_column_metadata_decryptor(
        &self,
        column_name: &[u8],
    ) -> Arc<dyn BlockDecryptor> {
        // Once GCM CTR mode is implemented, data and metadata decryptors may be different
        self.get_column_data_decryptor(column_name)
    }

    pub(crate) fn file_aad(&self) -> &Vec<u8> {
        &self.file_aad
    }

    pub(crate) fn is_column_encrypted(&self, column_name: &[u8]) -> bool {
        // Column is encrypted if either uniform encryption is used or an encryption key is set for the column
        match self.decryption_properties.column_keys.as_ref() {
            None => true,
            Some(keys) => keys.contains_key(column_name),
        }
    }
}
