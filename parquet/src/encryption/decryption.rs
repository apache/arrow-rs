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

use crate::errors::Result;
use ring::aead::{Aad, LessSafeKey, UnboundKey, AES_128_GCM};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::sync::Arc;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const SIZE_LEN: usize = 4;

pub trait BlockDecryptor: Debug + Send + Sync {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub(crate) struct RingGcmBlockDecryptor {
    key: LessSafeKey,
}

impl RingGcmBlockDecryptor {
    pub(crate) fn new(key_bytes: &[u8]) -> Self {
        // todo support other key sizes
        let key = UnboundKey::new(&AES_128_GCM, key_bytes).unwrap();

        Self {
            key: LessSafeKey::new(key),
        }
    }
}

impl BlockDecryptor for RingGcmBlockDecryptor {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut result =
            Vec::with_capacity(length_and_ciphertext.len() - SIZE_LEN - NONCE_LEN - TAG_LEN);
        result.extend_from_slice(&length_and_ciphertext[SIZE_LEN + NONCE_LEN..]);

        let nonce = ring::aead::Nonce::try_assume_unique_for_key(
            &length_and_ciphertext[SIZE_LEN..SIZE_LEN + NONCE_LEN],
        )?;

        self.key.open_in_place(nonce, Aad::from(aad), &mut result)?;

        // Truncate result to remove the tag
        result.resize(result.len() - TAG_LEN, 0u8);
        Ok(result)
    }
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

    pub fn has_column_keys(&self) -> bool {
        self.column_keys.is_some()
    }

    pub fn aad_prefix(&self) -> Option<Vec<u8>> {
        self.aad_prefix.clone()
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

    pub fn with_column_key(mut self, key: Vec<u8>, value: Vec<u8>) -> Self {
        let mut column_keys = self.column_keys.unwrap_or_default();
        column_keys.insert(key, value);
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
