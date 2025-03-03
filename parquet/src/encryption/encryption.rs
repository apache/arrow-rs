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

use std::collections::HashMap;
use std::io::Write;
use ring::rand::{SecureRandom, SystemRandom};
use thrift::protocol::TCompactOutputProtocol;
use crate::encryption::ciphers::{BlockEncryptor, RingGcmBlockEncryptor};
use crate::errors::Result;
use crate::file::writer::TrackedWrite;
use crate::thrift::TSerializable;

#[derive(Debug, Clone, PartialEq)]
pub struct EncryptionKey {
    key: Vec<u8>,
    key_metadata: Option<Vec<u8>>
}

impl EncryptionKey {
    pub fn new(key: Vec<u8>) -> EncryptionKey {
        Self {
            key,
            key_metadata: None,
        }
    }

    pub fn set_metadata(&mut self, metadata: Vec<u8>) {
        self.key_metadata = Some(metadata);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileEncryptionProperties {
    encrypt_footer: bool,
    footer_key: EncryptionKey,
    column_keys: HashMap<Vec<u8>, EncryptionKey>,
    aad_prefix: Option<Vec<u8>>,
    aad_file_unique: Vec<u8>,
    file_aad: Vec<u8>,
    store_aad_prefix: bool,
}

impl FileEncryptionProperties {
    pub fn builder(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        EncryptionPropertiesBuilder::new(footer_key)
    }

    pub fn encrypt_footer(&self) -> bool {
        self.encrypt_footer
    }

    pub fn file_aad(&self) -> &[u8] {
        &self.file_aad
    }
}

pub struct EncryptionPropertiesBuilder {
    footer_key: EncryptionKey,
    column_keys: HashMap<Vec<u8>, EncryptionKey>,
    aad_prefix: Option<Vec<u8>>,
    encrypt_footer: bool,
    store_aad_prefix: bool,
}

impl EncryptionPropertiesBuilder {
    pub fn new(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        Self {
            footer_key: EncryptionKey::new(footer_key),
            column_keys: HashMap::default(),
            aad_prefix: None,
            encrypt_footer: true,
            store_aad_prefix: true,
        }
    }

    pub fn with_plaintext_footer(mut self, plaintext_footer: bool) -> Self {
        self.encrypt_footer = !plaintext_footer;
        self
    }

    pub fn with_footer_key_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.footer_key.set_metadata(metadata);
        self
    }

    pub fn with_aad_prefix_storage(mut self, store_aad_prefix: bool) -> Self {
        self.store_aad_prefix = store_aad_prefix;
        self
    }

    pub fn build(self) -> Result<FileEncryptionProperties> {
        // Generate unique AAD for file
        let rng = SystemRandom::new();
        let mut aad_file_unique = vec![0u8; 8];
        rng.fill(&mut aad_file_unique)?;

        let file_aad = match self.aad_prefix.as_ref() {
            None => aad_file_unique.clone(),
            Some(aad_prefix) => [aad_prefix.clone(), aad_file_unique.clone()].concat(),
        };

        Ok(FileEncryptionProperties {
            encrypt_footer: self.encrypt_footer,
            footer_key: self.footer_key,
            column_keys: self.column_keys,
            aad_prefix: self.aad_prefix,
            aad_file_unique,
            file_aad,
            store_aad_prefix: self.store_aad_prefix,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FileEncryptor<'a> {
    file_encryption_properties: &'a FileEncryptionProperties,
}

impl<'a> FileEncryptor<'a> {
    pub(crate) fn new(
        file_encryption_properties: &'a FileEncryptionProperties,
    ) -> Self {
        Self {
            file_encryption_properties,
        }
    }

    // let footer_encryptor = RingGcmBlockEncryptor::new(&encryption_properties.footer_key.clone());
    // let mut column_encryptors: HashMap<Vec<u8>, Arc<dyn BlockEncryptor>> = HashMap::new();
    // if let Some(column_keys) = encryption_properties.column_keys.clone() {
    // for (column_name, key) in column_keys.iter() {
    // let column_encryptor = Arc::new(RingGcmBlockEncryptor::new(key));
    // column_encryptors.insert(column_name.clone(), column_encryptor);
    // }
    // }
    pub(crate) fn get_footer_encryptor(&self) -> RingGcmBlockEncryptor {
        RingGcmBlockEncryptor::new(&self.file_encryption_properties.footer_key.key.clone())
    }
}

pub(crate) fn encrypt_object<T: TSerializable, W: Write>(
    object: T,
    file_encryption_properties: &FileEncryptionProperties,
    sink: &mut W,
    module_aad: &[u8],
) -> Result<()> {
    let mut buffer: Vec<u8> = vec![];
    {
        let mut unencrypted_protocol = TCompactOutputProtocol::new(&mut buffer);
        object.write_to_out_protocol(&mut unencrypted_protocol)?;
    }

    let encryptor = FileEncryptor::new(file_encryption_properties);
    // TODO: Get correct encryptor (footer vs column, data vs metadata)
    let encrypted_buffer = encryptor
        .get_footer_encryptor()
        .encrypt(buffer.as_ref(), module_aad);

    sink.write_all(&encrypted_buffer)?;
    Ok(())
}