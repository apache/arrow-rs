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
use std::sync::Arc;
use crate::encryption::ciphers::{RingGcmBlockEncryptor, BlockEncryptor};

#[derive(Debug, Clone, PartialEq)]
pub struct FileEncryptionProperties {
    encrypt_footer: bool,
    footer_key: Vec<u8>,
    column_keys: Option<HashMap<Vec<u8>, Vec<u8>>>,
    aad_prefix: Option<Vec<u8>>,
}

impl FileEncryptionProperties {
    pub fn builder(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        EncryptionPropertiesBuilder::new(footer_key)
    }
}

pub struct EncryptionPropertiesBuilder {
    footer_key: Vec<u8>,
    column_keys: Option<HashMap<Vec<u8>, Vec<u8>>>,
    aad_prefix: Option<Vec<u8>>,
}

impl EncryptionPropertiesBuilder {
    pub fn new(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        Self {
            footer_key,
            column_keys: None,
            aad_prefix: None,
        }
    }

    pub fn build(self) -> crate::errors::Result<FileEncryptionProperties> {
        Ok(FileEncryptionProperties {
            encrypt_footer: true,
            footer_key: self.footer_key,
            column_keys: self.column_keys,
            aad_prefix: self.aad_prefix,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FileEncryptor {
    encryption_properties: FileEncryptionProperties,
    footer_encryptor: Option<Arc<dyn BlockEncryptor>>,
    column_encryptors: Option<HashMap<Vec<u8>, Arc<dyn BlockEncryptor>>>,
    file_aad: Vec<u8>,
}

impl FileEncryptor {
    pub(crate) fn new(
        encryption_properties: FileEncryptionProperties, file_aad: Vec<u8>,
    ) -> Self {
        let footer_encryptor = RingGcmBlockEncryptor::new(&encryption_properties.footer_key.clone());
        let mut column_encryptors: HashMap<Vec<u8>, Arc<dyn BlockEncryptor>> = HashMap::new();
        if let Some(column_keys) = encryption_properties.column_keys.clone() {
            for (column_name, key) in column_keys.iter() {
                let column_encryptor = Arc::new(RingGcmBlockEncryptor::new(key));
                column_encryptors.insert(column_name.clone(), column_encryptor);
            }
        }
        Self {
            encryption_properties,
            footer_encryptor: Some(Arc::new(footer_encryptor)),
            column_encryptors: Some(column_encryptors),
            file_aad,
        }
    }
}
