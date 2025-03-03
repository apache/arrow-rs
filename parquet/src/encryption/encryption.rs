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
use thrift::protocol::TCompactOutputProtocol;
use crate::encryption::ciphers::{BlockEncryptor, RingGcmBlockEncryptor};
use crate::file::writer::TrackedWrite;
use crate::thrift::TSerializable;

#[derive(Debug, Clone, PartialEq)]
pub struct FileEncryptionProperties {
    encrypt_footer: bool,
    footer_key: Vec<u8>,
    column_keys: Option<HashMap<Vec<u8>, Vec<u8>>>,
    pub(crate) aad_prefix: Option<Vec<u8>>,
}

impl FileEncryptionProperties {
    pub fn builder(footer_key: Vec<u8>) -> EncryptionPropertiesBuilder {
        EncryptionPropertiesBuilder::new(footer_key)
    }

    pub fn encrypt_footer(&self) -> bool {
        self.encrypt_footer
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
    file_encryption_properties: FileEncryptionProperties,
    file_aad: Vec<u8>,
}

impl FileEncryptor {
    pub(crate) fn new(
        file_encryption_properties: FileEncryptionProperties, file_aad: Option<Vec<u8>>,
    ) -> Self {
        Self {
            file_encryption_properties: file_encryption_properties,
            file_aad: file_aad.unwrap_or_default(),
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
        RingGcmBlockEncryptor::new(&self.file_encryption_properties.footer_key.clone())
    }
}

pub(crate) fn encrypt_object<T: TSerializable, W: Write>(
    object: T,
    file_encryption_properties: &FileEncryptionProperties,
    sink: &mut TrackedWrite<W>,
) -> Result<(), crate::errors::ParquetError> {
    // todo: encrypt
    let mut buffer: Vec<u8> = vec![];
    {
        let mut sink = TrackedWrite::new(&mut buffer);
        let mut unencrypted_protocol = TCompactOutputProtocol::new(&mut sink);
        object.write_to_out_protocol(&mut unencrypted_protocol)?;
    }

    // todo: concat aad components e.g. let file_aad = [aad_prefix.as_slice(), aad_file_unique.as_slice()].concat();
    let aad_prefix = file_encryption_properties
        .aad_prefix
        .clone()
        .unwrap_or(Vec::new());
    let encryptor =
        FileEncryptor::new(file_encryption_properties.clone(), Some(aad_prefix.clone()));
    let encrypted_buffer = encryptor
        .get_footer_encryptor()
        .encrypt(buffer.as_ref(), aad_prefix.as_slice());

    sink.write_all(encrypted_buffer.as_ref())?;
    Ok(())
}