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

//! Encryption implementation specific to Parquet, as described
//! in the [spec](https://github.com/apache/parquet-format/blob/master/Encryption.md).

use crate::encryption::decryption::BlockDecryptor;
use crate::errors::{ParquetError, Result};
use std::sync::Arc;

#[derive(PartialEq)]
pub(crate) enum ModuleType {
    Footer = 0,
    ColumnMetaData = 1,
    DataPage = 2,
    DictionaryPage = 3,
    DataPageHeader = 4,
    DictionaryPageHeader = 5,
}

pub fn create_footer_aad(file_aad: &[u8]) -> Result<Vec<u8>> {
    create_module_aad(file_aad, ModuleType::Footer, 0, 0, None)
}

pub(crate) fn create_page_aad(
    file_aad: &[u8],
    module_type: ModuleType,
    row_group_ordinal: usize,
    column_ordinal: usize,
    page_ordinal: Option<usize>,
) -> Result<Vec<u8>> {
    create_module_aad(
        file_aad,
        module_type,
        row_group_ordinal,
        column_ordinal,
        page_ordinal,
    )
}

fn create_module_aad(
    file_aad: &[u8],
    module_type: ModuleType,
    row_group_ordinal: usize,
    column_ordinal: usize,
    page_ordinal: Option<usize>,
) -> Result<Vec<u8>> {
    let module_buf = [module_type as u8];

    if module_buf[0] == (ModuleType::Footer as u8) {
        let mut aad = Vec::with_capacity(file_aad.len() + 1);
        aad.extend_from_slice(file_aad);
        aad.extend_from_slice(module_buf.as_ref());
        return Ok(aad);
    }

    if row_group_ordinal > i16::MAX as usize {
        return Err(general_err!(
            "Encrypted parquet files can't have more than {} row groups: {}",
            i16::MAX,
            row_group_ordinal
        ));
    }
    if column_ordinal > i16::MAX as usize {
        return Err(general_err!(
            "Encrypted parquet files can't have more than {} columns: {}",
            i16::MAX,
            column_ordinal
        ));
    }

    if module_buf[0] != (ModuleType::DataPageHeader as u8)
        && module_buf[0] != (ModuleType::DataPage as u8)
    {
        let mut aad = Vec::with_capacity(file_aad.len() + 5);
        aad.extend_from_slice(file_aad);
        aad.extend_from_slice(module_buf.as_ref());
        aad.extend_from_slice((row_group_ordinal as i16).to_le_bytes().as_ref());
        aad.extend_from_slice((column_ordinal as i16).to_le_bytes().as_ref());
        return Ok(aad);
    }

    let page_ordinal =
        page_ordinal.ok_or_else(|| general_err!("Page ordinal must be set for data pages"))?;

    if page_ordinal > i16::MAX as usize {
        return Err(general_err!(
            "Encrypted parquet files can't have more than {} pages per column chunk: {}",
            i16::MAX,
            page_ordinal
        ));
    }

    let mut aad = Vec::with_capacity(file_aad.len() + 7);
    aad.extend_from_slice(file_aad);
    aad.extend_from_slice(module_buf.as_ref());
    aad.extend_from_slice((row_group_ordinal as i16).to_le_bytes().as_ref());
    aad.extend_from_slice((column_ordinal as i16).to_le_bytes().as_ref());
    aad.extend_from_slice((page_ordinal as i16).to_le_bytes().as_ref());
    Ok(aad)
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
