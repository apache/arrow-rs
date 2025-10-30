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

use crate::basic::PageType;
use crate::column::page::CompressedPage;
use crate::encryption::ciphers::BlockEncryptor;
use crate::encryption::encrypt::{FileEncryptor, encrypt_thrift_object};
use crate::encryption::modules::{ModuleType, create_module_aad};
use crate::errors::ParquetError;
use crate::errors::Result;
use crate::file::metadata::thrift::PageHeader;
use bytes::Bytes;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
/// Encrypts page headers and page data for columns
pub(crate) struct PageEncryptor {
    file_encryptor: Arc<FileEncryptor>,
    block_encryptor: Box<dyn BlockEncryptor>,
    row_group_index: usize,
    column_index: usize,
    page_index: usize,
}

impl PageEncryptor {
    /// Create a [`PageEncryptor`] for a column if it should be encrypted
    pub fn create_if_column_encrypted(
        file_encryptor: &Option<Arc<FileEncryptor>>,
        row_group_index: usize,
        column_index: usize,
        column_path: &str,
    ) -> Result<Option<Self>> {
        match file_encryptor {
            Some(file_encryptor) if file_encryptor.is_column_encrypted(column_path) => {
                let block_encryptor = file_encryptor.get_column_encryptor(column_path)?;
                Ok(Some(Self {
                    file_encryptor: file_encryptor.clone(),
                    block_encryptor,
                    row_group_index,
                    column_index,
                    page_index: 0,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Update the page index after a data page has been processed
    pub fn increment_page(&mut self) {
        self.page_index += 1;
    }

    fn encrypt_page(&mut self, page: &CompressedPage) -> Result<Vec<u8>> {
        let module_type = if page.compressed_page().is_data_page() {
            ModuleType::DataPage
        } else {
            ModuleType::DictionaryPage
        };
        let aad = create_module_aad(
            self.file_encryptor.file_aad(),
            module_type,
            self.row_group_index,
            self.column_index,
            Some(self.page_index),
        )?;
        let encrypted_buffer = self.block_encryptor.encrypt(page.data(), &aad)?;

        Ok(encrypted_buffer)
    }

    /// Encrypt compressed column page data
    pub fn encrypt_compressed_page(&mut self, page: CompressedPage) -> Result<CompressedPage> {
        let encrypted_page = self.encrypt_page(&page)?;
        Ok(page.with_new_compressed_buffer(Bytes::from(encrypted_page)))
    }

    /// Encrypt a column page header
    pub fn encrypt_page_header<W: Write>(
        &mut self,
        page_header: &PageHeader,
        sink: &mut W,
    ) -> Result<()> {
        let module_type = match page_header.r#type {
            PageType::DATA_PAGE => ModuleType::DataPageHeader,
            PageType::DATA_PAGE_V2 => ModuleType::DataPageHeader,
            PageType::DICTIONARY_PAGE => ModuleType::DictionaryPageHeader,
            _ => {
                return Err(general_err!(
                    "Unsupported page type for page header encryption: {:?}",
                    page_header.r#type
                ));
            }
        };
        let aad = create_module_aad(
            self.file_encryptor.file_aad(),
            module_type,
            self.row_group_index,
            self.column_index,
            Some(self.page_index),
        )?;

        encrypt_thrift_object(page_header, &mut self.block_encryptor, sink, &aad)
    }
}
