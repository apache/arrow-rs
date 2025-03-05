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

use crate::column::page::CompressedPage;
use crate::encryption::ciphers::BlockEncryptor;
use crate::encryption::encrypt::{encrypt_object, FileEncryptor};
use crate::encryption::modules::{create_module_aad, ModuleType};
use crate::errors::ParquetError;
use crate::format::{PageHeader, PageType};
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
pub struct PageEncryptor {
    file_encryptor: Arc<FileEncryptor>,
    row_group_index: usize,
    column_index: usize,
    page_index: usize,
    column_path: Vec<u8>,
}

impl PageEncryptor {
    pub fn new(
        file_encryptor: Arc<FileEncryptor>,
        row_group_index: usize,
        column_index: usize,
        column_path: Vec<u8>,
    ) -> Self {
        Self {
            file_encryptor,
            row_group_index,
            column_index,
            page_index: 0,
            column_path,
        }
    }

    pub fn increment_page(&mut self) {
        self.page_index += 1;
    }

    pub fn encrypt_page(&self, page: &CompressedPage) -> crate::errors::Result<Vec<u8>> {
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
        let mut encryptor = self.file_encryptor.get_footer_encryptor();
        let encrypted_buffer = encryptor.encrypt(page.data(), &aad);

        Ok(encrypted_buffer)
    }

    pub fn encrypt_page_header<W: Write>(
        &self,
        page_header: PageHeader,
        sink: &mut W,
    ) -> crate::errors::Result<()> {
        let module_type = match page_header.type_ {
            PageType::DATA_PAGE => ModuleType::DataPageHeader,
            PageType::DATA_PAGE_V2 => ModuleType::DataPageHeader,
            PageType::DICTIONARY_PAGE => ModuleType::DictionaryPageHeader,
            _ => {
                return Err(general_err!(
                    "Unsupported page type for page header encryption: {:?}",
                    page_header.type_
                ))
            }
        };
        let aad = create_module_aad(
            self.file_encryptor.file_aad(),
            module_type,
            self.row_group_index,
            self.column_index,
            Some(self.page_index),
        )?;

        encrypt_object(page_header, &self.file_encryptor, sink, &aad)
    }
}
