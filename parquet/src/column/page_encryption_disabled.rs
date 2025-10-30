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
use crate::errors::Result;
use crate::file::metadata::thrift::PageHeader;
use std::io::Write;

#[derive(Debug)]
/// Dummy PageEncryptor struct that can never be instantiated,
/// provided to support compilation without the encryption feature enabled.
pub(crate) struct PageEncryptor {
    _empty: (),
}

impl PageEncryptor {
    pub fn increment_page(&mut self) {}

    pub fn encrypt_compressed_page(&mut self, _page: CompressedPage) -> Result<CompressedPage> {
        unreachable!("The encryption feature is disabled")
    }

    pub fn encrypt_page_header<W: Write>(
        &mut self,
        _page_header: &PageHeader,
        _sink: &mut W,
    ) -> Result<()> {
        unreachable!("The encryption feature is disabled")
    }
}
