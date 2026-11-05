// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// The methods below mirror the signatures of the real `PageEncryptor`, so that the
// callers do not need a `cfg` of their own.
#![expect(
    clippy::needless_pass_by_ref_mut,
    reason = "mirrors the encryption-enabled `PageEncryptor`"
)]

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
