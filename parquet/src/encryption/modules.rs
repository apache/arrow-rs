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

use crate::errors::ParquetError;

#[derive(PartialEq)]
pub(crate) enum ModuleType {
    Footer = 0,
    ColumnMetaData = 1,
    DataPage = 2,
    DictionaryPage = 3,
    DataPageHeader = 4,
    DictionaryPageHeader = 5,
    ColumnIndex = 6,
    OffsetIndex = 7,
    _BloomFilterHeader = 8,
    _BloomFilterBitset = 9,
}

pub fn create_footer_aad(file_aad: &[u8]) -> crate::errors::Result<Vec<u8>> {
    create_module_aad(file_aad, ModuleType::Footer, 0, 0, None)
}

pub(crate) fn create_module_aad(
    file_aad: &[u8],
    module_type: ModuleType,
    row_group_idx: usize,
    column_ordinal: usize,
    page_ordinal: Option<usize>,
) -> crate::errors::Result<Vec<u8>> {
    let module_buf = [module_type as u8];

    if module_buf[0] == (ModuleType::Footer as u8) {
        let mut aad = Vec::with_capacity(file_aad.len() + 1);
        aad.extend_from_slice(file_aad);
        aad.extend_from_slice(module_buf.as_ref());
        return Ok(aad);
    }

    if row_group_idx > i16::MAX as usize {
        return Err(general_err!(
            "Encrypted parquet files can't have more than {} row groups: {}",
            i16::MAX,
            row_group_idx
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
        aad.extend_from_slice((row_group_idx as i16).to_le_bytes().as_ref());
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
    aad.extend_from_slice((row_group_idx as i16).to_le_bytes().as_ref());
    aad.extend_from_slice((column_ordinal as i16).to_le_bytes().as_ref());
    aad.extend_from_slice((page_ordinal as i16).to_le_bytes().as_ref());
    Ok(aad)
}
