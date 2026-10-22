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

//! Internal metadata parsing routines
//!
//! These functions parse thrift-encoded metadata from a byte slice
//! into the corresponding Rust structures

use std::ops::Range;

use bytes::Bytes;

use crate::errors::{ParquetError, Result};
use crate::file::metadata::page_index::{PageIndex, PageIndexBuilder, PageIndexProvider};
use crate::file::metadata::thrift::parquet_metadata_from_bytes;
use crate::file::metadata::{
    ColumnChunkMask, ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions,
};

use crate::file::page_index::column_index::ColumnIndexMetaData;
use crate::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use crate::file::page_index::offset_index::OffsetIndexMetaData;

/// Helper struct for metadata parsing
///
/// This structure parses thrift-encoded bytes into the correct Rust structs,
/// such as [`ParquetMetaData`], handling decryption if necessary.
//
// Note this structure is used to minimize the number of
// places to add `#[cfg(feature = "encryption")]` checks.
pub(crate) use inner::MetadataParser;

#[cfg(feature = "encryption")]
mod inner {
    use std::sync::Arc;

    use super::*;
    use crate::encryption::decrypt::FileDecryptionProperties;

    /// API for decoding metadata that may be encrypted
    #[derive(Debug, Default)]
    pub(crate) struct MetadataParser {
        // the credentials and keys needed to decrypt metadata
        file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
        // metadata parsing options
        metadata_options: Option<Arc<ParquetMetaDataOptions>>,
    }

    impl MetadataParser {
        pub(crate) fn new() -> Self {
            MetadataParser::default()
        }

        pub(crate) fn with_file_decryption_properties(
            mut self,
            file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
        ) -> Self {
            self.file_decryption_properties = file_decryption_properties;
            self
        }

        pub(crate) fn with_metadata_options(
            self,
            options: Option<Arc<ParquetMetaDataOptions>>,
        ) -> Self {
            Self {
                metadata_options: options,
                ..self
            }
        }

        pub(crate) fn decode_metadata(
            &self,
            buf: &[u8],
            encrypted_footer: bool,
        ) -> Result<ParquetMetaData> {
            if encrypted_footer || self.file_decryption_properties.is_some() {
                crate::file::metadata::thrift::encryption::parquet_metadata_with_encryption(
                    self.file_decryption_properties.as_ref(),
                    encrypted_footer,
                    buf,
                    self.metadata_options.as_deref(),
                )
            } else {
                decode_metadata(buf, self.metadata_options.as_deref())
            }
        }
    }

    pub(super) fn parse_single_column_index(
        bytes: &[u8],
        metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        row_group_index: usize,
        col_index: usize,
    ) -> Result<ColumnIndexMetaData> {
        use crate::encryption::decrypt::CryptoContext;
        match &column.column_crypto_metadata {
            Some(crypto_metadata) => {
                let file_decryptor = metadata.file_decryptor.as_ref().ok_or_else(|| {
                    general_err!("Cannot decrypt column index, no file decryptor set")
                })?;
                let crypto_context = CryptoContext::for_column(
                    file_decryptor,
                    crypto_metadata,
                    row_group_index,
                    col_index,
                )?;
                let column_decryptor = crypto_context.metadata_decryptor();
                let aad = crypto_context.create_column_index_aad()?;
                let plaintext = column_decryptor.decrypt(bytes, &aad)?;
                decode_column_index(&plaintext, column.column_type())
            }
            None => decode_column_index(bytes, column.column_type()),
        }
    }

    pub(super) fn parse_single_offset_index(
        bytes: &[u8],
        metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        row_group_index: usize,
        col_index: usize,
    ) -> Result<OffsetIndexMetaData> {
        use crate::encryption::decrypt::CryptoContext;
        match &column.column_crypto_metadata {
            Some(crypto_metadata) => {
                let file_decryptor = metadata.file_decryptor.as_ref().ok_or_else(|| {
                    general_err!("Cannot decrypt offset index, no file decryptor set")
                })?;
                let crypto_context = CryptoContext::for_column(
                    file_decryptor,
                    crypto_metadata,
                    row_group_index,
                    col_index,
                )?;
                let column_decryptor = crypto_context.metadata_decryptor();
                let aad = crypto_context.create_offset_index_aad()?;
                let plaintext = column_decryptor.decrypt(bytes, &aad)?;
                decode_offset_index(&plaintext)
            }
            None => decode_offset_index(bytes),
        }
    }
}

#[cfg(not(feature = "encryption"))]
mod inner {
    use super::*;
    use std::sync::Arc;
    /// parallel implementation when encryption feature is not enabled
    ///
    /// This has the same API as the encryption-enabled version
    #[derive(Debug, Default)]
    pub(crate) struct MetadataParser {
        // metadata parsing options
        metadata_options: Option<Arc<ParquetMetaDataOptions>>,
    }

    impl MetadataParser {
        pub(crate) fn new() -> Self {
            MetadataParser::default()
        }

        pub(crate) fn with_metadata_options(
            self,
            options: Option<Arc<ParquetMetaDataOptions>>,
        ) -> Self {
            Self {
                metadata_options: options,
            }
        }

        pub(crate) fn decode_metadata(
            &self,
            buf: &[u8],
            encrypted_footer: bool,
        ) -> Result<ParquetMetaData> {
            if encrypted_footer {
                Err(general_err!(
                    "Parquet file has an encrypted footer but the encryption feature is disabled"
                ))
            } else {
                decode_metadata(buf, self.metadata_options.as_deref())
            }
        }
    }

    pub(super) fn parse_single_column_index(
        bytes: &[u8],
        _metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        _row_group_index: usize,
        _col_index: usize,
    ) -> Result<ColumnIndexMetaData> {
        decode_column_index(bytes, column.column_type())
    }

    pub(super) fn parse_single_offset_index(
        bytes: &[u8],
        _metadata: &ParquetMetaData,
        _column: &ColumnChunkMetaData,
        _row_group_index: usize,
        _col_index: usize,
    ) -> Result<OffsetIndexMetaData> {
        decode_offset_index(bytes)
    }
}

/// Decodes [`ParquetMetaData`] from the provided bytes.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thrift compact binary protocol, as specified
/// by the [Parquet Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
pub(crate) fn decode_metadata(
    buf: &[u8],
    options: Option<&ParquetMetaDataOptions>,
) -> Result<ParquetMetaData> {
    parquet_metadata_from_bytes(buf, options)
}

/// Parses page indexes from the provided bytes.
///
/// Arguments
/// * `metadata` - The footer metadata describing the page index locations.
/// * `column_index_policy` - The policy for handling column index parsing (e.g.,
///   Required, Optional, Skip).
/// * `offset_index_policy` - The policy for handling offset index parsing (e.g.,
///   Required, Optional, Skip).
/// * `column_index_mask` - The row groups and leaf columns whose column indexes are parsed.
///   Ignored when `column_index_policy` is [`PageIndexPolicy::Skip`].
/// * `offset_index_mask` - The row groups and leaf columns whose offset indexes are parsed.
///   Ignored when `offset_index_policy` is [`PageIndexPolicy::Skip`].
/// * `bytes` - The byte slice containing the page index data.
/// * `start_offset` - The offset where `bytes` begin in the file.
pub(crate) fn parse_page_index(
    metadata: &ParquetMetaData,
    column_index_policy: PageIndexPolicy,
    offset_index_policy: PageIndexPolicy,
    column_index_mask: &ColumnChunkMask,
    offset_index_mask: &ColumnChunkMask,
    bytes: &Bytes,
    start_offset: u64,
) -> Result<Option<PageIndex>> {
    let num_row_groups = metadata.num_row_groups();
    let num_columns = metadata.file_metadata().schema_descr().num_columns();
    let mut builder = PageIndexBuilder::default();

    if column_index_policy != PageIndexPolicy::Skip {
        builder.allocate_column_indexes(num_row_groups, num_columns);
        parse_column_index(
            metadata,
            column_index_policy,
            column_index_mask,
            &mut builder,
            bytes,
            start_offset,
        )?;
    }
    if offset_index_policy != PageIndexPolicy::Skip {
        builder.allocate_offset_indexes(num_row_groups, num_columns);
        parse_offset_index(
            metadata,
            offset_index_policy,
            offset_index_mask,
            &mut builder,
            bytes,
            start_offset,
        )?;
    }

    let page_index = builder.build();
    if page_index.has_column_indexes() || page_index.has_offset_indexes() {
        Ok(Some(page_index))
    } else {
        Ok(None)
    }
}

fn parse_column_index(
    metadata: &ParquetMetaData,
    column_index_policy: PageIndexPolicy,
    mask: &ColumnChunkMask,
    page_index_builder: &mut PageIndexBuilder,
    bytes: &Bytes,
    start_offset: u64,
) -> Result<()> {
    if column_index_policy == PageIndexPolicy::Skip {
        return Ok(());
    }
    for rg_idx in mask.row_group_indices(metadata.num_row_groups()) {
        let rg = metadata.row_group(rg_idx);
        for col_idx in mask.column_indices(rg.num_columns()) {
            let col = rg.column(col_idx);
            if let Some(r) = col.column_index_range() {
                let idx_bytes = get_index_bytes(bytes, start_offset, r)?;
                let idx =
                    inner::parse_single_column_index(idx_bytes, metadata, col, rg_idx, col_idx)?;
                page_index_builder.put_column_index(idx, rg_idx, col_idx);
            }
        }
    }

    Ok(())
}

fn parse_offset_index(
    metadata: &ParquetMetaData,
    offset_index_policy: PageIndexPolicy,
    mask: &ColumnChunkMask,
    page_index_builder: &mut PageIndexBuilder,
    bytes: &Bytes,
    start_offset: u64,
) -> Result<()> {
    if offset_index_policy == PageIndexPolicy::Skip {
        return Ok(());
    }
    for rg_idx in mask.row_group_indices(metadata.num_row_groups()) {
        let rg = metadata.row_group(rg_idx);
        for col_idx in mask.column_indices(rg.num_columns()) {
            let col = rg.column(col_idx);
            if let Some(r) = col.offset_index_range() {
                let idx_bytes = get_index_bytes(bytes, start_offset, r)?;
                let idx =
                    inner::parse_single_offset_index(idx_bytes, metadata, col, rg_idx, col_idx)?;
                page_index_builder.put_offset_index(idx, rg_idx, col_idx);
            } else if offset_index_policy == PageIndexPolicy::Required {
                return Err(general_err!("missing offset index"));
            }
        }
    }

    Ok(())
}

fn get_index_bytes(bytes: &[u8], start_offset: u64, range: Range<u64>) -> Result<&[u8]> {
    let start = usize::try_from(range.start - start_offset)?;
    let end = usize::try_from(range.end - start_offset)?;
    Ok(&bytes[start..end])
}
