// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Internal metadata parsing routines.

use crate::file::metadata::extensions::ThriftStruct;
use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

#[cfg(feature = "encryption")]
use crate::encryption::decrypt::{CryptoContext, FileDecryptionProperties};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::column_chunk::{ColumnChunkEntries, ColumnChunkKey};
use crate::file::metadata::extensions::{ObserverHandle, SiteInfo};
use crate::file::metadata::page_index::{PageIndex, PageIndexProvider};
use crate::file::metadata::thrift::parquet_metadata_from_bytes;
use crate::file::metadata::{
    ColumnChunkMask, ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions,
};
use crate::file::page_index::column_index::ColumnIndexMetaData;
use crate::file::page_index::index_reader::{
    read_column_index, read_offset_index, read_offset_index_with_listener,
};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::parquet_thrift::ThriftSliceInputProtocol;

/// The on-disk index kind, independent of extension observation.
#[derive(Clone, Copy)]
enum PageIndexKind {
    Column,
    Offset,
}

/// Configuration shared by footer and page-index parsing, including decryption.
#[derive(Debug, Default)]
pub(crate) struct MetadataParser {
    #[cfg(feature = "encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    metadata_options: Option<Arc<ParquetMetaDataOptions>>,
}

impl MetadataParser {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn with_file_decryption_properties(
        mut self,
        properties: Option<Arc<FileDecryptionProperties>>,
    ) -> Self {
        self.file_decryption_properties = properties;
        self
    }

    pub(crate) fn with_metadata_options(
        mut self,
        options: Option<Arc<ParquetMetaDataOptions>>,
    ) -> Self {
        self.metadata_options = options;
        self
    }

    pub(crate) fn decode_metadata(
        &self,
        buf: &[u8],
        encrypted_footer: bool,
    ) -> Result<ParquetMetaData> {
        #[cfg(feature = "encryption")]
        if encrypted_footer || self.file_decryption_properties.is_some() {
            return super::thrift::encryption::parquet_metadata_with_encryption(
                self.file_decryption_properties.as_ref(),
                encrypted_footer,
                buf,
                self.metadata_options.as_deref(),
            );
        }
        if encrypted_footer {
            return Err(general_err!(
                "Parquet file has an encrypted footer but the encryption feature is disabled"
            ));
        }
        decode_metadata(buf, self.metadata_options.as_deref())
    }

    /// Resolve observation once per serialized root, before choosing its protocol.
    fn observer(&self) -> Option<&ObserverHandle> {
        self.metadata_options.as_ref()?.extension_observer.as_ref()
    }

    /// Borrow plaintext indexes; only encrypted indexes need an owned buffer.
    fn index_bytes<'a>(
        bytes: &'a [u8],
        metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        row_group: usize,
        column_index: usize,
        kind: PageIndexKind,
    ) -> Result<Cow<'a, [u8]>> {
        #[cfg(feature = "encryption")]
        if let Some(crypto_metadata) = &column.column_crypto_metadata {
            let file_decryptor = metadata.file_decryptor.as_ref().ok_or_else(|| {
                general_err!(
                    "Cannot decrypt {} index, no file decryptor set",
                    match kind {
                        PageIndexKind::Column => "column",
                        PageIndexKind::Offset => "offset",
                    }
                )
            })?;
            let crypto = CryptoContext::for_column(
                file_decryptor,
                crypto_metadata,
                row_group,
                column_index,
            )?;
            let aad = match kind {
                PageIndexKind::Column => crypto.create_column_index_aad()?,
                PageIndexKind::Offset => crypto.create_offset_index_aad()?,
            };
            let plaintext = crypto.metadata_decryptor().decrypt(bytes, &aad)?;
            return Ok(Cow::Owned(plaintext));
        }
        #[cfg(not(feature = "encryption"))]
        let _ = (metadata, column, row_group, column_index, kind);
        Ok(Cow::Borrowed(bytes))
    }

    /// Parse requested indexes from an already fetched range. Policies and masks
    /// control which roots are decoded; observation never changes those choices.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn parse_page_index(
        &self,
        metadata: &ParquetMetaData,
        column_index_policy: PageIndexPolicy,
        offset_index_policy: PageIndexPolicy,
        column_index_mask: &ColumnChunkMask,
        offset_index_mask: &ColumnChunkMask,
        bytes: &[u8],
        start_offset: u64,
    ) -> Result<Option<PageIndex>> {
        let column_indexes = if column_index_policy == PageIndexPolicy::Skip {
            None
        } else {
            Some(self.parse_column_index(metadata, column_index_mask, bytes, start_offset)?)
        };
        let offset_indexes = if offset_index_policy == PageIndexPolicy::Skip {
            None
        } else {
            Some(self.parse_offset_index(
                metadata,
                offset_index_policy,
                offset_index_mask,
                bytes,
                start_offset,
            )?)
        };
        let page_index = PageIndex::from_entries(column_indexes, offset_indexes);
        if page_index.has_column_indexes() || page_index.has_offset_indexes() {
            Ok(Some(page_index))
        } else {
            Ok(None)
        }
    }

    fn parse_column_index(
        &self,
        metadata: &ParquetMetaData,
        mask: &ColumnChunkMask,
        bytes: &[u8],
        start_offset: u64,
    ) -> Result<ColumnChunkEntries<ColumnIndexMetaData>> {
        let mut entries =
            reserve_index_entries(metadata, mask, ColumnChunkMetaData::column_index_range)?;
        // Mask axes are sorted and unique, so present entries remain in row-major
        // order even when selected chunks have no index.
        for rg_idx in mask.row_group_indices(metadata.num_row_groups()) {
            let rg = metadata.row_group(rg_idx);
            for col_idx in mask.column_indices(rg.num_columns()) {
                let col = rg.column(col_idx);
                if let Some(range) = col.column_index_range() {
                    let bytes = get_index_bytes(bytes, start_offset, range)?;
                    let bytes = Self::index_bytes(
                        bytes,
                        metadata,
                        col,
                        rg_idx,
                        col_idx,
                        PageIndexKind::Column,
                    )?;
                    let index = crate::file::metadata::extensions::with_read_extensions!(
                        ThriftSliceInputProtocol::new(&bytes),
                        self.observer(),
                        SiteInfo::column(ThriftStruct::ColumnIndex, Some(rg_idx), Some(col_idx)),
                        with_observer,
                        |mut prot| read_column_index(&mut prot, col.column_type())
                    )?;
                    entries.push((ColumnChunkKey::try_new(rg_idx, col_idx)?, index));
                }
            }
        }
        ColumnChunkEntries::try_from_sorted_entries(
            metadata.num_row_groups(),
            metadata.file_metadata().schema_descr().num_columns(),
            entries,
        )
    }

    fn parse_offset_index(
        &self,
        metadata: &ParquetMetaData,
        policy: PageIndexPolicy,
        mask: &ColumnChunkMask,
        bytes: &[u8],
        start_offset: u64,
    ) -> Result<ColumnChunkEntries<OffsetIndexMetaData>> {
        let mut entries =
            reserve_index_entries(metadata, mask, ColumnChunkMetaData::offset_index_range)?;
        for rg_idx in mask.row_group_indices(metadata.num_row_groups()) {
            let rg = metadata.row_group(rg_idx);
            for col_idx in mask.column_indices(rg.num_columns()) {
                let col = rg.column(col_idx);
                if let Some(range) = col.offset_index_range() {
                    let bytes = get_index_bytes(bytes, start_offset, range)?;
                    let bytes = Self::index_bytes(
                        bytes,
                        metadata,
                        col,
                        rg_idx,
                        col_idx,
                        PageIndexKind::Offset,
                    )?;
                    let index = if let Some(observer) = self.observer() {
                        let root = SiteInfo::column(
                            ThriftStruct::OffsetIndex,
                            Some(rg_idx),
                            Some(col_idx),
                        );
                        read_offset_index_with_listener(&bytes, observer, root)?
                    } else {
                        read_offset_index(&mut ThriftSliceInputProtocol::new(&bytes))?
                    };
                    entries.push((ColumnChunkKey::try_new(rg_idx, col_idx)?, index));
                } else if policy == PageIndexPolicy::Required {
                    return Err(general_err!("missing offset index"));
                }
            }
        }
        ColumnChunkEntries::try_from_sorted_entries(
            metadata.num_row_groups(),
            metadata.file_metadata().schema_descr().num_columns(),
            entries,
        )
    }
}

/// Decode a plaintext Thrift footer (without the length/magic trailer).
pub(crate) fn decode_metadata(
    buf: &[u8],
    options: Option<&ParquetMetaDataOptions>,
) -> Result<ParquetMetaData> {
    parquet_metadata_from_bytes(buf, options)
}

/// Reserve only for selected chunks that actually have an index range. An extra
/// metadata-only pass avoids both vector growth for dense reads and reserving a
/// full rectangular grid for sparse reads or files with missing indexes.
fn reserve_index_entries<T>(
    metadata: &ParquetMetaData,
    mask: &ColumnChunkMask,
    index_range: impl Fn(&ColumnChunkMetaData) -> Option<Range<u64>>,
) -> Result<Vec<(ColumnChunkKey, T)>> {
    let num_entries =
        mask.row_group_indices(metadata.num_row_groups())
            .try_fold(0_usize, |count, rg_idx| {
                let rg = metadata.row_group(rg_idx);
                let present = mask
                    .column_indices(rg.num_columns())
                    .filter(|&col_idx| index_range(rg.column(col_idx)).is_some())
                    .count();
                count.checked_add(present).ok_or_else(|| {
                    ParquetError::General("column chunk entry count exceeds usize::MAX".to_string())
                })
            })?;
    let mut entries = Vec::new();
    entries.try_reserve_exact(num_entries).map_err(|err| {
        ParquetError::General(format!(
            "failed to allocate storage for {num_entries} column chunk entries: {err}"
        ))
    })?;
    Ok(entries)
}

fn get_index_bytes(bytes: &[u8], start_offset: u64, range: Range<u64>) -> Result<&[u8]> {
    let start = usize::try_from(range.start - start_offset)?;
    let end = usize::try_from(range.end - start_offset)?;
    Ok(&bytes[start..end])
}
