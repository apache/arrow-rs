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

use crate::errors::ParquetError;
use crate::file::metadata::thrift::parquet_metadata_from_bytes;
use crate::file::metadata::{
    ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions,
};

use crate::file::page_index::column_index::ColumnIndexMetaData;
use crate::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use bytes::Bytes;

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
    use crate::errors::Result;

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
    ) -> crate::errors::Result<ColumnIndexMetaData> {
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
    ) -> crate::errors::Result<OffsetIndexMetaData> {
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
    use crate::errors::Result;
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
    ) -> crate::errors::Result<ColumnIndexMetaData> {
        decode_column_index(bytes, column.column_type())
    }

    pub(super) fn parse_single_offset_index(
        bytes: &[u8],
        _metadata: &ParquetMetaData,
        _column: &ColumnChunkMetaData,
        _row_group_index: usize,
        _col_index: usize,
    ) -> crate::errors::Result<OffsetIndexMetaData> {
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
) -> crate::errors::Result<ParquetMetaData> {
    parquet_metadata_from_bytes(buf, options)
}

/// Parses column index from the provided bytes and adds it to the metadata.
///
/// Arguments
/// * `metadata` - The ParquetMetaData to which the parsed column index will be added.
/// * `column_index_policy` - The policy for handling column index parsing (e.g.,
///   Required, Optional, Skip).
/// * `bytes` - The byte slice containing the column index data.
/// * `start_offset` - The offset where `bytes` begin in the file.
pub(crate) fn parse_column_index(
    metadata: &mut ParquetMetaData,
    column_index_policy: PageIndexPolicy,
    bytes: &Bytes,
    start_offset: u64,
) -> crate::errors::Result<()> {
    if column_index_policy == PageIndexPolicy::Skip {
        return Ok(());
    }
    let index = metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(rg_idx, x)| {
            x.columns()
                .iter()
                .enumerate()
                .map(|(col_idx, c)| match c.column_index_range() {
                    Some(r) => {
                        let r_start = usize::try_from(r.start - start_offset)?;
                        let r_end = usize::try_from(r.end - start_offset)?;
                        inner::parse_single_column_index(
                            &bytes[r_start..r_end],
                            metadata,
                            c,
                            rg_idx,
                            col_idx,
                        )
                    }
                    None => Ok(ColumnIndexMetaData::NONE),
                })
                .collect::<crate::errors::Result<Vec<_>>>()
        })
        .collect::<crate::errors::Result<Vec<_>>>()?;

    metadata.set_column_index(Some(index));
    Ok(())
}

pub(crate) fn parse_offset_index(
    metadata: &mut ParquetMetaData,
    offset_index_policy: PageIndexPolicy,
    bytes: &Bytes,
    start_offset: u64,
) -> crate::errors::Result<()> {
    if offset_index_policy == PageIndexPolicy::Skip {
        return Ok(());
    }
    let row_groups = metadata.row_groups();
    let mut all_indexes = Vec::with_capacity(row_groups.len());
    for (rg_idx, x) in row_groups.iter().enumerate() {
        let mut row_group_indexes = Vec::with_capacity(x.columns().len());
        for (col_idx, c) in x.columns().iter().enumerate() {
            let result = match c.offset_index_range() {
                Some(r) => {
                    let r_start = usize::try_from(r.start - start_offset)?;
                    let r_end = usize::try_from(r.end - start_offset)?;
                    inner::parse_single_offset_index(
                        &bytes[r_start..r_end],
                        metadata,
                        c,
                        rg_idx,
                        col_idx,
                    )
                }
                None => Err(general_err!("missing offset index")),
            };

            match result {
                Ok(index) => row_group_indexes.push(index),
                Err(e) => {
                    if offset_index_policy == PageIndexPolicy::Required {
                        return Err(e);
                    } else {
                        // Invalidate and return
                        metadata.set_column_index(None);
                        metadata.set_offset_index(None);
                        return Ok(());
                    }
                }
            }
        }
        all_indexes.push(row_group_indexes);
    }
    metadata.set_offset_index(Some(all_indexes));
    Ok(())
}
