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

use crate::basic::ColumnOrder;
use crate::errors::ParquetError;
use crate::file::metadata::{
    ColumnChunkMetaData, FileMetaData, PageIndexPolicy, ParquetMetaData, RowGroupMetaData,
};
use crate::file::page_index::index::Index;
use crate::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::schema::types;
use crate::schema::types::SchemaDescriptor;
use crate::thrift::TCompactSliceInputProtocol;
use crate::thrift::TSerializable;
use bytes::Bytes;
use std::sync::Arc;

#[cfg(feature = "encryption")]
use crate::encryption::{
    decrypt::{FileDecryptionProperties, FileDecryptor},
    modules::create_footer_aad,
};
#[cfg(feature = "encryption")]
use crate::format::EncryptionAlgorithm;

/// Decodes [`ParquetMetaData`] from the provided bytes.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thrift compact binary protocol, as specified
/// by the [Parquet Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
pub(crate) fn decode_metadata(buf: &[u8]) -> crate::errors::Result<ParquetMetaData> {
    let mut prot = TCompactSliceInputProtocol::new(buf);

    let t_file_metadata: crate::format::FileMetaData =
        crate::format::FileMetaData::read_from_in_protocol(&mut prot)
            .map_err(|e| general_err!("Could not parse metadata: {}", e))?;
    let schema = types::from_thrift(&t_file_metadata.schema)?;
    let schema_descr = Arc::new(SchemaDescriptor::new(schema));

    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
        row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_descr)?;

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_descr,
        column_orders,
    );

    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
pub(crate) fn parse_column_orders(
    t_column_orders: Option<Vec<crate::format::ColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> crate::errors::Result<Option<Vec<ColumnOrder>>> {
    match t_column_orders {
        Some(orders) => {
            // Should always be the case
            if orders.len() != schema_descr.num_columns() {
                return Err(general_err!("Column order length mismatch"));
            };
            let mut res = Vec::new();
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    crate::format::ColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Ok(Some(res))
        }
        None => Ok(None),
    }
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
                        parse_single_column_index(
                            &bytes[r_start..r_end],
                            metadata,
                            c,
                            rg_idx,
                            col_idx,
                        )
                    }
                    None => Ok(Index::NONE),
                })
                .collect::<crate::errors::Result<Vec<_>>>()
        })
        .collect::<crate::errors::Result<Vec<_>>>()?;

    metadata.set_column_index(Some(index));
    Ok(())
}

#[cfg(feature = "encryption")]
fn parse_single_column_index(
    bytes: &[u8],
    metadata: &ParquetMetaData,
    column: &ColumnChunkMetaData,
    row_group_index: usize,
    col_index: usize,
) -> crate::errors::Result<Index> {
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

#[cfg(not(feature = "encryption"))]
fn parse_single_column_index(
    bytes: &[u8],
    _metadata: &ParquetMetaData,
    column: &ColumnChunkMetaData,
    _row_group_index: usize,
    _col_index: usize,
) -> crate::errors::Result<Index> {
    decode_column_index(bytes, column.column_type())
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
                    parse_single_offset_index(&bytes[r_start..r_end], metadata, c, rg_idx, col_idx)
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

#[cfg(feature = "encryption")]
fn parse_single_offset_index(
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

#[cfg(not(feature = "encryption"))]
fn parse_single_offset_index(
    bytes: &[u8],
    _metadata: &ParquetMetaData,
    _column: &ColumnChunkMetaData,
    _row_group_index: usize,
    _col_index: usize,
) -> crate::errors::Result<OffsetIndexMetaData> {
    decode_offset_index(bytes)
}

/// Decodes [`ParquetMetaData`] from the provided bytes, handling metadata that may be encrypted.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thrift compact binary protocol, as specified
/// by the [Parquet Spec]. Buffer can be encrypted with AES GCM or AES CTR
/// ciphers as specfied in the [Parquet Encryption Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
/// [Parquet Encryption Spec]: https://parquet.apache.org/docs/file-format/data-pages/encryption/
#[cfg(feature = "encryption")]
pub(crate) fn decode_metadata_with_encryption(
    buf: &[u8],
    encrypted_footer: bool,
    file_decryption_properties: Option<&FileDecryptionProperties>,
) -> crate::errors::Result<ParquetMetaData> {
    let mut prot = TCompactSliceInputProtocol::new(buf);
    let mut file_decryptor = None;
    let decrypted_fmd_buf;

    if encrypted_footer {
        if let Some(file_decryption_properties) = file_decryption_properties {
            let t_file_crypto_metadata: crate::format::FileCryptoMetaData =
                crate::format::FileCryptoMetaData::read_from_in_protocol(&mut prot)
                    .map_err(|e| general_err!("Could not parse crypto metadata: {}", e))?;
            let supply_aad_prefix = match &t_file_crypto_metadata.encryption_algorithm {
                EncryptionAlgorithm::AESGCMV1(algo) => algo.supply_aad_prefix,
                _ => Some(false),
            }
            .unwrap_or(false);
            if supply_aad_prefix && file_decryption_properties.aad_prefix().is_none() {
                return Err(general_err!(
                        "Parquet file was encrypted with an AAD prefix that is not stored in the file, \
                        but no AAD prefix was provided in the file decryption properties"
                    ));
            }
            let decryptor = get_file_decryptor(
                t_file_crypto_metadata.encryption_algorithm,
                t_file_crypto_metadata.key_metadata.as_deref(),
                file_decryption_properties,
            )?;
            let footer_decryptor = decryptor.get_footer_decryptor();
            let aad_footer = create_footer_aad(decryptor.file_aad())?;

            decrypted_fmd_buf = footer_decryptor?
                .decrypt(prot.as_slice().as_ref(), aad_footer.as_ref())
                .map_err(|_| {
                    general_err!(
                        "Provided footer key and AAD were unable to decrypt parquet footer"
                    )
                })?;
            prot = TCompactSliceInputProtocol::new(decrypted_fmd_buf.as_ref());

            file_decryptor = Some(decryptor);
        } else {
            return Err(general_err!(
                "Parquet file has an encrypted footer but decryption properties were not provided"
            ));
        }
    }

    use crate::format::FileMetaData as TFileMetaData;
    let t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|e| general_err!("Could not parse metadata: {}", e))?;
    let schema = types::from_thrift(&t_file_metadata.schema)?;
    let schema_descr = Arc::new(SchemaDescriptor::new(schema));

    if let (Some(algo), Some(file_decryption_properties)) = (
        t_file_metadata.encryption_algorithm,
        file_decryption_properties,
    ) {
        // File has a plaintext footer but encryption algorithm is set
        let file_decryptor_value = get_file_decryptor(
            algo,
            t_file_metadata.footer_signing_key_metadata.as_deref(),
            file_decryption_properties,
        )?;
        if file_decryption_properties.check_plaintext_footer_integrity() && !encrypted_footer {
            file_decryptor_value.verify_plaintext_footer_signature(buf)?;
        }
        file_decryptor = Some(file_decryptor_value);
    }

    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
        let r = RowGroupMetaData::from_encrypted_thrift(
            schema_descr.clone(),
            rg,
            file_decryptor.as_ref(),
        )?;
        row_groups.push(r);
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_descr)?;

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_descr,
        column_orders,
    );
    let mut metadata = ParquetMetaData::new(file_metadata, row_groups);

    metadata.with_file_decryptor(file_decryptor);

    Ok(metadata)
}

#[cfg(feature = "encryption")]
fn get_file_decryptor(
    encryption_algorithm: EncryptionAlgorithm,
    footer_key_metadata: Option<&[u8]>,
    file_decryption_properties: &FileDecryptionProperties,
) -> crate::errors::Result<FileDecryptor> {
    match encryption_algorithm {
        EncryptionAlgorithm::AESGCMV1(algo) => {
            let aad_file_unique = algo
                .aad_file_unique
                .ok_or_else(|| general_err!("AAD unique file identifier is not set"))?;
            let aad_prefix = if let Some(aad_prefix) = file_decryption_properties.aad_prefix() {
                aad_prefix.clone()
            } else {
                algo.aad_prefix.unwrap_or_default()
            };

            FileDecryptor::new(
                file_decryption_properties,
                footer_key_metadata,
                aad_file_unique,
                aad_prefix,
            )
        }
        EncryptionAlgorithm::AESGCMCTRV1(_) => Err(nyi_err!(
            "The AES_GCM_CTR_V1 encryption algorithm is not yet supported"
        )),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::basic::{SortOrder, Type};
    use crate::file::metadata::SchemaType;
    use crate::format::ColumnOrder as TColumnOrder;
    use crate::format::TypeDefinedOrder;
    #[test]
    fn test_metadata_column_orders_parse() {
        // Define simple schema, we do not need to provide logical types.
        let fields = vec![
            Arc::new(
                SchemaType::primitive_type_builder("col1", Type::INT32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                SchemaType::primitive_type_builder("col2", Type::FLOAT)
                    .build()
                    .unwrap(),
            ),
        ];
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
        ]);

        assert_eq!(
            parse_column_orders(t_column_orders, &schema_descr).unwrap(),
            Some(vec![
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            ])
        );

        // Test when no column orders are defined.
        assert_eq!(parse_column_orders(None, &schema_descr).unwrap(), None);
    }

    #[test]
    fn test_metadata_column_orders_len_mismatch() {
        let schema = SchemaType::group_type_builder("schema").build().unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![TColumnOrder::TYPEORDER(TypeDefinedOrder::new())]);

        let res = parse_column_orders(t_column_orders, &schema_descr);
        assert!(res.is_err());
        assert!(format!("{:?}", res.unwrap_err()).contains("Column order length mismatch"));
    }
}
