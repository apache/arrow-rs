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
//! In general these functions parse thrift-encoded metadata from a byte slice
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
