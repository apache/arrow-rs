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

//! Support for reading [`ColumnIndexMetaData`] and [`OffsetIndexMetaData`] from parquet metadata.

use crate::basic::{BoundaryOrder, Type};
use crate::data_type::Int96;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::ChunkReader;
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    ThriftSliceInputProtocol, WriteThrift, WriteThriftField, read_thrift_vec,
};
use crate::thrift_struct;
use std::io::Write;
use std::ops::Range;

/// Computes the covering range of two optional ranges
///
/// For example `acc_range(Some(7..9), Some(1..3)) = Some(1..9)`
pub(crate) fn acc_range(a: Option<Range<u64>>, b: Option<Range<u64>>) -> Option<Range<u64>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.start.min(b.start)..a.end.max(b.end)),
        (None, x) | (x, None) => x,
    }
}

/// Reads per-column [`ColumnIndexMetaData`] for all columns of a row group by
/// decoding [`ColumnIndex`] .
///
/// Returns a vector of `index[column_number]`.
///
/// Returns `None` if this row group does not contain a [`ColumnIndex`].
///
/// See [Page Index Documentation] for more details.
///
/// [Page Index Documentation]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
#[deprecated(
    since = "55.2.0",
    note = "Use ParquetMetaDataReader instead; will be removed in 58.0.0"
)]
pub fn read_columns_indexes<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Option<Vec<ColumnIndexMetaData>>, ParquetError> {
    let fetch = chunks
        .iter()
        .fold(None, |range, c| acc_range(range, c.column_index_range()));

    let fetch = match fetch {
        Some(r) => r,
        None => return Ok(None),
    };

    let bytes = reader.get_bytes(fetch.start as _, (fetch.end - fetch.start).try_into()?)?;

    Some(
        chunks
            .iter()
            .map(|c| match c.column_index_range() {
                Some(r) => decode_column_index(
                    &bytes[usize::try_from(r.start - fetch.start)?
                        ..usize::try_from(r.end - fetch.start)?],
                    c.column_type(),
                ),
                None => Ok(ColumnIndexMetaData::NONE),
            })
            .collect(),
    )
    .transpose()
}

/// Reads per-column [`OffsetIndexMetaData`] for all columns of a row group by
/// decoding [`OffsetIndex`] .
///
/// Returns a vector of `offset_index[column_number]`.
///
/// Returns `None` if this row group does not contain an [`OffsetIndex`].
///
/// See [Page Index Documentation] for more details.
///
/// [Page Index Documentation]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
#[deprecated(
    since = "55.2.0",
    note = "Use ParquetMetaDataReader instead; will be removed in 58.0.0"
)]
pub fn read_offset_indexes<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Option<Vec<OffsetIndexMetaData>>, ParquetError> {
    let fetch = chunks
        .iter()
        .fold(None, |range, c| acc_range(range, c.offset_index_range()));

    let fetch = match fetch {
        Some(r) => r,
        None => return Ok(None),
    };

    let bytes = reader.get_bytes(fetch.start as _, (fetch.end - fetch.start).try_into()?)?;

    Some(
        chunks
            .iter()
            .map(|c| match c.offset_index_range() {
                Some(r) => decode_offset_index(
                    &bytes[usize::try_from(r.start - fetch.start)?
                        ..usize::try_from(r.end - fetch.start)?],
                ),
                None => Err(general_err!("missing offset index")),
            })
            .collect(),
    )
    .transpose()
}

pub(crate) fn decode_offset_index(data: &[u8]) -> Result<OffsetIndexMetaData, ParquetError> {
    let mut prot = ThriftSliceInputProtocol::new(data);

    // Try to read fast-path first. If that fails, fall back to slower but more robust
    // decoder.
    match OffsetIndexMetaData::try_from_fast(&mut prot) {
        Ok(offset_index) => Ok(offset_index),
        Err(_) => {
            prot = ThriftSliceInputProtocol::new(data);
            OffsetIndexMetaData::read_thrift(&mut prot)
        }
    }
}

// private struct only used for decoding then discarded
thrift_struct!(
pub(super) struct ThriftColumnIndex<'a> {
  1: required list<bool> null_pages
  2: required list<'a><binary> min_values
  3: required list<'a><binary> max_values
  4: required BoundaryOrder boundary_order
  5: optional list<i64> null_counts
  6: optional list<i64> repetition_level_histograms;
  7: optional list<i64> definition_level_histograms;
}
);

pub(crate) fn decode_column_index(
    data: &[u8],
    column_type: Type,
) -> Result<ColumnIndexMetaData, ParquetError> {
    let mut prot = ThriftSliceInputProtocol::new(data);
    let index = ThriftColumnIndex::read_thrift(&mut prot)?;

    let index = match column_type {
        Type::BOOLEAN => {
            ColumnIndexMetaData::BOOLEAN(PrimitiveColumnIndex::<bool>::try_from_thrift(index)?)
        }
        Type::INT32 => {
            ColumnIndexMetaData::INT32(PrimitiveColumnIndex::<i32>::try_from_thrift(index)?)
        }
        Type::INT64 => {
            ColumnIndexMetaData::INT64(PrimitiveColumnIndex::<i64>::try_from_thrift(index)?)
        }
        Type::INT96 => {
            ColumnIndexMetaData::INT96(PrimitiveColumnIndex::<Int96>::try_from_thrift(index)?)
        }
        Type::FLOAT => {
            ColumnIndexMetaData::FLOAT(PrimitiveColumnIndex::<f32>::try_from_thrift(index)?)
        }
        Type::DOUBLE => {
            ColumnIndexMetaData::DOUBLE(PrimitiveColumnIndex::<f64>::try_from_thrift(index)?)
        }
        Type::BYTE_ARRAY => {
            ColumnIndexMetaData::BYTE_ARRAY(ByteArrayColumnIndex::try_from_thrift(index)?)
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(ByteArrayColumnIndex::try_from_thrift(index)?)
        }
    };

    Ok(index)
}
