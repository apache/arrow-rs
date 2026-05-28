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
use crate::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
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
