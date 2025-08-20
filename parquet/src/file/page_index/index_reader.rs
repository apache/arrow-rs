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

//! Support for reading [`Index`] and [`OffsetIndexMetaData`] from parquet metadata.

use crate::basic::{BoundaryOrder, Type};
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, FixedLenByteArray, Int96};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::page_index::index::Index;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::ChunkReader;
use crate::parquet_thrift::{FieldType, ThriftCompactInputProtocol};
use crate::thrift_struct;
use crate::util::bit_util::*;
use std::marker::PhantomData;
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

/// Reads per-column [`Index`] for all columns of a row group by
/// decoding [`ColumnIndex`] .
///
/// Returns a vector of `index[column_number]`.
///
/// Returns `None` if this row group does not contain a [`ColumnIndex`].
///
/// See [Page Index Documentation] for more details.
///
/// [Page Index Documentation]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`ColumnIndex`]: crate::format::ColumnIndex
#[deprecated(
    since = "55.2.0",
    note = "Use ParquetMetaDataReader instead; will be removed in 58.0.0"
)]
pub fn read_columns_indexes<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Option<Vec<Index>>, ParquetError> {
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
                None => Ok(Index::NONE),
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
/// [`OffsetIndex`]: crate::format::OffsetIndex
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
    let mut prot = ThriftCompactInputProtocol::new(data);
    OffsetIndexMetaData::try_from(&mut prot)
}

thrift_struct!(
pub(crate) struct ColumnIndex<'a> {
  1: required list<bool> null_pages
  2: required list<'a><binary> min_values
  3: required list<'a><binary> max_values
  4: required BoundaryOrder boundary_order
  5: optional list<i64> null_counts
  6: optional list<i64> repetition_level_histograms;
  7: optional list<i64> definition_level_histograms;
}
);

/// column index
pub struct NativeColumnIndex<T: ParquetValueType> {
    phantom_data: PhantomData<T>,
    null_pages: Vec<bool>,
    boundary_order: BoundaryOrder,
    null_counts: Option<Vec<i64>>,
    repetition_level_histograms: Option<Vec<i64>>,
    definition_level_histograms: Option<Vec<i64>>,
    // raw bytes for min and max values
    min_bytes: Vec<u8>,
    min_offsets: Vec<usize>, // offsets are really only needed for BYTE_ARRAY
    max_bytes: Vec<u8>,
    max_offsets: Vec<usize>,
}

impl<T: ParquetValueType> NativeColumnIndex<T> {
    fn try_new(index: ColumnIndex) -> Result<Self> {
        let len = index.null_pages.len();

        let min_len = index.min_values.iter().map(|&v| v.len()).sum();
        let max_len = index.max_values.iter().map(|&v| v.len()).sum();
        let mut min_bytes = vec![0u8; min_len];
        let mut max_bytes = vec![0u8; max_len];

        let mut min_offsets = vec![0usize; len + 1];
        let mut max_offsets = vec![0usize; len + 1];

        let mut min_pos = 0;
        let mut max_pos = 0;

        for (i, is_null) in index.null_pages.iter().enumerate().take(len) {
            if !is_null {
                let min = index.min_values[i];
                let dst = &mut min_bytes[min_pos..min_pos + min.len()];
                dst.copy_from_slice(min);
                min_offsets[i] = min_pos;
                min_pos += min.len();

                let max = index.max_values[i];
                let dst = &mut max_bytes[max_pos..max_pos + min.len()];
                dst.copy_from_slice(max);
                max_offsets[i] = max_pos;
                max_pos += max.len();
            } else {
                min_offsets[i] = min_pos;
                max_offsets[i] = max_pos;
            }
        }

        min_offsets[len] = min_pos;
        max_offsets[len] = max_pos;

        Ok(Self {
            phantom_data: PhantomData,
            null_pages: index.null_pages,
            boundary_order: index.boundary_order,
            null_counts: index.null_counts,
            repetition_level_histograms: index.repetition_level_histograms,
            definition_level_histograms: index.definition_level_histograms,
            min_bytes,
            min_offsets,
            max_bytes,
            max_offsets,
        })
    }

    /// Returns the number of pages
    pub fn num_pages(&self) -> u64 {
        self.null_pages.len() as u64
    }

    /// Returns the number of null values in the page indexed by `idx`
    pub fn null_count(&self, idx: usize) -> Option<i64> {
        self.null_counts.as_ref().map(|nc| nc[idx])
    }

    /// Returns the repetition level histogram for the page indexed by `idx`
    pub fn repetition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        if let Some(rep_hists) = self.repetition_level_histograms.as_ref() {
            let num_lvls = rep_hists.len() / self.num_pages() as usize;
            let start = num_lvls * idx;
            Some(&rep_hists[start..start + num_lvls])
        } else {
            None
        }
    }

    /// Returns the definition level histogram for the page indexed by `idx`
    pub fn definition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        if let Some(def_hists) = self.definition_level_histograms.as_ref() {
            let num_lvls = def_hists.len() / self.num_pages() as usize;
            let start = num_lvls * idx;
            Some(&def_hists[start..start + num_lvls])
        } else {
            None
        }
    }

    /// Returns whether this is an all null page
    pub fn is_null_page(&self, idx: usize) -> bool {
        self.null_pages[idx]
    }

    /// Returns the minimum value in the page indexed by `idx` as raw bytes
    ///
    /// It is `None` when all values are null
    pub fn min_value_bytes(&self, idx: usize) -> Option<&[u8]> {
        if self.null_pages[idx] {
            None
        } else {
            let start = self.min_offsets[idx];
            let end = self.min_offsets[idx + 1];
            Some(&self.min_bytes[start..end])
        }
    }

    /// Returns the maximum value in the page indexed by `idx` as raw bytes
    ///
    /// It is `None` when all values are null
    pub fn max_value_bytes(&self, idx: usize) -> Option<&[u8]> {
        if self.null_pages[idx] {
            None
        } else {
            let start = self.max_offsets[idx];
            let end = self.max_offsets[idx + 1];
            Some(&self.max_bytes[start..end])
        }
    }
}

macro_rules! min_max_values {
    ($ty: ty) => {
        impl NativeColumnIndex<$ty> {
            /// Returns the minimum value in the page indexed by `idx`
            ///
            /// It is `None` when all values are null
            pub fn min_value(&self, idx: usize) -> Option<$ty> {
                <$ty>::try_from_le_slice(self.min_value_bytes(idx)?).ok()
            }

            /// Returns the maximum value in the page indexed by `idx`
            ///
            /// It is `None` when all values are null
            pub fn max_value(&self, idx: usize) -> Option<$ty> {
                <$ty>::try_from_le_slice(self.max_value_bytes(idx)?).ok()
            }
        }
    };
}

min_max_values!(bool);
min_max_values!(i32);
min_max_values!(i64);
min_max_values!(f32);
min_max_values!(f64);
min_max_values!(Int96);

/// index
#[allow(non_camel_case_types)]
pub enum ColumnIndexMetaData {
    /// Sometimes reading page index from parquet file
    /// will only return pageLocations without min_max index,
    /// `NONE` represents this lack of index information
    NONE,
    /// Boolean type index
    BOOLEAN(NativeColumnIndex<bool>),
    /// 32-bit integer type index
    INT32(NativeColumnIndex<i32>),
    /// 64-bit integer type index
    INT64(NativeColumnIndex<i64>),
    /// 96-bit integer type (timestamp) index
    INT96(NativeColumnIndex<Int96>),
    /// 32-bit floating point type index
    FLOAT(NativeColumnIndex<f32>),
    /// 64-bit floating point type index
    DOUBLE(NativeColumnIndex<f64>),
    /// Byte array type index
    BYTE_ARRAY(NativeColumnIndex<ByteArray>),
    /// Fixed length byte array type index
    FIXED_LEN_BYTE_ARRAY(NativeColumnIndex<FixedLenByteArray>),
}

impl ColumnIndexMetaData {
    /// Return min/max elements inside ColumnIndex are ordered or not.
    pub fn is_sorted(&self) -> bool {
        // 0:UNORDERED, 1:ASCENDING ,2:DESCENDING,
        if let Some(order) = self.get_boundary_order() {
            order != BoundaryOrder::UNORDERED
        } else {
            false
        }
    }

    /// Get boundary_order of this page index.
    pub fn get_boundary_order(&self) -> Option<BoundaryOrder> {
        match self {
            ColumnIndexMetaData::NONE => None,
            ColumnIndexMetaData::BOOLEAN(index) => Some(index.boundary_order),
            ColumnIndexMetaData::INT32(index) => Some(index.boundary_order),
            ColumnIndexMetaData::INT64(index) => Some(index.boundary_order),
            ColumnIndexMetaData::INT96(index) => Some(index.boundary_order),
            ColumnIndexMetaData::FLOAT(index) => Some(index.boundary_order),
            ColumnIndexMetaData::DOUBLE(index) => Some(index.boundary_order),
            ColumnIndexMetaData::BYTE_ARRAY(index) => Some(index.boundary_order),
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => Some(index.boundary_order),
        }
    }
}

pub(crate) fn decode_column_index(data: &[u8], column_type: Type) -> Result<Index, ParquetError> {
    let mut prot = ThriftCompactInputProtocol::new(data);
    let index = ColumnIndex::try_from(&mut prot)?;

    let index = match column_type {
        Type::BOOLEAN => ColumnIndexMetaData::BOOLEAN(NativeColumnIndex::<bool>::try_new(index)?),
        Type::INT32 => ColumnIndexMetaData::INT32(NativeColumnIndex::<i32>::try_new(index)?),
        Type::INT64 => ColumnIndexMetaData::INT64(NativeColumnIndex::<i64>::try_new(index)?),
        Type::INT96 => ColumnIndexMetaData::INT96(NativeColumnIndex::<Int96>::try_new(index)?),
        Type::FLOAT => ColumnIndexMetaData::FLOAT(NativeColumnIndex::<f32>::try_new(index)?),
        Type::DOUBLE => ColumnIndexMetaData::DOUBLE(NativeColumnIndex::<f64>::try_new(index)?),
        Type::BYTE_ARRAY => ColumnIndexMetaData::BYTE_ARRAY(NativeColumnIndex::try_new(index)?),
        Type::FIXED_LEN_BYTE_ARRAY => {
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(NativeColumnIndex::try_new(index)?)
        }
    };

    //Ok(index)
    Ok(Index::NONE)
}
