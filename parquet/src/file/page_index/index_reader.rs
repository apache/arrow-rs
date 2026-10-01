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

/// Decode a Thrift [`OffsetIndex`] from the provided bytes.
///
/// The passed in bytes contain a serialized Thrift `OffsetIndex` struct as
/// read from a Parquet file.
///
/// Returns an [`OffsetIndexMetaData`] containing page location information.
///
/// # Example
///
/// ```
/// # use parquet::file::reader::{FileReader, SerializedFileReader};
/// # use parquet::file::page_index::index_reader::decode_offset_index;
/// # use std::fs::File;
/// # use std::io::{Read, Seek};
/// # use parquet::errors::Result;
/// #
/// # fn read_offset_index() -> Result<()> {
/// // Open the Parquet file
/// let mut file = File::open("data.parquet")?;
/// let reader = SerializedFileReader::new(file.try_clone()?)?;
/// let metadata = reader.metadata();
///
/// // Select a row group and column to read
/// let row_group_idx = 0;
/// let column_idx = 0;
///
/// // Get the column chunk metadata
/// let row_group = metadata.row_group(row_group_idx);
/// let column_chunk = row_group.column(column_idx);
///
/// // Get the offset index byte range from the column metadata
/// if let Some(range) = column_chunk.offset_index_range() {
///     // Read the offset index bytes from the file
///     let mut buffer = vec![0u8; (range.end - range.start) as usize];
///     file.seek(std::io::SeekFrom::Start(range.start))?;
///     file.read_exact(&mut buffer)?;
///
///     // Decode the offset index
///     let offset_index = decode_offset_index(&buffer)?;
///
///     // Access page location information
///     for (i, page_location) in offset_index.page_locations().iter().enumerate() {
///         println!("Page {}: offset={}, size={}, first_row={}",
///             i,
///             page_location.offset,
///             page_location.compressed_page_size,
///             page_location.first_row_index
///         );
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/e94a5d090b324a0c0ee1adbb8ea6b099852dc3cc/src/main/thrift/parquet.thrift#L1253-L1273
pub fn decode_offset_index(data: &[u8]) -> Result<OffsetIndexMetaData, ParquetError> {
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
  8: optional list<i64> nan_counts
}
);

/// Decode a Thrift [`ColumnIndex`] from the provided bytes.
///
/// The passed in bytes contain a serialized Thrift `OffsetIndex` struct as
/// read from a Parquet file. The `column_type` can be obtained via
/// [`ColumnChunkMetaData::column_type`].
///
/// Returns a [`ColumnIndexMetaData`] containing per-page statistics.
///
/// # Example
///
/// ```
/// # use parquet::file::reader::{FileReader, SerializedFileReader};
/// # use parquet::file::page_index::index_reader::decode_column_index;
/// # use std::fs::File;
/// # use std::io::{Read, Seek};
/// # use parquet::errors::Result;
/// #
/// # fn read_column_index() -> Result<()> {
/// // Open the Parquet file
/// let mut file = File::open("data.parquet")?;
/// let reader = SerializedFileReader::new(file.try_clone()?)?;
/// let metadata = reader.metadata();
///
/// // Select a row group and column to read
/// let row_group_idx = 0;
/// let column_idx = 0;
///
/// // Get the column chunk metadata
/// let row_group = metadata.row_group(row_group_idx);
/// let column_chunk = row_group.column(column_idx);
///
/// // Get the column index byte range from the column metadata
/// if let Some(range) = column_chunk.column_index_range() {
///     // Get the column type for proper deserialization
///     let column_type = column_chunk.column_type();
///
///     // Read the column index bytes from the file
///     let mut buffer = vec![0u8; (range.end - range.start) as usize];
///     file.seek(std::io::SeekFrom::Start(range.start))?;
///     file.read_exact(&mut buffer)?;
///
///     // Decode the column index
///     let column_index = decode_column_index(&buffer, column_type)?;
///
///     // Access per-page statistics (example for INT32 column)
///     use parquet::file::page_index::column_index::ColumnIndexMetaData;
///     match column_index {
///         ColumnIndexMetaData::INT32(index) => {
///             for (i, (min, max)) in index.min_values().iter()
///                 .zip(index.max_values().iter())
///                 .enumerate() {
///                 println!("Page {}: min={}, max={}", i, min, max);
///             }
///         }
///         _ => println!("Column is not INT32 type"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`ColumnChunkMetaData::column_type`]: crate::file::metadata::ColumnChunkMetaData::column_type
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/e94a5d090b324a0c0ee1adbb8ea6b099852dc3cc/src/main/thrift/parquet.thrift#L1275-1373
pub fn decode_column_index(
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
