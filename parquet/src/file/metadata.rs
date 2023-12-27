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

//! Contains information about available Parquet metadata.
//!
//! The hierarchy of metadata is as follows:
//!
//! [`ParquetMetaData`](struct.ParquetMetaData.html) contains
//! [`FileMetaData`](struct.FileMetaData.html) and zero or more
//! [`RowGroupMetaData`](struct.RowGroupMetaData.html) for each row group.
//!
//! [`FileMetaData`](struct.FileMetaData.html) includes file version, application specific
//! metadata.
//!
//! Each [`RowGroupMetaData`](struct.RowGroupMetaData.html) contains information about row
//! group and one or more [`ColumnChunkMetaData`](struct.ColumnChunkMetaData.html) for
//! each column chunk.
//!
//! [`ColumnChunkMetaData`](struct.ColumnChunkMetaData.html) has information about column
//! chunk (primitive leaf column), including encoding/compression, number of values, etc.

use std::ops::Range;
use std::sync::Arc;

use crate::format::{
    BoundaryOrder, ColumnChunk, ColumnIndex, ColumnMetaData, OffsetIndex, PageLocation, RowGroup,
    SortingColumn,
};

use crate::basic::{ColumnOrder, Compression, Encoding, Type};
use crate::errors::{ParquetError, Result};
use crate::file::page_encoding_stats::{self, PageEncodingStats};
use crate::file::page_index::index::Index;
use crate::file::statistics::{self, Statistics};
use crate::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescPtr, SchemaDescriptor,
    Type as SchemaType,
};

/// [`Index`] for each row group of each column.
///
/// `column_index[row_group_number][column_number]` holds the
/// [`Index`] corresponding to column `column_number` of row group
/// `row_group_number`.
///
/// For example `column_index[2][3]` holds the [`Index`] for the forth
/// column in the third row group of the parquet file.
pub type ParquetColumnIndex = Vec<Vec<Index>>;

/// [`PageLocation`] for each datapage of each row group of each column.
///
/// `offset_index[row_group_number][column_number][page_number]` holds
/// the [`PageLocation`] corresponding to page `page_number` of column
/// `column_number`of row group `row_group_number`.
///
/// For example `offset_index[2][3][4]` holds the [`PageLocation`] for
/// the fifth page of the forth column in the third row group of the
/// parquet file.
pub type ParquetOffsetIndex = Vec<Vec<Vec<PageLocation>>>;

/// Global Parquet metadata.
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    file_metadata: FileMetaData,
    row_groups: Vec<RowGroupMetaData>,
    /// Page index for all pages in each column chunk
    column_index: Option<ParquetColumnIndex>,
    /// Offset index for all pages in each column chunk
    offset_index: Option<ParquetOffsetIndex>,
}

impl ParquetMetaData {
    /// Creates Parquet metadata from file metadata and a list of row
    /// group metadata
    pub fn new(file_metadata: FileMetaData, row_groups: Vec<RowGroupMetaData>) -> Self {
        ParquetMetaData {
            file_metadata,
            row_groups,
            column_index: None,
            offset_index: None,
        }
    }

    /// Creates Parquet metadata from file metadata, a list of row
    /// group metadata, and the column index structures.
    pub fn new_with_page_index(
        file_metadata: FileMetaData,
        row_groups: Vec<RowGroupMetaData>,
        column_index: Option<ParquetColumnIndex>,
        offset_index: Option<ParquetOffsetIndex>,
    ) -> Self {
        ParquetMetaData {
            file_metadata,
            row_groups,
            column_index,
            offset_index,
        }
    }

    /// Returns file metadata as reference.
    pub fn file_metadata(&self) -> &FileMetaData {
        &self.file_metadata
    }

    /// Returns number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }

    /// Returns row group metadata for `i`th position.
    /// Position should be less than number of row groups `num_row_groups`.
    pub fn row_group(&self, i: usize) -> &RowGroupMetaData {
        &self.row_groups[i]
    }

    /// Returns slice of row groups in this file.
    pub fn row_groups(&self) -> &[RowGroupMetaData] {
        &self.row_groups
    }

    /// Returns page indexes in this file.
    #[deprecated(note = "Use Self::column_index")]
    pub fn page_indexes(&self) -> Option<&ParquetColumnIndex> {
        self.column_index.as_ref()
    }

    /// Returns the column index for this file if loaded
    pub fn column_index(&self) -> Option<&ParquetColumnIndex> {
        self.column_index.as_ref()
    }

    /// Returns the offset index for this file if loaded
    #[deprecated(note = "Use Self::offset_index")]
    pub fn offset_indexes(&self) -> Option<&ParquetOffsetIndex> {
        self.offset_index.as_ref()
    }

    /// Returns offset indexes in this file.
    pub fn offset_index(&self) -> Option<&ParquetOffsetIndex> {
        self.offset_index.as_ref()
    }

    /// Override the column index
    #[cfg(feature = "arrow")]
    pub(crate) fn set_column_index(&mut self, index: Option<ParquetColumnIndex>) {
        self.column_index = index;
    }

    /// Override the offset index
    #[cfg(feature = "arrow")]
    pub(crate) fn set_offset_index(&mut self, index: Option<ParquetOffsetIndex>) {
        self.offset_index = index;
    }
}

pub type KeyValue = crate::format::KeyValue;

/// Reference counted pointer for [`FileMetaData`].
pub type FileMetaDataPtr = Arc<FileMetaData>;

/// Metadata for a Parquet file.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
    schema_descr: SchemaDescPtr,
    column_orders: Option<Vec<ColumnOrder>>,
}

impl FileMetaData {
    /// Creates new file metadata.
    pub fn new(
        version: i32,
        num_rows: i64,
        created_by: Option<String>,
        key_value_metadata: Option<Vec<KeyValue>>,
        schema_descr: SchemaDescPtr,
        column_orders: Option<Vec<ColumnOrder>>,
    ) -> Self {
        FileMetaData {
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        }
    }

    /// Returns version of this file.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns number of rows in the file.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub fn created_by(&self) -> Option<&str> {
        self.created_by.as_deref()
    }

    /// Returns key_value_metadata of this file.
    pub fn key_value_metadata(&self) -> Option<&Vec<KeyValue>> {
        self.key_value_metadata.as_ref()
    }

    /// Returns Parquet [`Type`] that describes schema in this file.
    ///
    /// [`Type`]: crate::schema::types::Type
    pub fn schema(&self) -> &SchemaType {
        self.schema_descr.root_schema()
    }

    /// Returns a reference to schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        &self.schema_descr
    }

    /// Returns reference counted clone for schema descriptor.
    pub fn schema_descr_ptr(&self) -> SchemaDescPtr {
        self.schema_descr.clone()
    }

    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None` is returned, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub fn column_orders(&self) -> Option<&Vec<ColumnOrder>> {
        self.column_orders.as_ref()
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::UNDEFINED)
    }
}

/// Reference counted pointer for [`RowGroupMetaData`].
pub type RowGroupMetaDataPtr = Arc<RowGroupMetaData>;

/// Metadata for a row group.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupMetaData {
    columns: Vec<ColumnChunkMetaData>,
    num_rows: i64,
    sorting_columns: Option<Vec<SortingColumn>>,
    total_byte_size: i64,
    schema_descr: SchemaDescPtr,
    // We can't infer from file offset of first column since there may empty columns in row group.
    file_offset: Option<i64>,
    ordinal: Option<i16>,
}

impl RowGroupMetaData {
    /// Returns builder for row group metadata.
    pub fn builder(schema_descr: SchemaDescPtr) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder::new(schema_descr)
    }

    /// Number of columns in this row group.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns column chunk metadata for `i`th column.
    pub fn column(&self, i: usize) -> &ColumnChunkMetaData {
        &self.columns[i]
    }

    /// Returns slice of column chunk metadata.
    pub fn columns(&self) -> &[ColumnChunkMetaData] {
        &self.columns
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// Returns the sort ordering of the rows in this RowGroup if any
    pub fn sorting_columns(&self) -> Option<&Vec<SortingColumn>> {
        self.sorting_columns.as_ref()
    }

    /// Total byte size of all uncompressed column data in this row group.
    pub fn total_byte_size(&self) -> i64 {
        self.total_byte_size
    }

    /// Total size of all compressed column data in this row group.
    pub fn compressed_size(&self) -> i64 {
        self.columns.iter().map(|c| c.total_compressed_size).sum()
    }

    /// Returns reference to a schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        self.schema_descr.as_ref()
    }

    /// Returns reference counted clone of schema descriptor.
    pub fn schema_descr_ptr(&self) -> SchemaDescPtr {
        self.schema_descr.clone()
    }

    /// Returns ordinal of this row group in file
    #[inline(always)]
    pub fn ordinal(&self) -> Option<i16> {
        self.ordinal
    }

    /// Returns file offset of this row group in file.
    #[inline(always)]
    pub fn file_offset(&self) -> Option<i64> {
        self.file_offset
    }

    /// Method to convert from Thrift.
    pub fn from_thrift(schema_descr: SchemaDescPtr, mut rg: RowGroup) -> Result<RowGroupMetaData> {
        assert_eq!(schema_descr.num_columns(), rg.columns.len());
        let total_byte_size = rg.total_byte_size;
        let num_rows = rg.num_rows;
        let mut columns = vec![];
        for (c, d) in rg.columns.drain(0..).zip(schema_descr.columns()) {
            let cc = ColumnChunkMetaData::from_thrift(d.clone(), c)?;
            columns.push(cc);
        }
        let sorting_columns = rg.sorting_columns;
        Ok(RowGroupMetaData {
            columns,
            num_rows,
            sorting_columns,
            total_byte_size,
            schema_descr,
            file_offset: rg.file_offset,
            ordinal: rg.ordinal,
        })
    }

    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> RowGroup {
        RowGroup {
            columns: self.columns().iter().map(|v| v.to_thrift()).collect(),
            total_byte_size: self.total_byte_size,
            num_rows: self.num_rows,
            sorting_columns: self.sorting_columns().cloned(),
            file_offset: self.file_offset(),
            total_compressed_size: Some(self.compressed_size()),
            ordinal: self.ordinal,
        }
    }

    /// Converts this [`RowGroupMetaData`] into a [`RowGroupMetaDataBuilder`]
    pub fn into_builder(self) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder(self)
    }
}

/// Builder for row group metadata.
pub struct RowGroupMetaDataBuilder(RowGroupMetaData);

impl RowGroupMetaDataBuilder {
    /// Creates new builder from schema descriptor.
    fn new(schema_descr: SchemaDescPtr) -> Self {
        Self(RowGroupMetaData {
            columns: Vec::with_capacity(schema_descr.num_columns()),
            schema_descr,
            file_offset: None,
            num_rows: 0,
            sorting_columns: None,
            total_byte_size: 0,
            ordinal: None,
        })
    }

    /// Sets number of rows in this row group.
    pub fn set_num_rows(mut self, value: i64) -> Self {
        self.0.num_rows = value;
        self
    }

    /// Sets the sorting order for columns
    pub fn set_sorting_columns(mut self, value: Option<Vec<SortingColumn>>) -> Self {
        self.0.sorting_columns = value;
        self
    }

    /// Sets total size in bytes for this row group.
    pub fn set_total_byte_size(mut self, value: i64) -> Self {
        self.0.total_byte_size = value;
        self
    }

    /// Sets column metadata for this row group.
    pub fn set_column_metadata(mut self, value: Vec<ColumnChunkMetaData>) -> Self {
        self.0.columns = value;
        self
    }

    /// Sets ordinal for this row group.
    pub fn set_ordinal(mut self, value: i16) -> Self {
        self.0.ordinal = Some(value);
        self
    }

    pub fn set_file_offset(mut self, value: i64) -> Self {
        self.0.file_offset = Some(value);
        self
    }

    /// Builds row group metadata.
    pub fn build(self) -> Result<RowGroupMetaData> {
        if self.0.schema_descr.num_columns() != self.0.columns.len() {
            return Err(general_err!(
                "Column length mismatch: {} != {}",
                self.0.schema_descr.num_columns(),
                self.0.columns.len()
            ));
        }

        Ok(self.0)
    }
}

/// Metadata for a column chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkMetaData {
    column_descr: ColumnDescPtr,
    encodings: Vec<Encoding>,
    file_path: Option<String>,
    file_offset: i64,
    num_values: i64,
    compression: Compression,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    data_page_offset: i64,
    index_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    statistics: Option<Statistics>,
    encoding_stats: Option<Vec<PageEncodingStats>>,
    bloom_filter_offset: Option<i64>,
    bloom_filter_length: Option<i32>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,
}

/// Represents common operations for a column chunk.
impl ColumnChunkMetaData {
    /// Returns builder for column chunk metadata.
    pub fn builder(column_descr: ColumnDescPtr) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder::new(column_descr)
    }

    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    pub fn file_path(&self) -> Option<&str> {
        self.file_path.as_deref()
    }

    /// Byte offset in `file_path()`.
    pub fn file_offset(&self) -> i64 {
        self.file_offset
    }

    /// Type of this column. Must be primitive.
    pub fn column_type(&self) -> Type {
        self.column_descr.physical_type()
    }

    /// Path (or identifier) of this column.
    pub fn column_path(&self) -> &ColumnPath {
        self.column_descr.path()
    }

    /// Descriptor for this column.
    pub fn column_descr(&self) -> &ColumnDescriptor {
        self.column_descr.as_ref()
    }

    /// Reference counted clone of descriptor for this column.
    pub fn column_descr_ptr(&self) -> ColumnDescPtr {
        self.column_descr.clone()
    }

    /// All encodings used for this column.
    pub fn encodings(&self) -> &Vec<Encoding> {
        &self.encodings
    }

    /// Total number of values in this column chunk.
    pub fn num_values(&self) -> i64 {
        self.num_values
    }

    /// Compression for this column.
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Returns the total compressed data size of this column chunk.
    pub fn compressed_size(&self) -> i64 {
        self.total_compressed_size
    }

    /// Returns the total uncompressed data size of this column chunk.
    pub fn uncompressed_size(&self) -> i64 {
        self.total_uncompressed_size
    }

    /// Returns the offset for the column data.
    pub fn data_page_offset(&self) -> i64 {
        self.data_page_offset
    }

    /// Returns the offset for the index page.
    pub fn index_page_offset(&self) -> Option<i64> {
        self.index_page_offset
    }

    /// Returns the offset for the dictionary page, if any.
    pub fn dictionary_page_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
    }

    /// Returns the offset and length in bytes of the column chunk within the file
    pub fn byte_range(&self) -> (u64, u64) {
        let col_start = match self.dictionary_page_offset() {
            Some(dictionary_page_offset) => dictionary_page_offset,
            None => self.data_page_offset(),
        };
        let col_len = self.compressed_size();
        assert!(
            col_start >= 0 && col_len >= 0,
            "column start and length should not be negative"
        );
        (col_start as u64, col_len as u64)
    }

    /// Returns statistics that are set for this column chunk,
    /// or `None` if no statistics are available.
    pub fn statistics(&self) -> Option<&Statistics> {
        self.statistics.as_ref()
    }

    /// Returns the offset for the page encoding stats,
    /// or `None` if no page encoding stats are available.
    pub fn page_encoding_stats(&self) -> Option<&Vec<PageEncodingStats>> {
        self.encoding_stats.as_ref()
    }

    /// Returns the offset for the bloom filter.
    pub fn bloom_filter_offset(&self) -> Option<i64> {
        self.bloom_filter_offset
    }

    /// Returns the offset for the bloom filter.
    pub fn bloom_filter_length(&self) -> Option<i32> {
        self.bloom_filter_length
    }

    /// Returns the offset for the column index.
    pub fn column_index_offset(&self) -> Option<i64> {
        self.column_index_offset
    }

    /// Returns the offset for the column index length.
    pub fn column_index_length(&self) -> Option<i32> {
        self.column_index_length
    }

    /// Returns the range for the offset index if any
    pub(crate) fn column_index_range(&self) -> Option<Range<usize>> {
        let offset = usize::try_from(self.column_index_offset?).ok()?;
        let length = usize::try_from(self.column_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Returns the offset for the offset index.
    pub fn offset_index_offset(&self) -> Option<i64> {
        self.offset_index_offset
    }

    /// Returns the offset for the offset index length.
    pub fn offset_index_length(&self) -> Option<i32> {
        self.offset_index_length
    }

    /// Returns the range for the offset index if any
    pub(crate) fn offset_index_range(&self) -> Option<Range<usize>> {
        let offset = usize::try_from(self.offset_index_offset?).ok()?;
        let length = usize::try_from(self.offset_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Method to convert from Thrift.
    pub fn from_thrift(column_descr: ColumnDescPtr, cc: ColumnChunk) -> Result<Self> {
        if cc.meta_data.is_none() {
            return Err(general_err!("Expected to have column metadata"));
        }
        let mut col_metadata: ColumnMetaData = cc.meta_data.unwrap();
        let column_type = Type::try_from(col_metadata.type_)?;
        let encodings = col_metadata
            .encodings
            .drain(0..)
            .map(Encoding::try_from)
            .collect::<Result<_>>()?;
        let compression = Compression::try_from(col_metadata.codec)?;
        let file_path = cc.file_path;
        let file_offset = cc.file_offset;
        let num_values = col_metadata.num_values;
        let total_compressed_size = col_metadata.total_compressed_size;
        let total_uncompressed_size = col_metadata.total_uncompressed_size;
        let data_page_offset = col_metadata.data_page_offset;
        let index_page_offset = col_metadata.index_page_offset;
        let dictionary_page_offset = col_metadata.dictionary_page_offset;
        let statistics = statistics::from_thrift(column_type, col_metadata.statistics)?;
        let encoding_stats = col_metadata
            .encoding_stats
            .as_ref()
            .map(|vec| {
                vec.iter()
                    .map(page_encoding_stats::try_from_thrift)
                    .collect::<Result<_>>()
            })
            .transpose()?;
        let bloom_filter_offset = col_metadata.bloom_filter_offset;
        let bloom_filter_length = col_metadata.bloom_filter_length;
        let offset_index_offset = cc.offset_index_offset;
        let offset_index_length = cc.offset_index_length;
        let column_index_offset = cc.column_index_offset;
        let column_index_length = cc.column_index_length;

        let result = ColumnChunkMetaData {
            column_descr,
            encodings,
            file_path,
            file_offset,
            num_values,
            compression,
            total_compressed_size,
            total_uncompressed_size,
            data_page_offset,
            index_page_offset,
            dictionary_page_offset,
            statistics,
            encoding_stats,
            bloom_filter_offset,
            bloom_filter_length,
            offset_index_offset,
            offset_index_length,
            column_index_offset,
            column_index_length,
        };
        Ok(result)
    }

    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> ColumnChunk {
        let column_metadata = self.to_column_metadata_thrift();

        ColumnChunk {
            file_path: self.file_path().map(|s| s.to_owned()),
            file_offset: self.file_offset,
            meta_data: Some(column_metadata),
            offset_index_offset: self.offset_index_offset,
            offset_index_length: self.offset_index_length,
            column_index_offset: self.column_index_offset,
            column_index_length: self.column_index_length,
            crypto_metadata: None,
            encrypted_column_metadata: None,
        }
    }

    /// Method to convert to Thrift `ColumnMetaData`
    pub fn to_column_metadata_thrift(&self) -> ColumnMetaData {
        ColumnMetaData {
            type_: self.column_type().into(),
            encodings: self.encodings().iter().map(|&v| v.into()).collect(),
            path_in_schema: self.column_path().as_ref().to_vec(),
            codec: self.compression.into(),
            num_values: self.num_values,
            total_uncompressed_size: self.total_uncompressed_size,
            total_compressed_size: self.total_compressed_size,
            key_value_metadata: None,
            data_page_offset: self.data_page_offset,
            index_page_offset: self.index_page_offset,
            dictionary_page_offset: self.dictionary_page_offset,
            statistics: statistics::to_thrift(self.statistics.as_ref()),
            encoding_stats: self
                .encoding_stats
                .as_ref()
                .map(|vec| vec.iter().map(page_encoding_stats::to_thrift).collect()),
            bloom_filter_offset: self.bloom_filter_offset,
            bloom_filter_length: self.bloom_filter_length,
        }
    }

    /// Converts this [`ColumnChunkMetaData`] into a [`ColumnChunkMetaDataBuilder`]
    pub fn into_builder(self) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder(self)
    }
}

/// Builder for column chunk metadata.
pub struct ColumnChunkMetaDataBuilder(ColumnChunkMetaData);

impl ColumnChunkMetaDataBuilder {
    /// Creates new column chunk metadata builder.
    fn new(column_descr: ColumnDescPtr) -> Self {
        Self(ColumnChunkMetaData {
            column_descr,
            encodings: Vec::new(),
            file_path: None,
            file_offset: 0,
            num_values: 0,
            compression: Compression::UNCOMPRESSED,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            data_page_offset: 0,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: None,
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
        })
    }

    /// Sets list of encodings for this column chunk.
    pub fn set_encodings(mut self, encodings: Vec<Encoding>) -> Self {
        self.0.encodings = encodings;
        self
    }

    /// Sets optional file path for this column chunk.
    pub fn set_file_path(mut self, value: String) -> Self {
        self.0.file_path = Some(value);
        self
    }

    /// Sets file offset in bytes.
    pub fn set_file_offset(mut self, value: i64) -> Self {
        self.0.file_offset = value;
        self
    }

    /// Sets number of values.
    pub fn set_num_values(mut self, value: i64) -> Self {
        self.0.num_values = value;
        self
    }

    /// Sets compression.
    pub fn set_compression(mut self, value: Compression) -> Self {
        self.0.compression = value;
        self
    }

    /// Sets total compressed size in bytes.
    pub fn set_total_compressed_size(mut self, value: i64) -> Self {
        self.0.total_compressed_size = value;
        self
    }

    /// Sets total uncompressed size in bytes.
    pub fn set_total_uncompressed_size(mut self, value: i64) -> Self {
        self.0.total_uncompressed_size = value;
        self
    }

    /// Sets data page offset in bytes.
    pub fn set_data_page_offset(mut self, value: i64) -> Self {
        self.0.data_page_offset = value;
        self
    }

    /// Sets optional dictionary page ofset in bytes.
    pub fn set_dictionary_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.dictionary_page_offset = value;
        self
    }

    /// Sets optional index page offset in bytes.
    pub fn set_index_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.index_page_offset = value;
        self
    }

    /// Sets statistics for this column chunk.
    pub fn set_statistics(mut self, value: Statistics) -> Self {
        self.0.statistics = Some(value);
        self
    }

    /// Sets page encoding stats for this column chunk.
    pub fn set_page_encoding_stats(mut self, value: Vec<PageEncodingStats>) -> Self {
        self.0.encoding_stats = Some(value);
        self
    }

    /// Sets optional bloom filter offset in bytes.
    pub fn set_bloom_filter_offset(mut self, value: Option<i64>) -> Self {
        self.0.bloom_filter_offset = value;
        self
    }

    /// Sets optional bloom filter length in bytes.
    pub fn set_bloom_filter_length(mut self, value: Option<i32>) -> Self {
        self.0.bloom_filter_length = value;
        self
    }

    /// Sets optional offset index offset in bytes.
    pub fn set_offset_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.offset_index_offset = value;
        self
    }

    /// Sets optional offset index length in bytes.
    pub fn set_offset_index_length(mut self, value: Option<i32>) -> Self {
        self.0.offset_index_length = value;
        self
    }

    /// Sets optional column index offset in bytes.
    pub fn set_column_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.column_index_offset = value;
        self
    }

    /// Sets optional column index length in bytes.
    pub fn set_column_index_length(mut self, value: Option<i32>) -> Self {
        self.0.column_index_length = value;
        self
    }

    /// Builds column chunk metadata.
    pub fn build(self) -> Result<ColumnChunkMetaData> {
        Ok(self.0)
    }
}

/// Builder for column index
pub struct ColumnIndexBuilder {
    null_pages: Vec<bool>,
    min_values: Vec<Vec<u8>>,
    max_values: Vec<Vec<u8>>,
    null_counts: Vec<i64>,
    boundary_order: BoundaryOrder,
    // If one page can't get build index, need to ignore all index in this column
    valid: bool,
}

impl Default for ColumnIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnIndexBuilder {
    pub fn new() -> Self {
        ColumnIndexBuilder {
            null_pages: Vec::new(),
            min_values: Vec::new(),
            max_values: Vec::new(),
            null_counts: Vec::new(),
            boundary_order: BoundaryOrder::UNORDERED,
            valid: true,
        }
    }

    pub fn append(
        &mut self,
        null_page: bool,
        min_value: Vec<u8>,
        max_value: Vec<u8>,
        null_count: i64,
    ) {
        self.null_pages.push(null_page);
        self.min_values.push(min_value);
        self.max_values.push(max_value);
        self.null_counts.push(null_count);
    }

    pub fn set_boundary_order(&mut self, boundary_order: BoundaryOrder) {
        self.boundary_order = boundary_order;
    }

    pub fn to_invalid(&mut self) {
        self.valid = false;
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Build and get the thrift metadata of column index
    pub fn build_to_thrift(self) -> ColumnIndex {
        ColumnIndex::new(
            self.null_pages,
            self.min_values,
            self.max_values,
            self.boundary_order,
            self.null_counts,
        )
    }
}

/// Builder for offset index
pub struct OffsetIndexBuilder {
    offset_array: Vec<i64>,
    compressed_page_size_array: Vec<i32>,
    first_row_index_array: Vec<i64>,
    current_first_row_index: i64,
}

impl Default for OffsetIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetIndexBuilder {
    pub fn new() -> Self {
        OffsetIndexBuilder {
            offset_array: Vec::new(),
            compressed_page_size_array: Vec::new(),
            first_row_index_array: Vec::new(),
            current_first_row_index: 0,
        }
    }

    pub fn append_row_count(&mut self, row_count: i64) {
        let current_page_row_index = self.current_first_row_index;
        self.first_row_index_array.push(current_page_row_index);
        self.current_first_row_index += row_count;
    }

    pub fn append_offset_and_size(&mut self, offset: i64, compressed_page_size: i32) {
        self.offset_array.push(offset);
        self.compressed_page_size_array.push(compressed_page_size);
    }

    /// Build and get the thrift metadata of offset index
    pub fn build_to_thrift(self) -> OffsetIndex {
        let locations = self
            .offset_array
            .iter()
            .zip(self.compressed_page_size_array.iter())
            .zip(self.first_row_index_array.iter())
            .map(|((offset, size), row_index)| PageLocation::new(*offset, *size, *row_index))
            .collect::<Vec<_>>();
        OffsetIndex::new(locations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::{Encoding, PageType};

    #[test]
    fn test_row_group_metadata_thrift_conversion() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(1)
            .build()
            .unwrap();

        let row_group_exp = row_group_meta.to_thrift();
        let row_group_res = RowGroupMetaData::from_thrift(schema_descr, row_group_exp.clone())
            .unwrap()
            .to_thrift();

        assert_eq!(row_group_res, row_group_exp);
    }

    #[test]
    fn test_row_group_metadata_thrift_conversion_empty() {
        let schema_descr = get_test_schema_descr();

        let row_group_meta = RowGroupMetaData::builder(schema_descr).build();

        assert!(row_group_meta.is_err());
        if let Err(e) = row_group_meta {
            assert_eq!(
                format!("{e}"),
                "Parquet error: Column length mismatch: 2 != 0"
            );
        }
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion() {
        let column_descr = get_test_schema_descr().column(0);

        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .set_encodings(vec![Encoding::PLAIN, Encoding::RLE])
            .set_file_path("file_path".to_owned())
            .set_file_offset(100)
            .set_num_values(1000)
            .set_compression(Compression::SNAPPY)
            .set_total_compressed_size(2000)
            .set_total_uncompressed_size(3000)
            .set_data_page_offset(4000)
            .set_dictionary_page_offset(Some(5000))
            .set_page_encoding_stats(vec![
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::PLAIN,
                    count: 3,
                },
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::RLE,
                    count: 5,
                },
            ])
            .set_bloom_filter_offset(Some(6000))
            .set_bloom_filter_length(Some(25))
            .set_offset_index_offset(Some(7000))
            .set_offset_index_length(Some(25))
            .set_column_index_offset(Some(8000))
            .set_column_index_length(Some(25))
            .build()
            .unwrap();

        let col_chunk_res =
            ColumnChunkMetaData::from_thrift(column_descr, col_metadata.to_thrift()).unwrap();

        assert_eq!(col_chunk_res, col_metadata);
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion_empty() {
        let column_descr = get_test_schema_descr().column(0);

        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .build()
            .unwrap();

        let col_chunk_exp = col_metadata.to_thrift();
        let col_chunk_res = ColumnChunkMetaData::from_thrift(column_descr, col_chunk_exp.clone())
            .unwrap()
            .to_thrift();

        assert_eq!(col_chunk_res, col_chunk_exp);
    }

    #[test]
    fn test_compressed_size() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for column_descr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(column_descr.clone())
                .set_total_compressed_size(500)
                .set_total_uncompressed_size(700)
                .build()
                .unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr)
            .set_num_rows(1000)
            .set_column_metadata(columns)
            .build()
            .unwrap();

        let compressed_size_res: i64 = row_group_meta.compressed_size();
        let compressed_size_exp: i64 = 1000;

        assert_eq!(compressed_size_res, compressed_size_exp);
    }

    /// Returns sample schema descriptor so we can create column metadata.
    fn get_test_schema_descr() -> SchemaDescPtr {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
