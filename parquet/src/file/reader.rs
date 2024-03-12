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

//! File reader API and methods to access file metadata, row group
//! readers to read individual column chunks, or access record
//! iterator.

use bytes::{Buf, Bytes};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::{io::Read, sync::Arc};

use crate::bloom_filter::Sbbf;
use crate::column::page::PageIterator;
use crate::column::{page::PageReader, reader::ColumnReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::*;
pub use crate::file::serialized_reader::{SerializedFileReader, SerializedPageReader};
use crate::record::reader::RowIter;
use crate::schema::types::Type as SchemaType;

use crate::basic::Type;

use crate::column::reader::ColumnReaderImpl;

/// Length should return the total number of bytes in the input source.
/// It's mainly used to read the metadata, which is at the end of the source.
#[allow(clippy::len_without_is_empty)]
pub trait Length {
    /// Returns the amount of bytes of the inner source.
    fn len(&self) -> u64;
}

/// The ChunkReader trait generates readers of chunks of a source.
///
/// For more information see [`File::try_clone`]
pub trait ChunkReader: Length + Send + Sync {
    type T: Read;

    /// Get a [`Read`] starting at the provided file offset
    ///
    /// Subsequent or concurrent calls to [`Self::get_read`] or [`Self::get_bytes`] may
    /// side-effect on previously returned [`Self::T`]. Care should be taken to avoid this
    ///
    /// See [`File::try_clone`] for more information
    fn get_read(&self, start: u64) -> Result<Self::T>;

    /// Get a range as bytes
    ///
    /// Concurrent calls to [`Self::get_bytes`] may result in interleaved output
    ///
    /// See [`File::try_clone`] for more information
    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes>;
}

impl Length for File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }
}

impl ChunkReader for File {
    type T = BufReader<File>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        Ok(BufReader::new(self.try_clone()?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(length);
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        let read = reader.take(length as _).read_to_end(&mut buffer)?;

        if read != length {
            return Err(eof_err!(
                "Expected to read {} bytes, read only {}",
                length,
                read
            ));
        }
        Ok(buffer.into())
    }
}

impl Length for Bytes {
    fn len(&self) -> u64 {
        self.len() as u64
    }
}

impl ChunkReader for Bytes {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        let start = start as usize;
        Ok(self.slice(start..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let start = start as usize;
        Ok(self.slice(start..start + length))
    }
}

// ----------------------------------------------------------------------
// APIs for file & row group readers

/// Parquet file reader API. With this, user can get metadata information about the
/// Parquet file, can get reader for each row group, and access record iterator.
pub trait FileReader: Send + Sync {
    /// Get metadata information about this file.
    fn metadata(&self) -> &ParquetMetaData;

    /// Get the total number of row groups for this file.
    fn num_row_groups(&self) -> usize;

    /// Get the `i`th row group reader. Note this doesn't do bound check.
    fn get_row_group(&self, i: usize) -> Result<Box<dyn RowGroupReader + '_>>;

    /// Get an iterator over the row in this file, see [`RowIter`] for caveats.
    ///
    /// Iterator will automatically load the next row group to advance.
    ///
    /// Projected schema can be a subset of or equal to the file schema, when it is None,
    /// full file schema is assumed.
    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

/// Parquet row group reader API. With this, user can get metadata information about the
/// row group, as well as readers for each individual column chunk.
pub trait RowGroupReader: Send + Sync {
    /// Get metadata information about this row group.
    fn metadata(&self) -> &RowGroupMetaData;

    /// Get the total number of column chunks in this row group.
    fn num_columns(&self) -> usize;

    /// Get page reader for the `i`th column chunk.
    fn get_column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>>;

    /// Get value reader for the `i`th column chunk.
    fn get_column_reader(&self, i: usize) -> Result<ColumnReader> {
        let schema_descr = self.metadata().schema_descr();
        let col_descr = schema_descr.column(i);
        let col_page_reader = self.get_column_page_reader(i)?;
        let col_reader = match col_descr.physical_type() {
            Type::BOOLEAN => {
                ColumnReader::BoolColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::INT32 => {
                ColumnReader::Int32ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::INT64 => {
                ColumnReader::Int64ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::INT96 => {
                ColumnReader::Int96ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::FLOAT => {
                ColumnReader::FloatColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::DOUBLE => {
                ColumnReader::DoubleColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
            }
            Type::BYTE_ARRAY => ColumnReader::ByteArrayColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::FIXED_LEN_BYTE_ARRAY => ColumnReader::FixedLenByteArrayColumnReader(
                ColumnReaderImpl::new(col_descr, col_page_reader),
            ),
        };
        Ok(col_reader)
    }

    /// Get bloom filter for the `i`th column chunk, if present and the reader was configured
    /// to read bloom filters.
    fn get_column_bloom_filter(&self, i: usize) -> Option<&Sbbf>;

    /// Get an iterator over the row in this file, see [`RowIter`] for caveats.
    ///
    /// Projected schema can be a subset of or equal to the file schema, when it is None,
    /// full file schema is assumed.
    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

// ----------------------------------------------------------------------
// Iterator

/// Implementation of page iterator for parquet file.
pub struct FilePageIterator {
    column_index: usize,
    row_group_indices: Box<dyn Iterator<Item = usize> + Send>,
    file_reader: Arc<dyn FileReader>,
}

impl FilePageIterator {
    /// Creates a page iterator for all row groups in file.
    pub fn new(column_index: usize, file_reader: Arc<dyn FileReader>) -> Result<Self> {
        let num_row_groups = file_reader.metadata().num_row_groups();

        let row_group_indices = Box::new(0..num_row_groups);

        Self::with_row_groups(column_index, row_group_indices, file_reader)
    }

    /// Create page iterator from parquet file reader with only some row groups.
    pub fn with_row_groups(
        column_index: usize,
        row_group_indices: Box<dyn Iterator<Item = usize> + Send>,
        file_reader: Arc<dyn FileReader>,
    ) -> Result<Self> {
        // Check that column_index is valid
        let num_columns = file_reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();

        if column_index >= num_columns {
            return Err(ParquetError::IndexOutOfBound(column_index, num_columns));
        }

        // We don't check iterators here because iterator may be infinite
        Ok(Self {
            column_index,
            row_group_indices,
            file_reader,
        })
    }
}

impl Iterator for FilePageIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Result<Box<dyn PageReader>>> {
        self.row_group_indices.next().map(|row_group_index| {
            self.file_reader
                .get_row_group(row_group_index)
                .and_then(|r| r.get_column_page_reader(self.column_index))
        })
    }
}

impl PageIterator for FilePageIterator {}
