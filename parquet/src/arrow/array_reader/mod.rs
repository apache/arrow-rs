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

//! Logic for reading into arrow arrays

use crate::errors::Result;
use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::arrow::record_reader::GenericRecordReader;
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::file::reader::{FilePageIterator, FileReader};
use crate::schema::types::SchemaDescPtr;

mod builder;
mod byte_array;
mod byte_array_dictionary;
mod empty_array;
mod fixed_len_byte_array;
mod list_array;
mod map_array;
mod null_array;
mod primitive_array;
mod struct_array;

#[cfg(test)]
mod test_util;

pub use builder::build_array_reader;
pub use byte_array::make_byte_array_reader;
pub use byte_array_dictionary::make_byte_array_dictionary_reader;
pub use fixed_len_byte_array::make_fixed_len_byte_array_reader;
pub use list_array::ListArrayReader;
pub use map_array::MapArrayReader;
pub use null_array::NullArrayReader;
pub use primitive_array::PrimitiveArrayReader;
pub use struct_array::StructArrayReader;

/// Array reader reads parquet data into arrow array.
pub trait ArrayReader: Send {
    fn as_any(&self) -> &dyn Any;

    /// Returns the arrow type of this array reader.
    fn get_data_type(&self) -> &ArrowType;

    /// Reads at most `batch_size` records into an arrow array and return it.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        self.read_records(batch_size)?;
        self.consume_batch()
    }

    /// Reads at most `batch_size` records' bytes into buffer
    ///
    /// Returns the number of records read, which can be less than `batch_size` if
    /// pages is exhausted.
    fn read_records(&mut self, batch_size: usize) -> Result<usize>;

    /// Consume all currently stored buffer data
    /// into an arrow array and return it.
    fn consume_batch(&mut self) -> Result<ArrayRef>;

    /// Skips over `num_records` records, returning the number of rows skipped
    fn skip_records(&mut self, num_records: usize) -> Result<usize>;

    /// If this array has a non-zero definition level, i.e. has a nullable parent
    /// array, returns the definition levels of data from the last call of `next_batch`
    ///
    /// Otherwise returns None
    ///
    /// This is used by parent [`ArrayReader`] to compute their null bitmaps
    fn get_def_levels(&self) -> Option<&[i16]>;

    /// If this array has a non-zero repetition level, i.e. has a repeated parent
    /// array, returns the repetition levels of data from the last call of `next_batch`
    ///
    /// Otherwise returns None
    ///
    /// This is used by parent [`ArrayReader`] to compute their array offsets
    fn get_rep_levels(&self) -> Option<&[i16]>;
}

/// A collection of row groups
pub trait RowGroupCollection {
    /// Get schema of parquet file.
    fn schema(&self) -> SchemaDescPtr;

    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize;

    /// Returns an iterator over the column chunks for particular column
    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>>;
}

impl RowGroupCollection for Arc<dyn FileReader> {
    fn schema(&self) -> SchemaDescPtr {
        self.metadata().file_metadata().schema_descr_ptr()
    }

    fn num_rows(&self) -> usize {
        self.metadata().file_metadata().num_rows() as usize
    }

    fn column_chunks(&self, column_index: usize) -> Result<Box<dyn PageIterator>> {
        let iterator = FilePageIterator::new(column_index, Arc::clone(self))?;
        Ok(Box::new(iterator))
    }
}

pub(crate) struct FileReaderRowGroupCollection {
    /// The underling file reader
    reader: Arc<dyn FileReader>,
    /// Optional list of row group indices to scan
    row_groups: Option<Vec<usize>>,
}

impl FileReaderRowGroupCollection {
    /// Creates a new [`RowGroupCollection`] from a `FileReader` and an optional
    /// list of row group indexes to scan
    pub fn new(reader: Arc<dyn FileReader>, row_groups: Option<Vec<usize>>) -> Self {
        Self { reader, row_groups }
    }
}

impl RowGroupCollection for FileReaderRowGroupCollection {
    fn schema(&self) -> SchemaDescPtr {
        self.reader.metadata().file_metadata().schema_descr_ptr()
    }

    fn num_rows(&self) -> usize {
        match &self.row_groups {
            None => self.reader.metadata().file_metadata().num_rows() as usize,
            Some(row_groups) => {
                let meta = self.reader.metadata().row_groups();
                row_groups
                    .iter()
                    .map(|x| meta[*x].num_rows() as usize)
                    .sum()
            }
        }
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        let iterator = match &self.row_groups {
            Some(row_groups) => FilePageIterator::with_row_groups(
                i,
                Box::new(row_groups.clone().into_iter()),
                Arc::clone(&self.reader),
            )?,
            None => FilePageIterator::new(i, Arc::clone(&self.reader))?,
        };

        Ok(Box::new(iterator))
    }
}

/// Uses `record_reader` to read up to `batch_size` records from `pages`
///
/// Returns the number of records read, which can be less than `batch_size` if
/// pages is exhausted.
fn read_records<V, CV>(
    record_reader: &mut GenericRecordReader<V, CV>,
    pages: &mut dyn PageIterator,
    batch_size: usize,
) -> Result<usize>
where
    V: ValuesBuffer,
    CV: ColumnValueDecoder<Slice = V::Slice>,
{
    let mut records_read = 0usize;
    while records_read < batch_size {
        let records_to_read = batch_size - records_read;

        let records_read_once = record_reader.read_records(records_to_read)?;
        records_read += records_read_once;

        // Record reader exhausted
        if records_read_once < records_to_read {
            if let Some(page_reader) = pages.next() {
                // Read from new page reader (i.e. column chunk)
                record_reader.set_page_reader(page_reader?)?;
            } else {
                // Page reader also exhausted
                break;
            }
        }
    }
    Ok(records_read)
}

/// Uses `record_reader` to skip up to `batch_size` records from`pages`
///
/// Returns the number of records skipped, which can be less than `batch_size` if
/// pages is exhausted
fn skip_records<V, CV>(
    record_reader: &mut GenericRecordReader<V, CV>,
    pages: &mut dyn PageIterator,
    batch_size: usize,
) -> Result<usize>
where
    V: ValuesBuffer,
    CV: ColumnValueDecoder<Slice = V::Slice>,
{
    let mut records_skipped = 0usize;
    while records_skipped < batch_size {
        let records_to_read = batch_size - records_skipped;

        let records_skipped_once = record_reader.skip_records(records_to_read)?;
        records_skipped += records_skipped_once;

        // Record reader exhausted
        if records_skipped_once < records_to_read {
            if let Some(page_reader) = pages.next() {
                // Read from new page reader (i.e. column chunk)
                record_reader.set_page_reader(page_reader?)?;
            } else {
                // Page reader also exhausted
                break;
            }
        }
    }
    Ok(records_skipped)
}
