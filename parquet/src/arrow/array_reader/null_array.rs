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

use crate::arrow::array_reader::{read_records, skip_records, ArrayReader};
use crate::arrow::record_reader::RecordReader;
use crate::column::page::PageIterator;
use crate::data_type::DataType;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use arrow_array::ArrayRef;
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

/// A NullArrayReader reads Parquet columns stored as null int32s with an Arrow
/// NullArray type.
pub struct NullArrayReader<T>
where
    T: DataType,
    T::T: ArrowNativeType,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    record_reader: RecordReader<T>,
}

impl<T> NullArrayReader<T>
where
    T: DataType,
    T::T: ArrowNativeType,
{
    /// Construct null array reader.
    pub fn new(pages: Box<dyn PageIterator>, column_desc: ColumnDescPtr) -> Result<Self> {
        let record_reader = RecordReader::<T>::new(column_desc);

        Ok(Self {
            data_type: ArrowType::Null,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader,
        })
    }
}

/// Implementation of primitive array reader.
impl<T> ArrayReader for NullArrayReader<T>
where
    T: DataType,
    T::T: ArrowNativeType,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type of primitive array.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        // convert to arrays
        let array = arrow_array::NullArray::new(self.record_reader.num_values());

        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();

        // Must consume bitmap buffer
        self.record_reader.consume_bitmap_buffer();

        self.record_reader.reset();
        Ok(Arc::new(array))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        skip_records(&mut self.record_reader, self.pages.as_mut(), num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }
}
