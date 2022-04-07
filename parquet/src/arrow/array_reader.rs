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

use std::any::Any;
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::mem::size_of;
use std::result::Result::Ok;
use std::sync::Arc;
use std::vec::Vec;

use arrow::array::{
    new_empty_array, Array, ArrayData, ArrayDataBuilder, ArrayRef, BooleanArray,
    BooleanBufferBuilder, DecimalArray, GenericListArray, Int16BufferBuilder, Int32Array,
    Int64Array, MapArray, OffsetSizeTrait, PrimitiveArray, StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::compute::take;
use arrow::datatypes::{
    ArrowPrimitiveType, BooleanType as ArrowBooleanType, DataType as ArrowType,
    Float32Type as ArrowFloat32Type, Float64Type as ArrowFloat64Type,
    Int32Type as ArrowInt32Type, Int64Type as ArrowInt64Type, ToByteSlice,
    UInt32Type as ArrowUInt32Type, UInt64Type as ArrowUInt64Type,
};
use arrow::util::bit_util;

use crate::arrow::converter::Converter;
use crate::arrow::record_reader::buffer::{ScalarValue, ValuesBuffer};
use crate::arrow::record_reader::{GenericRecordReader, RecordReader};
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::Type as PhysicalType;
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::column::reader::ColumnReaderImpl;
use crate::data_type::DataType;
use crate::errors::{ParquetError, ParquetError::ArrowError, Result};
use crate::file::reader::{FilePageIterator, FileReader};
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr};

mod builder;
mod byte_array;
mod byte_array_dictionary;
mod dictionary_buffer;
mod offset_buffer;

#[cfg(test)]
mod test_util;

pub use builder::build_array_reader;

pub use byte_array::make_byte_array_reader;
pub use byte_array_dictionary::make_byte_array_dictionary_reader;

/// Array reader reads parquet data into arrow array.
pub trait ArrayReader: Send {
    fn as_any(&self) -> &dyn Any;

    /// Returns the arrow type of this array reader.
    fn get_data_type(&self) -> &ArrowType;

    /// Reads at most `batch_size` records into an arrow array and return it.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef>;

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
    fn schema(&self) -> Result<SchemaDescPtr>;

    /// Returns an iterator over the column chunks for particular column
    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>>;
}

impl RowGroupCollection for Arc<dyn FileReader> {
    fn schema(&self) -> Result<SchemaDescPtr> {
        Ok(self.metadata().file_metadata().schema_descr_ptr())
    }

    fn column_chunks(&self, column_index: usize) -> Result<Box<dyn PageIterator>> {
        let iterator = FilePageIterator::new(column_index, Arc::clone(self))?;
        Ok(Box::new(iterator))
    }
}

/// Uses `record_reader` to read up to `batch_size` records from `pages`
///
/// Returns the number of records read, which can be less than batch_size if
/// pages is exhausted.
fn read_records<V, CV>(
    record_reader: &mut GenericRecordReader<V, CV>,
    pages: &mut dyn PageIterator,
    batch_size: usize,
) -> Result<usize>
where
    V: ValuesBuffer + Default,
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

/// A NullArrayReader reads Parquet columns stored as null int32s with an Arrow
/// NullArray type.
pub struct NullArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    column_desc: ColumnDescPtr,
    record_reader: RecordReader<T>,
    _type_marker: PhantomData<T>,
}

impl<T> NullArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    /// Construct null array reader.
    pub fn new(pages: Box<dyn PageIterator>, column_desc: ColumnDescPtr) -> Result<Self> {
        let record_reader = RecordReader::<T>::new(column_desc.clone());

        Ok(Self {
            data_type: ArrowType::Null,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            column_desc,
            record_reader,
            _type_marker: PhantomData,
        })
    }
}

/// Implementation of primitive array reader.
impl<T> ArrayReader for NullArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type of primitive array.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    /// Reads at most `batch_size` records into array.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)?;

        // convert to arrays
        let array = arrow::array::NullArray::new(self.record_reader.num_values());

        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels()?;
        self.rep_levels_buffer = self.record_reader.consume_rep_levels()?;

        // Must consume bitmap buffer
        self.record_reader.consume_bitmap_buffer()?;

        self.record_reader.reset();
        Ok(Arc::new(array))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    column_desc: ColumnDescPtr,
    record_reader: RecordReader<T>,
}

impl<T> PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    /// Construct primitive array reader.
    pub fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        arrow_type: Option<ArrowType>,
    ) -> Result<Self> {
        Self::new_with_options(pages, column_desc, arrow_type, false)
    }

    /// Construct primitive array reader with ability to only compute null mask and not
    /// buffer level data
    pub fn new_with_options(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        arrow_type: Option<ArrowType>,
        null_mask_only: bool,
    ) -> Result<Self> {
        // Check if Arrow type is specified, else create it from Parquet type
        let data_type = match arrow_type {
            Some(t) => t,
            None => parquet_to_arrow_field(column_desc.as_ref())?
                .data_type()
                .clone(),
        };

        let record_reader =
            RecordReader::<T>::new_with_options(column_desc.clone(), null_mask_only);

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            column_desc,
            record_reader,
        })
    }
}

/// Implementation of primitive array reader.
impl<T> ArrayReader for PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: ScalarValue,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type of primitive array.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    /// Reads at most `batch_size` records into array.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)?;

        let target_type = self.get_data_type().clone();
        let arrow_data_type = match T::get_physical_type() {
            PhysicalType::BOOLEAN => ArrowBooleanType::DATA_TYPE,
            PhysicalType::INT32 => {
                match target_type {
                    ArrowType::UInt32 => {
                        // follow C++ implementation and use overflow/reinterpret cast from  i32 to u32 which will map
                        // `i32::MIN..0` to `(i32::MAX as u32)..u32::MAX`
                        ArrowUInt32Type::DATA_TYPE
                    }
                    _ => ArrowInt32Type::DATA_TYPE,
                }
            }
            PhysicalType::INT64 => {
                match target_type {
                    ArrowType::UInt64 => {
                        // follow C++ implementation and use overflow/reinterpret cast from  i64 to u64 which will map
                        // `i64::MIN..0` to `(i64::MAX as u64)..u64::MAX`
                        ArrowUInt64Type::DATA_TYPE
                    }
                    _ => ArrowInt64Type::DATA_TYPE,
                }
            }
            PhysicalType::FLOAT => ArrowFloat32Type::DATA_TYPE,
            PhysicalType::DOUBLE => ArrowFloat64Type::DATA_TYPE,
            PhysicalType::INT96
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                unreachable!(
                    "PrimitiveArrayReaders don't support complex physical types"
                );
            }
        };

        // Convert to arrays by using the Parquet physical type.
        // The physical types are then cast to Arrow types if necessary

        let mut record_data = self.record_reader.consume_record_data()?;

        if T::get_physical_type() == PhysicalType::BOOLEAN {
            let mut boolean_buffer = BooleanBufferBuilder::new(record_data.len());

            for e in record_data.as_slice() {
                boolean_buffer.append(*e > 0);
            }
            record_data = boolean_buffer.finish();
        }

        let mut array_data = ArrayDataBuilder::new(arrow_data_type)
            .len(self.record_reader.num_values())
            .add_buffer(record_data);

        if let Some(b) = self.record_reader.consume_bitmap_buffer()? {
            array_data = array_data.null_bit_buffer(b);
        }

        let array_data = unsafe { array_data.build_unchecked() };
        let array = match T::get_physical_type() {
            PhysicalType::BOOLEAN => Arc::new(BooleanArray::from(array_data)) as ArrayRef,
            PhysicalType::INT32 => {
                Arc::new(PrimitiveArray::<ArrowInt32Type>::from(array_data)) as ArrayRef
            }
            PhysicalType::INT64 => {
                Arc::new(PrimitiveArray::<ArrowInt64Type>::from(array_data)) as ArrayRef
            }
            PhysicalType::FLOAT => {
                Arc::new(PrimitiveArray::<ArrowFloat32Type>::from(array_data)) as ArrayRef
            }
            PhysicalType::DOUBLE => {
                Arc::new(PrimitiveArray::<ArrowFloat64Type>::from(array_data)) as ArrayRef
            }
            PhysicalType::INT96
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                unreachable!(
                    "PrimitiveArrayReaders don't support complex physical types"
                );
            }
        };

        // cast to Arrow type
        // We make a strong assumption here that the casts should be infallible.
        // If the cast fails because of incompatible datatypes, then there might
        // be a bigger problem with how Arrow schemas are converted to Parquet.
        //
        // As there is not always a 1:1 mapping between Arrow and Parquet, there
        // are datatypes which we must convert explicitly.
        // These are:
        // - date64: we should cast int32 to date32, then date32 to date64.
        let array = match target_type {
            ArrowType::Date64 => {
                // this is cheap as it internally reinterprets the data
                let a = arrow::compute::cast(&array, &ArrowType::Date32)?;
                arrow::compute::cast(&a, &target_type)?
            }
            ArrowType::Decimal(p, s) => {
                let array = match array.data_type() {
                    ArrowType::Int32 => array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| v.into()))
                        .collect::<DecimalArray>(),

                    ArrowType::Int64 => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| v.into()))
                        .collect::<DecimalArray>(),
                    _ => {
                        return Err(ArrowError(format!(
                            "Cannot convert {:?} to decimal",
                            array.data_type()
                        )))
                    }
                }
                .with_precision_and_scale(p, s)?;

                Arc::new(array) as ArrayRef
            }
            _ => arrow::compute::cast(&array, &target_type)?,
        };

        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels()?;
        self.rep_levels_buffer = self.record_reader.consume_rep_levels()?;
        self.record_reader.reset();
        Ok(array)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    column_desc: ColumnDescPtr,
    column_reader: Option<ColumnReaderImpl<T>>,
    converter: C,
    _parquet_type_marker: PhantomData<T>,
    _converter_marker: PhantomData<C>,
}

impl<T, C> ArrayReader for ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + Send + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        // Try to initialize column reader
        if self.column_reader.is_none() {
            self.next_column_reader()?;
        }

        let mut data_buffer: Vec<T::T> = Vec::with_capacity(batch_size);
        data_buffer.resize_with(batch_size, T::T::default);

        let mut def_levels_buffer = if self.column_desc.max_def_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut rep_levels_buffer = if self.column_desc.max_rep_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut num_read = 0;

        while self.column_reader.is_some() && num_read < batch_size {
            let num_to_read = batch_size - num_read;
            let cur_data_buf = &mut data_buffer[num_read..];
            let cur_def_levels_buf =
                def_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let cur_rep_levels_buf =
                rep_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let (data_read, levels_read) =
                self.column_reader.as_mut().unwrap().read_batch(
                    num_to_read,
                    cur_def_levels_buf,
                    cur_rep_levels_buf,
                    cur_data_buf,
                )?;

            // Fill space
            if levels_read > data_read {
                def_levels_buffer.iter().for_each(|def_levels_buffer| {
                    let (mut level_pos, mut data_pos) = (levels_read, data_read);
                    while level_pos > 0 && data_pos > 0 {
                        if def_levels_buffer[num_read + level_pos - 1]
                            == self.column_desc.max_def_level()
                        {
                            cur_data_buf.swap(level_pos - 1, data_pos - 1);
                            level_pos -= 1;
                            data_pos -= 1;
                        } else {
                            level_pos -= 1;
                        }
                    }
                });
            }

            let values_read = max(levels_read, data_read);
            num_read += values_read;
            // current page exhausted && page iterator exhausted
            if values_read < num_to_read && !self.next_column_reader()? {
                break;
            }
        }

        data_buffer.truncate(num_read);
        def_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));
        rep_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));

        self.def_levels_buffer = def_levels_buffer;
        self.rep_levels_buffer = rep_levels_buffer;

        let data: Vec<Option<T::T>> = if self.def_levels_buffer.is_some() {
            data_buffer
                .into_iter()
                .zip(self.def_levels_buffer.as_ref().unwrap().iter())
                .map(|(t, def_level)| {
                    if *def_level == self.column_desc.max_def_level() {
                        Some(t)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            data_buffer.into_iter().map(Some).collect()
        };

        let mut array = self.converter.convert(data)?;

        if let ArrowType::Dictionary(_, _) = self.data_type {
            array = arrow::compute::cast(&array, &self.data_type)?;
        }

        Ok(array)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_deref()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_deref()
    }
}

impl<T, C> ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    pub fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        converter: C,
        arrow_type: Option<ArrowType>,
    ) -> Result<Self> {
        let data_type = match arrow_type {
            Some(t) => t,
            None => parquet_to_arrow_field(column_desc.as_ref())?
                .data_type()
                .clone(),
        };

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            column_desc,
            column_reader: None,
            converter,
            _parquet_type_marker: PhantomData,
            _converter_marker: PhantomData,
        })
    }

    fn next_column_reader(&mut self) -> Result<bool> {
        Ok(match self.pages.next() {
            Some(page) => {
                self.column_reader =
                    Some(ColumnReaderImpl::<T>::new(self.column_desc.clone(), page?));
                true
            }
            None => false,
        })
    }
}

/// Implementation of list array reader.
pub struct ListArrayReader<OffsetSize: OffsetSizeTrait> {
    item_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    item_type: ArrowType,
    list_def_level: i16,
    list_rep_level: i16,
    list_empty_def_level: i16,
    list_null_def_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
    _marker: PhantomData<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> ListArrayReader<OffsetSize> {
    /// Construct list array reader.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        item_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        list_null_def_level: i16,
        list_empty_def_level: i16,
    ) -> Self {
        Self {
            item_reader,
            data_type,
            item_type,
            list_def_level: def_level,
            list_rep_level: rep_level,
            list_null_def_level,
            list_empty_def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
            _marker: PhantomData,
        }
    }
}

/// Implementation of ListArrayReader. Nested lists and lists of structs are not yet supported.
impl<OffsetSize: OffsetSizeTrait> ArrayReader for ListArrayReader<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a List.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let next_batch_array = self.item_reader.next_batch(batch_size)?;

        if next_batch_array.len() == 0 {
            return Ok(new_empty_array(&self.data_type));
        }
        let def_levels = self
            .item_reader
            .get_def_levels()
            .ok_or_else(|| ArrowError("item_reader def levels are None.".to_string()))?;
        let rep_levels = self
            .item_reader
            .get_rep_levels()
            .ok_or_else(|| ArrowError("item_reader rep levels are None.".to_string()))?;

        if !((def_levels.len() == rep_levels.len())
            && (rep_levels.len() == next_batch_array.len()))
        {
            return Err(ArrowError(
                format!("Expected item_reader def_levels {} and rep_levels {} to be same length as batch {}", def_levels.len(), rep_levels.len(), next_batch_array.len()),
            ));
        }

        // List definitions can be encoded as 4 values:
        // - n + 0: the list slot is null
        // - n + 1: the list slot is not null, but is empty (i.e. [])
        // - n + 2: the list slot is not null, but its child is empty (i.e. [ null ])
        // - n + 3: the list slot is not null, and its child is not empty
        // Where n is the max definition level of the list's parent.
        // If a Parquet schema's only leaf is the list, then n = 0.

        // If the list index is at empty definition, the child slot is null
        let non_null_list_indices =
            def_levels.iter().enumerate().filter_map(|(index, def)| {
                (*def > self.list_empty_def_level).then(|| index as u32)
            });
        let indices = UInt32Array::from_iter_values(non_null_list_indices);
        let batch_values = take(&*next_batch_array.clone(), &indices, None)?;

        // first item in each list has rep_level = 0, subsequent items have rep_level = 1
        let mut offsets: Vec<OffsetSize> = Vec::new();
        let mut cur_offset = OffsetSize::zero();
        def_levels.iter().zip(rep_levels).for_each(|(d, r)| {
            if *r == 0 || d == &self.list_empty_def_level {
                offsets.push(cur_offset);
            }
            if d > &self.list_empty_def_level {
                cur_offset += OffsetSize::one();
            }
        });

        offsets.push(cur_offset);

        let num_bytes = bit_util::ceil(offsets.len(), 8);
        // TODO: A useful optimization is to use the null count to fill with
        // 0 or null, to reduce individual bits set in a loop.
        // To favour dense data, set every slot to true, then unset
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        let mut list_index = 0;
        for i in 0..rep_levels.len() {
            // If the level is lower than empty, then the slot is null.
            // When a list is non-nullable, its empty level = null level,
            // so this automatically factors that in.
            if rep_levels[i] == 0 && def_levels[i] < self.list_empty_def_level {
                bit_util::unset_bit(null_slice, list_index);
            }
            if rep_levels[i] == 0 {
                list_index += 1;
            }
        }
        let value_offsets = Buffer::from(&offsets.to_byte_slice());

        let list_data = ArrayData::builder(self.get_data_type().clone())
            .len(offsets.len() - 1)
            .add_buffer(value_offsets)
            .add_child_data(batch_values.data().clone())
            .null_bit_buffer(null_buf.into())
            .offset(next_batch_array.offset());

        let list_data = unsafe { list_data.build_unchecked() };

        let result_array = GenericListArray::<OffsetSize>::from(list_data);
        Ok(Arc::new(result_array))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Implementation of a map array reader.
pub struct MapArrayReader {
    key_reader: Box<dyn ArrayReader>,
    value_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    map_def_level: i16,
    map_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl MapArrayReader {
    pub fn new(
        key_reader: Box<dyn ArrayReader>,
        value_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            key_reader,
            value_reader,
            data_type,
            map_def_level: rep_level,
            map_rep_level: def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for MapArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let key_array = self.key_reader.next_batch(batch_size)?;
        let value_array = self.value_reader.next_batch(batch_size)?;

        // Check that key and value have the same lengths
        let key_length = key_array.len();
        if key_length != value_array.len() {
            return Err(general_err!(
                "Map key and value should have the same lengths."
            ));
        }

        let def_levels = self
            .key_reader
            .get_def_levels()
            .ok_or_else(|| ArrowError("item_reader def levels are None.".to_string()))?;
        let rep_levels = self
            .key_reader
            .get_rep_levels()
            .ok_or_else(|| ArrowError("item_reader rep levels are None.".to_string()))?;

        if !((def_levels.len() == rep_levels.len()) && (rep_levels.len() == key_length)) {
            return Err(ArrowError(
                "Expected item_reader def_levels and rep_levels to be same length as batch".to_string(),
            ));
        }

        let entry_data_type = if let ArrowType::Map(field, _) = &self.data_type {
            field.data_type().clone()
        } else {
            return Err(ArrowError("Expected a map arrow type".to_string()));
        };

        let entry_data = ArrayDataBuilder::new(entry_data_type)
            .len(key_length)
            .add_child_data(key_array.data().clone())
            .add_child_data(value_array.data().clone());
        let entry_data = unsafe { entry_data.build_unchecked() };

        let entry_len = rep_levels.iter().filter(|level| **level == 0).count();

        // first item in each list has rep_level = 0, subsequent items have rep_level = 1
        let mut offsets: Vec<i32> = Vec::new();
        let mut cur_offset = 0;
        def_levels.iter().zip(rep_levels).for_each(|(d, r)| {
            if *r == 0 || d == &self.map_def_level {
                offsets.push(cur_offset);
            }
            if d > &self.map_def_level {
                cur_offset += 1;
            }
        });
        offsets.push(cur_offset);

        let num_bytes = bit_util::ceil(offsets.len(), 8);
        // TODO: A useful optimization is to use the null count to fill with
        // 0 or null, to reduce individual bits set in a loop.
        // To favour dense data, set every slot to true, then unset
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        let mut list_index = 0;
        for i in 0..rep_levels.len() {
            // If the level is lower than empty, then the slot is null.
            // When a list is non-nullable, its empty level = null level,
            // so this automatically factors that in.
            if rep_levels[i] == 0 && def_levels[i] < self.map_def_level {
                // should be empty list
                bit_util::unset_bit(null_slice, list_index);
            }
            if rep_levels[i] == 0 {
                list_index += 1;
            }
        }
        let value_offsets = Buffer::from(&offsets.to_byte_slice());

        // Now we can build array data
        let array_data = ArrayDataBuilder::new(self.data_type.clone())
            .len(entry_len)
            .add_buffer(value_offsets)
            .null_bit_buffer(null_buf.into())
            .add_child_data(entry_data);

        let array_data = unsafe { array_data.build_unchecked() };

        Ok(Arc::new(MapArray::from(array_data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Implementation of struct array reader.
pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: ArrowType,
    struct_def_level: i16,
    struct_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl StructArrayReader {
    /// Construct struct array reader.
    pub fn new(
        data_type: ArrowType,
        children: Vec<Box<dyn ArrayReader>>,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            struct_rep_level: rep_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a struct.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    /// Read `batch_size` struct records.
    ///
    /// Definition levels of struct array is calculated as following:
    /// ```ignore
    /// def_levels[i] = min(child1_def_levels[i], child2_def_levels[i], ...,
    /// childn_def_levels[i]);
    /// ```
    ///
    /// Repetition levels of struct array is calculated as following:
    /// ```ignore
    /// rep_levels[i] = child1_rep_levels[i];
    /// ```
    ///
    /// The null bitmap of struct array is calculated from def_levels:
    /// ```ignore
    /// null_bitmap[i] = (def_levels[i] >= self.def_level);
    /// ```
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        if self.children.is_empty() {
            self.def_level_buffer = None;
            self.rep_level_buffer = None;
            return Ok(Arc::new(StructArray::from(Vec::new())));
        }

        let children_array = self
            .children
            .iter_mut()
            .map(|reader| reader.next_batch(batch_size))
            .try_fold(
                Vec::new(),
                |mut result, child_array| -> Result<Vec<ArrayRef>> {
                    result.push(child_array?);
                    Ok(result)
                },
            )?;

        // check that array child data has same size
        let children_array_len =
            children_array.first().map(|arr| arr.len()).ok_or_else(|| {
                general_err!("Struct array reader should have at least one child!")
            })?;

        let all_children_len_eq = children_array
            .iter()
            .all(|arr| arr.len() == children_array_len);
        if !all_children_len_eq {
            return Err(general_err!("Not all children array length are the same!"));
        }

        // Now we can build array data
        let mut array_data_builder = ArrayDataBuilder::new(self.data_type.clone())
            .len(children_array_len)
            .child_data(
                children_array
                    .iter()
                    .map(|x| x.data().clone())
                    .collect::<Vec<ArrayData>>(),
            );

        if self.struct_def_level != 0 {
            // calculate struct def level data
            let buffer_size = children_array_len * size_of::<i16>();
            let mut def_level_data_buffer = MutableBuffer::new(buffer_size);
            def_level_data_buffer.resize(buffer_size, 0);

            // Safety: the buffer is always treated as `u16` in the code below
            let def_level_data = unsafe { def_level_data_buffer.typed_data_mut() };

            def_level_data
                .iter_mut()
                .for_each(|v| *v = self.struct_def_level);

            for child in &self.children {
                if let Some(current_child_def_levels) = child.get_def_levels() {
                    if current_child_def_levels.len() != children_array_len {
                        return Err(general_err!("Child array length are not equal!"));
                    } else {
                        for i in 0..children_array_len {
                            def_level_data[i] =
                                min(def_level_data[i], current_child_def_levels[i]);
                        }
                    }
                }
            }

            // calculate bitmap for current array
            let mut bitmap_builder = BooleanBufferBuilder::new(children_array_len);
            for def_level in def_level_data {
                let not_null = *def_level >= self.struct_def_level;
                bitmap_builder.append(not_null);
            }

            array_data_builder =
                array_data_builder.null_bit_buffer(bitmap_builder.finish());

            self.def_level_buffer = Some(def_level_data_buffer.into());
        }

        let array_data = unsafe { array_data_builder.build_unchecked() };

        if self.struct_rep_level != 0 {
            // calculate struct rep level data, since struct doesn't add to repetition
            // levels, here we just need to keep repetition levels of first array
            // TODO: Verify that all children array reader has same repetition levels
            let rep_level_data = self
                .children
                .first()
                .ok_or_else(|| {
                    general_err!("Struct array reader should have at least one child!")
                })?
                .get_rep_levels()
                .map(|data| -> Result<Buffer> {
                    let mut buffer = Int16BufferBuilder::new(children_array_len);
                    buffer.append_slice(data);
                    Ok(buffer.finish())
                })
                .transpose()?;

            self.rep_level_buffer = rep_level_data;
        }
        Ok(Arc::new(StructArray::from(array_data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::VecDeque;
    use std::sync::Arc;

    use rand::distributions::uniform::SampleUniform;
    use rand::{thread_rng, Rng};

    use arrow::array::{
        Array, ArrayRef, LargeListArray, ListArray, PrimitiveArray, StringArray,
        StructArray,
    };
    use arrow::datatypes::{
        ArrowPrimitiveType, DataType as ArrowType, Date32Type as ArrowDate32, Field,
        Int32Type as ArrowInt32, Int64Type as ArrowInt64,
        Time32MillisecondType as ArrowTime32MillisecondArray,
        Time64MicrosecondType as ArrowTime64MicrosecondArray,
        TimestampMicrosecondType as ArrowTimestampMicrosecondType,
        TimestampMillisecondType as ArrowTimestampMillisecondType,
    };

    use crate::arrow::converter::{Utf8ArrayConverter, Utf8Converter};
    use crate::basic::{Encoding, Type as PhysicalType};
    use crate::column::page::{Page, PageReader};
    use crate::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
    use crate::errors::Result;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::{ColumnDescPtr, SchemaDescriptor};
    use crate::util::test_common::make_pages;
    use crate::util::test_common::page_util::{
        DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator,
    };

    use super::*;

    fn make_column_chunks<T: DataType>(
        column_desc: ColumnDescPtr,
        encoding: Encoding,
        num_levels: usize,
        min_value: T::T,
        max_value: T::T,
        def_levels: &mut Vec<i16>,
        rep_levels: &mut Vec<i16>,
        values: &mut Vec<T::T>,
        page_lists: &mut Vec<Vec<Page>>,
        use_v2: bool,
        num_chunks: usize,
    ) where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        for _i in 0..num_chunks {
            let mut pages = VecDeque::new();
            let mut data = Vec::new();
            let mut page_def_levels = Vec::new();
            let mut page_rep_levels = Vec::new();

            make_pages::<T>(
                column_desc.clone(),
                encoding,
                1,
                num_levels,
                min_value,
                max_value,
                &mut page_def_levels,
                &mut page_rep_levels,
                &mut data,
                &mut pages,
                use_v2,
            );

            def_levels.append(&mut page_def_levels);
            rep_levels.append(&mut page_rep_levels);
            values.append(&mut data);
            page_lists.push(Vec::from(pages));
        }
    }

    #[test]
    fn test_primitive_array_reader_empty_pages() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);
        let page_iterator = EmptyPageIterator::new(schema);

        let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
            Box::new(page_iterator),
            column_desc,
            None,
        )
        .unwrap();

        // expect no values to be read
        let array = array_reader.next_batch(50).unwrap();
        assert!(array.is_empty());
    }

    #[test]
    fn test_primitive_array_reader_data() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator =
                InMemoryPageIterator::new(schema, column_desc.clone(), page_lists);

            let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
                Box::new(page_iterator),
                column_desc,
                None,
            )
            .unwrap();

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[0..50].to_vec()),
                array
            );

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[50..150].to_vec()),
                array
            );

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[150..200].to_vec()),
                array
            );
        }
    }

    macro_rules! test_primitive_array_reader_one_type {
        ($arrow_parquet_type:ty, $physical_type:expr, $converted_type_str:expr, $result_arrow_type:ty, $result_arrow_cast_type:ty, $result_primitive_type:ty) => {{
            let message_type = format!(
                "
            message test_schema {{
              REQUIRED {:?} leaf ({});
          }}
            ",
                $physical_type, $converted_type_str
            );
            let schema = parse_message_type(&message_type)
                .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
                .unwrap();

            let column_desc = schema.column(0);

            // Construct page iterator
            {
                let mut data = Vec::new();
                let mut page_lists = Vec::new();
                make_column_chunks::<$arrow_parquet_type>(
                    column_desc.clone(),
                    Encoding::PLAIN,
                    100,
                    1,
                    200,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &mut data,
                    &mut page_lists,
                    true,
                    2,
                );
                let page_iterator = InMemoryPageIterator::new(
                    schema.clone(),
                    column_desc.clone(),
                    page_lists,
                );
                let mut array_reader = PrimitiveArrayReader::<$arrow_parquet_type>::new(
                    Box::new(page_iterator),
                    column_desc.clone(),
                    None,
                )
                .expect("Unable to get array reader");

                let array = array_reader
                    .next_batch(50)
                    .expect("Unable to get batch from reader");

                let result_data_type = <$result_arrow_type>::DATA_TYPE;
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$result_arrow_type>>()
                    .expect(
                        format!(
                            "Unable to downcast {:?} to {:?}",
                            array.data_type(),
                            result_data_type
                        )
                        .as_str(),
                    );

                // create expected array as primitive, and cast to result type
                let expected = PrimitiveArray::<$result_arrow_cast_type>::from(
                    data[0..50]
                        .iter()
                        .map(|x| *x as $result_primitive_type)
                        .collect::<Vec<$result_primitive_type>>(),
                );
                let expected = Arc::new(expected) as ArrayRef;
                let expected = arrow::compute::cast(&expected, &result_data_type)
                    .expect("Unable to cast expected array");
                assert_eq!(expected.data_type(), &result_data_type);
                let expected = expected
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$result_arrow_type>>()
                    .expect(
                        format!(
                            "Unable to downcast expected {:?} to {:?}",
                            expected.data_type(),
                            result_data_type
                        )
                        .as_str(),
                    );
                assert_eq!(expected, array);
            }
        }};
    }

    #[test]
    fn test_primitive_array_reader_temporal_types() {
        test_primitive_array_reader_one_type!(
            Int32Type,
            PhysicalType::INT32,
            "DATE",
            ArrowDate32,
            ArrowInt32,
            i32
        );
        test_primitive_array_reader_one_type!(
            Int32Type,
            PhysicalType::INT32,
            "TIME_MILLIS",
            ArrowTime32MillisecondArray,
            ArrowInt32,
            i32
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIME_MICROS",
            ArrowTime64MicrosecondArray,
            ArrowInt64,
            i64
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MILLIS",
            ArrowTimestampMillisecondType,
            ArrowInt64,
            i64
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MICROS",
            ArrowTimestampMicrosecondType,
            ArrowInt64,
            i64
        );
    }

    #[test]
    fn test_primitive_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL INT32 leaf;
            }
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut def_levels,
                &mut rep_levels,
                &mut Vec::new(),
                &mut page_lists,
                true,
                2,
            );

            let page_iterator =
                InMemoryPageIterator::new(schema, column_desc.clone(), page_lists);

            let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
                Box::new(page_iterator),
                column_desc,
                None,
            )
            .unwrap();

            let mut accu_len: usize = 0;

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
        }
    }

    #[test]
    fn test_complex_array_reader_no_pages() {
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);
        let pages: Vec<Vec<Page>> = Vec::new();
        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);

        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let values_per_page = 100; // this value is arbitrary in this test - the result should always be an array of 0 length
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn test_complex_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let num_pages = 2;
        let values_per_page = 100;
        let str_base = "Hello World";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let max_def_level = schema.column(0).max_def_level();
        let max_rep_level = schema.column(0).max_rep_level();

        assert_eq!(max_def_level, 2);
        assert_eq!(max_rep_level, 1);

        let mut rng = thread_rng();
        let column_desc = schema.column(0);
        let mut pages: Vec<Vec<Page>> = Vec::new();

        let mut rep_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut def_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut all_values = Vec::with_capacity(num_pages * values_per_page);

        for i in 0..num_pages {
            let mut values = Vec::with_capacity(values_per_page);

            for _ in 0..values_per_page {
                let def_level = rng.gen_range(0..max_def_level + 1);
                let rep_level = rng.gen_range(0..max_rep_level + 1);
                if def_level == max_def_level {
                    let len = rng.gen_range(1..str_base.len());
                    let slice = &str_base[..len];
                    values.push(ByteArray::from(slice));
                    all_values.push(Some(slice.to_string()));
                } else {
                    all_values.push(None)
                }
                rep_levels.push(rep_level);
                def_levels.push(def_level)
            }

            let range = i * values_per_page..(i + 1) * values_per_page;
            let mut pb =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);

            pb.add_rep_levels(max_rep_level, &rep_levels.as_slice()[range.clone()]);
            pb.add_def_levels(max_def_level, &def_levels.as_slice()[range]);
            pb.add_values::<ByteArrayType>(Encoding::PLAIN, values.as_slice());

            let data_page = pb.consume();
            pages.push(vec![data_page]);
        }

        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);

        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let mut accu_len: usize = 0;

        let array = array_reader.next_batch(values_per_page / 2).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
        accu_len += array.len();

        // Read next values_per_page values, the first values_per_page/2 ones are from the first column chunk,
        // and the last values_per_page/2 ones are from the second column chunk
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..array.len() {
            if array.is_valid(i) {
                assert_eq!(
                    all_values[i + accu_len].as_ref().unwrap().as_str(),
                    strings.value(i)
                )
            } else {
                assert_eq!(all_values[i + accu_len], None)
            }
        }
        accu_len += array.len();

        // Try to read values_per_page values, however there are only values_per_page/2 values
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
    }

    #[test]
    fn test_complex_array_reader_dict_enc_string() {
        use crate::encodings::encoding::{DictEncoder, Encoder};
        use crate::util::memory::MemTracker;
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let num_pages = 2;
        let values_per_page = 100;
        let str_base = "Hello World";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);
        let max_def_level = column_desc.max_def_level();
        let max_rep_level = column_desc.max_rep_level();

        assert_eq!(max_def_level, 2);
        assert_eq!(max_rep_level, 1);

        let mut rng = thread_rng();
        let mut pages: Vec<Vec<Page>> = Vec::new();

        let mut rep_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut def_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut all_values = Vec::with_capacity(num_pages * values_per_page);

        for i in 0..num_pages {
            let mem_tracker = Arc::new(MemTracker::new());
            let mut dict_encoder =
                DictEncoder::<ByteArrayType>::new(column_desc.clone(), mem_tracker);
            // add data page
            let mut values = Vec::with_capacity(values_per_page);

            for _ in 0..values_per_page {
                let def_level = rng.gen_range(0..max_def_level + 1);
                let rep_level = rng.gen_range(0..max_rep_level + 1);
                if def_level == max_def_level {
                    let len = rng.gen_range(1..str_base.len());
                    let slice = &str_base[..len];
                    values.push(ByteArray::from(slice));
                    all_values.push(Some(slice.to_string()));
                } else {
                    all_values.push(None)
                }
                rep_levels.push(rep_level);
                def_levels.push(def_level)
            }

            let range = i * values_per_page..(i + 1) * values_per_page;
            let mut pb =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            pb.add_rep_levels(max_rep_level, &rep_levels.as_slice()[range.clone()]);
            pb.add_def_levels(max_def_level, &def_levels.as_slice()[range]);
            let _ = dict_encoder.put(&values);
            let indices = dict_encoder
                .write_indices()
                .expect("write_indices() should be OK");
            pb.add_indices(indices);
            let data_page = pb.consume();
            // for each page log num_values vs actual values in page
            // println!("page num_values: {}, values.len(): {}", data_page.num_values(), values.len());
            // add dictionary page
            let dict = dict_encoder
                .write_dict()
                .expect("write_dict() should be OK");
            let dict_page = Page::DictionaryPage {
                buf: dict,
                num_values: dict_encoder.num_entries() as u32,
                encoding: Encoding::RLE_DICTIONARY,
                is_sorted: false,
            };
            pages.push(vec![dict_page, data_page]);
        }

        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);
        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let mut accu_len: usize = 0;

        // println!("---------- reading a batch of {} values ----------", values_per_page / 2);
        let array = array_reader.next_batch(values_per_page / 2).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
        accu_len += array.len();

        // Read next values_per_page values, the first values_per_page/2 ones are from the first column chunk,
        // and the last values_per_page/2 ones are from the second column chunk
        // println!("---------- reading a batch of {} values ----------", values_per_page);
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..array.len() {
            if array.is_valid(i) {
                assert_eq!(
                    all_values[i + accu_len].as_ref().unwrap().as_str(),
                    strings.value(i)
                )
            } else {
                assert_eq!(all_values[i + accu_len], None)
            }
        }
        accu_len += array.len();

        // Try to read values_per_page values, however there are only values_per_page/2 values
        // println!("---------- reading a batch of {} values ----------", values_per_page);
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
    }

    /// Array reader for test.
    struct InMemoryArrayReader {
        data_type: ArrowType,
        array: ArrayRef,
        def_levels: Option<Vec<i16>>,
        rep_levels: Option<Vec<i16>>,
    }

    impl InMemoryArrayReader {
        pub fn new(
            data_type: ArrowType,
            array: ArrayRef,
            def_levels: Option<Vec<i16>>,
            rep_levels: Option<Vec<i16>>,
        ) -> Self {
            Self {
                data_type,
                array,
                def_levels,
                rep_levels,
            }
        }
    }

    impl ArrayReader for InMemoryArrayReader {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_data_type(&self) -> &ArrowType {
            &self.data_type
        }

        fn next_batch(&mut self, _batch_size: usize) -> Result<ArrayRef> {
            Ok(self.array.clone())
        }

        fn get_def_levels(&self) -> Option<&[i16]> {
            self.def_levels.as_deref()
        }

        fn get_rep_levels(&self) -> Option<&[i16]> {
            self.rep_levels.as_deref()
        }
    }

    /// Iterator for testing reading empty columns
    struct EmptyPageIterator {
        schema: SchemaDescPtr,
    }

    impl EmptyPageIterator {
        fn new(schema: SchemaDescPtr) -> Self {
            EmptyPageIterator { schema }
        }
    }

    impl Iterator for EmptyPageIterator {
        type Item = Result<Box<dyn PageReader>>;

        fn next(&mut self) -> Option<Self::Item> {
            None
        }
    }

    impl PageIterator for EmptyPageIterator {
        fn schema(&mut self) -> Result<SchemaDescPtr> {
            Ok(self.schema.clone())
        }

        fn column_schema(&mut self) -> Result<ColumnDescPtr> {
            Ok(self.schema.column(0))
        }
    }

    #[test]
    fn test_struct_array_reader() {
        let array_1 = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![1, 2, 3, 4, 5]));
        let array_reader_1 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_1.clone(),
            Some(vec![0, 1, 2, 3, 1]),
            Some(vec![1, 1, 1, 1, 1]),
        );

        let array_2 = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![5, 4, 3, 2, 1]));
        let array_reader_2 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_2.clone(),
            Some(vec![0, 1, 3, 1, 2]),
            Some(vec![1, 1, 1, 1, 1]),
        );

        let struct_type = ArrowType::Struct(vec![
            Field::new("f1", array_1.data_type().clone(), true),
            Field::new("f2", array_2.data_type().clone(), true),
        ]);

        let mut struct_array_reader = StructArrayReader::new(
            struct_type,
            vec![Box::new(array_reader_1), Box::new(array_reader_2)],
            1,
            1,
        );

        let struct_array = struct_array_reader.next_batch(5).unwrap();
        let struct_array = struct_array.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(5, struct_array.len());
        assert_eq!(
            vec![true, false, false, false, false],
            (0..5)
                .map(|idx| struct_array.data_ref().is_null(idx))
                .collect::<Vec<bool>>()
        );
        assert_eq!(
            Some(vec![0, 1, 1, 1, 1].as_slice()),
            struct_array_reader.get_def_levels()
        );
        assert_eq!(
            Some(vec![1, 1, 1, 1, 1].as_slice()),
            struct_array_reader.get_rep_levels()
        );
    }

    #[test]
    fn test_list_array_reader() {
        // [[1, null, 2], null, [3, 4]]
        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
        ]));
        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![3, 2, 3, 0, 3, 3]),
            Some(vec![0, 1, 1, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<i32>::new(
            Box::new(item_array_reader),
            ArrowType::List(Box::new(Field::new("item", ArrowType::Int32, true))),
            ArrowType::Int32,
            1,
            1,
            0,
            1,
        );

        let next_batch = list_array_reader.next_batch(1024).unwrap();
        let list_array = next_batch.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(3, list_array.len());
        // This passes as I expect
        assert_eq!(1, list_array.null_count());

        assert_eq!(
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(1), None, Some(2)])
        );

        assert!(list_array.is_null(1));

        assert_eq!(
            list_array
                .value(2)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(3), Some(4)])
        );
    }

    #[test]
    fn test_large_list_array_reader() {
        // [[1, null, 2], null, [3, 4]]
        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
        ]));
        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![3, 2, 3, 0, 3, 3]),
            Some(vec![0, 1, 1, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<i64>::new(
            Box::new(item_array_reader),
            ArrowType::LargeList(Box::new(Field::new("item", ArrowType::Int32, true))),
            ArrowType::Int32,
            1,
            1,
            0,
            1,
        );

        let next_batch = list_array_reader.next_batch(1024).unwrap();
        let list_array = next_batch
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();

        assert_eq!(3, list_array.len());

        assert_eq!(
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(1), None, Some(2)])
        );

        assert!(list_array.is_null(1));

        assert_eq!(
            list_array
                .value(2)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(3), Some(4)])
        );
    }
}
