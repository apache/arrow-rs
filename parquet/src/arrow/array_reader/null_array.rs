use crate::arrow::array_reader::{read_records, ArrayReader};
use crate::arrow::record_reader::buffer::ScalarValue;
use crate::arrow::record_reader::RecordReader;
use crate::column::page::PageIterator;
use crate::data_type::DataType;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use arrow::array::ArrayRef;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

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
        self.def_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }
}
