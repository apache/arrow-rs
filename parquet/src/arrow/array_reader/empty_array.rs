use crate::arrow::array_reader::ArrayReader;
use crate::errors::Result;
use arrow_array::{ArrayRef, StructArray};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType as ArrowType, Fields};
use std::any::Any;
use std::sync::Arc;

/// Returns an [`ArrayReader`] that yields [`StructArray`] with no columns
/// but with row counts that correspond to the amount of data in the file
///
/// This is useful for when projection eliminates all columns within a collection
pub fn make_empty_array_reader(row_count: usize) -> Box<dyn ArrayReader> {
    Box::new(EmptyArrayReader::new(row_count))
}

struct EmptyArrayReader {
    data_type: ArrowType,
    remaining_rows: usize,
    need_consume_records: usize,
}

impl EmptyArrayReader {
    pub fn new(row_count: usize) -> Self {
        Self {
            data_type: ArrowType::Struct(Fields::empty()),
            remaining_rows: row_count,
            need_consume_records: 0,
        }
    }
}

impl ArrayReader for EmptyArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let len = self.remaining_rows.min(batch_size);
        self.remaining_rows -= len;
        self.need_consume_records += len;
        Ok(len)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(self.need_consume_records)
            .build()
            .unwrap();
        self.need_consume_records = 0;
        Ok(Arc::new(StructArray::from(data)))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let skipped = self.remaining_rows.min(num_records);
        self.remaining_rows -= skipped;
        Ok(skipped)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}
