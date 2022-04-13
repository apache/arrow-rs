use crate::arrow::array_reader::ArrayReader;
use crate::errors::Result;
use arrow::array::{ArrayDataBuilder, ArrayRef, StructArray};
use arrow::datatypes::DataType as ArrowType;
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
}

impl EmptyArrayReader {
    pub fn new(row_count: usize) -> Self {
        Self {
            data_type: ArrowType::Struct(vec![]),
            remaining_rows: row_count,
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

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let len = self.remaining_rows.min(batch_size);
        self.remaining_rows -= len;

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(len)
            .build()
            .unwrap();

        Ok(Arc::new(StructArray::from(data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}
