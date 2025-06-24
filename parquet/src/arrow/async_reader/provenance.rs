use std::ops::Range;
use std::sync::Arc;
use arrow_array::{ArrayRef, UInt32Array, UInt64Array, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use crate::arrow::arrow_reader::ReadPlan;

/// Wraps an existing `ParquetRecordBatchReader`, appending three
/// provenance columns to every `RecordBatch`.
///
/// * `__file_id`        – constant per file / partition
/// * __row_group_idx`   – constant per row group
/// * `__row_idx`        – absolute row number inside the file
pub struct ProvenanceReader<R> {
    inner: R,
    file_id: u32,
    row_group_idx: u32,
    // pre-computed absolute row numbers that survive all pruning
    row_numbers: Vec<u64>,
    cursor: u32, // how many rows have been consumed
}

impl<R> ProvenanceReader<R> {
    pub fn new(inner: R,
               plan: ReadPlan,
               file_id: u32,
               row_group_idx: u32,
               file_row_base: u64) -> Self {
        // Flatten `RowSelection` into absolute row numbers
        let row_numbers = match plan.selection() {
            // if None, keep every row
            None => (0..plan.batch_size())
                .map(|i| file_row_base + i as u64)
                .collect(),
            Some(sel) => sel.iter()
                .scan(file_row_base, |pos, seg| {
                    let start = *pos;
                    *pos += seg.row_count as u64;
                    (!seg.skip).then(|| start..*pos)
                })
                .flat_map(Range::into_iter)
                .collect(),
        };
        Self { inner, file_id, row_group_idx,
            row_numbers, cursor: 0 }
    }
}

impl<R> Iterator for ProvenanceReader<R>
where
    R: Iterator<Item = Result<RecordBatch, ArrowError>>,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut batch = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };

        let rows = batch.num_rows();
        let end  = self.cursor + rows as u32;

        // slice the pre-computed numbers for this batch
        let slice: &[u64] = &self.row_numbers[self.cursor as usize .. end as usize];
        self.cursor = end;

        // build arrays
        let row_idx_arr: ArrayRef = Arc::new(UInt64Array::from(slice.to_vec()));
        let rg_idx_arr: ArrayRef = Arc::new(UInt32Array::from(vec![self.row_group_idx; rows]));
        let file_id_arr: ArrayRef = Arc::new(UInt32Array ::from(vec![self.file_id; rows]));

        // splice into batch
        let mut cols = batch.columns().to_vec();
        cols.extend([file_id_arr, row_idx_arr, rg_idx_arr]);

        let mut fields = batch.schema().fields().clone();
        fields.to_vec().extend([
            Arc::new(Field::new("__file_id", DataType::Int32,  false)),
            Arc::new(Field::new("__row_idx", DataType::UInt64, false)),
            Arc::new(Field::new("__row_group_idx", DataType::UInt32, false)),
        ]);
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        match RecordBatch::try_new(schema, cols) {
            Ok(new_batch) => Some(Ok(new_batch)),
            Err(e)        => Some(Err(ParquetError::ArrowError(e.to_string()))),
        }
    }
}
