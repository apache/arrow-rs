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

use std::{collections::VecDeque, sync::Arc};

use arrow_array::{cast::AsArray, Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::filter::prep_null_mask_filter;

use crate::arrow::{
    array_reader::{build_array_reader, ArrayReader, RowGroups, StructArrayReader},
    arrow_reader::{ArrowPredicate, RowFilter, RowSelection, RowSelector},
};
use crate::errors::ParquetError;

use super::ParquetField;

pub struct FilteredParquetRecordBatchReader {
    batch_size: usize,
    array_reader: StructArrayReader,
    predicate_readers: Vec<Box<dyn ArrayReader>>,
    schema: SchemaRef,
    selection: VecDeque<RowSelector>,
    row_filter: Option<RowFilter>,
}

fn read_selection(
    reader: &mut dyn ArrayReader,
    selection: &RowSelection,
) -> Result<StructArray, ParquetError> {
    for selector in selection.iter() {
        if selector.skip {
            let skipped = reader.skip_records(selector.row_count)?;
            debug_assert_eq!(skipped, selector.row_count, "failed to skip rows");
        } else {
            let read_records = reader.read_records(selector.row_count)?;
            debug_assert_eq!(read_records, selector.row_count, "failed to read rows");
        }
    }
    let array = reader.consume_batch()?;
    let struct_array = array
        .as_struct_opt()
        .ok_or_else(|| general_err!("Struct array reader should return struct array"))?;
    Ok(struct_array.clone())
}

/// Take the next selection from the selection queue, and return the selection
/// whose selected row count is to_select or less (if input selection is exhausted).
fn take_next_selection(
    selection: &mut VecDeque<RowSelector>,
    to_select: usize,
) -> Option<RowSelection> {
    let mut current_selected = 0;
    let mut rt = Vec::new();
    while let Some(front) = selection.pop_front() {
        if front.skip {
            rt.push(front);
            continue;
        }

        if current_selected + front.row_count <= to_select {
            rt.push(front);
            current_selected += front.row_count;
        } else {
            let select = to_select - current_selected;
            let remaining = front.row_count - select;
            rt.push(RowSelector::select(select));
            selection.push_front(RowSelector::select(remaining));

            return Some(rt.into());
        }
    }
    if !rt.is_empty() {
        return Some(rt.into());
    }
    None
}

fn build_array_reader_for_filters(
    filters: &RowFilter,
    fields: &Option<Arc<ParquetField>>,
    row_group: &dyn RowGroups,
) -> Result<Vec<Box<dyn ArrayReader>>, ArrowError> {
    let mut array_readers = Vec::new();
    for predicate in filters.predicates.iter() {
        let predicate_projection = predicate.projection();
        let array_reader = build_array_reader(fields.as_deref(), predicate_projection, row_group)?;
        array_readers.push(array_reader);
    }
    Ok(array_readers)
}

impl FilteredParquetRecordBatchReader {
    fn new(batch_size: usize, array_reader: StructArrayReader, selection: RowSelection) -> Self {
        todo!()
    }

    fn build_predicate_filter(
        &mut self,
        selection: &RowSelection,
    ) -> Result<RowSelection, ArrowError> {
        match &mut self.row_filter {
            None => Ok(selection.clone()),
            Some(filter) => {
                debug_assert_eq!(
                    self.predicate_readers.len(),
                    filter.predicates.len(),
                    "predicate readers and predicates should have the same length"
                );
                let mut selection = selection.clone();

                for (predicate, reader) in filter
                    .predicates
                    .iter_mut()
                    .zip(self.predicate_readers.iter_mut())
                {
                    let array = read_selection(reader.as_mut(), &selection)?;
                    let batch = RecordBatch::from(array);
                    let input_rows = batch.num_rows();
                    let predicate_filter = predicate.evaluate(batch)?;
                    if predicate_filter.len() != input_rows {
                        return Err(ArrowError::ParquetError(format!(
                            "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                            predicate_filter.len()
                        )));
                    }
                    let predicate_filter = match predicate_filter.null_count() {
                        0 => predicate_filter,
                        _ => prep_null_mask_filter(&predicate_filter),
                    };
                    let raw = RowSelection::from_filters(&[predicate_filter]);
                    selection = selection.and_then(&raw);
                }
                Ok(selection)
            }
        }
    }
}

impl Iterator for FilteredParquetRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let selection = take_next_selection(&mut self.selection, self.batch_size)?;
        let filtered_selection = match self.build_predicate_filter(&selection) {
            Ok(selection) => selection,
            Err(e) => return Some(Err(e)),
        };

        let rt = read_selection(&mut self.array_reader, &filtered_selection);
        match rt {
            Ok(array) => Some(Ok(RecordBatch::from(array))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl RecordBatchReader for FilteredParquetRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_next_selection_exact_match() {
        let mut queue = VecDeque::from(vec![
            RowSelector::skip(5),
            RowSelector::select(3),
            RowSelector::skip(2),
            RowSelector::select(7),
        ]);

        // Request exactly 10 rows (5 skip + 3 select + 2 skip)
        let selection = take_next_selection(&mut queue, 3).unwrap();
        assert_eq!(
            selection,
            vec![
                RowSelector::skip(5),
                RowSelector::select(3),
                RowSelector::skip(2)
            ]
            .into()
        );

        // Check remaining queue
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].row_count, 7);
        assert_eq!(queue[0].skip, false);
    }

    #[test]
    fn test_take_next_selection_split_required() {
        let mut queue = VecDeque::from(vec![RowSelector::select(10), RowSelector::select(10)]);

        // Request 15 rows, which should split the first selector
        let selection = take_next_selection(&mut queue, 15).unwrap();

        assert_eq!(
            selection,
            vec![RowSelector::select(10), RowSelector::select(5)].into()
        );

        // Check remaining queue - should have 5 rows from split and original 10
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].skip, false);
        assert_eq!(queue[0].row_count, 5);
    }

    #[test]
    fn test_take_next_selection_empty_queue() {
        let mut queue = VecDeque::new();

        // Should return None for empty queue
        let selection = take_next_selection(&mut queue, 10);
        assert!(selection.is_none());

        // Test with queue that becomes empty
        queue.push_back(RowSelector::select(5));
        let selection = take_next_selection(&mut queue, 10).unwrap();
        assert_eq!(selection, vec![RowSelector::select(5)].into());

        // Queue should now be empty
        let selection = take_next_selection(&mut queue, 10);
        assert!(selection.is_none());
    }
}
