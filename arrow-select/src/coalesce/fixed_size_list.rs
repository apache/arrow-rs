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

use super::InProgressArray;
use crate::concat::concat;
use crate::filter::FilterPredicate;
use arrow_array::cast::AsArray;
use arrow_array::{new_empty_array, Array, ArrayRef, FixedSizeListArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::{ArrowError, Field};
use std::sync::Arc;

/// Specialized [`InProgressArray`] for [`FixedSizeListArray`].
///
/// Stores Arc-cloned outer FSL slices per `copy_rows` call and defers all
/// child value extraction and null-buffer construction to `finish()`.
///
/// This keeps `copy_rows` as cheap as `GenericInProgressArray::copy_rows`
/// (one Arc clone + one Vec push) while still avoiding the 2× peak memory
/// overhead from `GenericInProgressArray`'s use of `concat` over full arrays.
///
/// Null handling defers all bitmap work to `finish()`.  Only a single
/// `has_nulls: bool` flag is kept hot, avoiding any heap allocation until
/// a null-bearing batch is finalized.
#[derive(Debug)]
pub(crate) struct InProgressFixedSizeListArray {
    source: Option<ArrayRef>,    // 16B — Option<Arc<dyn Array>> via niche optimization
    value_slices: Vec<ArrayRef>, // 24B — outer FSL slices; child values extracted at finish()
    rows: usize,                 // 8B
    list_size: i32,              // 4B
    has_nulls: bool,             // 1B
    // 3B padding
    field: Arc<Field>,           // 8B
    // Total: ~64B = 1 cache line
}

impl InProgressFixedSizeListArray {
    pub(crate) fn new(list_size: i32, field: Arc<Field>, _batch_size: usize) -> Self {
        Self {
            source: None,
            value_slices: Vec::new(),
            rows: 0,
            list_size,
            has_nulls: false,
            field,
        }
    }
}

impl InProgressArray for InProgressFixedSizeListArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressFixedSizeListArray: source not set".to_string(),
            )
        })?;
        if !self.has_nulls && source.as_fixed_size_list().null_count() > 0 {
            self.has_nulls = true;
        }
        self.rows += len;
        self.value_slices.push(source.slice(offset, len));
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let filtered = filter.filter(source.as_ref())?;
        if filtered.as_fixed_size_list().null_count() > 0 {
            self.has_nulls = true;
        }
        self.rows += filtered.len();
        self.value_slices.push(filtered);
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let rows = std::mem::take(&mut self.rows);

        if rows == 0 {
            self.has_nulls = false;
            self.value_slices.clear();
            return Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(&self.field),
                self.list_size,
                new_empty_array(self.field.data_type()),
                None,
            )));
        }

        let list_size = self.list_size as usize;

        // Build null buffer only if any null was seen.  BooleanBuffer::slice is
        // O(1), so iterating value_slices here is cheap (no bit-counting until
        // BooleanBufferBuilder::finish).
        let nulls = if self.has_nulls {
            self.has_nulls = false;
            let mut builder = BooleanBufferBuilder::new(rows);
            let mut null_count = 0usize;
            for slice in &self.value_slices {
                let fsl = slice.as_fixed_size_list();
                if let Some(src_nulls) = fsl.nulls() {
                    null_count += fsl.null_count();
                    builder.append_buffer(&src_nulls.inner().slice(fsl.offset(), fsl.len()));
                } else {
                    builder.append_n(fsl.len(), true);
                }
            }
            // SAFETY: null_count was accumulated from exact per-source null counts
            // (fsl.null_count() is always exact for these slices).
            Some(unsafe { NullBuffer::new_unchecked(builder.finish(), null_count) })
        } else {
            None
        };

        // Extract child values from each outer FSL slice and concatenate.
        let values = if self.value_slices.len() == 1 {
            let fsl = self.value_slices[0].as_fixed_size_list();
            let child = fsl
                .values()
                .slice(fsl.offset() * list_size, fsl.len() * list_size);
            self.value_slices.clear();
            child
        } else {
            let child_refs: Vec<ArrayRef> = self
                .value_slices
                .iter()
                .map(|slice| {
                    let fsl = slice.as_fixed_size_list();
                    fsl.values()
                        .slice(fsl.offset() * list_size, fsl.len() * list_size)
                })
                .collect();
            self.value_slices.clear();
            let refs: Vec<&dyn Array> = child_refs.iter().map(|a| a.as_ref()).collect();
            concat(&refs)?
        };

        Ok(Arc::new(FixedSizeListArray::new(
            Arc::clone(&self.field),
            self.list_size,
            values,
            nulls,
        )))
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |s| s.get_array_memory_size())
            + self.value_slices.capacity() * std::mem::size_of::<ArrayRef>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::DataType;

    fn make_fsl(list_size: i32, values: &[i32]) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(FixedSizeListArray::new(
            field,
            list_size,
            Arc::new(Int32Array::from(values.to_vec())),
            None,
        ))
    }

    fn make_coalescer(list_size: i32) -> InProgressFixedSizeListArray {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        InProgressFixedSizeListArray::new(list_size, field, 8)
    }

    fn child_values(output: &ArrayRef) -> Vec<i32> {
        output
            .as_fixed_size_list()
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn test_compact_struct_size() {
        use super::super::generic::GenericInProgressArray;
        let fsl_size = std::mem::size_of::<InProgressFixedSizeListArray>();
        let generic_size = std::mem::size_of::<GenericInProgressArray>();
        assert!(
            fsl_size <= generic_size * 2,
            "InProgressFixedSizeListArray ({fsl_size}B) is more than 2× GenericInProgressArray \
             ({generic_size}B); hot-path state may have leaked into the base struct"
        );
    }

    #[test]
    fn test_roundtrip() {
        let input = make_fsl(2, &[1, 2, 3, 4]);
        let mut coalescer = make_coalescer(2);
        coalescer.set_source(Some(Arc::clone(&input)));
        coalescer.copy_rows(0, 2).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(child_values(&output), [1, 2, 3, 4]);
    }

    #[test]
    fn test_offset_copy() {
        let input = make_fsl(2, &[1, 2, 3, 4, 5, 6]);
        let mut coalescer = make_coalescer(2);
        coalescer.set_source(Some(Arc::clone(&input)));
        coalescer.copy_rows(1, 2).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(child_values(&output), [3, 4, 5, 6]);
    }

    #[test]
    fn test_sliced_source() {
        // Sliced source has a non-zero FSL offset; verify child range is correct.
        let base = make_fsl(2, &[1, 2, 3, 4, 5, 6]);
        let sliced = base.slice(1, 2); // logical rows: [3,4] and [5,6]
        let mut coalescer = make_coalescer(2);
        coalescer.set_source(Some(sliced));
        coalescer.copy_rows(0, 2).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(child_values(&output), [3, 4, 5, 6]);
    }

    #[test]
    fn test_nulls() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let nulls = arrow_buffer::NullBuffer::from(vec![true, false, true]);
        let input = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            2,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Some(nulls),
        ));
        let mut coalescer = InProgressFixedSizeListArray::new(2, field, 8);
        coalescer.set_source(Some(input));
        coalescer.copy_rows(0, 3).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 3);
        assert!(output.as_fixed_size_list().is_valid(0));
        assert!(output.as_fixed_size_list().is_null(1));
        assert!(output.as_fixed_size_list().is_valid(2));
    }

    #[test]
    fn test_multi_source_coalesce() {
        let input_a = make_fsl(2, &[1, 2, 3, 4]);
        let input_b = make_fsl(2, &[5, 6, 7, 8]);
        let mut coalescer = make_coalescer(2);
        coalescer.set_source(Some(Arc::clone(&input_a)));
        coalescer.copy_rows(0, 2).unwrap();
        coalescer.set_source(Some(Arc::clone(&input_b)));
        coalescer.copy_rows(0, 2).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 4);
        assert_eq!(child_values(&output), [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_finish_reuse() {
        let input = make_fsl(2, &[1, 2, 3, 4]);
        let mut coalescer = make_coalescer(2);

        coalescer.set_source(Some(Arc::clone(&input)));
        coalescer.copy_rows(0, 2).unwrap();
        let first = coalescer.finish().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(child_values(&first), [1, 2, 3, 4]);

        coalescer.set_source(Some(Arc::clone(&input)));
        coalescer.copy_rows(0, 1).unwrap();
        let second = coalescer.finish().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(child_values(&second), [1, 2]);
    }

    #[test]
    fn test_empty_finish() {
        let mut coalescer = make_coalescer(4);
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_mixed_null_nonnull_sources() {
        // Verify null tracking across null-free → null-bearing → null-free sources.
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let no_null = make_fsl(2, &[1, 2, 3, 4]);
        let with_null: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            2,
            Arc::new(Int32Array::from(vec![5, 6, 7, 8])),
            Some(arrow_buffer::NullBuffer::from(vec![true, false])),
        ));
        let no_null2: ArrayRef = make_fsl(2, &[9, 10]);
        let mut coalescer = InProgressFixedSizeListArray::new(2, Arc::clone(&field), 8);
        coalescer.set_source(Some(Arc::clone(&no_null)));
        coalescer.copy_rows(0, 2).unwrap();
        coalescer.set_source(Some(Arc::clone(&with_null)));
        coalescer.copy_rows(0, 2).unwrap();
        coalescer.set_source(Some(Arc::clone(&no_null2)));
        coalescer.copy_rows(0, 1).unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 5);
        let out_fsl = output.as_fixed_size_list();
        assert!(out_fsl.is_valid(0)); // from no_null
        assert!(out_fsl.is_valid(1)); // from no_null
        assert!(out_fsl.is_valid(2)); // from with_null row 0
        assert!(out_fsl.is_null(3)); // from with_null row 1
        assert!(out_fsl.is_valid(4)); // from no_null2
    }

    #[test]
    fn test_copy_rows_by_filter_from() {
        use crate::filter::{FilterBuilder, FilterPredicate};
        use arrow_array::BooleanArray;

        let input = make_fsl(2, &[1, 2, 3, 4, 5, 6]);
        // Keep rows 0 and 2 (filter = [true, false, true])
        let filter = BooleanArray::from(vec![true, false, true]);
        let predicate: FilterPredicate = FilterBuilder::new(&filter).build();

        let mut coalescer = make_coalescer(2);
        coalescer
            .copy_rows_by_filter_from(Arc::clone(&input), &predicate)
            .unwrap();
        let output = coalescer.finish().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(child_values(&output), [1, 2, 5, 6]);
    }
}
