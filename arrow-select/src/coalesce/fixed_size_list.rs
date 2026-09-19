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
use arrow_array::cast::AsArray;
use arrow_array::{new_empty_array, Array, ArrayRef, FixedSizeListArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use arrow_schema::{ArrowError, Field};
use std::sync::Arc;

/// Specialized [`InProgressArray`] for [`FixedSizeListArray`].
///
/// Buffers Arc-cloned child value slices per `copy_rows` call and concatenates
/// them once at `finish`, avoiding per-row child array reconstruction.
///
/// Null handling uses a two-phase strategy:
/// * When all sources so far have been null-free, only a `non_null_prefix`
///   counter is kept (zero heap overhead).
/// * On the first null-bearing `copy_rows` call, O(1) `BooleanBuffer` slices
///   are stored in `null_slices`; a `BooleanBufferBuilder` is built once at
///   `finish()` with exact capacity.  The running `null_count` is tracked
///   incrementally so `finish()` can use `NullBuffer::new_unchecked`, avoiding
///   a full `count_set_bits` scan over the assembled bitmap.
#[derive(Debug)]
pub(crate) struct InProgressFixedSizeListArray {
    source: Option<ArrayRef>,
    list_size: i32,
    field: Arc<Field>,
    /// All-valid rows accumulated before the first null-bearing source.
    /// When `null_slices` is empty this counter is the only null-tracking
    /// state, keeping the no-null path allocation-free.
    non_null_prefix: usize,
    /// Null-bit slices collected once the first null-bearing source is seen.
    /// `None` means all rows in that slice were valid.
    /// Built on demand; empty when no nulls have been seen yet.
    null_slices: Vec<(Option<BooleanBuffer>, usize)>,
    /// Accumulated FSL-level null count across all `null_slices` entries.
    /// Maintained incrementally so `finish()` avoids a `count_set_bits` scan.
    null_count: usize,
    value_slices: Vec<ArrayRef>,
    rows: usize,
}

impl InProgressFixedSizeListArray {
    pub(crate) fn new(list_size: i32, field: Arc<Field>, _batch_size: usize) -> Self {
        Self {
            source: None,
            list_size,
            field,
            non_null_prefix: 0,
            null_slices: Vec::new(),
            null_count: 0,
            value_slices: Vec::new(),
            rows: 0,
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
        let fsl = source.as_fixed_size_list();
        let list_size = self.list_size as usize;

        // Account for the FSL's own offset into the child values buffer.
        let child_start = (fsl.offset() + offset) * list_size;
        let child_slice = fsl.values().slice(child_start, len * list_size);
        self.value_slices.push(child_slice);

        if let Some(src_nulls) = fsl.nulls() {
            // First null-bearing source: flush the all-valid prefix into
            // null_slices so the BooleanBuffer accounting stays coherent.
            if self.null_slices.is_empty() && self.non_null_prefix > 0 {
                self.null_slices
                    .push((None, std::mem::take(&mut self.non_null_prefix)));
            }
            // BooleanBuffer::slice is O(1) — no bit-counting.
            let bool_buf = src_nulls.inner().slice(fsl.offset() + offset, len);
            // Track null_count incrementally so finish() can skip count_set_bits.
            // For full-source slices (offset 0, full length) we can reuse the
            // already-computed null_count on the FSL.  For partial slices we
            // pay one count_set_bits here rather than a full-bitmap scan later.
            let nc = if fsl.offset() == 0 && offset == 0 && len == fsl.len() {
                fsl.null_count()
            } else {
                len - bool_buf.count_set_bits()
            };
            self.null_count += nc;
            self.null_slices.push((Some(bool_buf), len));
        } else if self.null_slices.is_empty() {
            // Still in the all-valid prefix phase; keep it as a single counter.
            self.non_null_prefix += len;
        } else {
            // Already tracking nulls; record this all-valid slice.
            self.null_slices.push((None, len));
        }
        self.rows += len;
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let rows = std::mem::take(&mut self.rows);
        self.non_null_prefix = 0;

        // Build the null buffer only if at least one source contributed nulls.
        // `null_count` was accumulated incrementally in `copy_rows` so we can
        // use `new_unchecked` and skip the `count_set_bits` scan over `rows` bits.
        let nulls = if !self.null_slices.is_empty() {
            let null_count = std::mem::take(&mut self.null_count);
            let mut builder = BooleanBufferBuilder::new(rows);
            for (null_slice, len) in &self.null_slices {
                match null_slice {
                    Some(buf) => builder.append_buffer(buf),
                    None => builder.append_n(*len, true),
                }
            }
            self.null_slices.clear();
            // SAFETY: null_count was accumulated from per-source null counts,
            // which are exact (fsl.null_count() for full slices, count_set_bits
            // for partial slices).
            Some(unsafe { NullBuffer::new_unchecked(builder.finish(), null_count) })
        } else {
            None
        };

        let values = if rows == 0 {
            new_empty_array(self.field.data_type())
        } else {
            let refs: Vec<&dyn Array> = self.value_slices.iter().map(|s| s.as_ref()).collect();
            concat(&refs)?
        };
        self.value_slices.clear();

        Ok(Arc::new(FixedSizeListArray::new(
            Arc::clone(&self.field),
            self.list_size,
            values,
            nulls,
        )))
    }

    fn size(&self) -> usize {
        self.value_slices
            .iter()
            .map(|slice| slice.get_array_memory_size())
            .sum::<usize>()
            + self
                .source
                .as_ref()
                .map_or(0, |source| source.get_array_memory_size())
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
        // Verify the prefix flush path: non-null source, then null source, then non-null source.
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
}
