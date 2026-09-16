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

use std::mem::size_of;
use std::sync::Arc;

use super::InProgressArray;
use crate::concat::concat;
use crate::filter::FilterPredicate;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, new_empty_array};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{ArrowError, FieldRef};

/// Specialized [`InProgressArray`] for [`FixedSizeListArray`].
///
/// `FixedSizeList(T, N)` has no offset buffer — just an optional null bitmap
/// and a values child array of length `rows * N`. Instead of routing through
/// [`super::GenericInProgressArray`] → `concat_fallback` → `MutableArrayData`
/// (which also recurses the child through `MutableArrayData`), this
/// implementation:
///
/// 1. Builds the outer null bitmap incrementally via [`NullBufferBuilder`].
/// 2. Buffers child-value slices (one `ArrayRef` per [`InProgressArray::copy_rows`] call).
/// 3. At [`InProgressArray::finish`] time, calls [`concat`] on the child slices,
///    which dispatches to the type-optimized path (e.g. `concat_primitives` for
///    a `Float32` child).
#[derive(Debug)]
pub(crate) struct InProgressFixedSizeListArray {
    /// The field that describes the child values of this list.
    field: FieldRef,
    /// The fixed number of child elements per list entry.
    list_size: usize,
    /// Target output batch size (used to pre-size the null builder).
    batch_size: usize,
    /// The current source array, if any.
    source: Option<ArrayRef>,
    /// Whether [`Self::source`] is referenced by any entry in [`Self::buffered_child_values`].
    source_referenced_in_buffers: bool,
    /// Incremental null bitmap for the outer (list-level) nulls.
    nulls: NullBufferBuilder,
    /// Buffered child-value slices.  One entry per [`InProgressArray::copy_rows`] call.
    buffered_child_values: Vec<ArrayRef>,
    /// Accumulated size of arrays in [`Self::buffered_child_values`] whose
    /// backing memory is *not* shared with [`Self::source`] (i.e., produced by
    /// the filter path).
    total_non_shared_size: usize,
    /// Total number of list rows accumulated so far.
    row_count: usize,
}

impl InProgressFixedSizeListArray {
    pub(crate) fn new(field: FieldRef, list_size: i32, batch_size: usize) -> Self {
        Self {
            field,
            list_size: list_size as usize,
            batch_size,
            source: None,
            source_referenced_in_buffers: false,
            nulls: NullBufferBuilder::new(batch_size),
            buffered_child_values: Vec::new(),
            total_non_shared_size: 0,
            row_count: 0,
        }
    }
}

impl InProgressArray for InProgressFixedSizeListArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        if let Some(old) = self.source.take() {
            // Once the source is replaced, any child slice that was derived from
            // it can no longer be considered shared — count it now so that
            // `size()` stays accurate even after the source is dropped.
            if self.source_referenced_in_buffers {
                self.total_non_shared_size += old.get_array_memory_size();
            }
        }
        self.source_referenced_in_buffers = false;
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressFixedSizeListArray: source not set".to_string(),
            )
        })?;

        let fsl = source
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "InProgressFixedSizeListArray: source is not a FixedSizeListArray".to_string(),
                )
            })?;

        let n = self.list_size;

        // Append outer-level nulls.
        // NullBuffer::slice returns the sub-range [offset, offset+len).
        if let Some(nulls) = fsl.nulls() {
            let sliced = nulls.slice(offset, len);
            self.nulls.append_buffer(&sliced);
        } else {
            self.nulls.append_n_non_nulls(len);
        }

        // FixedSizeListArray::values() already starts at offset 0 within the
        // underlying child buffer (the struct slices the child in its own
        // slice() implementation).  So the physical values for list rows
        // [offset, offset+len) start at offset*n in the child.
        let child_start = offset * n;
        let child_len = len * n;
        let child_slice = fsl.values().slice(child_start, child_len);
        self.buffered_child_values.push(child_slice);

        // Mark that the current source is referenced by a buffered child slice
        // so that set_source() can account for the memory correctly.
        self.source_referenced_in_buffers = true;
        self.row_count += len;

        Ok(())
    }

    /// Override the filter-from path: apply the filter kernel once (which
    /// correctly handles both outer nulls and child values) and then buffer
    /// the result rather than going through [`Self::copy_rows`] one row at a
    /// time.
    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let filtered = filter.filter(source.as_ref())?;
        let len = filtered.len();
        if len == 0 {
            return Ok(());
        }

        let fsl = filtered
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "InProgressFixedSizeListArray: filtered result is not a FixedSizeListArray"
                        .to_string(),
                )
            })?;

        // The filter kernel produces a fresh array with no internal offset,
        // so values()[0..len*N] is exactly the data we want.
        if let Some(nulls) = fsl.nulls() {
            self.nulls.append_buffer(nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        }

        let n = self.list_size;
        let child_slice = fsl.values().slice(0, len * n);
        self.buffered_child_values.push(child_slice);

        // The filtered array is freshly allocated, so its memory is not shared
        // with any source — count it immediately.
        self.total_non_shared_size += filtered.get_array_memory_size();
        self.row_count += len;

        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let nulls = self.nulls.finish();

        // Concatenate all buffered child slices using the type-optimized path.
        let values = match self.buffered_child_values.len() {
            0 => new_empty_array(self.field.data_type()),
            1 => self.buffered_child_values.pop().unwrap(),
            _ => {
                let refs: Vec<&dyn Array> = self
                    .buffered_child_values
                    .iter()
                    .map(|a| a.as_ref())
                    .collect();
                concat(&refs)?
            }
        };

        let array = FixedSizeListArray::try_new(
            Arc::clone(&self.field),
            self.list_size as i32,
            values,
            nulls,
        )?;

        // Reset state for the next batch.
        self.buffered_child_values.clear();
        self.nulls = NullBufferBuilder::new(self.batch_size);
        self.total_non_shared_size = 0;
        self.source_referenced_in_buffers = false;
        self.row_count = 0;

        Ok(Arc::new(array))
    }

    fn size(&self) -> usize {
        self.total_non_shared_size
            + self.buffered_child_values.capacity() * size_of::<ArrayRef>()
            + self
                .source
                .as_ref()
                .map_or(0, |a| a.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::{BooleanArray, Float32Array, Int32Array};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    /// Build a `FieldRef` for `FixedSizeList<Int32>(size)`.
    fn int32_field(size: i32) -> (FieldRef, i32) {
        let field = Arc::new(Field::new(
            "item",
            DataType::Int32,
            true, // nullable items
        ));
        (field, size)
    }

    fn fsl_int32(rows: &[Option<Vec<Option<i32>>>], size: i32) -> FixedSizeListArray {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let mut flat: Vec<Option<i32>> = Vec::new();
        let mut outer_nulls: Vec<bool> = Vec::new();
        for row in rows {
            match row {
                None => {
                    outer_nulls.push(false);
                    for _ in 0..size {
                        flat.push(None);
                    }
                }
                Some(items) => {
                    outer_nulls.push(true);
                    for v in items {
                        flat.push(*v);
                    }
                }
            }
        }
        let values = Arc::new(Int32Array::from(flat)) as ArrayRef;
        let null_buf = arrow_buffer::NullBuffer::from(outer_nulls);
        FixedSizeListArray::try_new(field, size, values, Some(null_buf)).unwrap()
    }

    #[test]
    fn test_basic_roundtrip() {
        let (field, size) = int32_field(3);
        let batch_size = 8;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        // Source: [[1,2,3], [4,5,6], [7,8,9]]
        let src: ArrayRef = Arc::new(fsl_int32(
            &[
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5), Some(6)]),
                Some(vec![Some(7), Some(8), Some(9)]),
            ],
            size,
        ));
        ip.set_source(Some(src));
        ip.copy_rows(0, 3).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(result_fsl.len(), 3);
        assert_eq!(result_fsl.null_count(), 0);

        // Check child values
        let child = result_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(child.values().as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_partial_copy_with_offset() {
        let (field, size) = int32_field(2);
        let batch_size = 8;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        // Source: [[10,20], [30,40], [50,60]]
        let src: ArrayRef = Arc::new(fsl_int32(
            &[
                Some(vec![Some(10), Some(20)]),
                Some(vec![Some(30), Some(40)]),
                Some(vec![Some(50), Some(60)]),
            ],
            size,
        ));
        ip.set_source(Some(src));
        // Copy only rows 1..2 (i.e., [30,40])
        ip.copy_rows(1, 2).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(result_fsl.len(), 2);

        let child = result_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(child.values().as_ref(), &[30, 40, 50, 60]);
    }

    #[test]
    fn test_null_handling() {
        let (field, size) = int32_field(2);
        let batch_size = 8;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        // Source: [[1,2], null, [5,6]]
        let src: ArrayRef = Arc::new(fsl_int32(
            &[
                Some(vec![Some(1), Some(2)]),
                None,
                Some(vec![Some(5), Some(6)]),
            ],
            size,
        ));
        ip.set_source(Some(src));
        ip.copy_rows(0, 3).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(result_fsl.len(), 3);
        assert_eq!(result_fsl.null_count(), 1);
        assert!(result_fsl.is_valid(0));
        assert!(result_fsl.is_null(1));
        assert!(result_fsl.is_valid(2));
    }

    #[test]
    fn test_multiple_copy_rows_accumulate() {
        let (field, size) = int32_field(2);
        let batch_size = 10;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        // First source: [[1,2], [3,4]]
        let src1: ArrayRef = Arc::new(fsl_int32(
            &[Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])],
            size,
        ));
        ip.set_source(Some(Arc::clone(&src1)));
        ip.copy_rows(0, 2).unwrap();

        // Second source: [[5,6], [7,8]]
        let src2: ArrayRef = Arc::new(fsl_int32(
            &[Some(vec![Some(5), Some(6)]), Some(vec![Some(7), Some(8)])],
            size,
        ));
        ip.set_source(Some(src2));
        ip.copy_rows(0, 2).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(result_fsl.len(), 4);

        let child = result_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(child.values().as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_filter_path() {
        let (field, size) = int32_field(2);
        let batch_size = 10;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        // Source: [[1,2], [3,4], [5,6], [7,8]]
        let src: ArrayRef = Arc::new(fsl_int32(
            &[
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3), Some(4)]),
                Some(vec![Some(5), Some(6)]),
                Some(vec![Some(7), Some(8)]),
            ],
            size,
        ));

        // Filter: keep rows 0 and 2 ([1,2] and [5,6])
        let filter = BooleanArray::from(vec![true, false, true, false]);
        let predicate = FilterBuilder::new(&filter).build();

        ip.copy_rows_by_filter_from(src, &predicate).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(result_fsl.len(), 2);

        let child = result_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(child.values().as_ref(), &[1, 2, 5, 6]);
    }

    #[test]
    fn test_finish_resets_state() {
        let (field, size) = int32_field(2);
        let batch_size = 8;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        let src: ArrayRef = Arc::new(fsl_int32(
            &[Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])],
            size,
        ));
        ip.set_source(Some(src));
        ip.copy_rows(0, 2).unwrap();
        ip.finish().unwrap(); // consumes the batch

        // After finish, the in-progress array should be empty.
        let result = ip.finish().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_float32_child_roundtrip() {
        // Simulates the common embedding-vector case
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let size = 4i32;
        let batch_size = 1024;
        let mut ip = InProgressFixedSizeListArray::new(Arc::clone(&field), size, batch_size);

        let values = Arc::new(Float32Array::from(vec![
            1.0f32, 2.0, 3.0, 4.0, // row 0
            5.0, 6.0, 7.0, 8.0, // row 1
        ])) as ArrayRef;
        let src: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(Arc::clone(&field), size, values, None).unwrap());

        ip.set_source(Some(src));
        ip.copy_rows(0, 2).unwrap();

        let result = ip.finish().unwrap();
        let result_fsl = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(result_fsl.len(), 2);
        assert_eq!(result_fsl.null_count(), 0);

        let child = result_fsl
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.len(), 8);
    }
}
