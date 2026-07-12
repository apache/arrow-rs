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

use crate::coalesce::InProgressArray;
use crate::filter::{
    FilterIndices, FilterPredicate, FilterSelection, FilterSlices, filter_null_mask,
};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{BooleanBuffer, NullBuffer, NullBufferBuilder, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::fmt::Debug;
use std::sync::Arc;

/// InProgressArray for [`PrimitiveArray`]
#[derive(Debug)]
pub(crate) struct InProgressPrimitiveArray<T: ArrowPrimitiveType> {
    /// Data type of the array
    data_type: DataType,
    /// The current source, if any
    source: Option<ArrayRef>,
    /// the target batch size (and thus size for views allocation)
    batch_size: usize,
    /// In progress nulls
    nulls: NullBufferBuilder,
    /// The currently in progress array
    current: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> InProgressPrimitiveArray<T> {
    /// Create a new `InProgressPrimitiveArray`
    pub(crate) fn new(batch_size: usize, data_type: DataType) -> Self {
        Self {
            data_type,
            batch_size,
            source: None,
            nulls: NullBufferBuilder::new(batch_size),
            current: vec![],
        }
    }

    /// Allocate space for output values if necessary.
    ///
    /// This is done on write (when we know it is necessary) rather than
    /// eagerly to avoid allocations that are not used.
    fn ensure_capacity(&mut self) {
        if self.current.capacity() == 0 {
            self.current.reserve(self.batch_size);
        }
    }

    fn append_values_by_indices(
        current: &mut Vec<T::Native>,
        values: &[T::Native],
        indices: FilterIndices<'_>,
        selected_count: usize,
    ) {
        let current_len = current.len();
        let mut written = 0;

        unsafe {
            let mut out = current
                .spare_capacity_mut()
                .as_mut_ptr()
                .cast::<T::Native>();

            indices.for_each(|idx| {
                // SAFETY: indices are derived from the filter predicate for this source.
                out.write(*values.get_unchecked(idx));
                out = out.add(1);
                written += 1;
            });

            current.set_len(current_len + written);
        }

        debug_assert_eq!(written, selected_count);
    }

    fn append_values_by_slices(
        current: &mut Vec<T::Native>,
        values: &[T::Native],
        slices: FilterSlices<'_>,
        selected_count: usize,
    ) {
        let current_len = current.len();
        let mut written = 0;

        unsafe {
            let mut out = current
                .spare_capacity_mut()
                .as_mut_ptr()
                .cast::<T::Native>();

            slices.for_each(|(start, end)| {
                let len = end - start;
                // SAFETY: slices are derived from the filter predicate for this source.
                std::ptr::copy_nonoverlapping(values.as_ptr().add(start), out, len);
                out = out.add(len);
                written += len;
            });

            current.set_len(current_len + written);
        }

        debug_assert_eq!(written, selected_count);
    }
}

#[inline]
fn primitive_source<T: ArrowPrimitiveType>(
    source: &Option<ArrayRef>,
) -> Result<&PrimitiveArray<T>, ArrowError> {
    Ok(source
        .as_ref()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressPrimitiveArray: source not set".to_string(),
            )
        })?
        .as_primitive::<T>())
}

fn append_filtered_nulls(
    nulls: &mut NullBufferBuilder,
    source_nulls: Option<&NullBuffer>,
    filter: &FilterPredicate,
) {
    if let Some((null_count, filtered_nulls)) = filter_null_mask(source_nulls, filter) {
        let filtered_nulls = unsafe {
            NullBuffer::new_unchecked(
                BooleanBuffer::new(filtered_nulls, 0, filter.count()),
                null_count,
            )
        };
        nulls.append_buffer(&filtered_nulls);
    } else {
        nulls.append_n_non_nulls(filter.count());
    }
}

impl<T: ArrowPrimitiveType + Debug> InProgressArray for InProgressPrimitiveArray<T> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.ensure_capacity();

        let s = primitive_source::<T>(&self.source)?;

        // add nulls if necessary
        if let Some(nulls) = s.nulls().as_ref() {
            let nulls = nulls.slice(offset, len);
            self.nulls.append_buffer(&nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        };

        // Copy the values
        let values = s.values();
        // SAFETY: copy_rows is called with ranges derived from the source array.
        self.current
            .extend_from_slice(unsafe { values.get_unchecked(offset..offset + len) });

        Ok(())
    }

    fn copy_rows_by_filter(&mut self, filter: &FilterPredicate) -> Result<(), ArrowError> {
        match filter.selection() {
            FilterSelection::Indices(indices) => {
                self.ensure_capacity();
                let s = primitive_source::<T>(&self.source)?;

                append_filtered_nulls(&mut self.nulls, s.nulls(), filter);
                self.current.reserve(filter.count());
                Self::append_values_by_indices(
                    &mut self.current,
                    s.values(),
                    indices,
                    filter.count(),
                );
                Ok(())
            }
            FilterSelection::Slices(slices) => {
                self.ensure_capacity();
                let s = primitive_source::<T>(&self.source)?;

                append_filtered_nulls(&mut self.nulls, s.nulls(), filter);
                self.current.reserve(filter.count());
                Self::append_values_by_slices(
                    &mut self.current,
                    s.values(),
                    slices,
                    filter.count(),
                );
                Ok(())
            }
            // Other selection shapes reuse the generic copy_rows path.
            selection => self.copy_rows_by_selection(selection),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // take and reset the current values and nulls
        let values = std::mem::take(&mut self.current);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);

        let array = PrimitiveArray::<T>::try_new(ScalarBuffer::from(values), nulls)?
            // preserve timezone / precision+scale if applicable
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(array))
    }

    fn size(&self) -> usize {
        self.source.as_ref().map_or(0, |source| source.get_array_memory_size()) +
          self.current.capacity() * std::mem::size_of::<T>() +
          self.nulls.allocated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::types::Int32Type;
    use arrow_array::{BooleanArray, Int32Array};

    #[test]
    fn test_copy_rows_by_filter_index_iterator() {
        let source =
            Int32Array::from_iter((0..21).map(|idx| if idx % 5 == 0 { None } else { Some(idx) }));
        let filter = BooleanArray::from_iter(
            (0..21).map(|idx| Some(matches!(idx, 0 | 1 | 2 | 3 | 5 | 8 | 13))),
        );
        let predicate = FilterBuilder::new(&filter).build();
        let FilterSelection::Indices(indices) = predicate.selection() else {
            panic!("expected index iterator selection");
        };
        let mut selected_indices = Vec::new();
        indices.for_each(|idx| selected_indices.push(idx));
        assert_eq!(selected_indices, vec![0, 1, 2, 3, 5, 8, 13]);

        let mut in_progress = InProgressPrimitiveArray::<Int32Type>::new(7, DataType::Int32);
        in_progress.set_source(Some(Arc::new(source)));
        in_progress.copy_rows_by_filter(&predicate).unwrap();

        let result = in_progress.finish().unwrap();
        let result = result.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![
            None,
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(8),
            Some(13),
        ]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_copy_rows_by_filter_slice_iterator() {
        let source =
            Int32Array::from_iter((0..16).map(|idx| if idx % 5 == 0 { None } else { Some(idx) }));
        let filter = BooleanArray::from_iter((0..16).map(|idx| Some(!matches!(idx, 3 | 9))));
        let predicate = FilterBuilder::new(&filter).build();
        let FilterSelection::Slices(slices) = predicate.selection() else {
            panic!("expected slice iterator selection");
        };
        let mut selected_slices = Vec::new();
        slices.for_each(|slice| selected_slices.push(slice));
        assert_eq!(selected_slices, vec![(0, 3), (4, 9), (10, 16)]);

        let mut in_progress = InProgressPrimitiveArray::<Int32Type>::new(14, DataType::Int32);
        in_progress.set_source(Some(Arc::new(source)));
        in_progress.copy_rows_by_filter(&predicate).unwrap();

        let result = in_progress.finish().unwrap();
        let result = result.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![
            None,
            Some(1),
            Some(2),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            None,
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            None,
        ]);
        assert_eq!(result, &expected);
    }
}
