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
use crate::filter::{FilterPredicate, FilterSelection};
use arrow_array::builder::PrimitiveRunBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int32Array, RunArray};
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

const SPARSE_FILTER_MAX_SELECTIVITY_DENOMINATOR: usize = 24;

/// Sparse-copy specialization for `RunEndEncoded(Int32, Int32)`.
pub(crate) struct InProgressInt32RunArray {
    source: Option<ArrayRef>,
    data_type: DataType,
    batch_size: usize,
    builder: Option<PrimitiveRunBuilder<Int32Type, Int32Type>>,
    builder_rows: usize,
    buffered_arrays: Vec<ArrayRef>,
    source_data_referenced_in_buffers: bool,
    total_size_of_non_shared_buffers: usize,
}

impl std::fmt::Debug for InProgressInt32RunArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressInt32RunArray")
            .field("batch_size", &self.batch_size)
            .field("builder_rows", &self.builder_rows)
            .field("buffered_arrays", &self.buffered_arrays.len())
            .finish()
    }
}

impl InProgressInt32RunArray {
    pub(crate) fn new(data_type: DataType, batch_size: usize) -> Self {
        Self {
            source: None,
            data_type,
            batch_size,
            builder: None,
            builder_rows: 0,
            buffered_arrays: Vec::new(),
            source_data_referenced_in_buffers: false,
            total_size_of_non_shared_buffers: 0,
        }
    }

    fn flush_builder(&mut self) {
        let Some(mut builder) = self.builder.take() else {
            return;
        };
        let (_, run_ends, values) = builder.finish().into_parts();
        // SAFETY: the builder produced valid matching run ends and values; only
        // the schema fields are restored from the input type.
        let array = Arc::new(unsafe {
            RunArray::<Int32Type>::new_unchecked(self.data_type.clone(), run_ends, values)
        }) as ArrayRef;
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
        self.builder_rows = 0;
    }

    fn append_index(
        builder: &mut PrimitiveRunBuilder<Int32Type, Int32Type>,
        source: &RunArray<Int32Type>,
        values: &Int32Array,
        logical_index: usize,
    ) {
        let physical_index = source.get_physical_index(logical_index);
        builder.append_option(
            values
                .is_valid(physical_index)
                .then(|| values.value(physical_index)),
        );
    }

    fn buffer_materialized_filter(
        &mut self,
        source: &dyn Array,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        self.flush_builder();
        let filtered = filter.filter(source)?;
        self.total_size_of_non_shared_buffers += filtered.get_array_memory_size();
        self.buffered_arrays.push(filtered);
        Ok(())
    }
}

impl InProgressArray for InProgressInt32RunArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        if let Some(old_source) = self.source.take()
            && self.source_data_referenced_in_buffers
        {
            self.total_size_of_non_shared_buffers += old_source.get_array_memory_size();
        }
        self.source_data_referenced_in_buffers = false;
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.flush_builder();
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressInt32RunArray: source not set".to_string(),
            )
        })?;
        self.source_data_referenced_in_buffers = true;
        self.buffered_arrays.push(source.slice(offset, len));
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        if filter.count() > filter.filter_len() / SPARSE_FILTER_MAX_SELECTIVITY_DENOMINATOR {
            return self.buffer_materialized_filter(source.as_ref(), filter);
        }
        let source = source.as_run::<Int32Type>();
        let FilterSelection::Indices(indices) = filter.selection() else {
            return self.buffer_materialized_filter(source, filter);
        };
        let values = source.values().as_primitive::<Int32Type>();
        let builder = self.builder.get_or_insert_with(|| {
            PrimitiveRunBuilder::with_capacity(self.batch_size.min(filter.count()))
        });
        indices.for_each(|index| Self::append_index(builder, source, values, index));
        self.builder_rows += filter.count();
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.flush_builder();
        let result = match self.buffered_arrays.len() {
            0 => {
                let (_, run_ends, values) = PrimitiveRunBuilder::<Int32Type, Int32Type>::new()
                    .finish()
                    .into_parts();
                // SAFETY: all three components describe the same empty RunArray.
                Arc::new(unsafe {
                    RunArray::<Int32Type>::new_unchecked(self.data_type.clone(), run_ends, values)
                }) as ArrayRef
            }
            1 => self.buffered_arrays.pop().unwrap(),
            _ => concat(
                &self
                    .buffered_arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?,
        };
        self.buffered_arrays.clear();
        self.total_size_of_non_shared_buffers = 0;
        self.source_data_referenced_in_buffers = false;
        Ok(result)
    }

    fn size(&self) -> usize {
        self.total_size_of_non_shared_buffers
            + self.buffered_arrays.capacity() * size_of::<ArrayRef>()
            + self
                .source
                .as_ref()
                .map_or(0, |source| source.get_array_memory_size())
            + self.builder_rows * (size_of::<i32>() * 2 + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::BooleanArray;

    fn run_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend(values);
        Arc::new(builder.finish())
    }

    #[test]
    fn sparse_copy_preserves_values_and_runs() {
        let source = run_array((0..256).map(|index| match index {
            0..=63 => Some(1),
            64..=127 => None,
            128..=191 => Some(2),
            _ => Some(3),
        }));
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| matches!(index, 0 | 64 | 129 | 255))
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress = InProgressInt32RunArray::new(source.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let output = in_progress.finish().unwrap();
        let output = output.as_run::<Int32Type>();
        assert_eq!(output.run_ends().values(), &[1, 2, 3, 4]);
        assert_eq!(
            output.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), None, Some(2), Some(3)])
        );
    }

    #[test]
    fn preserves_order_when_sparse_and_dense_inputs_mix() {
        let first = run_array((0..256).map(|index| Some(index / 64)));
        let second = run_array([Some(9), Some(9), None]);
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| index == 0 || index == 255)
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress = InProgressInt32RunArray::new(first.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(first, &predicate)
            .unwrap();
        in_progress.set_source(Some(second));
        in_progress.copy_rows(0, 3).unwrap();
        let output = in_progress.finish().unwrap();
        let output = output.as_run::<Int32Type>();
        let decoded = (0..output.len())
            .map(|index| {
                let physical = output.get_physical_index(index);
                output
                    .values()
                    .as_primitive::<Int32Type>()
                    .is_valid(physical)
                    .then(|| output.values().as_primitive::<Int32Type>().value(physical))
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![Some(0), Some(3), Some(9), Some(9), None]);
    }
}
