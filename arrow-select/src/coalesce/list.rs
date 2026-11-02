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
use crate::interleave::interleave;
use arrow_array::{Array, ArrayRef, new_empty_array};
use arrow_schema::{ArrowError, DataType};

/// Bound retained input data while batching enough to amortize interleave.
const MAX_PENDING_SOURCES: usize = 64;
const MAX_PENDING_SOURCE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct InProgressListArray {
    /// The current source for contiguous row copies.
    source: Option<ArrayRef>,
    /// The output array type, either List or LargeList.
    data_type: DataType,
    /// Materialized sparse groups and contiguous source slices in output order.
    buffered_arrays: Vec<ArrayRef>,
    /// Source arrays retained until the next sparse interleave.
    sparse_sources: Vec<ArrayRef>,
    /// Selected `(source index, row index)` pairs for `sparse_sources`.
    sparse_indices: Vec<(usize, usize)>,
    /// Total memory size reported by the retained sparse sources.
    sparse_source_bytes: usize,
    /// Whether buffered slices reference the current source's data.
    source_data_referenced_in_buffers: bool,
    /// Memory retained by buffered arrays that is not counted by `source`.
    total_size_of_non_shared_buffers: usize,
}

impl InProgressListArray {
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            source: None,
            data_type,
            buffered_arrays: Vec::new(),
            sparse_sources: Vec::new(),
            sparse_indices: Vec::new(),
            sparse_source_bytes: 0,
            source_data_referenced_in_buffers: false,
            total_size_of_non_shared_buffers: 0,
        }
    }

    fn sparse_limit_reached(&self) -> bool {
        self.sparse_sources.len() >= MAX_PENDING_SOURCES
            || self.sparse_source_bytes >= MAX_PENDING_SOURCE_BYTES
    }

    fn flush_sparse(&mut self) -> Result<(), ArrowError> {
        if self.sparse_indices.is_empty() {
            self.sparse_sources.clear();
            self.sparse_source_bytes = 0;
            return Ok(());
        }

        let sources = self
            .sparse_sources
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        let array = interleave(&sources, &self.sparse_indices)?;
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
        self.sparse_sources.clear();
        self.sparse_indices.clear();
        self.sparse_source_bytes = 0;
        Ok(())
    }
}

impl InProgressArray for InProgressListArray {
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
        self.flush_sparse()?;
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError("InProgressListArray source not set".into())
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
        let source_size = source.get_array_memory_size();
        if !self.sparse_sources.is_empty()
            && (self.sparse_limit_reached()
                || self.sparse_source_bytes.saturating_add(source_size) > MAX_PENDING_SOURCE_BYTES)
        {
            self.flush_sparse()?;
        }

        let source_index = self.sparse_sources.len();
        match filter.selection() {
            FilterSelection::None => return Ok(()),
            FilterSelection::All { len } => self
                .sparse_indices
                .extend((0..len).map(|index| (source_index, index))),
            FilterSelection::Slices(slices) => slices.for_each(|(start, end)| {
                self.sparse_indices
                    .extend((start..end).map(|index| (source_index, index)));
            }),
            FilterSelection::Indices(indices) => indices.for_each(|index| {
                self.sparse_indices.push((source_index, index));
            }),
        }
        self.sparse_sources.push(source);
        self.sparse_source_bytes += source_size;
        if self.sparse_limit_reached() {
            self.flush_sparse()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.flush_sparse()?;
        let result = match self.buffered_arrays.len() {
            0 => new_empty_array(&self.data_type),
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
            + self.sparse_sources.capacity() * size_of::<ArrayRef>()
            + self.sparse_indices.capacity() * size_of::<(usize, usize)>()
            + self
                .source
                .as_ref()
                .map_or(0, |source| source.get_array_memory_size())
            + self.sparse_source_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::{BooleanArray, Int32Array, ListArray, StructArray, UInt32Array};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;
    use std::sync::Arc;

    fn list_array() -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = OffsetBuffer::from_lengths([2, 1, 0, 2, 0]);
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(3),
            Some(4),
        ]));
        let nulls = NullBuffer::from(vec![true, true, false, true, true]);
        Arc::new(ListArray::new(field, offsets, values, Some(nulls)))
    }

    #[test]
    fn sparse_copy_preserves_nulls_and_empty_lists() {
        let source = list_array();
        let filter = BooleanArray::from(vec![true, false, true, true, true]);
        let predicate = FilterBuilder::new(&filter).optimize().build();
        let expected = crate::filter::filter(source.as_ref(), &filter).unwrap();
        let mut in_progress = InProgressListArray::new(source.data_type().clone());
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        assert_eq!(in_progress.sparse_sources.len(), 1);
        assert!(in_progress.buffered_arrays.is_empty());
        assert_eq!(in_progress.finish().unwrap().as_ref(), expected.as_ref());
    }

    #[test]
    fn sparse_copy_nested_children() {
        let child = StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..6)) as ArrayRef,
        )]);
        let field = Arc::new(Field::new("item", child.data_type().clone(), true));
        let source = Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([2, 1, 3]),
            Arc::new(child),
            None,
        )) as ArrayRef;
        let filter = BooleanArray::from(vec![true, false, true]);
        let predicate = FilterBuilder::new(&filter).optimize().build();
        let expected = crate::take::take(
            source.as_ref(),
            &UInt32Array::from_iter_values([0, 2]),
            None,
        )
        .unwrap();
        let mut in_progress = InProgressListArray::new(source.data_type().clone());
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let output = in_progress.finish().unwrap();
        assert_eq!(output.as_ref(), expected.as_ref());
    }

    #[test]
    fn preserves_order_when_sparse_and_contiguous_inputs_mix() {
        let first = list_array();
        let second = list_array();
        let filter = BooleanArray::from(vec![true, false, false, true, false]);
        let predicate = FilterBuilder::new(&filter).optimize().build();
        let filtered = crate::filter::filter(first.as_ref(), &filter).unwrap();
        let second_slice = second.slice(1, 2);
        let expected = concat(&[filtered.as_ref(), second_slice.as_ref()]).unwrap();
        let mut in_progress = InProgressListArray::new(first.data_type().clone());
        in_progress
            .copy_rows_by_filter_from(first, &predicate)
            .unwrap();
        in_progress.set_source(Some(second));
        in_progress.copy_rows(1, 2).unwrap();

        assert_eq!(in_progress.finish().unwrap().as_ref(), expected.as_ref());
    }

    #[test]
    fn bounds_retained_sparse_sources() {
        let source = list_array();
        let filter = BooleanArray::from(vec![true, false, false, false, false]);
        let predicate = FilterBuilder::new(&filter).optimize().build();
        let mut in_progress = InProgressListArray::new(source.data_type().clone());

        for _ in 0..MAX_PENDING_SOURCES {
            in_progress
                .copy_rows_by_filter_from(Arc::clone(&source), &predicate)
                .unwrap();
        }

        assert!(in_progress.sparse_sources.is_empty());
        assert_eq!(in_progress.sparse_source_bytes, 0);
        assert_eq!(in_progress.buffered_arrays.len(), 1);
        assert_eq!(in_progress.finish().unwrap().len(), MAX_PENDING_SOURCES);
    }
}
