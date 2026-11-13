// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{InProgressArray, generic::GenericInProgressArray};
use crate::filter::{FilterPredicate, FilterSelection};
use arrow_array::cast::AsArray;
use arrow_array::types::RunEndIndexType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RunArray, make_array, new_empty_array};
use arrow_buffer::ArrowNativeType;
use arrow_cmp::make_comparator;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType, SortOptions};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

struct Run<R: RunEndIndexType> {
    source: usize,
    index: usize,
    end: R::Native,
}

pub(crate) struct InProgressRunArray<R: RunEndIndexType> {
    /// The baseline slice-and-concat path used for materialized filters.
    generic: GenericInProgressArray,
    /// Number of logical rows buffered by `generic`.
    generic_len: usize,
    /// The current source used by [`Self::copy_rows`].
    source: Option<ArrayRef>,
    /// Value arrays referenced by the output runs.
    sources: Vec<ArrayRef>,
    /// The value and cumulative logical end of each output run.
    runs: Vec<Run<R>>,
    /// Index in `sources` for the array selected by [`Self::set_source`].
    current_source: Option<usize>,
    /// The number of logical rows copied into the output.
    len: usize,
    /// The data type to use when producing an empty array.
    data_type: DataType,
}

impl<R: RunEndIndexType> Debug for InProgressRunArray<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("InProgressRunArray")
    }
}

impl<R: RunEndIndexType> InProgressRunArray<R> {
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            generic: GenericInProgressArray::new(),
            generic_len: 0,
            source: None,
            sources: Vec::new(),
            runs: Vec::new(),
            current_source: None,
            len: 0,
            data_type,
        }
    }

    fn next_run_end(&self, len: usize) -> Result<R::Native, ArrowError> {
        let next_len = self
            .len
            .checked_add(len)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;
        R::Native::from_usize(next_len)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))
    }

    fn append_first_run(
        &mut self,
        source: usize,
        index: usize,
        len: usize,
    ) -> Result<(), ArrowError> {
        let end = self.next_run_end(len)?;

        if let Some(last) = self.runs.last_mut() {
            let equal = (last.source, last.index) == (source, index)
                || make_comparator(
                    self.sources[last.source].as_ref(),
                    self.sources[source].as_ref(),
                    SortOptions::default(),
                )?(last.index, index)
                .is_eq();
            if equal {
                last.end = end;
                self.len += len;
                return Ok(());
            }
        }

        self.append_run(source, index, len)
    }

    fn append_run(&mut self, source: usize, index: usize, len: usize) -> Result<(), ArrowError> {
        let end = self.next_run_end(len)?;
        self.len += len;
        self.runs.push(Run { source, index, end });
        Ok(())
    }

    fn copy_rows_sparse(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        if len == 0 {
            return Ok(());
        }
        let source = self.source.clone().ok_or_else(|| {
            ArrowError::InvalidArgumentError("InProgressRunArray source not set".into())
        })?;
        let array = source.as_run::<R>();
        let run_ends = array.run_ends();
        let selection_start = run_ends.offset().checked_add(offset).ok_or_else(|| {
            ArrowError::InvalidArgumentError("RunArray selection overflow".into())
        })?;
        let selection_end = selection_start.checked_add(len).ok_or_else(|| {
            ArrowError::InvalidArgumentError("RunArray selection overflow".into())
        })?;
        let first = array.get_physical_index(offset);
        let last = array.get_physical_index(offset + len - 1);

        let source_idx = match self.current_source {
            Some(source) => source,
            None => {
                self.sources.push(array.values().clone());
                let source = self.sources.len() - 1;
                self.current_source = Some(source);
                source
            }
        };

        let run_len = |index: usize| {
            let start = index
                .checked_sub(1)
                .map(|idx| run_ends.values()[idx].as_usize())
                .unwrap_or_default()
                .max(selection_start);
            let end = run_ends.values()[index].as_usize().min(selection_end);
            end - start
        };
        self.append_first_run(source_idx, first, run_len(first))?;
        for index in first + 1..=last {
            self.append_run(source_idx, index, run_len(index))?;
        }
        Ok(())
    }

    fn finish_sparse(&mut self) -> Result<Option<ArrayRef>, ArrowError> {
        if self.runs.is_empty() {
            return Ok(None);
        }

        let data = self.sources.iter().map(|x| x.to_data()).collect::<Vec<_>>();
        let mut values = MutableArrayData::try_new(data.iter().collect(), false, self.runs.len())?;
        let mut run_ends = Vec::with_capacity(self.runs.len());
        let mut runs = self.runs.drain(..).peekable();
        while let Some(run) = runs.next() {
            let source = run.source;
            let start = run.index;
            let mut end = start + 1;
            run_ends.push(run.end);

            while let Some(run) = runs.next_if(|run| run.source == source && run.index == end) {
                run_ends.push(run.end);
                end += 1;
            }

            values.try_extend(source, start, end)?;
        }
        let values = make_array(values.freeze());
        let run_ends = PrimitiveArray::<R>::from_iter_values(run_ends);
        let result = RunArray::<R>::try_new(&run_ends, values.as_ref())?;

        self.sources.clear();
        self.current_source = None;
        self.len = 0;
        Ok(Some(Arc::new(result)))
    }

    fn flush_sparse_to_generic(&mut self) -> Result<(), ArrowError> {
        if let Some(array) = self.finish_sparse()? {
            let len = array.len();
            self.generic.set_source(Some(array));
            self.generic.copy_rows(0, len)?;
            self.generic.set_source(None);
            self.generic_len += len;
        }
        Ok(())
    }
}

impl<R: RunEndIndexType> InProgressArray for InProgressRunArray<R> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.generic.set_source(source.clone());
        self.source = source;
        self.current_source = None;
    }

    /// Buffers ordinary row ranges as slices for the generic concat path.
    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        if len == 0 {
            return Ok(());
        }
        if !self.runs.is_empty() {
            let source = self.source.clone();
            self.flush_sparse_to_generic()?;
            self.generic.set_source(source);
        }
        self.generic.copy_rows(offset, len)?;
        self.generic_len += len;
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        super::check_filter_len(filter, source.len())?;
        self.source = Some(source);
        self.current_source = None;
        let result = match filter.selection() {
            FilterSelection::None => Ok(()),
            FilterSelection::All { len } => self.copy_rows_sparse(0, len),
            FilterSelection::Slices(slices) => {
                slices.try_for_each(|(start, end)| self.copy_rows_sparse(start, end - start))
            }
            FilterSelection::Indices(indices) => {
                indices.try_for_each(|idx| self.copy_rows_sparse(idx, 1))
            }
        };
        self.source = None;
        self.current_source = None;
        result
    }

    /// Builds the run-end and value arrays directly from the accumulated runs.
    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.generic_len == 0 {
            if let Some(array) = self.finish_sparse()? {
                return Ok(array);
            }
            return Ok(new_empty_array(&self.data_type));
        }

        self.flush_sparse_to_generic()?;
        let result = self.generic.finish()?;
        self.generic_len = 0;
        Ok(result)
    }

    fn size(&self) -> usize {
        self.generic.size()
            + self
                .sources
                .iter()
                .map(|x| x.get_array_memory_size())
                .sum::<usize>()
            + self.sources.capacity() * size_of::<ArrayRef>()
            + self.runs.capacity() * size_of::<Run<R>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    use arrow_array::{BooleanArray, Int32Array, Int64Array, ListArray};
    use std::sync::Arc;

    fn assert_i32_runs(array: &ArrayRef, run_ends: &[i32], values: &[i32]) {
        let array = array.as_run::<Int32Type>();
        assert_eq!(array.run_ends().values(), run_ends);
        assert_eq!(array.values().as_primitive::<Int32Type>().values(), values);
    }

    fn sparse_copy<R: RunEndIndexType>(source: Arc<RunArray<R>>) {
        let source: ArrayRef = source;
        let filter = BooleanArray::from(
            (0..source.len())
                .map(|idx| matches!(idx, 0 | 64 | 129 | 255))
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();
        let expected = crate::filter::filter(source.as_ref(), &filter).unwrap();
        let mut output = InProgressRunArray::<R>::new(source.data_type().clone());
        output.copy_rows_by_filter_from(source, &predicate).unwrap();
        assert_eq!(output.finish().unwrap().as_ref(), expected.as_ref());
    }

    #[test]
    fn supports_all_run_end_widths_and_dynamic_values() {
        sparse_copy(Arc::new(RunArray::<Int16Type>::from_iter(
            (0..256).map(|idx| Some(["a", "b", "c", "d"][idx / 64])),
        )));
        let source = RunArray::<Int32Type>::from_iter(
            (0..320).map(|idx| Some(["a", "b", "c", "d", "e"][idx / 64])),
        );
        sparse_copy(Arc::new(source.slice(32, 256)));

        let run_ends = Int64Array::from(vec![64, 128, 192, 256]);
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(2)]),
        ]);
        sparse_copy(Arc::new(
            RunArray::<Int64Type>::try_new(&run_ends, &values).unwrap(),
        ));
    }

    #[test]
    fn merges_equal_values_across_sources() {
        let first = Arc::new(RunArray::<Int32Type>::from_iter([Some("same")])) as ArrayRef;
        let second = Arc::new(RunArray::<Int32Type>::from_iter([Some("same")])) as ArrayRef;
        let mut output = InProgressRunArray::<Int32Type>::new(first.data_type().clone());

        output.set_source(Some(first));
        output.copy_rows_sparse(0, 1).unwrap();
        output.set_source(Some(second));
        output.copy_rows_sparse(0, 1).unwrap();

        let result = output.finish().unwrap();
        let result = result.as_run::<Int32Type>();
        assert_eq!(result.run_ends().values(), &[2]);
        assert_eq!(result.values().len(), 1);
    }

    #[test]
    fn copy_rows_handles_partial_run_boundaries() {
        let run_ends = Int32Array::from(vec![3, 7, 9]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let source =
            Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap()) as ArrayRef;
        let mut output = InProgressRunArray::<Int32Type>::new(source.data_type().clone());

        output.set_source(Some(source));
        output.copy_rows_sparse(1, 4).unwrap();
        output.copy_rows_sparse(5, 3).unwrap();

        assert_i32_runs(&output.finish().unwrap(), &[2, 6, 7], &[1, 2, 3]);
    }

    #[test]
    fn copy_rows_handles_sliced_run_array() {
        let run_ends = Int32Array::from(vec![2, 5, 9, 11]);
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let source = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let source = Arc::new(source.slice(1, 9)) as ArrayRef;
        let mut output = InProgressRunArray::<Int32Type>::new(source.data_type().clone());

        output.set_source(Some(source));
        output.copy_rows_sparse(1, 2).unwrap();
        output.copy_rows_sparse(3, 4).unwrap();

        assert_i32_runs(&output.finish().unwrap(), &[3, 6], &[1, 2]);
    }

    #[test]
    fn preserves_order_when_sparse_and_generic_paths_mix() {
        let first = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 4]),
                &Int32Array::from(vec![1, 2]),
            )
            .unwrap(),
        ) as ArrayRef;
        let sparse = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 4]),
                &Int32Array::from(vec![3, 4]),
            )
            .unwrap(),
        ) as ArrayRef;
        let last = Arc::new(
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2]), &Int32Array::from(vec![5]))
                .unwrap(),
        ) as ArrayRef;
        let filter = BooleanArray::from(vec![false, true, true, false]);
        let predicate = FilterBuilder::new(&filter).build();
        let mut output = InProgressRunArray::<Int32Type>::new(first.data_type().clone());

        output.set_source(Some(first));
        output.copy_rows(0, 2).unwrap();
        output.copy_rows_by_filter_from(sparse, &predicate).unwrap();
        output.set_source(Some(last));
        output.copy_rows(0, 2).unwrap();

        assert_i32_runs(&output.finish().unwrap(), &[2, 3, 4, 6], &[1, 3, 4, 5]);
    }
}
