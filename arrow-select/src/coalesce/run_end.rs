// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::InProgressArray;
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
    /// The current source used by [`Self::copy_rows`].
    source: Option<ArrayRef>,
    /// Value arrays referenced by the output runs.
    sources: Vec<ArrayRef>,
    /// The value and cumulative logical end of each output run.
    runs: Vec<Run<R>>,
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
            source: None,
            sources: Vec::new(),
            runs: Vec::new(),
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
}

impl<R: RunEndIndexType> InProgressArray for InProgressRunArray<R> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    /// Copies a logical row range directly into the output runs.
    ///
    /// Only the first selected run can match the last buffered run. The remaining
    /// source runs already have the correct boundaries and can be appended directly.
    ///
    /// ```text
    /// source:           | A A A | B B B B | C C |
    /// selected 2..8:        [ A | B B B B | C ]
    /// buffered output:  | ... | A |
    ///                            ^ compare this boundary
    /// result:           | ... | A A | B B B B | C |
    ///                          merge     append    append
    /// ```
    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
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

        let values = array.values();
        let source_idx = self
            .sources
            .iter()
            .position(|source| Arc::ptr_eq(source, values))
            .unwrap_or_else(|| {
                self.sources.push(values.clone());
                self.sources.len() - 1
            });

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

    /// Builds the run-end and value arrays directly from the accumulated runs.
    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.runs.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        let data = self.sources.iter().map(|x| x.to_data()).collect::<Vec<_>>();
        let mut values = MutableArrayData::try_new(data.iter().collect(), false, self.runs.len())?;
        let mut run_ends = Vec::with_capacity(self.runs.len());
        for run in self.runs.drain(..) {
            values.try_extend(run.source, run.index, run.index + 1)?;
            run_ends.push(run.end);
        }
        let values = make_array(values.freeze());
        let run_ends = PrimitiveArray::<R>::from_iter_values(run_ends);
        let result = RunArray::<R>::try_new(&run_ends, values.as_ref())?;

        self.sources.clear();
        self.len = 0;
        Ok(Arc::new(result))
    }

    fn size(&self) -> usize {
        self.sources
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
        output.copy_rows(0, 1).unwrap();
        output.set_source(Some(second));
        output.copy_rows(0, 1).unwrap();

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
        output.copy_rows(1, 4).unwrap();
        output.copy_rows(5, 3).unwrap();

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
        output.copy_rows(1, 2).unwrap();
        output.copy_rows(3, 4).unwrap();

        assert_i32_runs(&output.finish().unwrap(), &[3, 6], &[1, 2]);
    }
}
