// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::InProgressArray;
use arrow_array::cast::AsArray;
use arrow_array::types::RunEndIndexType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RunArray, make_array, new_empty_array};
use arrow_buffer::ArrowNativeType;
use arrow_cmp::{DynComparator, make_comparator};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType, SortOptions};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

pub(crate) struct InProgressRunArray<R: RunEndIndexType> {
    /// The current source used by [`Self::copy_rows`].
    source: Option<ArrayRef>,
    /// The data type to use when producing an empty array.
    data_type: DataType,
    /// The cumulative logical end of each output run.
    run_ends: Vec<R::Native>,
    /// Value arrays referenced by the output runs.
    sources: Vec<ArrayRef>,
    /// The `(source, physical index)` of each output run value.
    indices: Vec<(usize, usize)>,
    /// Comparators cached for pairs of value arrays.
    comparators: HashMap<(usize, usize), DynComparator>,
    /// The value array for the current source.
    current_source: Option<usize>,
    /// The number of logical rows copied into the output.
    len: usize,
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
            data_type,
            run_ends: Vec::new(),
            sources: Vec::new(),
            indices: Vec::new(),
            comparators: HashMap::new(),
            current_source: None,
            len: 0,
        }
    }

    fn append_run(&mut self, source: usize, index: usize, len: usize) -> Result<(), ArrowError> {
        let next_len = self
            .len
            .checked_add(len)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;
        let run_end = R::Native::from_usize(next_len)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;

        if let Some(&(last_source, last_index)) = self.indices.last() {
            if (last_source, last_index) == (source, index) {
                self.len = next_len;
                *self.run_ends.last_mut().unwrap() = run_end;
                return Ok(());
            }
            let key = (last_source, source);
            if !self.comparators.contains_key(&key) {
                let comparator = make_comparator(
                    self.sources[last_source].as_ref(),
                    self.sources[source].as_ref(),
                    SortOptions::default(),
                )?;
                self.comparators.insert(key, comparator);
            }
            if self.comparators[&key](last_index, index).is_eq() {
                self.len = next_len;
                *self.run_ends.last_mut().unwrap() = run_end;
                *self.indices.last_mut().unwrap() = (source, index);
                return Ok(());
            }
        }

        self.len = next_len;
        self.run_ends.push(run_end);
        self.indices.push((source, index));
        Ok(())
    }
}

impl<R: RunEndIndexType> InProgressArray for InProgressRunArray<R> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
        self.current_source = None;
    }

    /// Copies a logical row range directly into the output runs.
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

        let source_idx = match self.current_source {
            Some(idx) => idx,
            None => {
                let values = array.values();
                let idx = self
                    .sources
                    .iter()
                    .position(|source| std::sync::Arc::ptr_eq(source, values))
                    .unwrap_or_else(|| {
                        self.sources.push(values.clone());
                        self.sources.len() - 1
                    });
                self.current_source = Some(idx);
                idx
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
        self.append_run(source_idx, first, run_len(first))?;

        let key = (source_idx, source_idx);
        if first != last && !self.comparators.contains_key(&key) {
            self.comparators.insert(
                key,
                make_comparator(
                    array.values().as_ref(),
                    array.values().as_ref(),
                    Default::default(),
                )?,
            );
        }
        for index in first + 1..=last {
            let next_len = self
                .len
                .checked_add(run_len(index))
                .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;
            let run_end = R::Native::from_usize(next_len)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;
            let last_index = self.indices.last().unwrap().1;
            if self.comparators[&key](last_index, index).is_eq() {
                *self.run_ends.last_mut().unwrap() = run_end;
                *self.indices.last_mut().unwrap() = (source_idx, index);
            } else {
                self.run_ends.push(run_end);
                self.indices.push((source_idx, index));
            }
            self.len = next_len;
        }
        Ok(())
    }

    /// Builds the run-end and value arrays directly from the accumulated runs.
    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.run_ends.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        let data = self.sources.iter().map(|x| x.to_data()).collect::<Vec<_>>();
        let mut values =
            MutableArrayData::try_new(data.iter().collect(), false, self.indices.len())?;
        let mut ranges = self.indices.iter().copied().peekable();
        while let Some((source, start)) = ranges.next() {
            let mut end = start + 1;
            while ranges
                .next_if(|&(next_source, next)| next_source == source && next == end)
                .is_some()
            {
                end += 1;
            }
            values.try_extend(source, start, end)?;
        }
        let values = make_array(values.freeze());
        let run_ends = PrimitiveArray::<R>::from_iter_values(self.run_ends.drain(..));
        let result = RunArray::<R>::try_new(&run_ends, values.as_ref())?;

        self.sources.clear();
        self.indices.clear();
        self.comparators.clear();
        self.current_source = None;
        self.len = 0;
        Ok(std::sync::Arc::new(result))
    }

    fn size(&self) -> usize {
        self.sources
            .iter()
            .map(|x| x.get_array_memory_size())
            .sum::<usize>()
            + self.run_ends.capacity() * size_of::<R::Native>()
            + self.sources.capacity() * size_of::<ArrayRef>()
            + self.indices.capacity() * size_of::<(usize, usize)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    use arrow_array::{BooleanArray, Int64Array, ListArray};
    use std::sync::Arc;

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
}
