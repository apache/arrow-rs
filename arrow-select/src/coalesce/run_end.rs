// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::InProgressArray;
use arrow_array::cast::AsArray;
use arrow_array::types::RunEndIndexType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RunArray, make_array, new_empty_array};
use arrow_buffer::ArrowNativeType;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

struct ValueRange {
    source: usize,
    start: usize,
    end: usize,
}

pub(crate) struct InProgressRunArray<R: RunEndIndexType> {
    source: Option<ArrayRef>,
    sources: Vec<ArrayRef>,
    run_ends: Vec<R::Native>,
    value_ranges: Vec<ValueRange>,
    last_value: Option<(usize, usize)>,
    current_source: Option<usize>,
    len: usize,
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
            run_ends: Vec::new(),
            value_ranges: Vec::new(),
            last_value: None,
            current_source: None,
            len: 0,
            data_type,
        }
    }

    fn native_run_end(end: usize) -> Result<R::Native, ArrowError> {
        R::Native::from_usize(end)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))
    }

    fn append_value_range(&mut self, source: usize, start: usize, end: usize) {
        if start == end {
            return;
        }
        if let Some(last) = self.value_ranges.last_mut()
            && last.source == source
            && last.end == start
        {
            last.end = end;
            return;
        }
        self.value_ranges.push(ValueRange { source, start, end });
    }

    /// Appends one contiguous logical row range, comparing only its first run
    /// with the buffered tail before bulk-appending the remaining runs.
    fn copy_rows_from(
        &mut self,
        array: &RunArray<R>,
        offset: usize,
        len: usize,
    ) -> Result<(), ArrowError> {
        if len == 0 {
            return Ok(());
        }
        let source_run_ends = array.run_ends();
        let selection_start = source_run_ends
            .offset()
            .checked_add(offset)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError("RunArray selection overflow".into())
            })?;
        let selection_end = selection_start.checked_add(len).ok_or_else(|| {
            ArrowError::InvalidArgumentError("RunArray selection overflow".into())
        })?;
        let output_start = self.len;
        let output_end = output_start
            .checked_add(len)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Run end overflow".into()))?;
        Self::native_run_end(output_end)?;

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

        if output_start == 0 && offset == 0 && len == array.len() && source_run_ends.offset() == 0 {
            self.run_ends
                .extend_from_slice(&source_run_ends.values()[first..=last]);
            self.append_value_range(source_idx, first, last + 1);
            self.last_value = Some((source_idx, last));
            self.len = output_end;
            return Ok(());
        }

        let run_end = |index: usize| {
            let selected = source_run_ends.values()[index]
                .as_usize()
                .min(selection_end)
                - selection_start;
            R::Native::from_usize(output_start + selected).unwrap()
        };

        // Preserve distinct physical runs, as RunArray interleave and concat do.
        let merged_first = self.last_value == Some((source_idx, first));
        let first_end = run_end(first);
        if merged_first {
            *self.run_ends.last_mut().unwrap() = first_end;
        } else {
            self.run_ends.push(first_end);
        }

        self.run_ends.reserve(last - first);
        self.run_ends.extend((first + 1..=last).map(run_end));

        let values_start = first + usize::from(merged_first);
        self.append_value_range(source_idx, values_start, last + 1);
        if first != last || !merged_first {
            self.last_value = Some((source_idx, last));
        }
        self.len = output_end;
        Ok(())
    }
}

impl<R: RunEndIndexType> InProgressArray for InProgressRunArray<R> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
        self.current_source = None;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.clone().ok_or_else(|| {
            ArrowError::InvalidArgumentError("InProgressRunArray source not set".into())
        })?;
        self.copy_rows_from(source.as_run::<R>(), offset, len)
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.run_ends.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        let data = self.sources.iter().map(|x| x.to_data()).collect::<Vec<_>>();
        let mut values =
            MutableArrayData::try_new(data.iter().collect(), false, self.run_ends.len())?;
        for range in self.value_ranges.drain(..) {
            values.try_extend(range.source, range.start, range.end)?;
        }
        let values = make_array(values.freeze());
        let run_ends = PrimitiveArray::<R>::from_iter_values(self.run_ends.drain(..));
        let result = RunArray::<R>::try_new(&run_ends, values.as_ref())?;

        self.sources.clear();
        self.current_source = None;
        self.last_value = None;
        self.len = 0;
        Ok(Arc::new(result))
    }

    fn size(&self) -> usize {
        self.sources
            .iter()
            .map(|x| x.get_array_memory_size())
            .sum::<usize>()
            + self.sources.capacity() * size_of::<ArrayRef>()
            + self.run_ends.capacity() * size_of::<R::Native>()
            + self.value_ranges.capacity() * size_of::<ValueRange>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    use arrow_array::{BooleanArray, Int32Array, Int64Array, ListArray};

    fn assert_runs(array: ArrayRef, run_ends: &[i32], values: &[i32]) {
        let array = array.as_run::<Int32Type>();
        assert_eq!(array.run_ends().values(), run_ends);
        assert_eq!(array.values().as_primitive::<Int32Type>().values(), values);
    }
    fn i32_run(run_ends: Vec<i32>, values: Vec<i32>) -> ArrayRef {
        Arc::new(
            RunArray::<Int32Type>::try_new(&Int32Array::from(run_ends), &Int32Array::from(values))
                .unwrap(),
        )
    }

    fn sparse_copy<R: RunEndIndexType>(source: Arc<RunArray<R>>) {
        let source: ArrayRef = source;
        let selected = [0, 64, 129, 255];
        let filter =
            BooleanArray::from_iter((0..source.len()).map(|i| Some(selected.contains(&i))));
        let expected = crate::filter::filter(source.as_ref(), &filter).unwrap();
        let mut output = InProgressRunArray::<R>::new(source.data_type().clone());
        output.set_source(Some(source));
        for index in selected {
            output.copy_rows(index, 1).unwrap();
        }
        assert_eq!(output.finish().unwrap().as_ref(), expected.as_ref());
    }

    #[test]
    fn supports_run_end_widths_slices_and_dynamic_values() {
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
    fn handles_partial_and_sliced_run_boundaries() {
        let source = i32_run(vec![3, 7, 9], vec![1, 2, 3]);
        let mut output = InProgressRunArray::<Int32Type>::new(source.data_type().clone());
        output.set_source(Some(source));
        output.copy_rows(1, 4).unwrap();
        output.copy_rows(5, 3).unwrap();
        assert_runs(output.finish().unwrap(), &[2, 6, 7], &[1, 2, 3]);

        let source = i32_run(vec![2, 5, 9, 11], vec![0, 1, 2, 3]);
        let source = Arc::new(source.as_run::<Int32Type>().slice(1, 9)) as ArrayRef;
        let mut output = InProgressRunArray::<Int32Type>::new(source.data_type().clone());
        output.set_source(Some(source));
        output.copy_rows(1, 2).unwrap();
        output.copy_rows(3, 4).unwrap();
        assert_runs(output.finish().unwrap(), &[3, 6], &[1, 2]);
    }

    #[test]
    fn preserves_order_when_filtered_and_contiguous_copies_mix() {
        let first = i32_run(vec![2, 4], vec![1, 2]);
        let sparse = i32_run(vec![2, 4], vec![3, 4]);
        let last = i32_run(vec![2], vec![5]);
        let mut output = InProgressRunArray::<Int32Type>::new(first.data_type().clone());

        output.set_source(Some(first));
        output.copy_rows(0, 2).unwrap();
        output.set_source(Some(sparse));
        output.copy_rows(1, 2).unwrap();
        output.set_source(Some(last));
        output.copy_rows(0, 2).unwrap();

        assert_runs(output.finish().unwrap(), &[2, 3, 4, 6], &[1, 3, 4, 5]);
    }

    #[test]
    fn preserves_equal_values_in_distinct_sources_as_distinct_runs() {
        let first = Arc::new(RunArray::<Int32Type>::from_iter([Some("same")])) as ArrayRef;
        let second = first.clone();
        let mut output = InProgressRunArray::<Int32Type>::new(first.data_type().clone());
        output.set_source(Some(first));
        output.copy_rows(0, 1).unwrap();
        output.set_source(Some(second));
        output.copy_rows(0, 1).unwrap();
        let result = output.finish().unwrap();
        assert_eq!(result.as_run::<Int32Type>().run_ends().values(), &[1, 2]);
    }
}
