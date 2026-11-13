// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::InProgressArray;
use crate::filter::{FilterPredicate, FilterSelection};
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
    /// The current source used by [`Self::copy_rows`].
    source: Option<ArrayRef>,
    /// Value arrays referenced by the output runs.
    sources: Vec<ArrayRef>,
    /// Cumulative logical end of each output run.
    run_ends: Vec<R::Native>,
    /// Contiguous ranges of values to copy at finish.
    value_ranges: Vec<ValueRange>,
    /// Source and physical index of the final output run value.
    last_value: Option<(usize, usize)>,
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

    fn first_value_matches(&self, source: usize, index: usize) -> bool {
        // As with RunArray interleave and concat, preserve distinct physical
        // runs even when their values happen to compare equal.
        self.last_value == Some((source, index))
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
        physical: Option<(usize, usize)>,
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

        let (first, last) = physical.unwrap_or_else(|| {
            (
                array.get_physical_index(offset),
                array.get_physical_index(offset + len - 1),
            )
        });
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
            // `output_end` was checked above, and every selected run end is no
            // greater than it.
            R::Native::from_usize(output_start + selected).unwrap()
        };

        let merged_first = self.first_value_matches(source_idx, first);
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
        self.copy_rows_from(source.as_run::<R>(), offset, len, None)
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        super::check_filter_len(filter, source.len())?;
        self.current_source = None;
        let array = source.as_run::<R>();
        let result = match filter.selection() {
            FilterSelection::None => Ok(()),
            FilterSelection::All { len } => self.copy_rows_from(array, 0, len, None),
            FilterSelection::Slices(slices) => {
                let run_ends = array.run_ends();
                let logical_offset = run_ends.offset();
                let values = run_ends.values();
                let mut physical = array.get_physical_index(0);
                slices.try_for_each(|(start, end)| {
                    let selection_start = logical_offset + start;
                    while values[physical].as_usize() <= selection_start {
                        physical += 1;
                    }
                    let first = physical;
                    let selection_end = logical_offset + end;
                    while values[physical].as_usize() < selection_end {
                        physical += 1;
                    }
                    self.copy_rows_from(array, start, end - start, Some((first, physical)))
                })
            }
            FilterSelection::Indices(indices) => {
                indices.try_for_each(|idx| self.copy_rows_from(array, idx, 1, None))
            }
        };
        self.current_source = None;
        result
    }

    /// Builds the run-end and value arrays directly from the accumulated runs.
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
    fn preserves_equal_values_across_sources_as_distinct_runs() {
        let first = Arc::new(RunArray::<Int32Type>::from_iter([Some("same")])) as ArrayRef;
        let second = Arc::new(RunArray::<Int32Type>::from_iter([Some("same")])) as ArrayRef;
        let mut output = InProgressRunArray::<Int32Type>::new(first.data_type().clone());

        output.set_source(Some(first));
        output.copy_rows(0, 1).unwrap();
        output.set_source(Some(second));
        output.copy_rows(0, 1).unwrap();

        let result = output.finish().unwrap();
        let result = result.as_run::<Int32Type>();
        assert_eq!(result.run_ends().values(), &[1, 2]);
        assert_eq!(result.values().len(), 2);
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

    #[test]
    fn preserves_order_when_filtered_and_contiguous_copies_mix() {
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
