// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::InProgressArray;
use crate::concat::concat;
use arrow_array::types::RunEndIndexType;
use arrow_array::{Array, ArrayRef, RunArray, cast::AsArray, new_empty_array};
use arrow_buffer::{ArrowNativeType, RunEndBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::{mem, sync::Arc};

/// Incrementally builds a run-end encoded array from logical slices.
///
/// Run ends are rebased while physical values remain as zero-copy source ranges.
/// Adjacent slices merge only when their boundary is the same physical value.
#[derive(Debug)]
pub(crate) struct InProgressRunArray<R: RunEndIndexType> {
    source: Option<ArrayRef>,
    /// Value arrays referenced by `ranges`.
    sources: Vec<ArrayRef>,
    /// Logical run ends in the buffered output.
    run_ends: Vec<R::Native>,
    /// `(source, start, end)` ranges of physical values to concatenate.
    ranges: Vec<(usize, usize, usize)>,
    /// Source and physical index of the buffered tail run.
    last: Option<(usize, usize)>,
    data_type: DataType,
}
impl<R: RunEndIndexType> InProgressRunArray<R> {
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            source: None,
            sources: vec![],
            run_ends: vec![],
            ranges: vec![],
            last: None,
            data_type,
        }
    }

    /// Copies every run end without physical index lookups or boundary clamps.
    #[inline(always)]
    fn copy_whole_array(
        &mut self,
        run_ends: &[R::Native],
        source_index: usize,
        output_offset: usize,
    ) {
        self.run_ends.extend(
            run_ends
                .iter()
                .map(|end| R::Native::usize_as(output_offset + end.as_usize())),
        );
        self.ranges.push((source_index, 0, run_ends.len()));
    }

    /// Appends the run ends intersecting a logical selection.
    /// Source run ends are clamped to the selection and rebased into the output.
    /// When the first selected run continues the buffered tail, its existing run
    /// end is extended instead of appending a duplicate physical value.
    #[inline(always)]
    #[expect(clippy::too_many_arguments, reason = "hot path uses scalar inputs")]
    fn append_selected_run_ends(
        &mut self,
        run_ends: &[R::Native],
        first_run: usize,
        last_run: usize,
        merge_first_run: bool,
        selection_start: usize,
        selection_end: usize,
        output_offset: usize,
    ) {
        let rebased_run_end = |physical_index: usize| {
            let run_end = run_ends[physical_index].as_usize().min(selection_end) - selection_start;
            R::Native::usize_as(output_offset + run_end)
        };

        if merge_first_run {
            *self.run_ends.last_mut().unwrap() = rebased_run_end(first_run);
        } else {
            self.run_ends.push(rebased_run_end(first_run));
        }
        self.run_ends
            .extend((first_run + 1..=last_run).map(rebased_run_end));
    }

    /// Retains the physical values referenced by the selected runs.
    /// The caller excludes an already-retained merged boundary run.
    /// Adjacent ranges from the same source are extended to avoid another concat input.
    #[inline(always)]
    fn retain_selected_values(&mut self, source_index: usize, first_run: usize, run_end: usize) {
        if first_run == run_end {
            return;
        }
        if let Some((range_source, _, range_end)) = self.ranges.last_mut()
            && *range_source == source_index
            && *range_end == first_run
        {
            *range_end = run_end;
        } else {
            self.ranges.push((source_index, first_run, run_end));
        }
    }

    /// Appends the logical slice `offset..offset + len`.
    #[inline(always)]
    fn copy_from(
        &mut self,
        array: &RunArray<R>,
        offset: usize,
        len: usize,
    ) -> Result<(), ArrowError> {
        let output_offset = self.run_ends.last().map_or(0, |x| x.as_usize());
        output_offset
            .checked_add(len)
            .and_then(R::Native::from_usize)
            .ok_or(ArrowError::RunEndIndexOverflowError)?;
        let run_ends = array.run_ends();
        let run_end_values = run_ends.values();
        let values = array.values();
        if !self
            .sources
            .last()
            .is_some_and(|source| Arc::ptr_eq(source, values))
        {
            self.sources.push(values.clone());
        }
        let source_index = self.sources.len() - 1;
        let first_run = array.get_physical_index(offset);
        let last_run = array.get_physical_index(offset + len - 1);
        let merge_first_run = self.last == Some((source_index, first_run));

        if !merge_first_run && offset == 0 && len == array.len() && run_ends.max_value() == len {
            self.copy_whole_array(run_end_values, source_index, output_offset);
        } else {
            let selection_start = run_ends.offset() + offset;
            self.append_selected_run_ends(
                run_end_values,
                first_run,
                last_run,
                merge_first_run,
                selection_start,
                selection_start + len,
                output_offset,
            );
            let first_value = first_run + usize::from(merge_first_run);
            self.retain_selected_values(source_index, first_value, last_run + 1);
        }
        self.last = Some((source_index, last_run));
        Ok(())
    }
}
impl<R: RunEndIndexType + std::fmt::Debug> InProgressArray for InProgressRunArray<R> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }
    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self
            .source
            .take()
            .ok_or_else(|| ArrowError::InvalidArgumentError("RunArray source not set".into()))?;
        let result = self.copy_from(source.as_run::<R>(), offset, len);
        self.source = Some(source);
        result
    }
    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.run_ends.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }
        let mut slices = self
            .ranges
            .drain(..)
            .map(|(source, start, end)| {
                let values = &self.sources[source];
                if start != 0 || end != values.len() {
                    values.slice(start, end - start)
                } else {
                    values.clone()
                }
            })
            .collect::<Vec<_>>();
        let values = if slices.len() == 1 {
            slices.pop().unwrap()
        } else {
            concat(&slices.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?
        };
        let len = self.run_ends.last().unwrap().as_usize();
        let run_ends = ScalarBuffer::from(mem::take(&mut self.run_ends));
        // SAFETY: copied run ends are increasing and match `values`.
        let result = unsafe {
            RunArray::<R>::new_unchecked(
                self.data_type.clone(),
                RunEndBuffer::new_unchecked(run_ends, 0, len),
                values,
            )
        };
        self.sources.clear();
        self.last = None;
        Ok(Arc::new(result))
    }

    fn size(&self) -> usize {
        self.sources
            .iter()
            .map(|x| x.get_array_memory_size())
            .sum::<usize>()
            + self.sources.capacity() * size_of::<ArrayRef>()
            + self.run_ends.capacity() * size_of::<R::Native>()
            + self.ranges.capacity() * size_of::<(usize, usize, usize)>()
    }
}
