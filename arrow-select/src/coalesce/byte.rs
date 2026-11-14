// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{InProgressArray, should_use_byte_array_direct_copy};
use crate::filter::{FilterIndices, FilterPredicate, FilterSelection};
use crate::take::take_arrays_unchecked;
use arrow_array::builder::GenericByteBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait, UInt32Array};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::ArrowError;
use std::sync::Arc;

pub(crate) struct InProgressByteArray<T: ByteArrayType> {
    source: Option<ArrayRef>,
    batch_size: usize,
    direct: GenericByteBuilder<T>,
}

#[inline(never)]
fn internal_error(message: &'static str) -> ArrowError {
    ArrowError::ComputeError(format!("Internal Error: {message}"))
}

impl<T: ByteArrayType> std::fmt::Debug for InProgressByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressByteArray")
            .field(
                "direct_rows",
                &self.direct.offsets_slice().len().saturating_sub(1),
            )
            .finish()
    }
}

impl<T: ByteArrayType> InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            direct: GenericByteBuilder::with_capacity(0, 0),
        }
    }

    fn checked_values_len(current: usize, additional: usize) -> Result<usize, ArrowError> {
        let new_len = current
            .checked_add(additional)
            .ok_or(ArrowError::OffsetOverflowError(usize::MAX))?;
        if new_len > T::Offset::MAX_OFFSET {
            return Err(ArrowError::OffsetOverflowError(new_len));
        }
        Ok(new_len)
    }

    fn ensure_capacity(&mut self, source: &GenericByteArray<T>, rows: usize) {
        if self.direct.offsets_slice().len() == 1 && !source.is_empty() {
            let offsets = source.value_offsets();
            let bytes = offsets[source.len()].as_usize() - offsets[0].as_usize();
            let rows = rows.min(self.batch_size);
            let estimated = bytes.saturating_mul(rows) / source.len();
            // Later slices may contain slightly larger values than this first sample.
            let capacity = estimated.saturating_add(estimated / 8);
            self.direct = GenericByteBuilder::with_capacity(rows, capacity);
        }
    }

    /// Reuses a byte array as a builder with bounded capacity; errors if offsets or values cannot be reused.
    #[inline(never)]
    fn into_builder_with_capacity(
        array: GenericByteArray<T>,
        batch_size: usize,
    ) -> Result<GenericByteBuilder<T>, ArrowError> {
        let selected_rows = array.len();
        if selected_rows == 0 || selected_rows >= batch_size {
            return array
                .into_builder()
                .map_err(|_| internal_error("byte-array buffers cannot be reused"));
        }
        let (offsets, values, nulls) = array.into_parts();
        let mut offsets = offsets
            .into_inner()
            .into_inner()
            .into_mutable()
            .map_err(|_| internal_error("byte-array buffers cannot be reused"))?;
        let mut values = values
            .into_mutable()
            .map_err(|_| internal_error("byte-array buffers cannot be reused"))?;

        let row_capacity = batch_size.min(selected_rows.saturating_add(1 << 16));
        let additional_rows = row_capacity.saturating_sub(selected_rows);
        offsets.reserve(additional_rows.saturating_mul(size_of::<T::Offset>()));

        let selected_bytes = values.len();
        let estimated = selected_bytes.saturating_mul(row_capacity) / selected_rows;
        let value_capacity = estimated
            .saturating_add(estimated / 8)
            .min(selected_bytes.saturating_add(1 << 20));
        values.reserve(value_capacity.saturating_sub(selected_bytes));

        // SAFETY: these came from a valid OffsetBuffer; reserve leaves every offset unchanged.
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        // SAFETY: offsets, values, and nulls retain the source array's lengths and contents.
        let array = unsafe { GenericByteArray::new_unchecked(offsets, values.into(), nulls) };
        array
            .into_builder()
            .map_err(|_| internal_error("byte-array buffers cannot be reused"))
    }

    fn append_filter_indices(
        &mut self,
        source: &GenericByteArray<T>,
        indices: FilterIndices<'_>,
        count: usize,
    ) -> Result<(), ArrowError> {
        self.ensure_capacity(source, count);
        indices.try_for_each(|idx| {
            if source.is_null(idx) {
                self.direct.append_null();
            } else {
                let value = source.value(idx);
                Self::checked_values_len(
                    self.direct.values_slice().len(),
                    source.value_length(idx).as_usize(),
                )?;
                self.direct.append_value(value);
            }
            Ok::<(), ArrowError>(())
        })?;
        Ok(())
    }
}

impl<T: ByteArrayType + 'static> InProgressArray for InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteArray: source not set".to_string(),
            )
        })?;
        let source = source.as_bytes::<T>().slice(offset, len);
        self.ensure_capacity(&source, len);
        self.direct.append_array(&source)
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        match filter.selection() {
            FilterSelection::None => Ok(()),
            // Index selections are sparse enough that copying values directly avoids the
            // extra allocation of a filtered byte array. Dense contiguous selections use
            // the filter kernel below, which copies their ranges in bulk.
            FilterSelection::Indices(indices) => {
                self.append_filter_indices(source.as_bytes::<T>(), indices, filter.count())
            }
            FilterSelection::Slices(_) | FilterSelection::All { .. } => {
                let filtered = filter.filter(source.as_ref())?;
                self.direct.append_array(filtered.as_bytes::<T>())
            }
        }
    }

    /// # Safety
    /// `indices` must not contain nulls, and every value must be less than
    /// `source.len()`.
    unsafe fn copy_rows_by_indices(
        &mut self,
        source: &ArrayRef,
        indices: &UInt32Array,
    ) -> Result<(), ArrowError> {
        if indices.is_empty() {
            return Ok(());
        }

        let byte_source = source.as_bytes::<T>();
        if should_use_byte_array_direct_copy(byte_source.len(), indices.len()) {
            let added = indices.values().iter().try_fold(0usize, |len, &idx| {
                let idx = idx as usize;
                let value_len = if byte_source.is_valid(idx) {
                    byte_source.value_length(idx).as_usize()
                } else {
                    0
                };
                len.checked_add(value_len)
                    .ok_or(ArrowError::OffsetOverflowError(usize::MAX))
            })?;
            Self::checked_values_len(self.direct.values_slice().len(), added)?;
            self.ensure_capacity(byte_source, indices.len());
            for &idx in indices.values() {
                let idx = idx as usize;
                if byte_source.is_null(idx) {
                    self.direct.append_null();
                } else {
                    // SAFETY: `idx` is in bounds by contract, and the row was checked non-null.
                    self.direct
                        .append_value(unsafe { byte_source.value_unchecked(idx) });
                }
            }
            return Ok(());
        }

        let is_empty = self.direct.offsets_slice().len() == 1;
        // SAFETY: the contract guarantees every index is non-null and below `source.len()`.
        let taken = unsafe { take_arrays_unchecked(std::slice::from_ref(source), indices)? }
            .into_iter()
            .next()
            .ok_or_else(|| internal_error("take returned no array"))?;
        let array = taken.as_bytes::<T>().clone();
        drop(taken);
        if is_empty {
            self.direct = Self::into_builder_with_capacity(array, self.batch_size)?;
        } else {
            self.direct.append_array(&array)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(self.direct.finish()))
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| source.get_array_memory_size())
            + self.direct.offsets_capacity() * size_of::<T::Offset>()
            + self.direct.values_capacity()
            + self.direct.validity_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::types::GenericStringType;
    use arrow_array::{BooleanArray, StringArray};

    #[test]
    fn copy_rows_and_filter_preserve_order() {
        let mut values = vec![Some("padding"); 4098];
        values[1..4].copy_from_slice(&[Some("zero"), None, Some("two")]);
        values[2048] = Some("far");
        let source: ArrayRef = Arc::new(StringArray::from(values).slice(1, 4096));
        let mask =
            BooleanArray::from_iter((0..source.len()).map(|idx| Some(matches!(idx, 0..=2 | 2047))));
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new(source.len());
        in_progress.set_source(Some(Arc::clone(&source)));
        in_progress.copy_rows(0, 1).unwrap();
        in_progress
            .copy_rows_by_filter_from(Arc::clone(&source), &predicate)
            .unwrap();
        in_progress.copy_rows(2, 1).unwrap();

        let actual = in_progress.finish().unwrap();
        let expected = StringArray::from(vec![
            Some("zero"),
            Some("zero"),
            None,
            Some("two"),
            Some("far"),
            Some("two"),
        ]);
        assert_eq!(actual.as_string::<i32>(), &expected);
    }

    #[test]
    fn checked_values_len_boundaries() {
        type Bytes = InProgressByteArray<GenericStringType<i32>>;
        let max = i32::MAX as usize;

        assert_eq!(Bytes::checked_values_len(max - 1, 1).unwrap(), max);
        assert!(Bytes::checked_values_len(max, 1).is_err());
        assert!(Bytes::checked_values_len(usize::MAX, 1).is_err());
    }

    #[test]
    fn into_builder_with_capacity_errors_for_shared_buffers() {
        let array = StringArray::from(vec!["x"]);
        assert!(InProgressByteArray::into_builder_with_capacity(array.clone(), 2).is_err());
    }
}
