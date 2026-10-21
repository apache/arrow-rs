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

use super::{InProgressArray, should_use_byte_array_direct_copy};
use crate::concat::concat;
use crate::filter::{FilterIndices, FilterPredicate, FilterSelection};
use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_buffer::{
    ArrowNativeType, Buffer, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::ArrowError;
use std::marker::PhantomData;
use std::sync::Arc;

struct DirectState<O> {
    /// In-progress nulls
    nulls: NullBufferBuilder,
    /// In-progress offsets
    offsets: Vec<O>,
    /// In-progress values
    values: Vec<u8>,
}

impl<O: ArrowNativeType> DirectState<O> {
    fn new(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows.saturating_add(1));
        offsets.push(O::usize_as(0));
        Self {
            nulls: NullBufferBuilder::new(rows),
            offsets,
            values: Vec::with_capacity(bytes),
        }
    }
}

/// InProgressArray for [`GenericByteArray`]
pub(crate) struct InProgressByteArray<T: ByteArrayType> {
    /// The current source
    source: Option<ArrayRef>,
    /// Is [`Self::source`] referenced in [`Self::buffered_arrays`]
    source_data_referenced_in_buffers: bool,
    /// The buffered array slices
    buffered_arrays: Vec<ArrayRef>,
    /// The number of bytes the arrays in [`Self::buffered_arrays`] takes
    total_size_of_non_shared_buffers: usize,
    /// The in-progress directly copied values
    direct: Option<Box<DirectState<T::Offset>>>,
    /// The byte array type
    _phantom: PhantomData<T>,
}

impl<T: ByteArrayType> std::fmt::Debug for InProgressByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressByteArray")
            .field("buffered_arrays", &self.buffered_arrays.len())
            .field(
                "direct_rows",
                &self
                    .direct
                    .as_ref()
                    .map_or(0, |state| state.offsets.len().saturating_sub(1)),
            )
            .finish()
    }
}

impl<T: ByteArrayType> InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
    pub(crate) fn new() -> Self {
        Self {
            source: None,
            source_data_referenced_in_buffers: false,
            buffered_arrays: Vec::new(),
            total_size_of_non_shared_buffers: 0,
            direct: None,
            _phantom: PhantomData,
        }
    }

    fn ensure_direct(&mut self, source: &GenericByteArray<T>, rows: usize) {
        self.direct.get_or_insert_with(|| {
            let offsets = source.value_offsets();
            let bytes = if source.is_empty() {
                0
            } else {
                let source_bytes = offsets[source.len()].as_usize() - offsets[0].as_usize();
                source_bytes.saturating_mul(rows) / source.len()
            };
            Box::new(DirectState::new(rows, bytes))
        });
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

    fn flush_direct(&mut self) {
        let Some(mut state) = self.direct.take() else {
            return;
        };
        if state.offsets.len() == 1 {
            return;
        }

        let array = GenericByteArray::<T>::new(
            OffsetBuffer::new(ScalarBuffer::from(state.offsets)),
            Buffer::from_vec(state.values),
            state.nulls.finish(),
        );
        let array: ArrayRef = Arc::new(array);
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }

    fn append_filter_nulls(
        nulls: &mut NullBufferBuilder,
        source_nulls: Option<&NullBuffer>,
        filter: &FilterPredicate,
    ) {
        if let Some(filtered) = filter.filter_nulls(source_nulls) {
            nulls.append_buffer(&filtered);
        } else {
            nulls.append_n_non_nulls(filter.count());
        }
    }

    fn append_filter_indices(
        &mut self,
        source: &GenericByteArray<T>,
        filter: &FilterPredicate,
        indices: FilterIndices<'_>,
    ) -> Result<(), ArrowError> {
        self.ensure_direct(source, filter.count());
        let state = self.direct.as_mut().expect("direct state initialized");
        Self::append_filter_nulls(&mut state.nulls, source.nulls(), filter);
        state.offsets.reserve(filter.count());

        let source_offsets = source.value_offsets();
        let source_values = source.value_data();
        indices.try_for_each(|idx| {
            let start = source_offsets[idx].as_usize();
            let end = source_offsets[idx + 1].as_usize();
            let new_len = Self::checked_values_len(state.values.len(), end - start)?;
            state.values.reserve(new_len - state.values.len());
            state.values.extend_from_slice(&source_values[start..end]);
            state.offsets.push(
                T::Offset::from_usize(state.values.len())
                    .ok_or(ArrowError::OffsetOverflowError(state.values.len()))?,
            );
            Ok(())
        })
    }

    fn push_materialized(&mut self, array: ArrayRef) {
        self.flush_direct();
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }

    fn empty_array() -> ArrayRef {
        Arc::new(GenericByteArray::<T>::new(
            OffsetBuffer::new(ScalarBuffer::from(vec![T::Offset::usize_as(0)])),
            Buffer::from_vec(Vec::<u8>::new()),
            None,
        ))
    }
}

impl<T: ByteArrayType + 'static> InProgressArray for InProgressByteArray<T>
where
    T::Offset: OffsetSizeTrait,
{
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
        self.flush_direct();
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressByteArray: source not set".to_string(),
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
        if !should_use_byte_array_direct_copy(filter.filter_len(), filter.count()) {
            let filtered = filter.filter(source.as_ref())?;
            self.push_materialized(filtered);
            return Ok(());
        }

        let source_bytes = source.as_bytes::<T>();
        match filter.selection() {
            FilterSelection::None => Ok(()),
            FilterSelection::Indices(indices) => {
                self.append_filter_indices(source_bytes, filter, indices)
            }
            FilterSelection::Slices(_) | FilterSelection::All { .. } => {
                let filtered = filter.filter(source.as_ref())?;
                self.push_materialized(filtered);
                Ok(())
            }
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.flush_direct();
        let result = if self.buffered_arrays.is_empty() {
            Self::empty_array()
        } else {
            concat(
                &self
                    .buffered_arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?
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
            + self.direct.as_ref().map_or(0, |state| {
                state.offsets.capacity() * size_of::<T::Offset>()
                    + state.values.capacity()
                    + state.nulls.allocated_size()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce::generic::GenericInProgressArray;
    use crate::filter::FilterBuilder;
    use arrow_array::types::{GenericBinaryType, GenericStringType};
    use arrow_array::{BooleanArray, StringArray};
    use arrow_schema::DataType;

    #[test]
    fn copy_rows_and_filter_preserve_order() {
        let mut values = vec![Some("padding"); 4096];
        values[..4].copy_from_slice(&[Some("zero"), None, Some("two"), Some("three")]);
        let source: ArrayRef = Arc::new(StringArray::from(values));
        let mask = BooleanArray::from_iter((0..source.len()).map(|idx| Some(idx < 4)));
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();
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
            Some("three"),
            Some("two"),
        ]);
        assert_eq!(actual.as_string::<i32>(), &expected);
    }

    #[test]
    fn copy_rows_by_filter_preserves_null_slot_bytes() {
        let offsets = (0..=2048).map(|offset| offset * 4).collect::<Vec<i32>>();
        let source: ArrayRef = Arc::new(StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(b"null".repeat(2048)),
            Some(NullBuffer::from_iter(
                std::iter::once(false).chain(std::iter::repeat_n(true, 2047)),
            )),
        ));
        let mask = BooleanArray::from_iter((0..source.len()).map(|idx| Some(idx < 2)));
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let expected = predicate.filter(source.as_ref()).unwrap();

        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let actual = in_progress.finish().unwrap();

        assert_eq!(actual.to_data(), expected.to_data());
    }

    #[test]
    fn copy_rows_by_filter_matches_filter_kernel() {
        let mut strings = vec![Some("padding"); 2048];
        strings[..4].copy_from_slice(&[Some("zero"), None, Some("two"), Some("three")]);

        for selected in [[0, 1], [0, 200]] {
            let mask = BooleanArray::from_iter(
                (0..strings.len()).map(|idx| Some(idx == selected[0] || idx == selected[1])),
            );
            let predicate = FilterBuilder::new(&mask).optimize().build();
            let source: ArrayRef = Arc::new(StringArray::from(strings.clone()));
            let expected = predicate.filter(source.as_ref()).unwrap();
            let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();
            in_progress
                .copy_rows_by_filter_from(source, &predicate)
                .unwrap();
            let actual = in_progress.finish().unwrap();
            assert_eq!(actual.as_ref(), expected.as_ref());
        }
    }

    #[test]
    fn copy_rows_by_filter_rebases_sliced_array_offsets() {
        let mut strings = vec![Some("padding"); 2050];
        strings[1] = Some("first");
        strings[2048] = Some("last");
        let source: ArrayRef = Arc::new(StringArray::from(strings));
        let source = source.slice(1, source.len() - 2);
        let mask = BooleanArray::from_iter(
            (0..source.len()).map(|idx| Some(idx == 0 || idx + 1 == source.len())),
        );
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let expected = predicate.filter(source.as_ref()).unwrap();

        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let actual = in_progress.finish().unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    #[test]
    fn checked_values_len_boundaries() {
        type Bytes = InProgressByteArray<GenericStringType<i32>>;
        let max = i32::MAX as usize;

        assert_eq!(Bytes::checked_values_len(max - 1, 1).unwrap(), max);
        assert!(matches!(
            Bytes::checked_values_len(max, 1),
            Err(ArrowError::OffsetOverflowError(value)) if value == max + 1
        ));
        assert!(matches!(
            Bytes::checked_values_len(usize::MAX, 1),
            Err(ArrowError::OffsetOverflowError(value)) if value == usize::MAX
        ));
    }

    #[test]
    fn filter_above_direct_threshold_uses_materialized_path() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("value"); 2048]));
        let mask = BooleanArray::from_iter((0..source.len()).map(|idx| Some(idx < 3)));
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let expected = predicate.filter(source.as_ref()).unwrap();

        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();

        assert!(in_progress.direct.is_none());
        assert_eq!(in_progress.finish().unwrap().as_ref(), expected.as_ref());
    }

    #[test]
    fn finish_empty_preserves_type() {
        let mut strings = InProgressByteArray::<GenericStringType<i64>>::new();
        assert_eq!(strings.finish().unwrap().data_type(), &DataType::LargeUtf8);

        let mut binary = InProgressByteArray::<GenericBinaryType<i64>>::new();
        assert_eq!(binary.finish().unwrap().data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn size_tracks_shared_source_memory() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("value"); 512]));
        let source_size = source.get_array_memory_size();
        let mut in_progress = InProgressByteArray::<GenericStringType<i32>>::new();

        in_progress.set_source(Some(source));
        assert_eq!(in_progress.size(), source_size);
        in_progress.copy_rows(0, 16).unwrap();
        let size_before_clear = in_progress.size();
        in_progress.set_source(None);
        assert_eq!(in_progress.size(), size_before_clear);

        in_progress.finish().unwrap();
        assert!(in_progress.size() < source_size);
    }

    #[test]
    fn direct_state_stays_out_of_the_base_struct() {
        let byte_size = size_of::<InProgressByteArray<GenericStringType<i32>>>();
        let generic_size = size_of::<GenericInProgressArray>();
        assert!(byte_size <= generic_size + size_of::<Option<Box<DirectState<i32>>>>());
    }
}
