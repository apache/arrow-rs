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
use crate::filter::{FilterIndices, FilterPredicate, FilterSelection};
use arrow_array::builder::GenericByteBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use std::sync::Arc;

pub(crate) struct InProgressByteArray<T: ByteArrayType> {
    source: Option<ArrayRef>,
    batch_size: usize,
    direct: GenericByteBuilder<T>,
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
}
