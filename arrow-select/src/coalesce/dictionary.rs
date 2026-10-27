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

use super::{InProgressArray, generic::GenericInProgressArray};
use crate::concat::concat;
use crate::filter::{FilterPredicate, FilterSelection};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, BinaryType, ByteArrayType, Int8Type, Int16Type, Int32Type, Int64Type,
    LargeBinaryType, LargeUtf8Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, Utf8Type,
};
use arrow_array::{Array, ArrayRef, DictionaryArray, GenericByteArray, PrimitiveArray};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

struct DirectState<K: ArrowDictionaryKeyType> {
    /// Shared dictionary values for the buffered keys.
    values: ArrayRef,
    /// Keys accumulated without rebuilding dictionary values.
    keys: Vec<K::Native>,
    /// Validity of `keys`.
    nulls: NullBufferBuilder,
}

fn create<K: ArrowDictionaryKeyType>(
    value_type: &DataType,
    batch_size: usize,
) -> Box<dyn InProgressArray> {
    match value_type {
        DataType::Utf8 => Box::new(InProgressDictionaryArray::<K, Utf8Type>::new(batch_size)),
        DataType::LargeUtf8 => Box::new(InProgressDictionaryArray::<K, LargeUtf8Type>::new(
            batch_size,
        )),
        DataType::Binary => Box::new(InProgressDictionaryArray::<K, BinaryType>::new(batch_size)),
        DataType::LargeBinary => Box::new(InProgressDictionaryArray::<K, LargeBinaryType>::new(
            batch_size,
        )),
        _ => Box::new(GenericInProgressArray::new()),
    }
}

pub(crate) fn create_in_progress_dictionary(
    key_type: &DataType,
    value_type: &DataType,
    batch_size: usize,
) -> Box<dyn InProgressArray> {
    match key_type {
        DataType::Int8 => create::<Int8Type>(value_type, batch_size),
        DataType::Int16 => create::<Int16Type>(value_type, batch_size),
        DataType::Int32 => create::<Int32Type>(value_type, batch_size),
        DataType::Int64 => create::<Int64Type>(value_type, batch_size),
        DataType::UInt8 => create::<UInt8Type>(value_type, batch_size),
        DataType::UInt16 => create::<UInt16Type>(value_type, batch_size),
        DataType::UInt32 => create::<UInt32Type>(value_type, batch_size),
        DataType::UInt64 => create::<UInt64Type>(value_type, batch_size),
        _ => unreachable!("illegal dictionary key type {key_type}"),
    }
}

/// Coalesces dictionaries with byte-array values without rebuilding shared mappings.
pub(crate) struct InProgressDictionaryArray<K: ArrowDictionaryKeyType, T: ByteArrayType> {
    /// Current source used by [`Self::copy_rows`].
    source: Option<ArrayRef>,
    /// Target output row count.
    batch_size: usize,
    /// Keys buffered while inputs share one dictionary values array.
    direct: Option<DirectState<K>>,
    /// Array slices retained after the dictionary values mapping changes.
    buffered_arrays: Vec<ArrayRef>,
    /// Upper bound for the bytes retained by `buffered_arrays`.
    buffered_array_bytes: usize,
    /// Retains the byte-array value type in the state type.
    value_type: PhantomData<T>,
}

impl<K: ArrowDictionaryKeyType, T: ByteArrayType> std::fmt::Debug
    for InProgressDictionaryArray<K, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressDictionaryArray").finish()
    }
}

impl<K: ArrowDictionaryKeyType, T: ByteArrayType> InProgressDictionaryArray<K, T> {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            direct: None,
            buffered_arrays: Vec::new(),
            buffered_array_bytes: 0,
            value_type: PhantomData,
        }
    }

    /// Returns the direct state while `values` is the same allocation as the
    /// buffered mapping. Keys cannot be reused across mappings, so a different
    /// `Arc` switches the current output batch to buffered `concat`.
    fn direct_for_values(
        &mut self,
        values: ArrayRef,
    ) -> Result<Option<&mut DirectState<K>>, ArrowError> {
        if !self.buffered_arrays.is_empty() {
            return Ok(None);
        }
        if let Some(direct) = self.direct.as_ref()
            && !direct.keys.is_empty()
            && !Arc::ptr_eq(&direct.values, &values)
        {
            let array = Arc::new(self.take_direct()?.unwrap()) as ArrayRef;
            self.push_buffered(array);
            return Ok(None);
        }
        if !self
            .direct
            .as_ref()
            .is_some_and(|direct| Arc::ptr_eq(&direct.values, &values))
        {
            self.direct = Some(DirectState {
                values,
                keys: Vec::with_capacity(self.batch_size),
                nulls: NullBufferBuilder::new(self.batch_size),
            });
        }
        Ok(self.direct.as_mut())
    }

    fn take_direct(&mut self) -> Result<Option<DictionaryArray<K>>, ArrowError> {
        let Some(mut direct) = self.direct.take() else {
            return Ok(None);
        };
        if direct.keys.is_empty() {
            return Ok(None);
        }
        let nulls = direct.nulls.finish();
        let keys = PrimitiveArray::<K>::try_new(ScalarBuffer::from(direct.keys), nulls)?;
        Ok(Some(DictionaryArray::try_new(keys, direct.values)?))
    }

    fn push_buffered(&mut self, array: ArrayRef) {
        self.buffered_array_bytes += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }
}

impl<K: ArrowDictionaryKeyType, T: ByteArrayType> InProgressArray
    for InProgressDictionaryArray<K, T>
{
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = Arc::clone(self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressDictionaryArray: source not set".to_string(),
            )
        })?);
        let dictionary = source.as_dictionary::<K>();
        let values = Arc::clone(dictionary.values());
        let keys = dictionary.keys();

        let Some(direct) = self.direct_for_values(values)? else {
            self.push_buffered(source.slice(offset, len));
            return Ok(());
        };
        if let Some(nulls) = keys.nulls().map(|nulls| nulls.slice(offset, len)) {
            direct.nulls.append_buffer(&nulls);
        } else {
            direct.nulls.append_n_non_nulls(len);
        }
        direct
            .keys
            .extend_from_slice(&keys.values()[offset..offset + len]);
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let dictionary = source.as_dictionary::<K>();
        dictionary
            .downcast_dict::<GenericByteArray<T>>()
            .expect("dictionary value type matched at construction");
        let values = Arc::clone(dictionary.values());
        let keys = dictionary.keys();
        if filter.count() == 0 {
            return Ok(());
        }
        // Sharing the values `Arc` reduces filtering to copying fixed-width keys.
        // A different mapping needs `concat` to remap those keys safely.
        let Some(direct) = self.direct_for_values(values)? else {
            self.push_buffered(filter.filter(source.as_ref())?);
            return Ok(());
        };
        if let Some(nulls) = filter.filter_nulls(keys.nulls()) {
            direct.nulls.append_buffer(&nulls);
        } else {
            direct.nulls.append_n_non_nulls(filter.count());
        }
        direct.keys.reserve(filter.count());

        match filter.selection() {
            FilterSelection::None => {}
            FilterSelection::All { len } => {
                direct.keys.extend_from_slice(&keys.values()[..len]);
            }
            FilterSelection::Slices(slices) => {
                slices.for_each(|(start, end)| {
                    direct.keys.extend_from_slice(&keys.values()[start..end]);
                });
            }
            FilterSelection::Indices(indices) => {
                indices.for_each(|index| direct.keys.push(keys.values()[index]));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.buffered_arrays.is_empty() {
            return match self.take_direct()? {
                Some(array) => Ok(Arc::new(array)),
                None => Ok(Arc::new(DictionaryArray::<K>::try_new(
                    PrimitiveArray::new_null(0),
                    Arc::new(GenericByteArray::<T>::new_null(0)),
                )?)),
            };
        }
        if let Some(array) = self.take_direct()? {
            self.push_buffered(Arc::new(array));
        }
        let arrays = self
            .buffered_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        let output = concat(&arrays)?;
        self.buffered_arrays.clear();
        self.buffered_array_bytes = 0;
        Ok(output)
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| source.get_array_memory_size())
            + self.buffered_array_bytes
            + self.buffered_arrays.capacity() * size_of::<ArrayRef>()
            + self.direct.as_ref().map_or(0, |direct| {
                direct.values.get_array_memory_size()
                    + direct.keys.capacity() * size_of::<K::Native>()
                    + direct.nulls.allocated_size()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce::BatchCoalescer;
    use crate::filter::FilterBuilder;
    use arrow_array::types::{Int32Type, UInt8Type};
    use arrow_array::{
        BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray, UInt8Array,
    };

    #[test]
    fn coalesces_shared_and_distinct_mappings() {
        let shared_values =
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef;
        let different_values = Arc::new(StringArray::from(vec!["b", "c"])) as ArrayRef;
        let first = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(2)]),
            Arc::clone(&shared_values),
        )) as ArrayRef;
        let same_backing = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![2, 0]),
            Arc::clone(&shared_values),
        )) as ArrayRef;
        let equivalent_backing = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 2]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        )) as ArrayRef;
        let different_backing = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 1]),
            different_values,
        )) as ArrayRef;

        // Pointer-identical mappings share one key space, so filtered and
        // unfiltered inputs can both append keys directly.
        let mut in_progress = InProgressDictionaryArray::<Int32Type, Utf8Type>::new(8);
        let filter = BooleanArray::from(vec![true, true, true, false]);
        in_progress
            .copy_rows_by_filter_from(Arc::clone(&first), &FilterBuilder::new(&filter).build())
            .unwrap();
        in_progress.set_source(Some(same_backing));
        in_progress.copy_rows(0, 2).unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        assert!(Arc::ptr_eq(output.values(), &shared_values));
        assert_eq!(output.keys().values(), &[0, 0, 1, 2, 0]);
        assert_eq!(output.keys().null_count(), 1);
        assert!(output.keys().is_null(1));

        // A different values allocation uses the concat fallback even if its
        // contents are equal.
        in_progress.set_source(Some(first));
        in_progress.copy_rows(3, 1).unwrap();
        in_progress.set_source(Some(equivalent_backing));
        in_progress.copy_rows(0, 2).unwrap();
        assert_eq!(in_progress.buffered_arrays.len(), 2);

        // A genuinely different mapping takes the same fallback and is remapped
        // into the output dictionary.
        in_progress.set_source(Some(different_backing));
        in_progress.copy_rows(0, 2).unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![Some("b"), Some("a"), Some("b"), Some("b"), Some("c")]
        );
    }

    #[test]
    fn filters_distinct_mappings_through_concat_fallback() {
        let filter = BooleanArray::from(vec![true, false, true]);
        let filter = FilterBuilder::new(&filter).build();
        let mut in_progress = create_in_progress_dictionary(&DataType::Int32, &DataType::Utf8, 4);

        // The raw keys are identical, but key 0 decodes differently in each input.
        for values in [vec!["a", "b"], vec!["b", "a"]] {
            let source = Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(values)),
            ));
            in_progress
                .copy_rows_by_filter_from(source, &filter)
                .unwrap();
        }

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![Some("a"), Some("a"), Some("b"), Some("b")]);
    }

    #[test]
    fn supports_binary_values_and_tracks_memory() {
        let mut in_progress =
            create_in_progress_dictionary(&DataType::UInt8, &DataType::Binary, 8192);
        assert_eq!(
            in_progress.finish().unwrap().data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Binary))
        );

        let values = Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"b"]));
        let source = Arc::new(DictionaryArray::<UInt8Type>::new(
            UInt8Array::from(vec![0, 1]),
            values,
        ));
        in_progress.set_source(Some(source));
        in_progress.copy_rows(0, 2).unwrap();
        let buffered_size = in_progress.size();
        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<UInt8Type>();
        assert_eq!(output.keys(), &UInt8Array::from(vec![0, 1]));
        assert_eq!(
            output.values().as_binary::<i32>(),
            &BinaryArray::from(vec![Some(b"a".as_slice()), Some(b"b")])
        );
        assert!(in_progress.size() < buffered_size);
    }

    #[test]
    fn releases_values_after_completed_batch_is_dropped() {
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let weak_values = Arc::downgrade(&values);
        let dictionary = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 1]),
            Arc::clone(&values),
        )) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("dict", dictionary)]).unwrap();
        let mut coalescer = BatchCoalescer::new(batch.schema(), batch.num_rows());

        drop(values);
        coalescer.push_batch(batch).unwrap();
        let output = coalescer.next_completed_batch().unwrap();
        assert!(weak_values.upgrade().is_some());

        drop(output);
        assert!(weak_values.upgrade().is_none());
    }
}
