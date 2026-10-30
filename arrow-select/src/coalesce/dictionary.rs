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

use super::{
    DICTIONARY_DIRECT_GATHER_MIN_VALUES_PER_ROW, InProgressArray, generic::GenericInProgressArray,
};
use crate::concat::concat;
use crate::dictionary::Interner;
use crate::filter::{FilterPredicate, FilterSelection};
use crate::interleave::interleave;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, BinaryType, ByteArrayType, Int8Type, Int16Type,
    Int32Type, Int64Type, LargeBinaryType, LargeUtf8Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type, Utf8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowNativeTypeOp, DictionaryArray, GenericByteArray, PrimitiveArray,
    downcast_integer_array,
};
use arrow_buffer::{ArrowNativeType, NullBufferBuilder, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

struct SourceRun {
    start: usize,
    end: usize,
    source: usize,
}

struct DirectState<K: ArrowDictionaryKeyType> {
    values: ArrayRef,
    keys: Vec<K::Native>,
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
    /// State used by ordinary contiguous batch coalescing.
    direct: Option<DirectState<K>>,
    /// Contiguous dictionary slices retained after their mapping changes.
    buffered_arrays: Vec<ArrayRef>,
    buffered_array_bytes: usize,
    /// Keys in output row order. They are remapped in place when mappings differ.
    keys: Vec<K::Native>,
    /// Validity of `keys`.
    nulls: NullBufferBuilder,
    /// Distinct dictionary value arrays retained by pointer identity.
    sources: Vec<ArrayRef>,
    /// Contiguous key ranges and their source mapping.
    runs: Vec<SourceRun>,
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
            keys: Vec::with_capacity(batch_size),
            nulls: NullBufferBuilder::new(batch_size),
            sources: Vec::new(),
            runs: Vec::new(),
            value_type: PhantomData,
        }
    }

    fn direct_for_values(
        &mut self,
        values: ArrayRef,
    ) -> Result<Option<&mut DirectState<K>>, ArrowError> {
        if !self.buffered_arrays.is_empty() {
            return Ok(None);
        }
        if let Some(direct) = self.direct.as_ref()
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
        let keys =
            PrimitiveArray::<K>::try_new(ScalarBuffer::from(direct.keys), direct.nulls.finish())?;
        Ok(Some(DictionaryArray::try_new(keys, direct.values)?))
    }

    fn push_buffered(&mut self, array: ArrayRef) {
        self.buffered_array_bytes += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }

    fn has_contiguous_state(&self) -> bool {
        self.direct.is_some() || !self.buffered_arrays.is_empty()
    }

    fn source_id(&mut self, values: ArrayRef) -> usize {
        if let Some(last) = self.runs.last()
            && Arc::ptr_eq(&self.sources[last.source], &values)
        {
            return last.source;
        }
        if let Some(source) = self
            .sources
            .iter()
            .position(|source| Arc::ptr_eq(source, &values))
        {
            return source;
        }
        self.sources.push(values);
        self.sources.len() - 1
    }

    fn append_run(&mut self, values: ArrayRef, start: usize) {
        let source = self.source_id(values);
        let end = self.keys.len();
        if let Some(last) = self.runs.last_mut()
            && last.source == source
            && last.end == start
        {
            last.end = end;
        } else {
            self.runs.push(SourceRun { start, end, source });
        }
    }

    fn append_slice(&mut self, dictionary: &DictionaryArray<K>, offset: usize, len: usize) {
        let start = self.keys.len();
        let keys = dictionary.keys();
        if let Some(nulls) = keys.nulls().map(|nulls| nulls.slice(offset, len)) {
            self.nulls.append_buffer(&nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        }
        self.keys
            .extend_from_slice(&keys.values()[offset..offset + len]);
        self.append_run(Arc::clone(dictionary.values()), start);
    }

    fn append_index(
        keys: &PrimitiveArray<K>,
        index: usize,
        output_keys: &mut Vec<K::Native>,
        output_nulls: &mut NullBufferBuilder,
    ) -> Result<(), ArrowError> {
        let key = keys.values().get(index).copied().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Take index {index} out of bounds for array of length {}",
                keys.len()
            ))
        })?;
        output_keys.push(key);
        output_nulls.append(keys.is_valid(index));
        Ok(())
    }

    fn append_indices<I: ArrowPrimitiveType>(
        &mut self,
        dictionary: &DictionaryArray<K>,
        indices: &PrimitiveArray<I>,
    ) -> Result<(), ArrowError> {
        let start = self.keys.len();
        self.keys.reserve(indices.len());
        if indices.null_count() == 0 && dictionary.keys().null_count() == 0 {
            let keys = dictionary.keys().values();
            if let Some(index) = indices
                .values()
                .iter()
                .find(|index| index.as_usize() >= keys.len())
            {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Take index {index:?} out of bounds for array of length {}",
                    keys.len()
                )));
            }
            self.keys
                .extend(indices.values().iter().map(|index| keys[index.as_usize()]));
            self.nulls.append_n_non_nulls(indices.len());
            self.append_run(Arc::clone(dictionary.values()), start);
            return Ok(());
        }
        for position in 0..indices.len() {
            if indices.is_null(position) {
                self.keys.push(K::Native::ZERO);
                self.nulls.append(false);
                continue;
            }
            Self::append_index(
                dictionary.keys(),
                indices.value(position).as_usize(),
                &mut self.keys,
                &mut self.nulls,
            )?;
        }
        self.append_run(Arc::clone(dictionary.values()), start);
        Ok(())
    }

    fn finish_multiple_sources(
        keys: &mut [K::Native],
        nulls: Option<&arrow_buffer::NullBuffer>,
        sources: &[ArrayRef],
        runs: &[SourceRun],
    ) -> Result<ArrayRef, ArrowError> {
        let sources = sources
            .iter()
            .map(|source| {
                source
                    .as_any()
                    .downcast_ref::<GenericByteArray<T>>()
                    .expect("dictionary value type matched at construction")
            })
            .collect::<Vec<_>>();
        let mut source_rows = vec![0; sources.len()];
        for run in runs {
            source_rows[run.source] += run.end - run.start;
        }
        let mut values = Interner::new(keys.len());
        let mut local_keys = sources
            .iter()
            .zip(source_rows)
            .map(|(source, rows)| {
                (source.len() <= rows.saturating_mul(2)).then(|| vec![None; source.len()])
            })
            .collect::<Vec<_>>();
        let mut value_indices = Vec::with_capacity(keys.len());

        for run in runs {
            let source = sources[run.source];
            for (offset, key) in keys[run.start..run.end].iter_mut().enumerate() {
                let position = run.start + offset;
                if nulls.is_some_and(|nulls| nulls.is_null(position)) {
                    continue;
                }

                let old_key = key.as_usize();
                if let Some(remap) = local_keys[run.source].as_ref()
                    && let Some(new_key) = remap.get(old_key).and_then(|key| *key)
                {
                    *key = new_key;
                    continue;
                }

                if old_key >= source.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionary key {old_key} out of bounds for dictionary of length {}",
                        source.len()
                    )));
                }
                let value: Option<&[u8]> = source
                    .is_valid(old_key)
                    .then(|| source.value(old_key).as_ref());
                let new_key = *values.intern(value, || {
                    let key = K::Native::from_usize(value_indices.len())
                        .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                    value_indices.push((run.source, old_key));
                    Ok::<_, ArrowError>(key)
                })?;
                if let Some(remap) = local_keys[run.source].as_mut() {
                    remap[old_key] = Some(new_key);
                }
                *key = new_key;
            }
        }

        let source_refs = sources
            .iter()
            .map(|source| *source as &dyn Array)
            .collect::<Vec<_>>();
        interleave(&source_refs, &value_indices)
    }

    fn finish_dense(
        keys: Vec<K::Native>,
        nulls: Option<arrow_buffer::NullBuffer>,
        sources: &[ArrayRef],
        runs: &[SourceRun],
    ) -> Result<ArrayRef, ArrowError> {
        let keys = PrimitiveArray::<K>::try_new(ScalarBuffer::from(keys), nulls)?;
        let arrays = runs
            .iter()
            .map(|run| {
                DictionaryArray::try_new(
                    keys.slice(run.start, run.end - run.start),
                    Arc::clone(&sources[run.source]),
                )
                .map(|array| Arc::new(array) as ArrayRef)
            })
            .collect::<Result<Vec<_>, _>>()?;
        concat(
            &arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )
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
        if len == 0 {
            return Ok(());
        }
        let dictionary = source.as_dictionary::<K>();
        if !self.keys.is_empty() {
            self.append_slice(dictionary, offset, len);
            return Ok(());
        }

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

    fn copy_rows_by_indices_from(
        &mut self,
        source: ArrayRef,
        indices: &dyn Array,
    ) -> Result<(), ArrowError> {
        if self.has_contiguous_state() {
            let selected = crate::take::take(source.as_ref(), indices, None)?;
            self.set_source(Some(selected));
            let result = self.copy_rows(0, indices.len());
            self.set_source(None);
            return result;
        }
        let dictionary = source.as_dictionary::<K>();
        downcast_integer_array!(
            indices => self.append_indices(dictionary, indices),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Take only supported for integers, got {data_type:?}"
            )))
        )
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        if self.has_contiguous_state() {
            if self.buffered_arrays.is_empty() {
                return Ok(Arc::new(self.take_direct()?.unwrap()));
            }
            if let Some(array) = self.take_direct()? {
                self.push_buffered(Arc::new(array));
            }
            let output = concat(
                &self
                    .buffered_arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?;
            self.buffered_arrays.clear();
            self.buffered_array_bytes = 0;
            return Ok(output);
        }

        let nulls = self.nulls.finish();
        let mut keys = std::mem::replace(&mut self.keys, Vec::with_capacity(self.batch_size));
        let sources = std::mem::take(&mut self.sources);
        let runs = std::mem::take(&mut self.runs);
        if sources.len() > 1
            && sources.iter().map(|source| source.len()).sum::<usize>()
                <= keys
                    .len()
                    .saturating_mul(DICTIONARY_DIRECT_GATHER_MIN_VALUES_PER_ROW)
        {
            return Self::finish_dense(keys, nulls, &sources, &runs);
        }
        let values = match sources.len() {
            0 => Arc::new(GenericByteArray::<T>::new_null(0)) as ArrayRef,
            1 => Arc::clone(&sources[0]),
            _ => Self::finish_multiple_sources(&mut keys, nulls.as_ref(), &sources, &runs)?,
        };
        let keys = PrimitiveArray::<K>::try_new(ScalarBuffer::from(keys), nulls)?;
        Ok(Arc::new(DictionaryArray::try_new(keys, values)?))
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| source.get_array_memory_size())
            + self
                .sources
                .iter()
                .map(|source| source.get_array_memory_size())
                .sum::<usize>()
            + self.sources.capacity() * size_of::<ArrayRef>()
            + self.runs.capacity() * size_of::<SourceRun>()
            + self.keys.capacity() * size_of::<K::Native>()
            + self.nulls.allocated_size()
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
        BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray, UInt8Array, UInt32Array,
    };

    #[test]
    fn empty_copy_does_not_initialize_state() {
        let mut in_progress = InProgressDictionaryArray::<Int32Type, Utf8Type>::new(8);
        let source = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(Vec::<i32>::new()),
            Arc::new(StringArray::from(vec!["unused"])),
        ));
        in_progress.set_source(Some(source));

        in_progress.copy_rows(0, 0).unwrap();

        assert!(in_progress.keys.is_empty());
        assert!(in_progress.sources.is_empty());
    }

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

        // Different values allocations retain separate source mappings even if
        // their contents are equal.
        in_progress.set_source(Some(first));
        in_progress.copy_rows(3, 1).unwrap();
        in_progress.set_source(Some(equivalent_backing));
        in_progress.copy_rows(0, 2).unwrap();
        assert_eq!(in_progress.buffered_arrays.len(), 2);

        // A genuinely different mapping is remapped into the output dictionary.
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
    fn filters_distinct_mappings() {
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
    fn gathers_alternating_sparse_dictionaries() {
        let first_values = Arc::new(StringArray::from_iter(
            (0..64).map(|index| (index != 1).then(|| format!("a-{index}"))),
        )) as ArrayRef;
        let second_values = Arc::new(StringArray::from_iter_values(
            (0..64).map(|index| format!("b-{index}")),
        )) as ArrayRef;
        let dictionary = |values: &ArrayRef| {
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0, 1]),
                Arc::clone(values),
            )) as ArrayRef
        };
        let first = RecordBatch::try_from_iter([("dict", dictionary(&first_values))]).unwrap();
        let second = RecordBatch::try_from_iter([("dict", dictionary(&second_values))]).unwrap();
        let third = RecordBatch::try_from_iter([("dict", dictionary(&first_values))]).unwrap();
        let indices = UInt32Array::from(vec![0, 1]);
        let mut coalescer = BatchCoalescer::new(first.schema(), 6);

        for batch in [first, second, third] {
            coalescer.push_batch_with_indices(batch, &indices).unwrap();
        }

        let output = coalescer.next_completed_batch().unwrap();
        let output = output.column(0).as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| {
                let key = key?;
                values.is_valid(key).then(|| values.value(key))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![
                Some("a-0"),
                None,
                Some("b-0"),
                Some("b-1"),
                Some("a-0"),
                None
            ]
        );
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
