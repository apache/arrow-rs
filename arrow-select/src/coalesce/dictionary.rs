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
use crate::dictionary::{byte_array_ptr_eq, merge_dictionary_values_with_masks};
use ahash::RandomState;
use arrow_array::cast::AsArray;
use arrow_array::types::ArrowDictionaryKeyType;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, PrimitiveArray, downcast_integer, new_empty_array,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBufferBuilder, NullBuffer, NullBufferBuilder, ScalarBuffer,
};
use arrow_schema::{ArrowError, DataType};
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

struct KeyChunk<K: ArrowDictionaryKeyType> {
    /// Zero-copy slice of source-local keys for the retained rows.
    keys: ScalarBuffer<K::Native>,
    /// Shared validity buffer from the source keys, if present.
    nulls: Option<NullBuffer>,
    /// Index of the values array in `InProgressDictionaryArray::sources`.
    source: usize,
}

pub(crate) fn create_in_progress_dictionary(
    key_type: &DataType,
    value_type: &DataType,
) -> Box<dyn InProgressArray> {
    macro_rules! create {
        ($key:ty, $value_type:ident) => {
            Box::new(InProgressDictionaryArray::<$key>::new($value_type))
        };
    }

    match value_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            downcast_integer! {
                key_type => (create, value_type),
                _ => unreachable!("illegal dictionary key type {key_type}"),
            }
        }
        _ => Box::new(GenericInProgressArray::new()),
    }
}

struct InProgressDictionaryArray<K: ArrowDictionaryKeyType> {
    /// Dictionary currently selected by [`InProgressArray::set_source`].
    source: Option<ArrayRef>,
    /// Number of output rows retained across `chunks`.
    len: usize,
    /// Typed empty values array used when finishing without any rows.
    empty_values: ArrayRef,
    /// Distinct dictionary values arrays retained by buffer identity.
    sources: Vec<ArrayRef>,
    /// Maps each retained values `Arc` address to its index in `sources`.
    source_ids: HashMap<usize, usize, RandomState>,
    /// Zero-copy key slices in output order, tagged with their values source.
    chunks: Vec<KeyChunk<K>>,
}

impl<K: ArrowDictionaryKeyType> std::fmt::Debug for InProgressDictionaryArray<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressDictionaryArray").finish()
    }
}

impl<K: ArrowDictionaryKeyType> InProgressDictionaryArray<K> {
    fn new(value_type: &DataType) -> Self {
        Self {
            source: None,
            len: 0,
            empty_values: new_empty_array(value_type),
            sources: Vec::new(),
            source_ids: HashMap::with_hasher(RandomState::with_seeds(0, 0, 0, 0)),
            chunks: Vec::new(),
        }
    }

    /// Returns a stable index for `values`, reusing arrays backed by the same buffers.
    fn source_id(&mut self, values: ArrayRef) -> usize {
        if let Some(last) = self.chunks.last()
            && (Arc::ptr_eq(&self.sources[last.source], &values)
                || byte_array_ptr_eq(self.sources[last.source].as_ref(), values.as_ref()))
        {
            return last.source;
        }
        let values_id = Arc::as_ptr(&values).cast::<()>() as usize;
        if let Some(&source) = self.source_ids.get(&values_id) {
            return source;
        }
        if let Some(source) = self
            .sources
            .iter()
            .position(|existing| byte_array_ptr_eq(existing.as_ref(), values.as_ref()))
        {
            return source;
        }
        let source = self.sources.len();
        self.sources.push(values);
        self.source_ids.insert(values_id, source);
        source
    }

    /// Compacts referenced values and remaps source-local keys while preserving chunk order.
    /// Equal referenced values are only deduplicated when their count would overflow
    /// `K`'s key space.
    fn merge_sources(
        sources: &[ArrayRef],
        chunks: &[KeyChunk<K>],
        len: usize,
    ) -> Result<(Vec<K::Native>, ArrayRef), ArrowError> {
        let mut referenced = sources
            .iter()
            .map(|source| {
                let mut mask = BooleanBufferBuilder::new(source.len());
                mask.advance(source.len());
                mask
            })
            .collect::<Vec<_>>();

        for chunk in chunks {
            let mask = &mut referenced[chunk.source];
            match &chunk.nulls {
                Some(nulls) => nulls
                    .inner()
                    .set_indices()
                    .for_each(|offset| mask.set_bit(chunk.keys[offset].as_usize(), true)),
                None => chunk
                    .keys
                    .iter()
                    .for_each(|key| mask.set_bit(key.as_usize(), true)),
            }
        }

        let referenced = referenced
            .into_iter()
            .map(BooleanBufferBuilder::build)
            .collect::<Vec<_>>();
        let source_refs = sources
            .iter()
            .map(|source| source.as_ref() as &dyn Array)
            .collect::<Vec<_>>();
        let merged = merge_dictionary_values_with_masks::<K>(&source_refs, &referenced)?;

        let mut keys = Vec::with_capacity(len);
        for chunk in chunks {
            let mapping = &merged.key_mappings[chunk.source];
            match &chunk.nulls {
                Some(nulls) => keys.extend(chunk.keys.iter().enumerate().map(|(offset, key)| {
                    if nulls.is_valid(offset) {
                        mapping[key.as_usize()]
                    } else {
                        K::Native::default()
                    }
                })),
                None => keys.extend(chunk.keys.iter().map(|key| mapping[key.as_usize()])),
            }
        }
        Ok((keys, merged.values))
    }

    /// Concatenates chunk validity in output order, omitting it when all keys are valid.
    fn build_nulls(chunks: &[KeyChunk<K>], len: usize) -> Option<NullBuffer> {
        let mut nulls = NullBufferBuilder::new(len);
        for chunk in chunks {
            match &chunk.nulls {
                Some(chunk_nulls) => nulls.append_buffer(chunk_nulls),
                None => nulls.append_n_non_nulls(chunk.keys.len()),
            }
        }
        nulls.finish()
    }
}

impl<K: ArrowDictionaryKeyType> InProgressArray for InProgressDictionaryArray<K> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        if len == 0 {
            return Ok(());
        }
        let source = Arc::clone(self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressDictionaryArray: source not set".to_string(),
            )
        })?);
        let dictionary = source.as_dictionary::<K>();
        let keys = dictionary.keys();
        let source = self.source_id(Arc::clone(dictionary.values()));
        self.chunks.push(KeyChunk {
            keys: keys.values().slice(offset, len),
            nulls: keys.nulls().map(|nulls| nulls.slice(offset, len)),
            source,
        });
        self.len += len;
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let sources = std::mem::take(&mut self.sources);
        let chunks = std::mem::take(&mut self.chunks);
        let len = std::mem::take(&mut self.len);
        self.source_ids.clear();

        let (keys, values) = match sources.len() {
            0 => (Vec::new(), Arc::clone(&self.empty_values)),
            1 => (
                chunks
                    .iter()
                    .flat_map(|chunk| chunk.keys.iter().copied())
                    .collect(),
                Arc::clone(&sources[0]),
            ),
            _ => Self::merge_sources(&sources, &chunks, len)?,
        };
        let nulls = Self::build_nulls(&chunks, len);
        let keys = PrimitiveArray::<K>::try_new(ScalarBuffer::from(keys), nulls)?;
        Ok(Arc::new(DictionaryArray::try_new(keys, values)?))
    }

    /// Estimates memory retained by the source, value arrays, key slices, and bookkeeping.
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
            + self.source_ids.capacity() * size_of::<(usize, usize)>()
            + self.chunks.capacity() * size_of::<KeyChunk<K>>()
            + self
                .chunks
                .iter()
                .map(|chunk| {
                    chunk.keys.inner().capacity()
                        + chunk
                            .nulls
                            .as_ref()
                            .map_or(0, |nulls| nulls.inner().inner().capacity())
                })
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce::BatchCoalescer;
    use arrow_array::types::{Int8Type, Int32Type};
    use arrow_array::{Int8Array, Int32Array, RecordBatch, StringArray};

    fn batch(values: &ArrayRef, keys: Vec<Option<i32>>) -> RecordBatch {
        let dictionary = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys),
            Arc::clone(values),
        )) as ArrayRef;
        RecordBatch::try_from_iter([("dict", dictionary)]).unwrap()
    }

    #[test]
    fn coalesces_repeated_and_distinct_values() {
        let first_values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let second_values = Arc::new(StringArray::from(vec!["b", "c"])) as ArrayRef;
        let batches = [
            batch(&first_values, vec![Some(0), None]),
            batch(&second_values, vec![Some(0), Some(1)]),
            batch(&first_values, vec![Some(1), Some(0)]),
        ];
        let mut coalescer = BatchCoalescer::new(batches[0].schema(), 6);
        for batch in batches {
            coalescer.push_batch(batch).unwrap();
        }

        let output = coalescer.next_completed_batch().unwrap();
        let output = output.column(0).as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![Some("a"), None, Some("b"), Some("c"), Some("b"), Some("a")]
        );
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn reuses_one_values_array() {
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let mut coalescer = BatchCoalescer::new(batch(&values, vec![Some(0)]).schema(), 2);
        coalescer.push_batch(batch(&values, vec![Some(0)])).unwrap();
        coalescer.push_batch(batch(&values, vec![Some(1)])).unwrap();

        let output = coalescer.next_completed_batch().unwrap();
        let output = output.column(0).as_dictionary::<Int32Type>();
        assert!(Arc::ptr_eq(output.values(), &values));
    }

    #[test]
    fn reuses_values_with_shared_buffers() {
        let values = StringArray::from_iter_values((0..128).map(|i| format!("value-{i}")));
        let first_values = Arc::new(values.clone()) as ArrayRef;
        let second_values = Arc::new(values) as ArrayRef;
        let make_batch = |values| {
            let dictionary =
                DictionaryArray::<Int8Type>::new(Int8Array::from_iter_values(0..=127), values);
            RecordBatch::try_from_iter([("dict", Arc::new(dictionary) as ArrayRef)]).unwrap()
        };

        let first = make_batch(Arc::clone(&first_values));
        let mut coalescer = BatchCoalescer::new(first.schema(), 256);
        coalescer.push_batch(first).unwrap();
        coalescer.push_batch(make_batch(second_values)).unwrap();

        let output = coalescer.next_completed_batch().unwrap();
        let output = output.column(0).as_dictionary::<Int8Type>();
        assert!(Arc::ptr_eq(output.values(), &first_values));
        assert_eq!(
            output.keys().values(),
            &(0..=127).chain(0..=127).collect::<Vec<_>>()
        );
    }

    #[test]
    fn remaps_sparse_values_across_sources() {
        let first = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from_iter_values([1, 2, 0, 2, 1]),
            Arc::new(StringArray::from(vec!["a", "unused-a", "b"])),
        )) as ArrayRef;
        let second = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from_iter_values([2, 1, 0, 1, 2]),
            Arc::new(StringArray::from(vec!["b", "c", "unused-b"])),
        )) as ArrayRef;

        let mut in_progress = InProgressDictionaryArray::<Int32Type>::new(&DataType::Utf8);
        in_progress.set_source(Some(first));
        in_progress.copy_rows(1, 3).unwrap();
        in_progress.set_source(Some(second));
        in_progress.copy_rows(1, 3).unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        assert_eq!(output.keys().values(), &[1, 0, 1, 3, 2, 3]);
        assert_eq!(
            output
                .values()
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), Some("b"), Some("c")]
        );
    }

    #[test]
    fn remaps_keys_when_deduplication_is_required() {
        let make_values = || {
            Arc::new(StringArray::from_iter_values(
                (0..70).map(|idx| format!("value-{idx}")),
            )) as ArrayRef
        };
        let first_values = make_values();
        let second_values = make_values();
        let make_dictionary = |values| {
            Arc::new(DictionaryArray::<Int8Type>::new(
                Int8Array::from_iter_values(0..70),
                values,
            )) as ArrayRef
        };

        let mut in_progress = InProgressDictionaryArray::<Int8Type>::new(&DataType::Utf8);
        in_progress.set_source(Some(make_dictionary(first_values)));
        in_progress.copy_rows(0, 70).unwrap();
        in_progress.set_source(Some(make_dictionary(second_values)));
        in_progress.copy_rows(0, 70).unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int8Type>();
        let values = output.values().as_string::<i32>();
        for (idx, key) in output.keys().values().iter().enumerate() {
            assert_eq!(values.value(*key as usize), format!("value-{}", idx % 70));
        }
        assert!(values.len() <= i8::MAX as usize + 1);
    }
}
