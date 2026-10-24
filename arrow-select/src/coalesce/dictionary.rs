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
use crate::concat::concat;
use crate::filter::{FilterPredicate, FilterSelection};
use crate::interleave::interleave;
use ahash::RandomState;
use arrow_array::builder::{ArrayBuilder, PrimitiveBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::ArrowDictionaryKeyType;
use arrow_array::{Array, ArrayRef, DictionaryArray, new_empty_array};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};
use std::collections::HashMap;
use std::sync::Arc;

const LOW_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR: usize = 128;
const HIGH_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR: usize = 64;
/// Linear lookup avoids hash-table setup for the smallest sparse selections.
const LINEAR_KEY_REMAP_MAX_LEN: usize = 32;

enum KeyRemap<V> {
    Linear(Vec<(usize, V)>),
    Hashed(HashMap<usize, V, RandomState>),
}

impl<V: Copy> KeyRemap<V> {
    fn with_capacity(capacity: usize) -> Self {
        if capacity <= LINEAR_KEY_REMAP_MAX_LEN {
            Self::Linear(Vec::with_capacity(capacity))
        } else {
            Self::Hashed(HashMap::with_capacity_and_hasher(
                capacity,
                RandomState::with_seeds(0, 0, 0, 0),
            ))
        }
    }

    fn get(&self, old_key: usize) -> Option<V> {
        match self {
            Self::Linear(remap) => remap
                .iter()
                .find_map(|(key, remapped)| (*key == old_key).then_some(*remapped)),
            Self::Hashed(remap) => remap.get(&old_key).copied(),
        }
    }

    fn insert(&mut self, old_key: usize, new_key: V) {
        match self {
            Self::Linear(remap) if remap.len() < LINEAR_KEY_REMAP_MAX_LEN => {
                remap.push((old_key, new_key));
            }
            Self::Linear(remap) => {
                let mut hashed = HashMap::with_capacity_and_hasher(
                    remap.len() + 1,
                    RandomState::with_seeds(0, 0, 0, 0),
                );
                hashed.extend(remap.drain(..));
                hashed.insert(old_key, new_key);
                *self = Self::Hashed(hashed);
            }
            Self::Hashed(remap) => {
                remap.insert(old_key, new_key);
            }
        }
    }

    fn reset(&mut self, capacity: usize) {
        match self {
            Self::Linear(remap) if capacity <= LINEAR_KEY_REMAP_MAX_LEN => {
                remap.clear();
                remap.reserve(capacity);
            }
            Self::Hashed(remap) if capacity > LINEAR_KEY_REMAP_MAX_LEN => {
                remap.clear();
                remap.reserve(capacity);
            }
            _ => *self = Self::with_capacity(capacity),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Linear(remap) => remap.clear(),
            Self::Hashed(remap) => remap.clear(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Linear(remap) => remap.capacity() * size_of::<(usize, V)>(),
            Self::Hashed(remap) => remap.capacity() * size_of::<(usize, V)>(),
        }
    }
}

fn try_for_each_selected_index(
    filter: &FilterPredicate,
    mut f: impl FnMut(usize) -> Result<(), ArrowError>,
) -> Result<(), ArrowError> {
    match filter.selection() {
        FilterSelection::None => {}
        FilterSelection::All { len } => {
            for index in 0..len {
                f(index)?;
            }
        }
        FilterSelection::Slices(slices) => {
            slices.try_for_each(|(start, end)| -> Result<(), ArrowError> {
                for index in start..end {
                    f(index)?;
                }
                Ok(())
            })?;
        }
        FilterSelection::Indices(indices) => {
            indices.try_for_each(f)?;
        }
    }
    Ok(())
}

/// Sparse-filter specialization for dictionary arrays.
pub(crate) struct InProgressDictionaryArray<K: ArrowDictionaryKeyType> {
    source: Option<ArrayRef>,
    data_type: DataType,
    batch_size: usize,
    keys: PrimitiveBuilder<K>,
    value_arrays: Vec<ArrayRef>,
    value_array_indices: HashMap<usize, usize, RandomState>,
    key_remaps: Vec<KeyRemap<K::Native>>,
    selected_values: Vec<(usize, usize)>,
    buffered_arrays: Vec<ArrayRef>,
    source_data_referenced_in_buffers: bool,
    total_size_of_non_shared_buffers: usize,
}

impl<K: ArrowDictionaryKeyType> std::fmt::Debug for InProgressDictionaryArray<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressDictionaryArray")
            .field("keys", &self.keys.len())
            .field("selected_values", &self.selected_values.len())
            .field("buffered_arrays", &self.buffered_arrays.len())
            .finish()
    }
}

impl<K: ArrowDictionaryKeyType> InProgressDictionaryArray<K> {
    pub(crate) fn new(data_type: DataType, batch_size: usize) -> Self {
        Self {
            source: None,
            data_type,
            batch_size,
            keys: PrimitiveBuilder::with_capacity(batch_size),
            value_arrays: Vec::new(),
            value_array_indices: HashMap::with_hasher(RandomState::with_seeds(0, 0, 0, 0)),
            key_remaps: Vec::new(),
            selected_values: Vec::new(),
            buffered_arrays: Vec::new(),
            source_data_referenced_in_buffers: false,
            total_size_of_non_shared_buffers: 0,
        }
    }

    fn buffer_array(&mut self, array: ArrayRef) {
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }

    fn append_index(
        keys: &mut PrimitiveBuilder<K>,
        remap: &mut KeyRemap<K::Native>,
        selected_values: &mut Vec<(usize, usize)>,
        dictionary: &DictionaryArray<K>,
        array_index: usize,
        index: usize,
    ) -> Result<(), ArrowError> {
        let Some(old_key) = dictionary.key(index) else {
            keys.append_null();
            return Ok(());
        };
        let new_key = match remap.get(old_key) {
            Some(key) => key,
            None => {
                let key = K::Native::from_usize(selected_values.len())
                    .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                remap.insert(old_key, key);
                selected_values.push((array_index, old_key));
                key
            }
        };
        keys.append_value(new_key);
        Ok(())
    }

    fn buffer_sparse_filter(
        &mut self,
        dictionary: &DictionaryArray<K>,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        if filter.count() == 0 {
            return Ok(());
        }
        if self.keys.capacity() == 0 {
            self.keys = PrimitiveBuilder::with_capacity(self.batch_size);
        }

        let values_id = Arc::as_ptr(dictionary.values()).cast::<()>() as usize;
        let array_index = match self.value_array_indices.get(&values_id) {
            Some(index) => *index,
            None => {
                let index = self.value_arrays.len();
                self.value_arrays.push(Arc::clone(dictionary.values()));
                self.value_array_indices.insert(values_id, index);
                let capacity = filter.count().min(dictionary.values().len());
                match self.key_remaps.get_mut(index) {
                    Some(remap) => remap.reset(capacity),
                    None => self.key_remaps.push(KeyRemap::with_capacity(capacity)),
                }
                index
            }
        };
        let remap = &mut self.key_remaps[array_index];
        try_for_each_selected_index(filter, |index| {
            Self::append_index(
                &mut self.keys,
                remap,
                &mut self.selected_values,
                dictionary,
                array_index,
                index,
            )
        })?;
        Ok(())
    }

    fn flush_sparse_filter(&mut self) -> Result<(), ArrowError> {
        if self.keys.len() == 0 {
            return Ok(());
        }

        let values = interleave(
            &self
                .value_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
            &self.selected_values,
        )?;
        let array = DictionaryArray::<K>::try_new(self.keys.finish(), values)?;
        self.value_arrays.clear();
        self.value_array_indices.clear();
        self.key_remaps.iter_mut().for_each(KeyRemap::clear);
        self.selected_values.clear();
        self.buffer_array(Arc::new(array));
        Ok(())
    }

    fn buffer_materialized_filter(
        &mut self,
        source: &dyn Array,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        self.flush_sparse_filter()?;
        let filtered = filter.filter(source)?;
        self.buffer_array(filtered);
        Ok(())
    }
}

impl<K: ArrowDictionaryKeyType> InProgressArray for InProgressDictionaryArray<K> {
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
        self.flush_sparse_filter()?;
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressDictionaryArray: source not set".to_string(),
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
        let dictionary = source.as_dictionary::<K>();
        let denominator = if dictionary.values().len() <= 256 {
            LOW_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR
        } else {
            HIGH_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR
        };
        if filter.count() > filter.filter_len() / denominator {
            return self.buffer_materialized_filter(source.as_ref(), filter);
        }

        self.buffer_sparse_filter(dictionary, filter)
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.flush_sparse_filter()?;
        let result = match self.buffered_arrays.len() {
            0 => new_empty_array(&self.data_type),
            1 => self.buffered_arrays.pop().unwrap(),
            _ => concat(
                &self
                    .buffered_arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?,
        };
        self.buffered_arrays.clear();
        self.total_size_of_non_shared_buffers = 0;
        self.source_data_referenced_in_buffers = false;
        Ok(result)
    }

    fn size(&self) -> usize {
        self.total_size_of_non_shared_buffers
            + self.buffered_arrays.capacity() * size_of::<ArrayRef>()
            + self.keys.capacity() * size_of::<K::Native>()
            + self.keys.validity_capacity()
            + self.value_arrays.capacity() * size_of::<ArrayRef>()
            + self.value_array_indices.capacity() * size_of::<(usize, usize)>()
            + self
                .value_arrays
                .iter()
                .map(|array| array.get_array_memory_size())
                .sum::<usize>()
            + self.key_remaps.capacity() * size_of::<KeyRemap<K::Native>>()
            + self.key_remaps.iter().map(KeyRemap::size).sum::<usize>()
            + self.selected_values.capacity() * size_of::<(usize, usize)>()
            + self
                .source
                .as_ref()
                .map_or(0, |source| source.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::{Int32Type, UInt8Type, UInt16Type};
    use arrow_array::{BooleanArray, Int32Array, StringArray, UInt8Array, UInt16Array};

    fn dictionary<K: ArrowDictionaryKeyType>(values: &[Option<&str>]) -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<K>::new();
        for value in values {
            match value {
                Some(value) => {
                    builder.append(value).unwrap();
                }
                None => {
                    builder.append_null();
                }
            }
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn preserves_order_across_materialized_and_dense_inputs() {
        let first = dictionary::<Int32Type>(&[Some("a"), Some("b"), None, Some("a")]);
        let second = dictionary::<Int32Type>(&[Some("b"), Some("c"), Some("a")]);
        let filter = BooleanArray::from(vec![true, false, true, true]);
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<Int32Type>::new(first.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(Arc::clone(&first), &predicate)
            .unwrap();
        in_progress.set_source(Some(second));
        in_progress.copy_rows(1, 2).unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![Some("a"), None, Some("a"), Some("c"), Some("a")]
        );
    }

    #[test]
    fn sparse_copy_supports_shared_value_types_across_inputs() {
        let first = (0..256)
            .map(|index| match index {
                0 => Some("a"),
                255 => Some("b"),
                _ => Some("unused"),
            })
            .collect::<Vec<_>>();
        let second = (0..256)
            .map(|index| match index {
                0 => Some("b"),
                255 => Some("c"),
                _ => Some("unused"),
            })
            .collect::<Vec<_>>();
        let first = dictionary::<Int32Type>(&first);
        let second = dictionary::<Int32Type>(&second);
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| index == 0 || index == 255)
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<Int32Type>::new(first.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(first, &predicate)
            .unwrap();
        in_progress
            .copy_rows_by_filter_from(second, &predicate)
            .unwrap();
        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<Int32Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![Some("a"), Some("b"), Some("b"), Some("c")]);
    }

    #[test]
    fn sparse_copy_reuses_shared_dictionary_values() {
        let values = Arc::new(StringArray::from_iter_values(
            (0..64).map(|index| format!("value-{index}")),
        ));
        let source = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from_iter_values((0..5120).map(|index| index % 64)),
            values,
        )) as ArrayRef;
        let filter = BooleanArray::from((0..5120).map(|index| index < 40).collect::<Vec<_>>());
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<Int32Type>::new(source.data_type().clone(), 80);
        let expected_keys = (0..40).chain(0..40).collect::<Vec<_>>();
        for _ in 0..2 {
            in_progress
                .copy_rows_by_filter_from(Arc::clone(&source), &predicate)
                .unwrap();
            in_progress
                .copy_rows_by_filter_from(Arc::clone(&source), &predicate)
                .unwrap();

            let output = in_progress.finish().unwrap();
            let output = output.as_dictionary::<Int32Type>();
            assert_eq!(output.values().len(), 40);
            assert_eq!(output.keys().values().as_ref(), expected_keys.as_slice());
        }
    }

    #[test]
    fn supports_other_key_types() {
        let input = (0..256)
            .map(|index| match index {
                0 => Some("a"),
                255 => Some("b"),
                _ => Some("unused"),
            })
            .collect::<Vec<_>>();
        let source = dictionary::<UInt8Type>(&input);
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| index == 0 || index == 255)
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<UInt8Type>::new(source.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<UInt8Type>();
        let values = output.values().as_string::<i32>();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![Some("a"), Some("b")]);
        assert_eq!(output.keys().values(), &[0, 1]);
    }

    #[test]
    fn supports_other_value_types_across_inputs() {
        let keys = || UInt8Array::from_iter_values((0..256).map(|index| (index % 2) as u8));
        let first = Arc::new(DictionaryArray::<UInt8Type>::new(
            keys(),
            Arc::new(BooleanArray::from(vec![true, false])),
        ));
        let second = Arc::new(DictionaryArray::<UInt8Type>::new(
            keys(),
            Arc::new(BooleanArray::from(vec![false, true])),
        ));
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| index == 0 || index == 255)
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<UInt8Type>::new(first.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(first, &predicate)
            .unwrap();
        in_progress
            .copy_rows_by_filter_from(second, &predicate)
            .unwrap();

        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<UInt8Type>();
        let values = output.values().as_boolean();
        let decoded = output
            .keys_iter()
            .map(|key| key.map(|key| values.value(key)))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
    }

    #[test]
    fn finish_empty_preserves_type() {
        let data_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int32));
        let mut in_progress = InProgressDictionaryArray::<UInt16Type>::new(data_type.clone(), 8);
        let output = in_progress.finish().unwrap();
        assert_eq!(output.data_type(), &data_type);
    }

    #[test]
    fn dictionary_value_nulls_retain_their_representation() {
        let keys = UInt16Array::from_iter_values((0..256).map(|index| (index % 2) as u16));
        let values = Int32Array::from(vec![Some(1), None]);
        let source = Arc::new(DictionaryArray::<UInt16Type>::new(keys, Arc::new(values)));
        let filter = BooleanArray::from(
            (0..256)
                .map(|index| index == 1 || index == 254)
                .collect::<Vec<_>>(),
        );
        let predicate = FilterBuilder::new(&filter).build();

        let mut in_progress =
            InProgressDictionaryArray::<UInt16Type>::new(source.data_type().clone(), 8);
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();
        let output = in_progress.finish().unwrap();
        let output = output.as_dictionary::<UInt16Type>();

        assert!(output.nulls().is_none());
        assert_eq!(output.logical_null_count(), 1);
        assert_eq!(output.keys().values(), &[0, 1]);
        assert_eq!(
            output.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None, Some(1)])
        );
    }
}
