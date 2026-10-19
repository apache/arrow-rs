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
use arrow_array::builder::{PrimitiveBuilder, StringDictionaryBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::ArrowDictionaryKeyType;
use arrow_array::{Array, ArrayRef, DictionaryArray, StringArray, new_empty_array};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

const LOW_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR: usize = 128;
const HIGH_CARDINALITY_MAX_SELECTIVITY_DENOMINATOR: usize = 64;

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
    utf8_values: bool,
    batch_size: usize,
    string_builder: Option<StringDictionaryBuilder<K>>,
    buffered_arrays: Vec<ArrayRef>,
    source_data_referenced_in_buffers: bool,
    total_size_of_non_shared_buffers: usize,
    builder_rows: usize,
    builder_value_bytes: usize,
}

impl<K: ArrowDictionaryKeyType> std::fmt::Debug for InProgressDictionaryArray<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressDictionaryArray")
            .field("batch_size", &self.batch_size)
            .field("builder_rows", &self.builder_rows)
            .field("buffered_arrays", &self.buffered_arrays.len())
            .finish()
    }
}

impl<K: ArrowDictionaryKeyType> InProgressDictionaryArray<K> {
    pub(crate) fn new(data_type: DataType, batch_size: usize) -> Self {
        let utf8_values = matches!(&data_type, DataType::Dictionary(_, value) if value.as_ref() == &DataType::Utf8);
        Self {
            source: None,
            data_type,
            utf8_values,
            batch_size,
            string_builder: None,
            buffered_arrays: Vec::new(),
            source_data_referenced_in_buffers: false,
            total_size_of_non_shared_buffers: 0,
            builder_rows: 0,
            builder_value_bytes: 0,
        }
    }

    fn append_string_index(
        builder: &mut StringDictionaryBuilder<K>,
        source: &DictionaryArray<K>,
        values: &StringArray,
        index: usize,
    ) -> Result<usize, ArrowError> {
        match source.key(index) {
            Some(key) => {
                let value = values.value(key);
                builder.append(value)?;
                Ok(value.len())
            }
            None => {
                builder.append_null();
                Ok(0)
            }
        }
    }

    fn flush_string_builder(&mut self) {
        let Some(mut builder) = self.string_builder.take() else {
            return;
        };
        let array = Arc::new(builder.finish()) as ArrayRef;
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
        self.builder_rows = 0;
        self.builder_value_bytes = 0;
    }

    fn buffer_array(&mut self, array: ArrayRef) {
        self.total_size_of_non_shared_buffers += array.get_array_memory_size();
        self.buffered_arrays.push(array);
    }

    fn append_generic_index(
        keys: &mut PrimitiveBuilder<K>,
        remap: &mut [Option<K::Native>],
        selected_values: &mut Vec<(usize, usize)>,
        dictionary: &DictionaryArray<K>,
        index: usize,
    ) -> Result<(), ArrowError> {
        let Some(old_key) = dictionary.key(index) else {
            keys.append_null();
            return Ok(());
        };
        let new_key = match remap[old_key] {
            Some(key) => key,
            None => {
                let key = K::Native::from_usize(selected_values.len())
                    .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                remap[old_key] = Some(key);
                selected_values.push((0, old_key));
                key
            }
        };
        keys.append_value(new_key);
        Ok(())
    }

    fn buffer_sparse_generic(
        &mut self,
        dictionary: &DictionaryArray<K>,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        self.flush_string_builder();
        if filter.count() == 0 {
            return Ok(());
        }

        let mut remap = vec![None; dictionary.values().len()];
        let mut selected_values = Vec::with_capacity(filter.count());
        let mut keys = PrimitiveBuilder::<K>::with_capacity(filter.count());
        try_for_each_selected_index(filter, |index| {
            Self::append_generic_index(
                &mut keys,
                &mut remap,
                &mut selected_values,
                dictionary,
                index,
            )
        })?;

        let values = interleave(&[dictionary.values().as_ref()], &selected_values)?;
        let array = DictionaryArray::<K>::try_new(keys.finish(), values)?;
        self.buffer_array(Arc::new(array));
        Ok(())
    }

    fn buffer_materialized_filter(
        &mut self,
        source: &dyn Array,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        self.flush_string_builder();
        let filtered = filter.filter(source)?;
        self.buffer_array(filtered);
        Ok(())
    }

    fn buffer_sparse_strings(
        &mut self,
        dictionary: &DictionaryArray<K>,
        values: &StringArray,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let builder = self.string_builder.get_or_insert_with(|| {
            StringDictionaryBuilder::with_capacity(
                self.batch_size,
                values.len(),
                values.get_array_memory_size(),
            )
        });

        let mut appended_value_bytes = 0;
        try_for_each_selected_index(filter, |index| {
            appended_value_bytes += Self::append_string_index(builder, dictionary, values, index)?;
            Ok(())
        })?;
        self.builder_rows += filter.count();
        self.builder_value_bytes += appended_value_bytes;
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
        self.flush_string_builder();
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

        if self.utf8_values {
            let values = dictionary.values().as_string::<i32>();
            if values.null_count() == 0 {
                return self.buffer_sparse_strings(dictionary, values, filter);
            }
        }
        self.buffer_sparse_generic(dictionary, filter)
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        self.flush_string_builder();
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
            + self
                .source
                .as_ref()
                .map_or(0, |source| source.get_array_memory_size())
            + self.builder_rows * size_of::<K::Native>()
            + self.builder_value_bytes
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
    fn sparse_copy_unifies_dictionary_values_across_inputs() {
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
        assert_eq!(
            output.values().as_string::<i32>(),
            &StringArray::from(vec!["a", "b", "c"])
        );
        assert_eq!(output.keys().values(), &[0, 1, 1, 2]);
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
