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

use crate::arrow::buffer::offset_buffer::OffsetBuffer;
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::errors::{ParquetError, Result};
use arrow_array::{Array, GenericByteArray, downcast_integer};
use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, OffsetSizeTrait,
    builder::{FixedSizeBinaryDictionaryBuilder, GenericByteDictionaryBuilder},
    cast::AsArray,
    make_array,
    types::{ArrowDictionaryKeyType, ByteArrayType},
};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowType;
use std::sync::Arc;

/// An array of variable length byte arrays that are potentially dictionary encoded
/// and can be converted into a corresponding [`ArrayRef`]
pub enum DictionaryBuffer<K: ArrowNativeType, V: OffsetSizeTrait> {
    Dict { keys: Vec<K>, values: ArrayRef },
    Values { values: OffsetBuffer<V> },
}

impl<K: ArrowNativeType + Ord, V: OffsetSizeTrait> DictionaryBuffer<K, V> {
    #[allow(unused)]
    pub fn len(&self) -> usize {
        match self {
            Self::Dict { keys, .. } => keys.len(),
            Self::Values { values } => values.len(),
        }
    }

    /// Returns a mutable reference to a keys array
    ///
    /// Returns None if the dictionary needs to be recomputed
    ///
    /// # Panic
    ///
    /// Panics if the dictionary is too large for `K`
    pub fn as_keys(&mut self, dictionary: &ArrayRef) -> Option<&mut Vec<K>> {
        assert!(K::from_usize(dictionary.len()).is_some());

        match self {
            Self::Dict { keys, values } => {
                // Need to discard fat pointer for equality check
                // - https://stackoverflow.com/a/67114787
                // - https://github.com/rust-lang/rust/issues/46139
                let values_ptr = values.as_ref() as *const _ as *const ();
                let dict_ptr = dictionary.as_ref() as *const _ as *const ();
                if values_ptr == dict_ptr {
                    Some(keys)
                } else if keys.is_empty() {
                    *values = Arc::clone(dictionary);
                    Some(keys)
                } else {
                    None
                }
            }
            Self::Values { values } if values.is_empty() => {
                *self = Self::Dict {
                    keys: Default::default(),
                    values: Arc::clone(dictionary),
                };
                match self {
                    Self::Dict { keys, .. } => Some(keys),
                    _ => unreachable!(),
                }
            }
            _ => None,
        }
    }

    /// Returns a mutable reference to a values array
    ///
    /// If this is currently dictionary encoded, this will convert from the
    /// dictionary encoded representation
    pub fn spill_values(&mut self) -> Result<&mut OffsetBuffer<V>> {
        match self {
            Self::Values { values } => Ok(values),
            Self::Dict { keys, values } => {
                let mut spilled = OffsetBuffer::with_capacity(0);
                let data = values.to_data();
                let dict_buffers = data.buffers();
                let dict_offsets = dict_buffers[0].typed_data::<V>();
                let dict_values = dict_buffers[1].as_slice();

                if values.is_empty() {
                    // If dictionary is empty, zero pad offsets
                    spilled.offsets.resize(keys.len() + 1, V::default());
                } else {
                    // Note: at this point null positions will have arbitrary dictionary keys
                    // and this will hydrate them to the corresponding byte array. This is
                    // likely sub-optimal, as we would prefer zero length null "slots", but
                    // spilling is already a degenerate case and so it is unclear if this is
                    // worth optimising for, e.g. by keeping a null mask around
                    spilled.extend_from_dictionary(keys.as_slice(), dict_offsets, dict_values)?;
                }

                *self = Self::Values { values: spilled };
                match self {
                    Self::Values { values } => Ok(values),
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(
        self,
        null_buffer: Option<Buffer>,
        data_type: &ArrowType,
    ) -> Result<ArrayRef> {
        assert!(matches!(data_type, ArrowType::Dictionary(_, _)));

        match self {
            Self::Dict { keys, values } => {
                // Validate keys unless dictionary is empty
                if !values.is_empty() {
                    let min = K::from_usize(0).unwrap();
                    let max = K::from_usize(values.len()).unwrap();

                    // using copied and fold gets auto-vectorized since rust 1.70
                    // all/any would allow early exit on invalid values
                    // but in the happy case all values have to be checked anyway
                    if !keys
                        .as_slice()
                        .iter()
                        .copied()
                        .fold(true, |a, x| a && x >= min && x < max)
                    {
                        return Err(general_err!(
                            "dictionary key beyond bounds of dictionary: 0..{}",
                            values.len()
                        ));
                    }
                }

                let ArrowType::Dictionary(_, value_type) = data_type else {
                    unreachable!()
                };
                let values = if let ArrowType::FixedSizeBinary(size) = **value_type {
                    let binary = values.as_binary::<i32>();
                    Arc::new(FixedSizeBinaryArray::new(
                        size,
                        binary.values().clone(),
                        binary.nulls().cloned(),
                    )) as _
                } else {
                    values
                };

                let builder = ArrayDataBuilder::new(data_type.clone())
                    .len(keys.len())
                    .add_buffer(Buffer::from_vec(keys))
                    .add_child_data(values.into_data())
                    .null_bit_buffer(null_buffer);

                let data = match cfg!(debug_assertions) {
                    true => builder.build().unwrap(),
                    false => unsafe { builder.build_unchecked() },
                };

                Ok(make_array(data))
            }
            Self::Values { values } => {
                let (key_type, value_type) = match data_type {
                    ArrowType::Dictionary(k, v) => (k, v.as_ref().clone()),
                    _ => unreachable!(),
                };

                let array = values.into_array(null_buffer, value_type);
                pack_values(key_type, &array)
            }
        }
    }
}

impl<K: ArrowNativeType, V: OffsetSizeTrait> ValuesBuffer for DictionaryBuffer<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        Self::Values {
            values: OffsetBuffer::with_capacity(capacity),
        }
    }

    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        match self {
            Self::Dict { keys, .. } => {
                keys.resize(read_offset + levels_read, K::default());
                keys.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
            Self::Values { values, .. } => {
                values.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
        }
    }
}

macro_rules! dict_helper {
    ($k:ty, $array:ident) => {
        match $array.data_type() {
            ArrowType::Utf8 => pack_values_impl::<$k, _>($array.as_string::<i32>()),
            ArrowType::LargeUtf8 => pack_values_impl::<$k, _>($array.as_string::<i64>()),
            ArrowType::Binary => pack_values_impl::<$k, _>($array.as_binary::<i32>()),
            ArrowType::LargeBinary => pack_values_impl::<$k, _>($array.as_binary::<i64>()),
            ArrowType::FixedSizeBinary(_) => {
                pack_fixed_values_impl::<$k>($array.as_fixed_size_binary())
            }
            _ => unreachable!(),
        }
    };
}

fn pack_values(key_type: &ArrowType, values: &ArrayRef) -> Result<ArrayRef> {
    downcast_integer! {
        key_type => (dict_helper, values),
            _ => unreachable!(),
    }
}

fn pack_values_impl<K: ArrowDictionaryKeyType, T: ByteArrayType>(
    array: &GenericByteArray<T>,
) -> Result<ArrayRef> {
    let mut builder = GenericByteDictionaryBuilder::<K, T>::with_capacity(array.len(), 1024, 1024);
    for x in array {
        match x {
            Some(x) => builder.append_value(x),
            None => builder.append_null(),
        }
    }
    let raw = builder.finish();
    Ok(Arc::new(raw))
}

fn pack_fixed_values_impl<K: ArrowDictionaryKeyType>(
    array: &FixedSizeBinaryArray,
) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryDictionaryBuilder::<K>::with_capacity(
        array.len(),
        1024,
        array.value_length(),
    );
    for x in array {
        match x {
            Some(x) => builder.append_value(x),
            None => builder.append_null(),
        }
    }
    let raw = builder.finish();
    Ok(Arc::new(raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::cast;
    use arrow_array::StringArray;

    #[test]
    fn test_dictionary_buffer() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8));

        let d1: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world", "", "a", "b"]));

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);

        // Read some data preserving the dictionary
        let values = &[1, 0, 3, 2, 4];
        buffer.as_keys(&d1).unwrap().extend_from_slice(values);

        let mut valid = vec![false, false, true, true, false, true, true, true];
        let valid_buffer = Buffer::from_iter(valid.iter().cloned());
        buffer.pad_nulls(0, values.len(), valid.len(), valid_buffer.as_slice());

        // Read some data not preserving the dictionary

        let values = buffer.spill_values().unwrap();
        let read_offset = values.len();
        values.try_push("bingo".as_bytes(), false).unwrap();
        values.try_push("bongo".as_bytes(), false).unwrap();

        valid.extend_from_slice(&[false, false, true, false, true]);
        let null_buffer = Buffer::from_iter(valid.iter().cloned());
        buffer.pad_nulls(read_offset, 2, 5, null_buffer.as_slice());

        assert_eq!(buffer.len(), 13);
        let split = std::mem::replace(&mut buffer, DictionaryBuffer::with_capacity(0));

        let array = split.into_array(Some(null_buffer), &dict_type).unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
                None,
                None,
                Some("world"),
                Some("hello"),
                None,
                Some("a"),
                Some(""),
                Some("b"),
                None,
                None,
                Some("bingo"),
                None,
                Some("bongo")
            ]
        );

        // Can recreate with new dictionary as values is empty
        assert!(matches!(&buffer, DictionaryBuffer::Values { .. }));
        assert_eq!(buffer.len(), 0);
        let d2 = Arc::new(StringArray::from(vec!["bingo", ""])) as ArrayRef;
        buffer
            .as_keys(&d2)
            .unwrap()
            .extend_from_slice(&[0, 1, 0, 1]);

        let array = std::mem::replace(&mut buffer, DictionaryBuffer::with_capacity(0))
            .into_array(None, &dict_type)
            .unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("bingo"), Some(""), Some("bingo"), Some("")]
        );

        // Can recreate with new dictionary as keys empty
        assert!(matches!(&buffer, DictionaryBuffer::Values { .. }));
        assert_eq!(buffer.len(), 0);
        let d3 = Arc::new(StringArray::from(vec!["bongo"])) as ArrayRef;
        buffer.as_keys(&d3).unwrap().extend_from_slice(&[0, 0]);

        // Cannot change dictionary as keys not empty
        let d4 = Arc::new(StringArray::from(vec!["bananas"])) as ArrayRef;
        assert!(buffer.as_keys(&d4).is_none());
    }

    #[test]
    fn test_validates_keys() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8));

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
        let d = Arc::new(StringArray::from(vec!["", "f"])) as ArrayRef;
        buffer.as_keys(&d).unwrap().extend_from_slice(&[0, 2, 0]);

        let err = buffer.into_array(None, &dict_type).unwrap_err().to_string();
        assert!(
            err.contains("dictionary key beyond bounds of dictionary: 0..2"),
            "{}",
            err
        );

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
        let d = Arc::new(StringArray::from(vec![""])) as ArrayRef;
        buffer.as_keys(&d).unwrap().extend_from_slice(&[0, 1, 0]);

        let err = buffer.spill_values().unwrap_err().to_string();
        assert!(
            err.contains("dictionary key beyond bounds of dictionary: 0..1"),
            "{}",
            err
        );
    }
}
