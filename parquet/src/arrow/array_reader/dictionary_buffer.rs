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

use crate::arrow::array_reader::offset_buffer::OffsetBuffer;
use crate::arrow::record_reader::buffer::{
    BufferQueue, ScalarBuffer, ScalarValue, ValuesBuffer,
};
use crate::column::reader::decoder::ValuesBufferSlice;
use crate::errors::{ParquetError, Result};
use arrow::array::{make_array, ArrayDataBuilder, ArrayRef, OffsetSizeTrait};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowNativeType, DataType as ArrowType};
use std::sync::Arc;

/// An array of variable length byte arrays that are potentially dictionary encoded
/// and can be converted into a corresponding [`ArrayRef`]
pub enum DictionaryBuffer<K: ScalarValue, V: ScalarValue> {
    Dict {
        keys: ScalarBuffer<K>,
        values: ArrayRef,
    },
    Values {
        values: OffsetBuffer<V>,
    },
}

impl<K: ScalarValue, V: ScalarValue> Default for DictionaryBuffer<K, V> {
    fn default() -> Self {
        Self::Values {
            values: Default::default(),
        }
    }
}

impl<K: ScalarValue + ArrowNativeType + Ord, V: ScalarValue + OffsetSizeTrait>
    DictionaryBuffer<K, V>
{
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
    pub fn as_keys(&mut self, dictionary: &ArrayRef) -> Option<&mut ScalarBuffer<K>> {
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
                let mut spilled = OffsetBuffer::default();
                let dict_buffers = values.data().buffers();
                let dict_offsets = unsafe { dict_buffers[0].typed_data::<V>() };
                let dict_values = dict_buffers[1].as_slice();

                if values.is_empty() {
                    // If dictionary is empty, zero pad offsets
                    spilled.offsets.resize(keys.len() + 1);
                } else {
                    // Note: at this point null positions will have arbitrary dictionary keys
                    // and this will hydrate them to the corresponding byte array. This is
                    // likely sub-optimal, as we would prefer zero length null "slots", but
                    // spilling is already a degenerate case and so it is unclear if this is
                    // worth optimising for, e.g. by keeping a null mask around
                    spilled.extend_from_dictionary(
                        keys.as_slice(),
                        dict_offsets,
                        dict_values,
                    )?;
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

                    // It may be possible to use SIMD here
                    if keys.as_slice().iter().any(|x| *x < min || *x >= max) {
                        return Err(general_err!(
                            "dictionary key beyond bounds of dictionary: 0..{}",
                            values.len()
                        ));
                    }
                }

                let mut builder = ArrayDataBuilder::new(data_type.clone())
                    .len(keys.len())
                    .add_buffer(keys.into())
                    .add_child_data(values.data().clone());

                if let Some(buffer) = null_buffer {
                    builder = builder.null_bit_buffer(buffer);
                }

                let data = match cfg!(debug_assertions) {
                    true => builder.build().unwrap(),
                    false => unsafe { builder.build_unchecked() },
                };

                Ok(make_array(data))
            }
            Self::Values { values } => {
                let value_type = match data_type {
                    ArrowType::Dictionary(_, v) => v.as_ref().clone(),
                    _ => unreachable!(),
                };

                // This will compute a new dictionary
                let array = arrow::compute::cast(
                    &values.into_array(null_buffer, value_type),
                    data_type,
                )
                .expect("cast should be infallible");

                Ok(array)
            }
        }
    }
}

impl<K: ScalarValue, V: ScalarValue> ValuesBufferSlice for DictionaryBuffer<K, V> {
    fn capacity(&self) -> usize {
        usize::MAX
    }
}

impl<K: ScalarValue, V: ScalarValue + OffsetSizeTrait> ValuesBuffer
    for DictionaryBuffer<K, V>
{
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        match self {
            Self::Dict { keys, .. } => {
                keys.resize(read_offset + levels_read);
                keys.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
            Self::Values { values, .. } => {
                values.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
        }
    }
}

impl<K: ScalarValue, V: ScalarValue + OffsetSizeTrait> BufferQueue
    for DictionaryBuffer<K, V>
{
    type Output = Self;
    type Slice = Self;

    fn split_off(&mut self, len: usize) -> Self::Output {
        match self {
            Self::Dict { keys, values } => Self::Dict {
                keys: keys.take(len),
                values: values.clone(),
            },
            Self::Values { values } => Self::Values {
                values: values.split_off(len),
            },
        }
    }

    fn spare_capacity_mut(&mut self, _batch_size: usize) -> &mut Self::Slice {
        self
    }

    fn set_len(&mut self, len: usize) {
        match self {
            Self::Dict { keys, .. } => keys.set_len(len),
            Self::Values { values } => values.set_len(len),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::compute::cast;

    #[test]
    fn test_dictionary_buffer() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8));

        let d1: ArrayRef =
            Arc::new(StringArray::from(vec!["hello", "world", "", "a", "b"]));

        let mut buffer = DictionaryBuffer::<i32, i32>::default();

        // Read some data preserving the dictionary
        let values = &[1, 0, 3, 2, 4];
        buffer.as_keys(&d1).unwrap().extend_from_slice(values);

        let mut valid = vec![false, false, true, true, false, true, true, true];
        let valid_buffer = Buffer::from_iter(valid.iter().cloned());
        buffer.pad_nulls(0, values.len(), valid.len(), valid_buffer.as_slice());

        // Split off some data

        let split = buffer.split_off(4);
        let null_buffer = Buffer::from_iter(valid.drain(0..4));
        let array = split.into_array(Some(null_buffer), &dict_type).unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![None, None, Some("world"), Some("hello")]
        );

        // Read some data not preserving the dictionary

        let values = buffer.spill_values().unwrap();
        let read_offset = values.len();
        values.try_push("bingo".as_bytes(), false).unwrap();
        values.try_push("bongo".as_bytes(), false).unwrap();

        valid.extend_from_slice(&[false, false, true, false, true]);
        let null_buffer = Buffer::from_iter(valid.iter().cloned());
        buffer.pad_nulls(read_offset, 2, 5, null_buffer.as_slice());

        assert_eq!(buffer.len(), 9);
        let split = buffer.split_off(9);

        let array = split.into_array(Some(null_buffer), &dict_type).unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
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

        let array = buffer.split_off(4).into_array(None, &dict_type).unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("bingo"), Some(""), Some("bingo"), Some("")]
        );

        // Can recreate with new dictionary as keys empty
        assert!(matches!(&buffer, DictionaryBuffer::Dict { .. }));
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

        let mut buffer = DictionaryBuffer::<i32, i32>::default();
        let d = Arc::new(StringArray::from(vec!["", "f"])) as ArrayRef;
        buffer.as_keys(&d).unwrap().extend_from_slice(&[0, 2, 0]);

        let err = buffer.into_array(None, &dict_type).unwrap_err().to_string();
        assert!(
            err.contains("dictionary key beyond bounds of dictionary: 0..2"),
            "{}",
            err
        );

        let mut buffer = DictionaryBuffer::<i32, i32>::default();
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
