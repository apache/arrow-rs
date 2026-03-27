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

use crate::arrow::buffer::view_buffer::ViewBuffer;
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::errors::{ParquetError, Result};
use arrow_array::{ArrayRef, StringViewArray};
use arrow_buffer::Buffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowType;
use std::sync::Arc;

/// A buffer that can store variable-length byte view data either as dictionary-encoded
/// (keys + dictionary) or as expanded views.
///
/// This is analogous to [`DictionaryBuffer`] but for view-based types (`Utf8View`, `BinaryView`).
/// When dictionary encoding is preserved, the output is a `DictionaryArray<Int32, Utf8View>`.
/// When dictionary encoding cannot be preserved (e.g., non-dictionary pages are encountered),
/// the buffer falls back to expanded views.
///
/// [`DictionaryBuffer`]: crate::arrow::buffer::dictionary_buffer::DictionaryBuffer
pub enum DictViewBuffer {
    /// Dictionary-encoded: stores keys and a reference to the dictionary
    Dict {
        keys: Vec<i32>,
        /// The dictionary as a StringViewArray (cheap Arc clone)
        dict: ArrayRef,
    },
    /// Fallback: expanded views (same as ViewBuffer)
    Values {
        values: ViewBuffer,
    },
}

impl Default for DictViewBuffer {
    fn default() -> Self {
        Self::Values {
            values: ViewBuffer::default(),
        }
    }
}

impl std::fmt::Debug for DictViewBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dict { keys, .. } => f
                .debug_struct("DictViewBuffer::Dict")
                .field("num_keys", &keys.len())
                .finish(),
            Self::Values { values } => f
                .debug_struct("DictViewBuffer::Values")
                .field("values", values)
                .finish(),
        }
    }
}

impl DictViewBuffer {
    #[allow(unused)]
    pub fn len(&self) -> usize {
        match self {
            Self::Dict { keys, .. } => keys.len(),
            Self::Values { values } => values.views.len(),
        }
    }

    /// Returns a mutable reference to the keys vec if we can preserve the dictionary.
    ///
    /// Returns `None` if:
    /// - The dictionary changed (different pointer) and keys are non-empty
    /// - The buffer has already fallen back to Values mode with data
    ///
    /// When `None` is returned, the caller should use `spill_values()` instead.
    pub fn as_keys(&mut self, dictionary: &ArrayRef) -> Option<&mut Vec<i32>> {
        match self {
            Self::Dict { keys, dict } => {
                // Check if the dictionary is the same object
                let dict_ptr = dict.as_ref() as *const _ as *const ();
                let new_ptr = dictionary.as_ref() as *const _ as *const ();
                if dict_ptr == new_ptr {
                    Some(keys)
                } else if keys.is_empty() {
                    *dict = Arc::clone(dictionary);
                    Some(keys)
                } else {
                    // Dictionary changed and we have existing keys - can't preserve
                    None
                }
            }
            Self::Values { values } if values.views.is_empty() => {
                *self = Self::Dict {
                    keys: Vec::new(),
                    dict: Arc::clone(dictionary),
                };
                match self {
                    Self::Dict { keys, .. } => Some(keys),
                    _ => unreachable!(),
                }
            }
            _ => None,
        }
    }

    /// Spills dictionary-encoded data into expanded views.
    ///
    /// If already in `Values` mode, returns the existing `ViewBuffer`.
    /// If in `Dict` mode, expands keys through the dictionary into views,
    /// then switches to `Values` mode.
    pub fn spill_values(&mut self) -> Result<&mut ViewBuffer> {
        match self {
            Self::Values { values } => Ok(values),
            Self::Dict { keys, dict } => {
                let mut spilled = ViewBuffer::default();

                if !dict.is_empty() && !keys.is_empty() {
                    // Expand the dictionary keys into views
                    let dict_view = dict
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            general_err!("DictViewBuffer dictionary is not StringViewArray")
                        })?;

                    // Copy dictionary buffers to the output
                    for buf in dict_view.data_buffers() {
                        spilled.buffers.push(buf.clone());
                    }

                    let dict_views = dict_view.views();
                    spilled.views.reserve(keys.len());

                    for &key in keys.iter() {
                        if key < 0 || (key as usize) >= dict_views.len() {
                            return Err(general_err!(
                                "dictionary key {} beyond bounds of dictionary: 0..{}",
                                key,
                                dict_views.len()
                            ));
                        }
                        let view = dict_views[key as usize];
                        // Adjust buffer indices since dict buffers are the first buffers
                        // in the output (base_buffer_idx = 0)
                        spilled.views.push(view);
                    }
                } else if !keys.is_empty() {
                    // Empty dictionary but non-empty keys - zero-length views
                    spilled.views.resize(keys.len(), 0u128);
                }

                *self = Self::Values { values: spilled };
                match self {
                    Self::Values { values } => Ok(values),
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Converts this buffer into an [`ArrayRef`].
    ///
    /// When in `Dict` mode, produces a `DictionaryArray<Int32, Utf8View>`.
    /// When in `Values` mode, packs the values into a fresh `DictionaryArray`.
    pub fn into_array(self, null_buffer: Option<Buffer>, data_type: &ArrowType) -> Result<ArrayRef> {
        assert!(
            matches!(data_type, ArrowType::Dictionary(_, _)),
            "DictViewBuffer requires Dictionary data type, got {data_type}"
        );

        match self {
            Self::Dict { keys, dict } => {
                // Validate keys
                if !dict.is_empty() {
                    let max = dict.len();
                    if !keys
                        .iter()
                        .copied()
                        .fold(true, |a, x| a && x >= 0 && (x as usize) < max)
                    {
                        return Err(general_err!(
                            "dictionary key beyond bounds of dictionary: 0..{}",
                            max
                        ));
                    }
                }

                let builder = ArrayDataBuilder::new(data_type.clone())
                    .len(keys.len())
                    .add_buffer(Buffer::from_vec(keys))
                    .add_child_data(dict.to_data())
                    .null_bit_buffer(null_buffer);

                let data = match cfg!(debug_assertions) {
                    true => builder.build().unwrap(),
                    false => unsafe { builder.build_unchecked() },
                };

                Ok(arrow_array::make_array(data))
            }
            Self::Values { values } => {
                // Need to create a dictionary from expanded values
                let ArrowType::Dictionary(_, value_type) = data_type else {
                    unreachable!()
                };
                let values_array = values.into_array(null_buffer, value_type);
                pack_string_view_values(&values_array, data_type)
            }
        }
    }
}

/// Pack a `StringViewArray` into a `DictionaryArray<Int32, StringViewArray>`.
///
/// This creates an identity-mapping dictionary where each row maps to its
/// own dictionary entry. This is a fallback for when dictionary encoding
/// couldn't be preserved, and is suboptimal but correct.
fn pack_string_view_values(array: &ArrayRef, dict_type: &ArrowType) -> Result<ArrayRef> {
    let len = array.len();

    // Create identity keys [0, 1, 2, ..., len-1]
    let keys: Vec<i32> = (0..len as i32).collect();

    let builder = ArrayDataBuilder::new(dict_type.clone())
        .len(len)
        .add_buffer(Buffer::from_vec(keys))
        .add_child_data(array.to_data())
        .null_bit_buffer(array.nulls().map(|n| n.buffer().clone()));

    let data = match cfg!(debug_assertions) {
        true => builder.build().unwrap(),
        false => unsafe { builder.build_unchecked() },
    };

    Ok(arrow_array::make_array(data))
}

impl ValuesBuffer for DictViewBuffer {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        match self {
            Self::Dict { keys, .. } => {
                keys.resize(read_offset + levels_read, 0i32);
                keys.pad_nulls(read_offset, values_read, levels_read, valid_mask);
            }
            Self::Values { values } => {
                values.pad_nulls(read_offset, values_read, levels_read, valid_mask);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::cast;
    use arrow_array::{Array, StringViewArray};
    use arrow_buffer::Buffer;

    fn utf8view_dictionary() -> ArrowType {
        ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8View))
    }

    #[test]
    fn test_dict_view_buffer_dict_mode() {
        let data_type = utf8view_dictionary();

        // Create a dictionary
        let dict: ArrayRef = Arc::new(StringViewArray::from(vec!["hello", "world", ""]));

        let mut buffer = DictViewBuffer::default();

        // Write keys preserving dictionary
        let keys = buffer.as_keys(&dict).unwrap();
        keys.extend_from_slice(&[1, 0, 2, 0]);

        assert!(matches!(buffer, DictViewBuffer::Dict { .. }));

        let array = buffer.into_array(None, &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let strings = cast(&array, &ArrowType::Utf8View).unwrap();
        let strings = strings
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("world"), Some("hello"), Some(""), Some("hello")]
        );
    }

    #[test]
    fn test_dict_view_buffer_with_nulls() {
        let data_type = utf8view_dictionary();

        let dict: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b"]));

        let mut buffer = DictViewBuffer::default();
        let keys = buffer.as_keys(&dict).unwrap();
        keys.extend_from_slice(&[0, 1]);

        let valid = [false, true, true, false];
        let valid_buffer = Buffer::from_iter(valid.iter().copied());
        buffer.pad_nulls(0, 2, valid.len(), valid_buffer.as_slice());

        let array = buffer
            .into_array(Some(valid_buffer), &data_type)
            .unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 2);

        let strings = cast(&array, &ArrowType::Utf8View).unwrap();
        let strings = strings
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![None, Some("a"), Some("b"), None]
        );
    }

    #[test]
    fn test_dict_view_buffer_spill() {
        let data_type = utf8view_dictionary();

        let dict: ArrayRef = Arc::new(StringViewArray::from(vec!["hello", "world"]));

        let mut buffer = DictViewBuffer::default();
        let keys = buffer.as_keys(&dict).unwrap();
        keys.extend_from_slice(&[0, 1]);

        // Spill to values mode
        let _values = buffer.spill_values().unwrap();

        assert!(matches!(buffer, DictViewBuffer::Values { .. }));

        let array = buffer.into_array(None, &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let strings = cast(&array, &ArrowType::Utf8View).unwrap();
        let strings = strings
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("hello"), Some("world")]
        );
    }

    #[test]
    fn test_dict_view_buffer_validates_keys() {
        let data_type = utf8view_dictionary();

        let dict: ArrayRef = Arc::new(StringViewArray::from(vec!["a"]));

        let mut buffer = DictViewBuffer::default();
        let keys = buffer.as_keys(&dict).unwrap();
        keys.extend_from_slice(&[0, 2, 0]); // key 2 is out of bounds

        let err = buffer.into_array(None, &data_type).unwrap_err().to_string();
        assert!(
            err.contains("dictionary key beyond bounds"),
            "{}",
            err
        );
    }
}
