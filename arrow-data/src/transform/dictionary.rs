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

use std::collections::HashMap;

use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};

use crate::{
    ArrayData,
    transform::{_MutableArrayData, Extend, MutableArrayData, utils::to_bytes_vec},
};

/// Fallback merge strategy used when optimized dictionary-merge paths cannot guarantee
/// correctness. I.e some fast-path algorithms may emit duplicate keys, which can overflow
/// the index type even if the logical keyspace is large enough.
///
/// This implementation prioritizes correctness over speed: it performs a full scan of
/// every input dictionary’s values, ensuring a de-duplicated, exhaustively validated
/// keyspace before constructing the merged dictionary.
/// The function returns
/// - a vector of closure representing the mutation over each input dictionaries
/// - an `ArrayData` of the merged value array (not the dictionary)
pub(crate) fn merge_dictionaries<'a>(
    key_data_type: &DataType,
    value_data_type: &DataType,
    dicts: &[&'a ArrayData],
) -> Result<(Vec<Extend<'a>>, ArrayData), ArrowError> {
    match key_data_type {
        DataType::UInt8 => merge_dictionaries_casted::<u8>(value_data_type, dicts),
        DataType::UInt16 => merge_dictionaries_casted::<u16>(value_data_type, dicts),
        DataType::UInt32 => merge_dictionaries_casted::<u32>(value_data_type, dicts),
        DataType::UInt64 => merge_dictionaries_casted::<u64>(value_data_type, dicts),
        DataType::Int8 => merge_dictionaries_casted::<i8>(value_data_type, dicts),
        DataType::Int16 => merge_dictionaries_casted::<i16>(value_data_type, dicts),
        DataType::Int32 => merge_dictionaries_casted::<i32>(value_data_type, dicts),
        DataType::Int64 => merge_dictionaries_casted::<i64>(value_data_type, dicts),
        _ => unreachable!(),
    }
}

fn merge_dictionaries_casted<'a, K: ArrowNativeType>(
    data_type: &DataType,
    dicts: &[&'a ArrayData],
) -> Result<(Vec<Extend<'a>>, ArrayData), ArrowError> {
    let mut dedup = HashMap::new();
    let mut indices = vec![];
    let mut data_refs = vec![];
    let new_dict_keys = dicts
        .iter()
        .enumerate()
        .map(|(dict_idx, dict)| {
            let value_data = dict.child_data().first().unwrap();
            let old_keys = dict.buffer::<K>(0);
            data_refs.push(value_data);
            let mut new_keys = vec![K::usize_as(0); old_keys.len()];
            let values = to_bytes_vec(data_type, value_data);
            for (key_index, old_key) in old_keys.iter().enumerate() {
                if dict.is_valid(key_index) {
                    let value = values[old_key.as_usize()];
                    match K::from_usize(dedup.len()) {
                        Some(idx) => {
                            let idx_for_value = dedup.entry(value).or_insert(idx);
                            // a new entry
                            if *idx_for_value == idx {
                                indices.push((dict_idx, old_key.as_usize()));
                            }

                            new_keys[key_index] = *idx_for_value;
                        }
                        // the built dictionary has reach the cap of the key type
                        None => match dedup.get(value) {
                            // as long as this value has already been indexed
                            // the merge dictionary is still valid
                            Some(previous_key) => {
                                new_keys[key_index] = *previous_key;
                            }
                            None => return Err(ArrowError::DictionaryKeyOverflowError),
                        },
                    };
                }
            }

            Ok(new_keys)
        })
        .collect::<Result<Vec<Vec<K>>, ArrowError>>()?;
    let shared_value_data = if indices.is_empty() {
        ArrayData::new_empty(data_refs[0].data_type())
    } else {
        let new_values_data = MutableArrayData::new(data_refs, false, indices.len());
        interleave(new_values_data, indices)
    };

    Ok((
        new_dict_keys
            .into_iter()
            .map(|keys| {
                Box::new(
                    move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
                        mutable
                            .buffer1
                            .extend_from_slice::<K>(&keys[start..start + len]);
                    },
                ) as Extend
            })
            .collect::<Vec<Extend>>(),
        shared_value_data,
    ))
}

fn interleave(mut array_data: MutableArrayData, indices: Vec<(usize, usize)>) -> ArrayData {
    let mut cur_array = indices[0].0;

    let mut start_row_idx = indices[0].1;
    let mut end_row_idx = start_row_idx + 1;

    for (array, row) in indices.iter().skip(1).copied() {
        if array == cur_array && row == end_row_idx {
            // subsequent row in same batch
            end_row_idx += 1;
            continue;
        }

        // emit current batch of rows for current buffer
        array_data.extend(cur_array, start_row_idx, end_row_idx);

        // start new batch of rows
        cur_array = array;
        start_row_idx = row;
        end_row_idx = start_row_idx + 1;
    }

    // emit final batch of rows
    array_data.extend(cur_array, start_row_idx, end_row_idx);
    array_data.freeze()
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use arrow_buffer::{ArrowNativeType, Buffer, ToByteSlice};
    use arrow_schema::{ArrowError, DataType};

    use crate::{
        ArrayData, new_buffers,
        transform::{_MutableArrayData, dictionary::merge_dictionaries},
    };

    fn create_dictionary_from_value_data<K: ArrowNativeType>(
        keys: Vec<K>,
        value: ArrayData,
        key_type: DataType,
    ) -> ArrayData {
        let keys_buffer = Buffer::from(keys.to_byte_slice());

        let dict_data_type =
            DataType::Dictionary(Box::new(key_type), Box::new(value.data_type().clone()));
        ArrayData::builder(dict_data_type.clone())
            .len(3)
            .add_buffer(keys_buffer)
            .add_child_data(value)
            .build()
            .unwrap()
    }
    fn create_dictionary<K: ArrowNativeType, V: ArrowNativeType>(
        keys: Vec<K>,
        value: Vec<V>,
        key_type: DataType,
        value_type: DataType,
    ) -> ArrayData {
        let keys_buffer = Buffer::from(keys.to_byte_slice());

        let value_data = ArrayData::builder(value_type.clone())
            .len(8)
            .add_buffer(Buffer::from(value.to_byte_slice()))
            .build()
            .unwrap();

        let dict_data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        ArrayData::builder(dict_data_type.clone())
            .len(3)
            .add_buffer(keys_buffer)
            .add_child_data(value_data.clone())
            .build()
            .unwrap()
    }

    // arrays containing concanated numeric character from 0 to 255
    // like ["0","1",..,"255"]
    fn make_numeric_string_array(numbers: Vec<u32>) -> ArrayData {
        let values = numbers.iter().map(|i| i.to_string()).collect::<String>();
        let mut acc = 0;

        let offset_iter = numbers
            .iter()
            .map(|i| if *i == 0 { 1 } else { i.ilog10() + 1 })
            .map(|length| {
                acc += length;
                acc
            });

        let offsets = once(0).chain(offset_iter).collect::<Vec<_>>();
        ArrayData::builder(DataType::Utf8)
            .len(numbers.len())
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values.as_bytes()))
            .build()
            .unwrap()
    }

    #[test]
    fn merge_string_value_dictionary() {
        let arr1 = create_dictionary_from_value_data(
            (0u8..=127).collect(),
            make_numeric_string_array((0..=127).collect()),
            DataType::UInt8,
        );
        let arr1_clone = create_dictionary_from_value_data(
            (0u8..=127).collect(),
            make_numeric_string_array((0..=127).collect()),
            DataType::UInt8,
        );
        let arr2 = create_dictionary_from_value_data(
            (0u8..=127).collect(),
            make_numeric_string_array((128..=255).collect()),
            DataType::UInt8,
        );
        // all possible values from arr1 and arr2 require keysize > 131072
        // which overflows for uint16

        let (extends, merged_value_arr) = merge_dictionaries(
            &DataType::UInt8,
            &DataType::Utf8,
            &[&arr1, &arr2, &arr1_clone],
        )
        .unwrap();

        // this array is used as value array for the new dictionary
        let expected_new_value = make_numeric_string_array((0..=255).collect());
        assert!(expected_new_value.eq(&merged_value_arr));

        let [buffer1, buffer2] = new_buffers(arr1.data_type(), 256);
        let mut data = _MutableArrayData {
            data_type: arr1.data_type().clone(),
            len: 0,
            null_count: 0,
            null_buffer: None,
            buffer1,
            buffer2,
            child_data: vec![],
        };

        // concat keys [0..127] [128..255] [0..128]
        for (index, extend) in extends.iter().enumerate() {
            extend(&mut data, index, 0, 128)
        }
        // key buffer after calling extends closure is also correct
        let expected_key_raw_buffer = (0u8..=255).chain(0u8..=127).collect::<Vec<_>>();
        assert_eq!(data.buffer1.as_slice(), &expected_key_raw_buffer);
    }

    #[test]
    fn total_distinct_keys_in_input_arrays_greater_than_key_size() {
        // all possible values from arr1 and arr2 require keysize > 131072
        // which overflows for uint16
        let arr1 = create_dictionary(
            (0u16..=65535).collect(),
            (0u32..=65535).collect(),
            DataType::UInt16,
            DataType::UInt32,
        );
        let arr2 = create_dictionary(
            (0u16..=65535).collect(),
            (65536u32..=131071).collect(),
            DataType::UInt16,
            DataType::UInt32,
        );
        assert!(matches!(
            merge_dictionaries(&DataType::UInt16, &DataType::UInt32, &[&arr1, &arr2]),
            Err(ArrowError::DictionaryKeyOverflowError),
        ));
    }
}
