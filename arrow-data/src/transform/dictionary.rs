use std::collections::HashMap;

use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};

use crate::{
    ArrayData,
    transform::{_MutableArrayData, Extend, MutableArrayData, utils::iter_in_bytes},
};

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
            let value_data = dict.child_data().get(0).unwrap();
            let old_keys = dict.buffer::<K>(0);
            data_refs.push(value_data);
            let mut new_keys = vec![K::usize_as(0); old_keys.len()];
            let values = iter_in_bytes(data_type, value_data);
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
