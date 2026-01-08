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

//! Interleave elements from multiple arrays

use crate::concat::concat;
use crate::dictionary::{merge_dictionary_values, should_merge_dictionary_values};
use arrow_array::builder::{BooleanBufferBuilder, PrimitiveBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, MutableBuffer, NullBuffer, OffsetBuffer};
use arrow_data::ByteView;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};
use std::sync::Arc;

macro_rules! primitive_helper {
    ($t:ty, $values:ident, $indices:ident, $data_type:ident) => {
        interleave_primitive::<$t>($values, $indices, $data_type)
    };
}

macro_rules! dict_helper {
    ($t:ty, $values:expr, $indices:expr) => {
        Ok(Arc::new(interleave_dictionaries::<$t>($values, $indices)?) as _)
    };
}

///
/// Takes elements by index from a list of [`Array`], creating a new [`Array`] from those values.
///
/// Each element in `indices` is a pair of `usize` with the first identifying the index
/// of the [`Array`] in `values`, and the second the index of the value within that [`Array`]
///
/// ```text
/// ┌─────────────────┐      ┌─────────┐                                  ┌─────────────────┐
/// │        A        │      │ (0, 0)  │        interleave(               │        A        │
/// ├─────────────────┤      ├─────────┤          [values0, values1],     ├─────────────────┤
/// │        D        │      │ (1, 0)  │          indices                 │        B        │
/// └─────────────────┘      ├─────────┤        )                         ├─────────────────┤
///   values array 0         │ (1, 1)  │      ─────────────────────────▶  │        C        │
///                          ├─────────┤                                  ├─────────────────┤
///                          │ (0, 1)  │                                  │        D        │
///                          └─────────┘                                  └─────────────────┘
/// ┌─────────────────┐       indices
/// │        B        │        array
/// ├─────────────────┤                                                    result
/// │        C        │
/// ├─────────────────┤
/// │        E        │
/// └─────────────────┘
///   values array 1
/// ```
///
/// For selecting values by index from a single array see [`crate::take`]
pub fn interleave(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    if values.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "interleave requires input of at least one array".to_string(),
        ));
    }
    let data_type = values[0].data_type();

    for array in values.iter().skip(1) {
        if array.data_type() != data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "It is not possible to interleave arrays of different data types ({} and {})",
                data_type,
                array.data_type()
            )));
        }
    }

    if indices.is_empty() {
        return Ok(new_empty_array(data_type));
    }

    downcast_primitive! {
        data_type => (primitive_helper, values, indices, data_type),
        DataType::Utf8 => interleave_bytes::<Utf8Type>(values, indices),
        DataType::LargeUtf8 => interleave_bytes::<LargeUtf8Type>(values, indices),
        DataType::Binary => interleave_bytes::<BinaryType>(values, indices),
        DataType::LargeBinary => interleave_bytes::<LargeBinaryType>(values, indices),
        DataType::BinaryView => interleave_views::<BinaryViewType>(values, indices),
        DataType::Utf8View => interleave_views::<StringViewType>(values, indices),
        DataType::Dictionary(k, _) => downcast_integer! {
            k.as_ref() => (dict_helper, values, indices),
            _ => unreachable!("illegal dictionary key type {k}")
        },
        DataType::Struct(fields) => interleave_struct(fields, values, indices),
        DataType::List(field) => interleave_list::<i32>(values, indices, field),
        DataType::LargeList(field) => interleave_list::<i64>(values, indices, field),
        _ => interleave_fallback(values, indices)
    }
}

/// Common functionality for interleaving arrays
///
/// T is the concrete Array type
struct Interleave<'a, T> {
    /// The input arrays downcast to T
    arrays: Vec<&'a T>,
    /// The null buffer of the interleaved output
    nulls: Option<NullBuffer>,
}

impl<'a, T: Array + 'static> Interleave<'a, T> {
    fn new(values: &[&'a dyn Array], indices: &'a [(usize, usize)]) -> Self {
        let mut has_nulls = false;
        let arrays: Vec<&T> = values
            .iter()
            .map(|x| {
                has_nulls = has_nulls || x.null_count() != 0;
                x.as_any().downcast_ref().unwrap()
            })
            .collect();

        let nulls = match has_nulls {
            true => {
                let nulls = BooleanBuffer::collect_bool(indices.len(), |i| {
                    let (a, b) = indices[i];
                    arrays[a].is_valid(b)
                });
                Some(nulls.into())
            }
            false => None,
        };

        Self { arrays, nulls }
    }
}

fn interleave_primitive<T: ArrowPrimitiveType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    data_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, PrimitiveArray<T>>::new(values, indices);

    let values = indices
        .iter()
        .map(|(a, b)| interleaved.arrays[*a].value(*b))
        .collect::<Vec<_>>();

    let array = PrimitiveArray::<T>::try_new(values.into(), interleaved.nulls)?;
    Ok(Arc::new(array.with_data_type(data_type.clone())))
}

fn interleave_bytes<T: ByteArrayType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteArray<T>>::new(values, indices);

    let mut capacity = 0;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(T::Offset::from_usize(0).unwrap());
    offsets.extend(indices.iter().map(|(a, b)| {
        let o = interleaved.arrays[*a].value_offsets();
        let element_len = o[*b + 1].as_usize() - o[*b].as_usize();
        capacity += element_len;
        T::Offset::from_usize(capacity).expect("overflow")
    }));

    let mut values = Vec::with_capacity(capacity);
    for (a, b) in indices {
        values.extend_from_slice(interleaved.arrays[*a].value(*b).as_ref());
    }

    // Safety: safe by construction
    let array = unsafe {
        let offsets = OffsetBuffer::new_unchecked(offsets.into());
        GenericByteArray::<T>::new_unchecked(offsets, values.into(), interleaved.nulls)
    };
    Ok(Arc::new(array))
}

fn interleave_dictionaries<K: ArrowDictionaryKeyType>(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let dictionaries: Vec<_> = arrays.iter().map(|x| x.as_dictionary::<K>()).collect();
    let (should_merge, has_overflow) =
        should_merge_dictionary_values::<K>(&dictionaries, indices.len());
    if !should_merge {
        return if has_overflow {
            interleave_fallback(arrays, indices)
        } else {
            interleave_fallback_dictionary::<K>(&dictionaries, indices)
        };
    }

    let masks: Vec<_> = dictionaries
        .iter()
        .enumerate()
        .map(|(a_idx, dictionary)| {
            let mut key_mask = BooleanBufferBuilder::new_from_buffer(
                MutableBuffer::new_null(dictionary.len()),
                dictionary.len(),
            );

            for (_, key_idx) in indices.iter().filter(|(a, _)| *a == a_idx) {
                key_mask.set_bit(*key_idx, true);
            }
            key_mask.finish()
        })
        .collect();

    let merged = merge_dictionary_values(&dictionaries, Some(&masks))?;

    // Recompute keys
    let mut keys = PrimitiveBuilder::<K>::with_capacity(indices.len());
    for (a, b) in indices {
        let old_keys: &PrimitiveArray<K> = dictionaries[*a].keys();
        match old_keys.is_valid(*b) {
            true => {
                let old_key = old_keys.values()[*b];
                keys.append_value(merged.key_mappings[*a][old_key.as_usize()])
            }
            false => keys.append_null(),
        }
    }
    let array = unsafe { DictionaryArray::new_unchecked(keys.finish(), merged.values) };
    Ok(Arc::new(array))
}

fn interleave_views<T: ByteViewType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteViewArray<T>>::new(values, indices);
    let mut buffers = Vec::new();

    // Contains the offsets of start buffer in `buffer_to_new_index`
    let mut offsets = Vec::with_capacity(interleaved.arrays.len() + 1);
    offsets.push(0);
    let mut total_buffers = 0;
    for a in interleaved.arrays.iter() {
        total_buffers += a.data_buffers().len();
        offsets.push(total_buffers);
    }

    // contains the mapping from old buffer index to new buffer index
    let mut buffer_to_new_index = vec![None; total_buffers];

    let views: Vec<u128> = indices
        .iter()
        .map(|(array_idx, value_idx)| {
            let array = interleaved.arrays[*array_idx];
            let view = array.views().get(*value_idx).unwrap();
            let view_len = *view as u32;
            if view_len <= 12 {
                return *view;
            }
            // value is big enough to be in a variadic buffer
            let view = ByteView::from(*view);
            let buffer_to_new_idx = offsets[*array_idx] + view.buffer_index as usize;
            let new_buffer_idx: u32 =
                *buffer_to_new_index[buffer_to_new_idx].get_or_insert_with(|| {
                    buffers.push(array.data_buffers()[view.buffer_index as usize].clone());
                    (buffers.len() - 1) as u32
                });
            view.with_buffer_index(new_buffer_idx).as_u128()
        })
        .collect();

    let array = unsafe {
        GenericByteViewArray::<T>::new_unchecked(views.into(), buffers, interleaved.nulls)
    };
    Ok(Arc::new(array))
}

fn interleave_struct(
    fields: &Fields,
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, StructArray>::new(values, indices);

    if fields.is_empty() {
        let array = StructArray::try_new_with_length(
            fields.clone(),
            vec![],
            interleaved.nulls,
            indices.len(),
        )?;
        return Ok(Arc::new(array));
    }

    let struct_fields_array: Result<Vec<_>, _> = (0..fields.len())
        .map(|i| {
            let field_values: Vec<&dyn Array> = interleaved
                .arrays
                .iter()
                .map(|x| x.column(i).as_ref())
                .collect();
            interleave(&field_values, indices)
        })
        .collect();

    let struct_array =
        StructArray::try_new(fields.clone(), struct_fields_array?, interleaved.nulls)?;
    Ok(Arc::new(struct_array))
}

fn interleave_list<O: OffsetSizeTrait>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericListArray<O>>::new(values, indices);

    let mut capacity = 0usize;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(O::from_usize(0).unwrap());
    offsets.extend(indices.iter().map(|(array, row)| {
        let o = interleaved.arrays[*array].value_offsets();
        let element_len = o[*row + 1].as_usize() - o[*row].as_usize();
        capacity += element_len;
        O::from_usize(capacity).expect("offset overflow")
    }));

    let mut child_indices = Vec::with_capacity(capacity);
    for (array, row) in indices {
        let list = interleaved.arrays[*array];
        let start = list.value_offsets()[*row].as_usize();
        let end = list.value_offsets()[*row + 1].as_usize();
        child_indices.extend((start..end).map(|i| (*array, i)));
    }

    let child_arrays: Vec<&dyn Array> = interleaved
        .arrays
        .iter()
        .map(|list| list.values().as_ref())
        .collect();

    let interleaved_values = interleave(&child_arrays, &child_indices)?;

    let offsets = OffsetBuffer::new(offsets.into());
    let list_array = GenericListArray::<O>::new(
        field.clone(),
        offsets,
        interleaved_values,
        interleaved.nulls,
    );

    Ok(Arc::new(list_array))
}

/// Fallback implementation of interleave using [`MutableArrayData`]
fn interleave_fallback(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let arrays: Vec<_> = values.iter().map(|x| x.to_data()).collect();
    let arrays: Vec<_> = arrays.iter().collect();
    let mut array_data = MutableArrayData::new(arrays, false, indices.len());

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
    Ok(make_array(array_data.freeze()))
}

/// Fallback implementation for interleaving dictionaries when it was determined
/// that the dictionary values should not be merged. This implementation concatenates
/// the value slices and recomputes the resulting dictionary keys.
///
/// # Panics
///
/// This function assumes that the combined dictionary values will not overflow the
/// key type. Callers must verify this condition [`should_merge_dictionary_values`]
/// before calling this function.
fn interleave_fallback_dictionary<K: ArrowDictionaryKeyType>(
    dictionaries: &[&DictionaryArray<K>],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let relative_offsets: Vec<usize> = dictionaries
        .iter()
        .scan(0usize, |offset, dict| {
            let current = *offset;
            *offset += dict.values().len();
            Some(current)
        })
        .collect();
    let all_values: Vec<&dyn Array> = dictionaries.iter().map(|d| d.values().as_ref()).collect();
    let concatenated_values = concat(&all_values)?;

    let any_nulls = dictionaries.iter().any(|d| d.keys().nulls().is_some());
    let (new_keys, nulls) = if any_nulls {
        let mut has_nulls = false;
        let new_keys: Vec<K::Native> = indices
            .iter()
            .map(|(array, row)| {
                let old_keys = dictionaries[*array].keys();
                if old_keys.is_valid(*row) {
                    let old_key = old_keys.values()[*row].as_usize();
                    K::Native::from_usize(relative_offsets[*array] + old_key)
                        .expect("key overflow should be checked by caller")
                } else {
                    has_nulls = true;
                    K::Native::ZERO
                }
            })
            .collect();

        let nulls = if has_nulls {
            let null_buffer = BooleanBuffer::collect_bool(indices.len(), |i| {
                let (array, row) = indices[i];
                dictionaries[array].keys().is_valid(row)
            });
            Some(NullBuffer::new(null_buffer))
        } else {
            None
        };
        (new_keys, nulls)
    } else {
        let new_keys: Vec<K::Native> = indices
            .iter()
            .map(|(array, row)| {
                let old_key = dictionaries[*array].keys().values()[*row].as_usize();
                K::Native::from_usize(relative_offsets[*array] + old_key)
                    .expect("key overflow should be checked by caller")
            })
            .collect();
        (new_keys, None)
    };

    let keys_array = PrimitiveArray::<K>::new(new_keys.into(), nulls);
    // SAFETY: keys_array is constructed from a valid set of keys.
    let array = unsafe { DictionaryArray::new_unchecked(keys_array, concatenated_values) };
    Ok(Arc::new(array))
}

/// Interleave rows by index from multiple [`RecordBatch`] instances and return a new [`RecordBatch`].
///
/// This function will call [`interleave`] on each array of the [`RecordBatch`] instances and assemble a new [`RecordBatch`].
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{StringArray, Int32Array, RecordBatch, UInt32Array};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use arrow_select::interleave::interleave_record_batch;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("a", DataType::Int32, true),
///     Field::new("b", DataType::Utf8, true),
/// ]));
///
/// let batch1 = RecordBatch::try_new(
///     schema.clone(),
///     vec![
///         Arc::new(Int32Array::from(vec![0, 1, 2])),
///         Arc::new(StringArray::from(vec!["a", "b", "c"])),
///     ],
/// ).unwrap();
///
/// let batch2 = RecordBatch::try_new(
///     schema.clone(),
///     vec![
///         Arc::new(Int32Array::from(vec![3, 4, 5])),
///         Arc::new(StringArray::from(vec!["d", "e", "f"])),
///     ],
/// ).unwrap();
///
/// let indices = vec![(0, 1), (1, 2), (0, 0), (1, 1)];
/// let interleaved = interleave_record_batch(&[&batch1, &batch2], &indices).unwrap();
///
/// let expected = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 5, 0, 4])),
///         Arc::new(StringArray::from(vec!["b", "f", "a", "e"])),
///     ],
/// ).unwrap();
/// assert_eq!(interleaved, expected);
/// ```
pub fn interleave_record_batch(
    record_batches: &[&RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batches[0].schema();
    let columns = (0..schema.fields().len())
        .map(|i| {
            let column_values: Vec<&dyn Array> = record_batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect();
            interleave(&column_values, indices)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32RunArray;
    use arrow_array::builder::{GenericListBuilder, Int32Builder, PrimitiveRunBuilder};
    use arrow_array::types::Int8Type;
    use arrow_schema::Field;

    #[test]
    fn test_primitive() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter_values([5, 6, 7]);
        let c = Int32Array::from_iter_values([8, 9, 10]);
        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (2, 0), (1, 1)]).unwrap();
        let v = values.as_primitive::<Int32Type>();
        assert_eq!(v.values(), &[4, 4, 10, 8, 6]);
    }

    #[test]
    fn test_primitive_nulls() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let b = Int32Array::from_iter([Some(1), Some(4), None]);
        let values = interleave(&[&a, &b], &[(0, 1), (1, 2), (1, 2), (0, 3), (0, 2)]).unwrap();
        let v: Vec<_> = values.as_primitive::<Int32Type>().into_iter().collect();
        assert_eq!(&v, &[Some(2), None, None, Some(4), Some(3)])
    }

    #[test]
    fn test_primitive_empty() {
        let a = Int32Array::from_iter_values([1, 2, 3, 4]);
        let v = interleave(&[&a], &[]).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_strings() {
        let a = StringArray::from_iter_values(["a", "b", "c"]);
        let b = StringArray::from_iter_values(["hello", "world", "foo"]);
        let values = interleave(&[&a, &b], &[(0, 2), (0, 2), (1, 0), (1, 1), (0, 1)]).unwrap();
        let v = values.as_string::<i32>();
        let values: Vec<_> = v.into_iter().collect();
        assert_eq!(
            &values,
            &[
                Some("c"),
                Some("c"),
                Some("hello"),
                Some("world"),
                Some("b")
            ]
        )
    }

    #[test]
    fn test_interleave_dictionary() {
        let a = DictionaryArray::<Int32Type>::from_iter(["a", "b", "c", "a", "b"]);
        let b = DictionaryArray::<Int32Type>::from_iter(["a", "c", "a", "c", "a"]);

        // Should not recompute dictionary
        let values =
            interleave(&[&a, &b], &[(0, 2), (0, 2), (0, 2), (1, 0), (1, 1), (0, 1)]).unwrap();
        let v = values.as_dictionary::<Int32Type>();
        assert_eq!(v.values().len(), 5);

        let vc = v.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = vc.into_iter().map(Option::unwrap).collect();
        assert_eq!(&collected, &["c", "c", "c", "a", "c", "b"]);

        // Should recompute dictionary
        let values = interleave(&[&a, &b], &[(0, 2), (0, 2), (1, 1)]).unwrap();
        let v = values.as_dictionary::<Int32Type>();
        assert_eq!(v.values().len(), 1);

        let vc = v.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = vc.into_iter().map(Option::unwrap).collect();
        assert_eq!(&collected, &["c", "c", "c"]);
    }

    #[test]
    fn test_interleave_dictionary_nulls() {
        let input_1_keys = Int32Array::from_iter_values([0, 2, 1, 3]);
        let input_1_values = StringArray::from(vec![Some("foo"), None, Some("bar"), Some("fiz")]);
        let input_1 = DictionaryArray::new(input_1_keys, Arc::new(input_1_values));
        let input_2: DictionaryArray<Int32Type> = vec![None].into_iter().collect();

        let expected = vec![Some("fiz"), None, None, Some("foo")];

        let values = interleave(
            &[&input_1 as _, &input_2 as _],
            &[(0, 3), (0, 2), (1, 0), (0, 0)],
        )
        .unwrap();
        let dictionary = values.as_dictionary::<Int32Type>();
        let actual: Vec<Option<&str>> = dictionary
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_dictionary_overflow_same_values() {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..50).map(|i| format!("v{i}")),
        ));

        // With 3 dictionaries of 50 values each, relative_offsets = [0, 50, 100]
        // Accessing key 49 from dict3 gives 100 + 49 = 149 which overflows Int8
        // (max 127).
        // This test case falls back to interleave_fallback because the
        // dictionaries share the same underlying values slice.
        let dict1 = DictionaryArray::<Int8Type>::new(
            Int8Array::from_iter_values([0, 1, 2]),
            values.clone(),
        );
        let dict2 = DictionaryArray::<Int8Type>::new(
            Int8Array::from_iter_values([0, 1, 2]),
            values.clone(),
        );
        let dict3 =
            DictionaryArray::<Int8Type>::new(Int8Array::from_iter_values([49]), values.clone());

        let indices = &[(0, 0), (1, 0), (2, 0)];
        let result = interleave(&[&dict1, &dict2, &dict3], indices).unwrap();

        let dict_result = result.as_dictionary::<Int8Type>();
        let string_result: Vec<_> = dict_result
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(string_result, vec!["v0", "v0", "v49"]);
    }

    fn test_interleave_lists<O: OffsetSizeTrait>() {
        // [[1, 2], null, [3]]
        let mut a = GenericListBuilder::<O, _>::new(Int32Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.append(false);
        a.values().append_value(3);
        a.append(true);
        let a = a.finish();

        // [[4], null, [5, 6, null]]
        let mut b = GenericListBuilder::<O, _>::new(Int32Builder::new());
        b.values().append_value(4);
        b.append(true);
        b.append(false);
        b.values().append_value(5);
        b.values().append_value(6);
        b.values().append_null();
        b.append(true);
        let b = b.finish();

        let values = interleave(&[&a, &b], &[(0, 2), (0, 1), (1, 0), (1, 2), (1, 1)]).unwrap();
        let v = values
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        // [[3], null, [4], [5, 6, null], null]
        let mut expected = GenericListBuilder::<O, _>::new(Int32Builder::new());
        expected.values().append_value(3);
        expected.append(true);
        expected.append(false);
        expected.values().append_value(4);
        expected.append(true);
        expected.values().append_value(5);
        expected.values().append_value(6);
        expected.values().append_null();
        expected.append(true);
        expected.append(false);
        let expected = expected.finish();

        assert_eq!(v, &expected);
    }

    #[test]
    fn test_lists() {
        test_interleave_lists::<i32>();
    }

    #[test]
    fn test_large_lists() {
        test_interleave_lists::<i64>();
    }

    #[test]
    fn test_struct_without_nulls() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter_values([5, 6, 7]);
            let string_col = StringArray::from_iter_values(["hello", "world", "foo"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let c = {
            let number_col = Int32Array::from_iter_values([8, 9, 10]);
            let string_col = StringArray::from_iter_values(["x", "y", "z"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (2, 0), (1, 1)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));
        assert_eq!(values_struct.null_count(), 0);

        let values_number = values_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(values_number.values(), &[4, 4, 10, 8, 6]);
        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("d"), Some("d"), Some("z"), Some("x"), Some("world")]
        );
    }

    #[test]
    fn test_struct_with_nulls_in_values() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, true),
            Field::new("string_col", DataType::Utf8, true),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter([Some(1), Some(4), None]);
            let string_col = StringArray::from(vec![Some("hello"), None, Some("foo")]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b], &[(0, 1), (1, 2), (1, 2), (0, 3), (1, 1)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));

        // The struct itself has no nulls, but the values do
        assert_eq!(values_struct.null_count(), 0);

        let values_number: Vec<_> = values_struct
            .column(0)
            .as_primitive::<Int32Type>()
            .into_iter()
            .collect();
        assert_eq!(values_number, &[Some(2), None, None, Some(4), Some(4)]);

        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("b"), Some("foo"), Some("foo"), Some("d"), None]
        );
    }

    #[test]
    fn test_struct_with_nulls() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let b = {
            let number_col = Int32Array::from_iter_values([5, 6, 7]);
            let string_col = StringArray::from_iter_values(["hello", "world", "foo"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                Some(NullBuffer::from(&[true, false, true])),
            )
            .unwrap()
        };

        let c = {
            let number_col = Int32Array::from_iter_values([8, 9, 10]);
            let string_col = StringArray::from_iter_values(["x", "y", "z"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };

        let values = interleave(&[&a, &b, &c], &[(0, 3), (0, 3), (2, 2), (1, 1), (2, 0)]).unwrap();
        let values_struct = values.as_struct();
        assert_eq!(values_struct.data_type(), &DataType::Struct(fields));

        let validity: Vec<bool> = {
            let null_buffer = values_struct.nulls().expect("should_have_nulls");

            null_buffer.iter().collect()
        };
        assert_eq!(validity, &[true, true, true, false, true]);
        let values_number = values_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(values_number.values(), &[4, 4, 10, 6, 8]);
        let values_string = values_struct.column(1).as_string::<i32>();
        let values_string: Vec<_> = values_string.into_iter().collect();
        assert_eq!(
            &values_string,
            &[Some("d"), Some("d"), Some("z"), Some("world"), Some("x"),]
        );
    }

    #[test]
    fn test_struct_empty() {
        let fields = Fields::from(vec![
            Field::new("number_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]);
        let a = {
            let number_col = Int32Array::from_iter_values([1, 2, 3, 4]);
            let string_col = StringArray::from_iter_values(["a", "b", "c", "d"]);

            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(number_col), Arc::new(string_col)],
                None,
            )
            .unwrap()
        };
        let v = interleave(&[&a], &[]).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.data_type(), &DataType::Struct(fields));
    }

    #[test]
    fn interleave_sparse_nulls() {
        let values = StringArray::from_iter_values((0..100).map(|x| x.to_string()));
        let keys = Int32Array::from_iter_values(0..10);
        let dict_a = DictionaryArray::new(keys, Arc::new(values));
        let values = StringArray::new_null(0);
        let keys = Int32Array::new_null(10);
        let dict_b = DictionaryArray::new(keys, Arc::new(values));

        let indices = &[(0, 0), (0, 1), (0, 2), (1, 0)];
        let array = interleave(&[&dict_a, &dict_b], indices).unwrap();

        let expected =
            DictionaryArray::<Int32Type>::from_iter(vec![Some("0"), Some("1"), Some("2"), None]);
        assert_eq!(array.as_ref(), &expected)
    }

    #[test]
    fn test_interleave_views() {
        let values = StringArray::from_iter_values([
            "hello",
            "world_long_string_not_inlined",
            "foo",
            "bar",
            "baz",
        ]);
        let view_a = StringViewArray::from(&values);

        let values = StringArray::from_iter_values([
            "test",
            "data",
            "more_long_string_not_inlined",
            "views",
            "here",
        ]);
        let view_b = StringViewArray::from(&values);

        let indices = &[
            (0, 2), // "foo"
            (1, 0), // "test"
            (0, 4), // "baz"
            (1, 3), // "views"
            (0, 1), // "world_long_string_not_inlined"
        ];

        // Test specialized implementation
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let fallback = interleave_fallback(&[&view_a, &view_b], indices).unwrap();
        let fallback_result = fallback.as_string_view();
        // note that fallback_result has 2 buffers, but only one long enough string to warrant a buffer
        assert_eq!(fallback_result.data_buffers().len(), 2);

        // Convert to strings for easier assertion
        let collected: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();

        let fallback_collected: Vec<_> = fallback_result
            .iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect();

        assert_eq!(&collected, &fallback_collected);

        assert_eq!(
            &collected,
            &[
                Some("foo".to_string()),
                Some("test".to_string()),
                Some("baz".to_string()),
                Some("views".to_string()),
                Some("world_long_string_not_inlined".to_string()),
            ]
        );
    }

    #[test]
    fn test_interleave_views_with_nulls() {
        let values = StringArray::from_iter([
            Some("hello"),
            None,
            Some("foo_long_string_not_inlined"),
            Some("bar"),
            None,
        ]);
        let view_a = StringViewArray::from(&values);

        let values = StringArray::from_iter([
            Some("test"),
            Some("data_long_string_not_inlined"),
            None,
            None,
            Some("here"),
        ]);
        let view_b = StringViewArray::from(&values);

        let indices = &[
            (0, 1), // null
            (1, 2), // null
            (0, 2), // "foo_long_string_not_inlined"
            (1, 3), // null
            (0, 4), // null
        ];

        // Test specialized implementation
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();
        assert_eq!(result.data_buffers().len(), 1);

        let fallback = interleave_fallback(&[&view_a, &view_b], indices).unwrap();
        let fallback_result = fallback.as_string_view();

        // Convert to strings for easier assertion
        let collected: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();

        let fallback_collected: Vec<_> = fallback_result
            .iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect();

        assert_eq!(&collected, &fallback_collected);

        assert_eq!(
            &collected,
            &[
                None,
                None,
                Some("foo_long_string_not_inlined".to_string()),
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_interleave_views_multiple_buffers() {
        let str1 = "very_long_string_from_first_buffer".as_bytes();
        let str2 = "very_long_string_from_second_buffer".as_bytes();
        let buffer1 = str1.to_vec().into();
        let buffer2 = str2.to_vec().into();

        let view1 = ByteView::new(str1.len() as u32, &str1[..4])
            .with_buffer_index(0)
            .with_offset(0)
            .as_u128();
        let view2 = ByteView::new(str2.len() as u32, &str2[..4])
            .with_buffer_index(1)
            .with_offset(0)
            .as_u128();
        let view_a =
            StringViewArray::try_new(vec![view1, view2].into(), vec![buffer1, buffer2], None)
                .unwrap();

        let str3 = "another_very_long_string_buffer_three".as_bytes();
        let str4 = "different_long_string_in_buffer_four".as_bytes();
        let buffer3 = str3.to_vec().into();
        let buffer4 = str4.to_vec().into();

        let view3 = ByteView::new(str3.len() as u32, &str3[..4])
            .with_buffer_index(0)
            .with_offset(0)
            .as_u128();
        let view4 = ByteView::new(str4.len() as u32, &str4[..4])
            .with_buffer_index(1)
            .with_offset(0)
            .as_u128();
        let view_b =
            StringViewArray::try_new(vec![view3, view4].into(), vec![buffer3, buffer4], None)
                .unwrap();

        let indices = &[
            (0, 0), // String from first buffer of array A
            (1, 0), // String from first buffer of array B
            (0, 1), // String from second buffer of array A
            (1, 1), // String from second buffer of array B
            (0, 0), // String from first buffer of array A again
            (1, 1), // String from second buffer of array B again
        ];

        // Test interleave
        let values = interleave(&[&view_a, &view_b], indices).unwrap();
        let result = values.as_string_view();

        assert_eq!(
            result.data_buffers().len(),
            4,
            "Expected four buffers (two from each input array)"
        );

        let result_strings: Vec<_> = result.iter().map(|x| x.map(|s| s.to_string())).collect();
        assert_eq!(
            result_strings,
            vec![
                Some("very_long_string_from_first_buffer".to_string()),
                Some("another_very_long_string_buffer_three".to_string()),
                Some("very_long_string_from_second_buffer".to_string()),
                Some("different_long_string_in_buffer_four".to_string()),
                Some("very_long_string_from_first_buffer".to_string()),
                Some("different_long_string_in_buffer_four".to_string()),
            ]
        );

        let views = result.views();
        let buffer_indices: Vec<_> = views
            .iter()
            .map(|raw_view| ByteView::from(*raw_view).buffer_index)
            .collect();

        assert_eq!(
            buffer_indices,
            vec![
                0, // First buffer from array A
                1, // First buffer from array B
                2, // Second buffer from array A
                3, // Second buffer from array B
                0, // First buffer from array A (reused)
                3, // Second buffer from array B (reused)
            ]
        );
    }

    #[test]
    fn test_interleave_run_end_encoded_primitive() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1, 1, 2, 2, 2, 3].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6, 6, 6].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 1), (1, 0), (0, 4), (1, 2), (0, 5)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 4, 2, 5, 3];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic = "assertion `left == right` failed\n  left: [1, 4, 2, 5, 6]\n right: [2, 5, 2, 5, 6]"]
    // TODO: fix interleave of RunArrays to account for sliced RunArray's
    // https://github.com/apache/arrow-rs/issues/9018
    fn test_interleave_run_end_encoded_sliced() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1, 1, 2, 2, 2, 3].into_iter().map(Some));
        let a = builder.finish();
        let a = a.slice(2, 3); // [2, 2, 2]

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6, 6, 6].into_iter().map(Some));
        let b = builder.finish();
        let b = b.slice(1, 3); // [5, 5, 6]

        let indices = &[(0, 1), (1, 0), (0, 3), (1, 2), (1, 3)];
        let result = interleave(&[&a, &b], indices).unwrap();

        let result = result.as_run::<Int32Type>();
        let result = result.downcast::<Int32Array>().unwrap();

        let expected = vec![2, 5, 2, 5, 6];
        let actual = result.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_string() {
        let a: Int32RunArray = vec!["hello", "hello", "world", "world", "foo"]
            .into_iter()
            .collect();
        let b: Int32RunArray = vec!["bar", "baz", "baz", "qux"].into_iter().collect();

        let indices = &[(0, 0), (1, 1), (0, 3), (1, 3), (0, 4)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec!["hello", "baz", "world", "qux", "foo"];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_string::<i32>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_with_nulls() {
        let a: Int32RunArray = vec![Some("a"), Some("a"), None, None, Some("b")]
            .into_iter()
            .collect();
        let b: Int32RunArray = vec![None, Some("c"), Some("c"), Some("d")]
            .into_iter()
            .collect();

        let indices = &[(0, 1), (1, 0), (0, 2), (1, 3), (0, 4)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![Some("a"), None, None, Some("d"), Some("b")];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            if result_run_array.values().is_null(physical_idx) {
                actual.push(None);
            } else {
                let value = result_run_array
                    .values()
                    .as_string::<i32>()
                    .value(physical_idx);
                actual.push(Some(value));
            }
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_different_run_types() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        builder.extend([1, 1, 2, 3, 3].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        builder.extend([4, 5, 5, 6].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 0), (1, 1), (0, 3), (1, 3)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &RunArray<Int16Type> = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 5, 3, 6];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_mixed_run_lengths() {
        let mut builder = PrimitiveRunBuilder::<Int64Type, Int32Type>::new();
        builder.extend([1, 2, 2, 2, 2, 3, 3, 4].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int64Type, Int32Type>::new();
        builder.extend([5, 5, 5, 6, 7, 7, 8, 8].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[
            (0, 0), // 1
            (1, 2), // 5
            (0, 3), // 2
            (1, 3), // 6
            (0, 6), // 3
            (1, 6), // 8
            (0, 7), // 4
            (1, 4), // 7
        ];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &RunArray<Int64Type> = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 5, 2, 6, 3, 8, 4, 7];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_interleave_run_end_encoded_empty_runs() {
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1].into_iter().map(Some));
        let a = builder.finish();

        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([2, 2, 2].into_iter().map(Some));
        let b = builder.finish();

        let indices = &[(0, 0), (1, 1), (1, 2)];
        let result = interleave(&[&a, &b], indices).unwrap();

        // The result should be a RunEndEncoded array
        assert!(matches!(result.data_type(), DataType::RunEndEncoded(_, _)));

        // Cast to RunArray to access values
        let result_run_array: &Int32RunArray = result.as_any().downcast_ref().unwrap();

        // Verify the logical values by accessing the logical array directly
        let expected = vec![1, 2, 2];
        let mut actual = Vec::new();
        for i in 0..result_run_array.len() {
            let physical_idx = result_run_array.get_physical_index(i);
            let value = result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .value(physical_idx);
            actual.push(value);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_struct_no_fields() {
        let fields = Fields::empty();
        let a = StructArray::try_new_with_length(fields.clone(), vec![], None, 10).unwrap();
        let v = interleave(&[&a], &[(0, 0)]).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v.data_type(), &DataType::Struct(fields));
    }

    #[test]
    fn test_interleave_fallback_dictionary_with_nulls() {
        let input_1_keys = Int32Array::from_iter([Some(0), None, Some(1)]);
        let input_1_values = StringArray::from_iter_values(["foo", "bar"]);
        let dict_a = DictionaryArray::new(input_1_keys, Arc::new(input_1_values));

        let input_2_keys = Int32Array::from_iter([Some(0), Some(1), None]);
        let input_2_values = StringArray::from_iter_values(["baz", "qux"]);
        let dict_b = DictionaryArray::new(input_2_keys, Arc::new(input_2_values));

        let indices = vec![
            (0, 0), // "foo"
            (0, 1), // null
            (1, 0), // "baz"
            (1, 2), // null
            (0, 2), // "bar"
            (1, 1), // "qux"
        ];

        let result =
            interleave_fallback_dictionary::<Int32Type>(&[&dict_a, &dict_b], &indices).unwrap();
        let dict_result = result.as_dictionary::<Int32Type>();

        let string_result = dict_result.downcast_dict::<StringArray>().unwrap();
        let collected: Vec<_> = string_result.into_iter().collect();
        assert_eq!(
            collected,
            vec![
                Some("foo"),
                None,
                Some("baz"),
                None,
                Some("bar"),
                Some("qux")
            ]
        );
    }
}
