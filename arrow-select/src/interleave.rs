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

use crate::dictionary::{merge_dictionary_values, should_merge_dictionary_values};
use arrow_array::builder::{BooleanBufferBuilder, BufferBuilder, PrimitiveBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, MutableBuffer, NullBuffer, NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType};
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
        DataType::Dictionary(k, _) => downcast_integer! {
            k.as_ref() => (dict_helper, values, indices),
            _ => unreachable!("illegal dictionary key type {k}")
        },
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
                let mut builder = NullBufferBuilder::new(indices.len());
                for (a, b) in indices {
                    let v = arrays[*a].is_valid(*b);
                    builder.append(v)
                }
                builder.finish()
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

    let mut values = Vec::with_capacity(indices.len());
    for (a, b) in indices {
        let v = interleaved.arrays[*a].value(*b);
        values.push(v)
    }

    let array = PrimitiveArray::<T>::new(values.into(), interleaved.nulls);
    Ok(Arc::new(array.with_data_type(data_type.clone())))
}

fn interleave_bytes<T: ByteArrayType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteArray<T>>::new(values, indices);

    let mut capacity = 0;
    let mut offsets = BufferBuilder::<T::Offset>::new(indices.len() + 1);
    offsets.append(T::Offset::from_usize(0).unwrap());
    for (a, b) in indices {
        let o = interleaved.arrays[*a].value_offsets();
        let element_len = o[*b + 1].as_usize() - o[*b].as_usize();
        capacity += element_len;
        offsets.append(T::Offset::from_usize(capacity).expect("overflow"));
    }

    let mut values = MutableBuffer::new(capacity);
    for (a, b) in indices {
        values.extend_from_slice(interleaved.arrays[*a].value(*b).as_ref());
    }

    // Safety: safe by construction
    let array = unsafe {
        let offsets = OffsetBuffer::new_unchecked(offsets.finish().into());
        GenericByteArray::<T>::new_unchecked(offsets, values.into(), interleaved.nulls)
    };
    Ok(Arc::new(array))
}

fn interleave_dictionaries<K: ArrowDictionaryKeyType>(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let dictionaries: Vec<_> = arrays.iter().map(|x| x.as_dictionary::<K>()).collect();
    if !should_merge_dictionary_values::<K>(&dictionaries, indices.len()) {
        return interleave_fallback(arrays, indices);
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};

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
    fn test_lists() {
        // [[1, 2], null, [3]]
        let mut a = ListBuilder::new(Int32Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.append(false);
        a.values().append_value(3);
        a.append(true);
        let a = a.finish();

        // [[4], null, [5, 6, null]]
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(4);
        b.append(true);
        b.append(false);
        b.values().append_value(5);
        b.values().append_value(6);
        b.values().append_null();
        b.append(true);
        let b = b.finish();

        let values = interleave(&[&a, &b], &[(0, 2), (0, 1), (1, 0), (1, 2), (1, 1)]).unwrap();
        let v = values.as_any().downcast_ref::<ListArray>().unwrap();

        // [[3], null, [4], [5, 6, null], null]
        let mut expected = ListBuilder::new(Int32Builder::new());
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
}
