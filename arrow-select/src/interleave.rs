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
use arrow_data::ByteView;
use arrow_schema::{ArrowError, DataType};
use std::collections::HashMap;
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

fn interleave_views<T: ByteViewType>(
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef, ArrowError> {
    let interleaved = Interleave::<'_, GenericByteViewArray<T>>::new(values, indices);
    let mut views_builder = BufferBuilder::new(indices.len());
    let mut buffers = Vec::new();

    // (input array_index, input buffer_index) -> output buffer_index
    let mut buffer_lookup: HashMap<(usize, u32), u32> = HashMap::new();
    for (array_idx, value_idx) in indices {
        let array = interleaved.arrays[*array_idx];
        let raw_view = array.views().get(*value_idx).unwrap();
        let view_len = *raw_view as u32;
        if view_len <= 12 {
            views_builder.append(*raw_view);
            continue;
        }
        // value is big enough to be in a variadic buffer
        let view = ByteView::from(*raw_view);
        let new_buffer_idx: &mut u32 = buffer_lookup
            .entry((*array_idx, view.buffer_index))
            .or_insert_with(|| {
                buffers.push(array.data_buffers()[view.buffer_index as usize].clone());
                (buffers.len() - 1) as u32
            });
        views_builder.append(view.with_buffer_index(*new_buffer_idx).into());
    }

    let array = unsafe {
        GenericByteViewArray::<T>::new_unchecked(views_builder.into(), buffers, interleaved.nulls)
    };
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
}
