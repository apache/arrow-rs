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

//! Defines concat kernel for `ArrayRef`
//!
//! Example:
//!
//! ```
//! use arrow_array::{ArrayRef, StringArray};
//! use arrow_select::concat::concat;
//!
//! let arr = concat(&[
//!     &StringArray::from(vec!["hello", "world"]),
//!     &StringArray::from(vec!["!"]),
//! ]).unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use crate::dictionary::{merge_dictionary_values, should_merge_dictionary_values};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_data::transform::{Capacities, MutableArrayData};
use arrow_schema::{ArrowError, DataType, FieldRef, SchemaRef};
use std::sync::Arc;

fn binary_capacity<T: ByteArrayType>(arrays: &[&dyn Array]) -> Capacities {
    let mut item_capacity = 0;
    let mut bytes_capacity = 0;
    for array in arrays {
        let a = array.as_bytes::<T>();

        // Guaranteed to always have at least one element
        let offsets = a.value_offsets();
        bytes_capacity += offsets[offsets.len() - 1].as_usize() - offsets[0].as_usize();
        item_capacity += a.len()
    }

    Capacities::Binary(item_capacity, Some(bytes_capacity))
}

fn fixed_size_list_capacity(arrays: &[&dyn Array], data_type: &DataType) -> Capacities {
    if let DataType::FixedSizeList(f, _) = data_type {
        let item_capacity = arrays.iter().map(|a| a.len()).sum();
        let child_data_type = f.data_type();
        match child_data_type {
            // These types should match the types that `get_capacity`
            // has special handling for.
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeList(_, _) => {
                let values: Vec<&dyn arrow_array::Array> = arrays
                    .iter()
                    .map(|a| a.as_fixed_size_list().values().as_ref())
                    .collect();
                Capacities::List(
                    item_capacity,
                    Some(Box::new(get_capacity(&values, child_data_type))),
                )
            }
            _ => Capacities::Array(item_capacity),
        }
    } else {
        unreachable!("illegal data type for fixed size list")
    }
}

fn concat_dictionaries<K: ArrowDictionaryKeyType>(
    arrays: &[&dyn Array],
) -> Result<ArrayRef, ArrowError> {
    let mut output_len = 0;
    let dictionaries: Vec<_> = arrays
        .iter()
        .map(|x| x.as_dictionary::<K>())
        .inspect(|d| output_len += d.len())
        .collect();

    if !should_merge_dictionary_values::<K>(&dictionaries, output_len) {
        return concat_fallback(arrays, Capacities::Array(output_len));
    }

    let merged = merge_dictionary_values(&dictionaries, None)?;

    // Recompute keys
    let mut key_values = Vec::with_capacity(output_len);

    let mut has_nulls = false;
    for (d, mapping) in dictionaries.iter().zip(merged.key_mappings) {
        has_nulls |= d.null_count() != 0;
        for key in d.keys().values() {
            // Use get to safely handle nulls
            key_values.push(mapping.get(key.as_usize()).copied().unwrap_or_default())
        }
    }

    let nulls = has_nulls.then(|| {
        let mut nulls = BooleanBufferBuilder::new(output_len);
        for d in &dictionaries {
            match d.nulls() {
                Some(n) => nulls.append_buffer(n.inner()),
                None => nulls.append_n(d.len(), true),
            }
        }
        NullBuffer::new(nulls.finish())
    });

    let keys = PrimitiveArray::<K>::new(key_values.into(), nulls);
    // Sanity check
    assert_eq!(keys.len(), output_len);

    let array = unsafe { DictionaryArray::new_unchecked(keys, merged.values) };
    Ok(Arc::new(array))
}

fn concat_lists<OffsetSize: OffsetSizeTrait>(
    arrays: &[&dyn Array],
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let mut output_len = 0;
    let mut list_has_nulls = false;
    let mut list_has_slices = false;

    let lists = arrays
        .iter()
        .map(|x| x.as_list::<OffsetSize>())
        .inspect(|l| {
            output_len += l.len();
            list_has_nulls |= l.null_count() != 0;
            list_has_slices |= l.offsets()[0] > OffsetSize::zero()
                || l.offsets().last().unwrap().as_usize() < l.values().len();
        })
        .collect::<Vec<_>>();

    let lists_nulls = list_has_nulls.then(|| {
        let mut nulls = BooleanBufferBuilder::new(output_len);
        for l in &lists {
            match l.nulls() {
                Some(n) => nulls.append_buffer(n.inner()),
                None => nulls.append_n(l.len(), true),
            }
        }
        NullBuffer::new(nulls.finish())
    });

    // If any of the lists have slices, we need to slice the values
    // to ensure that the offsets are correct
    let mut sliced_values;
    let values: Vec<&dyn Array> = if list_has_slices {
        sliced_values = Vec::with_capacity(lists.len());
        for l in &lists {
            // if the first offset is non-zero, we need to slice the values so when
            // we concatenate them below only the relevant values are included
            let offsets = l.offsets();
            let start_offset = offsets[0].as_usize();
            let end_offset = offsets.last().unwrap().as_usize();
            sliced_values.push(l.values().slice(start_offset, end_offset - start_offset));
        }
        sliced_values.iter().map(|a| a.as_ref()).collect()
    } else {
        lists.iter().map(|x| x.values().as_ref()).collect()
    };

    let concatenated_values = concat(values.as_slice())?;

    // Merge value offsets from the lists
    let value_offset_buffer =
        OffsetBuffer::<OffsetSize>::from_lengths(lists.iter().flat_map(|x| x.offsets().lengths()));

    let array = GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        value_offset_buffer,
        concatenated_values,
        lists_nulls,
    )?;

    Ok(Arc::new(array))
}

macro_rules! dict_helper {
    ($t:ty, $arrays:expr) => {
        return Ok(Arc::new(concat_dictionaries::<$t>($arrays)?) as _)
    };
}

fn get_capacity(arrays: &[&dyn Array], data_type: &DataType) -> Capacities {
    match data_type {
        DataType::Utf8 => binary_capacity::<Utf8Type>(arrays),
        DataType::LargeUtf8 => binary_capacity::<LargeUtf8Type>(arrays),
        DataType::Binary => binary_capacity::<BinaryType>(arrays),
        DataType::LargeBinary => binary_capacity::<LargeBinaryType>(arrays),
        DataType::FixedSizeList(_, _) => fixed_size_list_capacity(arrays, data_type),
        _ => Capacities::Array(arrays.iter().map(|a| a.len()).sum()),
    }
}

/// Concatenate multiple [Array] of the same type into a single [ArrayRef].
pub fn concat(arrays: &[&dyn Array]) -> Result<ArrayRef, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::ComputeError(
            "concat requires input of at least one array".to_string(),
        ));
    } else if arrays.len() == 1 {
        let array = arrays[0];
        return Ok(array.slice(0, array.len()));
    }

    let d = arrays[0].data_type();
    if arrays.iter().skip(1).any(|array| array.data_type() != d) {
        return Err(ArrowError::InvalidArgumentError(
            "It is not possible to concatenate arrays of different data types.".to_string(),
        ));
    }

    match d {
        DataType::Dictionary(k, _) => {
            downcast_integer! {
                k.as_ref() => (dict_helper, arrays),
                _ => unreachable!("illegal dictionary key type {k}")
            }
        }
        DataType::List(field) => concat_lists::<i32>(arrays, field),
        DataType::LargeList(field) => concat_lists::<i64>(arrays, field),
        _ => {
            let capacity = get_capacity(arrays, d);
            concat_fallback(arrays, capacity)
        }
    }
}

/// Concatenates arrays using MutableArrayData
///
/// This will naively concatenate dictionaries
fn concat_fallback(arrays: &[&dyn Array], capacity: Capacities) -> Result<ArrayRef, ArrowError> {
    let array_data: Vec<_> = arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
    let array_data = array_data.iter().collect();
    let mut mutable = MutableArrayData::with_capacities(array_data, false, capacity);

    for (i, a) in arrays.iter().enumerate() {
        mutable.extend(i, 0, a.len())
    }

    Ok(make_array(mutable.freeze()))
}

/// Concatenates `batches` together into a single [`RecordBatch`].
///
/// The output batch has the specified `schemas`; The schema of the
/// input are ignored.
///
/// Returns an error if the types of underlying arrays are different.
pub fn concat_batches<'a>(
    schema: &SchemaRef,
    input_batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> Result<RecordBatch, ArrowError> {
    // When schema is empty, sum the number of the rows of all batches
    if schema.fields().is_empty() {
        let num_rows: usize = input_batches.into_iter().map(RecordBatch::num_rows).sum();
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(num_rows);
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options);
    }

    let batches: Vec<&RecordBatch> = input_batches.into_iter().collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = concat(
            &batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        arrays.push(array);
    }
    RecordBatch::try_new(schema.clone(), arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{GenericListBuilder, StringDictionaryBuilder};
    use arrow_schema::{Field, Schema};
    use std::fmt::Debug;

    #[test]
    fn test_concat_empty_vec() {
        let re = concat(&[]);
        assert!(re.is_err());
    }

    #[test]
    fn test_concat_batches_no_columns() {
        // Test concat using empty schema / batches without columns
        let schema = Arc::new(Schema::empty());

        let mut options = RecordBatchOptions::default();
        options.row_count = Some(100);
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();
        // put in 2 batches of 100 rows each
        let re = concat_batches(&schema, &[batch.clone(), batch]).unwrap();

        assert_eq!(re.num_rows(), 200);
    }

    #[test]
    fn test_concat_one_element_vec() {
        let arr = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(2),
            None,
        ])) as ArrayRef;
        let result = concat(&[arr.as_ref()]).unwrap();
        assert_eq!(
            &arr, &result,
            "concatenating single element array gives back the same result"
        );
    }

    #[test]
    fn test_concat_incompatible_datatypes() {
        let re = concat(&[
            &PrimitiveArray::<Int64Type>::from(vec![Some(-1), Some(2), None]),
            &StringArray::from(vec![Some("hello"), Some("bar"), Some("world")]),
        ]);
        assert!(re.is_err());
    }

    #[test]
    fn test_concat_string_arrays() {
        let arr = concat(&[
            &StringArray::from(vec!["hello", "world"]),
            &StringArray::from(vec!["2", "3", "4"]),
            &StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]),
        ])
        .unwrap();

        let expected_output = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("foo"),
            Some("bar"),
            None,
            Some("baz"),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);
    }

    #[test]
    fn test_concat_primitive_arrays() {
        let arr = concat(&[
            &PrimitiveArray::<Int64Type>::from(vec![Some(-1), Some(-1), Some(2), None, None]),
            &PrimitiveArray::<Int64Type>::from(vec![Some(101), Some(102), Some(103), None]),
            &PrimitiveArray::<Int64Type>::from(vec![Some(256), Some(512), Some(1024)]),
        ])
        .unwrap();

        let expected_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);
    }

    #[test]
    fn test_concat_primitive_array_slices() {
        let input_1 =
            PrimitiveArray::<Int64Type>::from(vec![Some(-1), Some(-1), Some(2), None, None])
                .slice(1, 3);

        let input_2 =
            PrimitiveArray::<Int64Type>::from(vec![Some(101), Some(102), Some(103), None])
                .slice(1, 3);
        let arr = concat(&[&input_1, &input_2]).unwrap();

        let expected_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(2),
            None,
            Some(102),
            Some(103),
            None,
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);
    }

    #[test]
    fn test_concat_boolean_primitive_arrays() {
        let arr = concat(&[
            &BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                None,
                None,
                Some(false),
            ]),
            &BooleanArray::from(vec![None, Some(false), Some(true), Some(false)]),
        ])
        .unwrap();

        let expected_output = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(false),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);
    }

    #[test]
    fn test_concat_primitive_list_arrays() {
        let list1 = vec![
            Some(vec![Some(-1), Some(-1), Some(2), None, None]),
            Some(vec![]),
            None,
            Some(vec![Some(10)]),
        ];
        let list1_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list1.clone());

        let list2 = vec![
            None,
            Some(vec![Some(100), None, Some(101)]),
            Some(vec![Some(102)]),
        ];
        let list2_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list2.clone());

        let list3 = vec![Some(vec![Some(1000), Some(1001)])];
        let list3_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list3.clone());

        let array_result = concat(&[&list1_array, &list2_array, &list3_array]).unwrap();

        let expected = list1.into_iter().chain(list2).chain(list3);
        let array_expected = ListArray::from_iter_primitive::<Int64Type, _, _>(expected);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);
    }

    #[test]
    fn test_concat_primitive_list_arrays_slices() {
        let list1 = vec![
            Some(vec![Some(-1), Some(-1), Some(2), None, None]),
            Some(vec![]), // In slice
            None,         // In slice
            Some(vec![Some(10)]),
        ];
        let list1_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list1.clone());
        let list1_array = list1_array.slice(1, 2);
        let list1_values = list1.into_iter().skip(1).take(2);

        let list2 = vec![
            None,
            Some(vec![Some(100), None, Some(101)]),
            Some(vec![Some(102)]),
        ];
        let list2_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list2.clone());

        // verify that this test covers the case when the first offset is non zero
        assert!(list1_array.offsets()[0].as_usize() > 0);
        let array_result = concat(&[&list1_array, &list2_array]).unwrap();

        let expected = list1_values.chain(list2);
        let array_expected = ListArray::from_iter_primitive::<Int64Type, _, _>(expected);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);
    }

    #[test]
    fn test_concat_primitive_list_arrays_sliced_lengths() {
        let list1 = vec![
            Some(vec![Some(-1), Some(-1), Some(2), None, None]), // In slice
            Some(vec![]),                                        // In slice
            None,                                                // In slice
            Some(vec![Some(10)]),
        ];
        let list1_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list1.clone());
        let list1_array = list1_array.slice(0, 3); // no offset, but not all values
        let list1_values = list1.into_iter().take(3);

        let list2 = vec![
            None,
            Some(vec![Some(100), None, Some(101)]),
            Some(vec![Some(102)]),
        ];
        let list2_array = ListArray::from_iter_primitive::<Int64Type, _, _>(list2.clone());

        // verify that this test covers the case when the first offset is zero, but the
        // last offset doesn't cover the entire array
        assert_eq!(list1_array.offsets()[0].as_usize(), 0);
        assert!(list1_array.offsets().last().unwrap().as_usize() < list1_array.values().len());
        let array_result = concat(&[&list1_array, &list2_array]).unwrap();

        let expected = list1_values.chain(list2);
        let array_expected = ListArray::from_iter_primitive::<Int64Type, _, _>(expected);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);
    }

    #[test]
    fn test_concat_primitive_fixed_size_list_arrays() {
        let list1 = vec![
            Some(vec![Some(-1), None]),
            None,
            Some(vec![Some(10), Some(20)]),
        ];
        let list1_array =
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(list1.clone(), 2);

        let list2 = vec![
            None,
            Some(vec![Some(100), None]),
            Some(vec![Some(102), Some(103)]),
        ];
        let list2_array =
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(list2.clone(), 2);

        let list3 = vec![Some(vec![Some(1000), Some(1001)])];
        let list3_array =
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(list3.clone(), 2);

        let array_result = concat(&[&list1_array, &list2_array, &list3_array]).unwrap();

        let expected = list1.into_iter().chain(list2).chain(list3);
        let array_expected =
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(expected, 2);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);
    }

    #[test]
    fn test_concat_struct_arrays() {
        let field = Arc::new(Field::new("field", DataType::Int64, true));
        let input_primitive_1: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
        ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(101),
            Some(102),
            Some(103),
            None,
        ]));
        let input_struct_2 = StructArray::from(vec![(field.clone(), input_primitive_2)]);

        let input_primitive_3: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(256),
            Some(512),
            Some(1024),
        ]));
        let input_struct_3 = StructArray::from(vec![(field, input_primitive_3)]);

        let arr = concat(&[&input_struct_1, &input_struct_2, &input_struct_3]).unwrap();

        let expected_primitive_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])) as ArrayRef;

        let actual_primitive = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0);
        assert_eq!(actual_primitive, &expected_primitive_output);
    }

    #[test]
    fn test_concat_struct_array_slices() {
        let field = Arc::new(Field::new("field", DataType::Int64, true));
        let input_primitive_1: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
        ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(101),
            Some(102),
            Some(103),
            None,
        ]));
        let input_struct_2 = StructArray::from(vec![(field, input_primitive_2)]);

        let arr = concat(&[&input_struct_1.slice(1, 3), &input_struct_2.slice(1, 2)]).unwrap();

        let expected_primitive_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(2),
            None,
            Some(102),
            Some(103),
        ])) as ArrayRef;

        let actual_primitive = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0);
        assert_eq!(actual_primitive, &expected_primitive_output);
    }

    #[test]
    fn test_string_array_slices() {
        let input_1 = StringArray::from(vec!["hello", "A", "B", "C"]);
        let input_2 = StringArray::from(vec!["world", "D", "E", "Z"]);

        let arr = concat(&[&input_1.slice(1, 3), &input_2.slice(1, 2)]).unwrap();

        let expected_output = StringArray::from(vec!["A", "B", "C", "D", "E"]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);
    }

    #[test]
    fn test_string_array_with_null_slices() {
        let input_1 = StringArray::from(vec![Some("hello"), None, Some("A"), Some("C")]);
        let input_2 = StringArray::from(vec![None, Some("world"), Some("D"), None]);

        let arr = concat(&[&input_1.slice(1, 3), &input_2.slice(1, 2)]).unwrap();

        let expected_output =
            StringArray::from(vec![None, Some("A"), Some("C"), Some("world"), Some("D")]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);
    }

    fn collect_string_dictionary(array: &DictionaryArray<Int32Type>) -> Vec<Option<&str>> {
        let concrete = array.downcast_dict::<StringArray>().unwrap();
        concrete.into_iter().collect()
    }

    #[test]
    fn test_string_dictionary_array() {
        let input_1: DictionaryArray<Int32Type> = vec!["hello", "A", "B", "hello", "hello", "C"]
            .into_iter()
            .collect();
        let input_2: DictionaryArray<Int32Type> = vec!["hello", "E", "E", "hello", "F", "E"]
            .into_iter()
            .collect();

        let expected: Vec<_> = vec![
            "hello", "A", "B", "hello", "hello", "C", "hello", "E", "E", "hello", "F", "E",
        ]
        .into_iter()
        .map(Some)
        .collect();

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = concat.as_dictionary::<Int32Type>();
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);

        // Should have concatenated inputs together
        assert_eq!(
            dictionary.values().len(),
            input_1.values().len() + input_2.values().len(),
        )
    }

    #[test]
    fn test_string_dictionary_array_nulls() {
        let input_1: DictionaryArray<Int32Type> = vec![Some("foo"), Some("bar"), None, Some("fiz")]
            .into_iter()
            .collect();
        let input_2: DictionaryArray<Int32Type> = vec![None].into_iter().collect();
        let expected = vec![Some("foo"), Some("bar"), None, Some("fiz"), None];

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = concat.as_dictionary::<Int32Type>();
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);

        // Should have concatenated inputs together
        assert_eq!(
            dictionary.values().len(),
            input_1.values().len() + input_2.values().len(),
        )
    }

    #[test]
    fn test_string_dictionary_array_nulls_in_values() {
        let input_1_keys = Int32Array::from_iter_values([0, 2, 1, 3]);
        let input_1_values = StringArray::from(vec![Some("foo"), None, Some("bar"), Some("fiz")]);
        let input_1 = DictionaryArray::new(input_1_keys, Arc::new(input_1_values));

        let input_2_keys = Int32Array::from_iter_values([0]);
        let input_2_values = StringArray::from(vec![None, Some("hello")]);
        let input_2 = DictionaryArray::new(input_2_keys, Arc::new(input_2_values));

        let expected = vec![Some("foo"), Some("bar"), None, Some("fiz"), None];

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = concat.as_dictionary::<Int32Type>();
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_string_dictionary_merge() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..20 {
            builder.append(i.to_string()).unwrap();
        }
        let input_1 = builder.finish();

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..30 {
            builder.append(i.to_string()).unwrap();
        }
        let input_2 = builder.finish();

        let expected: Vec<_> = (0..20).chain(0..30).map(|x| x.to_string()).collect();
        let expected: Vec<_> = expected.iter().map(|x| Some(x.as_str())).collect();

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = concat.as_dictionary::<Int32Type>();
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);

        // Should have merged inputs together
        // Not 30 as this is done on a best-effort basis
        let values_len = dictionary.values().len();
        assert!((30..40).contains(&values_len), "{values_len}")
    }

    #[test]
    fn test_concat_string_sizes() {
        let a: LargeStringArray = ((0..150).map(|_| Some("foo"))).collect();
        let b: LargeStringArray = ((0..150).map(|_| Some("foo"))).collect();
        let c = LargeStringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        // 150 * 3 = 450
        // 150 * 3 = 450
        // 3 * 3   = 9
        // ------------+
        // 909
        // closest 64 byte aligned cap = 960

        let arr = concat(&[&a, &b, &c]).unwrap();
        // this would have been 1280 if we did not precompute the value lengths.
        assert_eq!(arr.to_data().buffers()[1].capacity(), 960);
    }

    #[test]
    fn test_dictionary_concat_reuse() {
        let array: DictionaryArray<Int8Type> = vec!["a", "a", "b", "c"].into_iter().collect();
        let copy: DictionaryArray<Int8Type> = array.clone();

        // dictionary is "a", "b", "c"
        assert_eq!(
            array.values(),
            &(Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef)
        );
        assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));

        // concatenate it with itself
        let combined = concat(&[&copy as _, &array as _]).unwrap();
        let combined = combined.as_dictionary::<Int8Type>();

        assert_eq!(
            combined.values(),
            &(Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef),
            "Actual: {combined:#?}"
        );

        assert_eq!(
            combined.keys(),
            &Int8Array::from(vec![0, 0, 1, 2, 0, 0, 1, 2])
        );

        // Should have reused the dictionary
        assert!(array
            .values()
            .to_data()
            .ptr_eq(&combined.values().to_data()));
        assert!(copy.values().to_data().ptr_eq(&combined.values().to_data()));

        let new: DictionaryArray<Int8Type> = vec!["d"].into_iter().collect();
        let combined = concat(&[&copy as _, &array as _, &new as _]).unwrap();
        let com = combined.as_dictionary::<Int8Type>();

        // Should not have reused the dictionary
        assert!(!array.values().to_data().ptr_eq(&com.values().to_data()));
        assert!(!copy.values().to_data().ptr_eq(&com.values().to_data()));
        assert!(!new.values().to_data().ptr_eq(&com.values().to_data()));
    }

    #[test]
    fn concat_record_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();
        let new_batch = concat_batches(&schema, [&batch1, &batch2]).unwrap();
        assert_eq!(new_batch.schema().as_ref(), schema.as_ref());
        assert_eq!(2, new_batch.num_columns());
        assert_eq!(4, new_batch.num_rows());
        let new_batch_owned = concat_batches(&schema, &[batch1, batch2]).unwrap();
        assert_eq!(new_batch_owned.schema().as_ref(), schema.as_ref());
        assert_eq!(2, new_batch_owned.num_columns());
        assert_eq!(4, new_batch_owned.num_rows());
    }

    #[test]
    fn concat_empty_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = concat_batches(&schema, []).unwrap();
        assert_eq!(batch.schema().as_ref(), schema.as_ref());
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn concat_record_batches_of_different_schemas_but_compatible_data() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // column names differ
        let schema2 = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let batch2 =
            RecordBatch::try_new(schema2, vec![Arc::new(Int32Array::from(vec![3, 4]))]).unwrap();
        // concat_batches simply uses the schema provided
        let batch = concat_batches(&schema1, [&batch1, &batch2]).unwrap();
        assert_eq!(batch.schema().as_ref(), schema1.as_ref());
        assert_eq!(4, batch.num_rows());
    }

    #[test]
    fn concat_record_batches_of_different_schemas_incompatible_data() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // column names differ
        let schema2 = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema2,
            vec![Arc::new(StringArray::from(vec!["foo", "bar"]))],
        )
        .unwrap();

        let error = concat_batches(&schema1, [&batch1, &batch2]).unwrap_err();
        assert_eq!(error.to_string(), "Invalid argument error: It is not possible to concatenate arrays of different data types.");
    }

    #[test]
    fn concat_capacity() {
        let a = Int32Array::from_iter_values(0..100);
        let b = Int32Array::from_iter_values(10..20);
        let a = concat(&[&a, &b]).unwrap();
        let data = a.to_data();
        assert_eq!(data.buffers()[0].len(), 440);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 20), &b]).unwrap();
        let data = a.to_data();
        assert_eq!(data.buffers()[0].len(), 120);
        assert_eq!(data.buffers()[0].capacity(), 128); // Nearest multiple of 64

        let a = StringArray::from_iter_values(std::iter::repeat("foo").take(100));
        let b = StringArray::from(vec!["bingo", "bongo", "lorem", ""]);

        let a = concat(&[&a, &b]).unwrap();
        let data = a.to_data();
        // (100 + 4 + 1) * size_of<i32>()
        assert_eq!(data.buffers()[0].len(), 420);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        // len("foo") * 100 + len("bingo") + len("bongo") + len("lorem")
        assert_eq!(data.buffers()[1].len(), 315);
        assert_eq!(data.buffers()[1].capacity(), 320); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 40), &b]).unwrap();
        let data = a.to_data();
        // (40 + 4 + 5) * size_of<i32>()
        assert_eq!(data.buffers()[0].len(), 180);
        assert_eq!(data.buffers()[0].capacity(), 192); // Nearest multiple of 64

        // len("foo") * 40 + len("bingo") + len("bongo") + len("lorem")
        assert_eq!(data.buffers()[1].len(), 135);
        assert_eq!(data.buffers()[1].capacity(), 192); // Nearest multiple of 64

        let a = LargeBinaryArray::from_iter_values(std::iter::repeat(b"foo").take(100));
        let b = LargeBinaryArray::from_iter_values(std::iter::repeat(b"cupcakes").take(10));

        let a = concat(&[&a, &b]).unwrap();
        let data = a.to_data();
        // (100 + 10 + 1) * size_of<i64>()
        assert_eq!(data.buffers()[0].len(), 888);
        assert_eq!(data.buffers()[0].capacity(), 896); // Nearest multiple of 64

        // len("foo") * 100 + len("cupcakes") * 10
        assert_eq!(data.buffers()[1].len(), 380);
        assert_eq!(data.buffers()[1].capacity(), 384); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 40), &b]).unwrap();
        let data = a.to_data();
        // (40 + 10 + 1) * size_of<i64>()
        assert_eq!(data.buffers()[0].len(), 408);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        // len("foo") * 40 + len("cupcakes") * 10
        assert_eq!(data.buffers()[1].len(), 200);
        assert_eq!(data.buffers()[1].capacity(), 256); // Nearest multiple of 64
    }

    #[test]
    fn concat_sparse_nulls() {
        let values = StringArray::from_iter_values((0..100).map(|x| x.to_string()));
        let keys = Int32Array::from(vec![1; 10]);
        let dict_a = DictionaryArray::new(keys, Arc::new(values));
        let values = StringArray::new_null(0);
        let keys = Int32Array::new_null(10);
        let dict_b = DictionaryArray::new(keys, Arc::new(values));
        let array = concat(&[&dict_a, &dict_b]).unwrap();
        assert_eq!(array.null_count(), 10);
        assert_eq!(array.logical_null_count(), 10);
    }

    #[test]
    fn concat_dictionary_list_array_simple() {
        let scalars = vec![
            create_single_row_list_of_dict(vec![Some("a")]),
            create_single_row_list_of_dict(vec![Some("a")]),
            create_single_row_list_of_dict(vec![Some("b")]),
        ];

        let arrays = scalars
            .iter()
            .map(|a| a as &(dyn Array))
            .collect::<Vec<_>>();
        let concat_res = concat(arrays.as_slice()).unwrap();

        let expected_list = create_list_of_dict(vec![
            // Row 1
            Some(vec![Some("a")]),
            Some(vec![Some("a")]),
            Some(vec![Some("b")]),
        ]);

        let list = concat_res.as_list::<i32>();

        // Assert that the list is equal to the expected list
        list.iter().zip(expected_list.iter()).for_each(|(a, b)| {
            assert_eq!(a, b);
        });

        assert_dictionary_has_unique_values::<_, StringArray>(
            list.values().as_dictionary::<Int32Type>(),
        );
    }

    #[test]
    fn concat_many_dictionary_list_arrays() {
        let number_of_unique_values = 8;
        let scalars = (0..80000)
            .map(|i| {
                create_single_row_list_of_dict(vec![Some(
                    (i % number_of_unique_values).to_string(),
                )])
            })
            .collect::<Vec<_>>();

        let arrays = scalars
            .iter()
            .map(|a| a as &(dyn Array))
            .collect::<Vec<_>>();
        let concat_res = concat(arrays.as_slice()).unwrap();

        let expected_list = create_list_of_dict(
            (0..80000)
                .map(|i| Some(vec![Some((i % number_of_unique_values).to_string())]))
                .collect::<Vec<_>>(),
        );

        let list = concat_res.as_list::<i32>();

        // Assert that the list is equal to the expected list
        list.iter().zip(expected_list.iter()).for_each(|(a, b)| {
            assert_eq!(a, b);
        });

        assert_dictionary_has_unique_values::<_, StringArray>(
            list.values().as_dictionary::<Int32Type>(),
        );
    }

    fn create_single_row_list_of_dict(
        list_items: Vec<Option<impl AsRef<str>>>,
    ) -> GenericListArray<i32> {
        let rows = list_items.into_iter().map(Some).collect();

        create_list_of_dict(vec![rows])
    }

    fn create_list_of_dict(
        rows: Vec<Option<Vec<Option<impl AsRef<str>>>>>,
    ) -> GenericListArray<i32> {
        let mut builder =
            GenericListBuilder::<i32, _>::new(StringDictionaryBuilder::<Int32Type>::new());

        for row in rows {
            builder.append_option(row);
        }

        builder.finish()
    }

    fn assert_dictionary_has_unique_values<'a, K, V>(array: &'a DictionaryArray<K>)
    where
        K: ArrowDictionaryKeyType,
        V: Sync + Send + 'static,
        &'a V: ArrayAccessor + IntoIterator,

        <&'a V as ArrayAccessor>::Item: Default + Clone + PartialEq + Debug + Ord,
        <&'a V as IntoIterator>::Item: Clone + PartialEq + Debug + Ord,
    {
        let dict = array.downcast_dict::<V>().unwrap();
        let mut values = dict.values().into_iter().collect::<Vec<_>>();

        // remove duplicates must be sorted first so we can compare
        values.sort();

        let mut unique_values = values.clone();

        unique_values.dedup();

        assert_eq!(
            values, unique_values,
            "There are duplicates in the value list (the value list here is sorted which is only for the assertion)"
        );
    }
}
