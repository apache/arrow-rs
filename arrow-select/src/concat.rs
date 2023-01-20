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
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::as_dictionary_array;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_data::transform::{Capacities, MutableArrayData};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use num::Integer;
use std::sync::Arc;

fn binary_capacity<T: ByteArrayType>(arrays: &[&dyn Array]) -> Capacities {
    let mut item_capacity = 0;
    let mut bytes_capacity = 0;
    for array in arrays {
        let a = array
            .as_any()
            .downcast_ref::<GenericByteArray<T>>()
            .unwrap();

        // Guaranteed to always have at least one element
        let offsets = a.value_offsets();
        bytes_capacity += offsets[offsets.len() - 1].as_usize() - offsets[0].as_usize();
        item_capacity += a.len()
    }

    Capacities::Binary(item_capacity, Some(bytes_capacity))
}

fn concat_dictionaries<K>(arrays: &[&dyn Array]) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    K::Native: Integer,
{
    let output_len = arrays.iter().map(|x| x.len()).sum();
    if !should_merge_dictionary_values::<K>(arrays, output_len) {
        return concat_fallback(arrays, Capacities::Array(output_len));
    }

    // Recompute dictionaries
    let dictionaries: Vec<_> = arrays
        .iter()
        .map(|a| (as_dictionary_array::<K>(*a), None))
        .collect();

    let merged = merge_dictionary_values(&dictionaries)?;

    // Recompute keys
    let mut keys = PrimitiveBuilder::<K>::with_capacity(output_len);

    for ((d, _), mapping) in dictionaries.iter().zip(merged.key_mappings) {
        for key in d.keys_iter() {
            keys.append_option(key.map(|x| mapping[x]));
        }
    }
    let keys = keys.finish().into_data();

    // Sanity check
    assert_eq!(keys.len(), output_len);

    let builder = keys
        .into_builder()
        .data_type(DataType::Dictionary(
            Box::new(K::DATA_TYPE),
            Box::new(merged.values.data_type().clone()),
        ))
        .child_data(vec![merged.values.data().clone()]);

    let data = unsafe { builder.build_unchecked() };
    Ok(Arc::new(DictionaryArray::<K>::from(data)))
}

macro_rules! dict_helper {
    ($t:ty, $arrays:expr) => {
        return Ok(Arc::new(concat_dictionaries::<$t>($arrays)?) as _)
    };
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
            "It is not possible to concatenate arrays of different data types."
                .to_string(),
        ));
    }

    let capacity = match d {
        DataType::Utf8 => binary_capacity::<Utf8Type>(arrays),
        DataType::LargeUtf8 => binary_capacity::<LargeUtf8Type>(arrays),
        DataType::Binary => binary_capacity::<BinaryType>(arrays),
        DataType::LargeBinary => binary_capacity::<LargeBinaryType>(arrays),
        DataType::Dictionary(k, v) if v.is_byte_array() => downcast_integer! {
            k.as_ref() => (dict_helper, arrays),
            _ => unreachable!("illegal dictionary key type {k}")
        },
        _ => Capacities::Array(arrays.iter().map(|a| a.len()).sum()),
    };

    concat_fallback(arrays, capacity)
}

fn concat_fallback(
    arrays: &[&dyn Array],
    capacity: Capacities,
) -> Result<ArrayRef, ArrowError> {
    let array_data = arrays.iter().map(|a| a.data()).collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(array_data, false, capacity);

    for (i, a) in arrays.iter().enumerate() {
        mutable.extend(i, 0, a.len())
    }

    Ok(make_array(mutable.freeze()))
}

/// Concatenates `batches` together into a single record batch.
pub fn concat_batches<'a>(
    schema: &SchemaRef,
    input_batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> Result<RecordBatch, ArrowError> {
    let batches: Vec<&RecordBatch> = input_batches.into_iter().collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    if let Some((i, _)) = batches
        .iter()
        .enumerate()
        .find(|&(_, batch)| batch.schema() != *schema)
    {
        return Err(ArrowError::InvalidArgumentError(format!(
            "batches[{}] schema is different with argument schema.",
            i
        )));
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
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_concat_empty_vec() {
        let re = concat(&[]);
        assert!(re.is_err());
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
            &PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]),
            &PrimitiveArray::<Int64Type>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]),
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
        let input_1 = PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
        ])
        .slice(1, 3);

        let input_2 = PrimitiveArray::<Int64Type>::from(vec![
            Some(101),
            Some(102),
            Some(103),
            None,
        ])
        .slice(1, 3);
        let arr = concat(&[input_1.as_ref(), input_2.as_ref()]).unwrap();

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
        let list1_array =
            ListArray::from_iter_primitive::<Int64Type, _, _>(list1.clone());

        let list2 = vec![
            None,
            Some(vec![Some(100), None, Some(101)]),
            Some(vec![Some(102)]),
        ];
        let list2_array =
            ListArray::from_iter_primitive::<Int64Type, _, _>(list2.clone());

        let list3 = vec![Some(vec![Some(1000), Some(1001)])];
        let list3_array =
            ListArray::from_iter_primitive::<Int64Type, _, _>(list3.clone());

        let array_result = concat(&[&list1_array, &list2_array, &list3_array]).unwrap();

        let expected = list1
            .into_iter()
            .chain(list2.into_iter())
            .chain(list3.into_iter());
        let array_expected = ListArray::from_iter_primitive::<Int64Type, _, _>(expected);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);
    }

    #[test]
    fn test_concat_struct_arrays() {
        let field = Field::new("field", DataType::Int64, true);
        let input_primitive_1: ArrayRef =
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef =
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]));
        let input_struct_2 = StructArray::from(vec![(field.clone(), input_primitive_2)]);

        let input_primitive_3: ArrayRef =
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
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
        let field = Field::new("field", DataType::Int64, true);
        let input_primitive_1: ArrayRef =
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef =
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]));
        let input_struct_2 = StructArray::from(vec![(field, input_primitive_2)]);

        let arr = concat(&[
            input_struct_1.slice(1, 3).as_ref(),
            input_struct_2.slice(1, 2).as_ref(),
        ])
        .unwrap();

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

        let arr = concat(&[input_1.slice(1, 3).as_ref(), input_2.slice(1, 2).as_ref()])
            .unwrap();

        let expected_output = StringArray::from(vec!["A", "B", "C", "D", "E"]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);
    }

    #[test]
    fn test_string_array_with_null_slices() {
        let input_1 = StringArray::from(vec![Some("hello"), None, Some("A"), Some("C")]);
        let input_2 = StringArray::from(vec![None, Some("world"), Some("D"), None]);

        let arr = concat(&[input_1.slice(1, 3).as_ref(), input_2.slice(1, 2).as_ref()])
            .unwrap();

        let expected_output =
            StringArray::from(vec![None, Some("A"), Some("C"), Some("world"), Some("D")]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);
    }

    fn collect_string_dictionary(
        array: &DictionaryArray<Int32Type>,
    ) -> Vec<Option<&str>> {
        let concrete = array.downcast_dict::<StringArray>().unwrap();
        concrete.into_iter().collect()
    }

    #[test]
    fn test_string_dictionary_array() {
        let input_1: DictionaryArray<Int32Type> =
            vec!["hello", "A", "B", "hello", "hello", "C"]
                .into_iter()
                .collect();
        let input_2: DictionaryArray<Int32Type> =
            vec!["hello", "E", "E", "hello", "F", "E"]
                .into_iter()
                .collect();

        let expected: Vec<_> = vec![
            "hello", "A", "B", "hello", "hello", "C", "hello", "E", "E", "hello", "F",
            "E",
        ]
        .into_iter()
        .map(Some)
        .collect();

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = as_dictionary_array::<Int32Type>(concat.as_ref());
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
        let input_1: DictionaryArray<Int32Type> =
            vec![Some("foo"), Some("bar"), None, Some("fiz")]
                .into_iter()
                .collect();
        let input_2: DictionaryArray<Int32Type> = vec![None].into_iter().collect();
        let expected = vec![Some("foo"), Some("bar"), None, Some("fiz"), None];

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = as_dictionary_array::<Int32Type>(concat.as_ref());
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);

        // Should have concatenated inputs together
        assert_eq!(
            dictionary.values().len(),
            input_1.values().len() + input_2.values().len(),
        )
    }

    #[test]
    fn test_string_dictionary_merge() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..20 {
            builder.append(&i.to_string()).unwrap();
        }
        let input_1 = builder.finish();

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..30 {
            builder.append(&i.to_string()).unwrap();
        }
        let input_2 = builder.finish();

        let expected: Vec<_> = (0..20).chain(0..30).map(|x| x.to_string()).collect();
        let expected: Vec<_> = expected.iter().map(|x| Some(x.as_str())).collect();

        let concat = concat(&[&input_1 as _, &input_2 as _]).unwrap();
        let dictionary = as_dictionary_array::<Int32Type>(concat.as_ref());
        let actual = collect_string_dictionary(dictionary);
        assert_eq!(actual, expected);

        // Should have merged inputs together
        // Not 30 as this is done on a best-effort basis
        assert_eq!(dictionary.values().len(), 33)
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
        assert_eq!(arr.data().buffers()[1].capacity(), 960);
    }

    #[test]
    fn test_dictionary_concat_reuse() {
        let array: DictionaryArray<Int8Type> =
            vec!["a", "a", "b", "c"].into_iter().collect();
        let copy: DictionaryArray<Int8Type> = array.clone();

        // dictionary is "a", "b", "c"
        assert_eq!(
            array.values(),
            &(Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef)
        );
        assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));

        // concatenate it with itself
        let combined = concat(&[&copy as _, &array as _]).unwrap();
        let combined = as_dictionary_array::<Int8Type>(combined.as_ref());

        assert_eq!(
            combined.values(),
            &(Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef),
            "Actual: {:#?}",
            combined
        );

        assert_eq!(
            combined.keys(),
            &Int8Array::from(vec![0, 0, 1, 2, 0, 0, 1, 2])
        );

        // Should have reused the dictionary
        assert!(array.data().child_data()[0].ptr_eq(&combined.data().child_data()[0]));
        assert!(copy.data().child_data()[0].ptr_eq(&combined.data().child_data()[0]));

        let new: DictionaryArray<Int8Type> = vec!["d"].into_iter().collect();
        let combined = concat(&[&copy as _, &array as _, &new as _]).unwrap();

        // Should not have reused the dictionary
        assert!(!array.data().child_data()[0].ptr_eq(&combined.data().child_data()[0]));
        assert!(!copy.data().child_data()[0].ptr_eq(&combined.data().child_data()[0]));
        assert!(!new.data().child_data()[0].ptr_eq(&combined.data().child_data()[0]));
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
    fn concat_record_batches_of_different_schemas() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();
        let error = concat_batches(&schema1, [&batch1, &batch2]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: batches[1] schema is different with argument schema.",
        );
    }

    #[test]
    fn concat_capacity() {
        let a = Int32Array::from_iter_values(0..100);
        let b = Int32Array::from_iter_values(10..20);
        let a = concat(&[&a, &b]).unwrap();
        let data = a.data();
        assert_eq!(data.buffers()[0].len(), 440);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 20), &b]).unwrap();
        let data = a.data();
        assert_eq!(data.buffers()[0].len(), 120);
        assert_eq!(data.buffers()[0].capacity(), 128); // Nearest multiple of 64

        let a = StringArray::from_iter_values(std::iter::repeat("foo").take(100));
        let b = StringArray::from(vec!["bingo", "bongo", "lorem", ""]);

        let a = concat(&[&a, &b]).unwrap();
        let data = a.data();
        // (100 + 4 + 1) * size_of<i32>()
        assert_eq!(data.buffers()[0].len(), 420);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        // len("foo") * 100 + len("bingo") + len("bongo") + len("lorem")
        assert_eq!(data.buffers()[1].len(), 315);
        assert_eq!(data.buffers()[1].capacity(), 320); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 40), &b]).unwrap();
        let data = a.data();
        // (40 + 4 + 5) * size_of<i32>()
        assert_eq!(data.buffers()[0].len(), 180);
        assert_eq!(data.buffers()[0].capacity(), 192); // Nearest multiple of 64

        // len("foo") * 40 + len("bingo") + len("bongo") + len("lorem")
        assert_eq!(data.buffers()[1].len(), 135);
        assert_eq!(data.buffers()[1].capacity(), 192); // Nearest multiple of 64

        let a = LargeBinaryArray::from_iter_values(std::iter::repeat(b"foo").take(100));
        let b =
            LargeBinaryArray::from_iter_values(std::iter::repeat(b"cupcakes").take(10));

        let a = concat(&[&a, &b]).unwrap();
        let data = a.data();
        // (100 + 10 + 1) * size_of<i64>()
        assert_eq!(data.buffers()[0].len(), 888);
        assert_eq!(data.buffers()[0].capacity(), 896); // Nearest multiple of 64

        // len("foo") * 100 + len("cupcakes") * 10
        assert_eq!(data.buffers()[1].len(), 380);
        assert_eq!(data.buffers()[1].capacity(), 384); // Nearest multiple of 64

        let a = concat(&[&a.slice(10, 40), &b]).unwrap();
        let data = a.data();
        // (40 + 10 + 1) * size_of<i64>()
        assert_eq!(data.buffers()[0].len(), 408);
        assert_eq!(data.buffers()[0].capacity(), 448); // Nearest multiple of 64

        // len("foo") * 40 + len("cupcakes") * 10
        assert_eq!(data.buffers()[1].len(), 200);
        assert_eq!(data.buffers()[1].capacity(), 256); // Nearest multiple of 64
    }
}
