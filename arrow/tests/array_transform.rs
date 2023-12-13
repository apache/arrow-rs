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

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Int16Array, Int32Array, Int64Array, Int64Builder, ListArray, ListBuilder, MapBuilder,
    NullArray, StringArray, StringBuilder, StringDictionaryBuilder, StructArray, UInt8Array,
    UnionArray,
};
use arrow::datatypes::Int16Type;
use arrow_buffer::Buffer;
use arrow_data::transform::MutableArrayData;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;

#[allow(unused)]
fn create_decimal_array(array: Vec<Option<i128>>, precision: u8, scale: i8) -> Decimal128Array {
    array
        .into_iter()
        .collect::<Decimal128Array>()
        .with_precision_and_scale(precision, scale)
        .unwrap()
}

#[test]
#[cfg(not(feature = "force_validate"))]
fn test_decimal() {
    let decimal_array =
        create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3).into_data();
    let arrays = vec![&decimal_array];
    let mut a = MutableArrayData::new(arrays, true, 3);
    a.extend(0, 0, 3);
    a.extend(0, 2, 3);
    let result = a.freeze();
    let array = Decimal128Array::from(result);
    let expected = create_decimal_array(vec![Some(1), Some(2), None, None], 10, 3);
    assert_eq!(array, expected);
}
#[test]
#[cfg(not(feature = "force_validate"))]
fn test_decimal_offset() {
    let decimal_array = create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3);
    let decimal_array = decimal_array.slice(1, 3).into_data(); // 2, null, 3
    let arrays = vec![&decimal_array];
    let mut a = MutableArrayData::new(arrays, true, 2);
    a.extend(0, 0, 2); // 2, null
    let result = a.freeze();
    let array = Decimal128Array::from(result);
    let expected = create_decimal_array(vec![Some(2), None], 10, 3);
    assert_eq!(array, expected);
}

#[test]
#[cfg(not(feature = "force_validate"))]
fn test_decimal_null_offset_nulls() {
    let decimal_array = create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3);
    let decimal_array = decimal_array.slice(1, 3).into_data(); // 2, null, 3
    let arrays = vec![&decimal_array];
    let mut a = MutableArrayData::new(arrays, true, 2);
    a.extend(0, 0, 2); // 2, null
    a.extend_nulls(3); // 2, null, null, null, null
    a.extend(0, 1, 3); //2, null, null, null, null, null, 3
    let result = a.freeze();
    let array = Decimal128Array::from(result);
    let expected =
        create_decimal_array(vec![Some(2), None, None, None, None, None, Some(3)], 10, 3);
    assert_eq!(array, expected);
}

/// tests extending from a primitive array w/ offset nor nulls
#[test]
fn test_primitive() {
    let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]).into_data();
    let arrays = vec![&b];
    let mut a = MutableArrayData::new(arrays, false, 3);
    a.extend(0, 0, 2);
    let result = a.freeze();
    let array = UInt8Array::from(result);
    let expected = UInt8Array::from(vec![Some(1), Some(2)]);
    assert_eq!(array, expected);
}

/// tests extending from a primitive array with offset w/ nulls
#[test]
fn test_primitive_offset() {
    let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]).into_data();
    let b = b.slice(1, 2);
    let arrays = vec![&b];
    let mut a = MutableArrayData::new(arrays, false, 2);
    a.extend(0, 0, 2);
    let result = a.freeze();
    let array = UInt8Array::from(result);
    let expected = UInt8Array::from(vec![Some(2), Some(3)]);
    assert_eq!(array, expected);
}

/// tests extending from a primitive array with offset and nulls
#[test]
fn test_primitive_null_offset() {
    let b = UInt8Array::from(vec![Some(1), None, Some(3)]);
    let b = b.slice(1, 2).into_data();
    let arrays = vec![&b];
    let mut a = MutableArrayData::new(arrays, false, 2);
    a.extend(0, 0, 2);
    let result = a.freeze();
    let array = UInt8Array::from(result);
    let expected = UInt8Array::from(vec![None, Some(3)]);
    assert_eq!(array, expected);
}

#[test]
fn test_primitive_null_offset_nulls() {
    let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]).into_data();
    let b = b.slice(1, 2);
    let arrays = vec![&b];
    let mut a = MutableArrayData::new(arrays, true, 2);
    a.extend(0, 0, 2);
    a.extend_nulls(3);
    a.extend(0, 1, 2);
    let result = a.freeze();
    let array = UInt8Array::from(result);
    let expected = UInt8Array::from(vec![Some(2), Some(3), None, None, None, Some(3)]);
    assert_eq!(array, expected);
}

#[test]
fn test_list_null_offset() {
    let int_builder = Int64Builder::with_capacity(24);
    let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
    builder.values().append_slice(&[1, 2, 3]);
    builder.append(true);
    builder.values().append_slice(&[4, 5]);
    builder.append(true);
    builder.values().append_slice(&[6, 7, 8]);
    builder.append(true);
    let array = builder.finish().into_data();
    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);
    mutable.extend(0, 0, 1);

    let result = mutable.freeze();
    let array = ListArray::from(result);

    let int_builder = Int64Builder::with_capacity(24);
    let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
    builder.values().append_slice(&[1, 2, 3]);
    builder.append(true);
    let expected = builder.finish();

    assert_eq!(array, expected);
}

/// tests extending from a variable-sized (strings and binary) array w/ offset with nulls
#[test]
fn test_variable_sized_nulls() {
    let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]).into_data();
    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);

    let result = mutable.freeze();
    let result = StringArray::from(result);

    let expected = StringArray::from(vec![Some("bc"), None]);
    assert_eq!(result, expected);
}

/// tests extending from a variable-sized (strings and binary) array
/// with an offset and nulls
#[test]
fn test_variable_sized_offsets() {
    let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
    let array = array.into_data().slice(1, 3);

    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 0, 3);

    let result = mutable.freeze();
    let result = StringArray::from(result);

    let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
    assert_eq!(result, expected);
}

#[test]
fn test_string_offsets() {
    let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
    let array = array.into_data().slice(1, 3);

    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 0, 3);

    let result = mutable.freeze();
    let result = StringArray::from(result);

    let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
    assert_eq!(result, expected);
}

#[test]
fn test_multiple_with_nulls() {
    let array1 = StringArray::from(vec!["hello", "world"]).into_data();
    let array2 = StringArray::from(vec![Some("1"), None]).into_data();

    let arrays = vec![&array1, &array2];

    let mut mutable = MutableArrayData::new(arrays, false, 5);

    mutable.extend(0, 0, 2);
    mutable.extend(1, 0, 2);

    let result = mutable.freeze();
    let result = StringArray::from(result);

    let expected = StringArray::from(vec![Some("hello"), Some("world"), Some("1"), None]);
    assert_eq!(result, expected);
}

#[test]
fn test_string_null_offset_nulls() {
    let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
    let array = array.into_data().slice(1, 3);

    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, true, 0);

    mutable.extend(0, 1, 3);
    mutable.extend_nulls(1);

    let result = mutable.freeze();
    let result = StringArray::from(result);

    let expected = StringArray::from(vec![None, Some("defh"), None]);
    assert_eq!(result, expected);
}

#[test]
fn test_bool() {
    let array = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]).into_data();
    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);

    let result = mutable.freeze();
    let result = BooleanArray::from(result);

    let expected = BooleanArray::from(vec![Some(true), None]);
    assert_eq!(result, expected);
}

#[test]
fn test_null() {
    let array1 = NullArray::new(10).into_data();
    let array2 = NullArray::new(5).into_data();
    let arrays = vec![&array1, &array2];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);
    mutable.extend(1, 0, 1);

    let result = mutable.freeze();
    let result = NullArray::from(result);

    let expected = NullArray::new(3);
    assert_eq!(result, expected);
}

fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayData {
    let values = StringArray::from(values.to_vec());
    let mut builder =
        StringDictionaryBuilder::<Int16Type>::new_with_dictionary(keys.len(), &values).unwrap();
    for key in keys {
        if let Some(v) = key {
            builder.append(v).unwrap();
        } else {
            builder.append_null()
        }
    }
    builder.finish().into_data()
}

#[test]
fn test_dictionary() {
    // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
    let array = create_dictionary_array(&["a", "b", "c"], &[Some("a"), Some("b"), None, Some("c")]);
    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);

    let result = mutable.freeze();
    let result = DictionaryArray::from(result);

    let expected = Int16Array::from(vec![Some(1), None]);
    assert_eq!(result.keys(), &expected);
}

#[test]
fn test_struct() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));

    let array = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
        .unwrap()
        .into_data();
    let arrays = vec![&array];
    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);
    let data = mutable.freeze();
    let array = StructArray::from(data);

    let expected =
        StructArray::try_from(vec![("f1", strings.slice(1, 2)), ("f2", ints.slice(1, 2))]).unwrap();
    assert_eq!(array, expected)
}

#[test]
fn test_struct_offset() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));

    let array = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
        .unwrap()
        .into_data()
        .slice(1, 3);
    let arrays = vec![&array];
    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);
    let data = mutable.freeze();
    let array = StructArray::from(data);

    let expected_strings: ArrayRef = Arc::new(StringArray::from(vec![None, Some("mark")]));
    let expected =
        StructArray::try_from(vec![("f1", expected_strings), ("f2", ints.slice(2, 2))]).unwrap();

    assert_eq!(array, expected);
}

#[test]
fn test_struct_nulls() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        Some(5),
    ]));

    let array = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
        .unwrap()
        .into_data();
    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);
    let data = mutable.freeze();
    let array = StructArray::from(data);

    let v: Vec<Option<&str>> = vec![None, None];
    let expected_string = Arc::new(StringArray::from(v)) as ArrayRef;
    let expected_int = Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef;

    let expected =
        StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)]).unwrap();
    assert_eq!(array, expected)
}

#[test]
fn test_struct_many() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        Some(5),
    ]));

    let array = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
        .unwrap()
        .into_data();
    let arrays = vec![&array, &array];
    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 3);
    mutable.extend(1, 0, 2);
    let data = mutable.freeze();
    let array = StructArray::from(data);

    let expected_string =
        Arc::new(StringArray::from(vec![None, None, Some("joe"), None])) as ArrayRef;
    let expected_int =
        Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)])) as ArrayRef;

    let expected =
        StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)]).unwrap();
    assert_eq!(array, expected)
}

#[test]
fn test_union_dense() {
    // Input data
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        Some("mark"),
        Some("doe"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    let offsets = Buffer::from_slice_ref([0, 0, 1, 1, 2, 2, 3, 4i32]);
    let type_ids = Buffer::from_slice_ref([42, 84, 42, 84, 84, 42, 84, 84i8]);

    let array = UnionArray::try_new(
        &[84, 42],
        type_ids,
        Some(offsets),
        vec![
            (Field::new("int", DataType::Int32, false), ints),
            (Field::new("string", DataType::Utf8, false), strings),
        ],
    )
    .unwrap()
    .into_data();
    let arrays = vec![&array];
    let mut mutable = MutableArrayData::new(arrays, false, 0);

    // Slice it by `MutableArrayData`
    mutable.extend(0, 4, 7);
    let data = mutable.freeze();
    let array = UnionArray::from(data);

    // Expected data
    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("doe")]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(4)]));
    let offsets = Buffer::from_slice_ref([0, 0, 1i32]);
    let type_ids = Buffer::from_slice_ref([84, 42, 84i8]);

    let expected = UnionArray::try_new(
        &[84, 42],
        type_ids,
        Some(offsets),
        vec![
            (Field::new("int", DataType::Int32, false), ints),
            (Field::new("string", DataType::Utf8, false), strings),
        ],
    )
    .unwrap();

    assert_eq!(array.to_data(), expected.to_data());
}

#[test]
fn test_binary_fixed_sized_offsets() {
    let array =
        FixedSizeBinaryArray::try_from_iter(vec![vec![0, 0], vec![0, 1], vec![0, 2]].into_iter())
            .expect("Failed to create FixedSizeBinaryArray from iterable");
    let array = array.slice(1, 2).into_data();
    // = [[0, 1], [0, 2]] due to the offset = 1

    let arrays = vec![&array];

    let mut mutable = MutableArrayData::new(arrays, false, 0);

    mutable.extend(0, 1, 2);
    mutable.extend(0, 0, 1);

    let result = mutable.freeze();
    let result = FixedSizeBinaryArray::from(result);

    let expected = FixedSizeBinaryArray::try_from_iter(vec![vec![0, 2], vec![0, 1]].into_iter())
        .expect("Failed to create FixedSizeBinaryArray from iterable");
    assert_eq!(result, expected);
}

#[test]
fn test_list_append() {
    let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(24));
    builder.values().append_slice(&[1, 2, 3]);
    builder.append(true);
    builder.values().append_slice(&[4, 5]);
    builder.append(true);
    builder.values().append_slice(&[6, 7, 8]);
    builder.values().append_slice(&[9, 10, 11]);
    builder.append(true);
    let a = builder.finish().into_data();

    let a_builder = Int64Builder::with_capacity(24);
    let mut a_builder = ListBuilder::<Int64Builder>::new(a_builder);
    a_builder.values().append_slice(&[12, 13]);
    a_builder.append(true);
    a_builder.append(true);
    a_builder.values().append_slice(&[14, 15]);
    a_builder.append(true);
    let b = a_builder.finish().into_data();

    let c = b.slice(1, 2);

    let mut mutable = MutableArrayData::new(vec![&a, &b, &c], false, 1);
    mutable.extend(0, 0, a.len());
    mutable.extend(1, 0, b.len());
    mutable.extend(2, 0, c.len());

    let finished = mutable.freeze();

    let expected_int_array = Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
        Some(10),
        Some(11),
        // append first array
        Some(12),
        Some(13),
        Some(14),
        Some(15),
        // append second array
        Some(14),
        Some(15),
    ]);
    let list_value_offsets = Buffer::from_slice_ref([0i32, 3, 5, 11, 13, 13, 15, 15, 17]);
    let expected_list_data = ArrayData::try_new(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        8,
        None,
        0,
        vec![list_value_offsets],
        vec![expected_int_array.into_data()],
    )
    .unwrap();
    assert_eq!(finished, expected_list_data);
}

#[test]
fn test_list_nulls_append() {
    let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(32));
    builder.values().append_slice(&[1, 2, 3]);
    builder.append(true);
    builder.values().append_slice(&[4, 5]);
    builder.append(true);
    builder.append(false);
    builder.values().append_slice(&[6, 7, 8]);
    builder.values().append_null();
    builder.values().append_null();
    builder.values().append_slice(&[9, 10, 11]);
    builder.append(true);
    let a = builder.finish().into_data();

    let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(32));
    builder.values().append_slice(&[12, 13]);
    builder.append(true);
    builder.append(false);
    builder.append(true);
    builder.values().append_null();
    builder.values().append_null();
    builder.values().append_slice(&[14, 15]);
    builder.append(true);
    let b = builder.finish().into_data();
    let c = b.slice(1, 2);
    let d = b.slice(2, 2);

    let mut mutable = MutableArrayData::new(vec![&a, &b, &c, &d], false, 10);

    mutable.extend(0, 0, a.len());
    mutable.extend(1, 0, b.len());
    mutable.extend(2, 0, c.len());
    mutable.extend(3, 0, d.len());
    let result = mutable.freeze();

    let expected_int_array = Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        None,
        None,
        Some(9),
        Some(10),
        Some(11),
        // second array
        Some(12),
        Some(13),
        None,
        None,
        Some(14),
        Some(15),
        // slice(1, 2) results in no values added
        None,
        None,
        Some(14),
        Some(15),
    ]);
    let list_value_offsets =
        Buffer::from_slice_ref([0, 3, 5, 5, 13, 15, 15, 15, 19, 19, 19, 19, 23]);
    let expected_list_data = ArrayData::try_new(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        12,
        Some(Buffer::from(&[0b11011011, 0b1110])),
        0,
        vec![list_value_offsets],
        vec![expected_int_array.into_data()],
    )
    .unwrap();
    assert_eq!(result, expected_list_data);
}

#[test]
fn test_map_nulls_append() {
    let mut builder = MapBuilder::<Int64Builder, Int64Builder>::new(
        None,
        Int64Builder::with_capacity(32),
        Int64Builder::with_capacity(32),
    );
    builder.keys().append_slice(&[1, 2, 3]);
    builder.values().append_slice(&[1, 2, 3]);
    builder.append(true).unwrap();
    builder.keys().append_slice(&[4, 5]);
    builder.values().append_slice(&[4, 5]);
    builder.append(true).unwrap();
    builder.append(false).unwrap();
    builder.keys().append_slice(&[6, 7, 8, 100, 101, 9, 10, 11]);
    builder.values().append_slice(&[6, 7, 8]);
    builder.values().append_null();
    builder.values().append_null();
    builder.values().append_slice(&[9, 10, 11]);
    builder.append(true).unwrap();

    let a = builder.finish().into_data();

    let mut builder = MapBuilder::<Int64Builder, Int64Builder>::new(
        None,
        Int64Builder::with_capacity(32),
        Int64Builder::with_capacity(32),
    );

    builder.keys().append_slice(&[12, 13]);
    builder.values().append_slice(&[12, 13]);
    builder.append(true).unwrap();
    builder.append(false).unwrap();
    builder.append(true).unwrap();
    builder.keys().append_slice(&[100, 101, 14, 15]);
    builder.values().append_null();
    builder.values().append_null();
    builder.values().append_slice(&[14, 15]);
    builder.append(true).unwrap();

    let b = builder.finish().into_data();
    let c = b.slice(1, 2);
    let d = b.slice(2, 2);

    let mut mutable = MutableArrayData::new(vec![&a, &b, &c, &d], false, 10);

    mutable.extend(0, 0, a.len());
    mutable.extend(1, 0, b.len());
    mutable.extend(2, 0, c.len());
    mutable.extend(3, 0, d.len());
    let result = mutable.freeze();

    let expected_key_array = Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(100),
        Some(101),
        Some(9),
        Some(10),
        Some(11),
        // second array
        Some(12),
        Some(13),
        Some(100),
        Some(101),
        Some(14),
        Some(15),
        // slice(1, 2) results in no values added
        Some(100),
        Some(101),
        Some(14),
        Some(15),
    ]);

    let expected_value_array = Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        None,
        None,
        Some(9),
        Some(10),
        Some(11),
        // second array
        Some(12),
        Some(13),
        None,
        None,
        Some(14),
        Some(15),
        // slice(1, 2) results in no values added
        None,
        None,
        Some(14),
        Some(15),
    ]);

    let expected_entry_array = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", DataType::Int64, false)),
            Arc::new(expected_key_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new("values", DataType::Int64, true)),
            Arc::new(expected_value_array) as ArrayRef,
        ),
    ]);

    let map_offsets = Buffer::from_slice_ref([0, 3, 5, 5, 13, 15, 15, 15, 19, 19, 19, 19, 23]);

    let expected_list_data = ArrayData::try_new(
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Int64, false),
                    Field::new("values", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        ),
        12,
        Some(Buffer::from(&[0b11011011, 0b1110])),
        0,
        vec![map_offsets],
        vec![expected_entry_array.into_data()],
    )
    .unwrap();
    assert_eq!(result, expected_list_data);
}

#[test]
fn test_list_of_strings_append() {
    // [["alpha", "beta", None]]
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_value("Hello");
    builder.values().append_value("Arrow");
    builder.values().append_null();
    builder.append(true);
    let a = builder.finish().into_data();

    // [["alpha", "beta"], [None], ["gamma", "delta", None]]
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_value("alpha");
    builder.values().append_value("beta");
    builder.append(true);
    builder.values().append_null();
    builder.append(true);
    builder.values().append_value("gamma");
    builder.values().append_value("delta");
    builder.values().append_null();
    builder.append(true);
    let b = builder.finish().into_data();

    let mut mutable = MutableArrayData::new(vec![&a, &b], false, 10);

    mutable.extend(0, 0, a.len());
    mutable.extend(1, 0, b.len());
    mutable.extend(1, 1, 3);
    mutable.extend(1, 0, 0);
    let result = mutable.freeze();

    let expected_string_array = StringArray::from(vec![
        // extend a[0..a.len()]
        // a[0]
        Some("Hello"),
        Some("Arrow"),
        None,
        // extend b[0..b.len()]
        // b[0]
        Some("alpha"),
        Some("beta"),
        // b[1]
        None,
        // b[2]
        Some("gamma"),
        Some("delta"),
        None,
        // extend b[1..3]
        // b[1]
        None,
        // b[2]
        Some("gamma"),
        Some("delta"),
        None,
        // extend b[0..0]
    ]);
    let list_value_offsets = Buffer::from_slice_ref([0, 3, 5, 6, 9, 10, 13]);
    let expected_list_data = ArrayData::try_new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        6,
        None,
        0,
        vec![list_value_offsets],
        vec![expected_string_array.into_data()],
    )
    .unwrap();
    assert_eq!(result, expected_list_data);
}

#[test]
fn test_fixed_size_binary_append() {
    let a = vec![Some(vec![1, 2]), Some(vec![3, 4]), Some(vec![5, 6])];
    let a = FixedSizeBinaryArray::try_from_sparse_iter_with_size(a.into_iter(), 2)
        .expect("Failed to create FixedSizeBinaryArray from iterable")
        .into_data();

    let b = vec![
        None,
        Some(vec![7, 8]),
        Some(vec![9, 10]),
        None,
        Some(vec![13, 14]),
        None,
    ];
    let b = FixedSizeBinaryArray::try_from_sparse_iter_with_size(b.into_iter(), 2)
        .expect("Failed to create FixedSizeBinaryArray from iterable")
        .into_data();

    let mut mutable = MutableArrayData::new(vec![&a, &b], false, 10);

    mutable.extend(0, 0, a.len());
    mutable.extend(1, 0, b.len());
    mutable.extend(1, 1, 4);
    mutable.extend(1, 2, 3);
    mutable.extend(1, 5, 5);
    let result = mutable.freeze();

    let expected = vec![
        // a
        Some(vec![1, 2]),
        Some(vec![3, 4]),
        Some(vec![5, 6]),
        // b
        None,
        Some(vec![7, 8]),
        Some(vec![9, 10]),
        None,
        Some(vec![13, 14]),
        None,
        // b[1..4]
        Some(vec![7, 8]),
        Some(vec![9, 10]),
        None,
        // b[2..3]
        Some(vec![9, 10]),
        // b[4..4]
    ];
    let expected = FixedSizeBinaryArray::try_from_sparse_iter_with_size(expected.into_iter(), 2)
        .expect("Failed to create FixedSizeBinaryArray from iterable")
        .into_data();
    assert_eq!(result, expected);
}

#[test]
fn test_extend_nulls() {
    let int = Int32Array::from(vec![1, 2, 3, 4]).into_data();
    let mut mutable = MutableArrayData::new(vec![&int], true, 4);
    mutable.extend(0, 2, 3);
    mutable.extend_nulls(2);

    let data = mutable.freeze();
    data.validate_full().unwrap();
    let out = Int32Array::from(data);

    assert_eq!(out.null_count(), 2);
    assert_eq!(out.iter().collect::<Vec<_>>(), vec![Some(3), None, None]);
}

#[test]
#[should_panic(expected = "MutableArrayData not nullable")]
fn test_extend_nulls_panic() {
    let int = Int32Array::from(vec![1, 2, 3, 4]).into_data();
    let mut mutable = MutableArrayData::new(vec![&int], false, 4);
    mutable.extend_nulls(2);
}

#[test]
#[should_panic(expected = "Arrays with inconsistent types passed to MutableArrayData")]
fn test_mixed_types() {
    let a = StringArray::from(vec!["abc", "def"]).to_data();
    let b = Int32Array::from(vec![1, 2, 3]).to_data();
    MutableArrayData::new(vec![&a, &b], false, 4);
}

/*
// this is an old test used on a meanwhile removed dead code
// that is still useful when `MutableArrayData` supports fixed-size lists.
#[test]
fn test_fixed_size_list_append() -> Result<()> {
    let int_builder = UInt16Builder::new(64);
    let mut builder = FixedSizeListBuilder::<UInt16Builder>::new(int_builder, 2);
    builder.values().append_slice(&[1, 2])?;
    builder.append(true)?;
    builder.values().append_slice(&[3, 4])?;
    builder.append(false)?;
    builder.values().append_slice(&[5, 6])?;
    builder.append(true)?;

    let a_builder = UInt16Builder::new(64);
    let mut a_builder = FixedSizeListBuilder::<UInt16Builder>::new(a_builder, 2);
    a_builder.values().append_slice(&[7, 8])?;
    a_builder.append(true)?;
    a_builder.values().append_slice(&[9, 10])?;
    a_builder.append(true)?;
    a_builder.values().append_slice(&[11, 12])?;
    a_builder.append(false)?;
    a_builder.values().append_slice(&[13, 14])?;
    a_builder.append(true)?;
    a_builder.values().append_null()?;
    a_builder.values().append_null()?;
    a_builder.append(true)?;
    let a = a_builder.finish();

    // append array
    builder.append_data(&[
        a.data(),
        a.slice(1, 3).data(),
        a.slice(2, 1).data(),
        a.slice(5, 0).data(),
    ])?;
    let finished = builder.finish();

    let expected_int_array = UInt16Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        // append first array
        Some(7),
        Some(8),
        Some(9),
        Some(10),
        Some(11),
        Some(12),
        Some(13),
        Some(14),
        None,
        None,
        // append slice(1, 3)
        Some(9),
        Some(10),
        Some(11),
        Some(12),
        Some(13),
        Some(14),
        // append slice(2, 1)
        Some(11),
        Some(12),
    ]);
    let expected_list_data = ArrayData::new(
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::UInt16, true)),
            2,
        ),
        12,
        None,
        None,
        0,
        vec![],
        vec![expected_int_array.data()],
    );
    let expected_list =
        FixedSizeListArray::from(Arc::new(expected_list_data) as ArrayData);
    assert_eq!(&expected_list.values(), &finished.values());
    assert_eq!(expected_list.len(), finished.len());

    Ok(())
}
*/
