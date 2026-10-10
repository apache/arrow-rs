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

use std::sync::Arc;

use DataType::*;
use arrow_array::builder::{
    GenericBinaryBuilder, PrimitiveBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type, Int64Type, UInt16Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Date32Array, DictionaryArray,
    FixedSizeBinaryArray, Int32Array, LargeStringArray, StringArray, StringViewArray, StructArray,
    TimestampSecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};
#[test]
fn test_fixed_size_binary_to_dictionary() {
    let bytes_1 = b"Hiiii".as_slice();
    let bytes_2 = b"Hello".as_slice();

    let binary_data = vec![Some(bytes_1), Some(bytes_2), Some(bytes_1), None];
    let a1 = Arc::new(FixedSizeBinaryArray::try_from(binary_data.clone()).unwrap()) as ArrayRef;

    let cast_type = DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::FixedSizeBinary(5)),
    );
    let cast_array = cast(&a1, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(
        array_to_strings(&cast_array),
        vec!["4869696969", "48656c6c6f", "4869696969", "null"]
    );
    // dictionary should only have two distinct values
    let dict_array = cast_array.as_dictionary::<Int8Type>();
    assert_eq!(dict_array.values().len(), 2);
}

#[test]
fn test_binary_to_dictionary() {
    let mut builder = GenericBinaryBuilder::<i32>::new();
    builder.append_value(b"hello");
    builder.append_value(b"hiiii");
    builder.append_value(b"hiiii"); // duplicate
    builder.append_null();
    builder.append_value(b"rustt");

    let a1 = builder.finish();

    let cast_type = DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::FixedSizeBinary(5)),
    );
    let cast_array = cast(&a1, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(
        array_to_strings(&cast_array),
        vec![
            "68656c6c6f",
            "6869696969",
            "6869696969",
            "null",
            "7275737474"
        ]
    );
    // dictionary should only have three distinct values
    let dict_array = cast_array.as_dictionary::<Int8Type>();
    assert_eq!(dict_array.values().len(), 3);
}

#[test]
fn test_cast_string_array_to_dict_utf8_view() {
    let array = StringArray::from(vec![Some("one"), None, Some("three"), Some("one")]);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 2); // "one" and "three" deduplicated

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![Some("one"), None, Some("three"), Some("one")]);

    let keys = dict_array.keys();
    assert!(keys.is_null(1));
    assert_eq!(keys.value(0), keys.value(3));
    assert_ne!(keys.value(0), keys.value(2));
}

#[test]
fn test_cast_string_array_to_dict_utf8_view_null_vs_literal_null() {
    let array = StringArray::from(vec![Some("one"), None, Some("null"), Some("one")]);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 2);

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![Some("one"), None, Some("null"), Some("one")]);

    let keys = dict_array.keys();
    assert!(keys.is_null(1));
    assert_eq!(keys.value(0), keys.value(3));
    assert_ne!(keys.value(0), keys.value(2));
}

#[test]
fn test_cast_string_view_array_to_dict_utf8_view() {
    let array = StringViewArray::from(vec![Some("one"), None, Some("three"), Some("one")]);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 2); // "one" and "three" deduplicated

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![Some("one"), None, Some("three"), Some("one")]);

    let keys = dict_array.keys();
    assert!(keys.is_null(1));
    assert_eq!(keys.value(0), keys.value(3));
    assert_ne!(keys.value(0), keys.value(2));
}

#[test]
fn test_cast_string_view_slice_to_dict_utf8_view() {
    let array = StringViewArray::from(vec![
        Some("zero"),
        Some("one"),
        None,
        Some("three"),
        Some("one"),
    ]);
    let view = array.slice(1, 4);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(view.data_type(), &cast_type));
    let cast_array = cast(&view, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 2);

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![Some("one"), None, Some("three"), Some("one")]);

    let keys = dict_array.keys();
    assert!(keys.is_null(1));
    assert_eq!(keys.value(0), keys.value(3));
    assert_ne!(keys.value(0), keys.value(2));
}

#[test]
fn test_cast_binary_array_to_dict_binary_view() {
    let mut builder = GenericBinaryBuilder::<i32>::new();
    builder.append_value(b"hello");
    builder.append_value(b"hiiii");
    builder.append_value(b"hiiii"); // duplicate
    builder.append_null();
    builder.append_value(b"rustt");

    let array = builder.finish();

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::BinaryView);
    assert_eq!(dict_array.values().len(), 3);

    let typed = dict_array.downcast_dict::<BinaryViewArray>().unwrap();
    let actual: Vec<Option<&[u8]>> = typed.into_iter().collect();
    assert_eq!(
        actual,
        vec![
            Some(b"hello".as_slice()),
            Some(b"hiiii".as_slice()),
            Some(b"hiiii".as_slice()),
            None,
            Some(b"rustt".as_slice())
        ]
    );

    let keys = dict_array.keys();
    assert!(keys.is_null(3));
    assert_eq!(keys.value(1), keys.value(2));
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_cast_binary_view_array_to_dict_binary_view() {
    let view = BinaryViewArray::from_iter([
        Some(b"hello".as_slice()),
        Some(b"hiiii".as_slice()),
        Some(b"hiiii".as_slice()), // duplicate
        None,
        Some(b"rustt".as_slice()),
    ]);

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(view.data_type(), &cast_type));
    let cast_array = cast(&view, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::BinaryView);
    assert_eq!(dict_array.values().len(), 3);

    let typed = dict_array.downcast_dict::<BinaryViewArray>().unwrap();
    let actual: Vec<Option<&[u8]>> = typed.into_iter().collect();
    assert_eq!(
        actual,
        vec![
            Some(b"hello".as_slice()),
            Some(b"hiiii".as_slice()),
            Some(b"hiiii".as_slice()),
            None,
            Some(b"rustt".as_slice())
        ]
    );

    let keys = dict_array.keys();
    assert!(keys.is_null(3));
    assert_eq!(keys.value(1), keys.value(2));
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_cast_binary_view_slice_to_dict_binary_view() {
    let view = BinaryViewArray::from_iter([
        Some(b"hello".as_slice()),
        Some(b"hiiii".as_slice()),
        Some(b"hiiii".as_slice()), // duplicate
        None,
        Some(b"rustt".as_slice()),
    ]);
    let sliced = view.slice(1, 4);

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(sliced.data_type(), &cast_type));
    let cast_array = cast(&sliced, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::BinaryView);
    assert_eq!(dict_array.values().len(), 2);

    let typed = dict_array.downcast_dict::<BinaryViewArray>().unwrap();
    let actual: Vec<Option<&[u8]>> = typed.into_iter().collect();
    assert_eq!(
        actual,
        vec![
            Some(b"hiiii".as_slice()),
            Some(b"hiiii".as_slice()),
            None,
            Some(b"rustt".as_slice())
        ]
    );

    let keys = dict_array.keys();
    assert!(keys.is_null(2));
    assert_eq!(keys.value(0), keys.value(1));
    assert_ne!(keys.value(0), keys.value(3));
}

#[test]
fn test_cast_string_array_to_dict_utf8_view_key_overflow_u8() {
    let array = StringArray::from_iter_values((0..257).map(|i| format!("v{i}")));

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let err = cast(&array, &cast_type).unwrap_err();
    assert!(matches!(err, ArrowError::DictionaryKeyOverflowError));
}

#[test]
fn test_cast_large_string_array_to_dict_utf8_view() {
    let array = LargeStringArray::from(vec![Some("one"), None, Some("three"), Some("one")]);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 2); // "one" and "three" deduplicated

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![Some("one"), None, Some("three"), Some("one")]);

    let keys = dict_array.keys();
    assert!(keys.is_null(1));
    assert_eq!(keys.value(0), keys.value(3));
    assert_ne!(keys.value(0), keys.value(2));
}

#[test]
fn test_cast_large_binary_array_to_dict_binary_view() {
    let mut builder = GenericBinaryBuilder::<i64>::new();
    builder.append_value(b"hello");
    builder.append_value(b"world");
    builder.append_value(b"hello"); // duplicate
    builder.append_null();

    let array = builder.finish();

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::BinaryView);
    assert_eq!(dict_array.values().len(), 2); // "hello" and "world" deduplicated

    let typed = dict_array.downcast_dict::<BinaryViewArray>().unwrap();
    let actual: Vec<Option<&[u8]>> = typed.into_iter().collect();
    assert_eq!(
        actual,
        vec![
            Some(b"hello".as_slice()),
            Some(b"world".as_slice()),
            Some(b"hello".as_slice()),
            None
        ]
    );

    let keys = dict_array.keys();
    assert!(keys.is_null(3));
    assert_eq!(keys.value(0), keys.value(2));
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_cast_struct_array_to_dict_struct() {
    // Cast a StructArray into Dictionary<UInt32, Struct{…}>. The dictionary
    // value type's child fields may differ from the source's (here:
    // Utf8 source → Utf8View child for `name`), so the per-field cast
    // must run before identity keys are emitted. This is the "as long as
    // the struct can be cast to the dict value" contract.
    let names = StringArray::from(vec![Some("alpha"), None, Some("gamma")]);
    let ids = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
    let source = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(names) as ArrayRef,
        ),
        (
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(ids) as ArrayRef,
        ),
    ]);

    let target_value_type = DataType::Struct(
        vec![
            Field::new("name", DataType::Utf8View, true),
            Field::new("id", DataType::Int64, false),
        ]
        .into(),
    );
    let cast_type = DataType::Dictionary(
        Box::new(DataType::UInt32),
        Box::new(target_value_type.clone()),
    );
    assert!(can_cast_types(source.data_type(), &cast_type));

    let cast_array = cast(&source, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(cast_array.len(), 3);

    let dict = cast_array.as_dictionary::<UInt32Type>();
    assert_eq!(dict.values().data_type(), &target_value_type);
    // No dedup is performed for struct values — one row, one key.
    assert_eq!(dict.values().len(), 3);

    // Source row 1 was a `Utf8`-null in the `name` field but the whole
    // struct row was valid (StructArray::from above takes per-field
    // nulls only). The dictionary's logical null mask therefore mirrors
    // the source struct's row-level null mask — all rows valid here.
    let keys = dict.keys();
    assert_eq!(keys.values(), &[0u32, 1, 2]);
    assert_eq!(keys.null_count(), 0);

    let struct_values = dict.values().as_struct();
    let names_out = struct_values
        .column_by_name("name")
        .unwrap()
        .as_string_view();
    assert_eq!(names_out.value(0), "alpha");
    assert!(names_out.is_null(1));
    assert_eq!(names_out.value(2), "gamma");
    let ids_out = struct_values
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(ids_out.values(), &[1i64, 2, 3]);
}

#[test]
fn test_cast_struct_array_to_dict_struct_row_nulls() {
    // Row-level nulls on the source struct must surface as null keys on
    // the dictionary, since the dictionary's logical null mask is
    // determined by the keys.
    let names = StringArray::from(vec![Some("alpha"), Some("beta"), Some("gamma")]);
    let ids = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
    let source = StructArray::try_new(
        vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]
        .into(),
        vec![Arc::new(names) as ArrayRef, Arc::new(ids) as ArrayRef],
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap();

    let target_value_type = DataType::Struct(
        vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]
        .into(),
    );
    let cast_type = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(target_value_type));

    let cast_array = cast(&source, &cast_type).unwrap();
    let dict = cast_array.as_dictionary::<UInt32Type>();
    assert_eq!(dict.len(), 3);
    let keys = dict.keys();
    assert!(!keys.is_null(0));
    assert!(keys.is_null(1));
    assert!(!keys.is_null(2));
}

#[test]
fn test_cast_struct_array_to_dict_struct_key_overflow() {
    // Source has 300 rows but the dictionary key type is UInt8 (max 255).
    // We must return a CastError instead of silently truncating.
    let n = 300;
    let names = StringArray::from((0..n).map(|i| Some(format!("v{i}"))).collect::<Vec<_>>());
    let source = StructArray::from(vec![(
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(names) as ArrayRef,
    )]);

    let cast_type = DataType::Dictionary(
        Box::new(DataType::UInt8),
        Box::new(DataType::Struct(
            vec![Field::new("name", DataType::Utf8, true)].into(),
        )),
    );
    let err = cast(&source, &cast_type).unwrap_err().to_string();
    assert!(
        err.contains("Cannot fit") && err.contains("dictionary keys"),
        "expected key-overflow error, got: {err}"
    );
}

#[test]
fn test_cast_empty_string_array_to_dict_utf8_view() {
    let array = StringArray::from(Vec::<Option<&str>>::new());

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(cast_array.len(), 0);
}

#[test]
fn test_cast_empty_binary_array_to_dict_binary_view() {
    let array = BinaryArray::from(Vec::<Option<&[u8]>>::new());

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(cast_array.len(), 0);
}

#[test]
fn test_cast_all_null_string_array_to_dict_utf8_view() {
    let array = StringArray::from(vec![None::<&str>, None, None]);

    let cast_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(cast_array.null_count(), 3);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::Utf8View);
    assert_eq!(dict_array.values().len(), 0);
    assert_eq!(dict_array.keys().null_count(), 3);

    let typed = dict_array.downcast_dict::<StringViewArray>().unwrap();
    let actual: Vec<Option<&str>> = typed.into_iter().collect();
    assert_eq!(actual, vec![None, None, None]);
}

#[test]
fn test_cast_all_null_binary_array_to_dict_binary_view() {
    let array = BinaryArray::from(vec![None::<&[u8]>, None, None]);

    let cast_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::BinaryView));
    assert!(can_cast_types(array.data_type(), &cast_type));
    let cast_array = cast(&array, &cast_type).unwrap();
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(cast_array.null_count(), 3);

    let dict_array = cast_array.as_dictionary::<UInt16Type>();
    assert_eq!(dict_array.values().data_type(), &DataType::BinaryView);
    assert_eq!(dict_array.values().len(), 0);
    assert_eq!(dict_array.keys().null_count(), 3);

    let typed = dict_array.downcast_dict::<BinaryViewArray>().unwrap();
    let actual: Vec<Option<&[u8]>> = typed.into_iter().collect();
    assert_eq!(actual, vec![None, None, None]);
}

#[test]
fn test_cast_utf8_dict() {
    // FROM a dictionary with of Utf8 values
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    builder.append("one").unwrap();
    builder.append_null();
    builder.append("three").unwrap();
    let array: ArrayRef = Arc::new(builder.finish());

    let expected = vec!["one", "null", "three"];

    // Test casting TO StringArray
    let cast_type = Utf8;
    let cast_array = cast(&array, &cast_type).expect("cast to UTF-8 failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    // Test casting TO Dictionary (with different index sizes)

    let cast_type = Dictionary(Box::new(Int16), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(Int32), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(Int64), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(UInt16), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(UInt32), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    let cast_type = Dictionary(Box::new(UInt64), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);
}

#[test]
fn test_cast_dict_to_dict_bad_index_value_primitive() {
    // test converting from an array that has indexes of a type
    // that are out of bounds for a particular other kind of
    // index.

    let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();

    // add 200 distinct values (which can be stored by a
    // dictionary indexed by int32, but not a dictionary indexed
    // with int8)
    for i in 0..200 {
        builder.append(i).unwrap();
    }
    let array: ArrayRef = Arc::new(builder.finish());

    let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let res = cast(&array, &cast_type);
    assert!(res.is_err());
    let actual_error = format!("{res:?}");
    let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
    assert!(
        actual_error.contains(expected_error),
        "did not find expected error '{actual_error}' in actual error '{expected_error}'"
    );
}

#[test]
fn test_cast_dict_to_dict_bad_index_value_utf8() {
    // Same test as test_cast_dict_to_dict_bad_index_value but use
    // string values (and encode the expected behavior here);

    let mut builder = StringDictionaryBuilder::<Int32Type>::new();

    // add 200 distinct values (which can be stored by a
    // dictionary indexed by int32, but not a dictionary indexed
    // with int8)
    for i in 0..200 {
        let val = format!("val{i}");
        builder.append(&val).unwrap();
    }
    let array = builder.finish();

    let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let res = cast(&array, &cast_type);
    assert!(res.is_err());
    let actual_error = format!("{res:?}");
    let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
    assert!(
        actual_error.contains(expected_error),
        "did not find expected error '{actual_error}' in actual error '{expected_error}'"
    );
}

#[test]
fn test_cast_nested_dictionary_to_dictionary_reuses_values() {
    let inner = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), None, Some(1)]),
        Arc::new(StringArray::from(vec!["x", "y"])),
    );
    let nested = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(0)]),
        Arc::new(inner),
    );

    let result = cast(&nested, &Dictionary(Box::new(Int32), Box::new(Utf8))).unwrap();
    let result = result.as_dictionary::<Int32Type>();

    assert_eq!(
        result.keys(),
        &Int32Array::from(vec![Some(0), None, Some(1), None, Some(0)])
    );
    assert_eq!(
        result.values().as_string::<i32>(),
        &StringArray::from(vec!["x", "y"])
    );
    let logical: Vec<Option<&str>> = result
        .downcast_dict::<StringArray>()
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(logical, vec![Some("x"), None, Some("y"), None, Some("x")]);
}

#[test]
fn test_cast_primitive_dict() {
    // FROM a dictionary with of INT32 values
    let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
    builder.append(1).unwrap();
    builder.append_null();
    builder.append(3).unwrap();
    let array: ArrayRef = Arc::new(builder.finish());

    let expected = vec!["1", "null", "3"];

    // Test casting TO PrimitiveArray, different dictionary type
    let cast_array = cast(&array, &Utf8).expect("cast to UTF-8 failed");
    assert_eq!(array_to_strings(&cast_array), expected);
    assert_eq!(cast_array.data_type(), &Utf8);

    let cast_array = cast(&array, &Int64).expect("cast to int64 failed");
    assert_eq!(array_to_strings(&cast_array), expected);
    assert_eq!(cast_array.data_type(), &Int64);
}

#[test]
fn test_cast_primitive_array_to_dict() {
    let mut builder = PrimitiveBuilder::<Int32Type>::new();
    builder.append_value(1);
    builder.append_null();
    builder.append_value(3);
    let array: ArrayRef = Arc::new(builder.finish());

    let expected = vec!["1", "null", "3"];

    // Cast to a dictionary (same value type, Int32)
    let cast_type = Dictionary(Box::new(UInt8), Box::new(Int32));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);

    // Cast to a dictionary (different value type, Int8)
    let cast_type = Dictionary(Box::new(UInt8), Box::new(Int8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);
}

#[test]
fn test_cast_time_array_to_dict() {
    use DataType::*;

    let array = Arc::new(Date32Array::from(vec![Some(1000), None, Some(2000)])) as ArrayRef;

    let expected = vec!["1972-09-27", "null", "1975-06-24"];

    let cast_type = Dictionary(Box::new(UInt8), Box::new(Date32));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);
}

#[test]
fn test_cast_timestamp_array_to_dict() {
    use DataType::*;

    let array = Arc::new(
        TimestampSecondArray::from(vec![Some(1000), None, Some(2000)]).with_timezone_utc(),
    ) as ArrayRef;

    let expected = vec!["1970-01-01T00:16:40", "null", "1970-01-01T00:33:20"];

    let cast_type = Dictionary(Box::new(UInt8), Box::new(Timestamp(TimeUnit::Second, None)));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);
}

#[test]
fn test_cast_string_array_to_dict() {
    use DataType::*;

    let array = Arc::new(StringArray::from(vec![Some("one"), None, Some("three")])) as ArrayRef;

    let expected = vec!["one", "null", "three"];

    // Cast to a dictionary (same value type, Utf8)
    let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
    let cast_array = cast(&array, &cast_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &cast_type);
    assert_eq!(array_to_strings(&cast_array), expected);
}

/// Print the `DictionaryArray` `array` as a vector of strings
fn array_to_strings(array: &ArrayRef) -> Vec<String> {
    let options = FormatOptions::new().with_null("null");
    let formatter = ArrayFormatter::try_new(array.as_ref(), &options).unwrap();
    (0..array.len())
        .map(|i| formatter.value(i).to_string())
        .collect()
}
