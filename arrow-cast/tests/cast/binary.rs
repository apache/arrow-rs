// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray,
    GenericBinaryArray, GenericStringArray, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray, StringViewArray,
};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::DataType;
use rstest::rstest;

#[rstest]
#[case::binary(0_i32)]
#[case::large_binary(0_i64)]
fn test_cast_binary_to_fixed_size_binary<O: OffsetSizeTrait>(#[case] offset_type: O) {
    let _ = offset_type;
    let bytes_1 = b"Hiiii".as_slice();
    let bytes_2 = b"Hello".as_slice();

    let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
    let array = GenericBinaryArray::<O>::from(binary_data);
    let array_ref = cast(&array, &DataType::FixedSizeBinary(5)).unwrap();
    let down_cast = array_ref
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(bytes_1, down_cast.value(0));
    assert_eq!(bytes_2, down_cast.value(1));
    assert!(down_cast.is_null(2));
}

#[rstest]
#[case::binary(0_i32)]
#[case::large_binary(0_i64)]
fn test_cast_binary_to_fixed_size_binary_length_mismatch<O: OffsetSizeTrait>(
    #[case] offset_type: O,
) {
    let _ = offset_type;
    let bytes_1 = b"Hi".as_slice();
    let bytes_2 = b"Hello".as_slice();

    let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
    let array = GenericBinaryArray::<O>::from(binary_data);
    let result = cast_with_options(
        &array,
        &DataType::FixedSizeBinary(5),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(result.is_err());
}

#[rstest]
#[case::binary(DataType::Binary)]
#[case::large_binary(DataType::LargeBinary)]
#[case::binary_view(DataType::BinaryView)]
fn test_fixed_size_binary_to_binary(#[case] to_type: DataType) {
    let bytes_1 = b"Hiiii".as_slice();
    let bytes_2 = b"Hello".as_slice();

    let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
    let array = FixedSizeBinaryArray::try_from(binary_data.clone()).unwrap();
    let array_ref = cast(&array, &to_type).unwrap();
    let actual: Vec<_> = match to_type {
        DataType::Binary => array_ref.as_binary::<i32>().iter().collect(),
        DataType::LargeBinary => array_ref.as_binary::<i64>().iter().collect(),
        DataType::BinaryView => array_ref.as_binary_view().iter().collect(),
        _ => unreachable!(),
    };
    assert_eq!(actual, binary_data);
}

#[rstest]
#[case::int16(
    Arc::new(Int16Array::from(vec![Some(1), Some(511), None])) as ArrayRef,
    [1_i16.to_le_bytes().to_vec(), 511_i16.to_le_bytes().to_vec()]
)]
#[case::int64(
    Arc::new(Int64Array::from(vec![Some(-1), Some(123456789), None])) as ArrayRef,
    [(-1_i64).to_le_bytes().to_vec(), 123456789_i64.to_le_bytes().to_vec()]
)]
fn test_numeric_to_binary<O: OffsetSizeTrait>(
    #[case] array: ArrayRef,
    #[case] expected: [Vec<u8>; 2],
    #[values(0_i32, 0_i64)] offset_type: O,
) {
    let _ = offset_type;
    let array_ref = cast(&array, &GenericBinaryArray::<O>::DATA_TYPE).unwrap();
    let down_cast = array_ref.as_binary::<O>();
    assert_eq!(expected[0].as_slice(), down_cast.value(0));
    assert_eq!(expected[1].as_slice(), down_cast.value(1));
    assert!(down_cast.is_null(2));
}

const VIEW_TEST_DATA: [Option<&str>; 5] = [
    Some("hello"),
    Some("repeated"),
    None,
    Some("large payload over 12 bytes"),
    Some("repeated"),
];

#[test]
fn test_string_view_to_binary_view() {
    let string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);

    assert!(can_cast_types(
        string_view_array.data_type(),
        &DataType::BinaryView
    ));

    let binary_view_array = cast(&string_view_array, &DataType::BinaryView).unwrap();
    assert_eq!(binary_view_array.data_type(), &DataType::BinaryView);

    let expect_binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
    assert_eq!(binary_view_array.as_ref(), &expect_binary_view_array);
}

#[test]
fn test_binary_view_to_string_view() {
    let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);

    assert!(can_cast_types(
        binary_view_array.data_type(),
        &DataType::Utf8View
    ));

    let string_view_array = cast(&binary_view_array, &DataType::Utf8View).unwrap();
    assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

    let expect_string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
    assert_eq!(string_view_array.as_ref(), &expect_string_view_array);
}

#[test]
fn test_binary_view_to_string_view_with_invalid_utf8() {
    let binary_view_array = BinaryViewArray::from_iter(vec![
        Some(b"valid".as_slice()),
        Some(&[0xff]),
        Some(b"utf8".as_slice()),
        None,
    ]);

    let strict_options = CastOptions {
        safe: false,
        ..Default::default()
    };

    assert!(cast_with_options(&binary_view_array, &DataType::Utf8View, &strict_options).is_err());

    let safe_options = CastOptions {
        safe: true,
        ..Default::default()
    };

    let string_view_array =
        cast_with_options(&binary_view_array, &DataType::Utf8View, &safe_options).unwrap();
    assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

    let values: Vec<_> = string_view_array.as_string_view().iter().collect();

    assert_eq!(values, vec![Some("valid"), None, Some("utf8"), None]);
}

#[rstest]
#[case::utf8(Arc::new(StringArray::from_iter(VIEW_TEST_DATA)))]
#[case::large_utf8(Arc::new(LargeStringArray::from_iter(VIEW_TEST_DATA)))]
#[case::binary(Arc::new(BinaryArray::from_iter(VIEW_TEST_DATA)))]
#[case::large_binary(Arc::new(LargeBinaryArray::from_iter(VIEW_TEST_DATA)))]
fn test_string_or_binary_to_view(
    #[case] array: ArrayRef,
    #[values(DataType::Utf8View, DataType::BinaryView)] to_type: DataType,
) {
    assert!(can_cast_types(array.data_type(), &to_type));
    let actual = cast(&array, &to_type).unwrap();
    assert_eq!(actual.data_type(), &to_type);

    let expected: ArrayRef = match to_type {
        DataType::Utf8View => Arc::new(StringViewArray::from_iter(VIEW_TEST_DATA)),
        DataType::BinaryView => Arc::new(BinaryViewArray::from_iter(VIEW_TEST_DATA)),
        _ => unreachable!(),
    };
    assert_eq!(actual.as_ref(), expected.as_ref());
}

#[test]
fn test_dict_to_view() {
    let values = StringArray::from_iter(VIEW_TEST_DATA);
    let keys = Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
    let string_dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
    let typed_dict = string_dict_array.downcast_dict::<StringArray>().unwrap();

    let string_view_array = {
        let mut builder = StringViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
        for v in typed_dict {
            builder.append_option(v);
        }
        builder.finish()
    };
    let expected_string_array_type = string_view_array.data_type();
    let casted_string_array = cast(&string_dict_array, expected_string_array_type).unwrap();
    assert_eq!(casted_string_array.data_type(), expected_string_array_type);
    assert_eq!(casted_string_array.as_ref(), &string_view_array);

    let binary_buffer = cast(&typed_dict.values(), &DataType::Binary).unwrap();
    let binary_dict_array =
        DictionaryArray::<Int8Type>::new(typed_dict.keys().clone(), binary_buffer);
    let typed_binary_dict = binary_dict_array.downcast_dict::<BinaryArray>().unwrap();

    let binary_view_array = {
        let mut builder = BinaryViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
        for v in typed_binary_dict {
            builder.append_option(v);
        }
        builder.finish()
    };
    let expected_binary_array_type = binary_view_array.data_type();
    let casted_binary_array = cast(&binary_dict_array, expected_binary_array_type).unwrap();
    assert_eq!(casted_binary_array.data_type(), expected_binary_array_type);
    assert_eq!(casted_binary_array.as_ref(), &binary_view_array);
}

#[test]
fn test_dict_to_view_null_dictionary_value_is_null() {
    // Ensure we preserve nulls in the values
    let keys = Int32Array::from_iter([Some(0), Some(1), Some(2), None, Some(1)]);

    let values = StringArray::from(vec![Some("aa"), None, Some("a value over twelve bytes")]);
    let dict = DictionaryArray::<Int32Type>::try_new(keys.clone(), Arc::new(values)).unwrap();
    let casted = cast(&dict, &DataType::Utf8View).unwrap();
    assert_eq!(
        casted.as_string_view().iter().collect::<Vec<_>>(),
        vec![
            Some("aa"),
            None,
            Some("a value over twelve bytes"),
            None,
            None
        ]
    );
    // the same input cast to Utf8 goes through `unpack_dictionary` and always agreed
    let reference = cast(&dict, &DataType::Utf8).unwrap();
    assert_eq!(
        casted.as_string_view().iter().collect::<Vec<_>>(),
        reference.as_string::<i32>().iter().collect::<Vec<_>>()
    );

    let values = BinaryArray::from_opt_vec(vec![
        Some(b"aa".as_slice()),
        None,
        Some(b"a value over twelve bytes"),
    ]);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
    let casted = cast(&dict, &DataType::BinaryView).unwrap();
    assert_eq!(
        casted.as_binary_view().iter().collect::<Vec<_>>(),
        vec![
            Some(b"aa".as_slice()),
            None,
            Some(b"a value over twelve bytes"),
            None,
            None
        ]
    );
}

#[test]
fn test_view_to_dict() {
    let string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
    let string_dict_array: DictionaryArray<Int8Type> = VIEW_TEST_DATA.into_iter().collect();
    let casted_type = string_dict_array.data_type();
    let casted_dict_array = cast(&string_view_array, casted_type).unwrap();
    assert_eq!(casted_dict_array.data_type(), casted_type);
    assert_eq!(casted_dict_array.as_ref(), &string_dict_array);

    let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
    let binary_dict_array = string_dict_array.downcast_dict::<StringArray>().unwrap();
    let binary_buffer = cast(&binary_dict_array.values(), &DataType::Binary).unwrap();
    let binary_dict_array =
        DictionaryArray::<Int8Type>::new(binary_dict_array.keys().clone(), binary_buffer);
    let casted_type = binary_dict_array.data_type();
    let casted_binary_array = cast(&binary_view_array, casted_type).unwrap();
    assert_eq!(casted_binary_array.data_type(), casted_type);
    assert_eq!(casted_binary_array.as_ref(), &binary_dict_array);
}

#[test]
fn test_view_to_string() {
    _test_view_to_string::<i32>();
    _test_view_to_string::<i64>();
}

fn _test_view_to_string<O>()
where
    O: OffsetSizeTrait,
{
    let string_view_array = {
        let mut builder = StringViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
        for s in &VIEW_TEST_DATA {
            builder.append_option(*s);
        }
        builder.finish()
    };

    let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);

    let expected_string_array = GenericStringArray::<O>::from_iter(VIEW_TEST_DATA);
    let expected_type = expected_string_array.data_type();

    assert!(can_cast_types(string_view_array.data_type(), expected_type));
    assert!(can_cast_types(binary_view_array.data_type(), expected_type));

    let string_view_casted_array = cast(&string_view_array, expected_type).unwrap();
    assert_eq!(string_view_casted_array.data_type(), expected_type);
    assert_eq!(string_view_casted_array.as_ref(), &expected_string_array);

    let binary_view_casted_array = cast(&binary_view_array, expected_type).unwrap();
    assert_eq!(binary_view_casted_array.data_type(), expected_type);
    assert_eq!(binary_view_casted_array.as_ref(), &expected_string_array);
}

#[test]
fn test_view_to_binary() {
    _test_view_to_binary::<i32>();
    _test_view_to_binary::<i64>();
}

fn _test_view_to_binary<O>()
where
    O: OffsetSizeTrait,
{
    let view_array = {
        let mut builder = BinaryViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
        for s in &VIEW_TEST_DATA {
            builder.append_option(*s);
        }
        builder.finish()
    };

    let expected_binary_array = GenericBinaryArray::<O>::from_iter(VIEW_TEST_DATA);
    let expected_type = expected_binary_array.data_type();

    assert!(can_cast_types(view_array.data_type(), expected_type));

    let binary_array = cast(&view_array, expected_type).unwrap();
    assert_eq!(binary_array.data_type(), expected_type);

    assert_eq!(binary_array.as_ref(), &expected_binary_array);
}

#[test]
fn test_cast_invalid_utf8() {
    let v1: &[u8] = b"\xFF invalid";
    let v2: &[u8] = b"\x00 Foo";
    let s = BinaryArray::from(vec![v1, v2]);
    let options = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };
    let array = cast_with_options(&s, &DataType::Utf8, &options).unwrap();
    let a = array.as_string::<i32>();
    a.to_data().validate_full().unwrap();

    assert_eq!(a.null_count(), 1);
    assert_eq!(a.len(), 2);
    assert!(a.is_null(0));
    assert_eq!(a.value(0), "");
    assert_eq!(a.value(1), "\x00 Foo");
}

#[rstest]
#[case::binary(0_i32)]
#[case::large_binary(0_i64)]
fn test_cast_sliced_binary_with_hidden_invalid_utf8<O: OffsetSizeTrait>(
    #[case] offset_type: O,
    #[values(DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View)] to_type: DataType,
) {
    let _ = offset_type;
    // The invalid first value is outside the slice, so it must not fail the cast
    let values: Vec<&[u8]> = vec![b"\xFF", b"a", b"b", b"c", b"d", b"e", b"f"];
    let array = GenericBinaryArray::<O>::from(values).slice(1, 6);
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let expected = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
    let result = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(result.as_ref(), cast(&expected, &to_type).unwrap().as_ref());
}

#[rstest]
#[case::binary(0_i32)]
#[case::large_binary(0_i64)]
fn test_cast_sliced_binary_dict_with_hidden_invalid_utf8<O: OffsetSizeTrait>(
    #[case] offset_type: O,
) {
    let _ = offset_type;
    let values: Vec<&[u8]> = vec![b"\xFF", b"a", b"b", b"c", b"d", b"e", b"f"];
    let array = GenericBinaryArray::<O>::from(values).slice(1, 6);
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    // A dictionary with fewer keys than half its values takes a separate
    // path to Utf8View
    let keys = Int32Array::from(vec![5, 0]);
    let dict = DictionaryArray::try_new(keys, Arc::new(array)).unwrap();
    let result = cast_with_options(&dict, &DataType::Utf8View, &options).unwrap();
    assert_eq!(result.as_ref(), &StringViewArray::from(vec!["f", "a"]));
}

#[test]
fn test_string_dicts_to_binary_view() {
    let expected = BinaryViewArray::from_iter(vec![
        VIEW_TEST_DATA[1],
        VIEW_TEST_DATA[0],
        None,
        VIEW_TEST_DATA[3],
        None,
        VIEW_TEST_DATA[1],
        VIEW_TEST_DATA[4],
    ]);

    let values_arrays: [ArrayRef; _] = [
        Arc::new(StringArray::from_iter(VIEW_TEST_DATA)),
        Arc::new(StringViewArray::from_iter(VIEW_TEST_DATA)),
        Arc::new(LargeStringArray::from_iter(VIEW_TEST_DATA)),
    ];
    for values in values_arrays {
        let keys = Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
        let string_dict_array = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();

        let casted = cast(&string_dict_array, &DataType::BinaryView).unwrap();
        assert_eq!(casted.as_ref(), &expected);
    }
}

#[test]
fn test_binary_dicts_to_string_view() {
    let expected = StringViewArray::from_iter(vec![
        VIEW_TEST_DATA[1],
        VIEW_TEST_DATA[0],
        None,
        VIEW_TEST_DATA[3],
        None,
        VIEW_TEST_DATA[1],
        VIEW_TEST_DATA[4],
    ]);

    let values_arrays: [ArrayRef; _] = [
        Arc::new(BinaryArray::from_iter(VIEW_TEST_DATA)),
        Arc::new(BinaryViewArray::from_iter(VIEW_TEST_DATA)),
        Arc::new(LargeBinaryArray::from_iter(VIEW_TEST_DATA)),
    ];
    for values in values_arrays {
        let keys = Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
        let string_dict_array = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();

        let casted = cast(&string_dict_array, &DataType::Utf8View).unwrap();
        assert_eq!(casted.as_ref(), &expected);
    }
}
