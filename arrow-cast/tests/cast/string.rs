// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::{
    Array, GenericStringArray, Int32Array, LargeStringArray, OffsetSizeTrait, StringArray,
};
use arrow_cast::cast;
use arrow_schema::DataType;
use rstest::rstest;

#[rstest]
#[case::utf8(0_i32)]
#[case::large_utf8(0_i64)]
fn test_cast_to_strings<O: OffsetSizeTrait>(#[case] offset_type: O) {
    let _ = offset_type;
    let a = Int32Array::from(vec![1, 2, 3]);
    let out = cast(&a, &GenericStringArray::<O>::DATA_TYPE).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
}

#[rstest]
#[case::large_utf8_to_utf8(0_i64, 0_i32)]
#[case::utf8_to_large_utf8(0_i32, 0_i64)]
fn test_str_to_str_casts<I: OffsetSizeTrait, O: OffsetSizeTrait>(
    #[case] input_offset_type: I,
    #[case] output_offset_type: O,
    #[values(
        vec![Some("foo"), Some("bar"), Some("ham")],
        vec![Some("foo"), None, Some("bar")]
    )]
    data: Vec<Option<&str>>,
) {
    let _ = (input_offset_type, output_offset_type);
    let array = GenericStringArray::<I>::from(data);
    let to = cast(&array, &GenericStringArray::<O>::DATA_TYPE).unwrap();
    let expected = array.iter().collect::<Vec<_>>();
    let actual = to
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(expected, actual);
}

#[test]
fn test_utf8_cast_offsets() {
    // test if offset of the array is taken into account during cast
    let str_array = StringArray::from(vec!["a", "b", "c"]);
    let str_array = str_array.slice(1, 2);

    let out = cast(&str_array, &DataType::LargeUtf8).unwrap();

    let large_str_array = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
    let strs = large_str_array.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(strs, &["b", "c"])
}
