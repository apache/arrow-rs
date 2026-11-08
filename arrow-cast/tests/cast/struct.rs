// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray, StructArray};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{DataType, Field};

#[test]
fn test_cast_struct_to_struct() {
    let struct_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int32, false),
        ]
        .into(),
    );
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]
        .into(),
    );
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]);
    let casted_array = cast(&struct_array, &to_type).unwrap();
    let casted_array = casted_array.as_struct();
    assert_eq!(casted_array.data_type(), &to_type);
    let casted_boolean_array = casted_array
        .column(0)
        .as_string::<i32>()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let casted_int_array = casted_array
        .column(1)
        .as_string::<i32>()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(casted_boolean_array, vec!["false", "false", "true", "true"]);
    assert_eq!(casted_int_array, vec!["42", "28", "19", "31"]);

    // test for can't cast
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Date32, false),
            Field::new("b", DataType::Utf8, false),
        ]
        .into(),
    );
    assert!(!can_cast_types(&struct_type, &to_type));
    let result = cast(&struct_array, &to_type);
    assert_eq!(
        "Invalid argument error: Casting from Boolean to Date32 not supported",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_struct_to_struct_nullability() {
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![Some(42), None, Some(19), None]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, true)),
            int.clone() as ArrayRef,
        ),
    ]);

    // okay: nullable to nullable
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, true),
        ]
        .into(),
    );
    cast(&struct_array, &to_type).expect("Cast nullable to nullable struct field should work");

    // error: nullable to non-nullable
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]
        .into(),
    );
    cast(&struct_array, &to_type)
        .expect_err("Cast nullable to non-nullable struct field should fail");

    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![i32::MAX, 25, 1, 100]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]);

    // okay: non-nullable to non-nullable
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]
        .into(),
    );
    cast(&struct_array, &to_type)
        .expect("Cast non-nullable to non-nullable struct field should work");

    // err: non-nullable to non-nullable but overflowing return null during casting
    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int8, false),
        ]
        .into(),
    );
    cast(&struct_array, &to_type)
        .expect_err("Cast non-nullable to non-nullable struct field returning null should fail");
}

#[test]
fn test_cast_struct_to_non_struct() {
    let boolean = Arc::new(BooleanArray::from(vec![true, false]));
    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Boolean, false)),
        boolean.clone() as ArrayRef,
    )]);
    let to_type = DataType::Utf8;
    let result = cast(&struct_array, &to_type);
    assert_eq!(
        r#"Invalid argument error: Casting from Struct("a": non-null Boolean) to Utf8 not supported"#,
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_unsupported_struct_to_fixed_size_binary_cast() {
    let field = Field::new("item", DataType::Int32, false);
    let struct_array = StructArray::from(vec![(
        Arc::new(field),
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
    )]);
    let array_ref = Arc::new(struct_array) as ArrayRef;

    // Casting a struct to fixed-size binary is unsupported.
    let cast_result = cast(&array_ref, &DataType::FixedSizeBinary(10));

    assert!(cast_result.is_err());
}

#[test]
fn test_cast_non_struct_to_struct() {
    let array = StringArray::from(vec!["a", "b"]);
    let to_type = DataType::Struct(vec![Field::new("a", DataType::Boolean, false)].into());
    let result = cast(&array, &to_type);
    assert_eq!(
        r#"Invalid argument error: Casting from Utf8 to Struct("a": non-null Boolean) not supported"#,
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_struct_with_different_field_order() {
    // Test slow path: fields are in different order
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
    let string = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "qux"]));

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Utf8, false)),
            string.clone() as ArrayRef,
        ),
    ]);

    // Target has fields in different order: c, a, b instead of a, b, c
    let to_type = DataType::Struct(
        vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false), // Boolean to Utf8
            Field::new("b", DataType::Utf8, false), // Int32 to Utf8
        ]
        .into(),
    );

    let result = cast(&struct_array, &to_type).unwrap();
    let result_struct = result.as_struct();

    assert_eq!(result_struct.data_type(), &to_type);
    assert_eq!(result_struct.num_columns(), 3);

    // Verify field "c" (originally position 2, now position 0) remains Utf8
    let c_column = result_struct.column(0).as_string::<i32>();
    assert_eq!(
        c_column.into_iter().flatten().collect::<Vec<_>>(),
        vec!["foo", "bar", "baz", "qux"]
    );

    // Verify field "a" (originally position 0, now position 1) was cast from Boolean to Utf8
    let a_column = result_struct.column(1).as_string::<i32>();
    assert_eq!(
        a_column.into_iter().flatten().collect::<Vec<_>>(),
        vec!["false", "false", "true", "true"]
    );

    // Verify field "b" (originally position 1, now position 2) was cast from Int32 to Utf8
    let b_column = result_struct.column(2).as_string::<i32>();
    assert_eq!(
        b_column.into_iter().flatten().collect::<Vec<_>>(),
        vec!["42", "28", "19", "31"]
    );
}

#[test]
fn test_cast_struct_with_missing_field() {
    // Test that casting fails when target has a field not present in source
    let boolean = Arc::new(BooleanArray::from(vec![false, true]));
    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Boolean, false)),
        boolean.clone() as ArrayRef,
    )]);

    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false), // Field "b" doesn't exist in source
        ]
        .into(),
    );

    let result = cast(&struct_array, &to_type);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid argument error: Incorrect number of arrays for StructArray fields, expected 2 got 1"
    );
}

#[test]
fn test_cast_struct_with_subset_of_fields() {
    // Test casting to a struct with fewer fields (selecting a subset)
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
    let string = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "qux"]));

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Utf8, false)),
            string.clone() as ArrayRef,
        ),
    ]);

    // Target has only fields "c" and "a", omitting "b"
    let to_type = DataType::Struct(
        vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false),
        ]
        .into(),
    );

    let result = cast(&struct_array, &to_type).unwrap();
    let result_struct = result.as_struct();

    assert_eq!(result_struct.data_type(), &to_type);
    assert_eq!(result_struct.num_columns(), 2);

    // Verify field "c" remains Utf8
    let c_column = result_struct.column(0).as_string::<i32>();
    assert_eq!(
        c_column.into_iter().flatten().collect::<Vec<_>>(),
        vec!["foo", "bar", "baz", "qux"]
    );

    // Verify field "a" was cast from Boolean to Utf8
    let a_column = result_struct.column(1).as_string::<i32>();
    assert_eq!(
        a_column.into_iter().flatten().collect::<Vec<_>>(),
        vec!["false", "false", "true", "true"]
    );
}

#[test]
fn test_can_cast_struct_rename_field() {
    // can_cast_types returns true because the field types can be cast by position.
    let from_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]
        .into(),
    );

    let to_type = DataType::Struct(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Boolean, false), // Field "c" not in source
        ]
        .into(),
    );

    assert!(can_cast_types(&from_type, &to_type));
}
