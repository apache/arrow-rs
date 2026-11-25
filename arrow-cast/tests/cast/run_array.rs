// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int16Array, Int32Array, Int64Array, RunArray, StringArray,
    UInt64Array,
};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, cast, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field};

#[test]
fn test_run_end_encoded_to_primitive() {
    // Create a RunEndEncoded array: [1, 1, 2, 2, 2, 3]
    let run_ends = Int32Array::from(vec![2, 5, 6]);
    let values = Int32Array::from(vec![1, 2, 3]);
    let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(run_array) as ArrayRef;
    // Cast to Int64
    let cast_result = cast(&array_ref, &DataType::Int64).unwrap();
    // Verify the result is an Int64 array
    let result_run_array = cast_result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        result_run_array.values(),
        &[1i64, 1i64, 2i64, 2i64, 2i64, 3i64]
    );
}

#[test]
fn test_sliced_run_end_encoded_to_primitive() {
    let run_ends = Int32Array::from(vec![2, 5, 6]);
    let values = Int32Array::from(vec![1, 2, 3]);
    // [1, 1, 2, 2, 2, 3]
    let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
    let run_array = run_array.slice(3, 3); // [2, 2, 3]
    let array_ref = Arc::new(run_array) as ArrayRef;

    let cast_result = cast(&array_ref, &DataType::Int64).unwrap();
    let result_run_array = cast_result.as_primitive::<Int64Type>();
    assert_eq!(result_run_array.values(), &[2, 2, 3]);
}

#[test]
fn test_run_end_encoded_to_string() {
    let run_ends = Int32Array::from(vec![2, 3, 5]);
    let values = Int32Array::from(vec![10, 20, 30]);
    let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(run_array) as ArrayRef;

    // Cast to String
    let cast_result = cast(&array_ref, &DataType::Utf8).unwrap();

    // Verify the result is a String array
    let result_array = cast_result.as_any().downcast_ref::<StringArray>().unwrap();
    // Check that values are correct
    assert_eq!(result_array.value(0), "10");
    assert_eq!(result_array.value(1), "10");
    assert_eq!(result_array.value(2), "20");
}

#[test]
fn test_primitive_to_run_end_encoded() {
    // Create an Int32 array with repeated values: [1, 1, 2, 2, 2, 3]
    let source_array = Int32Array::from(vec![1, 1, 2, 2, 2, 3]);
    let array_ref = Arc::new(source_array) as ArrayRef;

    // Cast to RunEndEncoded<Int32, Int32>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int32,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();

    // Verify the result is a RunArray
    let result_run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();

    // Check run structure: runs should end at positions [2, 5, 6]
    assert_eq!(result_run_array.run_ends().values(), &[2, 5, 6]);

    // Check values: should be [1, 2, 3]
    let values_array = result_run_array.values().as_primitive::<Int32Type>();
    assert_eq!(values_array.values(), &[1, 2, 3]);
}

#[test]
fn test_primitive_to_run_end_encoded_with_nulls() {
    let source_array = Int32Array::from(vec![
        Some(1),
        Some(1),
        None,
        None,
        Some(2),
        Some(2),
        Some(3),
        Some(3),
        None,
        None,
        Some(4),
        Some(4),
        Some(5),
        Some(5),
        None,
        None,
    ]);
    let array_ref = Arc::new(source_array) as ArrayRef;
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int32,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();
    let result_run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();
    assert_eq!(
        result_run_array.run_ends().values(),
        &[2, 4, 6, 8, 10, 12, 14, 16]
    );
    assert_eq!(
        result_run_array
            .values()
            .as_primitive::<Int32Type>()
            .values(),
        &[1, 0, 2, 3, 0, 4, 5, 0]
    );
    assert_eq!(result_run_array.values().null_count(), 3);
}

#[test]
fn test_primitive_to_run_end_encoded_with_nulls_consecutive() {
    let source_array = Int64Array::from(vec![
        Some(1),
        Some(1),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(4),
        Some(20),
        Some(500),
        Some(500),
        None,
        None,
    ]);
    let array_ref = Arc::new(source_array) as ArrayRef;
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int16,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int64,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();
    let result_run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(
        result_run_array.run_ends().values(),
        &[2, 10, 11, 12, 14, 16]
    );
    assert_eq!(
        result_run_array
            .values()
            .as_primitive::<Int64Type>()
            .values(),
        &[1, 0, 4, 20, 500, 0]
    );
    assert_eq!(result_run_array.values().null_count(), 2);
}

#[test]
fn test_string_to_run_end_encoded() {
    // Create a String array with repeated values: ["a", "a", "b", "c", "c"]
    let source_array = StringArray::from(vec!["a", "a", "b", "c", "c"]);
    let array_ref = Arc::new(source_array) as ArrayRef;

    // Cast to RunEndEncoded<Int32, String>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();

    // Verify the result is a RunArray
    let result_run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();

    // Check run structure: runs should end at positions [2, 3, 5]
    assert_eq!(result_run_array.run_ends().values(), &[2, 3, 5]);

    // Check values: should be ["a", "b", "c"]
    let values_array = result_run_array.values().as_string::<i32>();
    assert_eq!(values_array.value(0), "a");
    assert_eq!(values_array.value(1), "b");
    assert_eq!(values_array.value(2), "c");
}

#[test]
fn test_empty_array_to_run_end_encoded() {
    // Create an empty Int32 array
    let source_array = Int32Array::from(Vec::<i32>::new());
    let array_ref = Arc::new(source_array) as ArrayRef;

    // Cast to RunEndEncoded<Int32, Int32>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int32,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();

    // Verify the result is an empty RunArray
    let result_run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();

    // Check that both run_ends and values are empty
    assert_eq!(result_run_array.run_ends().len(), 0);
    assert_eq!(result_run_array.values().len(), 0);
}

#[test]
fn test_run_end_encoded_with_nulls() {
    // Create a RunEndEncoded array with nulls: [1, 1, null, 2, 2]
    let run_ends = Int32Array::from(vec![2, 3, 5]);
    let values = Int32Array::from(vec![Some(1), None, Some(2)]);
    let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(run_array) as ArrayRef;

    // Cast to String
    let cast_result = cast(&array_ref, &DataType::Utf8).unwrap();

    // Verify the result preserves nulls
    let result_run_array = cast_result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result_run_array.value(0), "1");
    assert!(result_run_array.is_null(2));
    assert_eq!(result_run_array.value(4), "2");
}

#[test]
fn test_different_index_types() {
    // Test with Int16 index type
    let source_array = Int32Array::from(vec![1, 1, 2, 3, 3]);
    let array_ref = Arc::new(source_array) as ArrayRef;

    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int16,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int32,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();
    assert_eq!(cast_result.data_type(), &target_type);

    // Verify the cast worked correctly: values are [1, 2, 3]
    // and run-ends are [2, 3, 5]
    let run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(0), 1);
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(1), 2);
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(2), 3);
    assert_eq!(run_array.run_ends().values(), &[2i16, 3i16, 5i16]);

    // Test again with Int64 index type
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Int32,
            true,
        )),
    );
    let cast_result = cast(&array_ref, &target_type).unwrap();
    assert_eq!(cast_result.data_type(), &target_type);

    // Verify the cast worked correctly: values are [1, 2, 3]
    // and run-ends are [2, 3, 5]
    let run_array = cast_result
        .as_any()
        .downcast_ref::<RunArray<Int64Type>>()
        .unwrap();
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(0), 1);
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(1), 2);
    assert_eq!(run_array.values().as_primitive::<Int32Type>().value(2), 3);
    assert_eq!(run_array.run_ends().values(), &[2i64, 3i64, 5i64]);
}

/// Test casting RunEndEncoded<Int64, String> to RunEndEncoded<Int16, String> should fail
#[test]
fn test_cast_run_end_encoded_int64_to_int16_should_fail() {
    // Construct a valid REE array with Int64 run-ends
    let run_ends = Int64Array::from(vec![100_000, 400_000, 700_000]); // values too large for Int16
    let values = StringArray::from(vec!["a", "b", "c"]);

    let ree_array = RunArray::<Int64Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(ree_array) as ArrayRef;

    // Attempt to cast to RunEndEncoded<Int16, Utf8>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int16,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_options = CastOptions {
        safe: false, // This should make it fail instead of returning nulls
        format_options: FormatOptions::default(),
    };

    // This should fail due to run-end overflow
    let result: Result<Arc<dyn Array + 'static>, ArrowError> =
        cast_with_options(&array_ref, &target_type, &cast_options);

    let e = result.expect_err("Cast should have failed but succeeded");
    assert!(
        e.to_string()
            .contains("Cast error: Can't cast value 100000 to type Int16")
    );
}

#[test]
fn test_cast_run_end_encoded_int64_to_int16_with_safe_should_fail_with_null_invalid_error() {
    // Construct a valid REE array with Int64 run-ends
    let run_ends = Int64Array::from(vec![100_000, 400_000, 700_000]); // values too large for Int16
    let values = StringArray::from(vec!["a", "b", "c"]);

    let ree_array = RunArray::<Int64Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(ree_array) as ArrayRef;

    // Attempt to cast to RunEndEncoded<Int16, Utf8>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int16,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_options = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };

    // This fails even though safe is true because the run_ends array has null values
    let result: Result<Arc<dyn Array + 'static>, ArrowError> =
        cast_with_options(&array_ref, &target_type, &cast_options);
    let e = result.expect_err("Cast should have failed but succeeded");
    assert!(
        e.to_string()
            .contains("Invalid argument error: Found null values in run_ends array. The run_ends array should not have null values.")
    );
}

/// Test casting RunEndEncoded<Int16, String> to RunEndEncoded<Int64, String> should succeed
#[test]
fn test_cast_run_end_encoded_int16_to_int64_should_succeed() {
    // Construct a valid REE array with Int16 run-ends
    let run_ends = Int16Array::from(vec![2, 5, 8]); // values that fit in Int16
    let values = StringArray::from(vec!["a", "b", "c"]);

    let ree_array = RunArray::<Int16Type>::try_new(&run_ends, &values).unwrap();
    let array_ref = Arc::new(ree_array) as ArrayRef;

    // Attempt to cast to RunEndEncoded<Int64, Utf8> (upcast should succeed)
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    // This should succeed due to valid upcast
    let result: Result<Arc<dyn Array + 'static>, ArrowError> =
        cast_with_options(&array_ref, &target_type, &cast_options);

    let array_ref = result.expect("Cast should have succeeded but failed");
    // Downcast to RunArray<Int64Type>
    let run_array = array_ref
        .as_any()
        .downcast_ref::<RunArray<Int64Type>>()
        .unwrap();

    // Verify the cast worked correctly
    // Assert the values were cast correctly
    assert_eq!(run_array.run_ends().values(), &[2i64, 5i64, 8i64]);
    assert_eq!(run_array.values().as_string::<i32>().value(0), "a");
    assert_eq!(run_array.values().as_string::<i32>().value(1), "b");
    assert_eq!(run_array.values().as_string::<i32>().value(2), "c");
}

#[test]
fn test_cast_run_end_encoded_dictionary_to_run_end_encoded() {
    // Construct a valid dictionary encoded array
    let values = StringArray::from_iter([Some("a"), Some("b"), Some("c")]);
    let keys = UInt64Array::from_iter(vec![1, 1, 1, 0, 0, 0, 2, 2, 2]);
    let array_ref = Arc::new(DictionaryArray::new(keys, Arc::new(values))) as ArrayRef;

    // Attempt to cast to RunEndEncoded<Int64, Utf8>
    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    // This should succeed
    let result = cast_with_options(&array_ref, &target_type, &cast_options)
        .expect("Cast should have succeeded but failed");

    // Verify the cast worked correctly
    // Assert the values were cast correctly
    let run_array = result
        .as_any()
        .downcast_ref::<RunArray<Int64Type>>()
        .unwrap();
    assert_eq!(run_array.values().as_string::<i32>().value(0), "b");
    assert_eq!(run_array.values().as_string::<i32>().value(1), "a");
    assert_eq!(run_array.values().as_string::<i32>().value(2), "c");

    // Verify the run-ends were cast correctly (run ends at 3, 6, 9)
    assert_eq!(run_array.run_ends().values(), &[3i64, 6i64, 9i64]);
}

#[test]
fn test_cast_between_sliced_run_end_encoded() {
    let run_ends = Int16Array::from(vec![2, 5, 8]);
    let values = StringArray::from(vec!["a", "b", "c"]);

    let ree_array = RunArray::<Int16Type>::try_new(&run_ends, &values).unwrap();
    let ree_array = ree_array.slice(1, 2);
    let array_ref = Arc::new(ree_array) as ArrayRef;

    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new(
            Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        )),
        Arc::new(Field::new(
            Field::REE_VALUES_FIELD_DEFAULT_NAME,
            DataType::Utf8,
            true,
        )),
    );
    let cast_options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    let result = cast_with_options(&array_ref, &target_type, &cast_options).unwrap();
    let run_array = result.as_run::<Int64Type>();
    let run_array = run_array.downcast::<StringArray>().unwrap();

    let expected = vec!["a", "b"];
    let actual = run_array.into_iter().flatten().collect::<Vec<_>>();

    assert_eq!(expected, actual);
}
