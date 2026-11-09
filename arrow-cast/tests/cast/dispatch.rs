// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DictionaryArray, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, NullArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UnionArray, new_null_array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::DataType::{Date32, Int32};
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, UnionFields, UnionMode};

#[test]
fn test_cast_unsupported_types_error() {
    let array = BooleanArray::from(vec![true, false]);
    assert!(!can_cast_types(array.data_type(), &Date32));
    for safe in [true, false] {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        let error = cast_with_options(&array, &Date32, &options).unwrap_err();
        assert!(matches!(error, ArrowError::InvalidArgumentError(_)));
    }
}

#[test]
fn test_cast_invalid_value_error() {
    let array = StringArray::from(vec!["x"]);
    assert!(can_cast_types(array.data_type(), &Int32));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let error = cast_with_options(&array, &Int32, &options).unwrap_err();
    assert!(matches!(error, ArrowError::CastError(_)));

    let result = cast(&array, &Int32).unwrap();
    assert!(result.is_null(0));
}

#[test]
fn test_cast_null_array_to_from_decimal_array() {
    let data_type = DataType::Decimal128(12, 4);
    let array = new_null_array(&DataType::Null, 4);
    assert_eq!(array.data_type(), &DataType::Null);
    let cast_array = cast(&array, &data_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), &data_type);
    for i in 0..4 {
        assert!(cast_array.is_null(i));
    }

    let array = new_null_array(&data_type, 4);
    assert_eq!(array.data_type(), &data_type);
    let cast_array = cast(&array, &DataType::Null).expect("cast failed");
    assert_eq!(cast_array.data_type(), &DataType::Null);
    assert_eq!(cast_array.len(), 4);
    assert_eq!(cast_array.logical_nulls().unwrap().null_count(), 4);
}

#[test]
fn test_cast_null_array_to_primitive_array() {
    macro_rules! typed_test {
        ($ARR_TYPE:ident, $DATATYPE:ident, $TYPE:tt) => {{
            {
                let array = Arc::new(NullArray::new(6)) as ArrayRef;
                let expected = $ARR_TYPE::from(vec![None; 6]);
                let cast_type = DataType::$DATATYPE;
                let cast_array = cast(&array, &cast_type).expect("cast failed");
                let cast_array = cast_array.as_primitive::<$TYPE>();
                assert_eq!(cast_array.data_type(), &cast_type);
                assert_eq!(cast_array, &expected);
            }
        }};
    }

    typed_test!(Int16Array, Int16, Int16Type);
    typed_test!(Int32Array, Int32, Int32Type);
    typed_test!(Int64Array, Int64, Int64Type);

    typed_test!(UInt16Array, UInt16, UInt16Type);
    typed_test!(UInt32Array, UInt32, UInt32Type);
    typed_test!(UInt64Array, UInt64, UInt64Type);

    typed_test!(Float16Array, Float16, Float16Type);
    typed_test!(Float32Array, Float32, Float32Type);
    typed_test!(Float64Array, Float64, Float64Type);

    typed_test!(Date32Array, Date32, Date32Type);
    typed_test!(Date64Array, Date64, Date64Type);
}

fn cast_from_null_to_other_base(data_type: &DataType, is_complex: bool) {
    // Cast from null to data_type
    let array = new_null_array(&DataType::Null, 4);
    assert_eq!(array.data_type(), &DataType::Null);
    let cast_array = cast(&array, data_type).expect("cast failed");
    assert_eq!(cast_array.data_type(), data_type);
    for i in 0..4 {
        if is_complex {
            assert!(cast_array.logical_nulls().unwrap().is_null(i));
        } else {
            assert!(cast_array.is_null(i));
        }
    }
}

fn cast_from_null_to_other(data_type: &DataType) {
    cast_from_null_to_other_base(data_type, false);
}

fn cast_from_null_to_other_complex(data_type: &DataType) {
    cast_from_null_to_other_base(data_type, true);
}

#[test]
fn test_cast_null_to_variable_sized() {
    cast_from_null_to_other(&DataType::Utf8);
    cast_from_null_to_other(&DataType::LargeUtf8);
    cast_from_null_to_other(&DataType::Binary);
    cast_from_null_to_other(&DataType::LargeBinary);
}

#[test]
fn test_cast_null_to_nested_type() {
    // Cast null to map
    let data_type = DataType::Map(
        Arc::new(Field::new_struct(
            Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
            vec![
                Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Int32, true),
            ],
            false,
        )),
        false,
    );
    cast_from_null_to_other(&data_type);

    // Cast null to list
    let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    cast_from_null_to_other(&data_type);
    let data_type = DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
    cast_from_null_to_other(&data_type);
    let data_type =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 4);
    cast_from_null_to_other(&data_type);

    // Cast null to dictionary
    let values = vec![None, None, None, None] as Vec<Option<&str>>;
    let array: DictionaryArray<Int8Type> = values.into_iter().collect();
    let array = Arc::new(array) as ArrayRef;
    let data_type = array.data_type().to_owned();
    cast_from_null_to_other(&data_type);

    // Cast null to struct
    let data_type = DataType::Struct(vec![Field::new("data", DataType::Int64, false)].into());
    cast_from_null_to_other(&data_type);

    let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
    cast_from_null_to_other(&target_type);

    let target_type = DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
    cast_from_null_to_other(&target_type);

    let fields = UnionFields::from_fields(vec![Field::new("a", DataType::Int64, false)]);
    let target_type = DataType::Union(fields, UnionMode::Sparse);
    cast_from_null_to_other_complex(&target_type);

    let target_type = DataType::RunEndEncoded(
        Arc::new(Field::new("item", DataType::Int32, true)),
        Arc::new(Field::new("item", DataType::Int32, true)),
    );
    cast_from_null_to_other_complex(&target_type);
}

#[test]
fn test_can_cast_interval_to_int64_matches_cast() {
    // Assert the two agree for every interval unit, rather than hard coding the answer.
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(IntervalYearMonthArray::from(vec![12])),
        Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(1, 2)])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3),
        ])),
    ];
    for array in arrays {
        let from = array.data_type();
        assert_eq!(
            can_cast_types(from, &DataType::Int64),
            cast(&array, &DataType::Int64).is_ok(),
            "can_cast_types disagrees with cast for {from} -> Int64"
        );
    }
}

#[test]
fn test_cast_union_to_int64_skips_uncastable_interval_child() {
    // `resolve_child_array` picks the first child `can_cast_types` accepts, so an
    // over-permissive answer made it pick the interval child instead of the `Utf8` one.
    let fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("iv", DataType::Interval(IntervalUnit::YearMonth), true),
            Field::new("s", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let union = UnionArray::try_new(
        fields,
        vec![0, 1, 0].into(),
        None,
        vec![
            Arc::new(IntervalYearMonthArray::from(vec![Some(12), None, Some(24)])),
            Arc::new(StringArray::from(vec![None, Some("77"), None])),
        ],
    )
    .unwrap();

    let casted = cast(&union, &DataType::Int64).unwrap();
    let casted = casted.as_primitive::<Int64Type>();
    assert!(casted.is_null(0));
    assert_eq!(casted.value(1), 77);
    assert!(casted.is_null(2));
}

const CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

#[test]
#[expect(clippy::assertions_on_constants)]
fn test_const_options() {
    assert!(CAST_OPTIONS.safe)
}
