// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::{AsArray, as_boolean_array};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float16Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::DataType;
use arrow_schema::DataType::{
    Boolean, Float16, Float32, Float64, Int8, Int16, Int32, Int64, LargeUtf8, UInt8, UInt16,
    UInt32, UInt64, Utf8, Utf8View,
};
use half::f16;

#[test]
fn test_cast_i32_to_f64() {
    let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
    let b = cast(&array, &DataType::Float64).unwrap();
    let c = b.as_primitive::<Float64Type>();
    assert_eq!(5.0, c.value(0));
    assert_eq!(6.0, c.value(1));
    assert_eq!(7.0, c.value(2));
    assert_eq!(8.0, c.value(3));
    assert_eq!(9.0, c.value(4));
}

#[test]
fn test_cast_i32_to_u8() {
    let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
    let b = cast(&array, &DataType::UInt8).unwrap();
    let c = b.as_primitive::<UInt8Type>();
    assert!(!c.is_valid(0));
    assert_eq!(6, c.value(1));
    assert!(!c.is_valid(2));
    assert_eq!(8, c.value(3));
    // overflows return None
    assert!(!c.is_valid(4));
}

#[test]
#[should_panic(expected = "Can't cast value -5 to type UInt8")]
fn test_cast_int32_to_u8_with_error() {
    let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
    // overflow with the error
    let cast_option = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let result = cast_with_options(&array, &DataType::UInt8, &cast_option);
    assert!(result.is_err());
    result.unwrap();
}

#[test]
fn test_cast_i32_to_u8_sliced() {
    let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
    assert_eq!(0, array.offset());
    let array = array.slice(2, 3);
    let b = cast(&array, &DataType::UInt8).unwrap();
    assert_eq!(3, b.len());
    let c = b.as_primitive::<UInt8Type>();
    assert!(!c.is_valid(0));
    assert_eq!(8, c.value(1));
    // overflows return None
    assert!(!c.is_valid(2));
}

#[test]
fn test_cast_i32_to_i32() {
    let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(5, c.value(0));
    assert_eq!(6, c.value(1));
    assert_eq!(7, c.value(2));
    assert_eq!(8, c.value(3));
    assert_eq!(9, c.value(4));
}

#[test]
fn test_cast_int_to_utf8view() {
    let inputs = vec![
        Arc::new(Int8Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(Int16Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(Int64Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(UInt8Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(UInt16Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
    ];
    let expected: ArrayRef = Arc::new(StringViewArray::from(vec![
        None,
        Some("8"),
        Some("9"),
        Some("10"),
    ]));

    for array in inputs {
        assert!(can_cast_types(array.data_type(), &DataType::Utf8View));
        let arr = cast(&array, &DataType::Utf8View).unwrap();
        assert_eq!(expected.as_ref(), arr.as_ref());
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_float_to_utf8view() {
    let inputs = vec![
        Arc::new(Float16Array::from(vec![
            Some(f16::from_f64(1.5)),
            Some(f16::from_f64(2.5)),
            None,
        ])) as ArrayRef,
        Arc::new(Float32Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
    ];

    let expected: ArrayRef = Arc::new(StringViewArray::from(vec![Some("1.5"), Some("2.5"), None]));

    for array in inputs {
        assert!(can_cast_types(array.data_type(), &DataType::Utf8View));
        let arr = cast(&array, &DataType::Utf8View).unwrap();
        assert_eq!(expected.as_ref(), arr.as_ref());
    }
}

#[test]
fn test_cast_utf8_to_i32() {
    let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(5, c.value(0));
    assert_eq!(6, c.value(1));
    assert!(!c.is_valid(2));
    assert_eq!(8, c.value(3));
    assert!(!c.is_valid(4));
}

#[test]
fn test_cast_utf8view_to_i32() {
    let array = StringViewArray::from(vec!["5", "6", "seven", "8", "9.1"]);
    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(5, c.value(0));
    assert_eq!(6, c.value(1));
    assert!(!c.is_valid(2));
    assert_eq!(8, c.value(3));
    assert!(!c.is_valid(4));
}

#[test]
fn test_cast_utf8view_to_f32() {
    let array = StringViewArray::from(vec!["3", "4.56", "seven", "8.9"]);
    let b = cast(&array, &DataType::Float32).unwrap();
    let c = b.as_primitive::<Float32Type>();
    assert_eq!(3.0, c.value(0));
    assert_eq!(4.56, c.value(1));
    assert!(!c.is_valid(2));
    assert_eq!(8.9, c.value(3));
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_string_to_f16() {
    let arrays = [
        Arc::new(StringViewArray::from(vec!["3", "4.56", "seven", "8.9"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["3", "4.56", "seven", "8.9"])),
        Arc::new(LargeStringArray::from(vec!["3", "4.56", "seven", "8.9"])),
    ];
    for array in arrays {
        let b = cast(&array, &DataType::Float16).unwrap();
        let c = b.as_primitive::<Float16Type>();
        assert_eq!(half::f16::from_f32(3.0), c.value(0));
        assert_eq!(half::f16::from_f32(4.56), c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(half::f16::from_f32(8.9), c.value(3));
    }
}

#[test]
fn test_cast_with_options_utf8_to_i32() {
    let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
    let result = cast_with_options(
        &array,
        &DataType::Int32,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => {
            assert!(
                e.to_string()
                    .contains("Cast error: Cannot cast string 'seven' to value of Int32 type",),
                "Error: {e}"
            )
        }
    }
}

#[test]
fn test_cast_utf8_to_bool() {
    let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
    let casted = cast(&strings, &DataType::Boolean).unwrap();
    let expected = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None]);
    assert_eq!(*as_boolean_array(&casted), expected);
}

#[test]
fn test_cast_utf8view_to_bool() {
    let strings = StringViewArray::from(vec!["true", "false", "invalid", " Y ", ""]);
    let casted = cast(&strings, &DataType::Boolean).unwrap();
    let expected = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None]);
    assert_eq!(*as_boolean_array(&casted), expected);
}

#[test]
fn test_cast_with_options_utf8_to_bool() {
    let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
    let casted = cast_with_options(
        &strings,
        &DataType::Boolean,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    match casted {
        Ok(_) => panic!("expected error"),
        Err(e) => {
            assert!(
                e.to_string()
                    .contains("Cast error: Cannot cast value 'invalid' to value of Boolean type")
            )
        }
    }
}

#[test]
fn test_cast_bool_to_i32() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(1, c.value(0));
    assert_eq!(0, c.value(1));
    assert!(!c.is_valid(2));
}

#[test]
fn test_cast_bool_numeric_sliced() {
    for nulls in [
        None,
        Some(NullBuffer::from(
            [false, true, true, false, true, true].repeat(12),
        )),
        Some(NullBuffer::new_null(72)),
    ] {
        // Include true values under nulls and slices crossing a bitmap word boundary.
        let booleans = BooleanArray::new(
            BooleanBuffer::from([true, false, true, false, true, false].repeat(12)),
            nulls.clone(),
        );
        let numbers = Int32Array::new([1, 0, 1, 0, 1, 0].repeat(12).into(), nulls);
        for data_type in [
            Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64,
        ] {
            let expected = cast(&numbers, &data_type).unwrap();
            for offset in [0, 3, 63, 72] {
                let input = booleans.slice(offset, 72 - offset);
                let expected = expected.slice(offset, 72 - offset);
                let actual = cast(&input, &data_type).unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref());

                let actual = cast(expected.as_ref(), &Boolean).unwrap();
                assert_eq!(actual.as_ref(), &input);
            }
        }
    }
}

#[test]
fn test_cast_float_to_bool_special_values() {
    let numbers = Float64Array::from(vec![
        0.0,
        -0.0,
        1.5,
        -1.5,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]);
    let expected = BooleanArray::from(vec![false, false, true, true, true, true, true]);
    for data_type in [Float16, Float32, Float64] {
        let input = cast(&numbers, &data_type).unwrap();
        let actual = cast(input.as_ref(), &Boolean).unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }
}

#[test]
fn test_cast_bool_to_string() {
    for nulls in [
        None,
        Some(NullBuffer::from([false, false, true, true].repeat(18))),
        Some(NullBuffer::new_null(72)),
    ] {
        // Include both Boolean values under nulls, an unaligned slice, and an empty slice.
        let array = BooleanArray::new(BooleanBuffer::from([true, false].repeat(36)), nulls);
        let expected: Vec<_> = ["true", "false"]
            .repeat(36)
            .into_iter()
            .enumerate()
            .map(|(i, value)| array.is_valid(i).then_some(value))
            .collect();
        for (offset, len) in [(0, 72), (3, 65), (72, 0)] {
            let input = array.slice(offset, len);
            for data_type in [Utf8, LargeUtf8, Utf8View] {
                let actual = cast(&input, &data_type).unwrap();
                let values: Vec<_> = match data_type {
                    Utf8 => actual.as_string::<i32>().iter().collect(),
                    LargeUtf8 => actual.as_string::<i64>().iter().collect(),
                    Utf8View => actual.as_string_view().iter().collect(),
                    _ => unreachable!(),
                };
                assert_eq!(values, expected[offset..offset + len]);
            }
        }
    }
}

#[test]
fn test_cast_bool_to_f64() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Float64).unwrap();
    let c = b.as_primitive::<Float64Type>();
    assert_eq!(1.0, c.value(0));
    assert_eq!(0.0, c.value(1));
    assert!(!c.is_valid(2));
}

#[test]
fn test_cast_string_to_integral_overflow() {
    let str = Arc::new(StringArray::from(vec![
        Some("123"),
        Some("-123"),
        Some("86374"),
        Some("-40000"),
        None,
    ])) as ArrayRef;

    let options = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };
    let res = cast_with_options(&str, &DataType::Int16, &options).expect("should cast to i16");
    let expected = Arc::new(Int16Array::from(vec![
        Some(123),
        Some(-123),
        None,
        None,
        None,
    ])) as ArrayRef;
    assert_eq!(&res, &expected);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_f64() {
    let f64_values: Vec<f64> = vec![
        i64::MIN as f64,
        i32::MIN as f64,
        i16::MIN as f64,
        i8::MIN as f64,
        0_f64,
        u8::MAX as f64,
        u16::MAX as f64,
        u32::MAX as f64,
        u64::MAX as f64,
    ];
    let f64_array: ArrayRef = Arc::new(Float64Array::from(f64_values));

    let f64_expected = vec![
        -9223372036854776000.0,
        -2147483648.0,
        -32768.0,
        -128.0,
        0.0,
        255.0,
        65535.0,
        4294967295.0,
        18446744073709552000.0,
    ];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&f64_array, &DataType::Float64)
            .iter()
            .map(|i| i.parse::<f64>().unwrap())
            .collect::<Vec<f64>>()
    );

    let f32_expected = vec![
        -9223372000000000000.0,
        -2147483600.0,
        -32768.0,
        -128.0,
        0.0,
        255.0,
        65535.0,
        4294967300.0,
        18446744000000000000.0,
    ];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&f64_array, &DataType::Float32)
            .iter()
            .map(|i| i.parse::<f32>().unwrap())
            .collect::<Vec<f32>>()
    );

    let f16_expected = vec![
        f16::from_f64(-9223372000000000000.0),
        f16::from_f64(-2147483600.0),
        f16::from_f64(-32768.0),
        f16::from_f64(-128.0),
        f16::from_f64(0.0),
        f16::from_f64(255.0),
        f16::from_f64(65535.0),
        f16::from_f64(4294967300.0),
        f16::from_f64(18446744000000000000.0),
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&f64_array, &DataType::Float16)
            .iter()
            .map(|i| i.parse::<f16>().unwrap())
            .collect::<Vec<f16>>()
    );

    let i64_expected = vec![
        "-9223372036854775808",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "255",
        "65535",
        "4294967295",
        "null",
    ];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&f64_array, &DataType::Int64)
    );

    let i32_expected = vec![
        "null",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "255",
        "65535",
        "null",
        "null",
    ];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&f64_array, &DataType::Int32)
    );

    let i16_expected = vec![
        "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
    ];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&f64_array, &DataType::Int16)
    );

    let i8_expected = vec![
        "null", "null", "null", "-128", "0", "null", "null", "null", "null",
    ];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&f64_array, &DataType::Int8)
    );

    let u64_expected = vec![
        "null",
        "null",
        "null",
        "null",
        "0",
        "255",
        "65535",
        "4294967295",
        "null",
    ];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&f64_array, &DataType::UInt64)
    );

    let u32_expected = vec![
        "null",
        "null",
        "null",
        "null",
        "0",
        "255",
        "65535",
        "4294967295",
        "null",
    ];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&f64_array, &DataType::UInt32)
    );

    let u16_expected = vec![
        "null", "null", "null", "null", "0", "255", "65535", "null", "null",
    ];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&f64_array, &DataType::UInt16)
    );

    let u8_expected = vec![
        "null", "null", "null", "null", "0", "255", "null", "null", "null",
    ];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&f64_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_f32() {
    let f32_values: Vec<f32> = vec![
        i32::MIN as f32,
        i32::MIN as f32,
        i16::MIN as f32,
        i8::MIN as f32,
        0_f32,
        u8::MAX as f32,
        u16::MAX as f32,
        u32::MAX as f32,
        u32::MAX as f32,
    ];
    let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));

    let f64_expected = vec![
        "-2147483648.0",
        "-2147483648.0",
        "-32768.0",
        "-128.0",
        "0.0",
        "255.0",
        "65535.0",
        "4294967296.0",
        "4294967296.0",
    ];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&f32_array, &DataType::Float64)
    );

    let f32_expected = vec![
        "-2147483600.0",
        "-2147483600.0",
        "-32768.0",
        "-128.0",
        "0.0",
        "255.0",
        "65535.0",
        "4294967300.0",
        "4294967300.0",
    ];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&f32_array, &DataType::Float32)
    );

    let f16_expected = vec![
        "-inf", "-inf", "-32768.0", "-128.0", "0.0", "255.0", "inf", "inf", "inf",
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&f32_array, &DataType::Float16)
    );

    let i64_expected = vec![
        "-2147483648",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "255",
        "65535",
        "4294967296",
        "4294967296",
    ];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&f32_array, &DataType::Int64)
    );

    let i32_expected = vec![
        "-2147483648",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "255",
        "65535",
        "null",
        "null",
    ];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&f32_array, &DataType::Int32)
    );

    let i16_expected = vec![
        "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
    ];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&f32_array, &DataType::Int16)
    );

    let i8_expected = vec![
        "null", "null", "null", "-128", "0", "null", "null", "null", "null",
    ];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&f32_array, &DataType::Int8)
    );

    let u64_expected = vec![
        "null",
        "null",
        "null",
        "null",
        "0",
        "255",
        "65535",
        "4294967296",
        "4294967296",
    ];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&f32_array, &DataType::UInt64)
    );

    let u32_expected = vec![
        "null", "null", "null", "null", "0", "255", "65535", "null", "null",
    ];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&f32_array, &DataType::UInt32)
    );

    let u16_expected = vec![
        "null", "null", "null", "null", "0", "255", "65535", "null", "null",
    ];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&f32_array, &DataType::UInt16)
    );

    let u8_expected = vec![
        "null", "null", "null", "null", "0", "255", "null", "null", "null",
    ];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&f32_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_uint64() {
    let u64_values: Vec<u64> = vec![
        0,
        u8::MAX as u64,
        u16::MAX as u64,
        u32::MAX as u64,
        u64::MAX,
    ];
    let u64_array: ArrayRef = Arc::new(UInt64Array::from(u64_values));

    let f64_expected = vec![0.0, 255.0, 65535.0, 4294967295.0, 18446744073709552000.0];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&u64_array, &DataType::Float64)
            .iter()
            .map(|i| i.parse::<f64>().unwrap())
            .collect::<Vec<f64>>()
    );

    let f32_expected = vec![0.0, 255.0, 65535.0, 4294967300.0, 18446744000000000000.0];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&u64_array, &DataType::Float32)
            .iter()
            .map(|i| i.parse::<f32>().unwrap())
            .collect::<Vec<f32>>()
    );

    let f16_expected = vec![
        f16::from_f64(0.0),
        f16::from_f64(255.0),
        f16::from_f64(65535.0),
        f16::from_f64(4294967300.0),
        f16::from_f64(18446744000000000000.0),
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&u64_array, &DataType::Float16)
            .iter()
            .map(|i| i.parse::<f16>().unwrap())
            .collect::<Vec<f16>>()
    );

    let i64_expected = vec!["0", "255", "65535", "4294967295", "null"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&u64_array, &DataType::Int64)
    );

    let i32_expected = vec!["0", "255", "65535", "null", "null"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&u64_array, &DataType::Int32)
    );

    let i16_expected = vec!["0", "255", "null", "null", "null"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&u64_array, &DataType::Int16)
    );

    let i8_expected = vec!["0", "null", "null", "null", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&u64_array, &DataType::Int8)
    );

    let u64_expected = vec!["0", "255", "65535", "4294967295", "18446744073709551615"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&u64_array, &DataType::UInt64)
    );

    let u32_expected = vec!["0", "255", "65535", "4294967295", "null"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&u64_array, &DataType::UInt32)
    );

    let u16_expected = vec!["0", "255", "65535", "null", "null"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&u64_array, &DataType::UInt16)
    );

    let u8_expected = vec!["0", "255", "null", "null", "null"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&u64_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_uint32() {
    let u32_values: Vec<u32> = vec![0, u8::MAX as u32, u16::MAX as u32, u32::MAX];
    let u32_array: ArrayRef = Arc::new(UInt32Array::from(u32_values));

    let f64_expected = vec!["0.0", "255.0", "65535.0", "4294967295.0"];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&u32_array, &DataType::Float64)
    );

    let f32_expected = vec!["0.0", "255.0", "65535.0", "4294967300.0"];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&u32_array, &DataType::Float32)
    );

    let f16_expected = vec!["0.0", "255.0", "inf", "inf"];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&u32_array, &DataType::Float16)
    );

    let i64_expected = vec!["0", "255", "65535", "4294967295"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&u32_array, &DataType::Int64)
    );

    let i32_expected = vec!["0", "255", "65535", "null"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&u32_array, &DataType::Int32)
    );

    let i16_expected = vec!["0", "255", "null", "null"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&u32_array, &DataType::Int16)
    );

    let i8_expected = vec!["0", "null", "null", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&u32_array, &DataType::Int8)
    );

    let u64_expected = vec!["0", "255", "65535", "4294967295"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&u32_array, &DataType::UInt64)
    );

    let u32_expected = vec!["0", "255", "65535", "4294967295"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&u32_array, &DataType::UInt32)
    );

    let u16_expected = vec!["0", "255", "65535", "null"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&u32_array, &DataType::UInt16)
    );

    let u8_expected = vec!["0", "255", "null", "null"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&u32_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_uint16() {
    let u16_values: Vec<u16> = vec![0, u8::MAX as u16, u16::MAX];
    let u16_array: ArrayRef = Arc::new(UInt16Array::from(u16_values));

    let f64_expected = vec!["0.0", "255.0", "65535.0"];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&u16_array, &DataType::Float64)
    );

    let f32_expected = vec!["0.0", "255.0", "65535.0"];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&u16_array, &DataType::Float32)
    );

    let f16_expected = vec!["0.0", "255.0", "inf"];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&u16_array, &DataType::Float16)
    );

    let i64_expected = vec!["0", "255", "65535"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&u16_array, &DataType::Int64)
    );

    let i32_expected = vec!["0", "255", "65535"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&u16_array, &DataType::Int32)
    );

    let i16_expected = vec!["0", "255", "null"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&u16_array, &DataType::Int16)
    );

    let i8_expected = vec!["0", "null", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&u16_array, &DataType::Int8)
    );

    let u64_expected = vec!["0", "255", "65535"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&u16_array, &DataType::UInt64)
    );

    let u32_expected = vec!["0", "255", "65535"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&u16_array, &DataType::UInt32)
    );

    let u16_expected = vec!["0", "255", "65535"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&u16_array, &DataType::UInt16)
    );

    let u8_expected = vec!["0", "255", "null"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&u16_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_uint8() {
    let u8_values: Vec<u8> = vec![0, u8::MAX];
    let u8_array: ArrayRef = Arc::new(UInt8Array::from(u8_values));

    let f64_expected = vec!["0.0", "255.0"];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&u8_array, &DataType::Float64)
    );

    let f32_expected = vec!["0.0", "255.0"];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&u8_array, &DataType::Float32)
    );

    let f16_expected = vec!["0.0", "255.0"];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&u8_array, &DataType::Float16)
    );

    let i64_expected = vec!["0", "255"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&u8_array, &DataType::Int64)
    );

    let i32_expected = vec!["0", "255"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&u8_array, &DataType::Int32)
    );

    let i16_expected = vec!["0", "255"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&u8_array, &DataType::Int16)
    );

    let i8_expected = vec!["0", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&u8_array, &DataType::Int8)
    );

    let u64_expected = vec!["0", "255"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&u8_array, &DataType::UInt64)
    );

    let u32_expected = vec!["0", "255"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&u8_array, &DataType::UInt32)
    );

    let u16_expected = vec!["0", "255"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&u8_array, &DataType::UInt16)
    );

    let u8_expected = vec!["0", "255"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&u8_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_int64() {
    let i64_values: Vec<i64> = vec![
        i64::MIN,
        i32::MIN as i64,
        i16::MIN as i64,
        i8::MIN as i64,
        0,
        i8::MAX as i64,
        i16::MAX as i64,
        i32::MAX as i64,
        i64::MAX,
    ];
    let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_values));

    let f64_expected = vec![
        -9223372036854776000.0,
        -2147483648.0,
        -32768.0,
        -128.0,
        0.0,
        127.0,
        32767.0,
        2147483647.0,
        9223372036854776000.0,
    ];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&i64_array, &DataType::Float64)
            .iter()
            .map(|i| i.parse::<f64>().unwrap())
            .collect::<Vec<f64>>()
    );

    let f32_expected = vec![
        -9223372000000000000.0,
        -2147483600.0,
        -32768.0,
        -128.0,
        0.0,
        127.0,
        32767.0,
        2147483600.0,
        9223372000000000000.0,
    ];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&i64_array, &DataType::Float32)
            .iter()
            .map(|i| i.parse::<f32>().unwrap())
            .collect::<Vec<f32>>()
    );

    let f16_expected = vec![
        f16::from_f64(-9223372000000000000.0),
        f16::from_f64(-2147483600.0),
        f16::from_f64(-32768.0),
        f16::from_f64(-128.0),
        f16::from_f64(0.0),
        f16::from_f64(127.0),
        f16::from_f64(32767.0),
        f16::from_f64(2147483600.0),
        f16::from_f64(9223372000000000000.0),
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&i64_array, &DataType::Float16)
            .iter()
            .map(|i| i.parse::<f16>().unwrap())
            .collect::<Vec<f16>>()
    );

    let i64_expected = vec![
        "-9223372036854775808",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "127",
        "32767",
        "2147483647",
        "9223372036854775807",
    ];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&i64_array, &DataType::Int64)
    );

    let i32_expected = vec![
        "null",
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "127",
        "32767",
        "2147483647",
        "null",
    ];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&i64_array, &DataType::Int32)
    );

    assert_eq!(
        i32_expected,
        get_cast_values::<Date32Type>(&i64_array, &DataType::Date32)
    );

    let i16_expected = vec![
        "null", "null", "-32768", "-128", "0", "127", "32767", "null", "null",
    ];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&i64_array, &DataType::Int16)
    );

    let i8_expected = vec![
        "null", "null", "null", "-128", "0", "127", "null", "null", "null",
    ];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&i64_array, &DataType::Int8)
    );

    let u64_expected = vec![
        "null",
        "null",
        "null",
        "null",
        "0",
        "127",
        "32767",
        "2147483647",
        "9223372036854775807",
    ];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&i64_array, &DataType::UInt64)
    );

    let u32_expected = vec![
        "null",
        "null",
        "null",
        "null",
        "0",
        "127",
        "32767",
        "2147483647",
        "null",
    ];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&i64_array, &DataType::UInt32)
    );

    let u16_expected = vec![
        "null", "null", "null", "null", "0", "127", "32767", "null", "null",
    ];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&i64_array, &DataType::UInt16)
    );

    let u8_expected = vec![
        "null", "null", "null", "null", "0", "127", "null", "null", "null",
    ];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&i64_array, &DataType::UInt8)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_int32() {
    let i32_values: Vec<i32> = vec![
        i32::MIN,
        i16::MIN as i32,
        i8::MIN as i32,
        0,
        i8::MAX as i32,
        i16::MAX as i32,
        i32::MAX,
    ];
    let i32_array: ArrayRef = Arc::new(Int32Array::from(i32_values));

    let f64_expected = vec![
        "-2147483648.0",
        "-32768.0",
        "-128.0",
        "0.0",
        "127.0",
        "32767.0",
        "2147483647.0",
    ];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&i32_array, &DataType::Float64)
    );

    let f32_expected = vec![
        "-2147483600.0",
        "-32768.0",
        "-128.0",
        "0.0",
        "127.0",
        "32767.0",
        "2147483600.0",
    ];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&i32_array, &DataType::Float32)
    );

    let f16_expected = vec![
        f16::from_f64(-2147483600.0),
        f16::from_f64(-32768.0),
        f16::from_f64(-128.0),
        f16::from_f64(0.0),
        f16::from_f64(127.0),
        f16::from_f64(32767.0),
        f16::from_f64(2147483600.0),
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&i32_array, &DataType::Float16)
            .iter()
            .map(|i| i.parse::<f16>().unwrap())
            .collect::<Vec<f16>>()
    );

    let i16_expected = vec!["null", "-32768", "-128", "0", "127", "32767", "null"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&i32_array, &DataType::Int16)
    );

    let i8_expected = vec!["null", "null", "-128", "0", "127", "null", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&i32_array, &DataType::Int8)
    );

    let u64_expected = vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&i32_array, &DataType::UInt64)
    );

    let u32_expected = vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&i32_array, &DataType::UInt32)
    );

    let u16_expected = vec!["null", "null", "null", "0", "127", "32767", "null"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&i32_array, &DataType::UInt16)
    );

    let u8_expected = vec!["null", "null", "null", "0", "127", "null", "null"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&i32_array, &DataType::UInt8)
    );

    // The date32 to date64 cast increases the numerical values in order to keep the same dates.
    let i64_expected = vec![
        "-185542587187200000",
        "-2831155200000",
        "-11059200000",
        "0",
        "10972800000",
        "2831068800000",
        "185542587100800000",
    ];
    assert_eq!(
        i64_expected,
        get_cast_values::<Date64Type>(&i32_array, &DataType::Date64)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_int16() {
    let i16_values: Vec<i16> = vec![i16::MIN, i8::MIN as i16, 0, i8::MAX as i16, i16::MAX];
    let i16_array: ArrayRef = Arc::new(Int16Array::from(i16_values));

    let f64_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&i16_array, &DataType::Float64)
    );

    let f32_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&i16_array, &DataType::Float32)
    );

    let f16_expected = vec![
        f16::from_f64(-32768.0),
        f16::from_f64(-128.0),
        f16::from_f64(0.0),
        f16::from_f64(127.0),
        f16::from_f64(32767.0),
    ];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&i16_array, &DataType::Float16)
            .iter()
            .map(|i| i.parse::<f16>().unwrap())
            .collect::<Vec<f16>>()
    );

    let i64_expected = vec!["-32768", "-128", "0", "127", "32767"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&i16_array, &DataType::Int64)
    );

    let i32_expected = vec!["-32768", "-128", "0", "127", "32767"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&i16_array, &DataType::Int32)
    );

    let i16_expected = vec!["-32768", "-128", "0", "127", "32767"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&i16_array, &DataType::Int16)
    );

    let i8_expected = vec!["null", "-128", "0", "127", "null"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&i16_array, &DataType::Int8)
    );

    let u64_expected = vec!["null", "null", "0", "127", "32767"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&i16_array, &DataType::UInt64)
    );

    let u32_expected = vec!["null", "null", "0", "127", "32767"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&i16_array, &DataType::UInt32)
    );

    let u16_expected = vec!["null", "null", "0", "127", "32767"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&i16_array, &DataType::UInt16)
    );

    let u8_expected = vec!["null", "null", "0", "127", "null"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&i16_array, &DataType::UInt8)
    );
}

#[test]
fn test_cast_from_date32() {
    let i32_values: Vec<i32> = vec![
        i32::MIN,
        i16::MIN as i32,
        i8::MIN as i32,
        0,
        i8::MAX as i32,
        i16::MAX as i32,
        i32::MAX,
    ];
    let date32_array: ArrayRef = Arc::new(Date32Array::from(i32_values));

    let i64_expected = vec![
        "-2147483648",
        "-32768",
        "-128",
        "0",
        "127",
        "32767",
        "2147483647",
    ];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&date32_array, &DataType::Int64)
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_from_int8() {
    let i8_values: Vec<i8> = vec![i8::MIN, 0, i8::MAX];
    let i8_array = Int8Array::from(i8_values);

    let f64_expected = vec!["-128.0", "0.0", "127.0"];
    assert_eq!(
        f64_expected,
        get_cast_values::<Float64Type>(&i8_array, &DataType::Float64)
    );

    let f32_expected = vec!["-128.0", "0.0", "127.0"];
    assert_eq!(
        f32_expected,
        get_cast_values::<Float32Type>(&i8_array, &DataType::Float32)
    );

    let f16_expected = vec!["-128.0", "0.0", "127.0"];
    assert_eq!(
        f16_expected,
        get_cast_values::<Float16Type>(&i8_array, &DataType::Float16)
    );

    let i64_expected = vec!["-128", "0", "127"];
    assert_eq!(
        i64_expected,
        get_cast_values::<Int64Type>(&i8_array, &DataType::Int64)
    );

    let i32_expected = vec!["-128", "0", "127"];
    assert_eq!(
        i32_expected,
        get_cast_values::<Int32Type>(&i8_array, &DataType::Int32)
    );

    let i16_expected = vec!["-128", "0", "127"];
    assert_eq!(
        i16_expected,
        get_cast_values::<Int16Type>(&i8_array, &DataType::Int16)
    );

    let i8_expected = vec!["-128", "0", "127"];
    assert_eq!(
        i8_expected,
        get_cast_values::<Int8Type>(&i8_array, &DataType::Int8)
    );

    let u64_expected = vec!["null", "0", "127"];
    assert_eq!(
        u64_expected,
        get_cast_values::<UInt64Type>(&i8_array, &DataType::UInt64)
    );

    let u32_expected = vec!["null", "0", "127"];
    assert_eq!(
        u32_expected,
        get_cast_values::<UInt32Type>(&i8_array, &DataType::UInt32)
    );

    let u16_expected = vec!["null", "0", "127"];
    assert_eq!(
        u16_expected,
        get_cast_values::<UInt16Type>(&i8_array, &DataType::UInt16)
    );

    let u8_expected = vec!["null", "0", "127"];
    assert_eq!(
        u8_expected,
        get_cast_values::<UInt8Type>(&i8_array, &DataType::UInt8)
    );
}

/// Convert `array` into a vector of strings by casting to data type dt
fn get_cast_values<T>(array: &dyn Array, dt: &DataType) -> Vec<String>
where
    T: ArrowPrimitiveType,
{
    let c = cast(array, dt).unwrap();
    let a = c.as_primitive::<T>();
    let mut v: Vec<String> = vec![];
    for i in 0..array.len() {
        if a.is_null(i) {
            v.push("null".to_string())
        } else {
            v.push(format!("{:?}", a.value(i)));
        }
    }
    v
}
