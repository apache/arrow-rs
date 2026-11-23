// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{NullBuffer, i256};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::DataType::*;
use arrow_schema::{DataType, TimeUnit};
use chrono::{FixedOffset, NaiveDate, NaiveTime};
use half::f16;

/// Number of `unit`s per second; mirrors the private `time_unit_multiple` in `arrow_cast`.
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

macro_rules! assert_cast {
    ($array:expr, $datatype:expr, $output_array_type: ty, $expected:expr) => {{
        assert!(can_cast_types($array.data_type(), &$datatype));
        let out = cast(&$array, &$datatype).unwrap();
        let actual = out
            .as_any()
            .downcast_ref::<$output_array_type>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, $expected);
    }};
    ($array:expr, $datatype:expr, $output_array_type: ty, $options:expr, $expected:expr) => {{
        assert!(can_cast_types($array.data_type(), &$datatype));
        let out = cast_with_options(&$array, &$datatype, &$options).unwrap();
        let actual = out
            .as_any()
            .downcast_ref::<$output_array_type>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, $expected);
    }};
}

fn assert_temporal_overflow_is_safe(array: &dyn Array, to_type: &DataType) {
    let result = cast(array, to_type).unwrap();
    assert_eq!(result.null_count(), array.len());

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    assert!(cast_with_options(array, to_type, &options).is_err());
}

#[test]
fn test_cast_date32_to_date64() {
    let a = Date32Array::from(vec![10000, 17890]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date64).unwrap();
    let c = b.as_primitive::<Date64Type>();
    assert_eq!(864000000000, c.value(0));
    assert_eq!(1545696000000, c.value(1));
}

#[test]
fn test_cast_date64_to_date32() {
    let a = Date64Array::from(vec![
        Some(864000000005),
        Some(1545696000001),
        None,
        Some(-1),
        Some(-86400000),
        Some(-86400001),
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date32).unwrap();
    let c = b.as_primitive::<Date32Type>();
    assert_eq!(10000, c.value(0));
    assert_eq!(17890, c.value(1));
    assert!(c.is_null(2));
    assert_eq!(-1, c.value(3));
    assert_eq!(-1, c.value(4));
    assert_eq!(-2, c.value(5));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let d = cast_with_options(&array, &DataType::Date32, &options).unwrap();
    assert_eq!(c, d.as_primitive::<Date32Type>());
}

#[test]
fn test_cast_date64_to_date32_overflow() {
    let a = Date64Array::from(vec![i64::MAX]);
    let array = Arc::new(a) as ArrayRef;

    let b = cast(&array, &DataType::Date32).unwrap();
    let c = b.as_primitive::<Date32Type>();
    assert!(c.is_null(0));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let err = cast_with_options(&array, &DataType::Date32, &options).unwrap_err();
    assert!(
        err.to_string().contains("Cannot cast Date64 value"),
        "{err}"
    );
}

#[test]
fn test_cast_string_to_date32_with_surrounding_whitespace() {
    let array = StringArray::from(vec![" 2026-09-29 ", "2026-09-29"]);

    let result = cast(&array, &DataType::Date32).unwrap();
    let result = result.as_primitive::<Date32Type>();

    assert!(!result.is_null(0));
    assert_eq!(result.value(0), result.value(1));
}

#[test]
fn test_cast_string_to_date32() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("2018-12-25"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("2018-12-25"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("2018-12-25"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Date32;
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert_eq!(17890, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string 'Not a valid date' to value of Date32 type"
        );
    }
}

#[test]
fn test_cast_string_with_large_date_to_date32() {
    let array = Arc::new(StringArray::from(vec![
        Some("+10999-12-31"),
        Some("-0010-02-28"),
        Some("0010-02-28"),
        Some("0000-01-01"),
        Some("-0000-01-01"),
        Some("-0001-01-01"),
    ])) as ArrayRef;
    let to_type = DataType::Date32;
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let b = cast_with_options(&array, &to_type, &options).unwrap();
    let c = b.as_primitive::<Date32Type>();
    assert_eq!(3298139, c.value(0)); // 10999-12-31
    assert_eq!(-723122, c.value(1)); // -0010-02-28
    assert_eq!(-715817, c.value(2)); // 0010-02-28
    assert_eq!(c.value(3), c.value(4)); // Expect 0000-01-01 and -0000-01-01 to be parsed the same
    assert_eq!(-719528, c.value(3)); // 0000-01-01
    assert_eq!(-719528, c.value(4)); // -0000-01-01
    assert_eq!(-719893, c.value(5)); // -0001-01-01
}

#[test]
fn test_cast_invalid_string_with_large_date_to_date32() {
    // Large dates need to be prefixed with a + or - sign, otherwise they are not parsed correctly
    let array = Arc::new(StringArray::from(vec![Some("10999-12-31")])) as ArrayRef;
    let to_type = DataType::Date32;
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let err = cast_with_options(&array, &to_type, &options).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Cast error: Cannot cast string '10999-12-31' to value of Date32 type"
    );
}

#[test]
fn test_cast_string_format_yyyymmdd_to_date32() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("2020-12-25"),
        Some("20201117"),
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("2020-12-25"),
        Some("20201117"),
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("2020-12-25"),
        Some("20201117"),
    ])) as ArrayRef;

    for array in &[a0, a1, a2] {
        let to_type = DataType::Date32;
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let result = cast_with_options(&array, &to_type, &options).unwrap();
        let c = result.as_primitive::<Date32Type>();
        assert_eq!(
            chrono::NaiveDate::from_ymd_opt(2020, 12, 25),
            c.value_as_date(0)
        );
        assert_eq!(
            chrono::NaiveDate::from_ymd_opt(2020, 11, 17),
            c.value_as_date(1)
        );
    }
}

#[test]
fn test_cast_string_to_time32second() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Time32(TimeUnit::Second);
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Time32SecondType>();
        assert_eq!(29315, c.value(0));
        assert_eq!(29340, c.value(1));
        assert!(c.is_null(2));
        assert!(c.is_null(3));
        assert!(c.is_null(4));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(s) type"
        );
    }
}

#[test]
fn test_cast_string_to_time32millisecond() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("08:08:60.091323414"), // leap second
        Some("08:08:61.091323414"), // not valid
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Time32(TimeUnit::Millisecond);
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert_eq!(29315091, c.value(0));
        assert_eq!(29340091, c.value(1));
        assert!(c.is_null(2));
        assert!(c.is_null(3));
        assert!(c.is_null(4));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(ms) type"
        );
    }
}

#[test]
fn test_cast_string_to_time64microsecond() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Time64(TimeUnit::Microsecond);
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(29315091323, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string 'Not a valid time' to value of Time64(µs) type"
        );
    }
}

#[test]
fn test_cast_string_to_time64nanosecond() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("08:08:35.091323414"),
        Some("Not a valid time"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Time64(TimeUnit::Nanosecond);
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(29315091323414, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string 'Not a valid time' to value of Time64(ns) type"
        );
    }
}

#[test]
fn test_cast_string_to_date64() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("2020-09-08T12:00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("2020-09-08T12:00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("2020-09-08T12:00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        let to_type = DataType::Date64;
        let b = cast(array, &to_type).unwrap();
        let c = b.as_primitive::<Date64Type>();
        assert_eq!(1599566400000, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));

        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string 'Not a valid date' to value of Date64 type"
        );
    }
}

#[test]
fn test_cast_date32_to_int32() {
    let array = Date32Array::from(vec![10000, 17890]);
    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(10000, c.value(0));
    assert_eq!(17890, c.value(1));
}

#[test]
fn test_cast_int32_to_date32() {
    let array = Int32Array::from(vec![10000, 17890]);
    let b = cast(&array, &DataType::Date32).unwrap();
    let c = b.as_primitive::<Date32Type>();
    assert_eq!(10000, c.value(0));
    assert_eq!(17890, c.value(1));
}

#[test]
fn test_cast_date32_to_string() {
    let array = Date32Array::from(vec![Some(0), Some(10000), Some(13036), Some(17890), None]);
    let expected = vec![
        Some("1970-01-01"),
        Some("1997-05-19"),
        Some("2005-09-10"),
        Some("2018-12-25"),
        None,
    ];

    assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
    assert_cast!(array, DataType::Utf8, StringArray, expected);
    assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
}

#[test]
fn test_cast_date64_to_string() {
    let array = Date64Array::from(vec![
        Some(0),
        Some(10000 * 86400000),
        Some(13036 * 86400000),
        Some(17890 * 86400000),
        None,
    ]);
    let expected = vec![
        Some("1970-01-01T00:00:00"),
        Some("1997-05-19T00:00:00"),
        Some("2005-09-10T00:00:00"),
        Some("2018-12-25T00:00:00"),
        None,
    ];

    assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
    assert_cast!(array, DataType::Utf8, StringArray, expected);
    assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
}

#[test]
fn test_cast_utf8_to_date32() {
    use chrono::NaiveDate;
    let from_ymd = chrono::NaiveDate::from_ymd_opt;
    let since = chrono::NaiveDate::signed_duration_since;

    let a = StringArray::from(vec![
        "2000-01-01",          // valid date with leading 0s
        "2000-01-01T12:00:00", // valid datetime, will throw away the time part
        "2000-2-2",            // valid date without leading 0s
        "2000-00-00",          // invalid month and day
        "2000",                // just a year is invalid
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date32).unwrap();
    let c = b.as_primitive::<Date32Type>();

    // test valid inputs
    let date_value = since(
        NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        from_ymd(1970, 1, 1).unwrap(),
    )
    .num_days() as i32;
    assert!(c.is_valid(0)); // "2000-01-01"
    assert_eq!(date_value, c.value(0));

    assert!(c.is_valid(1)); // "2000-01-01T12:00:00"
    assert_eq!(date_value, c.value(1));

    let date_value = since(
        NaiveDate::from_ymd_opt(2000, 2, 2).unwrap(),
        from_ymd(1970, 1, 1).unwrap(),
    )
    .num_days() as i32;
    assert!(c.is_valid(2)); // "2000-2-2"
    assert_eq!(date_value, c.value(2));

    // test invalid inputs
    assert!(!c.is_valid(3)); // "2000-00-00"
    assert!(!c.is_valid(4)); // "2000"
}

#[test]
fn test_cast_utf8_to_date64() {
    let a = StringArray::from(vec![
        "2000-01-01T12:00:00", // date + time valid
        "2020-12-15T12:34:56", // date + time valid
        "2020-2-2T12:34:56",   // valid date time without leading 0s
        "2000-00-00T12:00:00", // invalid month and day
        "2000-01-01 12:00:00", // missing the 'T'
        "2000-01-01",          // just a date is invalid
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date64).unwrap();
    let c = b.as_primitive::<Date64Type>();

    // test valid inputs
    assert!(c.is_valid(0)); // "2000-01-01T12:00:00"
    assert_eq!(946728000000, c.value(0));
    assert!(c.is_valid(1)); // "2020-12-15T12:34:56"
    assert_eq!(1608035696000, c.value(1));
    assert!(!c.is_valid(2)); // "2020-2-2T12:34:56"

    assert!(!c.is_valid(3)); // "2000-00-00T12:00:00"
    assert!(c.is_valid(4)); // "2000-01-01 12:00:00"
    assert_eq!(946728000000, c.value(4));
    assert!(c.is_valid(5)); // "2000-01-01"
    assert_eq!(946684800000, c.value(5));
}

#[test]
fn test_cast_time32_second_to_int64() {
    let array = Time32SecondArray::from(vec![1000, 2000, 3000]);
    let array = Arc::new(array) as Arc<dyn Array>;
    let to_type = DataType::Int64;
    let cast_options = CastOptions::default();

    assert!(can_cast_types(array.data_type(), &to_type));

    let result = cast_with_options(&array, &to_type, &cast_options);
    assert!(
        result.is_ok(),
        "Failed to cast Time32(Second) to Int64: {:?}",
        result.err()
    );

    let cast_array = result.unwrap();
    let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(cast_array.value(0), 1000);
    assert_eq!(cast_array.value(1), 2000);
    assert_eq!(cast_array.value(2), 3000);
}

#[test]
fn test_cast_time32_millisecond_to_int64() {
    let array = Time32MillisecondArray::from(vec![1000, 2000, 3000]);
    let array = Arc::new(array) as Arc<dyn Array>;
    let to_type = DataType::Int64;
    let cast_options = CastOptions::default();

    assert!(can_cast_types(array.data_type(), &to_type));

    let result = cast_with_options(&array, &to_type, &cast_options);
    assert!(
        result.is_ok(),
        "Failed to cast Time32(Millisecond) to Int64: {:?}",
        result.err()
    );

    let cast_array = result.unwrap();
    let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(cast_array.value(0), 1000);
    assert_eq!(cast_array.value(1), 2000);
    assert_eq!(cast_array.value(2), 3000);
}

#[test]
fn test_cast_time32_millisecond_to_time64_nanosecond() {
    let array =
        Time32MillisecondArray::from(vec![Some(1_000), Some(2_000), None, Some(43_200_000)]);
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(c.value(0), 1_000_000_000);
    assert_eq!(c.value(1), 2_000_000_000);
    assert!(c.is_null(2));
    assert_eq!(c.value(3), 43_200_000_000_000);
}

#[test]
fn test_cast_time32_millisecond_to_time64_microsecond() {
    let array =
        Time32MillisecondArray::from(vec![Some(1_000), Some(2_000), None, Some(43_200_000)]);
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(c.value(0), 1_000_000);
    assert_eq!(c.value(1), 2_000_000);
    assert!(c.is_null(2));
    assert_eq!(c.value(3), 43_200_000_000);
}

#[test]
fn test_cast_time32_second_to_time64_nanosecond() {
    let array = Time32SecondArray::from(vec![Some(1), Some(60), None, Some(43_200)]);
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(c.value(0), 1_000_000_000);
    assert_eq!(c.value(1), 60_000_000_000);
    assert!(c.is_null(2));
    assert_eq!(c.value(3), 43_200_000_000_000);
}

#[test]
fn test_cast_time32_second_to_time64_microsecond() {
    let array = Time32SecondArray::from(vec![Some(1), Some(60), None, Some(43_200)]);
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(c.value(0), 1_000_000);
    assert_eq!(c.value(1), 60_000_000);
    assert!(c.is_null(2));
    assert_eq!(c.value(3), 43_200_000_000);
}

#[test]
fn test_cast_time32_second_to_time32_millisecond_overflow() {
    let array = Time32SecondArray::from(vec![i32::MAX]);

    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    let c = b.as_primitive::<Time32MillisecondType>();
    assert!(c.is_null(0));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let err =
        cast_with_options(&array, &DataType::Time32(TimeUnit::Millisecond), &options).unwrap_err();
    assert!(err.to_string().contains("Overflow"), "{err}");
}

#[test]
fn test_cast_time64_overflow() {
    let microseconds = Time64MicrosecondArray::from(vec![i64::MIN, i64::MAX]);
    for to_type in [
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Nanosecond),
    ] {
        assert_temporal_overflow_is_safe(&microseconds, &to_type);
    }

    let nanoseconds = Time64NanosecondArray::from(vec![i64::MIN, i64::MAX]);
    for to_type in [
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
    ] {
        assert_temporal_overflow_is_safe(&nanoseconds, &to_type);
    }
}

#[test]
fn test_cast_time64_to_time32_boundaries() {
    fn check<FROM, TO>(divisor: i64)
    where
        FROM: ArrowPrimitiveType<Native = i64>,
        TO: ArrowPrimitiveType<Native = i32>,
    {
        // Division truncates towards zero before the range check. Values
        // slightly outside the scaled i32 bounds can still be representable.
        let min = <i64 as From<i32>>::from(i32::MIN) * divisor - (divisor - 1);
        let max = <i64 as From<i32>>::from(i32::MAX) * divisor + (divisor - 1);
        let array = PrimitiveArray::<FROM>::new(
            vec![i64::MAX, min - 1, min, -divisor + 1, i64::MAX, max, max + 1].into(),
            Some(vec![true, true, true, true, false, true, true].into()),
        )
        .slice(1, 6);
        let expected = PrimitiveArray::<TO>::from_iter([
            None,
            Some(i32::MIN),
            Some(0),
            None,
            Some(i32::MAX),
            None,
        ]);
        let result = cast(&array, &TO::DATA_TYPE).unwrap();
        assert_eq!(result.as_primitive::<TO>(), &expected);

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        assert!(cast_with_options(&array, &TO::DATA_TYPE, &options).is_err());
        // The invalid physical value under the null must not cause an error.
        let result = cast_with_options(&array.slice(1, 4), &TO::DATA_TYPE, &options).unwrap();
        assert_eq!(result.as_primitive::<TO>(), &expected.slice(1, 4));
    }
    check::<Time64MicrosecondType, Time32SecondType>(MICROSECONDS);
    check::<Time64MicrosecondType, Time32MillisecondType>(MICROSECONDS / MILLISECONDS);
    check::<Time64NanosecondType, Time32SecondType>(NANOSECONDS);
    check::<Time64NanosecondType, Time32MillisecondType>(NANOSECONDS / MILLISECONDS);
}

#[test]
fn test_cast_string_to_time32_second_to_int64() {
    // Mimic: select arrow_cast('03:12:44'::time, 'Time32(Second)')::bigint;
    // raised in https://github.com/apache/datafusion/issues/19036
    let array = StringArray::from(vec!["03:12:44"]);
    let array = Arc::new(array) as Arc<dyn Array>;
    let cast_options = CastOptions::default();

    // 1. Cast String to Time32(Second)
    let time32_type = DataType::Time32(TimeUnit::Second);
    let time32_array = cast_with_options(&array, &time32_type, &cast_options).unwrap();

    // 2. Cast Time32(Second) to Int64
    let int64_type = DataType::Int64;
    assert!(can_cast_types(time32_array.data_type(), &int64_type));

    let result = cast_with_options(&time32_array, &int64_type, &cast_options);

    assert!(
        result.is_ok(),
        "Failed to cast Time32(Second) to Int64: {:?}",
        result.err()
    );

    let cast_array = result.unwrap();
    let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

    // 03:12:44 = 3*3600 + 12*60 + 44 = 10800 + 720 + 44 = 11564
    assert_eq!(cast_array.value(0), 11564);
}

#[test]
fn test_cast_integer_to_timestamp() {
    let array = Int64Array::from(vec![Some(2), Some(10), None]);
    let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    let array = Int8Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = Int16Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = Int32Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = UInt8Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = UInt16Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = UInt32Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = UInt64Array::from(vec![Some(2), Some(10), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);
}

#[test]
fn test_cast_timestamp_to_integer() {
    let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
        .with_timezone("UTC".to_string());
    let expected = cast(&array, &DataType::Int64).unwrap();

    let actual = cast(&cast(&array, &DataType::Int8).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::Int16).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::Int32).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::UInt8).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::UInt16).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::UInt32).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::UInt64).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_floating_to_timestamp() {
    let array = Int64Array::from(vec![Some(2), Some(10), None]);
    let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    let array = Float16Array::from(vec![
        Some(f16::from_f32(2.0)),
        Some(f16::from_f32(10.6)),
        None,
    ]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = Float32Array::from(vec![Some(2.0), Some(10.6), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = Float64Array::from(vec![Some(2.1), Some(10.2), None]);
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_timestamp_to_floating() {
    let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
        .with_timezone("UTC".to_string());
    let expected = cast(&array, &DataType::Int64).unwrap();

    let actual = cast(&cast(&array, &DataType::Float16).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::Float32).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(&cast(&array, &DataType::Float64).unwrap(), &DataType::Int64).unwrap();
    assert_eq!(&actual, &expected);
}

#[test]
fn test_cast_decimal_to_timestamp() {
    let array = Int64Array::from(vec![Some(2), Some(10), None]);
    let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    let array = Decimal128Array::from(vec![Some(200), Some(1000), None])
        .with_precision_and_scale(4, 2)
        .unwrap();
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);

    let array = Decimal256Array::from(vec![
        Some(i256::from_i128(2000)),
        Some(i256::from_i128(10000)),
        None,
    ])
    .with_precision_and_scale(5, 3)
    .unwrap();
    let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

    assert_eq!(&actual, &expected);
}

#[test]
fn test_cast_timestamp_to_decimal() {
    let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
        .with_timezone("UTC".to_string());
    let expected = cast(&array, &DataType::Int64).unwrap();

    let actual = cast(
        &cast(&array, &DataType::Decimal128(5, 2)).unwrap(),
        &DataType::Int64,
    )
    .unwrap();
    assert_eq!(&actual, &expected);

    let actual = cast(
        &cast(&array, &DataType::Decimal256(10, 5)).unwrap(),
        &DataType::Int64,
    )
    .unwrap();
    assert_eq!(&actual, &expected);
}

#[test]
fn test_cast_string_to_timestamp() {
    let a0 = Arc::new(StringViewArray::from(vec![
        Some("2020-09-08T12:00:00.123456789+00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a1 = Arc::new(StringArray::from(vec![
        Some("2020-09-08T12:00:00.123456789+00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    let a2 = Arc::new(LargeStringArray::from(vec![
        Some("2020-09-08T12:00:00.123456789+00:00"),
        Some("Not a valid date"),
        None,
    ])) as ArrayRef;
    for array in &[a0, a1, a2] {
        for time_unit in &[
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let to_type = DataType::Timestamp(*time_unit, None);
            let b = cast(array, &to_type).unwrap();

            match time_unit {
                TimeUnit::Second => {
                    let c = b.as_primitive::<TimestampSecondType>();
                    assert_eq!(1599566400, c.value(0));
                    assert!(c.is_null(1));
                    assert!(c.is_null(2));
                }
                TimeUnit::Millisecond => {
                    let c = b
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    assert_eq!(1599566400123, c.value(0));
                    assert!(c.is_null(1));
                    assert!(c.is_null(2));
                }
                TimeUnit::Microsecond => {
                    let c = b
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    assert_eq!(1599566400123456, c.value(0));
                    assert!(c.is_null(1));
                    assert!(c.is_null(2));
                }
                TimeUnit::Nanosecond => {
                    let c = b
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    assert_eq!(1599566400123456789, c.value(0));
                    assert!(c.is_null(1));
                    assert!(c.is_null(2));
                }
            }

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Parser error: Error parsing timestamp from 'Not a valid date': error parsing date"
            );
        }
    }
}

#[test]
fn test_cast_string_to_timestamp_overflow() {
    let array = StringArray::from(vec!["9800-09-08T12:00:00.123456789"]);
    let result = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
    let result = result.as_primitive::<TimestampSecondType>();
    assert_eq!(result.values(), &[247112596800]);
}

#[test]
fn test_cast_string_to_duration() {
    let array = StringArray::from(vec![Some("1"), Some("-2"), None, Some("invalid")]);
    let to_type = DataType::Duration(TimeUnit::Second);
    assert!(can_cast_types(array.data_type(), &to_type));

    let casted = cast(&array, &to_type).unwrap();
    assert_eq!(
        casted.as_primitive::<DurationSecondType>(),
        &DurationSecondArray::from(vec![Some(1), Some(-2), None, None])
    );

    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let err = cast_with_options(&array, &to_type, &options).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Cast error: Cannot cast string 'invalid' to value of Duration(s) type"
    );
}

#[test]
fn test_cast_string_with_units_to_duration() {
    let array = StringArray::from(vec![
        Some("1 hour 10 minutes"),
        Some("90 seconds"),
        Some("0.5 hours"),
        Some("-1 hour -30 minutes"),
        Some("2 days"),
        Some("1 week"),
        Some("45 days 14 hours 2 mins 34 secs"),
        None,
        Some("2 months"),
    ]);
    let to_type = DataType::Duration(TimeUnit::Second);

    let casted = cast(&array, &to_type).unwrap();
    assert_eq!(
        casted.as_primitive::<DurationSecondType>(),
        &DurationSecondArray::from(vec![
            Some(4200),
            Some(90),
            Some(1800),
            Some(-5400),
            Some(172800),
            Some(604800),
            Some(3938554),
            None,
            None
        ])
    );

    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let err = cast_with_options(&array, &to_type, &options).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Compute error: Cannot convert interval containing non-zero months to duration"
    );
}

#[test]
fn test_cast_fractional_calendar_units_to_duration() {
    let array = StringArray::from(vec![
        Some("0.5 months"),
        Some("-0.5 months"),
        Some("1.5 months"),
        Some("0.5 years"),
        Some("0.5 decades"),
        Some("15 days"),
        Some("0.5 days"),
        Some("0.5 weeks"),
    ]);
    let to_type = DataType::Duration(TimeUnit::Second);

    let casted = cast(&array, &to_type).unwrap();
    assert_eq!(
        casted.as_primitive::<DurationSecondType>(),
        &DurationSecondArray::from(vec![
            None,
            None,
            None,
            None,
            None,
            Some(1_296_000),
            Some(43_200),
            Some(302_400)
        ])
    );

    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let err = cast_with_options(&array, &to_type, &options).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Cast error: Cannot cast string '0.5 months' to value of Duration(s) type"
    );
}

#[test]
fn test_cast_large_string_and_view_to_duration() {
    let to_type = DataType::Duration(TimeUnit::Nanosecond);
    for array in [
        Arc::new(LargeStringArray::from(vec![
            Some("10"),
            Some("1 minute"),
            Some("1 day"),
            None,
        ])) as ArrayRef,
        Arc::new(StringViewArray::from(vec![
            Some("10"),
            Some("1 minute"),
            Some("1 day"),
            None,
        ])) as ArrayRef,
    ] {
        let casted = cast(&array, &to_type).unwrap();
        assert_eq!(
            casted.as_primitive::<DurationNanosecondType>(),
            &DurationNanosecondArray::from(vec![
                Some(10),
                Some(60_000_000_000),
                Some(86_400_000_000_000),
                None
            ])
        );
    }
}

/// Compares arithmetic casts with Chrono for representable dates, including
/// negative and fractional boundaries, nulls, and nonzero-offset slices.
fn check_timestamp_casts<T: ArrowTimestampType>() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    let units = time_unit_multiple(&T::UNIT);
    let around = |x: i64| [x - 1, x, x + 1, -x - 1, -x, -x + 1];
    let mut values = vec![i64::MIN, i64::MAX];
    for base in [0, 60, 3_600, 86_400] {
        for seconds in around(base) {
            for fraction in [-1, 0, 1, units - 1] {
                values.push(seconds * units + fraction);
            }
        }
    }
    for boundary in [1_000, 1_000_000] {
        values.extend(around(boundary));
    }
    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..256 {
        values.push(rng.random());
        let seconds = rng.random_range(-2_000_000_000..2_000_000_000);
        values.push(seconds * units + rng.random_range(0..units));
    }
    values.retain(|&v| as_datetime::<T>(v).is_some());
    let no_nulls = PrimitiveArray::<T>::new(values.clone().into(), None);
    let mixed = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    for array in [no_nulls, mixed.slice(1, mixed.len() - 2)] {
        let dates: Vec<_> = array
            .iter()
            .map(|v| v.map(|v| as_datetime::<T>(v).unwrap()))
            .collect();
        let expected: [ArrayRef; 5] = [
            Arc::new(Date32Array::from_iter(
                dates
                    .iter()
                    .map(|d| d.map(|d| Date32Type::from_naive_date(d.date()))),
            )),
            Arc::new(Time32SecondArray::from_iter(
                dates.iter().map(|d| d.map(|d| time_to_time32s(d.time()))),
            )),
            Arc::new(Time32MillisecondArray::from_iter(
                dates.iter().map(|d| d.map(|d| time_to_time32ms(d.time()))),
            )),
            Arc::new(Time64MicrosecondArray::from_iter(
                dates.iter().map(|d| d.map(|d| time_to_time64us(d.time()))),
            )),
            Arc::new(Time64NanosecondArray::from_iter(
                dates.iter().map(|d| d.map(|d| time_to_time64ns(d.time()))),
            )),
        ];
        for expected in expected {
            let actual = cast(&array, expected.data_type()).unwrap();
            assert_eq!(actual.as_ref(), expected.as_ref(), "{}", T::DATA_TYPE);
        }
    }
}

#[test]
fn test_cast_timestamp_without_timezone_matches_chrono() {
    check_timestamp_casts::<TimestampSecondType>();
    check_timestamp_casts::<TimestampMillisecondType>();
    check_timestamp_casts::<TimestampMicrosecondType>();
    check_timestamp_casts::<TimestampNanosecondType>();
}

#[test]
fn test_cast_timestamp_to_time_extremes() {
    fn check<T: ArrowTimestampType>(min: (u32, u32, u32, u32), max: (u32, u32, u32, u32)) {
        let time = |(h, m, s, ns)| NaiveTime::from_hms_nano_opt(h, m, s, ns).unwrap();
        let times = [Some(time(min)), None, Some(time(max))];
        let array = PrimitiveArray::<T>::from_iter([Some(i64::MIN), None, Some(i64::MAX)]);
        let expected: [ArrayRef; 4] = [
            Arc::new(Time32SecondArray::from_iter(
                times.map(|v| v.map(time_to_time32s)),
            )),
            Arc::new(Time32MillisecondArray::from_iter(
                times.map(|v| v.map(time_to_time32ms)),
            )),
            Arc::new(Time64MicrosecondArray::from_iter(
                times.map(|v| v.map(time_to_time64us)),
            )),
            Arc::new(Time64NanosecondArray::from_iter(
                times.map(|v| v.map(time_to_time64ns)),
            )),
        ];
        for expected in expected {
            assert_eq!(
                cast(&array, expected.data_type()).unwrap().as_ref(),
                expected.as_ref()
            );
        }
    }
    check::<TimestampSecondType>((8, 29, 52, 0), (15, 30, 7, 0));
    check::<TimestampMillisecondType>((16, 47, 4, 192_000_000), (7, 12, 55, 807_000_000));
    check::<TimestampMicrosecondType>((19, 59, 5, 224_192_000), (4, 0, 54, 775_807_000));
    // Nanosecond extremes are representable in Chrono and covered above.
}

#[test]
fn test_cast_timestamp_to_date32_range() {
    fn check<T: ArrowTimestampType>() {
        let day = SECONDS_IN_DAY * time_unit_multiple(&T::UNIT);
        let first = i32::MIN as i64 * day;
        let last = (i32::MAX as i64 + 1) * day - 1;
        let array = PrimitiveArray::<T>::new(
            vec![first, i64::MAX, last].into(),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let expected = Date32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)]);
        for safe in [true, false] {
            let options = CastOptions {
                safe,
                ..Default::default()
            };
            assert_eq!(
                cast_with_options(&array, &Date32, &options)
                    .unwrap()
                    .as_ref(),
                &expected
            );
        }
        // Day counts that do not fit in i32 become null when safe, otherwise an error.
        let array =
            PrimitiveArray::<T>::from_iter_values([first - 1, last, last + 1, i64::MIN, i64::MAX]);
        let expected = Date32Array::from(vec![None, Some(i32::MAX), None, None, None]);
        assert_eq!(cast(&array, &Date32).unwrap().as_ref(), &expected);
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        assert!(cast_with_options(&array, &Date32, &options).is_err());
    }
    check::<TimestampSecondType>();
    check::<TimestampMillisecondType>();
    // The entire microsecond domain fits in Date32, including dates outside Chrono's range.
    let array = TimestampMicrosecondArray::from(vec![Some(i64::MIN), None, Some(i64::MAX)]);
    assert_eq!(
        cast(&array, &Date32).unwrap().as_ref(),
        &Date32Array::from(vec![Some(-106_751_992), None, Some(106_751_991)]),
    );
}

#[test]
fn test_cast_timestamp_date_time_timezone_validation() {
    let invalid = TimestampSecondArray::from(vec![0]).with_timezone("invalid timezone");
    let extreme = TimestampSecondArray::from(vec![0, i64::MAX]).with_timezone("+00:00");
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    for target in [
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
    ] {
        // A timezone that fails to parse is an error even when the cast is safe.
        assert!(cast(&invalid, &target).is_err());
        // A timestamp outside Chrono's date range is null when safe, otherwise an error.
        let actual = cast(&extreme, &target).unwrap();
        assert!(actual.is_valid(0));
        assert!(actual.is_null(1));
        assert!(cast_with_options(&extreme, &target, &options).is_err());
    }
}

#[test]
fn test_cast_timestamp_to_date32() {
    let array =
        TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None])
            .with_timezone("+00:00".to_string());
    let b = cast(&array, &DataType::Date32).unwrap();
    let c = b.as_primitive::<Date32Type>();
    assert_eq!(10000, c.value(0));
    assert_eq!(17890, c.value(1));
    assert!(c.is_null(2));
}
#[test]
fn test_cast_timestamp_to_date32_zone() {
    let strings = StringArray::from_iter([
        Some("1970-01-01T00:00:01"),
        Some("1970-01-01T23:59:59"),
        None,
        Some("2020-03-01T02:00:23+00:00"),
    ]);
    let dt = DataType::Timestamp(TimeUnit::Millisecond, Some("-07:00".into()));
    let timestamps = cast(&strings, &dt).unwrap();
    let dates = cast(timestamps.as_ref(), &DataType::Date32).unwrap();

    let c = dates.as_primitive::<Date32Type>();
    let expected = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    assert_eq!(c.value_as_date(0).unwrap(), expected);
    assert_eq!(c.value_as_date(1).unwrap(), expected);
    assert!(c.is_null(2));
    let expected = NaiveDate::from_ymd_opt(2020, 2, 29).unwrap();
    assert_eq!(c.value_as_date(3).unwrap(), expected);
}
#[test]
fn test_cast_timestamp_to_date64() {
    let array =
        TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None]);
    let b = cast(&array, &DataType::Date64).unwrap();
    let c = b.as_primitive::<Date64Type>();
    assert_eq!(864000000000, c.value(0));
    assert_eq!(1545696000000, c.value(1));
    assert!(c.is_null(2));

    let array = TimestampSecondArray::from(vec![Some(864000000005), Some(1545696000001)]);
    let b = cast(&array, &DataType::Date64).unwrap();
    let c = b.as_primitive::<Date64Type>();
    assert_eq!(864000000000000, c.value(0));
    assert_eq!(1545696000000000, c.value(1));

    // test overflow, safe cast
    let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
    let b = cast(&array, &DataType::Date64).unwrap();
    assert!(b.is_null(0));
    // test overflow, unsafe cast
    let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let b = cast_with_options(&array, &DataType::Date64, &options);
    assert!(b.is_err());
}

#[test]
fn test_cast_timestamp_to_date64_whole_days() {
    // 2024-05-01T10:30:00, 1970-01-01T00:00:00, and one millisecond before the epoch
    let millis: [i64; 3] = [1_714_559_400_000, 0, -1];
    let expected = Date64Array::from(vec![1_714_521_600_000, 0, -MILLISECONDS_IN_DAY]);
    let arrays: [ArrayRef; 4] = [
        Arc::new(TimestampSecondArray::from(
            millis.map(|x| x.div_euclid(1_000)).to_vec(),
        )),
        Arc::new(TimestampMillisecondArray::from(millis.to_vec())),
        Arc::new(TimestampMicrosecondArray::from(
            millis.map(|x| x * 1_000).to_vec(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            millis.map(|x| x * 1_000_000).to_vec(),
        )),
    ];
    for array in arrays {
        let actual = cast(&array, &DataType::Date64).unwrap();
        let actual = actual.as_primitive::<Date64Type>();
        // The second timestamp array rounds -1ms down to -1s, which is still the
        // day before the epoch.
        assert_eq!(actual, &expected, "{}", array.data_type());
    }

    // With a timezone the date is the local date, as for Date32.
    // 1970-01-01T00:00:01Z and 1970-01-01T20:00:00Z are 1969-12-31 and 1970-01-01 in -05:00.
    let array = TimestampSecondArray::from(vec![1, 72_000]).with_timezone("-05:00");
    let date64 = cast(&array, &DataType::Date64).unwrap();
    assert_eq!(
        date64.as_primitive::<Date64Type>(),
        &Date64Array::from(vec![-MILLISECONDS_IN_DAY, 0])
    );
    let date32 = cast(&array, &DataType::Date32).unwrap();
    assert_eq!(
        cast(&date32, &DataType::Date64).unwrap().as_ref(),
        date64.as_ref()
    );

    // Flooring i64::MIN milliseconds to a whole day overflows.
    let array = TimestampMillisecondArray::from(vec![i64::MIN]);
    assert!(cast(&array, &DataType::Date64).unwrap().is_null(0));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    assert!(cast_with_options(&array, &DataType::Date64, &options).is_err());
}

#[test]
fn test_cast_timestamp_to_time64() {
    // test timestamp secs
    let array = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
        .with_timezone("+01:00".to_string());
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(3605000000, c.value(0));
    assert_eq!(3601000000, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(3605000000000, c.value(0));
    assert_eq!(3601000000000, c.value(1));
    assert!(c.is_null(2));

    // test timestamp milliseconds
    let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(3605000000, c.value(0));
    assert_eq!(3601000000, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(3605000000000, c.value(0));
    assert_eq!(3601000000000, c.value(1));
    assert!(c.is_null(2));

    // test timestamp microseconds
    let a = TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(3605000000, c.value(0));
    assert_eq!(3601000000, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(3605000000000, c.value(0));
    assert_eq!(3601000000000, c.value(1));
    assert!(c.is_null(2));

    // Test fractional timestamps on both sides of the epoch.
    let a = TimestampNanosecondArray::from(vec![Some(86_405_123_456_789), Some(-1), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    let c = b.as_primitive::<Time64MicrosecondType>();
    assert_eq!(3_605_123_456, c.value(0));
    assert_eq!(3_599_999_999, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<Time64NanosecondType>();
    assert_eq!(3_605_123_456_789, c.value(0));
    assert_eq!(3_599_999_999_999, c.value(1));
    assert!(c.is_null(2));

    // test overflow, safe cast
    let a = TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
    assert!(b.is_null(0));
    let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
    assert!(b.is_null(0));
    // test overflow, unsafe cast
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let b = cast_with_options(&array, &DataType::Time64(TimeUnit::Microsecond), &options);
    assert!(b.is_err());
    let b = cast_with_options(&array, &DataType::Time64(TimeUnit::Nanosecond), &options);
    assert!(b.is_err());
    // Time64 only supports microseconds and nanoseconds
    let b = cast(&array, &DataType::Time64(TimeUnit::Millisecond));
    assert!(b.is_err());
}

#[test]
fn test_cast_timestamp_to_time32() {
    // test timestamp secs
    let a = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
    let c = b.as_primitive::<Time32SecondType>();
    assert_eq!(3605, c.value(0));
    assert_eq!(3601, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    let c = b.as_primitive::<Time32MillisecondType>();
    assert_eq!(3605000, c.value(0));
    assert_eq!(3601000, c.value(1));
    assert!(c.is_null(2));

    // test timestamp milliseconds
    let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
    let c = b.as_primitive::<Time32SecondType>();
    assert_eq!(3605, c.value(0));
    assert_eq!(3601, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    let c = b.as_primitive::<Time32MillisecondType>();
    assert_eq!(3605000, c.value(0));
    assert_eq!(3601000, c.value(1));
    assert!(c.is_null(2));

    // test timestamp microseconds
    let a = TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
    let c = b.as_primitive::<Time32SecondType>();
    assert_eq!(3605, c.value(0));
    assert_eq!(3601, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    let c = b.as_primitive::<Time32MillisecondType>();
    assert_eq!(3605000, c.value(0));
    assert_eq!(3601000, c.value(1));
    assert!(c.is_null(2));

    // Test fractional timestamps on both sides of the epoch.
    let a = TimestampNanosecondArray::from(vec![Some(86_405_123_456_789), Some(-1), None])
        .with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
    let c = b.as_primitive::<Time32SecondType>();
    assert_eq!(3605, c.value(0));
    assert_eq!(3599, c.value(1));
    assert!(c.is_null(2));
    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    let c = b.as_primitive::<Time32MillisecondType>();
    assert_eq!(3_605_123, c.value(0));
    assert_eq!(3_599_999, c.value(1));
    assert!(c.is_null(2));

    // test overflow, safe cast
    let a = TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
    assert!(b.is_null(0));
    let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
    assert!(b.is_null(0));
    // test overflow, unsafe cast
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let b = cast_with_options(&array, &DataType::Time32(TimeUnit::Second), &options);
    assert!(b.is_err());
    let b = cast_with_options(&array, &DataType::Time32(TimeUnit::Millisecond), &options);
    assert!(b.is_err());
}

// Cast Timestamp(_, None) -> Timestamp(_, Some(timezone))
#[test]
fn test_cast_timestamp_with_timezone_1() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        Some("2000-01-01T00:00:00.123456789"),
        Some("2010-01-01T00:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+0700".into()));
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T00:00:00.123456+07:00", result.value(0));
    assert_eq!("2010-01-01T00:00:00.123456+07:00", result.value(1));
    assert!(result.is_null(2));
}

// Cast Timestamp(_, Some(timezone)) -> Timestamp(_, None)
#[test]
fn test_cast_timestamp_with_timezone_2() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        Some("2000-01-01T07:00:00.123456789"),
        Some("2010-01-01T07:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some("+0700".into()));
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    // Check intermediate representation is correct
    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T07:00:00.123+07:00", result.value(0));
    assert_eq!("2010-01-01T07:00:00.123+07:00", result.value(1));
    assert!(result.is_null(2));

    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T00:00:00.123", result.value(0));
    assert_eq!("2010-01-01T00:00:00.123", result.value(1));
    assert!(result.is_null(2));
}

// Cast Timestamp(_, Some(timezone)) -> Timestamp(_, Some(timezone))
#[test]
fn test_cast_timestamp_with_timezone_3() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        Some("2000-01-01T07:00:00.123456789"),
        Some("2010-01-01T07:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+0700".into()));
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    // Check intermediate representation is correct
    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T07:00:00.123456+07:00", result.value(0));
    assert_eq!("2010-01-01T07:00:00.123456+07:00", result.value(1));
    assert!(result.is_null(2));

    let to_type = DataType::Timestamp(TimeUnit::Second, Some("-08:00".into()));
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("1999-12-31T16:00:00-08:00", result.value(0));
    assert_eq!("2009-12-31T16:00:00-08:00", result.value(1));
    assert!(result.is_null(2));
}

/// The i64 value of a `Timestamp(Second, None)` holding the given wall clock
/// reading (a naive timestamp is stored as if it were UTC).
fn naive_seconds(y: i32, m: u32, d: u32, h: u32, min: u32) -> i64 {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, min, 0)
        .unwrap()
        .and_utc()
        .timestamp()
}

/// The instant, in seconds since the epoch, denoted by the given wall clock
/// reading at a fixed offset of `offset_hours`.
fn instant_seconds(y: i32, m: u32, d: u32, h: u32, min: u32, offset_hours: i32) -> i64 {
    let offset = FixedOffset::east_opt(offset_hours * 3600).unwrap();
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, min, 0)
        .unwrap()
        .and_local_timezone(offset)
        .unwrap()
        .timestamp()
}

// Cast Timestamp(_, None) -> Timestamp(_, Some(IANA timezone)) across DST
// transitions. See `resolve_local_offset`.
#[test]
fn test_cast_timestamp_to_named_timezone_dst() {
    // Unambiguous, EDT (-04:00) is in effect.
    let unambiguous = naive_seconds(2024, 11, 1, 0, 0);
    // Ambiguous: the clocks go back at 2024-11-03T02:00 EDT, so 01:30
    // happens twice, first at -04:00 and then at -05:00.
    let ambiguous = naive_seconds(2024, 11, 3, 1, 30);
    // Nonexistent: the clocks go forward at 2024-03-10T02:00 EST, so 02:30
    // never happens.
    let nonexistent = naive_seconds(2024, 3, 10, 2, 30);
    assert_eq!(
        [unambiguous, ambiguous, nonexistent],
        [1_730_419_200, 1_730_597_400, 1_710_037_800]
    );

    let array = TimestampSecondArray::from(vec![
        Some(unambiguous),
        Some(ambiguous),
        Some(nonexistent),
        None,
    ]);
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampSecondType>();

    assert_eq!(c.value(0), instant_seconds(2024, 11, 1, 0, 0, -4));
    assert_eq!(c.value(0), 1_730_433_600);
    // The later of the two candidates, i.e. EST rather than EDT.
    assert_eq!(c.value(1), instant_seconds(2024, 11, 3, 1, 30, -5));
    assert_eq!(c.value(1), 1_730_615_400);
    // Shifted forward by the one hour gap: 02:30 EST is 03:30 EDT.
    assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 2, 30, -5));
    assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 3, 30, -4));
    assert_eq!(c.value(2), 1_710_055_800);
    assert!(c.is_null(3));
}

/// The rows of the string array used by the string -> timestamp DST tests,
/// and the instants they denote in `America/New_York` according to
/// PostgreSQL 17.11.
///
/// They deliberately straddle both transitions of 2024, so a single offset
/// resolved once for the whole array cannot satisfy all of them.
const DST_STRINGS: [&str; 5] = [
    // EDT (-04:00), before the November transition.
    "2024-11-02 12:00:00",
    // Ambiguous: 01:30 happens twice, the later (EST) instant wins.
    "2024-11-03 01:30:00",
    // EST (-05:00), after the November transition.
    "2024-11-03 12:00:00",
    // Nonexistent: 02:30 is skipped in March and shifts to 03:30 EDT.
    "2024-03-10 02:30:00",
    // EST (-05:00), before the March transition.
    "2024-03-09 12:00:00",
];

const DST_SECONDS: [i64; 5] = [
    1_730_563_200,
    1_730_615_400,
    1_730_653_200,
    1_710_055_800,
    1_710_003_600,
];

// Casting strings to `Timestamp(_, Some(tz))` goes through
// `string_to_datetime`, not through `adjust_timestamp_to_timezone`, so it
// needs its own coverage of the DST rules. See `crate::local_time`.
#[test]
fn test_cast_string_to_named_timezone_dst() {
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let mut values: Vec<Option<&str>> = DST_STRINGS.iter().copied().map(Some).collect();
    values.push(None);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.clone())),
        Arc::new(LargeStringArray::from(values.clone())),
        Arc::new(StringViewArray::from(values)),
    ];

    for array in arrays {
        let from_type = array.data_type().clone();
        let b = cast_with_options(&array, &to_type, &options)
            .unwrap_or_else(|e| panic!("{from_type:?}: {e}"));
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampSecondType>();
        for (i, expected) in DST_SECONDS.iter().enumerate() {
            assert_eq!(c.value(i), *expected, "{from_type:?} row {i}");
        }
        assert!(c.is_null(5), "{from_type:?} null row");
    }
}

// The same values must not silently become null under `safe` casting either.
#[test]
fn test_cast_string_to_named_timezone_dst_safe() {
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
    let options = CastOptions {
        safe: true,
        ..Default::default()
    };

    let array = StringArray::from(vec![
        Some(DST_STRINGS[1]),
        Some(DST_STRINGS[3]),
        Some("not a timestamp"),
        None,
    ]);

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(c.value(0), DST_SECONDS[1]);
    assert_eq!(c.value(1), DST_SECONDS[3]);
    // Genuinely unparsable input still becomes null.
    assert!(c.is_null(2));
    assert!(c.is_null(3));
    assert_eq!(c.null_count(), 2);
}

// Sub-second precision survives the shift applied to a nonexistent reading.
#[test]
fn test_cast_string_to_named_timezone_dst_nanos() {
    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let array = StringArray::from(vec![
        "2024-03-10 02:30:00.123456789",
        "2024-11-03 01:30:00.123456789",
    ]);
    let b = cast_with_options(&array, &to_type, &options).unwrap();
    let c = b.as_primitive::<TimestampNanosecondType>();
    assert_eq!(c.value(0), 1_710_055_800 * 1_000_000_000 + 123_456_789);
    assert_eq!(c.value(1), 1_730_615_400 * 1_000_000_000 + 123_456_789);
}

// A date-only string reads local midnight, which is itself skipped or
// repeated in some zones.
#[test]
fn test_cast_string_to_named_timezone_dst_date_only() {
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    // Local midnight does not exist in Sao Paulo on 2018-11-04.
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/Sao_Paulo".into()));
    let array = StringArray::from(vec!["2018-11-04"]);
    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(
        b.as_primitive::<TimestampSecondType>().value(0),
        1_541_300_400
    );

    // Local midnight happens twice in Havana on 2024-11-03.
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/Havana".into()));
    let array = StringArray::from(vec!["2024-11-03"]);
    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(
        b.as_primitive::<TimestampSecondType>().value(0),
        1_730_610_000
    );
}

// A timezone named by the string itself is resolved the same way, matching
// PostgreSQL.
#[test]
fn test_cast_string_to_timezone_named_in_string_dst() {
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let array = StringArray::from(vec![
        "2024-03-10 02:30:00 America/New_York",
        "2024-11-03 01:30:00 America/New_York",
    ]);
    let b = cast_with_options(&array, &to_type, &options).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(c.value(0), 1_710_055_800);
    assert_eq!(c.value(1), 1_730_615_400);
}

// The same values must not become null when `safe` casting is requested.
#[test]
fn test_cast_timestamp_to_named_timezone_dst_safe() {
    let array = TimestampSecondArray::from(vec![
        Some(naive_seconds(2024, 11, 1, 0, 0)),
        Some(naive_seconds(2024, 11, 3, 1, 30)),
        Some(naive_seconds(2024, 3, 10, 2, 30)),
        None,
    ]);
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
    let options = CastOptions {
        safe: true,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(c.null_count(), 1);
    assert_eq!(c.value(0), 1_730_433_600);
    assert_eq!(c.value(1), 1_730_615_400);
    assert_eq!(c.value(2), 1_710_055_800);
    assert!(c.is_null(3));
}

// Southern hemisphere: the transitions run the other way around.
#[test]
fn test_cast_timestamp_to_named_timezone_dst_southern_hemisphere() {
    // Ambiguous: clocks go back at 2024-04-07T03:00 AEDT (+11:00 -> +10:00).
    let ambiguous = naive_seconds(2024, 4, 7, 2, 30);
    // Nonexistent: clocks go forward at 2024-10-06T02:00 AEST (+10:00 -> +11:00).
    let nonexistent = naive_seconds(2024, 10, 6, 2, 30);

    let array = TimestampSecondArray::from(vec![Some(ambiguous), Some(nonexistent)]);
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("Australia/Sydney".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampSecondType>();

    // The later of the two candidates, i.e. AEST (+10:00).
    assert_eq!(c.value(0), instant_seconds(2024, 4, 7, 2, 30, 10));
    assert_eq!(c.value(0), 1_712_421_000);
    // Shifted forward by the one hour gap: 02:30 AEST is 03:30 AEDT.
    assert_eq!(c.value(1), instant_seconds(2024, 10, 6, 2, 30, 10));
    assert_eq!(c.value(1), instant_seconds(2024, 10, 6, 3, 30, 11));
    assert_eq!(c.value(1), 1_728_145_800);
}

// The resolution is independent of the time unit.
#[test]
fn test_cast_timestamp_to_named_timezone_dst_nanosecond() {
    let ambiguous = naive_seconds(2024, 11, 3, 1, 30) * 1_000_000_000 + 123_456_789;
    let nonexistent = naive_seconds(2024, 3, 10, 2, 30) * 1_000_000_000 + 123_456_789;
    let array = TimestampNanosecondArray::from(vec![Some(ambiguous), Some(nonexistent)]);
    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampNanosecondType>();
    assert_eq!(c.value(0), 1_730_615_400 * 1_000_000_000 + 123_456_789);
    assert_eq!(c.value(1), 1_710_055_800 * 1_000_000_000 + 123_456_789);
}

// Unit conversion still composes with the timezone adjustment.
#[test]
fn test_cast_timestamp_to_named_timezone_dst_changing_unit() {
    let array = TimestampSecondArray::from(vec![
        Some(naive_seconds(2024, 11, 3, 1, 30)),
        Some(naive_seconds(2024, 3, 10, 2, 30)),
    ]);
    let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampMillisecondType>();
    assert_eq!(c.value(0), 1_730_615_400_000);
    assert_eq!(c.value(1), 1_710_055_800_000);
}

// A fixed offset has no transitions, so the same readings are unaffected.
#[test]
fn test_cast_timestamp_to_fixed_offset_timezone_unaffected() {
    let array = TimestampSecondArray::from(vec![
        Some(naive_seconds(2024, 11, 1, 0, 0)),
        Some(naive_seconds(2024, 11, 3, 1, 30)),
        Some(naive_seconds(2024, 3, 10, 2, 30)),
        None,
    ]);
    let to_type = DataType::Timestamp(TimeUnit::Second, Some("+08:00".into()));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let b = cast_with_options(&array, &to_type, &options).unwrap();
    assert_eq!(b.data_type(), &to_type);
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(c.value(0), instant_seconds(2024, 11, 1, 0, 0, 8));
    assert_eq!(c.value(1), instant_seconds(2024, 11, 3, 1, 30, 8));
    assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 2, 30, 8));
    assert!(c.is_null(3));
}

#[test]
fn test_cast_date64_to_timestamp() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(864000000, c.value(0));
    assert_eq!(1545696000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date64_to_timestamp_ms() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(864000000005, c.value(0));
    assert_eq!(1545696000001, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date64_to_timestamp_us() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(864000000005000, c.value(0));
    assert_eq!(1545696000001000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date64_to_timestamp_ns() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(864000000005000000, c.value(0));
    assert_eq!(1545696000001000000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_timestamp_to_i64() {
    let array =
        TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None])
            .with_timezone("UTC".to_string());
    let b = cast(&array, &DataType::Int64).unwrap();
    let c = b.as_primitive::<Int64Type>();
    assert_eq!(&DataType::Int64, c.data_type());
    assert_eq!(864000000005, c.value(0));
    assert_eq!(1545696000001, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_and_timestamp_with_timezone() {
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let a = Date32Array::from(vec![Some(18628), None, None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;

    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
    )
    .unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));

    let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00", result.value(0));
}

#[test]
fn test_cast_date32_to_timestamp_with_timezone() {
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
    )
    .unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(1609438500, c.value(0));
    assert_eq!(1640974500, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
    assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
}

#[test]
fn test_cast_date32_to_timestamp_with_timezone_ms() {
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Millisecond, Some(tz.into())),
    )
    .unwrap();
    let c = b.as_primitive::<TimestampMillisecondType>();
    assert_eq!(1609438500000, c.value(0));
    assert_eq!(1640974500000, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
    assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
}

#[test]
fn test_cast_date32_to_timestamp_with_timezone_us() {
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, Some(tz.into())),
    )
    .unwrap();
    let c = b.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(1609438500000000, c.value(0));
    assert_eq!(1640974500000000, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
    assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
}

#[test]
fn test_cast_date32_to_timestamp_with_timezone_ns() {
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
    )
    .unwrap();
    let c = b.as_primitive::<TimestampNanosecondType>();
    assert_eq!(1609438500000000000, c.value(0));
    assert_eq!(1640974500000000000, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
    assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
}

#[test]
fn test_cast_date64_to_timestamp_with_timezone() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
    )
    .unwrap();

    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(863979300, c.value(0));
    assert_eq!(1545675300, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("1997-05-19T00:00:00+05:45", result.value(0));
    assert_eq!("2018-12-25T00:00:00+05:45", result.value(1));
}

#[test]
fn test_cast_date64_to_timestamp_with_timezone_ms() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Millisecond, Some(tz.into())),
    )
    .unwrap();

    let c = b.as_primitive::<TimestampMillisecondType>();
    assert_eq!(863979300005, c.value(0));
    assert_eq!(1545675300001, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
    assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
}

#[test]
fn test_cast_date64_to_timestamp_with_timezone_us() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, Some(tz.into())),
    )
    .unwrap();

    let c = b.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(863979300005000, c.value(0));
    assert_eq!(1545675300001000, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
    assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
}

#[test]
fn test_cast_date64_to_timestamp_with_timezone_ns() {
    let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
    )
    .unwrap();

    let c = b.as_primitive::<TimestampNanosecondType>();
    assert_eq!(863979300005000000, c.value(0));
    assert_eq!(1545675300001000000, c.value(1));
    assert!(c.is_null(2));

    let string_array = cast(&c, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
    assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
}

#[test]
fn test_cast_timestamp_to_strings() {
    // "2018-12-25T00:00:02.001", "1997-05-19T00:00:03.005", None
    let array =
        TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
    let expected = vec![
        Some("1997-05-19T00:00:03.005"),
        Some("2018-12-25T00:00:02.001"),
        None,
    ];

    assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
    assert_cast!(array, DataType::Utf8, StringArray, expected);
    assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
}

#[test]
fn test_cast_timestamp_to_strings_opt() {
    let ts_format = "%Y-%m-%d %H:%M:%S%.6f";
    let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
    let cast_options = CastOptions {
        safe: true,
        format_options: FormatOptions::default()
            .with_timestamp_format(Some(ts_format))
            .with_timestamp_tz_format(Some(ts_format)),
    };

    // "2018-12-25T00:00:02.001", "1997-05-19T00:00:03.005", None
    let array_without_tz =
        TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
    let expected = vec![
        Some("1997-05-19 00:00:03.005000"),
        Some("2018-12-25 00:00:02.001000"),
        None,
    ];
    assert_cast!(
        array_without_tz,
        DataType::Utf8View,
        StringViewArray,
        cast_options,
        expected
    );
    assert_cast!(
        array_without_tz,
        DataType::Utf8,
        StringArray,
        cast_options,
        expected
    );
    assert_cast!(
        array_without_tz,
        DataType::LargeUtf8,
        LargeStringArray,
        cast_options,
        expected
    );

    let array_with_tz =
        TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None])
            .with_timezone(tz.to_string());
    let expected = vec![
        Some("1997-05-19 05:45:03.005000"),
        Some("2018-12-25 05:45:02.001000"),
        None,
    ];
    assert_cast!(
        array_with_tz,
        DataType::Utf8View,
        StringViewArray,
        cast_options,
        expected
    );
    assert_cast!(
        array_with_tz,
        DataType::Utf8,
        StringArray,
        cast_options,
        expected
    );
    assert_cast!(
        array_with_tz,
        DataType::LargeUtf8,
        LargeStringArray,
        cast_options,
        expected
    );
}

#[test]
fn test_cast_between_timestamps() {
    let array =
        TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(864000003, c.value(0));
    assert_eq!(1545696002, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_outside_supported_range_for_nanoseconds() {
    const EXPECTED_ERROR_MESSAGE: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

    let array = StringArray::from(vec![Some("1650-01-01 01:01:01.000001")]);

    let cast_options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    let result = cast_with_options(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        &cast_options,
    );

    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "Cast error: Overflow converting {} to Nanosecond. {}",
            array.value(0),
            EXPECTED_ERROR_MESSAGE
        )
    );
}

#[test]
fn test_cast_date32_to_timestamp() {
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
    let c = b.as_primitive::<TimestampSecondType>();
    assert_eq!(1609459200, c.value(0));
    assert_eq!(1640995200, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_ms() {
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(1609459200000, c.value(0));
    assert_eq!(1640995200000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_us() {
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(1609459200000000, c.value(0));
    assert_eq!(1640995200000000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_ns() {
    let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    let c = b
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(1609459200000000000, c.value(0));
    assert_eq!(1640995200000000000, c.value(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_us_overflow() {
    const MAX_DAYS_MICROS: i32 = (i64::MAX / MICROSECONDS_IN_DAY) as i32;
    let a = Date32Array::from(vec![Some(MAX_DAYS_MICROS), Some(MAX_DAYS_MICROS + 1), None]);
    let array = Arc::new(a) as ArrayRef;
    let err = cast_with_options(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(err.is_err());

    let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    let c = b.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(MAX_DAYS_MICROS as i64 * MICROSECONDS_IN_DAY, c.value(0));
    assert!(c.is_null(1));
    assert!(c.is_null(2));
}

#[test]
fn test_cast_date32_to_timestamp_ns_overflow() {
    // 2262-04-11, 2062-04-12
    let upper_limit = 106_751;
    let a = Date32Array::from(vec![Some(upper_limit), Some(upper_limit + 1), None]);
    let array = Arc::new(a) as ArrayRef;
    let err = cast_with_options(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(err.is_err());

    let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    let c = b.as_primitive::<TimestampNanosecondType>();
    assert_eq!(upper_limit as i64 * NANOSECONDS_IN_DAY, c.value(0));
    assert!(c.is_null(1));
    assert!(c.is_null(2));
}

#[test]
fn test_timezone_cast() {
    let a = StringArray::from(vec![
        "2000-01-01T12:00:00", // date + time valid
        "2020-12-15T12:34:56", // date + time valid
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    let v = b.as_primitive::<TimestampNanosecondType>();

    assert_eq!(v.value(0), 946728000000000000);
    assert_eq!(v.value(1), 1608035696000000000);

    let b = cast(
        &b,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
    )
    .unwrap();
    let v = b.as_primitive::<TimestampNanosecondType>();

    assert_eq!(v.value(0), 946728000000000000);
    assert_eq!(v.value(1), 1608035696000000000);

    let b = cast(
        &b,
        &DataType::Timestamp(TimeUnit::Millisecond, Some("+02:00".into())),
    )
    .unwrap();
    let v = b.as_primitive::<TimestampMillisecondType>();

    assert_eq!(v.value(0), 946728000000);
    assert_eq!(v.value(1), 1608035696000);
}

#[test]
fn test_cast_utf8_to_timestamp() {
    fn test_tz(tz: Arc<str>) {
        let valid = StringArray::from(vec![
            "2023-01-01 04:05:06.789000-08:00",
            "2023-01-01 04:05:06.789000-07:00",
            "2023-01-01 04:05:06.789 -0800",
            "2023-01-01 04:05:06.789 -08:00",
            "2023-01-01 040506 +0730",
            "2023-01-01 040506 +07:30",
            "2023-01-01 04:05:06.789",
            "2023-01-01 04:05:06",
            "2023-01-01",
        ]);

        let array = Arc::new(valid) as ArrayRef;
        let b = cast_with_options(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.clone())),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        )
        .unwrap();

        let tz = tz.as_ref().parse().unwrap();

        let as_tz = |v: i64| as_datetime_with_timezone::<TimestampNanosecondType>(v, tz).unwrap();

        let as_utc = |v: &i64| as_tz(*v).naive_utc().to_string();
        let as_local = |v: &i64| as_tz(*v).naive_local().to_string();

        let values = b.as_primitive::<TimestampNanosecondType>().values();
        let utc_results: Vec<_> = values.iter().map(as_utc).collect();
        let local_results: Vec<_> = values.iter().map(as_local).collect();

        // Absolute timestamps should be parsed preserving the same UTC instant
        assert_eq!(
            &utc_results[..6],
            &[
                "2023-01-01 12:05:06.789".to_string(),
                "2023-01-01 11:05:06.789".to_string(),
                "2023-01-01 12:05:06.789".to_string(),
                "2023-01-01 12:05:06.789".to_string(),
                "2022-12-31 20:35:06".to_string(),
                "2022-12-31 20:35:06".to_string(),
            ]
        );
        // Non-absolute timestamps should be parsed preserving the same local instant
        assert_eq!(
            &local_results[6..],
            &[
                "2023-01-01 04:05:06.789".to_string(),
                "2023-01-01 04:05:06".to_string(),
                "2023-01-01 00:00:00".to_string()
            ]
        )
    }

    test_tz("+00:00".into());
    test_tz("+02:00".into());
}

#[test]
fn test_cast_utf8_to_timestamptz() {
    let valid = StringArray::from(vec!["2023-01-01"]);

    let array = Arc::new(valid) as ArrayRef;
    let b = cast(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
    )
    .unwrap();

    let expect = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));

    assert_eq!(b.data_type(), &expect);
    let c = b
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(1672531200000000000, c.value(0));
}

#[test]
fn test_cast_below_unixtimestamp() {
    let valid = StringArray::from(vec![
        "1900-01-03 23:59:59",
        "1969-12-31 00:00:01",
        "1989-12-31 00:00:01",
    ]);

    let array = Arc::new(valid) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .unwrap();

    let ts_array = casted_array
        .as_primitive::<TimestampNanosecondType>()
        .values()
        .iter()
        .map(|ts| ts / 1_000_000)
        .collect::<Vec<_>>();

    let array = TimestampMillisecondArray::from(ts_array).with_timezone("+00:00".to_string());
    let casted_array = cast(&array, &DataType::Date32).unwrap();
    let date_array = casted_array.as_primitive::<Date32Type>();
    let casted_array = cast(&date_array, &DataType::Utf8).unwrap();
    let string_array = casted_array.as_string::<i32>();
    assert_eq!("1900-01-03", string_array.value(0));
    assert_eq!("1969-12-31", string_array.value(1));
    assert_eq!("1989-12-31", string_array.value(2));
}

#[test]
fn test_cast_string_to_timestamp_invalid_tz() {
    // content after Z should be ignored
    let bad_timestamp = "2023-12-05T21:58:10.45ZZTOP";
    let array = StringArray::from(vec![Some(bad_timestamp)]);

    let data_types = [
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ];

    let cast_options = CastOptions {
        safe: false,
        ..Default::default()
    };

    for dt in data_types {
        // The trailing detail of the message differs depending on whether
        // `chrono-tz` is enabled, so only the common prefix is asserted.
        let err = cast_with_options(&array, &dt, &cast_options)
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("Parser error: Invalid timezone \"ZZTOP\":"),
            "{err}"
        );
    }
}

#[test]
fn test_cast_date64_to_timestamp_overflow() {
    let array = Date64Array::from(vec![i64::MIN, i64::MAX]);
    for to_type in [
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        assert_temporal_overflow_is_safe(&array, &to_type);
    }
}

#[test]
fn test_cast_temporal_scaling_boundaries() {
    fn check<FROM, TO>(multiplier: i64)
    where
        FROM: ArrowPrimitiveType<Native = i64>,
        TO: ArrowPrimitiveType<Native = i64>,
    {
        let min = i64::MIN / multiplier;
        let max = i64::MAX / multiplier;
        let array = PrimitiveArray::<FROM>::new(
            vec![i64::MAX, min - 1, min, -1, i64::MAX, max, max + 1].into(),
            Some(vec![true, true, true, true, false, true, true].into()),
        )
        .slice(1, 6);
        let expected = PrimitiveArray::<TO>::from_iter([
            None,
            Some(min * multiplier),
            Some(-multiplier),
            None,
            Some(max * multiplier),
            None,
        ]);
        let result = cast(&array, &TO::DATA_TYPE).unwrap();
        assert_eq!(result.as_primitive::<TO>(), &expected);
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        assert!(cast_with_options(&array, &TO::DATA_TYPE, &options).is_err());
        let result = cast_with_options(&array.slice(1, 4), &TO::DATA_TYPE, &options).unwrap();
        assert_eq!(result.as_primitive::<TO>(), &expected.slice(1, 4));
    }
    check::<Time64MicrosecondType, Time64NanosecondType>(NANOSECONDS / MICROSECONDS);
    check::<Date64Type, TimestampMicrosecondType>(MICROSECONDS / MILLISECONDS);
    check::<Date64Type, TimestampNanosecondType>(NANOSECONDS / MILLISECONDS);
}

#[test]
fn test_cast_timestamp_and_duration_to_coarser_unit() {
    // Truncates toward zero, as `/` does, and keeps nulls
    let edges = [
        i64::MIN,
        i64::MIN + 1,
        -1_000_000_001,
        -1_000_000_000,
        -999_999_999,
        -1001,
        -1000,
        -999,
        -1,
        0,
        1,
        999,
        1000,
        1001,
        999_999_999,
        1_000_000_000,
        1_000_000_001,
        i64::MAX - 1,
        i64::MAX,
    ];
    let values: Int64Array = edges
        .into_iter()
        .map(Some)
        .chain([None])
        .cycle()
        .take(70)
        .collect();
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for array in [values.clone(), values.slice(3, 60)] {
        for (i, from) in units.into_iter().enumerate() {
            for to in &units[..i] {
                let ratio = time_unit_multiple(&from) / time_unit_multiple(to);
                let expected: Int64Array = array.iter().map(|v| v.map(|v| v / ratio)).collect();
                for (from_type, to_type) in [
                    (Timestamp(from, None), Timestamp(*to, None)),
                    (Duration(from), Duration(*to)),
                ] {
                    let input = cast(&array, &from_type).unwrap();
                    let output = cast(&input, &to_type).unwrap();
                    assert_eq!(output.data_type(), &to_type);
                    let output = cast(&output, &Int64).unwrap();
                    assert_eq!(
                        output.as_primitive::<Int64Type>(),
                        &expected,
                        "{from_type} to {to_type}"
                    );
                }
            }
        }
    }
}
