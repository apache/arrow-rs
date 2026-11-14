// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::{DataType, TimeUnit};

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
