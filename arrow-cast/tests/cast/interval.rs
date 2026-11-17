// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

/// Number of `unit`s per second; mirrors the private `time_unit_multiple` in `arrow_cast`.
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

macro_rules! test_safe_string_to_interval {
    ($data_vec:expr, $interval_unit:expr, $array_ty:ty, $expect_vec:expr) => {
        let source_string_array = Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;

        let options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };

        let target_interval_array = cast_with_options(
            &source_string_array.clone(),
            &DataType::Interval($interval_unit),
            &options,
        )
        .unwrap()
        .as_any()
        .downcast_ref::<$array_ty>()
        .unwrap()
        .clone() as $array_ty;

        let target_string_array =
            cast_with_options(&target_interval_array, &DataType::Utf8, &options)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();

        let expect_string_array = StringArray::from($expect_vec);

        assert_eq!(target_string_array, expect_string_array);

        let target_large_string_array =
            cast_with_options(&target_interval_array, &DataType::LargeUtf8, &options)
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .clone();

        let expect_large_string_array = LargeStringArray::from($expect_vec);

        assert_eq!(target_large_string_array, expect_large_string_array);
    };
}

#[test]
fn test_cast_string_to_interval_year_month() {
    test_safe_string_to_interval!(
        vec![
            Some("1 year 1 month"),
            Some("1.5 years 13 month"),
            Some("30 days"),
            Some("31 days"),
            Some("2 months 31 days"),
            Some("2 months 31 days 1 second"),
            Some("foobar"),
        ],
        IntervalUnit::YearMonth,
        IntervalYearMonthArray,
        vec![
            Some("1 years 1 mons"),
            Some("2 years 7 mons"),
            None,
            None,
            None,
            None,
            None,
        ]
    );
}

#[test]
fn test_cast_string_to_interval_day_time() {
    test_safe_string_to_interval!(
        vec![
            Some("1 year 1 month"),
            Some("1.5 years 13 month"),
            Some("30 days"),
            Some("1 day 2 second 3.5 milliseconds"),
            Some("foobar"),
        ],
        IntervalUnit::DayTime,
        IntervalDayTimeArray,
        vec![
            Some("390 days"),
            Some("930 days"),
            Some("30 days"),
            None,
            None,
        ]
    );
}

#[test]
fn test_cast_string_to_interval_month_day_nano() {
    test_safe_string_to_interval!(
        vec![
            Some("1 year 1 month 1 day"),
            None,
            Some("1.5 years 13 month 35 days 1.4 milliseconds"),
            Some("3 days"),
            Some("8 seconds"),
            None,
            Some("1 day 29800 milliseconds"),
            Some("3 months 1 second"),
            Some("6 minutes 120 second"),
            Some("2 years 39 months 9 days 19 hours 1 minute 83 seconds 399222 milliseconds"),
            Some("foobar"),
        ],
        IntervalUnit::MonthDayNano,
        IntervalMonthDayNanoArray,
        vec![
            Some("13 mons 1 days"),
            None,
            Some("31 mons 35 days 0.001400000 secs"),
            Some("3 days"),
            Some("8.000000000 secs"),
            None,
            Some("1 days 29.800000000 secs"),
            Some("3 mons 1.000000000 secs"),
            Some("8 mins"),
            Some("63 mons 9 days 19 hours 9 mins 2.222000000 secs"),
            None,
        ]
    );
}

macro_rules! test_unsafe_string_to_interval_err {
    ($data_vec:expr, $interval_unit:expr, $error_msg:expr) => {
        let string_array = Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let arrow_err = cast_with_options(
            &string_array.clone(),
            &DataType::Interval($interval_unit),
            &options,
        )
        .unwrap_err();
        assert_eq!($error_msg, arrow_err.to_string());
    };
}

#[test]
fn test_cast_string_to_interval_err() {
    test_unsafe_string_to_interval_err!(
        vec![Some("foobar")],
        IntervalUnit::YearMonth,
        r#"Parser error: Invalid input syntax for type interval: "foobar""#
    );
    test_unsafe_string_to_interval_err!(
        vec![Some("foobar")],
        IntervalUnit::DayTime,
        r#"Parser error: Invalid input syntax for type interval: "foobar""#
    );
    test_unsafe_string_to_interval_err!(
        vec![Some("foobar")],
        IntervalUnit::MonthDayNano,
        r#"Parser error: Invalid input syntax for type interval: "foobar""#
    );
    test_unsafe_string_to_interval_err!(
        vec![Some("2 months 31 days 1 second")],
        IntervalUnit::YearMonth,
        "Cast error: Cannot cast 2 months 31 days 1 second to IntervalYearMonth. Only year and month fields are allowed."
    );
    test_unsafe_string_to_interval_err!(
        vec![Some("1 day 1.5 milliseconds")],
        IntervalUnit::DayTime,
        "Cast error: Cannot cast 1 day 1.5 milliseconds to IntervalDayTime because the nanos part isn't multiple of milliseconds"
    );

    // overflow
    test_unsafe_string_to_interval_err!(
        vec![Some(format!(
            "{} century {} year {} month",
            i64::MAX - 2,
            i64::MAX - 2,
            i64::MAX - 2
        ))],
        IntervalUnit::DayTime,
        format!(
            "Arithmetic overflow: Overflow happened on: {} * 100",
            i64::MAX - 2
        )
    );
    test_unsafe_string_to_interval_err!(
        vec![Some(format!(
            "{} year {} month {} day",
            i64::MAX - 2,
            i64::MAX - 2,
            i64::MAX - 2
        ))],
        IntervalUnit::MonthDayNano,
        format!(
            "Arithmetic overflow: Overflow happened on: {} * 12",
            i64::MAX - 2
        )
    );
}

#[test]
fn test_cast_duration_to_i64() {
    let base = vec![5, 6, 7, 8, 100000000];

    let duration_arrays = vec![
        Arc::new(DurationNanosecondArray::from(base.clone())) as ArrayRef,
        Arc::new(DurationMicrosecondArray::from(base.clone())) as ArrayRef,
        Arc::new(DurationMillisecondArray::from(base.clone())) as ArrayRef,
        Arc::new(DurationSecondArray::from(base.clone())) as ArrayRef,
    ];

    for arr in duration_arrays {
        assert!(can_cast_types(arr.data_type(), &DataType::Int64));
        let result = cast(&arr, &DataType::Int64).unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(base.as_slice(), result.values());
    }
}

#[test]
fn test_cast_between_durations_and_numerics() {
    fn test_cast_between_durations<FromType, ToType>()
    where
        FromType: ArrowPrimitiveType<Native = i64>,
        ToType: ArrowPrimitiveType<Native = i64>,
        PrimitiveArray<FromType>: From<Vec<Option<i64>>>,
    {
        let DataType::Duration(from_unit) = FromType::DATA_TYPE else {
            panic!("Expected a duration type")
        };
        let DataType::Duration(to_unit) = ToType::DATA_TYPE else {
            panic!("Expected a duration type")
        };
        let from_size = time_unit_multiple(&from_unit);
        let to_size = time_unit_multiple(&to_unit);

        let (v1_before, v2_before) = (8640003005, 1696002001);
        let (v1_after, v2_after) = if from_size >= to_size {
            (
                v1_before / (from_size / to_size),
                v2_before / (from_size / to_size),
            )
        } else {
            (
                v1_before * (to_size / from_size),
                v2_before * (to_size / from_size),
            )
        };

        let array = PrimitiveArray::<FromType>::from(vec![Some(v1_before), Some(v2_before), None]);
        let b = cast(&array, &ToType::DATA_TYPE).unwrap();
        let c = b.as_primitive::<ToType>();
        assert_eq!(v1_after, c.value(0));
        assert_eq!(v2_after, c.value(1));
        assert!(c.is_null(2));
    }

    // between each individual duration type
    test_cast_between_durations::<DurationSecondType, DurationMillisecondType>();
    test_cast_between_durations::<DurationSecondType, DurationMicrosecondType>();
    test_cast_between_durations::<DurationSecondType, DurationNanosecondType>();
    test_cast_between_durations::<DurationMillisecondType, DurationSecondType>();
    test_cast_between_durations::<DurationMillisecondType, DurationMicrosecondType>();
    test_cast_between_durations::<DurationMillisecondType, DurationNanosecondType>();
    test_cast_between_durations::<DurationMicrosecondType, DurationSecondType>();
    test_cast_between_durations::<DurationMicrosecondType, DurationMillisecondType>();
    test_cast_between_durations::<DurationMicrosecondType, DurationNanosecondType>();
    test_cast_between_durations::<DurationNanosecondType, DurationSecondType>();
    test_cast_between_durations::<DurationNanosecondType, DurationMillisecondType>();
    test_cast_between_durations::<DurationNanosecondType, DurationMicrosecondType>();

    // cast failed
    let array = DurationSecondArray::from(vec![
        Some(i64::MAX),
        Some(8640203410378005),
        Some(10241096),
        None,
    ]);
    let b = cast(&array, &DataType::Duration(TimeUnit::Nanosecond)).unwrap();
    let c = b.as_primitive::<DurationNanosecondType>();
    assert!(c.is_null(0));
    assert!(c.is_null(1));
    assert_eq!(10241096000000000, c.value(2));
    assert!(c.is_null(3));

    // durations to numerics
    let array = DurationSecondArray::from(vec![
        Some(i64::MAX),
        Some(8640203410378005),
        Some(10241096),
        None,
    ]);
    let b = cast(&array, &DataType::Int64).unwrap();
    let c = b.as_primitive::<Int64Type>();
    assert_eq!(i64::MAX, c.value(0));
    assert_eq!(8640203410378005, c.value(1));
    assert_eq!(10241096, c.value(2));
    assert!(c.is_null(3));

    let b = cast(&array, &DataType::Int32).unwrap();
    let c = b.as_primitive::<Int32Type>();
    assert_eq!(0, c.value(0));
    assert_eq!(0, c.value(1));
    assert_eq!(10241096, c.value(2));
    assert!(c.is_null(3));

    // numerics to durations
    let array = Int32Array::from(vec![Some(i32::MAX), Some(802034103), Some(10241096), None]);
    let b = cast(&array, &DataType::Duration(TimeUnit::Second)).unwrap();
    let c = b.as_any().downcast_ref::<DurationSecondArray>().unwrap();
    assert_eq!(i32::MAX as i64, c.value(0));
    assert_eq!(802034103, c.value(1));
    assert_eq!(10241096, c.value(2));
    assert!(c.is_null(3));
}

/// helper function to test casting from duration to interval
fn cast_from_duration_to_interval<T: ArrowTemporalType<Native = i64>>(
    array: Vec<i64>,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
    let array = PrimitiveArray::<T>::new(array.into(), None);
    let array = Arc::new(array) as ArrayRef;
    let interval = DataType::Interval(IntervalUnit::MonthDayNano);
    let out = cast_with_options(&array, &interval, cast_options)?;
    let out = out.as_primitive::<IntervalMonthDayNanoType>().clone();
    Ok(out)
}

#[test]
fn test_cast_from_duration_to_interval() {
    // from duration second to interval month day nano
    let array = vec![1234567];
    let casted_array =
        cast_from_duration_to_interval::<DurationSecondType>(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(0, 0, 1234567000000000)
    );

    let array = vec![i64::MAX];
    let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
        array.clone(),
        &CastOptions::default(),
    )
    .unwrap();
    assert!(!casted_array.is_valid(0));

    let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
        array,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_err());

    // from duration millisecond to interval month day nano
    let array = vec![1234567];
    let casted_array =
        cast_from_duration_to_interval::<DurationMillisecondType>(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(0, 0, 1234567000000)
    );

    let array = vec![i64::MAX];
    let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
        array.clone(),
        &CastOptions::default(),
    )
    .unwrap();
    assert!(!casted_array.is_valid(0));

    let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
        array,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_err());

    // from duration microsecond to interval month day nano
    let array = vec![1234567];
    let casted_array =
        cast_from_duration_to_interval::<DurationMicrosecondType>(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(0, 0, 1234567000)
    );

    let array = vec![i64::MAX];
    let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
        array.clone(),
        &CastOptions::default(),
    )
    .unwrap();
    assert!(!casted_array.is_valid(0));

    let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
        array,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_err());

    // from duration nanosecond to interval month day nano
    let array = vec![1234567];
    let casted_array =
        cast_from_duration_to_interval::<DurationNanosecondType>(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(0, 0, 1234567)
    );

    let array = vec![i64::MAX];
    let casted_array = cast_from_duration_to_interval::<DurationNanosecondType>(
        array,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .unwrap();
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(0, 0, i64::MAX)
    );
}

/// helper function to test casting from interval to duration
fn cast_from_interval_to_duration<T: ArrowTemporalType>(
    array: &IntervalMonthDayNanoArray,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError> {
    let casted_array = cast_with_options(&array, &T::DATA_TYPE, cast_options)?;
    casted_array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| ArrowError::ComputeError(format!("Failed to downcast to {}", T::DATA_TYPE)))
        .cloned()
}

#[test]
fn test_cast_from_interval_to_duration() {
    let nullable = CastOptions::default();
    let fallible = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let v = IntervalMonthDayNano::new(0, 0, 1234567);

    // from interval month day nano to duration second
    let array = vec![v].into();
    let casted_array: DurationSecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert_eq!(casted_array.value(0), 0);

    let array = vec![IntervalMonthDayNano::MAX].into();
    let casted_array: DurationSecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert!(!casted_array.is_valid(0));

    let res = cast_from_interval_to_duration::<DurationSecondType>(&array, &fallible);
    assert!(res.is_err());

    // from interval month day nano to duration millisecond
    let array = vec![v].into();
    let casted_array: DurationMillisecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert_eq!(casted_array.value(0), 1);

    let array = vec![IntervalMonthDayNano::MAX].into();
    let casted_array: DurationMillisecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert!(!casted_array.is_valid(0));

    let res = cast_from_interval_to_duration::<DurationMillisecondType>(&array, &fallible);
    assert!(res.is_err());

    // from interval month day nano to duration microsecond
    let array = vec![v].into();
    let casted_array: DurationMicrosecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert_eq!(casted_array.value(0), 1234);

    let array = vec![IntervalMonthDayNano::MAX].into();
    let casted_array =
        cast_from_interval_to_duration::<DurationMicrosecondType>(&array, &nullable).unwrap();
    assert!(!casted_array.is_valid(0));

    let casted_array = cast_from_interval_to_duration::<DurationMicrosecondType>(&array, &fallible);
    assert!(casted_array.is_err());

    // from interval month day nano to duration nanosecond
    let array = vec![v].into();
    let casted_array: DurationNanosecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert_eq!(casted_array.value(0), 1234567);

    let array = vec![IntervalMonthDayNano::MAX].into();
    let casted_array: DurationNanosecondArray =
        cast_from_interval_to_duration(&array, &nullable).unwrap();
    assert!(!casted_array.is_valid(0));

    let casted_array = cast_from_interval_to_duration::<DurationNanosecondType>(&array, &fallible);
    assert!(casted_array.is_err());

    let array = vec![
        IntervalMonthDayNanoType::make_value(0, 1, 0),
        IntervalMonthDayNanoType::make_value(-1, 0, 0),
        IntervalMonthDayNanoType::make_value(1, 1, 0),
        IntervalMonthDayNanoType::make_value(1, 0, 1),
        IntervalMonthDayNanoType::make_value(0, 0, -1),
    ]
    .into();
    let casted_array =
        cast_from_interval_to_duration::<DurationNanosecondType>(&array, &nullable).unwrap();
    assert!(!casted_array.is_valid(0));
    assert!(!casted_array.is_valid(1));
    assert!(!casted_array.is_valid(2));
    assert!(!casted_array.is_valid(3));
    assert!(casted_array.is_valid(4));
    assert_eq!(casted_array.value(4), -1);
}

/// helper function to test casting from interval year month to interval month day nano
fn cast_from_interval_year_month_to_interval_month_day_nano(
    array: Vec<i32>,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
    let array = PrimitiveArray::<IntervalYearMonthType>::from(array);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Interval(IntervalUnit::MonthDayNano),
        cast_options,
    )?;
    casted_array
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError("Failed to downcast to IntervalMonthDayNanoArray".to_string())
        })
        .cloned()
}

#[test]
fn test_cast_from_interval_year_month_to_interval_month_day_nano() {
    // from interval year month to interval month day nano
    let array = vec![1234567];
    let casted_array =
        cast_from_interval_year_month_to_interval_month_day_nano(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        casted_array.value(0),
        IntervalMonthDayNano::new(1234567, 0, 0)
    );
}

/// helper function to test casting from interval day time to interval month day nano
fn cast_from_interval_day_time_to_interval_month_day_nano(
    array: Vec<IntervalDayTime>,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
    let array = PrimitiveArray::<IntervalDayTimeType>::from(array);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Interval(IntervalUnit::MonthDayNano),
        cast_options,
    )?;
    Ok(casted_array
        .as_primitive::<IntervalMonthDayNanoType>()
        .clone())
}

#[test]
fn test_cast_from_interval_day_time_to_interval_month_day_nano() {
    // from interval day time to interval month day nano
    let array = vec![IntervalDayTime::new(123, 0)];
    let casted_array =
        cast_from_interval_day_time_to_interval_month_day_nano(array, &CastOptions::default())
            .unwrap();
    assert_eq!(
        casted_array.data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(casted_array.value(0), IntervalMonthDayNano::new(0, 123, 0));
}
