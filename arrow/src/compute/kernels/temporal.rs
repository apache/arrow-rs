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

//! Defines temporal kernels for time and date related functions.

use arrow_array::{downcast_dictionary_array, downcast_temporal_array};
use chrono::{DateTime, Datelike, NaiveDateTime, NaiveTime, Offset, Timelike};
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use arrow_array::temporal_conversions::{
    as_datetime, as_datetime_with_timezone, as_time,
};

use arrow_array::timezone::Tz;

/// This function takes an `ArrayIter` of input array and an extractor `op` which takes
/// an input `NaiveTime` and returns time component (e.g. hour) as `i32` value.
/// The extracted values are built by the given `builder` to be an `Int32Array`.
fn as_time_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<Int32Type>,
    op: F,
) -> Int32Array
where
    F: Fn(NaiveTime) -> i32,
    i64: From<T::Native>,
{
    iter.into_iter().for_each(|value| {
        if let Some(value) = value {
            match as_time::<T>(i64::from(value)) {
                Some(dt) => builder.append_value(op(dt)),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    });

    builder.finish()
}

/// This function takes an `ArrayIter` of input array and an extractor `op` which takes
/// an input `NaiveDateTime` and returns data time component (e.g. hour) as `i32` value.
/// The extracted values are built by the given `builder` to be an `Int32Array`.
fn as_datetime_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<Int32Type>,
    op: F,
) -> Int32Array
where
    F: Fn(NaiveDateTime) -> i32,
    i64: From<T::Native>,
{
    iter.into_iter().for_each(|value| {
        if let Some(value) = value {
            match as_datetime::<T>(i64::from(value)) {
                Some(dt) => builder.append_value(op(dt)),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    });

    builder.finish()
}

/// This function extracts date time component (e.g. hour) from an array of datatime.
/// `iter` is the `ArrayIter` of input datatime array. `builder` is used to build the
/// returned `Int32Array` containing the extracted components. `tz` is timezone string
/// which will be added to datetime values in the input array. `parsed` is a `Parsed`
/// object used to parse timezone string. `op` is the extractor closure which takes
/// data time object of `NaiveDateTime` type and returns `i32` value of extracted
/// component.
fn extract_component_from_datetime_array<
    A: ArrayAccessor<Item = T::Native>,
    T: ArrowTemporalType,
    F,
>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<Int32Type>,
    tz: &str,
    op: F,
) -> Result<Int32Array>
where
    F: Fn(DateTime<Tz>) -> i32,
    i64: From<T::Native>,
{
    let tz: Tz = tz.parse()?;
    for value in iter {
        match value {
            Some(value) => match as_datetime_with_timezone::<T>(value.into(), tz) {
                Some(time) => builder.append_value(op(time)),
                _ => {
                    return Err(ArrowError::ComputeError(
                        "Unable to read value as datetime".to_string(),
                    ))
                }
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(ArrowError::ComputeError(format!("{}: {:?}", $msg, $param))) }
    };
}

pub(crate) use return_compute_error_with;

// Internal trait, which is used for mapping values from DateLike structures
trait ChronoDateExt {
    /// Returns a value in range `1..=4` indicating the quarter this date falls into
    fn quarter(&self) -> u32;

    /// Returns a value in range `0..=3` indicating the quarter (zero-based) this date falls into
    fn quarter0(&self) -> u32;

    /// Returns the day of week; Monday is encoded as `0`, Tuesday as `1`, etc.
    fn num_days_from_monday(&self) -> i32;

    /// Returns the day of week; Sunday is encoded as `0`, Monday as `1`, etc.
    fn num_days_from_sunday(&self) -> i32;
}

impl<T: Datelike> ChronoDateExt for T {
    fn quarter(&self) -> u32 {
        self.quarter0() + 1
    }

    fn quarter0(&self) -> u32 {
        self.month0() / 3
    }

    fn num_days_from_monday(&self) -> i32 {
        self.weekday().num_days_from_monday() as i32
    }

    fn num_days_from_sunday(&self) -> i32 {
        self.weekday().num_days_from_sunday() as i32
    }
}

/// Parse the given string into a string representing fixed-offset that is correct as of the given
/// UTC NaiveDateTime.
/// Note that the offset is function of time and can vary depending on whether daylight savings is
/// in effect or not. e.g. Australia/Sydney is +10:00 or +11:00 depending on DST.
#[deprecated(note = "Use arrow_array::timezone::Tz instead")]
pub fn using_chrono_tz_and_utc_naive_date_time(
    tz: &str,
    utc: NaiveDateTime,
) -> Option<chrono::offset::FixedOffset> {
    use chrono::TimeZone;
    let tz: Tz = tz.parse().ok()?;
    Some(tz.offset_from_utc_datetime(&utc).fix())
}

/// Extracts the hours of a given array as an array of integers within
/// the range of [0, 23]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn hour_dyn(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let hour_values = hour_dyn(array.values())?;
                    Ok(Arc::new(array.with_values(&hour_values)))
                }
                dt => return_compute_error_with!("hour does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   hour(array)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("hour does not support", dt),
            )
        }
    }
}

/// Extracts the hours of a given temporal primitive array as an array of integers within
/// the range of [0, 23].
pub fn hour<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let b = Int32Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Time32(_) | DataType::Time64(_) => {
            let iter = ArrayIter::new(array);
            Ok(as_time_with_op::<&PrimitiveArray<T>, T, _>(iter, b, |t| {
                t.hour() as i32
            }))
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                b,
                |t| t.hour() as i32,
            ))
        }
        DataType::Timestamp(_, Some(tz)) => {
            let iter = ArrayIter::new(array);
            extract_component_from_datetime_array::<&PrimitiveArray<T>, T, _>(
                iter,
                b,
                tz,
                |t| t.hour() as i32,
            )
        }
        _ => return_compute_error_with!("hour does not support", array.data_type()),
    }
}

/// Extracts the years of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn year_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "year", |t| t.year() as i32)
}

/// Extracts the years of a given temporal primitive array as an array of integers
pub fn year<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "year", |t| t.year() as i32)
}

/// Extracts the quarter of a given temporal array as an array of integersa within
/// the range of [1, 4]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn quarter_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "quarter", |t| t.quarter() as i32)
}

/// Extracts the quarter of a given temporal primitive array as an array of integers within
/// the range of [1, 4].
pub fn quarter<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "quarter", |t| t.quarter() as i32)
}

/// Extracts the month of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn month_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "month", |t| t.month() as i32)
}

/// Extracts the month of a given temporal primitive array as an array of integers within
/// the range of [1, 12].
pub fn month<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "month", |t| t.month() as i32)
}

/// Extracts the day of week of a given temporal array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
///
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn num_days_from_monday_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "num_days_from_monday", |t| {
        t.num_days_from_monday() as i32
    })
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
pub fn num_days_from_monday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "num_days_from_monday", |t| {
        t.num_days_from_monday() as i32
    })
}

/// Extracts the day of week of a given temporal array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
///
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn num_days_from_sunday_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "num_days_from_sunday", |t| {
        t.num_days_from_sunday() as i32
    })
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
pub fn num_days_from_sunday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "num_days_from_sunday", |t| {
        t.num_days_from_sunday() as i32
    })
}

/// Extracts the day of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn day_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "day", |t| t.day() as i32)
}

/// Extracts the day of a given temporal primitive array as an array of integers
pub fn day<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "day", |t| t.day() as i32)
}

/// Extracts the day of year of a given temporal array as an array of integers
/// The day of year that ranges from 1 to 366.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn doy_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "doy", |t| t.ordinal() as i32)
}

/// Extracts the day of year of a given temporal primitive array as an array of integers
/// The day of year that ranges from 1 to 366
pub fn doy<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "doy", |t| t.ordinal() as i32)
}

/// Extracts the minutes of a given temporal primitive array as an array of integers
pub fn minute<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "minute", |t| t.minute() as i32)
}

/// Extracts the week of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn week_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "week", |t| t.iso_week().week() as i32)
}

/// Extracts the week of a given temporal primitive array as an array of integers
pub fn week<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "week", |t| t.iso_week().week() as i32)
}

/// Extracts the seconds of a given temporal primitive array as an array of integers
pub fn second<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "second", |t| t.second() as i32)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers
pub fn nanosecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    time_fraction_internal(array, "nanosecond", |t| t.nanosecond() as i32)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn nanosecond_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "nanosecond", |t| t.nanosecond() as i32)
}

/// Extracts the time fraction of a given temporal array as an array of integers
fn time_fraction_dyn<F>(array: &dyn Array, name: &str, op: F) -> Result<ArrayRef>
where
    F: Fn(NaiveDateTime) -> i32,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let values = time_fraction_dyn(array.values(), name, op)?;
                    Ok(Arc::new(array.with_values(&values)))
                }
                dt => return_compute_error_with!(format!("{} does not support", name), dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   time_fraction_internal(array, name, op)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!(format!("{} does not support", name), dt),
            )
        }
    }
}

/// Extracts the time fraction of a given temporal array as an array of integers
fn time_fraction_internal<T, F>(
    array: &PrimitiveArray<T>,
    name: &str,
    op: F,
) -> Result<Int32Array>
where
    F: Fn(NaiveDateTime) -> i32,
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let b = Int32Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            Ok(as_datetime_with_op::<_, T, _>(iter, b, op))
        }
        DataType::Timestamp(_, Some(tz)) => {
            let iter = ArrayIter::new(array);
            extract_component_from_datetime_array::<_, T, _>(iter, b, tz, |t| {
                op(t.naive_local())
            })
        }
        _ => return_compute_error_with!(
            format!("{} does not support", name),
            array.data_type()
        ),
    }
}

/// Extracts the minutes of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn minute_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "minute", |t| t.minute() as i32)
}

/// Extracts the seconds of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn second_dyn(array: &dyn Array) -> Result<ArrayRef> {
    time_fraction_dyn(array, "second", |t| t.second() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temporal_array_date64_hour() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = hour(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_hour() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15148)].into();

        let b = hour(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(0, b.value(2));
    }

    #[test]
    fn test_temporal_array_time32_second_hour() {
        let a: PrimitiveArray<Time32SecondType> = vec![37800, 86339].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_time64_micro_hour() {
        let a: PrimitiveArray<Time64MicrosecondType> =
            vec![37800000000, 86339000000].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_hour() {
        let a: TimestampMicrosecondArray = vec![37800000000, 86339000000].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_date64_year() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = year(&a).unwrap();
        assert_eq!(2018, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2019, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_year() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15448)].into();

        let b = year(&a).unwrap();
        assert_eq!(2011, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2012, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_quarter() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1566275025000)].into();

        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(3, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_quarter() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(300)].into();

        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_quarter_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400 * 90])
            .with_timezone("+00:00".to_string());
        let b = quarter(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 90])
            .with_timezone("-10:00".to_string());
        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_month() {
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_month() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(31)].into();

        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_month_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400 * 31])
            .with_timezone("+00:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 31])
            .with_timezone("-10:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_day_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a =
            TimestampSecondArray::from(vec![86400]).with_timezone("+00:00".to_string());
        let b = day(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a =
            TimestampSecondArray::from(vec![86400]).with_timezone("-10:00".to_string());
        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_weekday() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = num_days_from_monday(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_weekday0() {
        //1483228800000 -> 2017-01-01 (Sunday)
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1483228800000),
            None,
            Some(1514764800000),
            Some(1550636625000),
        ]
        .into();

        let b = num_days_from_sunday(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
        assert_eq!(3, b.value(3));
    }

    #[test]
    fn test_temporal_array_date64_day() {
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(20, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_day() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(31)].into();

        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_doy() {
        //1483228800000 -> 2017-01-01 (Sunday)
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1483228800000),
            Some(1514764800000),
            None,
            Some(1550636625000),
        ]
        .into();

        let b = doy(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(1, b.value(1));
        assert!(!b.is_valid(2));
        assert_eq!(51, b.value(3));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_year() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = year(&a).unwrap();
        assert_eq!(2021, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2024, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_minute() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = minute(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(23, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_minute() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = minute(&a).unwrap();
        assert_eq!(57, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(44, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_week() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(7)].into();

        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_week() {
        // 1646116175000 -> 2022.03.01 , 1641171600000 -> 2022.01.03
        // 1640998800000 -> 2022.01.01
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1646116175000),
            None,
            Some(1641171600000),
            Some(1640998800000),
        ]
        .into();

        let b = week(&a).unwrap();
        assert_eq!(9, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
        assert_eq!(52, b.value(3));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_week() {
        //1612025847000000 -> 2021.1.30
        //1722015847000000 -> 2024.7.27
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = week(&a).unwrap();
        assert_eq!(4, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(30, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_second() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = second(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(45, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_second() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = second(&a).unwrap();
        assert_eq!(27, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(7, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_second_with_timezone() {
        let a =
            TimestampSecondArray::from(vec![10, 20]).with_timezone("+00:00".to_string());
        let b = second(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(20, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_timezone() {
        let a =
            TimestampSecondArray::from(vec![0, 60]).with_timezone("+00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(50, b.value(0));
        assert_eq!(51, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_negative_timezone() {
        let a =
            TimestampSecondArray::from(vec![60 * 55]).with_timezone("-00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(5, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("+01:00".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("+0100".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_minutes() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("+01".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_initial_sign() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("0100".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_with_only_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("01:00".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("Asia/Kolkata".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(15, b.value(0));
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_temporal_array_timestamp_hour_with_dst_timezone_using_chrono_tz() {
        //
        // 1635577147 converts to 2021-10-30 17:59:07 in time zone Australia/Sydney (AEDT)
        // The offset (difference to UTC) is +11:00. Note that daylight savings is in effect on 2021-10-30.
        // When daylight savings is not in effect, Australia/Sydney has an offset difference of +10:00.

        let a = TimestampMillisecondArray::from(vec![Some(1635577147000)])
            .with_timezone("Australia/Sydney".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(17, b.value(0));
    }

    #[cfg(not(feature = "chrono-tz"))]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10])
            .with_timezone("Asia/Kolkatta".to_string());
        assert!(matches!(hour(&a), Err(ArrowError::ParseError(_))))
    }

    #[test]
    fn test_temporal_array_timestamp_week_without_timezone() {
        // 1970-01-01T00:00:00                     -> 1970-01-01T00:00:00 Thursday (week 1)
        // 1970-01-01T00:00:00 + 4 days            -> 1970-01-05T00:00:00 Monday   (week 2)
        // 1970-01-01T00:00:00 + 4 days - 1 second -> 1970-01-04T23:59:59 Sunday   (week 1)
        let a = TimestampSecondArray::from(vec![0, 86400 * 4, 86400 * 4 - 1]);
        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(2, b.value(1));
        assert_eq!(1, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_week_with_timezone() {
        // 1970-01-01T01:00:00+01:00                     -> 1970-01-01T01:00:00+01:00 Thursday (week 1)
        // 1970-01-01T01:00:00+01:00 + 4 days            -> 1970-01-05T01:00:00+01:00 Monday   (week 2)
        // 1970-01-01T01:00:00+01:00 + 4 days - 1 second -> 1970-01-05T00:59:59+01:00 Monday   (week 2)
        let a = TimestampSecondArray::from(vec![0, 86400 * 4, 86400 * 4 - 1])
            .with_timezone("+01:00".to_string());
        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(2, b.value(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_hour_minute_second_dictionary_array() {
        let a = TimestampSecondArray::from(vec![
            60 * 60 * 10 + 61,
            60 * 60 * 20 + 122,
            60 * 60 * 30 + 183,
        ])
        .with_timezone("+01:00".to_string());

        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 1]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = hour_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![11, 21, 7])).unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = time_fraction_dyn(&dict, "minute", |t| t.minute() as i32).unwrap();

        let b_old = minute_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![1, 2, 3])).unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b = time_fraction_dyn(&dict, "second", |t| t.second() as i32).unwrap();

        let b_old = second_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![1, 2, 3])).unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b =
            time_fraction_dyn(&dict, "nanosecond", |t| t.nanosecond() as i32).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![0, 0, 0, 0, 0]))
                .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_year_dictionary_array() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = year_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![2018, 2019, 2019, 2018]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_quarter_month_dictionary_array() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1566275025000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = quarter_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![1, 3, 3, 1])).unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = month_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::try_new(&keys, &Int32Array::from(vec![1, 8, 8, 1])).unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_num_days_from_monday_sunday_day_doy_week_dictionary_array() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1), Some(0), None]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = num_days_from_monday_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![Some(0), Some(2), Some(2), Some(0), None]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = num_days_from_sunday_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![Some(1), Some(3), Some(3), Some(1), None]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = day_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![Some(1), Some(20), Some(20), Some(1), None]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = doy_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![Some(1), Some(51), Some(51), Some(1), None]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = week_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![Some(1), Some(8), Some(8), Some(1), None]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_date64_nanosecond() {
        // new Date(1667328721453)
        // Tue Nov 01 2022 11:52:01 GMT-0700 (Pacific Daylight Time)
        //
        // new Date(1667328721453).getMilliseconds()
        // 453

        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = nanosecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453_000_000, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();
        let b = nanosecond_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::try_new(
            &keys,
            &Int32Array::from(vec![None, Some(453_000_000)]),
        )
        .unwrap();
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }
}
