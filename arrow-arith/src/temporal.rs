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

use std::sync::Arc;

use arrow_array::cast::{downcast_array, AsArray};
use chrono::{DateTime, Datelike, NaiveDateTime, Offset, TimeZone, Timelike, Utc};

use arrow_array::temporal_conversions::{
    date32_to_datetime, date64_to_datetime, time32ms_to_time, time32s_to_time, time64ns_to_time,
    time64us_to_time, timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType, TimeUnit};

/// Valid parts to extract from date/timestamp arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    /// Quarter of the year, in range `1..=4`
    Quarter,
    /// Calendar year
    Year,
    /// Month in the year, in range `1..=12`
    Month,
    /// ISO week of the year, in range `1..=53`
    Week,
    /// Day of the month, in range `1..=31`
    Day,
    /// Day of the week, in range `0..=6`, where Sunday is 0
    DayOfWeekSunday0,
    /// Day of the week, in range `0..=6`, where Monday is 0
    DayOfWeekMonday0,
    /// Day of year, in range `1..=366`
    DayOfYear,
    /// Hour of the day, in range `0..=23`
    Hour,
    /// Minute of the hour, in range `0..=59`
    Minute,
    /// Second of the minute, in range `0..=59`
    Second,
    /// Millisecond of the second
    Millisecond,
    /// Microsecond of the second
    Microsecond,
    /// Nanosecond of the second
    Nanosecond,
}

impl std::fmt::Display for DatePart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Returns function to extract relevant [`DatePart`] from a [`NaiveDateTime`].
fn get_naive_date_time_part_extract_fn(part: DatePart) -> fn(NaiveDateTime) -> i32 {
    match part {
        DatePart::Quarter => |d| d.quarter() as i32,
        DatePart::Year => |d| d.year(),
        DatePart::Month => |d| d.month() as i32,
        DatePart::Week => |d| d.iso_week().week() as i32,
        DatePart::Day => |d| d.day() as i32,
        DatePart::DayOfWeekSunday0 => |d| d.num_days_from_sunday(),
        DatePart::DayOfWeekMonday0 => |d| d.num_days_from_monday(),
        DatePart::DayOfYear => |d| d.ordinal() as i32,
        DatePart::Hour => |d| d.hour() as i32,
        DatePart::Minute => |d| d.minute() as i32,
        DatePart::Second => |d| d.second() as i32,
        DatePart::Millisecond => |d| (d.nanosecond() / 1_000_000) as i32,
        DatePart::Microsecond => |d| (d.nanosecond() / 1_000) as i32,
        DatePart::Nanosecond => |d| (d.nanosecond()) as i32,
    }
}

/// Returns function to extract relevant [`DatePart`] from a [`DateTime`].
fn get_date_time_tz_part_extract_fn(part: DatePart) -> fn(DateTime<Tz>) -> i32 {
    match part {
        DatePart::Quarter => |d| d.quarter() as i32,
        DatePart::Year => |d| d.year(),
        DatePart::Month => |d| d.month() as i32,
        DatePart::Week => |d| d.iso_week().week() as i32,
        DatePart::Day => |d| d.day() as i32,
        DatePart::DayOfWeekSunday0 => |d| d.num_days_from_sunday(),
        DatePart::DayOfWeekMonday0 => |d| d.num_days_from_monday(),
        DatePart::DayOfYear => |d| d.ordinal() as i32,
        DatePart::Hour => |d| d.hour() as i32,
        DatePart::Minute => |d| d.minute() as i32,
        DatePart::Second => |d| d.second() as i32,
        DatePart::Millisecond => |d| (d.nanosecond() / 1_000_000) as i32,
        DatePart::Microsecond => |d| (d.nanosecond() / 1_000) as i32,
        DatePart::Nanosecond => |d| (d.nanosecond()) as i32,
    }
}

fn date_part_time32_s(
    array: &PrimitiveArray<Time32SecondType>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    match part {
        DatePart::Hour => Ok(array.unary_opt(|d| time32s_to_time(d).map(|c| c.hour() as i32))),
        // TODO expand support for Time types, see: https://github.com/apache/arrow-rs/issues/5261
        _ => return_compute_error_with!(format!("{part} does not support"), array.data_type()),
    }
}

fn date_part_time32_ms(
    array: &PrimitiveArray<Time32MillisecondType>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    match part {
        DatePart::Hour => Ok(array.unary_opt(|d| time32ms_to_time(d).map(|c| c.hour() as i32))),
        // TODO expand support for Time types, see: https://github.com/apache/arrow-rs/issues/5261
        _ => return_compute_error_with!(format!("{part} does not support"), array.data_type()),
    }
}

fn date_part_time64_us(
    array: &PrimitiveArray<Time64MicrosecondType>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    match part {
        DatePart::Hour => Ok(array.unary_opt(|d| time64us_to_time(d).map(|c| c.hour() as i32))),
        // TODO expand support for Time types, see: https://github.com/apache/arrow-rs/issues/5261
        _ => return_compute_error_with!(format!("{part} does not support"), array.data_type()),
    }
}

fn date_part_time64_ns(
    array: &PrimitiveArray<Time64NanosecondType>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    match part {
        DatePart::Hour => Ok(array.unary_opt(|d| time64ns_to_time(d).map(|c| c.hour() as i32))),
        // TODO expand support for Time types, see: https://github.com/apache/arrow-rs/issues/5261
        _ => return_compute_error_with!(format!("{part} does not support"), array.data_type()),
    }
}

fn date_part_date32(array: &PrimitiveArray<Date32Type>, part: DatePart) -> Int32Array {
    // Date32 only encodes number of days, so these will always be 0
    if let DatePart::Hour
    | DatePart::Minute
    | DatePart::Second
    | DatePart::Millisecond
    | DatePart::Microsecond
    | DatePart::Nanosecond = part
    {
        array.unary(|_| 0)
    } else {
        let map_func = get_naive_date_time_part_extract_fn(part);
        array.unary_opt(|d| date32_to_datetime(d).map(map_func))
    }
}

fn date_part_date64(array: &PrimitiveArray<Date64Type>, part: DatePart) -> Int32Array {
    let map_func = get_naive_date_time_part_extract_fn(part);
    array.unary_opt(|d| date64_to_datetime(d).map(map_func))
}

fn date_part_timestamp_s(
    array: &PrimitiveArray<TimestampSecondType>,
    part: DatePart,
) -> Int32Array {
    // TimestampSecond only encodes number of seconds, so these will always be 0
    if let DatePart::Millisecond | DatePart::Microsecond | DatePart::Nanosecond = part {
        array.unary(|_| 0)
    } else {
        let map_func = get_naive_date_time_part_extract_fn(part);
        array.unary_opt(|d| timestamp_s_to_datetime(d).map(map_func))
    }
}

fn date_part_timestamp_s_tz(
    array: &PrimitiveArray<TimestampSecondType>,
    part: DatePart,
    tz: Tz,
) -> Int32Array {
    // TimestampSecond only encodes number of seconds, so these will always be 0
    if let DatePart::Millisecond | DatePart::Microsecond | DatePart::Nanosecond = part {
        array.unary(|_| 0)
    } else {
        let map_func = get_date_time_tz_part_extract_fn(part);
        array.unary_opt(|d| {
            timestamp_s_to_datetime(d)
                .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
                .map(map_func)
        })
    }
}

fn date_part_timestamp_ms(
    array: &PrimitiveArray<TimestampMillisecondType>,
    part: DatePart,
) -> Int32Array {
    let map_func = get_naive_date_time_part_extract_fn(part);
    array.unary_opt(|d| timestamp_ms_to_datetime(d).map(map_func))
}

fn date_part_timestamp_ms_tz(
    array: &PrimitiveArray<TimestampMillisecondType>,
    part: DatePart,
    tz: Tz,
) -> Int32Array {
    let map_func = get_date_time_tz_part_extract_fn(part);
    array.unary_opt(|d| {
        timestamp_ms_to_datetime(d)
            .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
            .map(map_func)
    })
}

fn date_part_timestamp_us(
    array: &PrimitiveArray<TimestampMicrosecondType>,
    part: DatePart,
) -> Int32Array {
    let map_func = get_naive_date_time_part_extract_fn(part);
    array.unary_opt(|d| timestamp_us_to_datetime(d).map(map_func))
}

fn date_part_timestamp_us_tz(
    array: &PrimitiveArray<TimestampMicrosecondType>,
    part: DatePart,
    tz: Tz,
) -> Int32Array {
    let map_func = get_date_time_tz_part_extract_fn(part);
    array.unary_opt(|d| {
        timestamp_us_to_datetime(d)
            .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
            .map(map_func)
    })
}

fn date_part_timestamp_ns(
    array: &PrimitiveArray<TimestampNanosecondType>,
    part: DatePart,
) -> Int32Array {
    let map_func = get_naive_date_time_part_extract_fn(part);
    array.unary_opt(|d| timestamp_ns_to_datetime(d).map(map_func))
}

fn date_part_timestamp_ns_tz(
    array: &PrimitiveArray<TimestampNanosecondType>,
    part: DatePart,
    tz: Tz,
) -> Int32Array {
    let map_func = get_date_time_tz_part_extract_fn(part);
    array.unary_opt(|d| {
        timestamp_ns_to_datetime(d)
            .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
            .map(map_func)
    })
}

/// Given array, return new array with the extracted [`DatePart`].
///
/// Returns an [`Int32Array`] unless input was a dictionary type, in which case returns
/// the dictionary but with this function applied onto its values.
///
/// Returns error if attempting to extract date part from unsupported type (i.e. non-date/timestamp types).
pub fn date_part(array: &dyn Array, part: DatePart) -> Result<ArrayRef, ArrowError> {
    downcast_primitive_array!(
        array => {
            let array = primitive_array_date_part(array, part)?;
            let array = Arc::new(array) as ArrayRef;
            Ok(array)
        }
        DataType::Dictionary(_, _) => {
            let array = array.as_any_dictionary();
            let values = date_part(array.values(), part)?;
            let new_array = array.with_values(Arc::new(values) as ArrayRef);
            Ok(new_array)
        }
        t => return_compute_error_with!(format!("{part} does not support"), t),
    )
}

/// Dispatch to specialized function depending on the array type. Since we don't need to consider
/// dictionary arrays here, can strictly control return type to be [`Int32Array`].
fn primitive_array_date_part<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    match array.data_type() {
        DataType::Date32 => {
            let array = downcast_array::<Date32Array>(array);
            Ok(date_part_date32(&array, part))
        }
        DataType::Date64 => {
            let array = downcast_array::<Date64Array>(array);
            Ok(date_part_date64(&array, part))
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            let array = downcast_array::<TimestampSecondArray>(array);
            Ok(date_part_timestamp_s(&array, part))
        }
        DataType::Timestamp(TimeUnit::Second, Some(tz)) => {
            let array = downcast_array::<TimestampSecondArray>(array);
            let tz = tz.parse()?;
            Ok(date_part_timestamp_s_tz(&array, part, tz))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = downcast_array::<TimestampMillisecondArray>(array);
            Ok(date_part_timestamp_ms(&array, part))
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => {
            let array = downcast_array::<TimestampMillisecondArray>(array);
            let tz = tz.parse()?;
            Ok(date_part_timestamp_ms_tz(&array, part, tz))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = downcast_array::<TimestampMicrosecondArray>(array);
            Ok(date_part_timestamp_us(&array, part))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            let array = downcast_array::<TimestampMicrosecondArray>(array);
            let tz = tz.parse()?;
            Ok(date_part_timestamp_us_tz(&array, part, tz))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = downcast_array::<TimestampNanosecondArray>(array);
            Ok(date_part_timestamp_ns(&array, part))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => {
            let array = downcast_array::<TimestampNanosecondArray>(array);
            let tz = tz.parse()?;
            Ok(date_part_timestamp_ns_tz(&array, part, tz))
        }
        DataType::Time32(TimeUnit::Second) => {
            let array = downcast_array::<Time32SecondArray>(array);
            Ok(date_part_time32_s(&array, part)?)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let array = downcast_array::<Time32MillisecondArray>(array);
            Ok(date_part_time32_ms(&array, part)?)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let array = downcast_array::<Time64MicrosecondArray>(array);
            Ok(date_part_time64_us(&array, part)?)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = downcast_array::<Time64NanosecondArray>(array);
            Ok(date_part_time64_ns(&array, part)?)
        }
        // TODO: support Interval
        // DataType::Interval(_) => todo!(),
        _ => return_compute_error_with!(format!("{part} does not support"), array.data_type()),
    }
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
    let tz: Tz = tz.parse().ok()?;
    Some(tz.offset_from_utc_datetime(&utc).fix())
}

/// Extracts the hours of a given array as an array of integers within
/// the range of [0, 23]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn hour_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Hour)
}

/// Extracts the hours of a given temporal primitive array as an array of integers within
/// the range of [0, 23].
pub fn hour<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Hour)
}

/// Extracts the years of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn year_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Year)
}

/// Extracts the years of a given temporal primitive array as an array of integers
pub fn year<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Year)
}

/// Extracts the quarter of a given temporal array as an array of integersa within
/// the range of [1, 4]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn quarter_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Quarter)
}

/// Extracts the quarter of a given temporal primitive array as an array of integers within
/// the range of [1, 4].
pub fn quarter<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Quarter)
}

/// Extracts the month of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn month_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Month)
}

/// Extracts the month of a given temporal primitive array as an array of integers within
/// the range of [1, 12].
pub fn month<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Month)
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
pub fn num_days_from_monday_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfWeekMonday0)
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
pub fn num_days_from_monday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::DayOfWeekMonday0)
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
pub fn num_days_from_sunday_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfWeekSunday0)
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
pub fn num_days_from_sunday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::DayOfWeekSunday0)
}

/// Extracts the day of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn day_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Day)
}

/// Extracts the day of a given temporal primitive array as an array of integers
pub fn day<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Day)
}

/// Extracts the day of year of a given temporal array as an array of integers
/// The day of year that ranges from 1 to 366.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn doy_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfYear)
}

/// Extracts the day of year of a given temporal primitive array as an array of integers
/// The day of year that ranges from 1 to 366
pub fn doy<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::DayOfYear)
}

/// Extracts the minutes of a given temporal primitive array as an array of integers
pub fn minute<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Minute)
}

/// Extracts the week of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn week_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Week)
}

/// Extracts the week of a given temporal primitive array as an array of integers
pub fn week<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Week)
}

/// Extracts the seconds of a given temporal primitive array as an array of integers
pub fn second<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Second)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers
pub fn nanosecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Nanosecond)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn nanosecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Nanosecond)
}

/// Extracts the microseconds of a given temporal primitive array as an array of integers
pub fn microsecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Microsecond)
}

/// Extracts the microseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn microsecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Microsecond)
}

/// Extracts the milliseconds of a given temporal primitive array as an array of integers
pub fn millisecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    primitive_array_date_part(array, DatePart::Millisecond)
}
/// Extracts the milliseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn millisecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Millisecond)
}

/// Extracts the minutes of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn minute_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Minute)
}

/// Extracts the seconds of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
pub fn second_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Second)
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
        let a: PrimitiveArray<Time64MicrosecondType> = vec![37800000000, 86339000000].into();

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
        let a = TimestampSecondArray::from(vec![86400 * 90]).with_timezone("+00:00".to_string());
        let b = quarter(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 90]).with_timezone("-10:00".to_string());
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
        let a = TimestampSecondArray::from(vec![86400 * 31]).with_timezone("+00:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 31]).with_timezone("-10:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_day_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400]).with_timezone("+00:00".to_string());
        let b = day(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400]).with_timezone("-10:00".to_string());
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
        let a = TimestampSecondArray::from(vec![10, 20]).with_timezone("+00:00".to_string());
        let b = second(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(20, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_timezone() {
        let a = TimestampSecondArray::from(vec![0, 60]).with_timezone("+00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(50, b.value(0));
        assert_eq!(51, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_negative_timezone() {
        let a = TimestampSecondArray::from(vec![60 * 55]).with_timezone("-00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(5, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+01:00".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+0100".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_minutes() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+01".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_initial_sign() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("0100".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_with_only_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("01:00".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
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
        let dict = DictionaryArray::try_new(keys.clone(), Arc::new(a)).unwrap();

        let b = hour_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![11, 21, 7])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = date_part(&dict, DatePart::Minute).unwrap();

        let b_old = minute_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 2, 3])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b = date_part(&dict, DatePart::Second).unwrap();

        let b_old = second_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 2, 3])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b = date_part(&dict, DatePart::Nanosecond).unwrap();

        let expected_dict =
            DictionaryArray::new(keys, Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_year_dictionary_array() {
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = year_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::new(
            keys,
            Arc::new(Int32Array::from(vec![2018, 2019, 2019, 2018])),
        );
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_quarter_month_dictionary_array() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1566275025000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = quarter_dyn(&dict).unwrap();

        let expected =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 3, 3, 1])));
        assert_eq!(b.as_ref(), &expected);

        let b = month_dyn(&dict).unwrap();

        let expected = DictionaryArray::new(keys, Arc::new(Int32Array::from(vec![1, 8, 8, 1])));
        assert_eq!(b.as_ref(), &expected);
    }

    #[test]
    fn test_num_days_from_monday_sunday_day_doy_week_dictionary_array() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1), Some(0), None]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = num_days_from_monday_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(0), Some(2), Some(2), Some(0), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = num_days_from_sunday_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(3), Some(3), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = day_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(20), Some(20), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = doy_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(51), Some(51), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = week_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(8), Some(8), Some(1), None]);
        let expected = DictionaryArray::new(keys, Arc::new(a));
        assert_eq!(b.as_ref(), &expected);
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
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = nanosecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453_000_000)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_date64_microsecond() {
        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = microsecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453_000, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = microsecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453_000)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_date64_millisecond() {
        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = millisecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = millisecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }
}
