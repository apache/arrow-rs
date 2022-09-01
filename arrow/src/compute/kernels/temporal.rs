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

use chrono::{Datelike, Timelike};

use crate::array::as_datetime;
use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use chrono::format::strftime::StrftimeItems;
use chrono::format::{parse, Parsed};
use chrono::FixedOffset;

macro_rules! extract_component_from_array {
    ($array:ident, $builder:ident, $extract_fn:ident, $using:ident) => {
        for i in 0..$array.len() {
            if $array.is_null(i) {
                $builder.append_null();
            } else {
                match $array.$using(i) {
                    Some(dt) => $builder.append_value(dt.$extract_fn() as i32),
                    None => $builder.append_null(),
                }
            }
        }
    };
    ($array:ident, $builder:ident, $extract_fn1:ident, $extract_fn2:ident, $using:ident) => {
        for i in 0..$array.len() {
            if $array.is_null(i) {
                $builder.append_null();
            } else {
                match $array.$using(i) {
                    Some(dt) => {
                        $builder.append_value(dt.$extract_fn1().$extract_fn2() as i32);
                    }
                    None => $builder.append_null(),
                }
            }
        }
    };
    ($array:ident, $builder:ident, $extract_fn:ident, $using:ident, $tz:ident, $parsed:ident) => {
        if ($tz.starts_with('+') || $tz.starts_with('-')) && !$tz.contains(':') {
            return_compute_error_with!(
                "Invalid timezone",
                "Expected format [+-]XX:XX".to_string()
            )
        } else {
            let tz_parse_result = parse(&mut $parsed, $tz, StrftimeItems::new("%z"));
            let fixed_offset_from_parsed = match tz_parse_result {
                Ok(_) => match $parsed.to_fixed_offset() {
                    Ok(fo) => Some(fo),
                    err => return_compute_error_with!("Invalid timezone", err),
                },
                _ => None,
            };

            for i in 0..$array.len() {
                if $array.is_null(i) {
                    $builder.append_null();
                } else {
                    match $array.value_as_datetime(i) {
                        Some(utc) => {
                            let fixed_offset = match fixed_offset_from_parsed {
                                Some(fo) => fo,
                                None => match using_chrono_tz_and_utc_naive_date_time(
                                    $tz, utc,
                                ) {
                                    Some(fo) => fo,
                                    err => return_compute_error_with!(
                                        "Unable to parse timezone",
                                        err
                                    ),
                                },
                            };
                            match $array.$using(i, fixed_offset) {
                                Some(dt) => {
                                    $builder.append_value(dt.$extract_fn() as i32);
                                }
                                None => $builder.append_null(),
                            }
                        }
                        err => return_compute_error_with!(
                            "Unable to read value as datetime",
                            err
                        ),
                    }
                }
            }
        }
    };
}

macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(ArrowError::ComputeError(format!("{}: {:?}", $msg, $param))) }
    };
}

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

#[cfg(not(feature = "chrono-tz"))]
pub fn using_chrono_tz_and_utc_naive_date_time(
    _tz: &str,
    _utc: chrono::NaiveDateTime,
) -> Option<FixedOffset> {
    None
}

/// Parse the given string into a string representing fixed-offset that is correct as of the given
/// UTC NaiveDateTime.
/// Note that the offset is function of time and can vary depending on whether daylight savings is
/// in effect or not. e.g. Australia/Sydney is +10:00 or +11:00 depending on DST.
#[cfg(feature = "chrono-tz")]
pub fn using_chrono_tz_and_utc_naive_date_time(
    tz: &str,
    utc: chrono::NaiveDateTime,
) -> Option<FixedOffset> {
    use chrono::{Offset, TimeZone};
    tz.parse::<chrono_tz::Tz>()
        .map(|tz| tz.offset_from_utc_datetime(&utc).fix())
        .ok()
}

/// Extracts the hours of a given temporal array as an array of integers
pub fn hour<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            hour_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => hour_internal::<T, A>(array, dt),
    }
}

/// Extracts the hours of a given temporal array as an array of integers
fn hour_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Time32(_) | DataType::Time64(_) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, hour, |value| as_time::<T>(
                i64::from(value)
            ));
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, hour, |value| as_datetime::<T>(
                i64::from(value)
            ))
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                hour,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("hour does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the years of a given temporal array as an array of integers
pub fn year<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            year_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => year_internal::<T, A>(array, dt),
    }
}

/// Extracts the years of a given temporal array as an array of integers
fn year_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, year, |value| as_datetime::<T>(
                i64::from(value)
            ))
        }
        _t => return_compute_error_with!("year does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the quarter of a given temporal array as an array of integers
pub fn quarter<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            quarter_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => quarter_internal::<T, A>(array, dt),
    }
}

/// Extracts the quarter of a given temporal array as an array of integers
fn quarter_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, quarter, |value| as_datetime::<T>(
                i64::from(value)
            ))
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                quarter,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("quarter does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the month of a given temporal array as an array of integers
pub fn month<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            month_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => month_internal::<T, A>(array, dt),
    }
}

/// Extracts the month of a given temporal array as an array of integers
fn month_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, month, |value| as_datetime::<T>(
                i64::from(value)
            ))
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                month,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("month does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the day of week of a given temporal array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
pub fn num_days_from_monday<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            num_days_from_monday_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => num_days_from_monday_internal::<T, A>(array, dt),
    }
}

/// Extracts the day of week of a given temporal array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
fn num_days_from_monday_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, num_days_from_monday, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                num_days_from_monday,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("weekday does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the day of week of a given temporal array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
pub fn num_days_from_sunday<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            num_days_from_sunday_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => num_days_from_sunday_internal::<T, A>(array, dt),
    }
}

/// Extracts the day of week of a given temporal array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
fn num_days_from_sunday_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, num_days_from_sunday, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                num_days_from_sunday,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!(
            "num_days_from_sunday does not support",
            array.data_type()
        ),
    }

    Ok(b.finish())
}

/// Extracts the day of a given temporal array as an array of integers
pub fn day<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            day_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => day_internal::<T, A>(array, dt),
    }
}

/// Extracts the day of a given temporal array as an array of integers
fn day_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, day, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(ref tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                day,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("day does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the day of year of a given temporal array as an array of integers
/// The day of year that ranges from 1 to 366
pub fn doy<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            doy_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => doy_internal::<T, A>(array, dt),
    }
}

/// Extracts the day of year of a given temporal array as an array of integers
/// The day of year that ranges from 1 to 366
fn doy_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, ordinal, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(ref tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                ordinal,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("doy does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the minutes of a given temporal array as an array of integers
pub fn minute<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            minute_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => minute_internal::<T, A>(array, dt),
    }
}

/// Extracts the minutes of a given temporal array as an array of integers
fn minute_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, minute, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                minute,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("minute does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the week of a given temporal array as an array of integers
pub fn week<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            week_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => week_internal::<T, A>(array, dt),
    }
}

/// Extracts the week of a given temporal array as an array of integers
fn week_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());

    match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, iso_week, week, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        _ => return_compute_error_with!("week does not support", array.data_type()),
    }

    Ok(b.finish())
}

/// Extracts the seconds of a given temporal array as an array of integers
pub fn second<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    match array.data_type().clone() {
        DataType::Dictionary(_, value_type) => {
            second_internal::<T, A>(array, value_type.as_ref().clone())
        }
        dt => second_internal::<T, A>(array, dt),
    }
}

/// Extracts the seconds of a given temporal array as an array of integers
fn second_internal<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
    dt: DataType,
) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::with_capacity(array.len());
    match dt {
        DataType::Date64 | DataType::Timestamp(_, None) => {
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(iter, b, second, |value| {
                as_datetime::<T>(i64::from(value))
            })
        }
        DataType::Timestamp(_, Some(tz)) => {
            let mut scratch = Parsed::new();
            let iter = ArrayIter::new(array);
            _extract_component_from_array!(
                iter,
                b,
                second,
                |value, tz| as_datetime::<T>(i64::from(value))
                    .map(|datetime| datetime + tz),
                tz,
                scratch,
                |value| as_datetime::<T>(i64::from(value))
            )
        }
        _ => return_compute_error_with!("second does not support", array.data_type()),
    }

    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "chrono-tz")]
    use chrono::NaiveDate;

    #[test]
    fn test_temporal_array_date64_hour() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = hour::<Date64Type, _>(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_hour() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15148)].into();

        let b = hour::<Date32Type, _>(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(0, b.value(2));
    }

    #[test]
    fn test_temporal_array_time32_second_hour() {
        let a: PrimitiveArray<Time32SecondType> = vec![37800, 86339].into();

        let b = hour::<Time32SecondType, _>(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_time64_micro_hour() {
        let a: PrimitiveArray<Time64MicrosecondType> =
            vec![37800000000, 86339000000].into();

        let b = hour::<Time64MicrosecondType, _>(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_hour() {
        let a: TimestampMicrosecondArray = vec![37800000000, 86339000000].into();

        let b = hour::<TimestampMicrosecondType, _>(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_date64_year() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = year::<Date64Type, _>(&a).unwrap();
        assert_eq!(2018, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2019, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_year() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15448)].into();

        let b = year::<Date32Type, _>(&a).unwrap();
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

        let b = quarter::<Date64Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(3, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_quarter() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(300)].into();

        let b = quarter::<Date32Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_quarter_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a =
            TimestampSecondArray::from_vec(vec![86400 * 90], Some("+00:00".to_string()));
        let b = quarter::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a =
            TimestampSecondArray::from_vec(vec![86400 * 90], Some("-10:00".to_string()));
        let b = quarter::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_month() {
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = month::<Date64Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_month() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(31)].into();

        let b = month::<Date32Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_month_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a =
            TimestampSecondArray::from_vec(vec![86400 * 31], Some("+00:00".to_string()));
        let b = month::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a =
            TimestampSecondArray::from_vec(vec![86400 * 31], Some("-10:00".to_string()));
        let b = month::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_day_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from_vec(vec![86400], Some("+00:00".to_string()));
        let b = day::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from_vec(vec![86400], Some("-10:00".to_string()));
        let b = day::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_weekday() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = num_days_from_monday::<Date64Type, _>(&a).unwrap();
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

        let b = num_days_from_sunday::<Date64Type, _>(&a).unwrap();
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

        let b = day::<Date64Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(20, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_day() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(31)].into();

        let b = day::<Date32Type, _>(&a).unwrap();
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

        let b = doy::<Date64Type, _>(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(1, b.value(1));
        assert!(!b.is_valid(2));
        assert_eq!(51, b.value(3));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_year() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = year::<TimestampMicrosecondType, _>(&a).unwrap();
        assert_eq!(2021, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2024, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_minute() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = minute::<Date64Type, _>(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(23, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_minute() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = minute::<TimestampMicrosecondType, _>(&a).unwrap();
        assert_eq!(57, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(44, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_week() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(7)].into();

        let b = week::<Date32Type, _>(&a).unwrap();
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

        let b = week::<Date64Type, _>(&a).unwrap();
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

        let b = week::<TimestampMicrosecondType, _>(&a).unwrap();
        assert_eq!(4, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(30, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_second() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = second::<Date64Type, _>(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(45, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_second() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = second::<TimestampMicrosecondType, _>(&a).unwrap();
        assert_eq!(27, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(7, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_second_with_timezone() {
        let a = TimestampSecondArray::from_vec(vec![10, 20], Some("+00:00".to_string()));
        let b = second::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(20, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_timezone() {
        let a = TimestampSecondArray::from_vec(vec![0, 60], Some("+00:50".to_string()));
        let b = minute::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(50, b.value(0));
        assert_eq!(51, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_negative_timezone() {
        let a = TimestampSecondArray::from_vec(vec![60 * 55], Some("-00:50".to_string()));
        let b = minute::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(5, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone() {
        let a = TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("+01:00".to_string()),
        );
        let b = hour::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_colon() {
        let a =
            TimestampSecondArray::from_vec(vec![60 * 60 * 10], Some("+0100".to_string()));
        assert!(matches!(
            hour::<TimestampSecondType, _>(&a),
            Err(ArrowError::ComputeError(_))
        ))
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_initial_sign() {
        let a =
            TimestampSecondArray::from_vec(vec![60 * 60 * 10], Some("0100".to_string()));
        assert!(matches!(
            hour::<TimestampSecondType, _>(&a),
            Err(ArrowError::ComputeError(_))
        ))
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_with_only_colon() {
        let a =
            TimestampSecondArray::from_vec(vec![60 * 60 * 10], Some("01:00".to_string()));
        assert!(matches!(
            hour::<TimestampSecondType, _>(&a),
            Err(ArrowError::ComputeError(_))
        ))
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        let a = TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("Asia/Kolkata".to_string()),
        );
        let b = hour::<TimestampSecondType, _>(&a).unwrap();
        assert_eq!(15, b.value(0));
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_temporal_array_timestamp_hour_with_dst_timezone_using_chrono_tz() {
        //
        // 1635577147 converts to 2021-10-30 17:59:07 in time zone Australia/Sydney (AEDT)
        // The offset (difference to UTC) is +11:00. Note that daylight savings is in effect on 2021-10-30.
        // When daylight savings is not in effect, Australia/Sydney has an offset difference of +10:00.

        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(1635577147000)],
            Some("Australia/Sydney".to_string()),
        );
        let b = hour::<TimestampMillisecondType, _>(&a).unwrap();
        assert_eq!(17, b.value(0));
    }

    #[cfg(not(feature = "chrono-tz"))]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        let a = TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("Asia/Kolkatta".to_string()),
        );
        assert!(matches!(
            hour::<TimestampSecondType, _>(&a),
            Err(ArrowError::ComputeError(_))
        ))
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_using_chrono_tz_and_utc_naive_date_time() {
        let sydney_tz = "Australia/Sydney".to_string();
        let sydney_offset_without_dst = FixedOffset::east(10 * 60 * 60);
        let sydney_offset_with_dst = FixedOffset::east(11 * 60 * 60);
        // Daylight savings ends
        // When local daylight time was about to reach
        // Sunday, 4 April 2021, 3:00:00 am clocks were turned backward 1 hour to
        // Sunday, 4 April 2021, 2:00:00 am local standard time instead.

        // Daylight savings starts
        // When local standard time was about to reach
        // Sunday, 3 October 2021, 2:00:00 am clocks were turned forward 1 hour to
        // Sunday, 3 October 2021, 3:00:00 am local daylight time instead.

        // Sydney 2021-04-04T02:30:00+11:00 is 2021-04-03T15:30:00Z
        let utc_just_before_sydney_dst_ends =
            NaiveDate::from_ymd(2021, 4, 3).and_hms_nano(15, 30, 0, 0);
        assert_eq!(
            using_chrono_tz_and_utc_naive_date_time(
                &sydney_tz,
                utc_just_before_sydney_dst_ends
            ),
            Some(sydney_offset_with_dst)
        );
        // Sydney 2021-04-04T02:30:00+10:00 is 2021-04-03T16:30:00Z
        let utc_just_after_sydney_dst_ends =
            NaiveDate::from_ymd(2021, 4, 3).and_hms_nano(16, 30, 0, 0);
        assert_eq!(
            using_chrono_tz_and_utc_naive_date_time(
                &sydney_tz,
                utc_just_after_sydney_dst_ends
            ),
            Some(sydney_offset_without_dst)
        );
        // Sydney 2021-10-03T01:30:00+10:00 is 2021-10-02T15:30:00Z
        let utc_just_before_sydney_dst_starts =
            NaiveDate::from_ymd(2021, 10, 2).and_hms_nano(15, 30, 0, 0);
        assert_eq!(
            using_chrono_tz_and_utc_naive_date_time(
                &sydney_tz,
                utc_just_before_sydney_dst_starts
            ),
            Some(sydney_offset_without_dst)
        );
        // Sydney 2021-04-04T03:30:00+11:00 is 2021-10-02T16:30:00Z
        let utc_just_after_sydney_dst_starts =
            NaiveDate::from_ymd(2022, 10, 2).and_hms_nano(16, 30, 0, 0);
        assert_eq!(
            using_chrono_tz_and_utc_naive_date_time(
                &sydney_tz,
                utc_just_after_sydney_dst_starts
            ),
            Some(sydney_offset_with_dst)
        );
    }

    #[test]
    fn test_hour_minute_second_dictionary_array() {
        let a = TimestampSecondArray::from_vec(
            vec![60 * 60 * 10 + 61, 60 * 60 * 20 + 122, 60 * 60 * 30 + 183],
            Some("+01:00".to_string()),
        );

        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 1]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = hour::<TimestampSecondType, _>(
            dict.downcast_dict::<TimestampSecondArray>().unwrap(),
        )
        .unwrap();

        let expected = Int32Array::from(vec![11, 11, 21, 7, 21]);
        assert_eq!(expected, b);

        let b = minute::<TimestampSecondType, _>(
            dict.downcast_dict::<TimestampSecondArray>().unwrap(),
        )
        .unwrap();

        let expected = Int32Array::from(vec![1, 1, 2, 3, 2]);
        assert_eq!(expected, b);

        let b = second::<TimestampSecondType, _>(
            dict.downcast_dict::<TimestampSecondArray>().unwrap(),
        )
        .unwrap();

        let expected = Int32Array::from(vec![1, 1, 2, 3, 2]);
        assert_eq!(expected, b);
    }

    #[test]
    fn test_year_dictionary_array() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b =
            year::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap()).unwrap();

        let expected = Int32Array::from(vec![2018, 2019, 2019, 2018]);
        assert_eq!(expected, b);
    }

    #[test]
    fn test_quarter_month_dictionary_array() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1566275025000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = quarter::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap())
            .unwrap();

        let expected = Int32Array::from(vec![1, 3, 3, 1]);
        assert_eq!(expected, b);

        let b =
            month::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap()).unwrap();

        let expected = Int32Array::from(vec![1, 8, 8, 1]);
        assert_eq!(expected, b);
    }

    #[test]
    fn test_num_days_from_monday_sunday_day_doy_week_dictionary_array() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1), Some(0), None]);
        let dict = DictionaryArray::try_new(&keys, &a).unwrap();

        let b = num_days_from_monday::<Date64Type, _>(
            dict.downcast_dict::<Date64Array>().unwrap(),
        )
        .unwrap();
        let expected = Int32Array::from(vec![Some(0), Some(2), Some(2), Some(0), None]);
        assert_eq!(expected, b);

        let b = num_days_from_sunday::<Date64Type, _>(
            dict.downcast_dict::<Date64Array>().unwrap(),
        )
        .unwrap();
        let expected = Int32Array::from(vec![Some(1), Some(3), Some(3), Some(1), None]);
        assert_eq!(expected, b);

        let b =
            day::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap()).unwrap();
        let expected = Int32Array::from(vec![Some(1), Some(20), Some(20), Some(1), None]);
        assert_eq!(expected, b);

        let b =
            doy::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap()).unwrap();
        let expected = Int32Array::from(vec![Some(1), Some(51), Some(51), Some(1), None]);
        assert_eq!(expected, b);

        let b =
            week::<Date64Type, _>(dict.downcast_dict::<Date64Array>().unwrap()).unwrap();
        let expected = Int32Array::from(vec![Some(1), Some(8), Some(8), Some(1), None]);
        assert_eq!(expected, b);
    }
}
