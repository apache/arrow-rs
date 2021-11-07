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
                $builder.append_null()?;
            } else {
                match $array.$using(i) {
                    Some(dt) => $builder.append_value(dt.$extract_fn() as i32)?,
                    None => $builder.append_null()?,
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
            for i in 0..$array.len() {
                if $array.is_null(i) {
                    $builder.append_null()?;
                } else {
                    match $array.value_as_datetime(i) {
                        Some(utc) => {
                            let fixed_offset = match parse(
                                &mut $parsed,
                                $tz,
                                StrftimeItems::new("%z"),
                            ) {
                                Ok(_) => match $parsed.to_fixed_offset() {
                                    Ok(fo) => fo,
                                    err => return_compute_error_with!(
                                        "Invalid timezone",
                                        err
                                    ),
                                },
                                _ => match using_chrono_tz_and_utc_naive_date_time(
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
                                    $builder.append_value(dt.$extract_fn() as i32)?
                                }
                                None => $builder.append_null()?,
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
pub fn hour<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::new(array.len());
    match array.data_type() {
        &DataType::Time32(_) | &DataType::Time64(_) => {
            extract_component_from_array!(array, b, hour, value_as_time)
        }
        &DataType::Date32 | &DataType::Date64 | &DataType::Timestamp(_, None) => {
            extract_component_from_array!(array, b, hour, value_as_datetime)
        }
        &DataType::Timestamp(_, Some(ref tz)) => {
            let mut scratch = Parsed::new();
            extract_component_from_array!(
                array,
                b,
                hour,
                value_as_datetime_with_tz,
                tz,
                scratch
            )
        }
        dt => return_compute_error_with!("hour does not support", dt),
    }

    Ok(b.finish())
}

/// Extracts the years of a given temporal array as an array of integers
pub fn year<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::new(array.len());
    match array.data_type() {
        &DataType::Date32 | &DataType::Date64 | &DataType::Timestamp(_, _) => {
            extract_component_from_array!(array, b, year, value_as_datetime)
        }
        dt => return_compute_error_with!("year does not support", dt),
    }

    Ok(b.finish())
}

/// Extracts the minutes of a given temporal array as an array of integers
pub fn minute<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::new(array.len());
    match array.data_type() {
        &DataType::Date64 | &DataType::Timestamp(_, None) => {
            extract_component_from_array!(array, b, minute, value_as_datetime)
        }
        &DataType::Timestamp(_, Some(ref tz)) => {
            let mut scratch = Parsed::new();
            extract_component_from_array!(
                array,
                b,
                minute,
                value_as_datetime_with_tz,
                tz,
                scratch
            )
        }
        dt => return_compute_error_with!("minute does not support", dt),
    }

    Ok(b.finish())
}

/// Extracts the seconds of a given temporal array as an array of integers
pub fn second<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::new(array.len());
    match array.data_type() {
        &DataType::Date64 | &DataType::Timestamp(_, None) => {
            extract_component_from_array!(array, b, second, value_as_datetime)
        }
        &DataType::Timestamp(_, Some(ref tz)) => {
            let mut scratch = Parsed::new();
            extract_component_from_array!(
                array,
                b,
                second,
                value_as_datetime_with_tz,
                tz,
                scratch
            )
        }
        dt => return_compute_error_with!("second does not support", dt),
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
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![10, 20],
            Some("+00:00".to_string()),
        ));
        let b = second(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(20, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_timezone() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![0, 60],
            Some("+00:50".to_string()),
        ));
        let b = minute(&a).unwrap();
        assert_eq!(50, b.value(0));
        assert_eq!(51, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_negative_timezone() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 55],
            Some("-00:50".to_string()),
        ));
        let b = minute(&a).unwrap();
        assert_eq!(5, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("+01:00".to_string()),
        ));
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_colon() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("+0100".to_string()),
        ));
        assert!(matches!(hour(&a), Err(ArrowError::ComputeError(_))))
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_initial_sign() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("0100".to_string()),
        ));
        assert!(matches!(hour(&a), Err(ArrowError::ComputeError(_))))
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_with_only_colon() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("01:00".to_string()),
        ));
        assert!(matches!(hour(&a), Err(ArrowError::ComputeError(_))))
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("Asia/Kolkata".to_string()),
        ));
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

        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(1635577147000)],
            Some("Australia/Sydney".to_string()),
        );
        let b = hour(&a).unwrap();
        assert_eq!(17, b.value(0));
    }

    #[cfg(not(feature = "chrono-tz"))]
    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
        use std::sync::Arc;

        let a = Arc::new(TimestampSecondArray::from_vec(
            vec![60 * 60 * 10],
            Some("Asia/Kolkatta".to_string()),
        ));
        assert!(matches!(hour(&a), Err(ArrowError::ComputeError(_))))
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
}
