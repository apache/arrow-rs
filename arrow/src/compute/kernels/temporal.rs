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
            match parse(&mut $parsed, $tz, StrftimeItems::new("%z")) {
                Ok(_) => match $parsed.to_fixed_offset() {
                    Ok(fixed_offset) => {
                        for i in 0..$array.len() {
                            if $array.is_null(i) {
                                $builder.append_null()?;
                            } else {
                                match $array.$using(i, fixed_offset) {
                                    Some(dt) => {
                                        $builder.append_value(dt.$extract_fn() as i32)?
                                    }
                                    None => $builder.append_null()?,
                                }
                            }
                        }
                    }
                    err => return_compute_error_with!("Invalid timezone", err),
                },
                err => return_compute_error_with!("Unable to parse timezone", err),
            }
        }
    };
}

macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(ArrowError::ComputeError(format!("{}: {:?}", $msg, $param))) }
    };
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
}
