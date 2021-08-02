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
use crate::buffer::Buffer;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
/// Extracts the hours of a given temporal array as an array of integers
pub fn hour<T>(array: &PrimitiveArray<T>) -> Result<Int32Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: std::convert::From<T::Native>,
{
    let mut b = Int32Builder::new(array.len());
    match array.data_type() {
        &DataType::Time32(_) | &DataType::Time64(_) => {
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    match array.value_as_time(i) {
                        Some(time) => b.append_value(time.hour() as i32)?,
                        None => b.append_null()?,
                    };
                }
            }
        }
        &DataType::Date32 | &DataType::Date64 | &DataType::Timestamp(_, _) => {
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    match array.value_as_datetime(i) {
                        Some(dt) => b.append_value(dt.hour() as i32)?,
                        None => b.append_null()?,
                    }
                }
            }
        }
        dt => {
            return {
                Err(ArrowError::ComputeError(format!(
                    "hour does not support type {:?}",
                    dt
                )))
            }
        }
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
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    match array.value_as_datetime(i) {
                        Some(dt) => b.append_value(dt.year() as i32)?,
                        None => b.append_null()?,
                    }
                }
            }
        }
        dt => {
            return {
                Err(ArrowError::ComputeError(format!(
                    "year does not support type {:?}",
                    dt
                )))
            }
        }
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
        &DataType::Date64 | &DataType::Timestamp(_, _) => {
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    match array.value_as_datetime(i) {
                        Some(dt) => b.append_value(dt.minute() as i32)?,
                        None => b.append_null()?,
                    }
                }
            }
        }
        dt => {
            return {
                Err(ArrowError::ComputeError(format!(
                    "minute does not support type {:?}",
                    dt
                )))
            }
        }
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
        &DataType::Date64 | &DataType::Timestamp(_, _) => {
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    match array.value_as_datetime(i) {
                        Some(dt) => b.append_value(dt.second() as i32)?,
                        None => b.append_null()?,
                    }
                }
            }
        }
        dt => {
            return {
                Err(ArrowError::ComputeError(format!(
                    "second does not support type {:?}",
                    dt
                )))
            }
        }
    }

    Ok(b.finish())
}

/// Add the given `time_delta` to each time in the `date_time` array.
///
/// # Errors
/// If the arrays of different lengths.
pub fn add_time<DateTime, TimeDelta>(
    date_time: &PrimitiveArray<DateTime>,
    time_delta: &PrimitiveArray<TimeDelta>,
) -> Result<PrimitiveArray<DateTime>>
where
    DateTime: ArrowTimestampType,
    TimeDelta: ArrowTimeDeltaType,
{
    if date_time.len() != time_delta.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform add_time on arrays of different length".to_string(),
        ));
    }
    let null_bit_buffer = combine_option_bitmap(
        date_time.data_ref(),
        time_delta.data_ref(),
        date_time.len(),
    )?;

    let values = date_time
        .values()
        .iter()
        .zip(time_delta.values().iter())
        .map(|(date_time, time_delta)| {
            let date_time = DateTime::to_datetime(*date_time);
            let date_time = TimeDelta::add_time(date_time, *time_delta);
            DateTime::from_datetime(date_time)
        });
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = ArrayData::new(
        DateTime::DATA_TYPE,
        date_time.len(),
        None,
        null_bit_buffer,
        0,
        vec![buffer],
        vec![],
    );
    Ok(PrimitiveArray::<DateTime>::from(data))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime};
    use chronoutil::RelativeDuration;

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
    fn test_temporal_array_add_timestamp_s_interval_day() {
        let time1 = NaiveDateTime::from_timestamp(1612025847, 70);
        let time2 = NaiveDateTime::from_timestamp(1722015847, 7000);

        let timestamp: TimestampSecondArray =
            vec![Some(time1.timestamp()), None, Some(time2.timestamp()), None].into();
        let days5_ms700 = (5i64 << 32) + 700;
        let delta: IntervalDayTimeArray =
            vec![Some(days5_ms700), Some(days5_ms700), None, None].into();

        let actual = add_time(&timestamp, &delta).unwrap();
        let time1_plus = time1 + RelativeDuration::days(5) + Duration::milliseconds(700);
        let expected: TimestampSecondArray =
            vec![Some(time1_plus.timestamp()), None, None, None].into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_temporal_array_add_timestamp_s_interval_month() {
        let time1 = NaiveDateTime::from_timestamp(1612025847, 70);
        let time2 = NaiveDateTime::from_timestamp(1722015847, 7000);

        let timestamp: TimestampSecondArray =
            vec![Some(time1.timestamp()), None, Some(time2.timestamp()), None].into();
        let delta: IntervalYearMonthArray = vec![Some(5), Some(5), None, None].into();

        let actual = add_time(&timestamp, &delta).unwrap();
        let time1_plus = time1 + RelativeDuration::months(5);
        let expected: TimestampSecondArray =
            vec![Some(time1_plus.timestamp()), None, None, None].into();
        assert_eq!(actual, expected);
    }
}
