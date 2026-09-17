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

//! Cast support for temporal arrays: timestamps, dates, durations,
//! intervals, and timezones.

use arrow_array::{cast::*, temporal_conversions::*, timezone::Tz, types::*, *};
use arrow_buffer::IntervalMonthDayNano;
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{NaiveTime, Offset, TimeZone};
use std::sync::Arc;

use super::CastOptions;

/// Cast the array from interval year month to month day nano
pub(crate) fn cast_interval_year_month_to_interval_month_day_nano(
    array: &dyn Array,
    _cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalYearMonthType>();

    Ok(Arc::new(array.unary::<_, IntervalMonthDayNanoType>(|v| {
        let months = IntervalYearMonthType::to_months(v);
        IntervalMonthDayNanoType::make_value(months, 0, 0)
    })))
}

/// Cast the array from interval day time to month day nano
pub(crate) fn cast_interval_day_time_to_interval_month_day_nano(
    array: &dyn Array,
    _cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalDayTimeType>();
    let mul = 1_000_000;

    Ok(Arc::new(array.unary::<_, IntervalMonthDayNanoType>(|v| {
        let (days, ms) = IntervalDayTimeType::to_parts(v);
        IntervalMonthDayNanoType::make_value(0, days, ms as i64 * mul)
    })))
}

/// Cast the array from interval to duration
pub(crate) fn cast_month_day_nano_to_duration<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalMonthDayNanoType>();
    let scale = match D::DATA_TYPE {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array.iter().map(|v| {
            let v = v?;
            (v.days == 0 && v.months == 0).then_some(v.nanoseconds / scale)
        });
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| match v.days == 0 && v.months == 0 {
                    true => Ok((v.nanoseconds) / scale),
                    _ => Err(ArrowError::ComputeError(
                        "Cannot convert interval containing non-zero months or days to duration"
                            .to_string(),
                    )),
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(vec.iter())
        }))
    }
}

/// Cast the array from duration and interval
pub(crate) fn cast_duration_to_interval<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<D>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast duration to DurationArray of expected type"
                    .to_string(),
            )
        })?;

    let scale = match array.data_type() {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array.iter().map(|v| {
            v?.checked_mul(scale)
                .map(|v| IntervalMonthDayNano::new(0, 0, v))
        });
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| {
                    if let Ok(v) = v.mul_checked(scale) {
                        Ok(IntervalMonthDayNano::new(0, 0, v))
                    } else {
                        Err(ArrowError::ComputeError(format!(
                            "Cannot cast to {:?}. Overflowing on {:?}",
                            IntervalMonthDayNanoType::DATA_TYPE,
                            v
                        )))
                    }
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(vec.iter())
        }))
    }
}

pub(crate) fn make_timestamp_array(
    array: &PrimitiveArray<Int64Type>,
    unit: TimeUnit,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(
            array
                .reinterpret_cast::<TimestampSecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Millisecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMillisecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Microsecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Nanosecond => Arc::new(
            array
                .reinterpret_cast::<TimestampNanosecondType>()
                .with_timezone_opt(tz),
        ),
    }
}

pub(crate) fn make_duration_array(array: &PrimitiveArray<Int64Type>, unit: TimeUnit) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(array.reinterpret_cast::<DurationSecondType>()),
        TimeUnit::Millisecond => Arc::new(array.reinterpret_cast::<DurationMillisecondType>()),
        TimeUnit::Microsecond => Arc::new(array.reinterpret_cast::<DurationMicrosecondType>()),
        TimeUnit::Nanosecond => Arc::new(array.reinterpret_cast::<DurationNanosecondType>()),
    }
}

pub(crate) fn as_time_res_with_timezone<T: ArrowPrimitiveType>(
    v: i64,
    tz: Option<Tz>,
) -> Result<NaiveTime, ArrowError> {
    let time = match tz {
        Some(tz) => as_datetime_with_timezone::<T>(v, tz).map(|d| d.time()),
        None => as_datetime::<T>(v).map(|d| d.time()),
    };

    time.ok_or_else(|| {
        ArrowError::CastError(format!(
            "Failed to create naive time with {} {}",
            std::any::type_name::<T>(),
            v
        ))
    })
}

pub(crate) fn timestamp_to_date32<T: ArrowTimestampType>(
    array: &PrimitiveArray<T>,
) -> Result<ArrayRef, ArrowError> {
    let err = |x: i64| {
        ArrowError::CastError(format!(
            "Cannot convert {} {x} to datetime",
            std::any::type_name::<T>()
        ))
    };

    let array: Date32Array = match array.timezone() {
        Some(tz) => {
            let tz: Tz = tz.parse()?;
            array.try_unary(|x| {
                as_datetime_with_timezone::<T>(x, tz)
                    .ok_or_else(|| err(x))
                    .map(|d| Date32Type::from_naive_date(d.date_naive()))
            })?
        }
        None => array.try_unary(|x| {
            as_datetime::<T>(x)
                .ok_or_else(|| err(x))
                .map(|d| Date32Type::from_naive_date(d.date()))
        })?,
    };
    Ok(Arc::new(array))
}

pub(crate) fn adjust_timestamp_to_timezone<T: ArrowTimestampType>(
    array: PrimitiveArray<Int64Type>,
    to_tz: &Tz,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<Int64Type>, ArrowError> {
    let adjust = |o| {
        let local = as_datetime::<T>(o)?;
        let offset = to_tz.offset_from_local_datetime(&local).single()?;
        T::from_naive_datetime(local - offset.fix(), None)
    };
    let adjusted = if cast_options.safe {
        array.unary_opt::<_, Int64Type>(adjust)
    } else {
        array.try_unary::<_, Int64Type, _>(|o| {
            adjust(o).ok_or_else(|| {
                ArrowError::CastError("Cannot cast timezone to different timezone".to_string())
            })
        })?
    };
    Ok(adjusted)
}
