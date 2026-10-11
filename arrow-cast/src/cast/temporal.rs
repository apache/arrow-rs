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
use chrono::{FixedOffset, LocalResult, NaiveDateTime, NaiveTime, Offset, TimeDelta, TimeZone};
use std::sync::Arc;

use num_traits::AsPrimitive;

use super::{CastOptions, time_unit_multiple, units_per_day};

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

pub(crate) fn cast_timestamp_to_time<T: ArrowTimestampType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<T>();
    match to_type {
        DataType::Time32(TimeUnit::Second) => {
            timestamp_to_time::<T, Time32SecondType>(array, cast_options)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            timestamp_to_time::<T, Time32MillisecondType>(array, cast_options)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            timestamp_to_time::<T, Time64MicrosecondType>(array, cast_options)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            timestamp_to_time::<T, Time64NanosecondType>(array, cast_options)
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Casting from {} to {to_type} not supported",
            array.data_type()
        ))),
    }
}

trait TimeType: ArrowTemporalType {
    /// Number of units in one second.
    const UNIT_MULTIPLE: i64;

    fn from_naive_time(time: NaiveTime) -> Self::Native;
}

impl TimeType for Time32SecondType {
    const UNIT_MULTIPLE: i64 = 1;

    fn from_naive_time(time: NaiveTime) -> Self::Native {
        time_to_time32s(time)
    }
}

impl TimeType for Time32MillisecondType {
    const UNIT_MULTIPLE: i64 = MILLISECONDS;

    fn from_naive_time(time: NaiveTime) -> Self::Native {
        time_to_time32ms(time)
    }
}

impl TimeType for Time64MicrosecondType {
    const UNIT_MULTIPLE: i64 = MICROSECONDS;

    fn from_naive_time(time: NaiveTime) -> Self::Native {
        time_to_time64us(time)
    }
}

impl TimeType for Time64NanosecondType {
    const UNIT_MULTIPLE: i64 = NANOSECONDS;

    fn from_naive_time(time: NaiveTime) -> Self::Native {
        time_to_time64ns(time)
    }
}

fn timestamp_to_time<T, O>(
    array: &PrimitiveArray<T>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowTimestampType,
    O: TimeType,
    i64: AsPrimitive<O::Native>,
{
    // A time within one day fits its Time32 or Time64 representation.
    let array = match array.timezone() {
        Some(tz) => {
            let tz: Tz = tz.parse()?;
            let time = |v: i64| {
                as_datetime_with_timezone::<T>(v, tz).map(|d| O::from_naive_time(d.time()))
            };
            if cast_options.safe {
                array.unary_opt::<_, O>(time)
            } else {
                array.try_unary::<_, O, _>(|v| {
                    time(v).ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Failed to create naive time with {} {}",
                            std::any::type_name::<T>(),
                            v
                        ))
                    })
                })?
            }
        }
        None => array.unary::<_, O>(|v| {
            // The remainder within a day is the time of day; `rem_euclid` keeps it
            // nonnegative for timestamps before the epoch. The units are constants,
            // so the branch and the divisions fold at compile time.
            let from = time_unit_multiple(&T::UNIT);
            let time = v.rem_euclid(SECONDS_IN_DAY * from);
            let time = if from >= O::UNIT_MULTIPLE {
                time / (from / O::UNIT_MULTIPLE)
            } else {
                time * (O::UNIT_MULTIPLE / from)
            };
            time.as_()
        }),
    };
    Ok(Arc::new(array))
}

pub(crate) fn timestamp_to_date32<T: ArrowTimestampType>(
    array: &PrimitiveArray<T>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let err = |x: i64| {
        ArrowError::CastError(format!(
            "Cannot convert {} {x} to Date32",
            std::any::type_name::<T>()
        ))
    };

    let array: Date32Array = match array.timezone() {
        Some(tz) => {
            let tz: Tz = tz.parse()?;
            let date = |x: i64| {
                as_datetime_with_timezone::<T>(x, tz)
                    .map(|d| Date32Type::from_naive_date(d.date_naive()))
            };
            if cast_options.safe {
                array.unary_opt(date)
            } else {
                array.try_unary(|x| date(x).ok_or_else(|| err(x)))?
            }
        }
        None => {
            // Date32 stores days since the epoch. Round down so that a timestamp
            // just before the epoch belongs to the preceding day.
            let days = |x: i64| x.div_euclid(units_per_day::<T>());
            let all_in_range = match T::UNIT {
                // Every microsecond or nanosecond timestamp lies within the Date32 range.
                TimeUnit::Microsecond | TimeUnit::Nanosecond => true,
                // A branch-free scan lets the common case skip the per-value check.
                _ => {
                    let day = units_per_day::<T>();
                    let (lo, hi) = (i32::MIN as i64 * day, (i32::MAX as i64 + 1) * day);
                    array
                        .values()
                        .iter()
                        .fold(true, |ok, &x| ok & (lo <= x) & (x < hi))
                }
            };
            if all_in_range {
                array.unary(|x| days(x) as i32)
            } else if cast_options.safe {
                array.unary_opt(|x| i32::try_from(days(x)).ok())
            } else {
                array.try_unary(|x| i32::try_from(days(x)).map_err(|_| err(x)))?
            }
        }
    };
    Ok(Arc::new(array))
}

/// Returns the offset to use when interpreting `local` as a wall clock reading
/// in `tz`, or `None` if it cannot be resolved.
///
/// `None` is not expected in practice. With the current timezone database no
/// reading reaches it, because every ambiguous or nonexistent reading resolves
/// as described below. The `None` path is a safeguard against a future
/// timezone database that breaks the assumptions of the gap handling. Callers
/// then apply their usual error or null handling.
///
/// In an IANA timezone a wall clock reading does not always identify a unique
/// instant, and this function picks one following the same rules as PostgreSQL
/// and DuckDB:
///
/// * **Ambiguous** -- when the clocks go back ("fall back") the same reading
///   occurs twice. The *later* instant is chosen, i.e. the offset in effect
///   after the transition. For example `2024-11-03T01:30:00` in
///   `America/New_York` is read as `-05:00` (EST), not `-04:00` (EDT).
/// * **Nonexistent** -- when the clocks go forward ("spring forward") the
///   reading never occurs. It is shifted forward by the length of the gap,
///   which is the same as reading it with the offset in effect *before* the
///   transition. For example `2024-03-10T02:30:00` in `America/New_York` is
///   read as `-05:00` (EST) and therefore denotes `2024-03-10T03:30:00-04:00`.
///
/// Timezones with a fixed offset are never ambiguous and have no gaps.
///
/// See <https://github.com/apache/arrow-rs/issues/11037> for the PostgreSQL and
/// ICU (DuckDB) sources these rules are taken from.
fn resolve_local_offset(tz: &Tz, local: &NaiveDateTime) -> Option<FixedOffset> {
    match tz.offset_from_local_datetime(local) {
        LocalResult::Single(offset) => Some(offset.fix()),
        LocalResult::Ambiguous(_earlier, later) => Some(later.fix()),
        LocalResult::None => {
            // The reading falls in a gap. Recover the offset in effect before
            // the transition by probing 24 hours earlier.
            //
            // Two separate properties of the timezone database make this sound:
            //
            // 1. No local gap is longer than 24 hours, so the probe lands
            //    outside this gap and is itself resolvable. Seven zones have a
            //    gap of exactly 24 hours -- the dateline changes, such as
            //    `Pacific/Apia` in 2011 and `Pacific/Kiritimati` in 1994. At the
            //    last second of one of those the probe lands one second before
            //    the gap starts, so the true margin here is one second, not a
            //    comfortable one.
            // 2. No two transitions are closer together than 24 hours, so the
            //    offset the probe finds is the one in effect immediately before
            //    this transition, and not some older offset. The smallest
            //    observed interval is 167 hours (`America/Boa_Vista`, 2000).
            //
            // Property 1 is what makes the probe resolvable; property 2 is what
            // makes the answer correct. If the probe is still unresolvable, give
            // up and let the caller apply the usual error / null handling.
            tz.offset_from_local_datetime(&(*local - TimeDelta::hours(24)))
                .earliest()
                .map(|offset| offset.fix())
        }
    }
}

pub(crate) fn adjust_timestamp_to_timezone<T: ArrowTimestampType>(
    array: PrimitiveArray<Int64Type>,
    to_tz: &Tz,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<Int64Type>, ArrowError> {
    let adjust = |o| {
        let local = as_datetime::<T>(o)?;
        let offset = resolve_local_offset(to_tz, &local)?;
        T::from_naive_datetime(local - offset, None)
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
