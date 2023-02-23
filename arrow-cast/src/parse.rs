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

use arrow_array::types::*;
use arrow_array::ArrowPrimitiveType;
use arrow_schema::ArrowError;
use chrono::prelude::*;
use std::str::FromStr;

/// Accepts a string in RFC3339 / ISO8601 standard format and some
/// variants and converts it to a nanosecond precision timestamp.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// In addition to RFC3339 / ISO8601 standard timestamps, it also
/// accepts strings that use a space ` ` to separate the date and time
/// as well as strings that have no explicit timezone offset.
///
/// Examples of accepted inputs:
/// * `1997-01-31T09:26:56.123Z`        # RCF3339
/// * `1997-01-31T09:26:56.123-05:00`   # RCF3339
/// * `1997-01-31 09:26:56.123-05:00`   # close to RCF3339 but with a space rather than T
/// * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
/// * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
/// * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
/// * `1997-01-31`                      # close to RCF3339, only date no time
//
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// We hope to extend this function in the future with a second
/// parameter to specifying the format string.
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// This function interprets strings without an explicit time zone as
/// timestamps with offsets of the local time on the machine
///
/// For example, `1997-01-31 09:26:56.123Z` is interpreted as UTC, as
/// it has an explicit timezone specifier (“Z” for Zulu/UTC)
///
/// `1997-01-31T09:26:56.123` is interpreted as a local timestamp in
/// the timezone of the machine. For example, if
/// the system timezone is set to Americas/New_York (UTC-5) the
/// timestamp will be interpreted as though it were
/// `1997-01-31T09:26:56.123-05:00`
///
/// Some formats that supported by PostgresSql <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TIME-TABLE>
/// still not supported by chrono, like
///     "2023-01-01 040506 America/Los_Angeles",
///     "2023-01-01 04:05:06.789 +07:30:00",
///     "2023-01-01 040506 +07:30:00",
///     "2023-01-01 04:05:06.789 PST",
///     "2023-01-01 04:05:06.789 -08",
#[inline]
pub fn string_to_timestamp_nanos(s: &str) -> Result<i64, ArrowError> {
    // Fast path:  RFC3339 timestamp (with a T)
    // Example: 2020-09-08T13:42:29.190855Z
    if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
        return Ok(ts.timestamp_nanos());
    }

    // Implement quasi-RFC3339 support by trying to parse the
    // timestamp with various other format specifiers to to support
    // separating the date and time with a space ' ' rather than 'T' to be
    // (more) compatible with Apache Spark SQL

    let supported_formats = vec![
        "%Y-%m-%d %H:%M:%S%.f%:z", // Example: 2020-09-08 13:42:29.190855-05:00
        "%Y-%m-%d %H%M%S%.3f%:z",  // Example: "2023-01-01 040506 +07:30"
    ];

    for f in supported_formats.iter() {
        if let Ok(ts) = DateTime::parse_from_str(s, f) {
            return to_timestamp_nanos(ts.naive_utc());
        }
    }

    // with an explicit Z, using ' ' as a separator
    // Example: 2020-09-08 13:42:29Z
    if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
        return to_timestamp_nanos(ts.naive_utc());
    }

    // Support timestamps without an explicit timezone offset, again
    // to be compatible with what Apache Spark SQL does.

    // without a timezone specifier as a local time, using T as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return to_timestamp_nanos(ts);
    }

    // without a timezone specifier as a local time, using T as a
    // separator, no fractional seconds
    // Example: 2020-09-08T13:42:29
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(ts.timestamp_nanos());
    }

    // without a timezone specifier as a local time, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return to_timestamp_nanos(ts);
    }

    // without a timezone specifier as a local time, using ' ' as a
    // separator, no fractional seconds
    // Example: 2020-09-08 13:42:29
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(ts.timestamp_nanos());
    }

    // without a timezone specifier as a local time, only date
    // Example: 2020-09-08
    if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(ts) = dt.and_hms_opt(0, 0, 0) {
            return Ok(ts.timestamp_nanos());
        }
    }

    // Note we don't pass along the error message from the underlying
    // chrono parsing because we tried several different format
    // strings and we don't know which the user was trying to
    // match. Ths any of the specific error messages is likely to be
    // be more confusing than helpful
    Err(ArrowError::CastError(format!(
        "Error parsing '{s}' as timestamp"
    )))
}

/// Defensive check to prevent chrono-rs panics when nanosecond conversion happens on non-supported dates
#[inline]
fn to_timestamp_nanos(dt: NaiveDateTime) -> Result<i64, ArrowError> {
    if dt.timestamp().checked_mul(1_000_000_000).is_none() {
        return Err(ArrowError::ParseError(
            ERR_NANOSECONDS_NOT_SUPPORTED.to_string(),
        ));
    }

    Ok(dt.timestamp_nanos())
}

/// Accepts a string in ISO8601 standard format and some
/// variants and converts it to nanoseconds since midnight.
///
/// Examples of accepted inputs:
/// * `09:26:56.123 AM`
/// * `23:59:59`
/// * `6:00 pm`
//
/// Internally, this function uses the `chrono` library for the
/// time parsing
///
/// ## Timezone / Offset Handling
///
/// This function does not support parsing strings with a timezone
/// or offset specified, as it considers only time since midnight.
pub fn string_to_time_nanoseconds(s: &str) -> Result<i64, ArrowError> {
    // colon count, presence of decimal, presence of whitespace
    fn preprocess_time_string(string: &str) -> (usize, bool, bool) {
        string
            .as_bytes()
            .iter()
            .fold((0, false, false), |tup, char| match char {
                b':' => (tup.0 + 1, tup.1, tup.2),
                b'.' => (tup.0, true, tup.2),
                b' ' => (tup.0, tup.1, true),
                _ => tup,
            })
    }

    // Do a preprocess pass of the string to prune which formats to attempt parsing for
    let formats: &[&str] = match preprocess_time_string(s.trim()) {
        // 24-hour clock, with hour, minutes, seconds and fractions of a second specified
        // Examples:
        // * 09:50:12.123456789
        // *  9:50:12.123456789
        (2, true, false) => &["%H:%M:%S%.f", "%k:%M:%S%.f"],

        // 12-hour clock, with hour, minutes, seconds and fractions of a second specified
        // Examples:
        // * 09:50:12.123456789 PM
        // * 09:50:12.123456789 pm
        // *  9:50:12.123456789 AM
        // *  9:50:12.123456789 am
        (2, true, true) => &[
            "%I:%M:%S%.f %P",
            "%I:%M:%S%.f %p",
            "%l:%M:%S%.f %P",
            "%l:%M:%S%.f %p",
        ],

        // 24-hour clock, with hour, minutes and seconds specified
        // Examples:
        // * 09:50:12
        // *  9:50:12
        (2, false, false) => &["%H:%M:%S", "%k:%M:%S"],

        // 12-hour clock, with hour, minutes and seconds specified
        // Examples:
        // * 09:50:12 PM
        // * 09:50:12 pm
        // *  9:50:12 AM
        // *  9:50:12 am
        (2, false, true) => &["%I:%M:%S %P", "%I:%M:%S %p", "%l:%M:%S %P", "%l:%M:%S %p"],

        // 24-hour clock, with hour and minutes specified
        // Examples:
        // * 09:50
        // *  9:50
        (1, false, false) => &["%H:%M", "%k:%M"],

        // 12-hour clock, with hour and minutes specified
        // Examples:
        // * 09:50 PM
        // * 09:50 pm
        // *  9:50 AM
        // *  9:50 am
        (1, false, true) => &["%I:%M %P", "%I:%M %p", "%l:%M %P", "%l:%M %p"],

        _ => &[],
    };

    formats
        .iter()
        .find_map(|f| NaiveTime::parse_from_str(s, f).ok())
        .map(|nt| {
            nt.num_seconds_from_midnight() as i64 * 1_000_000_000 + nt.nanosecond() as i64
        })
        // Return generic error if failed to parse as unknown which format user intended for the string
        .ok_or_else(|| ArrowError::CastError(format!("Error parsing '{s}' as time")))
}

/// Specialized parsing implementations
/// used by csv and json reader
pub trait Parser: ArrowPrimitiveType {
    fn parse(string: &str) -> Option<Self::Native>;

    fn parse_formatted(string: &str, _format: &str) -> Option<Self::Native> {
        Self::parse(string)
    }
}

impl Parser for Float32Type {
    fn parse(string: &str) -> Option<f32> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}

impl Parser for Float64Type {
    fn parse(string: &str) -> Option<f64> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}

macro_rules! parser_primitive {
    ($t:ty) => {
        impl Parser for $t {
            fn parse(string: &str) -> Option<Self::Native> {
                string.parse::<Self::Native>().ok()
            }
        }
    };
}
parser_primitive!(UInt64Type);
parser_primitive!(UInt32Type);
parser_primitive!(UInt16Type);
parser_primitive!(UInt8Type);
parser_primitive!(Int64Type);
parser_primitive!(Int32Type);
parser_primitive!(Int16Type);
parser_primitive!(Int8Type);

impl Parser for TimestampNanosecondType {
    fn parse(string: &str) -> Option<i64> {
        string_to_timestamp_nanos(string).ok()
    }
}

impl Parser for TimestampMicrosecondType {
    fn parse(string: &str) -> Option<i64> {
        let nanos = string_to_timestamp_nanos(string).ok();
        nanos.map(|x| x / 1000)
    }
}

impl Parser for TimestampMillisecondType {
    fn parse(string: &str) -> Option<i64> {
        let nanos = string_to_timestamp_nanos(string).ok();
        nanos.map(|x| x / 1_000_000)
    }
}

impl Parser for TimestampSecondType {
    fn parse(string: &str) -> Option<i64> {
        let nanos = string_to_timestamp_nanos(string).ok();
        nanos.map(|x| x / 1_000_000_000)
    }
}

impl Parser for Time64NanosecondType {
    // Will truncate any fractions of a nanosecond
    fn parse(string: &str) -> Option<Self::Native> {
        string_to_time_nanoseconds(string)
            .ok()
            .or_else(|| string.parse::<Self::Native>().ok())
    }

    fn parse_formatted(string: &str, format: &str) -> Option<Self::Native> {
        let nt = NaiveTime::parse_from_str(string, format).ok()?;
        Some(
            nt.num_seconds_from_midnight() as i64 * 1_000_000_000
                + nt.nanosecond() as i64,
        )
    }
}

impl Parser for Time64MicrosecondType {
    // Will truncate any fractions of a microsecond
    fn parse(string: &str) -> Option<Self::Native> {
        string_to_time_nanoseconds(string)
            .ok()
            .map(|nanos| nanos / 1_000)
            .or_else(|| string.parse::<Self::Native>().ok())
    }

    fn parse_formatted(string: &str, format: &str) -> Option<Self::Native> {
        let nt = NaiveTime::parse_from_str(string, format).ok()?;
        Some(
            nt.num_seconds_from_midnight() as i64 * 1_000_000
                + nt.nanosecond() as i64 / 1_000,
        )
    }
}

impl Parser for Time32MillisecondType {
    // Will truncate any fractions of a millisecond
    fn parse(string: &str) -> Option<Self::Native> {
        string_to_time_nanoseconds(string)
            .ok()
            .map(|nanos| (nanos / 1_000_000) as i32)
            .or_else(|| string.parse::<Self::Native>().ok())
    }

    fn parse_formatted(string: &str, format: &str) -> Option<Self::Native> {
        let nt = NaiveTime::parse_from_str(string, format).ok()?;
        Some(
            nt.num_seconds_from_midnight() as i32 * 1_000
                + nt.nanosecond() as i32 / 1_000_000,
        )
    }
}

impl Parser for Time32SecondType {
    // Will truncate any fractions of a second
    fn parse(string: &str) -> Option<Self::Native> {
        string_to_time_nanoseconds(string)
            .ok()
            .map(|nanos| (nanos / 1_000_000_000) as i32)
            .or_else(|| string.parse::<Self::Native>().ok())
    }

    fn parse_formatted(string: &str, format: &str) -> Option<Self::Native> {
        let nt = NaiveTime::parse_from_str(string, format).ok()?;
        Some(
            nt.num_seconds_from_midnight() as i32
                + nt.nanosecond() as i32 / 1_000_000_000,
        )
    }
}

/// Number of days between 0001-01-01 and 1970-01-01
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

impl Parser for Date32Type {
    fn parse(string: &str) -> Option<i32> {
        let date = string.parse::<chrono::NaiveDate>().ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }

    fn parse_formatted(string: &str, format: &str) -> Option<i32> {
        let date = chrono::NaiveDate::parse_from_str(string, format).ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }
}

impl Parser for Date64Type {
    fn parse(string: &str) -> Option<i64> {
        let date_time = string.parse::<NaiveDateTime>().ok()?;
        Some(date_time.timestamp_millis())
    }

    fn parse_formatted(string: &str, format: &str) -> Option<i64> {
        use chrono::format::Fixed;
        use chrono::format::StrftimeItems;
        let fmt = StrftimeItems::new(format);
        let has_zone = fmt.into_iter().any(|item| match item {
            chrono::format::Item::Fixed(fixed_item) => matches!(
                fixed_item,
                Fixed::RFC2822
                    | Fixed::RFC3339
                    | Fixed::TimezoneName
                    | Fixed::TimezoneOffsetColon
                    | Fixed::TimezoneOffsetColonZ
                    | Fixed::TimezoneOffset
                    | Fixed::TimezoneOffsetZ
            ),
            _ => false,
        });
        if has_zone {
            let date_time = chrono::DateTime::parse_from_str(string, format).ok()?;
            Some(date_time.timestamp_millis())
        } else {
            let date_time = NaiveDateTime::parse_from_str(string, format).ok()?;
            Some(date_time.timestamp_millis())
        }
    }
}

pub(crate) fn parse_interval_year_month(
    value: &str,
) -> Result<<IntervalYearMonthType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, result_days, result_nanos) = parse_interval("years", value)?;
    if result_days != 0 || result_nanos != 0 {
        return Err(ArrowError::CastError(format!(
            "Cannot cast ${value} to IntervalYearMonth because the value isn't multiple of months"
        )));
    }
    Ok(IntervalYearMonthType::make_value(0, result_months))
}

pub(crate) fn parse_interval_day_time(
    value: &str,
) -> Result<<IntervalDayTimeType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, mut result_days, result_nanos) = parse_interval("days", value)?;
    if result_nanos % 1_000_000 != 0 {
        return Err(ArrowError::CastError(format!(
            "Cannot cast ${value} to IntervalDayTime because the nanos part isn't multiple of milliseconds"
        )));
    }
    result_days += result_months * 30;
    Ok(IntervalDayTimeType::make_value(
        result_days,
        (result_nanos / 1_000_000) as i32,
    ))
}

pub(crate) fn parse_interval_month_day_nano(
    value: &str,
) -> Result<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, result_days, result_nanos) = parse_interval("months", value)?;
    Ok(IntervalMonthDayNanoType::make_value(
        result_months,
        result_days,
        result_nanos,
    ))
}

#[inline]
pub(crate) fn year_month_interval_to_string(n: i32) -> String {
    format!("{n} months")
}

#[inline]
pub(crate) fn day_time_interval_to_string(n: i64) -> String {
    format!("{} days {} milliseconds", n >> 32, n & ((1 << 32) - 1))
}

#[inline]
pub(crate) fn month_day_nano_interval_to_string(n: i128) -> String {
    format!(
        "{} months {} days {} nanos",
        n >> 96,
        (n >> 64) & ((1 << 32) - 1),
        n & ((1 << 64) - 1)
    )
}

const SECONDS_PER_HOUR: f64 = 3_600_f64;
const NANOS_PER_SECOND: f64 = 1_000_000_000_f64;

#[derive(Clone, Copy)]
#[repr(u16)]
enum IntervalType {
    Century = 0b_00_0000_0001,
    Decade = 0b_00_0000_0010,
    Year = 0b_00_0000_0100,
    Month = 0b_00_0000_1000,
    Week = 0b_00_0001_0000,
    Day = 0b_00_0010_0000,
    Hour = 0b_00_0100_0000,
    Minute = 0b_00_1000_0000,
    Second = 0b_01_0000_0000,
    Millisecond = 0b_10_0000_0000,
}

impl FromStr for IntervalType {
    type Err = ArrowError;

    fn from_str(s: &str) -> Result<Self, ArrowError> {
        match s.to_lowercase().as_str() {
            "century" | "centuries" => Ok(Self::Century),
            "decade" | "decades" => Ok(Self::Decade),
            "year" | "years" => Ok(Self::Year),
            "month" | "months" => Ok(Self::Month),
            "week" | "weeks" => Ok(Self::Week),
            "day" | "days" => Ok(Self::Day),
            "hour" | "hours" => Ok(Self::Hour),
            "minute" | "minutes" => Ok(Self::Minute),
            "second" | "seconds" => Ok(Self::Second),
            "millisecond" | "milliseconds" => Ok(Self::Millisecond),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Unknown interval type: {s}"
            ))),
        }
    }
}

pub type MonthDayNano = (i32, i32, i64);

/// parse string value to a triple of aligned months, days, nanos
pub fn parse_interval(
    leading_field: &str,
    value: &str,
) -> Result<MonthDayNano, ArrowError> {
    // We are storing parts as integers, it's why we need to align parts fractional
    // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
    // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
    let align_interval_parts =
        |month_part: f64, mut day_part: f64, mut nanos_part: f64| -> (i64, i64, f64) {
            // Convert fractional month to days, It's not supported by Arrow types, but anyway
            day_part += (month_part - (month_part as i64) as f64) * 30_f64;

            // Convert fractional days to hours
            nanos_part += (day_part - ((day_part as i64) as f64))
                * 24_f64
                * SECONDS_PER_HOUR
                * NANOS_PER_SECOND;

            (month_part as i64, day_part as i64, nanos_part)
        };

    let mut used_interval_types = 0;

    let mut calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
     -> Result<(i64, i64, f64), ArrowError> {
        // @todo It's better to use Decimal in order to protect rounding errors
        // Wait https://github.com/apache/arrow/pull/9232
        let interval_period = match f64::from_str(interval_period_str) {
            Ok(n) => n,
            Err(_) => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported Interval Expression with value {value:?}"
                )));
            }
        };

        if interval_period > (i64::MAX as f64) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        let it = IntervalType::from_str(interval_type).map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid input syntax for type interval: {value:?}"
            ))
        })?;

        // Disallow duplicate interval types
        if used_interval_types & (it as u16) != 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid input syntax for type interval: {value:?}. Repeated type '{interval_type}'"
            )));
        } else {
            used_interval_types |= it as u16;
        }

        match it {
            IntervalType::Century => {
                Ok(align_interval_parts(interval_period * 1200_f64, 0.0, 0.0))
            }
            IntervalType::Decade => {
                Ok(align_interval_parts(interval_period * 120_f64, 0.0, 0.0))
            }
            IntervalType::Year => {
                Ok(align_interval_parts(interval_period * 12_f64, 0.0, 0.0))
            }
            IntervalType::Month => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
            IntervalType::Week => {
                Ok(align_interval_parts(0.0, interval_period * 7_f64, 0.0))
            }
            IntervalType::Day => Ok(align_interval_parts(0.0, interval_period, 0.0)),
            IntervalType::Hour => {
                Ok((0, 0, interval_period * SECONDS_PER_HOUR * NANOS_PER_SECOND))
            }
            IntervalType::Minute => {
                Ok((0, 0, interval_period * 60_f64 * NANOS_PER_SECOND))
            }
            IntervalType::Second => Ok((0, 0, interval_period * NANOS_PER_SECOND)),
            IntervalType::Millisecond => Ok((0, 0, interval_period * 1_000_000f64)),
        }
    };

    let mut result_month: i64 = 0;
    let mut result_days: i64 = 0;
    let mut result_nanos: i128 = 0;

    let mut parts = value.split_whitespace();

    loop {
        let interval_period_str = parts.next();
        if interval_period_str.is_none() {
            break;
        }

        let unit = parts.next().unwrap_or(leading_field);

        let (diff_month, diff_days, diff_nanos) =
            calculate_from_part(interval_period_str.unwrap(), unit)?;

        result_month += diff_month;

        if result_month > (i32::MAX as i64) {
            return Err(ArrowError::NotYetImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        result_days += diff_days;

        if result_days > (i32::MAX as i64) {
            return Err(ArrowError::NotYetImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        result_nanos += diff_nanos as i128;

        if result_nanos > (i64::MAX as i128) {
            return Err(ArrowError::NotYetImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }
    }

    Ok((result_month as i32, result_days as i32, result_nanos as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_to_timestamp_timezone() {
        // Explicit timezone
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855+00:00").unwrap()
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855Z").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp("2020-09-08T13:42:29Z").unwrap()
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855-05:00").unwrap()
        );
    }

    #[test]
    fn string_to_timestamp_timezone_space() {
        // Ensure space rather than T between time and date is accepted
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855+00:00").unwrap()
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855Z").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp("2020-09-08 13:42:29Z").unwrap()
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855-05:00").unwrap()
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function: mktime
    fn string_to_timestamp_no_timezone() {
        // This test is designed to succeed in regardless of the local
        // timezone the test machine is running. Thus it is still
        // somewhat susceptible to bugs in the use of chrono
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_nano_opt(13, 42, 29, 190855000).unwrap(),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08T13:42:29.190855").unwrap()
        );

        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08 13:42:29.190855").unwrap()
        );

        // Also ensure that parsing timestamps with no fractional
        // second part works as well
        let naive_datetime_whole_secs = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_opt(13, 42, 29).unwrap(),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime_whole_secs.timestamp_nanos(),
            parse_timestamp("2020-09-08T13:42:29").unwrap()
        );

        assert_eq!(
            naive_datetime_whole_secs.timestamp_nanos(),
            parse_timestamp("2020-09-08 13:42:29").unwrap()
        );

        // ensure without time work
        // no time, should be the nano second at
        // 2020-09-08 0:0:0
        let naive_datetime_no_time = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );

        assert_eq!(
            naive_datetime_no_time.timestamp_nanos(),
            parse_timestamp("2020-09-08").unwrap()
        )
    }

    #[test]
    fn string_to_timestamp_invalid() {
        // Test parsing invalid formats

        // It would be nice to make these messages better
        expect_timestamp_parse_error("", "Error parsing '' as timestamp");
        expect_timestamp_parse_error("SS", "Error parsing 'SS' as timestamp");
        expect_timestamp_parse_error(
            "Wed, 18 Feb 2015 23:16:09 GMT",
            "Error parsing 'Wed, 18 Feb 2015 23:16:09 GMT' as timestamp",
        );
    }

    // Parse a timestamp to timestamp int with a useful human readable error message
    fn parse_timestamp(s: &str) -> Result<i64, ArrowError> {
        let result = string_to_timestamp_nanos(s);
        if let Err(e) = &result {
            eprintln!("Error parsing timestamp '{s}': {e:?}");
        }
        result
    }

    fn expect_timestamp_parse_error(s: &str, expected_err: &str) {
        match string_to_timestamp_nanos(s) {
            Ok(v) => panic!(
                "Expected error '{expected_err}' while parsing '{s}', but parsed {v} instead"
            ),
            Err(e) => {
                assert!(e.to_string().contains(expected_err),
                        "Can not find expected error '{expected_err}' while parsing '{s}'. Actual error '{e}'");
            }
        }
    }

    #[test]
    fn string_without_timezone_to_timestamp() {
        // string without timezone should always output the same regardless the local or session timezone

        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_nano_opt(13, 42, 29, 190855000).unwrap(),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08T13:42:29.190855").unwrap()
        );

        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08 13:42:29.190855").unwrap()
        );

        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_nano_opt(13, 42, 29, 0).unwrap(),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08T13:42:29").unwrap()
        );

        assert_eq!(
            naive_datetime.timestamp_nanos(),
            parse_timestamp("2020-09-08 13:42:29").unwrap()
        );
    }

    #[test]
    fn parse_time64_nanos() {
        assert_eq!(
            Time64NanosecondType::parse("02:10:01.1234567899999999"),
            Some(7_801_123_456_789)
        );
        assert_eq!(
            Time64NanosecondType::parse("02:10:01.1234567"),
            Some(7_801_123_456_700)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01.1234567"),
            Some(7_801_123_456_700)
        );
        assert_eq!(
            Time64NanosecondType::parse("12:10:01.123456789 AM"),
            Some(601_123_456_789)
        );
        assert_eq!(
            Time64NanosecondType::parse("12:10:01.123456789 am"),
            Some(601_123_456_789)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01.12345678 PM"),
            Some(51_001_123_456_780)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01.12345678 pm"),
            Some(51_001_123_456_780)
        );
        assert_eq!(
            Time64NanosecondType::parse("02:10:01"),
            Some(7_801_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01"),
            Some(7_801_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("12:10:01 AM"),
            Some(601_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("12:10:01 am"),
            Some(601_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01 PM"),
            Some(51_001_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10:01 pm"),
            Some(51_001_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("02:10"),
            Some(7_800_000_000_000)
        );
        assert_eq!(Time64NanosecondType::parse("2:10"), Some(7_800_000_000_000));
        assert_eq!(
            Time64NanosecondType::parse("12:10 AM"),
            Some(600_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("12:10 am"),
            Some(600_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10 PM"),
            Some(51_000_000_000_000)
        );
        assert_eq!(
            Time64NanosecondType::parse("2:10 pm"),
            Some(51_000_000_000_000)
        );

        // parse directly as nanoseconds
        assert_eq!(Time64NanosecondType::parse("1"), Some(1));

        // leap second
        assert_eq!(
            Time64NanosecondType::parse("23:59:60"),
            Some(86_400_000_000_000)
        );

        // custom format
        assert_eq!(
            Time64NanosecondType::parse_formatted(
                "02 - 10 - 01 - .1234567",
                "%H - %M - %S - %.f"
            ),
            Some(7_801_123_456_700)
        );
    }

    #[test]
    fn parse_time64_micros() {
        // expected formats
        assert_eq!(
            Time64MicrosecondType::parse("02:10:01.1234"),
            Some(7_801_123_400)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10:01.1234"),
            Some(7_801_123_400)
        );
        assert_eq!(
            Time64MicrosecondType::parse("12:10:01.123456 AM"),
            Some(601_123_456)
        );
        assert_eq!(
            Time64MicrosecondType::parse("12:10:01.123456 am"),
            Some(601_123_456)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10:01.12345 PM"),
            Some(51_001_123_450)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10:01.12345 pm"),
            Some(51_001_123_450)
        );
        assert_eq!(
            Time64MicrosecondType::parse("02:10:01"),
            Some(7_801_000_000)
        );
        assert_eq!(Time64MicrosecondType::parse("2:10:01"), Some(7_801_000_000));
        assert_eq!(
            Time64MicrosecondType::parse("12:10:01 AM"),
            Some(601_000_000)
        );
        assert_eq!(
            Time64MicrosecondType::parse("12:10:01 am"),
            Some(601_000_000)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10:01 PM"),
            Some(51_001_000_000)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10:01 pm"),
            Some(51_001_000_000)
        );
        assert_eq!(Time64MicrosecondType::parse("02:10"), Some(7_800_000_000));
        assert_eq!(Time64MicrosecondType::parse("2:10"), Some(7_800_000_000));
        assert_eq!(Time64MicrosecondType::parse("12:10 AM"), Some(600_000_000));
        assert_eq!(Time64MicrosecondType::parse("12:10 am"), Some(600_000_000));
        assert_eq!(
            Time64MicrosecondType::parse("2:10 PM"),
            Some(51_000_000_000)
        );
        assert_eq!(
            Time64MicrosecondType::parse("2:10 pm"),
            Some(51_000_000_000)
        );

        // parse directly as microseconds
        assert_eq!(Time64MicrosecondType::parse("1"), Some(1));

        // leap second
        assert_eq!(
            Time64MicrosecondType::parse("23:59:60"),
            Some(86_400_000_000)
        );

        // custom format
        assert_eq!(
            Time64MicrosecondType::parse_formatted(
                "02 - 10 - 01 - .1234",
                "%H - %M - %S - %.f"
            ),
            Some(7_801_123_400)
        );
    }

    #[test]
    fn parse_time32_millis() {
        // expected formats
        assert_eq!(Time32MillisecondType::parse("02:10:01.1"), Some(7_801_100));
        assert_eq!(Time32MillisecondType::parse("2:10:01.1"), Some(7_801_100));
        assert_eq!(
            Time32MillisecondType::parse("12:10:01.123 AM"),
            Some(601_123)
        );
        assert_eq!(
            Time32MillisecondType::parse("12:10:01.123 am"),
            Some(601_123)
        );
        assert_eq!(
            Time32MillisecondType::parse("2:10:01.12 PM"),
            Some(51_001_120)
        );
        assert_eq!(
            Time32MillisecondType::parse("2:10:01.12 pm"),
            Some(51_001_120)
        );
        assert_eq!(Time32MillisecondType::parse("02:10:01"), Some(7_801_000));
        assert_eq!(Time32MillisecondType::parse("2:10:01"), Some(7_801_000));
        assert_eq!(Time32MillisecondType::parse("12:10:01 AM"), Some(601_000));
        assert_eq!(Time32MillisecondType::parse("12:10:01 am"), Some(601_000));
        assert_eq!(Time32MillisecondType::parse("2:10:01 PM"), Some(51_001_000));
        assert_eq!(Time32MillisecondType::parse("2:10:01 pm"), Some(51_001_000));
        assert_eq!(Time32MillisecondType::parse("02:10"), Some(7_800_000));
        assert_eq!(Time32MillisecondType::parse("2:10"), Some(7_800_000));
        assert_eq!(Time32MillisecondType::parse("12:10 AM"), Some(600_000));
        assert_eq!(Time32MillisecondType::parse("12:10 am"), Some(600_000));
        assert_eq!(Time32MillisecondType::parse("2:10 PM"), Some(51_000_000));
        assert_eq!(Time32MillisecondType::parse("2:10 pm"), Some(51_000_000));

        // parse directly as milliseconds
        assert_eq!(Time32MillisecondType::parse("1"), Some(1));

        // leap second
        assert_eq!(Time32MillisecondType::parse("23:59:60"), Some(86_400_000));

        // custom format
        assert_eq!(
            Time32MillisecondType::parse_formatted(
                "02 - 10 - 01 - .1",
                "%H - %M - %S - %.f"
            ),
            Some(7_801_100)
        );
    }

    #[test]
    fn parse_time32_secs() {
        // expected formats
        assert_eq!(Time32SecondType::parse("02:10:01.1"), Some(7_801));
        assert_eq!(Time32SecondType::parse("02:10:01"), Some(7_801));
        assert_eq!(Time32SecondType::parse("2:10:01"), Some(7_801));
        assert_eq!(Time32SecondType::parse("12:10:01 AM"), Some(601));
        assert_eq!(Time32SecondType::parse("12:10:01 am"), Some(601));
        assert_eq!(Time32SecondType::parse("2:10:01 PM"), Some(51_001));
        assert_eq!(Time32SecondType::parse("2:10:01 pm"), Some(51_001));
        assert_eq!(Time32SecondType::parse("02:10"), Some(7_800));
        assert_eq!(Time32SecondType::parse("2:10"), Some(7_800));
        assert_eq!(Time32SecondType::parse("12:10 AM"), Some(600));
        assert_eq!(Time32SecondType::parse("12:10 am"), Some(600));
        assert_eq!(Time32SecondType::parse("2:10 PM"), Some(51_000));
        assert_eq!(Time32SecondType::parse("2:10 pm"), Some(51_000));

        // parse directly as seconds
        assert_eq!(Time32SecondType::parse("1"), Some(1));

        // leap second
        assert_eq!(Time32SecondType::parse("23:59:60"), Some(86400));

        // custom format
        assert_eq!(
            Time32SecondType::parse_formatted("02 - 10 - 01", "%H - %M - %S"),
            Some(7_801)
        );
    }

    #[test]
    fn string_to_timestamp_old() {
        parse_timestamp("1677-06-14T07:29:01.256")
            .map_err(|e| assert!(e.to_string().ends_with(ERR_NANOSECONDS_NOT_SUPPORTED)))
            .unwrap_err();
    }
}
