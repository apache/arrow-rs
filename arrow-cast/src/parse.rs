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

    // timezone offset, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855-05:00
    if let Ok(ts) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(ts.timestamp_nanos());
    }

    // with an explicit Z, using ' ' as a separator
    // Example: 2020-09-08 13:42:29Z
    if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
        return Ok(ts.timestamp_nanos());
    }

    // Support timestamps without an explicit timezone offset, again
    // to be compatible with what Apache Spark SQL does.

    // without a timezone specifier as a local time, using T as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(ts.timestamp_nanos());
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
        return Ok(ts.timestamp_nanos());
    }

    // without a timezone specifier as a local time, using ' ' as a
    // separator, no fractional seconds
    // Example: 2020-09-08 13:42:29
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(ts.timestamp_nanos());
    }

    // Note we don't pass along the error message from the underlying
    // chrono parsing because we tried several different format
    // strings and we don't know which the user was trying to
    // match. Ths any of the specific error messages is likely to be
    // be more confusing than helpful
    Err(ArrowError::CastError(format!(
        "Error parsing '{}' as timestamp",
        s
    )))
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
        .ok_or_else(|| ArrowError::CastError(format!("Error parsing '{}' as time", s)))
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
            eprintln!("Error parsing timestamp '{}': {:?}", s, e);
        }
        result
    }

    fn expect_timestamp_parse_error(s: &str, expected_err: &str) {
        match string_to_timestamp_nanos(s) {
            Ok(v) => panic!(
                "Expected error '{}' while parsing '{}', but parsed {} instead",
                expected_err, s, v
            ),
            Err(e) => {
                assert!(e.to_string().contains(expected_err),
                        "Can not find expected error '{}' while parsing '{}'. Actual error '{}'",
                        expected_err, s, e);
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
}
