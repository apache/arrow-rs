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

use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::ArrowPrimitiveType;
use arrow_schema::ArrowError;
use chrono::prelude::*;

/// Helper for parsing timestamps
struct TimestampParser {
    digits: [u8; 32],
    mask: u32,
}

impl TimestampParser {
    fn new(bytes: &[u8]) -> Self {
        let mut digits = [0; 32];
        let mut mask = 0;

        for (idx, (o, i)) in digits.iter_mut().zip(bytes).enumerate() {
            *o = i.wrapping_sub(b'0');
            mask |= ((*o < 10) as u32) << idx
        }

        Self { digits, mask }
    }

    /// Returns true if the byte at `idx` equals `b`
    fn test(&self, idx: usize, b: u8) -> bool {
        self.digits[idx] == b.wrapping_sub(b'0')
    }

    /// Parses a date of the form `1997-01-31`
    fn date(&self) -> Option<NaiveDate> {
        if self.mask & 0b1111111111 != 0b1101101111
            || !self.test(4, b'-')
            || !self.test(7, b'-')
        {
            return None;
        }

        let year = self.digits[0] as u16 * 1000
            + self.digits[1] as u16 * 100
            + self.digits[2] as u16 * 10
            + self.digits[3] as u16;

        let month = self.digits[5] * 10 + self.digits[6];
        let day = self.digits[8] * 10 + self.digits[9];

        NaiveDate::from_ymd_opt(year as _, month as _, day as _)
    }

    /// Parses a time of any of forms
    /// - `09:26:56`
    /// - `09:26:56.123`
    /// - `09:26:56.123456`
    /// - `09:26:56.123456789`
    /// - `092656`
    ///
    /// Returning the end byte offset
    fn time(&self) -> Option<(NaiveTime, usize)> {
        match (self.mask >> 11) & 0b11111111 {
            // 09:26:56
            0b11011011 if self.test(13, b':') && self.test(16, b':') => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[14] * 10 + self.digits[15];
                let second = self.digits[17] * 10 + self.digits[18];
                let time = NaiveTime::from_hms_opt(hour as _, minute as _, second as _)?;

                let millis = || {
                    self.digits[20] as u32 * 100_000_000
                        + self.digits[21] as u32 * 10_000_000
                        + self.digits[22] as u32 * 1_000_000
                };

                let micros = || {
                    self.digits[23] as u32 * 100_000
                        + self.digits[24] as u32 * 10_000
                        + self.digits[25] as u32 * 1_000
                };

                let nanos = || {
                    self.digits[26] as u32 * 100
                        + self.digits[27] as u32 * 10
                        + self.digits[28] as u32
                };

                match self.test(19, b'.') {
                    true => match (self.mask >> 20).trailing_ones() {
                        3 => Some((time.with_nanosecond(millis())?, 23)),
                        6 => Some((time.with_nanosecond(millis() + micros())?, 26)),
                        9 => Some((
                            time.with_nanosecond(millis() + micros() + nanos())?,
                            29,
                        )),
                        _ => None,
                    },
                    false => Some((time, 19)),
                }
            }
            // 09:26:56
            0b111111 => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[13] * 10 + self.digits[14];
                let second = self.digits[15] * 10 + self.digits[16];
                let time = NaiveTime::from_hms_opt(hour as _, minute as _, second as _)?;
                Some((time, 17))
            }
            _ => None,
        }
    }
}

/// Accepts a string and parses it relative to the provided `timezone`
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
/// * `1997-01-31 092656`               # close to RCF3339, no fractional seconds
/// * `1997-01-31 092656+04:00`         # close to RCF3339, no fractional seconds or time separator
/// * `1997-01-31`                      # close to RCF3339, only date no time
///
/// Some formats that supported by PostgresSql <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TIME-TABLE>
/// still not supported by chrono, like
///     "2023-01-01 040506 America/Los_Angeles",
///     "2023-01-01 04:05:06.789 +07:30:00",
///     "2023-01-01 040506 +07:30:00",
///     "2023-01-01 04:05:06.789 PST",
///     "2023-01-01 04:05:06.789 -08",
pub fn string_to_datetime<T: TimeZone>(
    timezone: &T,
    s: &str,
) -> Result<DateTime<T>, ArrowError> {
    let err = |ctx: &str| {
        ArrowError::ParseError(format!("Error parsing timestamp from '{s}': {ctx}"))
    };

    let bytes = s.as_bytes();
    if bytes.len() < 10 {
        return Err(err("timestamp must contain at least 10 characters"));
    }

    let parser = TimestampParser::new(bytes);
    let date = parser.date().ok_or_else(|| err("error parsing date"))?;
    if bytes.len() == 10 {
        let offset = timezone.offset_from_local_date(&date);
        let offset = offset
            .single()
            .ok_or_else(|| err("error computing offset"))?;

        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        return Ok(DateTime::from_local(date.and_time(time), offset));
    }

    let (time, tz_offset) = parser.time().ok_or_else(|| err("error parsing time"))?;
    let datetime = date.and_time(time);
    if bytes.len() <= tz_offset {
        let offset = timezone.offset_from_local_datetime(&datetime);
        let offset = offset
            .single()
            .ok_or_else(|| err("error computing offset"))?;
        return Ok(DateTime::from_local(datetime, offset));
    }

    if bytes[tz_offset] == b'z' || bytes[tz_offset] == b'Z' {
        let offset = timezone.offset_from_local_datetime(&datetime);
        let offset = offset
            .single()
            .ok_or_else(|| err("error computing offset"))?;
        return Ok(DateTime::from_utc(datetime, offset));
    }

    // Parse remainder of string as timezone
    let parsed_tz: Tz = s[tz_offset..].trim_start().parse()?;
    let offset = parsed_tz.offset_from_local_datetime(&datetime);
    let offset = offset
        .single()
        .ok_or_else(|| err("error computing offset"))?;
    Ok(DateTime::<Tz>::from_local(datetime, offset).with_timezone(timezone))
}

/// Accepts a string in RFC3339 / ISO8601 standard format and some
/// variants and converts it to a nanosecond precision timestamp.
///
/// See [`string_to_datetime`] for the full set of supported formats
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
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
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// This function interprets string without an explicit time zone as timestamps
/// relative to UTC, see [`string_to_datetime`] for alternative semantics
///
/// For example, both `1997-01-31 09:26:56.123Z`, `1997-01-31T09:26:56.123`,
/// and `1997-01-31T14:26:56.123-05:00` will be parsed as the same value
///
#[inline]
pub fn string_to_timestamp_nanos(s: &str) -> Result<i64, ArrowError> {
    to_timestamp_nanos(string_to_datetime(&Utc, s)?.naive_utc())
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
        let parser = TimestampParser::new(string.as_bytes());
        let date = parser.date()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }

    fn parse_formatted(string: &str, format: &str) -> Option<i32> {
        let date = NaiveDate::parse_from_str(string, format).ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }
}

impl Parser for Date64Type {
    fn parse(string: &str) -> Option<i64> {
        let date_time = string_to_datetime(&Utc, string).ok()?;
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
    use arrow_array::timezone::Tz;

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
        expect_timestamp_parse_error("", "Error parsing timestamp from '': timestamp must contain at least 10 characters");
        expect_timestamp_parse_error("SS", "Error parsing timestamp from 'SS': timestamp must contain at least 10 characters");
        expect_timestamp_parse_error(
            "Wed, 18 Feb 2015 23:16:09 GMT",
            "Parser error: Error parsing timestamp from 'Wed, 18 Feb 2015 23:16:09 GMT': error parsing date",
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

        let tz: Tz = "+02:00".parse().unwrap();
        let date = string_to_datetime(&tz, "2020-09-08 13:42:29").unwrap();
        let utc = date.naive_utc().to_string();
        assert_eq!(utc, "2020-09-08 11:42:29");
        let local = date.naive_local().to_string();
        assert_eq!(local, "2020-09-08 13:42:29");

        let date = string_to_datetime(&tz, "2020-09-08 13:42:29Z").unwrap();
        let utc = date.naive_utc().to_string();
        assert_eq!(utc, "2020-09-08 13:42:29");
        let local = date.naive_local().to_string();
        assert_eq!(local, "2020-09-08 15:42:29");

        let dt =
            NaiveDateTime::parse_from_str("2020-09-08T13:42:29Z", "%Y-%m-%dT%H:%M:%SZ")
                .unwrap();
        let local: Tz = "+08:00".parse().unwrap();

        // Parsed as offset from UTC
        let date = string_to_datetime(&local, "2020-09-08T13:42:29Z").unwrap();
        assert_eq!(dt, date.naive_utc());
        assert_ne!(dt, date.naive_local());

        // Parsed as offset from local
        let date = string_to_datetime(&local, "2020-09-08 13:42:29").unwrap();
        assert_eq!(dt, date.naive_local());
        assert_ne!(dt, date.naive_utc());
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
