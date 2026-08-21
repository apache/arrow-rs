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

//! [`Parser`] implementations for converting strings to Arrow types
//!
//! Used by the CSV and JSON readers to convert strings to Arrow types
use arrow_array::ArrowNativeTypeOp;
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use chrono::prelude::*;
use half::f16;
use std::str::FromStr;

/// Parse nanoseconds from the first `N` values in digits, subtracting the offset `O`
#[inline]
fn parse_nanos<const N: usize, const O: u8>(digits: &[u8]) -> u32 {
    digits[..N]
        .iter()
        .fold(0_u32, |acc, v| acc * 10 + v.wrapping_sub(O) as u32)
        * 10_u32.pow((9 - N) as _)
}

/// Helper for parsing RFC3339 timestamps
struct TimestampParser {
    /// The timestamp bytes to parse minus `b'0'`
    ///
    /// This makes interpretation as an integer inexpensive
    digits: [u8; 32],
    /// A mask containing a `1` bit where the corresponding byte is a valid ASCII digit
    mask: u32,
}

impl TimestampParser {
    fn new(bytes: &[u8]) -> Self {
        let mut digits = [0; 32];
        let mut mask = 0;

        // Treating all bytes the same way, helps LLVM vectorise this correctly
        for (idx, (o, i)) in digits.iter_mut().zip(bytes).enumerate() {
            *o = i.wrapping_sub(b'0');
            mask |= ((*o < 10) as u32) << idx
        }

        Self { digits, mask }
    }

    /// Returns true if the byte at `idx` in the original string equals `b`
    fn test(&self, idx: usize, b: u8) -> bool {
        self.digits[idx] == b.wrapping_sub(b'0')
    }

    /// Parses a date of the form `1997-01-31`
    fn date(&self) -> Option<NaiveDate> {
        if self.mask & 0b1111111111 != 0b1101101111 || !self.test(4, b'-') || !self.test(7, b'-') {
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
        // Make a NaiveTime handling leap seconds
        let time = |hour, min, sec, nano| match sec {
            60 => {
                let nano = 1_000_000_000 + nano;
                NaiveTime::from_hms_nano_opt(hour as _, min as _, 59, nano)
            }
            _ => NaiveTime::from_hms_nano_opt(hour as _, min as _, sec as _, nano),
        };

        match (self.mask >> 11) & 0b11111111 {
            // 09:26:56
            0b11011011 if self.test(13, b':') && self.test(16, b':') => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[14] * 10 + self.digits[15];
                let second = self.digits[17] * 10 + self.digits[18];

                match self.test(19, b'.') {
                    true => {
                        let digits = (self.mask >> 20).trailing_ones();
                        let nanos = match digits {
                            0 => return None,
                            1 => parse_nanos::<1, 0>(&self.digits[20..21]),
                            2 => parse_nanos::<2, 0>(&self.digits[20..22]),
                            3 => parse_nanos::<3, 0>(&self.digits[20..23]),
                            4 => parse_nanos::<4, 0>(&self.digits[20..24]),
                            5 => parse_nanos::<5, 0>(&self.digits[20..25]),
                            6 => parse_nanos::<6, 0>(&self.digits[20..26]),
                            7 => parse_nanos::<7, 0>(&self.digits[20..27]),
                            8 => parse_nanos::<8, 0>(&self.digits[20..28]),
                            _ => parse_nanos::<9, 0>(&self.digits[20..29]),
                        };
                        Some((time(hour, minute, second, nanos)?, 20 + digits as usize))
                    }
                    false => Some((time(hour, minute, second, 0)?, 19)),
                }
            }
            // 092656
            0b111111 => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[13] * 10 + self.digits[14];
                let second = self.digits[15] * 10 + self.digits[16];
                let time = time(hour, minute, second, 0)?;
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
/// * `2023-01-01 04:05:06.789 -08`     # close to RCF3339, no fractional seconds or time separator
/// * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
/// * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
/// * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
/// * `1997-01-31 092656`               # close to RCF3339, no fractional seconds
/// * `1997-01-31 092656+04:00`         # close to RCF3339, no fractional seconds or time separator
/// * `1997-01-31`                      # close to RCF3339, only date no time
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// Some formats supported by PostgresSql <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TIME-TABLE>
/// are not supported, like
///
/// * "2023-01-01 04:05:06.789 +07:30:00",
/// * "2023-01-01 040506 +07:30:00",
/// * "2023-01-01 04:05:06.789 PST",
///
/// [IANA timezones]: https://www.iana.org/time-zones
pub fn string_to_datetime<T: TimeZone>(timezone: &T, s: &str) -> Result<DateTime<T>, ArrowError> {
    let err =
        |ctx: &str| ArrowError::ParseError(format!("Error parsing timestamp from '{s}': {ctx}"));

    let bytes = s.as_bytes();
    if bytes.len() < 10 {
        return Err(err("timestamp must contain at least 10 characters"));
    }

    let parser = TimestampParser::new(bytes);
    let date = parser.date().ok_or_else(|| err("error parsing date"))?;
    if bytes.len() == 10 {
        let datetime = date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        return timezone
            .from_local_datetime(&datetime)
            .single()
            .ok_or_else(|| err("error computing timezone offset"));
    }

    if !parser.test(10, b'T') && !parser.test(10, b't') && !parser.test(10, b' ') {
        return Err(err("invalid timestamp separator"));
    }

    let (time, mut tz_offset) = parser.time().ok_or_else(|| err("error parsing time"))?;
    let datetime = date.and_time(time);

    if tz_offset == 32 {
        // Decimal overrun
        while tz_offset < bytes.len() && bytes[tz_offset].is_ascii_digit() {
            tz_offset += 1;
        }
    }

    if bytes.len() <= tz_offset {
        return timezone
            .from_local_datetime(&datetime)
            .single()
            .ok_or_else(|| err("error computing timezone offset"));
    }

    if (bytes[tz_offset] == b'z' || bytes[tz_offset] == b'Z') && tz_offset == bytes.len() - 1 {
        return Ok(timezone.from_utc_datetime(&datetime));
    }

    // Parse remainder of string as timezone
    let parsed_tz: Tz = s[tz_offset..].trim_start().parse()?;
    let parsed = parsed_tz
        .from_local_datetime(&datetime)
        .single()
        .ok_or_else(|| err("error computing timezone offset"))?;

    Ok(parsed.with_timezone(timezone))
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
/// In particular:
///
/// ```
/// # use arrow_cast::parse::string_to_timestamp_nanos;
/// // Note all three of these timestamps are parsed as the same value
/// let a = string_to_timestamp_nanos("1997-01-31 09:26:56.123Z").unwrap();
/// let b = string_to_timestamp_nanos("1997-01-31T09:26:56.123").unwrap();
/// let c = string_to_timestamp_nanos("1997-01-31T14:26:56.123+05:00").unwrap();
///
/// assert_eq!(a, b);
/// assert_eq!(b, c);
/// ```
///
#[inline]
pub fn string_to_timestamp_nanos(s: &str) -> Result<i64, ArrowError> {
    to_timestamp_nanos(string_to_datetime(&Utc, s)?.naive_utc())
}

/// Fallible conversion of [`NaiveDateTime`] to `i64` nanoseconds
#[inline]
fn to_timestamp_nanos(dt: NaiveDateTime) -> Result<i64, ArrowError> {
    dt.and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| ArrowError::ParseError(ERR_NANOSECONDS_NOT_SUPPORTED.to_string()))
}

/// Accepts a string in ISO8601 standard format and some
/// variants and converts it to nanoseconds since midnight.
///
/// Examples of accepted inputs:
///
/// * `09:26:56.123 AM`
/// * `23:59:59`
/// * `6:00 pm`
///
/// Internally, this function uses the `chrono` library for the time parsing
///
/// ## Timezone / Offset Handling
///
/// This function does not support parsing strings with a timezone
/// or offset specified, as it considers only time since midnight.
pub fn string_to_time_nanoseconds(s: &str) -> Result<i64, ArrowError> {
    let nt = string_to_time(s)
        .ok_or_else(|| ArrowError::ParseError(format!("Failed to parse \'{s}\' as time")))?;
    Ok(nt.num_seconds_from_midnight() as i64 * 1_000_000_000 + nt.nanosecond() as i64)
}

fn string_to_time(s: &str) -> Option<NaiveTime> {
    let bytes = s.as_bytes();
    if bytes.len() < 4 {
        return None;
    }

    let (am, bytes) = match bytes.get(bytes.len() - 3..) {
        Some(b" AM" | b" am" | b" Am" | b" aM") => (Some(true), &bytes[..bytes.len() - 3]),
        Some(b" PM" | b" pm" | b" pM" | b" Pm") => (Some(false), &bytes[..bytes.len() - 3]),
        _ => (None, bytes),
    };

    if bytes.len() < 4 {
        return None;
    }

    let mut digits = [b'0'; 6];

    // Extract hour
    let bytes = match (bytes[1], bytes[2]) {
        (b':', _) => {
            digits[1] = bytes[0];
            &bytes[2..]
        }
        (_, b':') => {
            digits[0] = bytes[0];
            digits[1] = bytes[1];
            &bytes[3..]
        }
        _ => return None,
    };

    if bytes.len() < 2 {
        return None; // Minutes required
    }

    // Extract minutes
    digits[2] = bytes[0];
    digits[3] = bytes[1];

    let nanoseconds = match bytes.get(2) {
        Some(b':') => {
            if bytes.len() < 5 {
                return None;
            }

            // Extract seconds
            digits[4] = bytes[3];
            digits[5] = bytes[4];

            // Extract sub-seconds if any
            match bytes.get(5) {
                Some(b'.') => {
                    let decimal = &bytes[6..];
                    if decimal.iter().any(|x| !x.is_ascii_digit()) {
                        return None;
                    }
                    match decimal.len() {
                        0 => return None,
                        1 => parse_nanos::<1, b'0'>(decimal),
                        2 => parse_nanos::<2, b'0'>(decimal),
                        3 => parse_nanos::<3, b'0'>(decimal),
                        4 => parse_nanos::<4, b'0'>(decimal),
                        5 => parse_nanos::<5, b'0'>(decimal),
                        6 => parse_nanos::<6, b'0'>(decimal),
                        7 => parse_nanos::<7, b'0'>(decimal),
                        8 => parse_nanos::<8, b'0'>(decimal),
                        _ => parse_nanos::<9, b'0'>(decimal),
                    }
                }
                Some(_) => return None,
                None => 0,
            }
        }
        Some(_) => return None,
        None => 0,
    };

    digits.iter_mut().for_each(|x| *x = x.wrapping_sub(b'0'));
    if digits.iter().any(|x| *x > 9) {
        return None;
    }

    let hour = match (digits[0] * 10 + digits[1], am) {
        (12, Some(true)) => 0,               // 12:00 AM -> 00:00
        (h @ 1..=11, Some(true)) => h,       // 1:00 AM -> 01:00
        (12, Some(false)) => 12,             // 12:00 PM -> 12:00
        (h @ 1..=11, Some(false)) => h + 12, // 1:00 PM -> 13:00
        (_, Some(_)) => return None,
        (h, None) => h,
    };

    // Handle leap second
    let (second, nanoseconds) = match digits[4] * 10 + digits[5] {
        60 => (59, nanoseconds + 1_000_000_000),
        s => (s, nanoseconds),
    };

    NaiveTime::from_hms_nano_opt(
        hour as _,
        (digits[2] * 10 + digits[3]) as _,
        second as _,
        nanoseconds,
    )
}

/// Specialized parsing implementations to convert strings to Arrow types.
///
/// This is used by csv and json reader and can be used directly as well.
///
/// # Example
///
/// To parse a string to a [`Date32Type`]:
///
/// ```
/// use arrow_cast::parse::Parser;
/// use arrow_array::types::Date32Type;
/// let date = Date32Type::parse("2021-01-01").unwrap();
/// assert_eq!(date, 18628);
/// ```
///
/// To parse a string to a [`TimestampNanosecondType`]:
///
/// ```
/// use arrow_cast::parse::Parser;
/// use arrow_array::types::TimestampNanosecondType;
/// let ts = TimestampNanosecondType::parse("2021-01-01T00:00:00.123456789Z").unwrap();
/// assert_eq!(ts, 1609459200123456789);
/// ```
pub trait Parser: ArrowPrimitiveType {
    /// Parse a string to the native type
    fn parse(string: &str) -> Option<Self::Native>;

    /// Parse a string to the native type with a format string
    ///
    /// When not implemented, the format string is unused, and this method is equivalent to [parse](#tymethod.parse)
    fn parse_formatted(string: &str, _format: &str) -> Option<Self::Native> {
        Self::parse(string)
    }
}

impl Parser for Float16Type {
    fn parse(string: &str) -> Option<f16> {
        if let Ok(raw_float) = lexical_core::parse(string.as_bytes()) {
            return Some(f16::from_f32(raw_float));
        }
        let string = trim_pre_and_post_whitespace(string);
        lexical_core::parse(string.as_bytes())
            .ok()
            .map(f16::from_f32)
    }
}

impl Parser for Float32Type {
    fn parse(string: &str) -> Option<f32> {
        if let Ok(raw_float) = lexical_core::parse(string.as_bytes()) {
            return Some(raw_float);
        }
        let string = trim_pre_and_post_whitespace(string);
        lexical_core::parse(string.as_bytes()).ok()
    }
}

impl Parser for Float64Type {
    fn parse(string: &str) -> Option<f64> {
        if let Ok(raw_float) = lexical_core::parse(string.as_bytes()) {
            return Some(raw_float);
        }
        let string = trim_pre_and_post_whitespace(string);
        lexical_core::parse(string.as_bytes()).ok()
    }
}

/// this is a no-op if the string starts and ends with a digit, otherwise it will trim whitespace from the start and end of the string.
#[inline]
fn trim_pre_and_post_whitespace(string: &str) -> &str {
    let bytes = string.as_bytes();
    let prefix = bytes.first().is_some_and(|b| !b.is_ascii_digit());
    let suffix = bytes.last().is_some_and(|b| !b.is_ascii_digit());
    match (prefix, suffix) {
        (false, false) => string,
        (true, false) => string.trim_ascii_start(),
        (false, true) => string.trim_ascii_end(),
        (true, true) => string.trim_ascii(),
    }
}

macro_rules! parser_primitive {
    ($t:ty) => {
        impl Parser for $t {
            fn parse(string: &str) -> Option<Self::Native> {
                let mut raw_bytes = string.as_bytes();
                if !raw_bytes.last().is_some_and(|x| x.is_ascii_digit()) {
                    raw_bytes = raw_bytes.trim_ascii_end();
                    if !raw_bytes.last().is_some_and(|x| x.is_ascii_digit()) {
                        return None;
                    }
                }
                match atoi::FromRadix10SignedChecked::from_radix_10_signed_checked(raw_bytes) {
                    (Some(n), x) if x == raw_bytes.len() => Some(n),
                    _ => {
                        let trimmed = raw_bytes.trim_ascii_start();
                        match atoi::FromRadix10SignedChecked::from_radix_10_signed_checked(trimmed)
                        {
                            (Some(n), x) if x == trimmed.len() => Some(n),
                            _ => None,
                        }
                    }
                }
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
parser_primitive!(DurationNanosecondType);
parser_primitive!(DurationMicrosecondType);
parser_primitive!(DurationMillisecondType);
parser_primitive!(DurationSecondType);

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
        Some(nt.num_seconds_from_midnight() as i64 * 1_000_000_000 + nt.nanosecond() as i64)
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
        Some(nt.num_seconds_from_midnight() as i64 * 1_000_000 + nt.nanosecond() as i64 / 1_000)
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
        Some(nt.num_seconds_from_midnight() as i32 * 1_000 + nt.nanosecond() as i32 / 1_000_000)
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
        Some(nt.num_seconds_from_midnight() as i32 + nt.nanosecond() as i32 / 1_000_000_000)
    }
}

/// Number of days between 0001-01-01 and 1970-01-01
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

/// Parse the ISO 8601 signed extended-year form (`±YYYY[Y...]-MM-DD`) into
/// raw `(year, month, day)` components, without validating the calendar date.
///
/// The caller must have already verified that `string` begins with `+` or `-`;
/// the year must have at least 4 digits. Returns `None` if the shape is
/// malformed or any component fails to parse numerically.
fn parse_extended_ymd(string: &str) -> Option<(i32, u32, u32)> {
    debug_assert!(string.starts_with('+') || string.starts_with('-'));
    // Skip the sign and look for the hyphen that terminates the year digits.
    // Per ISO 8601 the unsigned year part must be at least 4 digits.
    let rest = &string[1..];
    let hyphen = rest.find('-')?;
    if hyphen < 4 {
        return None;
    }
    // The year substring is the sign and the digits (but not the separator),
    // e.g. for "+10999-12-31", hyphen is 5 and s[..6] is "+10999".
    let year: i32 = string[..hyphen + 1].parse().ok()?;
    // The remainder should begin with a '-' which we strip off, leaving the month-day part.
    let remainder = string[hyphen + 1..].strip_prefix('-')?;
    let mut parts = remainder.splitn(2, '-');
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    Some((year, month, day))
}

fn parse_date(string: &str) -> Option<NaiveDate> {
    // If the date has an extended (signed) year such as "+10999-12-31" or "-0012-05-06"
    //
    // According to [ISO 8601], years have:
    //  Four digits or more for the year. Years in the range 0000 to 9999 will be pre-padded by
    //  zero to ensure four digits. Years outside that range will have a prefixed positive or negative symbol.
    //
    // [ISO 8601]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html#ISO_LOCAL_DATE
    if string.starts_with('+') || string.starts_with('-') {
        let (year, month, day) = parse_extended_ymd(string)?;
        return NaiveDate::from_ymd_opt(year, month, day);
    }

    if string.len() > 10 {
        // Try to parse as datetime and return just the date part
        return string_to_datetime(&Utc, string)
            .map(|dt| dt.date_naive())
            .ok();
    }
    let mut digits = [0; 10];
    let mut mask = 0;

    // Treating all bytes the same way, helps LLVM vectorise this correctly
    for (idx, (o, i)) in digits.iter_mut().zip(string.bytes()).enumerate() {
        *o = i.wrapping_sub(b'0');
        mask |= ((*o < 10) as u16) << idx
    }

    const HYPHEN: u8 = b'-'.wrapping_sub(b'0');

    //  refer to https://www.rfc-editor.org/rfc/rfc3339#section-3
    if digits[4] != HYPHEN {
        let (year, month, day) = match (mask, string.len()) {
            (0b11111111, 8) => (
                digits[0] as u16 * 1000
                    + digits[1] as u16 * 100
                    + digits[2] as u16 * 10
                    + digits[3] as u16,
                digits[4] * 10 + digits[5],
                digits[6] * 10 + digits[7],
            ),
            _ => return None,
        };
        return NaiveDate::from_ymd_opt(year as _, month as _, day as _);
    }

    let (month, day) = match mask {
        0b1101101111 => {
            if digits[7] != HYPHEN {
                return None;
            }
            (digits[5] * 10 + digits[6], digits[8] * 10 + digits[9])
        }
        0b101101111 => {
            if digits[7] != HYPHEN {
                return None;
            }
            (digits[5] * 10 + digits[6], digits[8])
        }
        0b110101111 => {
            if digits[6] != HYPHEN {
                return None;
            }
            (digits[5], digits[7] * 10 + digits[8])
        }
        0b10101111 => {
            if digits[6] != HYPHEN {
                return None;
            }
            (digits[5], digits[7])
        }
        _ => return None,
    };

    let year =
        digits[0] as u16 * 1000 + digits[1] as u16 * 100 + digits[2] as u16 * 10 + digits[3] as u16;

    NaiveDate::from_ymd_opt(year as _, month as _, day as _)
}

/// Parse a date string into days since 1970-01-01, covering the full
/// `Date32` range (years ≈ ±5,881,580) for the signed extended-year form.
///
/// The Gregorian calendar repeats exactly every 400 years (146,097 days), so
/// we fold the year into `[0, 400)`, validate the folded date, and add
/// `era * 146_097` to recover the absolute day count.
///
/// For all other inputs, behavior matches [`parse_date`].
fn parse_date_to_days(string: &str) -> Option<i32> {
    if string.starts_with('+') || string.starts_with('-') {
        let (year, month, day) = parse_extended_ymd(string)?;
        let y = year as i64;
        let era = y.div_euclid(400);
        let yoe = y.rem_euclid(400) as i32;
        let nd = NaiveDate::from_ymd_opt(yoe, month, day)?;
        let in_era = (nd.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i64;
        return i32::try_from(era * 146_097 + in_era).ok();
    }
    parse_date(string).map(|nd| nd.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
}

impl Parser for Date32Type {
    fn parse(string: &str) -> Option<i32> {
        parse_date_to_days(string)
    }

    fn parse_formatted(string: &str, format: &str) -> Option<i32> {
        let date = NaiveDate::parse_from_str(string, format).ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }
}

impl Parser for Date64Type {
    fn parse(string: &str) -> Option<i64> {
        if string.len() <= 10 {
            let datetime = NaiveDateTime::new(parse_date(string)?, NaiveTime::default());
            Some(datetime.and_utc().timestamp_millis())
        } else {
            let date_time = string_to_datetime(&Utc, string).ok()?;
            Some(date_time.timestamp_millis())
        }
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
            Some(date_time.and_utc().timestamp_millis())
        }
    }
}

/// Parses the string representation of a decimal number into the unscaled
/// native value of a decimal type with the given `precision` and `scale`.
///
/// The accepted syntax is:
///
/// ```text
/// [whitespace] [+|-] digits [. [digits]] [(e|E) [+|-] digits] [whitespace]
/// ```
///
/// or the same with the integer digits omitted (e.g. `.5`), as long as at
/// least one digit is present in the mantissa. ASCII whitespace is trimmed
/// from both ends. The exponent is applied before scaling, so `1.5e2` and
/// `150` parse identically.
///
/// Fractional digits beyond `scale` are not stored but round the result half
/// away from zero (e.g. `1.005` at scale 2 is `101`, `-1.005` is `-101`).
/// Negative scales are supported and round the integer part in the same way
/// (e.g. `150` at scale -2 is `2`).
///
/// Returns an error if the input is not a valid decimal string, or if the
/// result does not fit the given precision.
///
/// # Example
///
/// ```
/// # use arrow_array::types::Decimal128Type;
/// # use arrow_cast::parse::parse_decimal;
/// assert_eq!(parse_decimal::<Decimal128Type>("123.45", 10, 2).unwrap(), 12345);
/// assert_eq!(parse_decimal::<Decimal128Type>("1.005", 10, 2).unwrap(), 101);
/// assert_eq!(parse_decimal::<Decimal128Type>("1.5e2", 10, 0).unwrap(), 150);
/// assert!(parse_decimal::<Decimal128Type>("1234.5", 5, 2).is_err()); // does not fit
/// ```
pub fn parse_decimal<T: DecimalType>(
    s: &str,
    precision: u8,
    scale: i8,
) -> Result<T::Native, ArrowError> {
    parse_decimal_checked::<T>(s, precision, scale).map_err(|e| match e {
        DecimalParseError::Overflow => ArrowError::ParseError(format!(
            "{s:?} does not fit in {}({precision}, {scale})",
            T::PREFIX
        )),
        DecimalParseError::InvalidFormat => {
            ArrowError::ParseError(format!("Invalid decimal format: {s:?}"))
        }
    })
}

/// The reason a decimal string could not be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecimalParseError {
    /// The input is not a valid decimal string
    InvalidFormat,
    /// The value does not fit in the precision or the native type of the decimal
    Overflow,
}

/// Like [`parse_decimal`], but reports failures as a [`DecimalParseError`]
/// instead of formatting an error message, for callers that discard or
/// re-wrap the error.
pub(crate) fn parse_decimal_checked<T: DecimalType>(
    s: &str,
    precision: u8,
    scale: i8,
) -> Result<T::Native, DecimalParseError> {
    let value = parse_decimal_native::<T>(s, scale)?;
    if T::is_valid_decimal_precision(value, precision) {
        Ok(value)
    } else {
        Err(DecimalParseError::Overflow)
    }
}

/// Parses `s` as a decimal with the given `scale` into the native type of `T`,
/// checking only that the result fits the native type (not the precision).
///
/// See [`parse_decimal`] for the accepted syntax and rounding behaviour.
fn parse_decimal_native<T: DecimalType>(
    s: &str,
    scale: i8,
) -> Result<T::Native, DecimalParseError> {
    let bytes = s.as_bytes().trim_ascii();
    let (negative, mut mantissa) = split_sign(bytes);

    let mut scale = scale as i64;
    loop {
        let exponent_at = match parse_decimal_mantissa::<T>(mantissa, negative, scale) {
            Ok(value) => return Ok(value),
            Err(MantissaError::InvalidFormat) => return Err(DecimalParseError::InvalidFormat),
            Err(MantissaError::Exponent(index)) => index,
            // The digits before an exponent marker need not fit on their own
            // (e.g. "4825037936439135476E-14"), so the overflow only stands
            // if no marker follows
            Err(MantissaError::Overflow) => mantissa
                .iter()
                .position(|b| matches!(b, b'e' | b'E'))
                .ok_or(DecimalParseError::Overflow)?,
        };

        // If we saw an exponent, update the effective scale and rescan.
        // Exponents are rare, so the risk of repeated work is preferable to
        // the cost of scanning ahead for an exponent marker for every input.
        let exponent = parse_decimal_exponent(&mantissa[exponent_at + 1..])?;
        scale = scale.saturating_add(exponent);

        // Trim the exponent so the next iteration succeeds without rescanning
        mantissa = &mantissa[..exponent_at];
    }
}

/// Why scanning the digits of a decimal string stopped.
enum MantissaError {
    /// The input is not a valid decimal string
    InvalidFormat,
    /// The value does not fit in the native type
    Overflow,
    /// An exponent marker (`e` or `E`) was found at the given byte offset
    Exponent(usize),
}

impl From<DecimalParseError> for MantissaError {
    fn from(e: DecimalParseError) -> Self {
        match e {
            DecimalParseError::InvalidFormat => Self::InvalidFormat,
            DecimalParseError::Overflow => Self::Overflow,
        }
    }
}

/// The maximum number of decimal digits accumulated in a `u64` before the
/// chunk is folded into the native value: every 18-digit number fits in a
/// `u64`, and splits into two halves that each fit in a `u32` (see
/// [`decimal_chunk_to_native`]).
const MAX_CHUNK_DIGITS: usize = 18;

/// Scans `mantissa` (digits with at most one decimal point; the sign has
/// already been removed) and folds the digits that are significant at
/// `scale` into a native value, rounding half away from zero on the first
/// digit that is not.
#[inline]
fn parse_decimal_mantissa<T: DecimalType>(
    mantissa: &[u8],
    negative: bool,
    scale: i64,
) -> Result<T::Native, MantissaError> {
    // The number of integer and fractional digits that contribute to the
    // result. For a non-negative scale that is every integer digit and the
    // first `scale` fractional digits. For a negative scale the last `-scale`
    // integer digits (and every fractional digit) only matter for rounding.
    let (int_keep, frac_keep, mut round) = if scale >= 0 {
        (
            usize::MAX,
            usize::try_from(scale).unwrap_or(usize::MAX),
            true,
        )
    } else {
        let int_digits = mantissa.iter().take_while(|b| b.is_ascii_digit()).count();
        match usize::try_from(int_digits as i64 + scale) {
            Ok(keep) => (keep, 0, true),
            // Even the first digit is more than one position below the
            // least significant digit of the result: the value rounds to
            // zero regardless of what the digits are
            Err(_) => (0, 0, false),
        }
    };

    let mut value = T::Native::ZERO;
    let mut chunk = 0_u64;
    let mut chunk_len = 0_usize;
    let mut saw_point = false;
    let mut int_kept = 0_usize;
    let mut frac_kept = 0_usize;
    let mut first_discarded_digit = None;

    let mut index = 0;
    while let Some(&b) = mantissa.get(index) {
        match b {
            b'0'..=b'9' => {
                let digit = b - b'0';
                let (kept, keep) = if saw_point {
                    (&mut frac_kept, frac_keep)
                } else {
                    (&mut int_kept, int_keep)
                };
                if *kept < keep {
                    *kept += 1;
                    // Cannot overflow: the chunk is folded into `value` before it
                    // exceeds MAX_CHUNK_DIGITS digits, all of which fit in a u64
                    chunk = chunk * 10 + digit as u64;
                    chunk_len += 1;
                    if chunk_len == MAX_CHUNK_DIGITS {
                        value = fold_decimal_chunk::<T>(value, chunk, chunk_len, negative)?;
                        chunk = 0;
                        chunk_len = 0;
                    }
                } else {
                    first_discarded_digit.get_or_insert(digit);
                }
            }
            b'.' if !saw_point => saw_point = true,
            b'e' | b'E' => return Err(MantissaError::Exponent(index)),
            _ => return Err(MantissaError::InvalidFormat),
        }
        index += 1;
    }

    if chunk_len > 0 {
        value = fold_decimal_chunk::<T>(value, chunk, chunk_len, negative)?;
    }

    if int_kept == 0 && frac_kept == 0 && first_discarded_digit.is_none() {
        return Err(MantissaError::InvalidFormat);
    }

    // Scale the value up to the target scale. Skipped for zero, where computing
    // 10^missing could overflow the native type even though the result (zero)
    // is always representable.
    let missing = scale - frac_kept as i64;
    if missing > 0 && !value.is_zero() {
        value = value
            .mul_checked(decimal_pow::<T>(missing)?)
            .map_err(|_| MantissaError::Overflow)?;
    }

    round &= first_discarded_digit.is_some_and(|digit| digit >= 5);
    if round {
        value = if negative {
            value.sub_checked(T::Native::ONE)
        } else {
            value.add_checked(T::Native::ONE)
        }
        .map_err(|_| MantissaError::Overflow)?;
    }

    Ok(value)
}

/// Parses the digits of an exponent (`[+|-] digits`), saturating at the bounds
/// of `i64`; any exponent that large scales every non-zero mantissa out of
/// range of every decimal type.
fn parse_decimal_exponent(exponent: &[u8]) -> Result<i64, DecimalParseError> {
    let (negative, digits) = split_sign(exponent);
    if digits.is_empty() {
        return Err(DecimalParseError::InvalidFormat);
    }
    let mut value = 0_i64;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Err(DecimalParseError::InvalidFormat);
        }
        value = value.saturating_mul(10).saturating_add((b - b'0') as i64);
    }
    Ok(if negative { -value } else { value })
}

/// Splits an optional leading sign from `bytes`, returning whether it is `-`
/// and the bytes that follow it.
#[inline]
fn split_sign(bytes: &[u8]) -> (bool, &[u8]) {
    match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        Some(b'+') => (false, &bytes[1..]),
        _ => (false, bytes),
    }
}

/// Folds a chunk of up to [`MAX_CHUNK_DIGITS`] digits into `value`, producing
/// `value * 10^chunk_len + chunk` (`chunk` is negated first when parsing a
/// negative number).
#[inline]
fn fold_decimal_chunk<T: DecimalType>(
    value: T::Native,
    chunk: u64,
    chunk_len: usize,
    negative: bool,
) -> Result<T::Native, DecimalParseError> {
    let chunk = decimal_chunk_to_native::<T>(chunk, negative)?;

    // When `value` is zero the multiply would be a no-op; skipping it avoids
    // computing 10^chunk_len, which can overflow a narrow native type even
    // though the result (the chunk itself) is representable.
    if value.is_zero() {
        return Ok(chunk);
    }

    value
        .mul_checked(decimal_pow::<T>(chunk_len as i64)?)
        .map_err(|_| DecimalParseError::Overflow)?
        .add_checked(chunk)
        .map_err(|_| DecimalParseError::Overflow)
}

/// Converts a chunk of at most [`MAX_CHUNK_DIGITS`] digits to the native type,
/// negated if `negative`.
#[inline]
fn decimal_chunk_to_native<T: DecimalType>(
    chunk: u64,
    negative: bool,
) -> Result<T::Native, DecimalParseError> {
    // Every native type can represent +/- 10^9, so a chunk below that converts
    // losslessly through usize on every target. So does any chunk when the
    // native type holds MAX_CHUNK_DIGITS digits and usize holds a u64.
    const HALF: u64 = 1_000_000_000;
    if chunk < HALF || (T::MAX_PRECISION as usize >= MAX_CHUNK_DIGITS && usize::BITS >= 64) {
        let chunk = T::Native::usize_as(chunk as usize);
        // `ZERO.sub_wrapping` rather than `neg_wrapping`: the latter compiles
        // to measurably slower code for i256 (~10% on casting strings to
        // Decimal256)
        return Ok(if negative {
            T::Native::ZERO.sub_wrapping(chunk)
        } else {
            chunk
        });
    }
    // Otherwise narrow the chunk in two halves that are each below 10^9
    let low = T::Native::usize_as((chunk % HALF) as usize);
    let high = T::Native::usize_as((chunk / HALF) as usize)
        .mul_checked(T::Native::usize_as(HALF as usize))
        .map_err(|_| DecimalParseError::Overflow)?;
    // Negate before combining so that a chunk with the magnitude of the
    // native type's minimum value (e.g. "2147483648" for Decimal32) remains
    // representable
    if negative {
        T::Native::ZERO
            .sub_checked(high)
            .map_err(|_| DecimalParseError::Overflow)?
            .sub_checked(low)
            .map_err(|_| DecimalParseError::Overflow)
    } else {
        high.add_checked(low)
            .map_err(|_| DecimalParseError::Overflow)
    }
}

/// Returns `10^exp` as a `T::Native`, or an overflow error if the result does
/// not fit in the native type.
#[inline]
fn decimal_pow<T: DecimalType>(exp: i64) -> Result<T::Native, DecimalParseError> {
    // T::MAX_FOR_EACH_PRECISION[k] holds 10^k - 1, so adding one yields 10^k
    // without computing a power at runtime. Exponents beyond the table always
    // overflow: the native type cannot hold 10^(MAX_PRECISION + 1).
    usize::try_from(exp)
        .ok()
        .and_then(|exp| T::MAX_FOR_EACH_PRECISION.get(exp))
        .map(|max| max.add_wrapping(T::Native::ONE))
        .ok_or(DecimalParseError::Overflow)
}

/// Parse human-readable interval string to Arrow [IntervalYearMonthType]
pub fn parse_interval_year_month(
    value: &str,
) -> Result<<IntervalYearMonthType as ArrowPrimitiveType>::Native, ArrowError> {
    let config = IntervalParseConfig::new(IntervalUnit::Year);
    let interval = Interval::parse(value, &config)?;

    let months = interval.to_year_months().map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast {value} to IntervalYearMonth. Only year and month fields are allowed."
        ))
    })?;

    Ok(IntervalYearMonthType::make_value(0, months))
}

/// Parse human-readable interval string to Arrow [IntervalDayTimeType]
pub fn parse_interval_day_time(
    value: &str,
) -> Result<<IntervalDayTimeType as ArrowPrimitiveType>::Native, ArrowError> {
    let config = IntervalParseConfig::new(IntervalUnit::Day);
    let interval = Interval::parse(value, &config)?;

    let (days, millis) = interval.to_day_time().map_err(|_| ArrowError::CastError(format!(
        "Cannot cast {value} to IntervalDayTime because the nanos part isn't multiple of milliseconds"
    )))?;

    Ok(IntervalDayTimeType::make_value(days, millis))
}

/// Parse human-readable interval string to Arrow [IntervalMonthDayNanoType]
pub fn parse_interval_month_day_nano_config(
    value: &str,
    config: IntervalParseConfig,
) -> Result<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native, ArrowError> {
    let interval = Interval::parse(value, &config)?;

    let (months, days, nanos) = interval.to_month_day_nanos();

    Ok(IntervalMonthDayNanoType::make_value(months, days, nanos))
}

/// Parse human-readable interval string to Arrow [IntervalMonthDayNanoType]
pub fn parse_interval_month_day_nano(
    value: &str,
) -> Result<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native, ArrowError> {
    parse_interval_month_day_nano_config(value, IntervalParseConfig::new(IntervalUnit::Month))
}

const NANOS_PER_MILLIS: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000 * NANOS_PER_MILLIS;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
#[cfg(test)]
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;

/// Config to parse interval strings
///
/// Currently stores the `default_unit` to use if the string doesn't have one specified
#[derive(Debug, Clone)]
pub struct IntervalParseConfig {
    /// The default unit to use if none is specified
    /// e.g. `INTERVAL 1` represents `INTERVAL 1 SECOND` when default_unit = [IntervalUnit::Second]
    default_unit: IntervalUnit,
}

impl IntervalParseConfig {
    /// Create a new [IntervalParseConfig] with the given default unit
    pub fn new(default_unit: IntervalUnit) -> Self {
        Self { default_unit }
    }
}

#[rustfmt::skip]
#[derive(Debug, Clone, Copy)]
#[repr(u16)]
/// Represents the units of an interval, with each variant
/// corresponding to a bit in the interval's bitfield representation
pub enum IntervalUnit {
    /// A Century
    Century     = 0b_0000_0000_0001,
    /// A Decade
    Decade      = 0b_0000_0000_0010,
    /// A Year
    Year        = 0b_0000_0000_0100,
    /// A Month
    Month       = 0b_0000_0000_1000,
    /// A Week
    Week        = 0b_0000_0001_0000,
    /// A Day
    Day         = 0b_0000_0010_0000,
    /// An Hour
    Hour        = 0b_0000_0100_0000,
    /// A Minute
    Minute      = 0b_0000_1000_0000,
    /// A Second
    Second      = 0b_0001_0000_0000,
    /// A Millisecond
    Millisecond = 0b_0010_0000_0000,
    /// A Microsecond
    Microsecond = 0b_0100_0000_0000,
    /// A Nanosecond
    Nanosecond  = 0b_1000_0000_0000,
}

/// Logic for parsing interval unit strings
///
/// See <https://github.com/postgres/postgres/blob/2caa85f4aae689e6f6721d7363b4c66a2a6417d6/src/backend/utils/adt/datetime.c#L189>
/// for a list of unit names supported by PostgreSQL which we try to match here.
impl FromStr for IntervalUnit {
    type Err = ArrowError;

    fn from_str(s: &str) -> Result<Self, ArrowError> {
        match s.to_lowercase().as_str() {
            "c" | "cent" | "cents" | "century" | "centuries" => Ok(Self::Century),
            "dec" | "decs" | "decade" | "decades" => Ok(Self::Decade),
            "y" | "yr" | "yrs" | "year" | "years" => Ok(Self::Year),
            "mon" | "mons" | "month" | "months" => Ok(Self::Month),
            "w" | "week" | "weeks" => Ok(Self::Week),
            "d" | "day" | "days" => Ok(Self::Day),
            "h" | "hr" | "hrs" | "hour" | "hours" => Ok(Self::Hour),
            "m" | "min" | "mins" | "minute" | "minutes" => Ok(Self::Minute),
            "s" | "sec" | "secs" | "second" | "seconds" => Ok(Self::Second),
            "ms" | "msec" | "msecs" | "msecond" | "mseconds" | "millisecond" | "milliseconds" => {
                Ok(Self::Millisecond)
            }
            "us" | "usec" | "usecs" | "usecond" | "useconds" | "microsecond" | "microseconds" => {
                Ok(Self::Microsecond)
            }
            "nanosecond" | "nanoseconds" => Ok(Self::Nanosecond),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Unknown interval type: {s}"
            ))),
        }
    }
}

impl IntervalUnit {
    fn from_str_or_config(
        s: Option<&str>,
        config: &IntervalParseConfig,
    ) -> Result<Self, ArrowError> {
        match s {
            Some(s) => s.parse(),
            None => Ok(config.default_unit),
        }
    }
}

/// A tuple representing (months, days, nanoseconds) in an interval
pub type MonthDayNano = (i32, i32, i64);

/// Chosen based on the number of decimal digits in 1 week in nanoseconds
const INTERVAL_PRECISION: u32 = 15;

#[derive(Clone, Copy, Debug, PartialEq)]
struct IntervalAmount {
    /// The integer component of the interval amount
    integer: i64,
    /// The fractional component multiplied by 10^INTERVAL_PRECISION
    frac: i64,
}

#[cfg(test)]
impl IntervalAmount {
    fn new(integer: i64, frac: i64) -> Self {
        Self { integer, frac }
    }
}

impl FromStr for IntervalAmount {
    type Err = ArrowError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('.') {
            Some((integer, frac))
                if frac.len() <= INTERVAL_PRECISION as usize
                    && !frac.is_empty()
                    && !frac.starts_with('-') =>
            {
                // integer will be "" for values like ".5"
                // and "-" for values like "-.5"
                let explicit_neg = integer.starts_with('-');
                let integer = if integer.is_empty() || integer == "-" {
                    Ok(0)
                } else {
                    integer.parse::<i64>().map_err(|_| {
                        ArrowError::ParseError(format!("Failed to parse {s} as interval amount"))
                    })
                }?;

                let frac_unscaled = frac.parse::<i64>().map_err(|_| {
                    ArrowError::ParseError(format!("Failed to parse {s} as interval amount"))
                })?;

                // scale fractional part by interval precision
                let frac = frac_unscaled * 10_i64.pow(INTERVAL_PRECISION - frac.len() as u32);

                // propagate the sign of the integer part to the fractional part
                let frac = if integer < 0 || explicit_neg {
                    -frac
                } else {
                    frac
                };

                let result = Self { integer, frac };

                Ok(result)
            }
            Some((_, frac)) if frac.starts_with('-') => Err(ArrowError::ParseError(format!(
                "Failed to parse {s} as interval amount"
            ))),
            Some((_, frac)) if frac.len() > INTERVAL_PRECISION as usize => {
                Err(ArrowError::ParseError(format!(
                    "{s} exceeds the precision available for interval amount"
                )))
            }
            Some(_) | None => {
                let integer = s.parse::<i64>().map_err(|_| {
                    ArrowError::ParseError(format!("Failed to parse {s} as interval amount"))
                })?;

                let result = Self { integer, frac: 0 };
                Ok(result)
            }
        }
    }
}

#[derive(Debug, Default, PartialEq)]
struct Interval {
    months: i32,
    days: i32,
    nanos: i64,
}

impl Interval {
    fn new(months: i32, days: i32, nanos: i64) -> Self {
        Self {
            months,
            days,
            nanos,
        }
    }

    fn to_year_months(&self) -> Result<i32, ArrowError> {
        match (self.months, self.days, self.nanos) {
            (months, days, nanos) if days == 0 && nanos == 0 => Ok(months),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Unable to represent interval with days and nanos as year-months: {self:?}"
            ))),
        }
    }

    fn to_day_time(&self) -> Result<(i32, i32), ArrowError> {
        let days = self.months.mul_checked(30)?.add_checked(self.days)?;

        match self.nanos {
            nanos if nanos % NANOS_PER_MILLIS == 0 => {
                let millis = (self.nanos / 1_000_000).try_into().map_err(|_| {
                    ArrowError::InvalidArgumentError(format!(
                        "Unable to represent {} nanos as milliseconds in a signed 32-bit integer",
                        self.nanos
                    ))
                })?;

                Ok((days, millis))
            }
            nanos => Err(ArrowError::InvalidArgumentError(format!(
                "Unable to represent {nanos} as milliseconds"
            ))),
        }
    }

    fn to_month_day_nanos(&self) -> (i32, i32, i64) {
        (self.months, self.days, self.nanos)
    }

    /// Parse string value in traditional Postgres format such as
    /// `1 year 2 months 3 days 4 hours 5 minutes 6 seconds`
    fn parse(value: &str, config: &IntervalParseConfig) -> Result<Self, ArrowError> {
        let components = parse_interval_components(value, config)?;

        components
            .into_iter()
            .try_fold(Self::default(), |result, (amount, unit)| {
                result.add(amount, unit)
            })
    }

    /// Interval addition following Postgres behavior. Fractional units will be spilled into smaller units.
    /// When the interval unit is larger than months, the result is rounded to total months and not spilled to days/nanos.
    /// Fractional parts of weeks and days are represented using days and nanoseconds.
    /// e.g. INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
    /// e.g. INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
    /// [Postgres reference](https://www.postgresql.org/docs/15/datatype-datetime.html#DATATYPE-INTERVAL-INPUT:~:text=Field%20values%20can,fractional%20on%20output.)
    fn add(&self, amount: IntervalAmount, unit: IntervalUnit) -> Result<Self, ArrowError> {
        let result = match unit {
            IntervalUnit::Century => {
                let months_int = amount.integer.mul_checked(100)?.mul_checked(12)?;
                let month_frac = amount.frac * 12 / 10_i64.pow(INTERVAL_PRECISION - 2);
                let months = months_int
                    .add_checked(month_frac)?
                    .try_into()
                    .map_err(|_| {
                        ArrowError::ParseError(format!(
                            "Unable to represent {} centuries as months in a signed 32-bit integer",
                            amount.integer
                        ))
                    })?;

                Self::new(self.months.add_checked(months)?, self.days, self.nanos)
            }
            IntervalUnit::Decade => {
                let months_int = amount.integer.mul_checked(10)?.mul_checked(12)?;

                let month_frac = amount.frac * 12 / 10_i64.pow(INTERVAL_PRECISION - 1);
                let months = months_int
                    .add_checked(month_frac)?
                    .try_into()
                    .map_err(|_| {
                        ArrowError::ParseError(format!(
                            "Unable to represent {} decades as months in a signed 32-bit integer",
                            amount.integer
                        ))
                    })?;

                Self::new(self.months.add_checked(months)?, self.days, self.nanos)
            }
            IntervalUnit::Year => {
                let months_int = amount.integer.mul_checked(12)?;
                let month_frac = amount.frac * 12 / 10_i64.pow(INTERVAL_PRECISION);
                let months = months_int
                    .add_checked(month_frac)?
                    .try_into()
                    .map_err(|_| {
                        ArrowError::ParseError(format!(
                            "Unable to represent {} years as months in a signed 32-bit integer",
                            amount.integer
                        ))
                    })?;

                Self::new(self.months.add_checked(months)?, self.days, self.nanos)
            }
            IntervalUnit::Month => {
                let months = amount.integer.try_into().map_err(|_| {
                    ArrowError::ParseError(format!(
                        "Unable to represent {} months in a signed 32-bit integer",
                        amount.integer
                    ))
                })?;

                let days = amount.frac * 3 / 10_i64.pow(INTERVAL_PRECISION - 1);
                let days = days.try_into().map_err(|_| {
                    ArrowError::ParseError(format!(
                        "Unable to represent {} months as days in a signed 32-bit integer",
                        amount.frac / 10_i64.pow(INTERVAL_PRECISION)
                    ))
                })?;

                Self::new(
                    self.months.add_checked(months)?,
                    self.days.add_checked(days)?,
                    self.nanos,
                )
            }
            IntervalUnit::Week => {
                let days = amount.integer.mul_checked(7)?.try_into().map_err(|_| {
                    ArrowError::ParseError(format!(
                        "Unable to represent {} weeks as days in a signed 32-bit integer",
                        amount.integer
                    ))
                })?;

                let nanos = amount.frac * 7 * 24 * 6 * 6 / 10_i64.pow(INTERVAL_PRECISION - 11);

                Self::new(
                    self.months,
                    self.days.add_checked(days)?,
                    self.nanos.add_checked(nanos)?,
                )
            }
            IntervalUnit::Day => {
                let days = amount.integer.try_into().map_err(|_| {
                    ArrowError::InvalidArgumentError(format!(
                        "Unable to represent {} days in a signed 32-bit integer",
                        amount.integer
                    ))
                })?;

                let nanos = amount.frac * 24 * 6 * 6 / 10_i64.pow(INTERVAL_PRECISION - 11);

                Self::new(
                    self.months,
                    self.days.add_checked(days)?,
                    self.nanos.add_checked(nanos)?,
                )
            }
            IntervalUnit::Hour => {
                let nanos_int = amount.integer.mul_checked(NANOS_PER_HOUR)?;
                let nanos_frac = amount.frac * 6 * 6 / 10_i64.pow(INTERVAL_PRECISION - 11);
                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
            IntervalUnit::Minute => {
                let nanos_int = amount.integer.mul_checked(NANOS_PER_MINUTE)?;
                let nanos_frac = amount.frac * 6 / 10_i64.pow(INTERVAL_PRECISION - 10);

                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
            IntervalUnit::Second => {
                let nanos_int = amount.integer.mul_checked(NANOS_PER_SECOND)?;
                let nanos_frac = amount.frac / 10_i64.pow(INTERVAL_PRECISION - 9);
                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
            IntervalUnit::Millisecond => {
                let nanos_int = amount.integer.mul_checked(NANOS_PER_MILLIS)?;
                let nanos_frac = amount.frac / 10_i64.pow(INTERVAL_PRECISION - 6);
                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
            IntervalUnit::Microsecond => {
                let nanos_int = amount.integer.mul_checked(1_000)?;
                let nanos_frac = amount.frac / 10_i64.pow(INTERVAL_PRECISION - 3);
                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
            IntervalUnit::Nanosecond => {
                let nanos_int = amount.integer;
                let nanos_frac = amount.frac / 10_i64.pow(INTERVAL_PRECISION);
                let nanos = nanos_int.add_checked(nanos_frac)?;

                Interval::new(self.months, self.days, self.nanos.add_checked(nanos)?)
            }
        };

        Ok(result)
    }
}

/// parse the string into a vector of interval components i.e. (amount, unit) tuples
fn parse_interval_components(
    value: &str,
    config: &IntervalParseConfig,
) -> Result<Vec<(IntervalAmount, IntervalUnit)>, ArrowError> {
    let raw_pairs = split_interval_components(value);

    // parse amounts and units
    let Ok(pairs): Result<Vec<(IntervalAmount, IntervalUnit)>, ArrowError> = raw_pairs
        .iter()
        .map(|(a, u)| Ok((a.parse()?, IntervalUnit::from_str_or_config(*u, config)?)))
        .collect()
    else {
        return Err(ArrowError::ParseError(format!(
            "Invalid input syntax for type interval: {value:?}"
        )));
    };

    // collect parsed results
    let (amounts, units): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();

    // duplicate units?
    let mut observed_interval_types = 0;
    for (unit, (_, raw_unit)) in units.iter().zip(raw_pairs) {
        if observed_interval_types & (*unit as u16) != 0 {
            return Err(ArrowError::ParseError(format!(
                "Invalid input syntax for type interval: {:?}. Repeated type '{}'",
                value,
                raw_unit.unwrap_or_default(),
            )));
        }

        observed_interval_types |= *unit as u16;
    }

    let result = amounts.iter().copied().zip(units.iter().copied());

    Ok(result.collect::<Vec<_>>())
}

/// Split an interval into a vec of amounts and units.
///
/// Pairs are separated by spaces, but within a pair the amount and unit may or may not be separated by a space.
///
/// This should match the behavior of PostgreSQL's interval parser.
fn split_interval_components(value: &str) -> Vec<(&str, Option<&str>)> {
    let mut result = vec![];
    let mut words = value.split(char::is_whitespace);
    while let Some(word) = words.next() {
        if let Some(split_word_at) = word.find(not_interval_amount) {
            let (amount, unit) = word.split_at(split_word_at);
            result.push((amount, Some(unit)));
        } else if let Some(unit) = words.next() {
            result.push((word, Some(unit)));
        } else {
            result.push((word, None));
            break;
        }
    }
    result
}

/// test if a character is NOT part of an interval numeric amount
fn not_interval_amount(c: char) -> bool {
    !c.is_ascii_digit() && c != '.' && c != '-'
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::temporal_conversions::date32_to_datetime;
    use arrow_buffer::i256;

    #[test]
    fn test_parse_nanos() {
        assert_eq!(parse_nanos::<3, 0>(&[1, 2, 3]), 123_000_000);
        assert_eq!(parse_nanos::<5, 0>(&[1, 2, 3, 4, 5]), 123_450_000);
        assert_eq!(parse_nanos::<6, b'0'>(b"123456"), 123_456_000);
    }

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
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08T13:42:29.190855").unwrap()
        );

        assert_eq!(
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08 13:42:29.190855").unwrap()
        );

        // Also ensure that parsing timestamps with no fractional
        // second part works as well
        let datetime_whole_secs = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_opt(13, 42, 29).unwrap(),
        )
        .and_utc();

        // Ensure both T and ' ' variants work
        assert_eq!(
            datetime_whole_secs.timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08T13:42:29").unwrap()
        );

        assert_eq!(
            datetime_whole_secs.timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08 13:42:29").unwrap()
        );

        // ensure without time work
        // no time, should be the nano second at
        // 2020-09-08 0:0:0
        let datetime_no_time = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
        .and_utc();

        assert_eq!(
            datetime_no_time.timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08").unwrap()
        )
    }

    #[test]
    fn string_to_timestamp_chrono() {
        let cases = [
            "2020-09-08T13:42:29Z",
            "1969-01-01T00:00:00.1Z",
            "2020-09-08T12:00:12.12345678+00:00",
            "2020-09-08T12:00:12+00:00",
            "2020-09-08T12:00:12.1+00:00",
            "2020-09-08T12:00:12.12+00:00",
            "2020-09-08T12:00:12.123+00:00",
            "2020-09-08T12:00:12.1234+00:00",
            "2020-09-08T12:00:12.12345+00:00",
            "2020-09-08T12:00:12.123456+00:00",
            "2020-09-08T12:00:12.1234567+00:00",
            "2020-09-08T12:00:12.12345678+00:00",
            "2020-09-08T12:00:12.123456789+00:00",
            "2020-09-08T12:00:12.12345678912z",
            "2020-09-08T12:00:12.123456789123Z",
            "2020-09-08T12:00:12.123456789123+02:00",
            "2020-09-08T12:00:12.12345678912345Z",
            "2020-09-08T12:00:12.1234567891234567+02:00",
            "2020-09-08T12:00:60Z",
            "2020-09-08T12:00:60.123Z",
            "2020-09-08T12:00:60.123456+02:00",
            "2020-09-08T12:00:60.1234567891234567+02:00",
            "2020-09-08T12:00:60.999999999+02:00",
            "2020-09-08t12:00:12.12345678+00:00",
            "2020-09-08t12:00:12+00:00",
            "2020-09-08t12:00:12Z",
        ];

        for case in cases {
            let chrono = DateTime::parse_from_rfc3339(case).unwrap();
            let chrono_utc = chrono.with_timezone(&Utc);

            let custom = string_to_datetime(&Utc, case).unwrap();
            assert_eq!(chrono_utc, custom)
        }
    }

    #[test]
    fn string_to_timestamp_naive() {
        let cases = [
            "2018-11-13T17:11:10.011375885995",
            "2030-12-04T17:11:10.123",
            "2030-12-04T17:11:10.1234",
            "2030-12-04T17:11:10.123456",
        ];
        for case in cases {
            let chrono = NaiveDateTime::parse_from_str(case, "%Y-%m-%dT%H:%M:%S%.f").unwrap();
            let custom = string_to_datetime(&Utc, case).unwrap();
            assert_eq!(chrono, custom.naive_utc())
        }
    }

    #[test]
    fn string_to_timestamp_invalid() {
        // Test parsing invalid formats
        let cases = [
            ("", "timestamp must contain at least 10 characters"),
            ("SS", "timestamp must contain at least 10 characters"),
            ("Wed, 18 Feb 2015 23:16:09 GMT", "error parsing date"),
            ("1997-01-31H09:26:56.123Z", "invalid timestamp separator"),
            ("1997-01-31  09:26:56.123Z", "error parsing time"),
            ("1997:01:31T09:26:56.123Z", "error parsing date"),
            ("1997:1:31T09:26:56.123Z", "error parsing date"),
            ("1997-01-32T09:26:56.123Z", "error parsing date"),
            ("1997-13-32T09:26:56.123Z", "error parsing date"),
            ("1997-02-29T09:26:56.123Z", "error parsing date"),
            ("2015-02-30T17:35:20-08:00", "error parsing date"),
            ("1997-01-10T9:26:56.123Z", "error parsing time"),
            ("2015-01-20T25:35:20-08:00", "error parsing time"),
            ("1997-01-10T09:61:56.123Z", "error parsing time"),
            ("1997-01-10T09:61:90.123Z", "error parsing time"),
            ("1997-01-10T12:00:6.123Z", "error parsing time"),
            ("1997-01-31T092656.123Z", "error parsing time"),
            ("1997-01-10T12:00:06.", "error parsing time"),
            ("1997-01-10T12:00:06. ", "error parsing time"),
        ];

        for (s, ctx) in cases {
            let expected = format!("Parser error: Error parsing timestamp from '{s}': {ctx}");
            let actual = string_to_datetime(&Utc, s).unwrap_err().to_string();
            assert_eq!(actual, expected)
        }
    }

    // Parse a timestamp to timestamp int with a useful human readable error message
    fn parse_timestamp(s: &str) -> Result<i64, ArrowError> {
        let result = string_to_timestamp_nanos(s);
        if let Err(e) = &result {
            eprintln!("Error parsing timestamp '{s}': {e:?}");
        }
        result
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
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08T13:42:29.190855").unwrap()
        );

        assert_eq!(
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08 13:42:29.190855").unwrap()
        );

        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 9, 8).unwrap(),
            NaiveTime::from_hms_nano_opt(13, 42, 29, 0).unwrap(),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
            parse_timestamp("2020-09-08T13:42:29").unwrap()
        );

        assert_eq!(
            naive_datetime.and_utc().timestamp_nanos_opt().unwrap(),
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
            NaiveDateTime::parse_from_str("2020-09-08T13:42:29Z", "%Y-%m-%dT%H:%M:%SZ").unwrap();
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
    fn parse_date32() {
        let cases = [
            "2020-09-08",
            "2020-9-8",
            "2020-09-8",
            "2020-9-08",
            "2020-12-1",
            "1690-2-5",
            "2020-09-08 01:02:03",
        ];
        for case in cases {
            let v = date32_to_datetime(Date32Type::parse(case).unwrap()).unwrap();
            let expected = NaiveDate::parse_from_str(case, "%Y-%m-%d")
                .or_else(|_| NaiveDate::parse_from_str(case, "%Y-%m-%d %H:%M:%S"))
                .unwrap();
            assert_eq!(v.date(), expected);
        }

        let err_cases = [
            "",
            "80-01-01",
            "342",
            "Foo",
            "2020-09-08-03",
            "2020--04-03",
            "2020--",
            "2020-09-08 01",
            "2020-09-08 01:02",
            "2020-09-08 01-02-03",
            "2020-9-8 01:02:03",
            "2020-09-08 1:2:3",
        ];
        for case in err_cases {
            assert_eq!(Date32Type::parse(case), None);
        }
    }

    #[test]
    fn parse_date32_extended_year() {
        // `Date32` covers any i32 days-from-epoch, verify we can parse it
        let cases: &[(&str, i32)] = &[
            ("+1970-01-01", 0),
            ("+2024-01-01", 19_723),
            ("-0001-01-01", -719_893),
            ("+29349-01-26", 10_000_000),
            ("+2739877-01-03", 1_000_000_000),
            // Extremes of the Date32 representable range.
            ("+5881580-07-11", i32::MAX),
            ("-5877641-06-23", i32::MIN),
        ];
        for (input, expected) in cases {
            assert_eq!(Date32Type::parse(input), Some(*expected), "input: {input}");
        }

        // One past Date32::MAX / MIN overflows i32 days-from-epoch.
        assert_eq!(Date32Type::parse("+5881580-07-12"), None);
        assert_eq!(Date32Type::parse("-5877641-06-22"), None);
        // Invalid calendar dates still rejected regardless of year magnitude.
        assert_eq!(Date32Type::parse("+2739877-02-30"), None);
        assert_eq!(Date32Type::parse("+2739877-13-01"), None);
        assert_eq!(Date32Type::parse("-2739877-02-30"), None);
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
            Time64NanosecondType::parse_formatted("02 - 10 - 01 - .1234567", "%H - %M - %S - %.f"),
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
            Time64MicrosecondType::parse_formatted("02 - 10 - 01 - .1234", "%H - %M - %S - %.f"),
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
            Time32MillisecondType::parse_formatted("02 - 10 - 01 - .1", "%H - %M - %S - %.f"),
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
    fn test_string_to_time_invalid() {
        let cases = [
            "25:00",
            "9:00:",
            "009:00",
            "09:0:00",
            "25:00:00",
            "13:00 AM",
            "13:00 PM",
            "12:00. AM",
            "09:0:00",
            "09:01:0",
            "09:01:1",
            "9:1:0",
            "09:01:0",
            "1:00.123",
            "1:00:00.123f",
            " 9:00:00",
            ":09:00",
            "T9:00:00",
            "AM",
        ];
        for case in cases {
            assert!(string_to_time(case).is_none(), "{case}");
        }
    }

    #[test]
    fn test_string_to_time_chrono() {
        let cases = [
            ("1:00", "%H:%M"),
            ("12:00", "%H:%M"),
            ("13:00", "%H:%M"),
            ("24:00", "%H:%M"),
            ("1:00:00", "%H:%M:%S"),
            ("12:00:30", "%H:%M:%S"),
            ("13:00:59", "%H:%M:%S"),
            ("24:00:60", "%H:%M:%S"),
            ("09:00:00", "%H:%M:%S%.f"),
            ("0:00:30.123456", "%H:%M:%S%.f"),
            ("0:00 AM", "%I:%M %P"),
            ("1:00 AM", "%I:%M %P"),
            ("12:00 AM", "%I:%M %P"),
            ("13:00 AM", "%I:%M %P"),
            ("0:00 PM", "%I:%M %P"),
            ("1:00 PM", "%I:%M %P"),
            ("12:00 PM", "%I:%M %P"),
            ("13:00 PM", "%I:%M %P"),
            ("1:00 pM", "%I:%M %P"),
            ("1:00 Pm", "%I:%M %P"),
            ("1:00 aM", "%I:%M %P"),
            ("1:00 Am", "%I:%M %P"),
            ("1:00:30.123456 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.123456789 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.123456789123 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.1234 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.123456 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.123456789123456789 PM", "%I:%M:%S%.f %P"),
            ("1:00:30.12F456 PM", "%I:%M:%S%.f %P"),
        ];
        for (s, format) in cases {
            let chrono = NaiveTime::parse_from_str(s, format).ok();
            let custom = string_to_time(s);
            assert_eq!(chrono, custom, "{s}");
        }
    }

    #[test]
    fn test_parse_interval() {
        let config = IntervalParseConfig::new(IntervalUnit::Month);

        assert_eq!(
            Interval::new(1i32, 0i32, 0i64),
            Interval::parse("1 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(2i32, 0i32, 0i64),
            Interval::parse("2 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(-1i32, -18i32, -(NANOS_PER_DAY / 5)),
            Interval::parse("-1.5 months -3.2 days", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 15i32, 0),
            Interval::parse("0.5 months", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 15i32, 0),
            Interval::parse(".5 months", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, -15i32, 0),
            Interval::parse("-0.5 months", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, -15i32, 0),
            Interval::parse("-.5 months", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(2i32, 10i32, 9 * NANOS_PER_HOUR),
            Interval::parse("2.1 months 7.25 days 3 hours", &config).unwrap(),
        );

        assert_eq!(
            Interval::parse("1 centurys 1 month", &config)
                .unwrap_err()
                .to_string(),
            r#"Parser error: Invalid input syntax for type interval: "1 centurys 1 month""#
        );

        assert_eq!(
            Interval::new(37i32, 0i32, 0i64),
            Interval::parse("3 year 1 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(35i32, 0i32, 0i64),
            Interval::parse("3 year -1 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(-37i32, 0i32, 0i64),
            Interval::parse("-3 year -1 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(-35i32, 0i32, 0i64),
            Interval::parse("-3 year 1 month", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 5i32, 0i64),
            Interval::parse("5 days", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 7i32, 3 * NANOS_PER_HOUR),
            Interval::parse("7 days 3 hours", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 7i32, 5 * NANOS_PER_MINUTE),
            Interval::parse("7 days 5 minutes", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, 7i32, -5 * NANOS_PER_MINUTE),
            Interval::parse("7 days -5 minutes", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(0i32, -7i32, 5 * NANOS_PER_HOUR),
            Interval::parse("-7 days 5 hours", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(
                0i32,
                -7i32,
                -5 * NANOS_PER_HOUR - 5 * NANOS_PER_MINUTE - 5 * NANOS_PER_SECOND
            ),
            Interval::parse("-7 days -5 hours -5 minutes -5 seconds", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(12i32, 0i32, 25 * NANOS_PER_MILLIS),
            Interval::parse("1 year 25 millisecond", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(
                12i32,
                1i32,
                (NANOS_PER_SECOND as f64 * 0.000000001_f64) as i64
            ),
            Interval::parse("1 year 1 day 0.000000001 seconds", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(12i32, 1i32, NANOS_PER_MILLIS / 10),
            Interval::parse("1 year 1 day 0.1 milliseconds", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(12i32, 1i32, 1000i64),
            Interval::parse("1 year 1 day 1 microsecond", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(12i32, 1i32, 1i64),
            Interval::parse("1 year 1 day 1 nanoseconds", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(1i32, 0i32, -NANOS_PER_SECOND),
            Interval::parse("1 month -1 second", &config).unwrap(),
        );

        assert_eq!(
            Interval::new(
                -13i32,
                -8i32,
                -NANOS_PER_HOUR
                    - NANOS_PER_MINUTE
                    - NANOS_PER_SECOND
                    - (1.11_f64 * NANOS_PER_MILLIS as f64) as i64
            ),
            Interval::parse(
                "-1 year -1 month -1 week -1 day -1 hour -1 minute -1 second -1.11 millisecond",
                &config
            )
            .unwrap(),
        );

        // no units
        assert_eq!(
            Interval::new(1, 0, 0),
            Interval::parse("1", &config).unwrap()
        );
        assert_eq!(
            Interval::new(42, 0, 0),
            Interval::parse("42", &config).unwrap()
        );
        assert_eq!(
            Interval::new(0, 0, 42_000_000_000),
            Interval::parse("42", &IntervalParseConfig::new(IntervalUnit::Second)).unwrap()
        );

        // shorter units
        assert_eq!(
            Interval::new(1, 0, 0),
            Interval::parse("1 mon", &config).unwrap()
        );
        assert_eq!(
            Interval::new(1, 0, 0),
            Interval::parse("1 mons", &config).unwrap()
        );
        assert_eq!(
            Interval::new(0, 0, 1_000_000),
            Interval::parse("1 ms", &config).unwrap()
        );
        assert_eq!(
            Interval::new(0, 0, 1_000),
            Interval::parse("1 us", &config).unwrap()
        );

        // no space
        assert_eq!(
            Interval::new(0, 0, 1_000),
            Interval::parse("1us", &config).unwrap()
        );
        assert_eq!(
            Interval::new(0, 0, NANOS_PER_SECOND),
            Interval::parse("1s", &config).unwrap()
        );
        assert_eq!(
            Interval::new(1, 2, 10_864_000_000_000),
            Interval::parse("1mon 2days 3hr 1min 4sec", &config).unwrap()
        );

        assert_eq!(
            Interval::new(
                -13i32,
                -8i32,
                -NANOS_PER_HOUR
                    - NANOS_PER_MINUTE
                    - NANOS_PER_SECOND
                    - (1.11_f64 * NANOS_PER_MILLIS as f64) as i64
            ),
            Interval::parse(
                "-1year -1month -1week -1day -1 hour -1 minute -1 second -1.11millisecond",
                &config
            )
            .unwrap(),
        );

        assert_eq!(
            Interval::parse("1h s", &config).unwrap_err().to_string(),
            r#"Parser error: Invalid input syntax for type interval: "1h s""#
        );

        assert_eq!(
            Interval::parse("1XX", &config).unwrap_err().to_string(),
            r#"Parser error: Invalid input syntax for type interval: "1XX""#
        );
    }

    #[test]
    fn test_duplicate_interval_type() {
        let config = IntervalParseConfig::new(IntervalUnit::Month);

        let err = Interval::parse("1 month 1 second 1 second", &config)
            .expect_err("parsing interval should have failed");
        assert_eq!(
            r#"ParseError("Invalid input syntax for type interval: \"1 month 1 second 1 second\". Repeated type 'second'")"#,
            format!("{err:?}")
        );

        // test with singular and plural forms
        let err = Interval::parse("1 century 2 centuries", &config)
            .expect_err("parsing interval should have failed");
        assert_eq!(
            r#"ParseError("Invalid input syntax for type interval: \"1 century 2 centuries\". Repeated type 'centuries'")"#,
            format!("{err:?}")
        );
    }

    #[test]
    fn test_interval_amount_parsing() {
        // integer
        let result = IntervalAmount::from_str("123").unwrap();
        let expected = IntervalAmount::new(123, 0);

        assert_eq!(result, expected);

        // positive w/ fractional
        let result = IntervalAmount::from_str("0.3").unwrap();
        let expected = IntervalAmount::new(0, 3 * 10_i64.pow(INTERVAL_PRECISION - 1));

        assert_eq!(result, expected);

        // negative w/ fractional
        let result = IntervalAmount::from_str("-3.5").unwrap();
        let expected = IntervalAmount::new(-3, -5 * 10_i64.pow(INTERVAL_PRECISION - 1));

        assert_eq!(result, expected);

        // invalid: missing fractional
        let result = IntervalAmount::from_str("3.");
        assert!(result.is_err());

        // invalid: sign in fractional
        let result = IntervalAmount::from_str("3.-5");
        assert!(result.is_err());
    }

    #[test]
    fn test_interval_precision() {
        let config = IntervalParseConfig::new(IntervalUnit::Month);

        let result = Interval::parse("100000.1 days", &config).unwrap();
        let expected = Interval::new(0_i32, 100_000_i32, NANOS_PER_DAY / 10);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_interval_addition() {
        // add 4.1 centuries
        let start = Interval::new(1, 2, 3);
        let expected = Interval::new(4921, 2, 3);

        let result = start
            .add(
                IntervalAmount::new(4, 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Century,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add 10.25 decades
        let start = Interval::new(1, 2, 3);
        let expected = Interval::new(1231, 2, 3);

        let result = start
            .add(
                IntervalAmount::new(10, 25 * 10_i64.pow(INTERVAL_PRECISION - 2)),
                IntervalUnit::Decade,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add 30.3 years (reminder: Postgres logic does not spill to days/nanos when interval is larger than a month)
        let start = Interval::new(1, 2, 3);
        let expected = Interval::new(364, 2, 3);

        let result = start
            .add(
                IntervalAmount::new(30, 3 * 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Year,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add 1.5 months
        let start = Interval::new(1, 2, 3);
        let expected = Interval::new(2, 17, 3);

        let result = start
            .add(
                IntervalAmount::new(1, 5 * 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Month,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add -2 weeks
        let start = Interval::new(1, 25, 3);
        let expected = Interval::new(1, 11, 3);

        let result = start
            .add(IntervalAmount::new(-2, 0), IntervalUnit::Week)
            .unwrap();

        assert_eq!(result, expected);

        // add 2.2 days
        let start = Interval::new(12, 15, 3);
        let expected = Interval::new(12, 17, 3 + 17_280 * NANOS_PER_SECOND);

        let result = start
            .add(
                IntervalAmount::new(2, 2 * 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Day,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add 12.5 hours
        let start = Interval::new(1, 2, 3);
        let expected = Interval::new(1, 2, 3 + 45_000 * NANOS_PER_SECOND);

        let result = start
            .add(
                IntervalAmount::new(12, 5 * 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Hour,
            )
            .unwrap();

        assert_eq!(result, expected);

        // add -1.5 minutes
        let start = Interval::new(0, 0, -3);
        let expected = Interval::new(0, 0, -90_000_000_000 - 3);

        let result = start
            .add(
                IntervalAmount::new(-1, -5 * 10_i64.pow(INTERVAL_PRECISION - 1)),
                IntervalUnit::Minute,
            )
            .unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn string_to_timestamp_old() {
        parse_timestamp("1677-06-14T07:29:01.256")
            .map_err(|e| assert!(e.to_string().ends_with(ERR_NANOSECONDS_NOT_SUPPORTED)))
            .unwrap_err();
    }

    #[test]
    fn test_parse_decimal_with_parameter() {
        let tests = [
            ("0", 0i128),
            ("123.123", 123123i128),
            ("123.1234", 123123i128),
            ("123.1", 123100i128),
            ("123", 123000i128),
            ("-123.123", -123123i128),
            ("-123.1234", -123123i128),
            ("-123.1", -123100i128),
            ("-123", -123000i128),
            ("0.0000123", 0i128),
            ("12.", 12000i128),
            ("-12.", -12000i128),
            ("00.1", 100i128),
            ("-00.1", -100i128),
            ("12345678912345678.1234", 12345678912345678123i128),
            ("-12345678912345678.1234", -12345678912345678123i128),
            ("99999999999999999.999", 99999999999999999999i128),
            ("-99999999999999999.999", -99999999999999999999i128),
            (".123", 123i128),
            ("-.123", -123i128),
            ("123.", 123000i128),
            ("-123.", -123000i128),
        ];
        for (s, i) in tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, 20, 3);
            assert_eq!(i, result_128.unwrap());
            let result_256 = parse_decimal::<Decimal256Type>(s, 20, 3);
            assert_eq!(i256::from_i128(i), result_256.unwrap());
        }

        let e_notation_tests = [
            ("1.23e3", "1230.0", 2),
            ("5.6714e+2", "567.14", 4),
            ("5.6714e-2", "0.056714", 4),
            ("5.6714e-2", "0.056714", 3),
            ("5.6741214125e2", "567.41214125", 4),
            ("8.91E4", "89100.0", 2),
            ("3.14E+5", "314000.0", 2),
            ("2.718e0", "2.718", 2),
            ("9.999999e-1", "0.9999999", 4),
            ("1.23e+3", "1230", 2),
            ("1.234559e+3", "1234.559", 2),
            ("1.00E-10", "0.0000000001", 11),
            ("1.23e-4", "0.000123", 2),
            ("9.876e7", "98760000.0", 2),
            ("5.432E+8", "543200000.0", 10),
            ("1.234567e9", "1234567000.0", 2),
            ("1.234567e2", "123.45670000", 2),
            ("4749.3e-5", "0.047493", 10),
            ("4749.3e+5", "474930000", 10),
            ("4749.3e-5", "0.047493", 1),
            ("4749.3e+5", "474930000", 1),
            ("0E-8", "0", 10),
            ("0E+6", "0", 10),
            ("0e0", "0", 10),
            ("-0e0", "0", 10),
            ("00e48", "0", 10),
            ("1E-8", "0.00000001", 10),
            ("12E+6", "12000000", 10),
            ("12E-6", "0.000012", 10),
            ("0.1e-6", "0.0000001", 10),
            ("0.1e+6", "100000", 10),
            ("0.12e-6", "0.00000012", 10),
            ("0.12e+6", "120000", 10),
            ("000000000001e0", "000000000001", 3),
            ("000001.1034567002e0", "000001.1034567002", 3),
            ("1.234e16", "12340000000000000", 0),
            ("123.4e16", "1234000000000000000", 0),
            ("15e-1", "1.5", 0),
            ("1.25e1", "12.5", 0),
            ("1.5e-1", "0.15", 1),
        ];
        for (e, d, scale) in e_notation_tests {
            let result_128_e = parse_decimal::<Decimal128Type>(e, 20, scale);
            let result_128_d = parse_decimal::<Decimal128Type>(d, 20, scale);
            assert_eq!(result_128_e.unwrap(), result_128_d.unwrap(), "{e} vs {d}");
            let result_256_e = parse_decimal::<Decimal256Type>(e, 20, scale);
            let result_256_d = parse_decimal::<Decimal256Type>(d, 20, scale);
            assert_eq!(result_256_e.unwrap(), result_256_d.unwrap(), "{e} vs {d}");
        }
        let can_not_parse_tests = [
            "123,123",
            ".",
            "123.123.123",
            "",
            "+",
            "-",
            "e",
            "e5",
            "-.",
            "+e-11",
            "-.E+3",
            ".e5",
            "1.3e+e3",
            "5.6714ee-2",
            "4.11ee-+4",
            "4.11e++4",
            "1.1e.12",
            "1.23e+3.",
            "1.23e+3.1",
            "1e",
            "1e+",
            "1e-",
            "1e5e5",
            "1 000",
            "1_000",
            "- 1",
            "1.5 x",
            "0x10",
            "NaN",
            "inf",
            "\u{661}\u{662}",
            "\u{ff11}",
        ];
        for s in can_not_parse_tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, 20, 3);
            assert_eq!(
                format!("Parser error: Invalid decimal format: {s:?}"),
                result_128.unwrap_err().to_string()
            );
            let result_256 = parse_decimal::<Decimal256Type>(s, 20, 3);
            assert_eq!(
                format!("Parser error: Invalid decimal format: {s:?}"),
                result_256.unwrap_err().to_string()
            );
        }
        let overflow_parse_tests = [
            ("12345678", 3),
            ("1.2345678e7", 3),
            ("12345678.9", 3),
            ("1.23456789e+7", 3),
            ("99999999.99", 3),
            ("9.999999999e7", 3),
            ("12345678908765.123456", 3),
            ("123456789087651234.56e-4", 3),
            ("1234560000000", 0),
            ("12345678900.0", 0),
            ("1.23456e12", 0),
            ("9999999.9995", 3),
            ("1e99999", 0),
            ("1e40", 0),
        ];
        for (s, scale) in overflow_parse_tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, 10, scale);
            let expected_128 =
                format!("Parser error: {s:?} does not fit in Decimal128(10, {scale})");
            assert_eq!(result_128.unwrap_err().to_string(), expected_128);

            let result_256 = parse_decimal::<Decimal256Type>(s, 10, scale);
            let expected_256 =
                format!("Parser error: {s:?} does not fit in Decimal256(10, {scale})");
            assert_eq!(result_256.unwrap_err().to_string(), expected_256);
        }

        let edge_tests_128 = [
            (
                "99999999999999999999999999999999999999",
                99999999999999999999999999999999999999i128,
                0,
            ),
            (
                "999999999999999999999999999999999999.99",
                99999999999999999999999999999999999999i128,
                2,
            ),
            (
                "9999999999999999999999999.9999999999999",
                99999999999999999999999999999999999999i128,
                13,
            ),
            (
                "9999999999999999999999999",
                99999999999999999999999990000000000000i128,
                13,
            ),
            (
                "0.99999999999999999999999999999999999999",
                99999999999999999999999999999999999999i128,
                38,
            ),
            (
                "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001016744",
                0i128,
                15,
            ),
            ("1.016744e-320", 0i128, 15),
            ("-1e3", -1000000000i128, 6),
            ("+1e3", 1000000000i128, 6),
            ("-1e31", -10000000000000000000000000000000000000i128, 6),
            // More digits than an i128 can hold, but a small value
            ("10000000000000000000000000000000000000000e-39", 10i128, 0),
            // Digits beyond the scale round; here the result still fits
            (
                "99999999999999999999999999999999999994e-1",
                9999999999999999999999999999999999999i128,
                0,
            ),
        ];
        for (s, i, scale) in edge_tests_128 {
            let result_128 = parse_decimal::<Decimal128Type>(s, 38, scale);
            assert_eq!(i, result_128.unwrap(), "{s}");
        }
        // Rounding carries into a 39th digit, which does not fit
        assert!(
            parse_decimal::<Decimal128Type>("999999999999999999999999999999999999999e-1", 38, 0)
                .is_err()
        );

        let edge_tests_256 = [
            (
                "9999999999999999999999999999999999999999999999999999999999999999999999999999",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999",
                )
                .unwrap(),
                0,
            ),
            (
                "999999999999999999999999999999999999999999999999999999999999999999999999.9999",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999",
                )
                .unwrap(),
                4,
            ),
            (
                "99999999999999999999999999999999999999999999999999.99999999999999999999999999",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999",
                )
                .unwrap(),
                26,
            ),
            (
                "9.999999999999999999999999999999999999999999999999999999999999999999999999999e49",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999",
                )
                .unwrap(),
                26,
            ),
            (
                "99999999999999999999999999999999999999999999999999",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999900000000000000000000000000",
                )
                .unwrap(),
                26,
            ),
            (
                "9.9999999999999999999999999999999999999999999999999e+49",
                i256::from_string(
                    "9999999999999999999999999999999999999999999999999900000000000000000000000000",
                )
                .unwrap(),
                26,
            ),
        ];
        for (s, i, scale) in edge_tests_256 {
            let result = parse_decimal::<Decimal256Type>(s, 76, scale);
            assert_eq!(i, result.unwrap());
        }

        let zero_scale_tests = [
            (".123", 0, 3),
            ("0.123", 0, 3),
            ("1.0", 1, 3),
            ("1.2", 1, 3),
            ("1.00", 1, 3),
            ("1.23", 1, 3),
            ("1.000", 1, 3),
            ("1.123", 1, 3),
            ("1.5", 2, 3),
            ("1.9", 2, 3),
            ("123.0", 123, 3),
            ("123.4", 123, 3),
            ("123.00", 123, 3),
            ("123.45", 123, 3),
            ("123.5", 124, 3),
            ("123.000000000000000000004", 123, 3),
            ("0.123e2", 12, 3),
            ("0.123e4", 1230, 10),
            ("1.23e4", 12300, 10),
            ("12.3e4", 123000, 10),
            ("123e4", 1230000, 10),
            (
                "20000000000000000000000000000000000002.0",
                20000000000000000000000000000000000002,
                38,
            ),
        ];
        for (s, i, precision) in zero_scale_tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, precision, 0).unwrap();
            assert_eq!(i, result_128, "{s}");
        }

        let can_not_parse_zero_scale = [".", "blag", "", "+", "-", "e"];
        for s in can_not_parse_zero_scale {
            let result_128 = parse_decimal::<Decimal128Type>(s, 5, 0);
            assert_eq!(
                format!("Parser error: Invalid decimal format: {s:?}"),
                result_128.unwrap_err().to_string(),
            );
        }
    }

    #[test]
    fn test_parse_decimal_rounds_half_away_from_zero() {
        let tests = [
            ("1.234", 2, 123),
            ("1.235", 2, 124),
            ("1.2350000", 2, 124),
            ("1.2349999", 2, 123),
            ("-1.234", 2, -123),
            ("-1.235", 2, -124),
            ("-0.004", 2, 0),
            ("-0.005", 2, -1),
            (".5", 0, 1),
            ("-.5", 0, -1),
            ("0.5", 0, 1),
            ("1.5", 0, 2),
            ("2.5", 0, 3),
            ("-2.5", 0, -3),
            ("1.99", 1, 20),
            ("0.995", 2, 100),
            ("9.99", 1, 100),
            ("123.4567891", 5, 12345679),
            ("123.45", 0, 123),
            ("0.0000123", 3, 0),
            ("12.", 2, 1200),
            (".12", 2, 12),
            ("+.12", 2, 12),
            ("-.12", 2, -12),
        ];
        for (s, scale, expected) in tests {
            assert_eq!(
                parse_decimal::<Decimal128Type>(s, 38, scale).unwrap(),
                expected,
                "{s} at scale {scale}"
            );
            assert_eq!(
                parse_decimal::<Decimal256Type>(s, 76, scale).unwrap(),
                i256::from_i128(expected),
                "{s} at scale {scale}"
            );
        }
    }

    #[test]
    fn test_parse_decimal_rounding_overflow() {
        // Rounding up can push the value past the precision ...
        assert!(parse_decimal::<Decimal128Type>("99999.5", 5, 0).is_err());
        assert!(parse_decimal::<Decimal128Type>("9.995", 3, 2).is_err());
        assert_eq!(parse_decimal::<Decimal128Type>("9.994", 3, 2).unwrap(), 999);
        assert_eq!(parse_decimal::<Decimal128Type>("0.995", 3, 2).unwrap(), 100);

        // ... or past the native type itself
        assert_eq!(
            parse_decimal_native::<Decimal32Type>("2147483647.5", 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal32Type>("-2147483648.5", 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal128Type>(&format!("{}.5", i128::MAX), 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal128Type>(&format!("{}.5", i128::MIN), 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal256Type>(&format!("{}.5", i256::MAX), 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal256Type>(&format!("{}.5", i256::MIN), 0),
            Err(DecimalParseError::Overflow)
        );
    }

    #[test]
    fn test_parse_decimal_native_full_range() {
        // The native range exceeds the largest precision; the precision check
        // is the caller's responsibility
        assert_eq!(
            parse_decimal_native::<Decimal32Type>("-2147483648", 0),
            Ok(i32::MIN)
        );
        assert_eq!(
            parse_decimal_native::<Decimal32Type>("2147483648", 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal64Type>("-9223372036854775808", 0),
            Ok(i64::MIN)
        );
        assert_eq!(
            parse_decimal_native::<Decimal64Type>("9223372036854775808", 0),
            Err(DecimalParseError::Overflow)
        );
        assert_eq!(
            parse_decimal_native::<Decimal128Type>(&i128::MAX.to_string(), 0),
            Ok(i128::MAX)
        );
        assert_eq!(
            parse_decimal_native::<Decimal128Type>(&i128::MIN.to_string(), 0),
            Ok(i128::MIN)
        );
        assert_eq!(
            parse_decimal_native::<Decimal256Type>(&i256::MAX.to_string(), 0),
            Ok(i256::MAX)
        );
        assert_eq!(
            parse_decimal_native::<Decimal256Type>(&i256::MIN.to_string(), 0),
            Ok(i256::MIN)
        );
        // The unscaled value (integer digits scaled by 10^21) far exceeds the
        // i256 range, so this must report overflow rather than wrapping to an
        // arbitrary (possibly in-range) value
        let input = format!("{}.12345678901234567890123", "7".repeat(71));
        assert_eq!(
            parse_decimal_native::<Decimal256Type>(&input, 21),
            Err(DecimalParseError::Overflow)
        );

        assert!(parse_decimal::<Decimal128Type>(&i128::MAX.to_string(), 38, 0).is_err());
        assert!(parse_decimal::<Decimal32Type>("-2147483648", 9, 0).is_err());
    }

    #[test]
    fn test_parse_decimal_integer_widths() {
        assert_eq!(
            parse_decimal::<Decimal32Type>("123.45", 9, 2).unwrap(),
            12_345_i32
        );
        assert_eq!(
            parse_decimal::<Decimal32Type>("-9999999.994", 9, 2).unwrap(),
            -999_999_999_i32
        );
        assert!(parse_decimal::<Decimal32Type>("9999999.995", 9, 2).is_err());
        assert!(parse_decimal::<Decimal32Type>("-9999999.995", 9, 2).is_err());
        assert_eq!(
            parse_decimal::<Decimal64Type>("123.45", 18, 2).unwrap(),
            12_345_i64
        );
        assert_eq!(
            parse_decimal::<Decimal64Type>("9999999999999999.99", 18, 2).unwrap(),
            999_999_999_999_999_999_i64
        );
        assert!(parse_decimal::<Decimal64Type>("10000000000000000.00", 18, 2).is_err());
        // Fractional parts longer than any native integer type parse fine;
        // digits beyond the scale only matter for rounding
        assert_eq!(
            parse_decimal::<Decimal64Type>(&format!(".{}", "5".repeat(100)), 18, 4).unwrap(),
            5_556_i64
        );
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!(".{}", "1".repeat(100)), 38, 4).unwrap(),
            1_111_i128
        );
    }

    #[test]
    fn test_parse_decimal_exponent() {
        let tests = [
            ("1e2", 0, 100),
            ("1E2", 0, 100),
            ("1e+2", 0, 100),
            ("1e+02", 0, 100),
            ("1.5e2", 0, 150),
            ("1.5e2", 2, 15000),
            ("1.5e-1", 1, 2),
            ("15e-1", 0, 2),
            ("1e-2", 1, 0),
            ("1e-3", 2, 0),
            ("0e0", 2, 0),
            ("-0e0", 2, 0),
            ("0E5", 2, 0),
            ("0e99999", 2, 0),
            ("00e48", 8, 0),
            ("+00.0E+41", 12, 0),
            ("1.25e1", 0, 13),
            ("1e-99999", 2, 0),
            ("1.5e-400", 2, 0),
            ("123456789e-9", 9, 123456789),
            ("0.000000001e9", 0, 1),
            ("5e-1", 0, 1),
            ("4e-1", 0, 0),
            ("-5e-1", 0, -1),
        ];
        for (s, scale, expected) in tests {
            assert_eq!(
                parse_decimal::<Decimal128Type>(s, 38, scale).unwrap(),
                expected,
                "{s} at scale {scale}"
            );
            assert_eq!(
                parse_decimal::<Decimal32Type>(s, 9, scale).unwrap(),
                expected as i32,
                "{s} at scale {scale}"
            );
        }

        // Exponents shift digits across the decimal point without losing any
        assert_eq!(
            parse_decimal::<Decimal32Type>("4825037936439135476.2609835314269495255615E-14", 9, 4)
                .unwrap(),
            482503794
        );
        assert_eq!(
            parse_decimal::<Decimal32Type>(
                "+18232335063972188138031550982650807591758238.0724251287782783777442440E-58",
                1,
                0
            )
            .unwrap(),
            0
        );
        assert_eq!(
            parse_decimal::<Decimal128Type>("4825037936439135476.2609835314269495255615E-14", 9, 1)
                .unwrap(),
            482504
        );
        assert!(
            parse_decimal::<Decimal128Type>("4825037936439135476.2609835314269495255615E-14", 5, 1)
                .is_err()
        );
        // Absurdly long exponents saturate rather than wrap
        assert!(parse_decimal::<Decimal128Type>(&format!("1e{}", "9".repeat(30)), 38, 0).is_err());
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!("1e-{}", "9".repeat(30)), 38, 0).unwrap(),
            0
        );
    }

    #[test]
    fn test_parse_decimal_negative_scale() {
        let tests = [
            ("1234.5", -2, 12),
            ("150", -2, 2),
            ("149", -2, 1),
            ("-150", -2, -2),
            ("-149", -2, -1),
            ("50", -2, 1),
            ("49", -2, 0),
            ("5", -1, 1),
            ("4", -1, 0),
            ("0.5", -1, 0),
            ("5.9", -1, 1),
            ("1e5", -2, 1000),
            ("1.5e5", -2, 1500),
            ("0.9e2", -1, 9),
            (".5e3", -2, 5),
            ("12345", -5, 0),
            ("12345", -4, 1),
            ("000123456", -3, 123),
            ("0", -5, 0),
            ("-0.0", -5, 0),
        ];
        for (s, scale, expected) in tests {
            assert_eq!(
                parse_decimal::<Decimal128Type>(s, 38, scale).unwrap(),
                expected,
                "{s} at scale {scale}"
            );
            assert_eq!(
                parse_decimal::<Decimal32Type>(s, 9, scale).unwrap(),
                expected as i32,
                "{s} at scale {scale}"
            );
            assert_eq!(
                parse_decimal::<Decimal256Type>(s, 76, scale).unwrap(),
                i256::from_i128(expected),
                "{s} at scale {scale}"
            );
        }
        // The integer part can be wider than the native type as long as the
        // scaled value fits
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!("1{}", "0".repeat(50)), 38, -40).unwrap(),
            10_000_000_000
        );
        assert_eq!(
            parse_decimal::<Decimal32Type>("123456789012", 9, -5).unwrap(),
            1234568
        );
        assert!(parse_decimal::<Decimal32Type>("123456789012", 9, -2).is_err());
    }

    #[test]
    fn test_parse_decimal_whitespace_and_long_input() {
        for s in [" 1.5", "1.5 ", " 1.5 ", "\t1.5\n", "\r\n1.5\x0c"] {
            assert_eq!(
                parse_decimal::<Decimal128Type>(s, 38, 1).unwrap(),
                15,
                "{s:?}"
            );
        }
        // Only ASCII whitespace is trimmed, as for the other CSV parsers
        assert!(parse_decimal::<Decimal128Type>("\u{a0}1.5", 38, 1).is_err());
        assert!(parse_decimal::<Decimal128Type>("1.5\u{2003}", 38, 1).is_err());
        assert!(parse_decimal::<Decimal128Type>(" ", 38, 1).is_err());

        // Long inputs report overflow rather than wrapping or panicking
        for s in [
            "1".repeat(255),
            "1".repeat(256),
            "1".repeat(300),
            format!("{}.5", "1".repeat(300)),
            format!("1e{}", "9".repeat(300)),
        ] {
            let err = parse_decimal::<Decimal128Type>(&s, 38, 0).unwrap_err();
            assert!(err.to_string().contains("does not fit"), "{err}");
        }
        // Long fractions only matter for rounding
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!("0.{}", "0".repeat(200)), 38, 10).unwrap(),
            0
        );
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!("0.{}1", "0".repeat(200)), 38, 10).unwrap(),
            0
        );
        assert_eq!(
            parse_decimal::<Decimal128Type>(&format!("1.{}", "9".repeat(300)), 38, 2).unwrap(),
            200
        );
        // 10^scale overflows the native type, but zero is still representable
        assert_eq!(parse_decimal::<Decimal32Type>("0", 9, 10).unwrap(), 0);
        assert_eq!(parse_decimal::<Decimal32Type>("-0.0", 9, 10).unwrap(), 0);
        assert_eq!(parse_decimal::<Decimal64Type>("0", 18, 20).unwrap(), 0);
        assert_eq!(parse_decimal::<Decimal128Type>("0", 38, 40).unwrap(), 0);
        assert!(parse_decimal::<Decimal32Type>("1", 9, 10).is_err());
    }

    #[test]
    fn test_parse_decimal_matches_bigint_reference() {
        use num_bigint::BigInt;
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        /// Generates random decimal strings with a known exact value and checks
        /// that `parse_decimal` rounds them correctly or reports overflow
        fn check<T: DecimalType>(rng: &mut StdRng, iterations: usize)
        where
            T::Native: std::fmt::Display,
        {
            let random_digits = |rng: &mut StdRng, len: usize| -> String {
                (0..len)
                    .map(|_| char::from(b'0' + rng.random_range(0..10u8)))
                    .collect()
            };
            for _ in 0..iterations {
                let sign = ["", "+", "-"][rng.random_range(0..3)];
                let int_len = rng.random_range(0..=40);
                let frac_len = if rng.random_bool(0.3) {
                    0
                } else {
                    rng.random_range(0..=40)
                };
                if int_len == 0 && frac_len == 0 {
                    continue;
                }
                let int = random_digits(rng, int_len);
                let frac = random_digits(rng, frac_len);
                let mut s = format!("{sign}{int}");
                if frac_len > 0 || rng.random_bool(0.2) {
                    s.push('.');
                    s.push_str(&frac);
                }
                let exponent: i64 = if rng.random_bool(0.3) {
                    rng.random_range(-60..=60)
                } else {
                    0
                };
                if exponent != 0 || rng.random_bool(0.1) {
                    s.push(if rng.random_bool(0.5) { 'e' } else { 'E' });
                    if exponent >= 0 && rng.random_bool(0.5) {
                        s.push('+');
                    }
                    s.push_str(&exponent.to_string());
                }
                let precision = rng.random_range(1..=T::MAX_PRECISION);
                let scale = rng.random_range(-10..=T::MAX_SCALE.min(precision as i8));

                // value = mantissa * 10^(exponent - frac_len), scaled by 10^scale
                // and rounded half away from zero
                let mantissa: BigInt = format!("{int}{frac}").parse().unwrap();
                let shift = exponent - frac_len as i64 + scale as i64;
                let mut expected = if shift >= 0 {
                    mantissa * BigInt::from(10).pow(shift as u32)
                } else {
                    let divisor = BigInt::from(10).pow((-shift) as u32);
                    let quotient = &mantissa / &divisor;
                    if (&mantissa % &divisor) * 2 >= divisor {
                        quotient + 1
                    } else {
                        quotient
                    }
                };
                if sign == "-" {
                    expected = -expected;
                }
                let limit = BigInt::from(10).pow(precision as u32);
                let fits = expected < limit && expected > -limit;

                match (fits, parse_decimal::<T>(&s, precision, scale)) {
                    (true, Ok(actual)) => {
                        let actual: BigInt = actual.to_string().parse().unwrap();
                        assert_eq!(
                            actual,
                            expected,
                            "{s:?} as {}({precision}, {scale})",
                            T::PREFIX
                        );
                    }
                    (false, Err(_)) => {}
                    (true, Err(e)) => {
                        panic!(
                            "{s:?} as {}({precision}, {scale}): expected {expected}, got {e}",
                            T::PREFIX
                        )
                    }
                    (false, Ok(actual)) => panic!(
                        "{s:?} as {}({precision}, {scale}): expected overflow, got {actual}",
                        T::PREFIX
                    ),
                }
            }
        }

        let mut rng = StdRng::seed_from_u64(0xDEC1_3A15);
        check::<Decimal32Type>(&mut rng, 5_000);
        check::<Decimal64Type>(&mut rng, 5_000);
        check::<Decimal128Type>(&mut rng, 5_000);
        check::<Decimal256Type>(&mut rng, 5_000);
    }

    #[test]
    fn test_parse_empty() {
        assert_eq!(Int32Type::parse(""), None);
        assert_eq!(Int64Type::parse(""), None);
        assert_eq!(UInt32Type::parse(""), None);
        assert_eq!(UInt64Type::parse(""), None);
        assert_eq!(Float32Type::parse(""), None);
        assert_eq!(Float64Type::parse(""), None);
        assert_eq!(Int32Type::parse("+"), None);
        assert_eq!(Int64Type::parse("+"), None);
        assert_eq!(UInt32Type::parse("+"), None);
        assert_eq!(UInt64Type::parse("+"), None);
        assert_eq!(Float32Type::parse("+"), None);
        assert_eq!(Float64Type::parse("+"), None);
        assert_eq!(TimestampNanosecondType::parse(""), None);
        assert_eq!(Date32Type::parse(""), None);
    }

    #[test]
    fn test_parse_interval_month_day_nano_config() {
        let interval = parse_interval_month_day_nano_config(
            "1",
            IntervalParseConfig::new(IntervalUnit::Second),
        )
        .unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.nanoseconds, NANOS_PER_SECOND);
    }
    #[test]
    fn test_parse_prefix_white_space() {
        assert_eq!(Float64Type::parse(" 1.5"), Some(1.5));
        assert_eq!(Float64Type::parse("\t\n 20.54"), Some(20.54));
        assert_eq!(Float64Type::parse("\n2.5"), Some(2.5));
        assert_eq!(Float64Type::parse("\n-942.5423"), Some(-942.5423));
        assert_eq!(Float64Type::parse("\n\t\n\t\n40.5123"), Some(40.5123));
        assert_eq!(Float64Type::parse(" 1.5"), Some(1.5));
        assert_eq!(Float64Type::parse("\n\t\n\t\n-40.5123"), Some(-40.5123));
        assert_eq!(Float64Type::parse(" -1.5"), Some(-1.5));
        assert_eq!(Int32Type::parse(" 3"), Some(3));
        assert_eq!(Int32Type::parse("          30"), Some(30));
        assert_eq!(Int32Type::parse("\n \n 100"), Some(100));
        assert_eq!(Int32Type::parse(" \n25"), Some(25));
        assert_eq!(Int32Type::parse("\t800"), Some(800));
        assert_eq!(Int32Type::parse("\t  \n \t 851"), Some(851));
        assert_eq!(Int32Type::parse("\t\n\t\n\n\n\t1"), Some(1));
        assert_eq!(Int32Type::parse(" \n-25"), Some(-25));
        assert_eq!(Int32Type::parse("\t-800"), Some(-800));

        // suffix whitespace
        assert_eq!(Float64Type::parse("1.5 "), Some(1.5));
        assert_eq!(Float64Type::parse("40.5123\n"), Some(40.5123));
        assert_eq!(Float64Type::parse("40.5123\n\t\n\t\n"), Some(40.5123));
        assert_eq!(Float64Type::parse("-942.5423\t"), Some(-942.5423));
        assert_eq!(Int32Type::parse("3 "), Some(3));
        assert_eq!(Int32Type::parse("30          "), Some(30));
        assert_eq!(Int32Type::parse("-25 \n"), Some(-25));
        assert_eq!(Int32Type::parse("800\t"), Some(800));
        // whitespace on both sides
        assert_eq!(Float64Type::parse(" 1.5 "), Some(1.5));
        assert_eq!(Float64Type::parse("\t\n 20.54 \t"), Some(20.54));
        assert_eq!(Float64Type::parse("\n-942.5423\n"), Some(-942.5423));
        assert_eq!(Int32Type::parse(" 3 "), Some(3));
        assert_eq!(Int32Type::parse("\n \n 100 \n"), Some(100));
        assert_eq!(Int32Type::parse("\t-800\t\n"), Some(-800));

        // trailing non-whitespace chars should not parse
        assert_eq!(Float64Type::parse("1.5abc"), None);
        assert_eq!(Float64Type::parse("40.5123x"), None);
        assert_eq!(Int32Type::parse("30x"), None);
        assert_eq!(Int32Type::parse("100px"), None);
        assert_eq!(Int32Type::parse("-25!"), None);
        assert_eq!(Int32Type::parse("3j"), None);
        assert_eq!(Int32Type::parse("3"), Some(3));
    }
}
