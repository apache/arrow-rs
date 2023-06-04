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
use arrow_array::{ArrowNativeTypeOp, ArrowPrimitiveType};
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

/// Helper for parsing timestamps
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
            .ok_or_else(|| err("error computing timezone offset"))?;

        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        return Ok(DateTime::from_local(date.and_time(time), offset));
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
        let offset = timezone.offset_from_local_datetime(&datetime);
        let offset = offset
            .single()
            .ok_or_else(|| err("error computing timezone offset"))?;
        return Ok(DateTime::from_local(datetime, offset));
    }

    if bytes[tz_offset] == b'z' || bytes[tz_offset] == b'Z' {
        let offset = timezone.offset_from_local_datetime(&datetime);
        let offset = offset
            .single()
            .ok_or_else(|| err("error computing timezone offset"))?;
        return Ok(DateTime::from_utc(datetime, offset));
    }

    // Parse remainder of string as timezone
    let parsed_tz: Tz = s[tz_offset..].trim_start().parse()?;
    let offset = parsed_tz.offset_from_local_datetime(&datetime);
    let offset = offset
        .single()
        .ok_or_else(|| err("error computing timezone offset"))?;
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
    let nt = string_to_time(s).ok_or_else(|| {
        ArrowError::ParseError(format!("Failed to parse \'{s}\' as time"))
    })?;
    Ok(nt.num_seconds_from_midnight() as i64 * 1_000_000_000 + nt.nanosecond() as i64)
}

fn string_to_time(s: &str) -> Option<NaiveTime> {
    let bytes = s.as_bytes();
    if bytes.len() < 4 {
        return None;
    }

    let (am, bytes) = match bytes.get(bytes.len() - 3..) {
        Some(b" AM" | b" am" | b" Am" | b" aM") => {
            (Some(true), &bytes[..bytes.len() - 3])
        }
        Some(b" PM" | b" pm" | b" pM" | b" Pm") => {
            (Some(false), &bytes[..bytes.len() - 3])
        }
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

/// Specialized parsing implementations
/// used by csv and json reader
pub trait Parser: ArrowPrimitiveType {
    fn parse(string: &str) -> Option<Self::Native>;

    fn parse_formatted(string: &str, _format: &str) -> Option<Self::Native> {
        Self::parse(string)
    }
}

impl Parser for Float16Type {
    fn parse(string: &str) -> Option<f16> {
        lexical_core::parse(string.as_bytes())
            .ok()
            .map(f16::from_f32)
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
                lexical_core::parse::<Self::Native>(string.as_bytes()).ok()
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

/// Parse the string format decimal value to i128/i256 format and checking the precision and scale.
/// The result value can't be out of bounds.
pub fn parse_decimal<T: DecimalType>(
    s: &str,
    precision: u8,
    scale: i8,
) -> Result<T::Native, ArrowError> {
    let mut result = T::Native::usize_as(0);
    let mut fractionals = 0;
    let mut digits = 0;
    let base = T::Native::usize_as(10);

    let bs = s.as_bytes();
    let (bs, negative) = match bs.first() {
        Some(b'-') => (&bs[1..], true),
        Some(b'+') => (&bs[1..], false),
        _ => (bs, false),
    };

    if bs.is_empty() {
        return Err(ArrowError::ParseError(format!(
            "can't parse the string value {s} to decimal"
        )));
    }

    let mut bs = bs.iter();
    // Overflow checks are not required if 10^(precision - 1) <= T::MAX holds.
    // Thus, if we validate the precision correctly, we can skip overflow checks.
    while let Some(b) = bs.next() {
        match b {
            b'0'..=b'9' => {
                if digits == 0 && *b == b'0' {
                    // Ignore leading zeros.
                    continue;
                }
                digits += 1;
                result = result.mul_wrapping(base);
                result = result.add_wrapping(T::Native::usize_as((b - b'0') as usize));
            }
            b'.' => {
                for b in bs.by_ref() {
                    if !b.is_ascii_digit() {
                        return Err(ArrowError::ParseError(format!(
                            "can't parse the string value {s} to decimal"
                        )));
                    }
                    if fractionals == scale {
                        // We have processed all the digits that we need. All that
                        // is left is to validate that the rest of the string contains
                        // valid digits.
                        continue;
                    }
                    fractionals += 1;
                    digits += 1;
                    result = result.mul_wrapping(base);
                    result =
                        result.add_wrapping(T::Native::usize_as((b - b'0') as usize));
                }

                // Fail on "."
                if digits == 0 {
                    return Err(ArrowError::ParseError(format!(
                        "can't parse the string value {s} to decimal"
                    )));
                }
            }
            _ => {
                return Err(ArrowError::ParseError(format!(
                    "can't parse the string value {s} to decimal"
                )));
            }
        }
    }

    if fractionals < scale {
        let exp = scale - fractionals;
        if exp as u8 + digits > precision {
            return Err(ArrowError::ParseError("parse decimal overflow".to_string()));
        }
        let mul = base.pow_wrapping(exp as _);
        result = result.mul_wrapping(mul);
    } else if digits > precision {
        return Err(ArrowError::ParseError("parse decimal overflow".to_string()));
    }

    Ok(if negative {
        result.neg_wrapping()
    } else {
        result
    })
}

pub fn parse_interval_year_month(
    value: &str,
) -> Result<<IntervalYearMonthType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, result_days, result_nanos) = parse_interval("years", value)?;
    if result_days != 0 || result_nanos != 0 {
        return Err(ArrowError::CastError(format!(
            "Cannot cast {value} to IntervalYearMonth. Only year and month fields are allowed."
        )));
    }
    Ok(IntervalYearMonthType::make_value(0, result_months))
}

pub fn parse_interval_day_time(
    value: &str,
) -> Result<<IntervalDayTimeType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, mut result_days, result_nanos) = parse_interval("days", value)?;
    if result_nanos % 1_000_000 != 0 {
        return Err(ArrowError::CastError(format!(
            "Cannot cast {value} to IntervalDayTime because the nanos part isn't multiple of milliseconds"
        )));
    }
    result_days += result_months * 30;
    Ok(IntervalDayTimeType::make_value(
        result_days,
        (result_nanos / 1_000_000) as i32,
    ))
}

pub fn parse_interval_month_day_nano(
    value: &str,
) -> Result<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native, ArrowError> {
    let (result_months, result_days, result_nanos) = parse_interval("months", value)?;
    Ok(IntervalMonthDayNanoType::make_value(
        result_months,
        result_days,
        result_nanos,
    ))
}

const SECONDS_PER_HOUR: f64 = 3_600_f64;
const NANOS_PER_MILLIS: f64 = 1_000_000_f64;
const NANOS_PER_SECOND: f64 = 1_000_f64 * NANOS_PER_MILLIS;
#[cfg(test)]
const NANOS_PER_MINUTE: f64 = 60_f64 * NANOS_PER_SECOND;
#[cfg(test)]
const NANOS_PER_HOUR: f64 = 60_f64 * NANOS_PER_MINUTE;
#[cfg(test)]
const NANOS_PER_DAY: f64 = 24_f64 * NANOS_PER_HOUR;

#[rustfmt::skip]
#[derive(Clone, Copy)]
#[repr(u16)]
enum IntervalType {
    Century     = 0b_0000_0000_0001,
    Decade      = 0b_0000_0000_0010,
    Year        = 0b_0000_0000_0100,
    Month       = 0b_0000_0000_1000,
    Week        = 0b_0000_0001_0000,
    Day         = 0b_0000_0010_0000,
    Hour        = 0b_0000_0100_0000,
    Minute      = 0b_0000_1000_0000,
    Second      = 0b_0001_0000_0000,
    Millisecond = 0b_0010_0000_0000,
    Microsecond = 0b_0100_0000_0000,
    Nanosecond  = 0b_1000_0000_0000,
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
            "microsecond" | "microseconds" => Ok(Self::Microsecond),
            "nanosecond" | "nanoseconds" => Ok(Self::Nanosecond),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Unknown interval type: {s}"
            ))),
        }
    }
}

pub type MonthDayNano = (i32, i32, i64);

/// parse string value to a triple of aligned months, days, nanos.
/// leading field is the default unit. e.g. `INTERVAL 1` represents `INTERVAL 1 SECOND` when leading_filed = 'second'
fn parse_interval(leading_field: &str, value: &str) -> Result<MonthDayNano, ArrowError> {
    let mut used_interval_types = 0;

    let mut calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
     -> Result<(i32, i32, i64), ArrowError> {
        // TODO: Use fixed-point arithmetic to avoid truncation and rounding errors (#3809)
        let interval_period = match f64::from_str(interval_period_str) {
            Ok(n) => n,
            Err(_) => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported Interval Expression with value {value:?}"
                )));
            }
        };

        if interval_period > (i64::MAX as f64) {
            return Err(ArrowError::ParseError(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        let it = IntervalType::from_str(interval_type).map_err(|_| {
            ArrowError::ParseError(format!(
                "Invalid input syntax for type interval: {value:?}"
            ))
        })?;

        // Disallow duplicate interval types
        if used_interval_types & (it as u16) != 0 {
            return Err(ArrowError::ParseError(format!(
                "Invalid input syntax for type interval: {value:?}. Repeated type '{interval_type}'"
            )));
        } else {
            used_interval_types |= it as u16;
        }

        match it {
            IntervalType::Century => {
                align_interval_parts(interval_period.mul_checked(1200_f64)?, 0.0, 0.0)
            }
            IntervalType::Decade => {
                align_interval_parts(interval_period.mul_checked(120_f64)?, 0.0, 0.0)
            }
            IntervalType::Year => {
                align_interval_parts(interval_period.mul_checked(12_f64)?, 0.0, 0.0)
            }
            IntervalType::Month => align_interval_parts(interval_period, 0.0, 0.0),
            IntervalType::Week => align_interval_parts(0.0, interval_period * 7_f64, 0.0),
            IntervalType::Day => align_interval_parts(0.0, interval_period, 0.0),
            IntervalType::Hour => Ok((
                0,
                0,
                (interval_period.mul_checked(SECONDS_PER_HOUR * NANOS_PER_SECOND))?
                    as i64,
            )),
            IntervalType::Minute => Ok((
                0,
                0,
                (interval_period.mul_checked(60_f64 * NANOS_PER_SECOND))? as i64,
            )),
            IntervalType::Second => Ok((
                0,
                0,
                (interval_period.mul_checked(NANOS_PER_SECOND))? as i64,
            )),
            IntervalType::Millisecond => {
                Ok((0, 0, (interval_period.mul_checked(1_000_000f64))? as i64))
            }
            IntervalType::Microsecond => {
                Ok((0, 0, (interval_period.mul_checked(1_000f64)?) as i64))
            }
            IntervalType::Nanosecond => Ok((0, 0, interval_period as i64)),
        }
    };

    let mut result_month: i32 = 0;
    let mut result_days: i32 = 0;
    let mut result_nanos: i64 = 0;

    let mut parts = value.split_whitespace();

    while let Some(interval_period_str) = parts.next() {
        let unit = parts.next().unwrap_or(leading_field);

        let (diff_month, diff_days, diff_nanos) =
            calculate_from_part(interval_period_str, unit)?;

        result_month =
            result_month
                .checked_add(diff_month)
                .ok_or(ArrowError::ParseError(format!(
                    "Interval field value out of range: {value:?}"
                )))?;

        result_days =
            result_days
                .checked_add(diff_days)
                .ok_or(ArrowError::ParseError(format!(
                    "Interval field value out of range: {value:?}"
                )))?;

        result_nanos =
            result_nanos
                .checked_add(diff_nanos)
                .ok_or(ArrowError::ParseError(format!(
                    "Interval field value out of range: {value:?}"
                )))?;
    }

    Ok((result_month, result_days, result_nanos))
}

/// The fractional units must be spilled to smaller units.
/// [reference Postgresql doc](https://www.postgresql.org/docs/15/datatype-datetime.html#DATATYPE-INTERVAL-INPUT:~:text=Field%20values%20can,fractional%20on%20output.)
/// INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
/// INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
fn align_interval_parts(
    month_part: f64,
    mut day_part: f64,
    mut nanos_part: f64,
) -> Result<(i32, i32, i64), ArrowError> {
    // Convert fractional month to days, It's not supported by Arrow types, but anyway
    day_part += (month_part - (month_part as i64) as f64) * 30_f64;

    // Convert fractional days to hours
    nanos_part += (day_part - ((day_part as i64) as f64))
        * 24_f64
        * SECONDS_PER_HOUR
        * NANOS_PER_SECOND;

    if month_part > i32::MAX as f64
        || month_part < i32::MIN as f64
        || day_part > i32::MAX as f64
        || day_part < i32::MIN as f64
        || nanos_part > i64::MAX as f64
        || nanos_part < i64::MIN as f64
    {
        return Err(ArrowError::ParseError(format!(
            "Parsed interval field value out of range: {month_part} months {day_part} days {nanos_part} nanos"
        )));
    }

    Ok((month_part as i32, day_part as i32, nanos_part as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::timezone::Tz;
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
            let chrono =
                NaiveDateTime::parse_from_str(case, "%Y-%m-%dT%H:%M:%S%.f").unwrap();
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
            let expected =
                format!("Parser error: Error parsing timestamp from '{s}': {ctx}");
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
        assert_eq!(
            (1i32, 0i32, 0i64),
            parse_interval("months", "1 month").unwrap(),
        );

        assert_eq!(
            (2i32, 0i32, 0i64),
            parse_interval("months", "2 month").unwrap(),
        );

        assert_eq!(
            (-1i32, -18i32, (-0.2 * NANOS_PER_DAY) as i64),
            parse_interval("months", "-1.5 months -3.2 days").unwrap(),
        );

        assert_eq!(
            (2i32, 10i32, (9.0 * NANOS_PER_HOUR) as i64),
            parse_interval("months", "2.1 months 7.25 days 3 hours").unwrap(),
        );

        assert_eq!(
            parse_interval("months", "1 centurys 1 month")
                .unwrap_err()
                .to_string(),
            r#"Parser error: Invalid input syntax for type interval: "1 centurys 1 month""#
        );

        assert_eq!(
            (37i32, 0i32, 0i64),
            parse_interval("months", "3 year 1 month").unwrap(),
        );

        assert_eq!(
            (35i32, 0i32, 0i64),
            parse_interval("months", "3 year -1 month").unwrap(),
        );

        assert_eq!(
            (-37i32, 0i32, 0i64),
            parse_interval("months", "-3 year -1 month").unwrap(),
        );

        assert_eq!(
            (-35i32, 0i32, 0i64),
            parse_interval("months", "-3 year 1 month").unwrap(),
        );

        assert_eq!(
            (0i32, 5i32, 0i64),
            parse_interval("months", "5 days").unwrap(),
        );

        assert_eq!(
            (0i32, 7i32, (3f64 * NANOS_PER_HOUR) as i64),
            parse_interval("months", "7 days 3 hours").unwrap(),
        );

        assert_eq!(
            (0i32, 7i32, (5f64 * NANOS_PER_MINUTE) as i64),
            parse_interval("months", "7 days 5 minutes").unwrap(),
        );

        assert_eq!(
            (0i32, 7i32, (-5f64 * NANOS_PER_MINUTE) as i64),
            parse_interval("months", "7 days -5 minutes").unwrap(),
        );

        assert_eq!(
            (0i32, -7i32, (5f64 * NANOS_PER_HOUR) as i64),
            parse_interval("months", "-7 days 5 hours").unwrap(),
        );

        assert_eq!(
            (
                0i32,
                -7i32,
                (-5f64 * NANOS_PER_HOUR
                    - 5f64 * NANOS_PER_MINUTE
                    - 5f64 * NANOS_PER_SECOND) as i64
            ),
            parse_interval("months", "-7 days -5 hours -5 minutes -5 seconds").unwrap(),
        );

        assert_eq!(
            (12i32, 0i32, (25f64 * NANOS_PER_MILLIS) as i64),
            parse_interval("months", "1 year 25 millisecond").unwrap(),
        );

        assert_eq!(
            (12i32, 1i32, (0.000000001 * NANOS_PER_SECOND) as i64),
            parse_interval("months", "1 year 1 day 0.000000001 seconds").unwrap(),
        );

        assert_eq!(
            (12i32, 1i32, (0.1 * NANOS_PER_MILLIS) as i64),
            parse_interval("months", "1 year 1 day 0.1 milliseconds").unwrap(),
        );

        assert_eq!(
            (12i32, 1i32, 1000i64),
            parse_interval("months", "1 year 1 day 1 microsecond").unwrap(),
        );

        assert_eq!(
            (12i32, 1i32, 1i64),
            parse_interval("months", "1 year 1 day 1 nanoseconds").unwrap(),
        );

        assert_eq!(
            (1i32, 0i32, (-NANOS_PER_SECOND) as i64),
            parse_interval("months", "1 month -1 second").unwrap(),
        );

        assert_eq!(
            (-13i32, -8i32, (- NANOS_PER_HOUR - NANOS_PER_MINUTE - NANOS_PER_SECOND - 1.11 * NANOS_PER_MILLIS) as i64),
            parse_interval("months", "-1 year -1 month -1 week -1 day -1 hour -1 minute -1 second -1.11 millisecond").unwrap(),
        );
    }

    #[test]
    fn test_duplicate_interval_type() {
        let err = parse_interval("months", "1 month 1 second 1 second")
            .expect_err("parsing interval should have failed");
        assert_eq!(
            r#"ParseError("Invalid input syntax for type interval: \"1 month 1 second 1 second\". Repeated type 'second'")"#,
            format!("{err:?}")
        );
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
        let can_not_parse_tests = ["123,123", ".", "123.123.123", "", "+", "-"];
        for s in can_not_parse_tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, 20, 3);
            assert_eq!(
                format!("Parser error: can't parse the string value {s} to decimal"),
                result_128.unwrap_err().to_string()
            );
            let result_256 = parse_decimal::<Decimal256Type>(s, 20, 3);
            assert_eq!(
                format!("Parser error: can't parse the string value {s} to decimal"),
                result_256.unwrap_err().to_string()
            );
        }
        let overflow_parse_tests = ["12345678", "12345678.9", "99999999.99"];
        for s in overflow_parse_tests {
            let result_128 = parse_decimal::<Decimal128Type>(s, 10, 3);
            let expected_128 = "Parser error: parse decimal overflow";
            let actual_128 = result_128.unwrap_err().to_string();

            assert!(
                actual_128.contains(expected_128),
                "actual: '{actual_128}', expected: '{expected_128}'"
            );

            let result_256 = parse_decimal::<Decimal256Type>(s, 10, 3);
            let expected_256 = "Parser error: parse decimal overflow";
            let actual_256 = result_256.unwrap_err().to_string();

            assert!(
                actual_256.contains(expected_256),
                "actual: '{actual_256}', expected: '{expected_256}'"
            );
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
        ];
        for (s, i, scale) in edge_tests_128 {
            let result_128 = parse_decimal::<Decimal128Type>(s, 38, scale);
            assert_eq!(i, result_128.unwrap());
        }
        let edge_tests_256 = [
            (
                "9999999999999999999999999999999999999999999999999999999999999999999999999999",
i256::from_string("9999999999999999999999999999999999999999999999999999999999999999999999999999").unwrap(),
                0,
            ),
            (
                "999999999999999999999999999999999999999999999999999999999999999999999999.9999",
                i256::from_string("9999999999999999999999999999999999999999999999999999999999999999999999999999").unwrap(),
                4,
            ),
            (
                "99999999999999999999999999999999999999999999999999.99999999999999999999999999",
                i256::from_string("9999999999999999999999999999999999999999999999999999999999999999999999999999").unwrap(),
                26,
            ),
            (
                "99999999999999999999999999999999999999999999999999",
                i256::from_string("9999999999999999999999999999999999999999999999999900000000000000000000000000").unwrap(),
                26,
            ),
        ];
        for (s, i, scale) in edge_tests_256 {
            let result = parse_decimal::<Decimal256Type>(s, 76, scale);
            assert_eq!(i, result.unwrap());
        }
    }
}
