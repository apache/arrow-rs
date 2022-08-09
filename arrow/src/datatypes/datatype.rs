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

use num::{BigInt, Num, ToPrimitive};
use std::cmp::Ordering;
use std::fmt;

use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value, Value::String as VString};

use crate::error::{ArrowError, Result};

use super::Field;

/// The set of datatypes that are supported by this implementation of Apache Arrow.
///
/// The Arrow specification on data types includes some more types.
/// See also [`Schema.fbs`](https://github.com/apache/arrow/blob/master/format/Schema.fbs)
/// for Arrow's specification.
///
/// The variants of this enum include primitive fixed size types as well as parametric or
/// nested types.
/// Currently the Rust implementation supports the following  nested types:
///  - `List<T>`
///  - `Struct<T, U, V, ...>`
///
/// Nested types can themselves be nested within other arrays.
/// For more information on these types please see
/// [the physical memory layout of Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    Timestamp(TimeUnit, Option<String>),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32,
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits). Values are evenly divisible by 86400000.
    Date64,
    /// A 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time32(TimeUnit),
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// A list of some logical data type with variable length.
    List(Box<Field>),
    /// A list of some logical data type with fixed length.
    FixedSizeList(Box<Field>, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    LargeList(Box<Field>),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Vec<Field>),
    /// A nested datatype that can represent slots of differing types. Components:
    ///
    /// 1. [`Field`] for each possible child type the Union can hold
    /// 2. The corresponding `type_id` used to identify which Field
    /// 3. The type of union (Sparse or Dense)
    Union(Vec<Field>, Vec<i8>, UnionMode),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// Exact 128-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    Decimal128(usize, usize),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    Decimal256(usize, usize),
    /// A Map is a logical nested type that is represented as
    ///
    /// `List<entries: Struct<key: K, value: V>>`
    ///
    /// The keys and values are each respectively contiguous.
    /// The key and value types are not constrained, but keys should be
    /// hashable and unique.
    /// Whether the keys are sorted can be set in the `bool` after the `Field`.
    ///
    /// In a field with Map type, the field has a child Struct field, which then
    /// has two children: key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(Box<Field>, bool),
}

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

/// YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO interval in SQL style.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (days, milliseconds) (8-bytes in total).
    DayTime,
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// The values are stored contiguously in 16 byte blocks. Months and
    /// days are encoded as 32 bit integers and nanoseconds is encoded as a
    /// 64 bit integer. All integers are signed. Each field is independent
    /// (e.g. there is no constraint that nanoseconds have the same sign
    /// as days or that the quantity of nanoseconds represents less
    /// than a day's worth of time).
    MonthDayNano,
}

// Sparse or Dense union layouts
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UnionMode {
    Sparse,
    Dense,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// Max value of little-endian format for each precision.
pub(crate) const MAX_DECIMAL_BYTES_FOR_EACH_PRECISION: [[u8; 16]; 38] = [
    9_i128.to_le_bytes(),
    99_i128.to_le_bytes(),
    999_i128.to_le_bytes(),
    9999_i128.to_le_bytes(),
    99999_i128.to_le_bytes(),
    999999_i128.to_le_bytes(),
    9999999_i128.to_le_bytes(),
    99999999_i128.to_le_bytes(),
    999999999_i128.to_le_bytes(),
    9999999999_i128.to_le_bytes(),
    99999999999_i128.to_le_bytes(),
    999999999999_i128.to_le_bytes(),
    9999999999999_i128.to_le_bytes(),
    99999999999999_i128.to_le_bytes(),
    999999999999999_i128.to_le_bytes(),
    9999999999999999_i128.to_le_bytes(),
    99999999999999999_i128.to_le_bytes(),
    999999999999999999_i128.to_le_bytes(),
    9999999999999999999_i128.to_le_bytes(),
    99999999999999999999_i128.to_le_bytes(),
    999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999_i128.to_le_bytes(),
    999999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999999_i128.to_le_bytes(),
    999999999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999999999_i128.to_le_bytes(),
    999999999999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999999999999_i128.to_le_bytes(),
    999999999999999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999999999999999_i128.to_le_bytes(),
    999999999999999999999999999999999999_i128.to_le_bytes(),
    9999999999999999999999999999999999999_i128.to_le_bytes(),
    99999999999999999999999999999999999999_i128.to_le_bytes(),
];

pub(crate) const MIN_DECIMAL_BYTES_FOR_EACH_PRECISION: [[u8; 16]; 38] = [
    (-9_i128).to_le_bytes(),
    (-99_i128).to_le_bytes(),
    (-999_i128).to_le_bytes(),
    (-9999_i128).to_le_bytes(),
    (-99999_i128).to_le_bytes(),
    (-999999_i128).to_le_bytes(),
    (-9999999_i128).to_le_bytes(),
    (-99999999_i128).to_le_bytes(),
    (-999999999_i128).to_le_bytes(),
    (-9999999999_i128).to_le_bytes(),
    (-99999999999_i128).to_le_bytes(),
    (-999999999999_i128).to_le_bytes(),
    (-9999999999999_i128).to_le_bytes(),
    (-99999999999999_i128).to_le_bytes(),
    (-999999999999999_i128).to_le_bytes(),
    (-9999999999999999_i128).to_le_bytes(),
    (-99999999999999999_i128).to_le_bytes(),
    (-999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999999999999999_i128).to_le_bytes(),
    (-999999999999999999999999999999999999_i128).to_le_bytes(),
    (-9999999999999999999999999999999999999_i128).to_le_bytes(),
    (-99999999999999999999999999999999999999_i128).to_le_bytes(),
];

// Max value of little-endian format for each precision.
pub(crate) const MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION: [[u8; 16]; 1] =
    [(-99999999999999999999999999999999999999_i128).to_le_bytes()];

pub(crate) const MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION: [[u8; 16]; 1] =
    [(-99999999999999999999999999999999999999_i128).to_le_bytes()];

/// `MAX_DECIMAL_FOR_EACH_PRECISION[p]` holds the maximum `i128` value
/// that can be stored in [DataType::Decimal128] value of precision `p`
pub const MAX_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = [
    9,
    99,
    999,
    9999,
    99999,
    999999,
    9999999,
    99999999,
    999999999,
    9999999999,
    99999999999,
    999999999999,
    9999999999999,
    99999999999999,
    999999999999999,
    9999999999999999,
    99999999999999999,
    999999999999999999,
    9999999999999999999,
    99999999999999999999,
    999999999999999999999,
    9999999999999999999999,
    99999999999999999999999,
    999999999999999999999999,
    9999999999999999999999999,
    99999999999999999999999999,
    999999999999999999999999999,
    9999999999999999999999999999,
    99999999999999999999999999999,
    999999999999999999999999999999,
    9999999999999999999999999999999,
    99999999999999999999999999999999,
    999999999999999999999999999999999,
    9999999999999999999999999999999999,
    99999999999999999999999999999999999,
    999999999999999999999999999999999999,
    9999999999999999999999999999999999999,
    99999999999999999999999999999999999999,
];

/// `MAX_DECIMAL_FOR_LARGER_PRECISION[p]` holds the maximum integer value
/// that can be stored in [DataType::Decimal256] value of precision `p` > 38
pub const MAX_DECIMAL_FOR_LARGER_PRECISION: [&str; 38] = [
    "999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999999999999999",
    "99999999999999999999999999999999999999999999999999999999999999999999999999",
    "999999999999999999999999999999999999999999999999999999999999999999999999999",
    "9999999999999999999999999999999999999999999999999999999999999999999999999999",
];

/// `MIN_DECIMAL_FOR_EACH_PRECISION[p]` holds the minimum `i128` value
/// that can be stored in a [DataType::Decimal128] value of precision `p`
pub const MIN_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = [
    -9,
    -99,
    -999,
    -9999,
    -99999,
    -999999,
    -9999999,
    -99999999,
    -999999999,
    -9999999999,
    -99999999999,
    -999999999999,
    -9999999999999,
    -99999999999999,
    -999999999999999,
    -9999999999999999,
    -99999999999999999,
    -999999999999999999,
    -9999999999999999999,
    -99999999999999999999,
    -999999999999999999999,
    -9999999999999999999999,
    -99999999999999999999999,
    -999999999999999999999999,
    -9999999999999999999999999,
    -99999999999999999999999999,
    -999999999999999999999999999,
    -9999999999999999999999999999,
    -99999999999999999999999999999,
    -999999999999999999999999999999,
    -9999999999999999999999999999999,
    -99999999999999999999999999999999,
    -999999999999999999999999999999999,
    -9999999999999999999999999999999999,
    -99999999999999999999999999999999999,
    -999999999999999999999999999999999999,
    -9999999999999999999999999999999999999,
    -99999999999999999999999999999999999999,
];

/// `MIN_DECIMAL_FOR_LARGER_PRECISION[p]` holds the minimum integer value
/// that can be stored in a [DataType::Decimal256] value of precision `p` > 38
pub const MIN_DECIMAL_FOR_LARGER_PRECISION: [&str; 38] = [
    "-999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999999999999999999999999999999999999999",
    "-999999999999999999999999999999999999999999999999999999999999999999999999999",
    "-9999999999999999999999999999999999999999999999999999999999999999999999999999",
];

/// The maximum precision for [DataType::Decimal128] values
pub const DECIMAL128_MAX_PRECISION: usize = 38;

/// The maximum scale for [DataType::Decimal128] values
pub const DECIMAL128_MAX_SCALE: usize = 38;

/// The maximum precision for [DataType::Decimal256] values
pub const DECIMAL256_MAX_PRECISION: usize = 76;

/// The maximum scale for [DataType::Decimal256] values
pub const DECIMAL256_MAX_SCALE: usize = 76;

/// The default scale for [DataType::Decimal128] and [DataType::Decimal256] values
pub const DECIMAL_DEFAULT_SCALE: usize = 10;

/// Validates that the specified `i128` value can be properly
/// interpreted as a Decimal number with precision `precision`
#[inline]
pub(crate) fn validate_decimal_precision(value: i128, precision: usize) -> Result<i128> {
    if precision > DECIMAL128_MAX_PRECISION {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Max precision of a Decimal128 is {}, but got {}",
            DECIMAL128_MAX_PRECISION, precision,
        )));
    }

    let max = MAX_DECIMAL_FOR_EACH_PRECISION[precision - 1];
    let min = MIN_DECIMAL_FOR_EACH_PRECISION[precision - 1];

    if value > max {
        Err(ArrowError::InvalidArgumentError(format!(
            "{} is too large to store in a Decimal128 of precision {}. Max is {}",
            value, precision, max
        )))
    } else if value < min {
        Err(ArrowError::InvalidArgumentError(format!(
            "{} is too small to store in a Decimal128 of precision {}. Min is {}",
            value, precision, min
        )))
    } else {
        Ok(value)
    }
}

// duplicate code
#[inline]
fn singed_cmp_le_bytes(left: &[u8], right: &[u8]) -> Ordering {
    assert_eq!(
        left.len(),
        right.len(),
        "Can't compare bytes array with different len: {}, {}",
        left.len(),
        right.len()
    );
    assert_ne!(left.len(), 0, "Can't compare bytes array of length 0");
    let len = left.len();
    // the sign bit is 1, the value is negative
    let left_negative = left[len - 1] >= 0x80_u8;
    let right_negative = right[len - 1] >= 0x80_u8;
    if left_negative != right_negative {
        return match left_negative {
            true => {
                // left is negative value
                // right is positive value
                Ordering::Less
            }
            false => Ordering::Greater,
        };
    }
    for i in 0..len {
        let l_byte = left[len - 1 - i];
        let r_byte = right[len - 1 - i];
        match l_byte.cmp(&r_byte) {
            Ordering::Less => {
                return Ordering::Less;
            }
            Ordering::Greater => {
                return Ordering::Greater;
            }
            Ordering::Equal => {}
        }
    }
    Ordering::Equal
}

#[inline]
pub(crate) fn validate_decimal_precision_with_bytes(
    lt_value: &[u8],
    precision: usize,
) -> Result<()> {
    if precision > DECIMAL128_MAX_PRECISION {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Max precision of a Decimal128 is {}, but got {}",
            DECIMAL128_MAX_PRECISION, precision,
        )));
    }

    let max = MAX_DECIMAL_BYTES_FOR_EACH_PRECISION[precision - 1];
    let min = MIN_DECIMAL_BYTES_FOR_EACH_PRECISION[precision - 1];
    if singed_cmp_le_bytes(lt_value, &max) == Ordering::Greater {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too large to store in a Decimal128 of precision {}. Max is {:?}",
            lt_value, precision, max
        )))
    } else if singed_cmp_le_bytes(lt_value, &min) == Ordering::Less {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too small to store in a Decimal128 of precision {}. Min is {:?}",
            lt_value, precision, min
        )))
    } else {
        Ok(())
    }
}

#[inline]
pub(crate) fn validate_decimal256_precision_with_bytes(
    lt_value: &[u8],
    precision: usize,
) -> Result<()> {
    if precision > DECIMAL256_MAX_PRECISION {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Max precision of a Decima256 is {}, but got {}",
            DECIMAL256_MAX_PRECISION, precision,
        )));
    }
    let max = MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[precision - 1];
    let min = MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[precision - 1];
    if singed_cmp_le_bytes(lt_value, &max) == Ordering::Greater {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too large to store in a Decimal256 of precision {}. Max is {:?}",
            lt_value, precision, max
        )))
    } else if singed_cmp_le_bytes(lt_value, &min) == Ordering::Less {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too small to store in a Decimal256 of precision {}. Min is {:?}",
            lt_value, precision, min
        )))
    } else {
        Ok(())
    }
}

/// Validates that the specified string value can be properly
/// interpreted as a Decimal256 number with precision `precision`
#[inline]
pub(crate) fn validate_decimal256_precision(
    value: &str,
    precision: usize,
) -> Result<BigInt> {
    if precision > 38 {
        let max_str = MAX_DECIMAL_FOR_LARGER_PRECISION[precision - 38 - 1];
        let min_str = MIN_DECIMAL_FOR_LARGER_PRECISION[precision - 38 - 1];

        let max = BigInt::from_str_radix(max_str, 10).unwrap();
        let min = BigInt::from_str_radix(min_str, 10).unwrap();

        let value = BigInt::from_str_radix(value, 10).unwrap();
        if value > max {
            Err(ArrowError::InvalidArgumentError(format!(
                "{} is too large to store in a Decimal256 of precision {}. Max is {}",
                value, precision, max
            )))
        } else if value < min {
            Err(ArrowError::InvalidArgumentError(format!(
                "{} is too small to store in a Decimal256 of precision {}. Min is {}",
                value, precision, min
            )))
        } else {
            Ok(value)
        }
    } else {
        let max = MAX_DECIMAL_FOR_EACH_PRECISION[precision - 1];
        let min = MIN_DECIMAL_FOR_EACH_PRECISION[precision - 1];
        let value = BigInt::from_str_radix(value, 10).unwrap();

        if value.to_i128().unwrap() > max {
            Err(ArrowError::InvalidArgumentError(format!(
                "{} is too large to store in a Decimal256 of precision {}. Max is {}",
                value, precision, max
            )))
        } else if value.to_i128().unwrap() < min {
            Err(ArrowError::InvalidArgumentError(format!(
                "{} is too small to store in a Decimal256 of precision {}. Min is {}",
                value, precision, min
            )))
        } else {
            Ok(value)
        }
    }
}

impl DataType {
    /// Parse a data type from a JSON representation.
    pub(crate) fn from(json: &Value) -> Result<DataType> {
        let default_field = Field::new("", DataType::Boolean, true);
        match *json {
            Value::Object(ref map) => match map.get("name") {
                Some(s) if s == "null" => Ok(DataType::Null),
                Some(s) if s == "bool" => Ok(DataType::Boolean),
                Some(s) if s == "binary" => Ok(DataType::Binary),
                Some(s) if s == "largebinary" => Ok(DataType::LargeBinary),
                Some(s) if s == "utf8" => Ok(DataType::Utf8),
                Some(s) if s == "largeutf8" => Ok(DataType::LargeUtf8),
                Some(s) if s == "fixedsizebinary" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("byteWidth") {
                        Ok(DataType::FixedSizeBinary(size.as_i64().unwrap() as i32))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a byteWidth for fixedsizebinary".to_string(),
                        ))
                    }
                }
                Some(s) if s == "decimal" => {
                    // return a list with any type as its child isn't defined in the map
                    let precision = match map.get("precision") {
                        Some(p) => Ok(p.as_u64().unwrap() as usize),
                        None => Err(ArrowError::ParseError(
                            "Expecting a precision for decimal".to_string(),
                        )),
                    }?;
                    let scale = match map.get("scale") {
                        Some(s) => Ok(s.as_u64().unwrap() as usize),
                        _ => Err(ArrowError::ParseError(
                            "Expecting a scale for decimal".to_string(),
                        )),
                    }?;
                    let bit_width: usize = match map.get("bitWidth") {
                        Some(b) => b.as_u64().unwrap() as usize,
                        _ => 128, // Default bit width
                    };

                    if bit_width == 128 {
                        Ok(DataType::Decimal128(precision, scale))
                    } else if bit_width == 256 {
                        Ok(DataType::Decimal256(precision, scale))
                    } else {
                        Err(ArrowError::ParseError(
                            "Decimal bit_width invalid".to_string(),
                        ))
                    }
                }
                Some(s) if s == "floatingpoint" => match map.get("precision") {
                    Some(p) if p == "HALF" => Ok(DataType::Float16),
                    Some(p) if p == "SINGLE" => Ok(DataType::Float32),
                    Some(p) if p == "DOUBLE" => Ok(DataType::Float64),
                    _ => Err(ArrowError::ParseError(
                        "floatingpoint precision missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "timestamp" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "timestamp unit missing or invalid".to_string(),
                        )),
                    };
                    let tz = match map.get("timezone") {
                        None => Ok(None),
                        Some(VString(tz)) => Ok(Some(tz.clone())),
                        _ => Err(ArrowError::ParseError(
                            "timezone must be a string".to_string(),
                        )),
                    };
                    Ok(DataType::Timestamp(unit?, tz?))
                }
                Some(s) if s == "date" => match map.get("unit") {
                    Some(p) if p == "DAY" => Ok(DataType::Date32),
                    Some(p) if p == "MILLISECOND" => Ok(DataType::Date64),
                    _ => Err(ArrowError::ParseError(
                        "date unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "time" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "time unit missing or invalid".to_string(),
                        )),
                    };
                    match map.get("bitWidth") {
                        Some(p) if p == 32 => Ok(DataType::Time32(unit?)),
                        Some(p) if p == 64 => Ok(DataType::Time64(unit?)),
                        _ => Err(ArrowError::ParseError(
                            "time bitWidth missing or invalid".to_string(),
                        )),
                    }
                }
                Some(s) if s == "duration" => match map.get("unit") {
                    Some(p) if p == "SECOND" => Ok(DataType::Duration(TimeUnit::Second)),
                    Some(p) if p == "MILLISECOND" => {
                        Ok(DataType::Duration(TimeUnit::Millisecond))
                    }
                    Some(p) if p == "MICROSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Microsecond))
                    }
                    Some(p) if p == "NANOSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Nanosecond))
                    }
                    _ => Err(ArrowError::ParseError(
                        "time unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "interval" => match map.get("unit") {
                    Some(p) if p == "DAY_TIME" => {
                        Ok(DataType::Interval(IntervalUnit::DayTime))
                    }
                    Some(p) if p == "YEAR_MONTH" => {
                        Ok(DataType::Interval(IntervalUnit::YearMonth))
                    }
                    Some(p) if p == "MONTH_DAY_NANO" => {
                        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                    }
                    _ => Err(ArrowError::ParseError(
                        "interval unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "int" => match map.get("isSigned") {
                    Some(&Value::Bool(true)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::Int8),
                            Some(16) => Ok(DataType::Int16),
                            Some(32) => Ok(DataType::Int32),
                            Some(64) => Ok(DataType::Int64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    Some(&Value::Bool(false)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::UInt8),
                            Some(16) => Ok(DataType::UInt16),
                            Some(32) => Ok(DataType::UInt32),
                            Some(64) => Ok(DataType::UInt64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    _ => Err(ArrowError::ParseError(
                        "int signed missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "list" => {
                    // return a list with any type as its child isn't defined in the map
                    Ok(DataType::List(Box::new(default_field)))
                }
                Some(s) if s == "largelist" => {
                    // return a largelist with any type as its child isn't defined in the map
                    Ok(DataType::LargeList(Box::new(default_field)))
                }
                Some(s) if s == "fixedsizelist" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("listSize") {
                        Ok(DataType::FixedSizeList(
                            Box::new(default_field),
                            size.as_i64().unwrap() as i32,
                        ))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a listSize for fixedsizelist".to_string(),
                        ))
                    }
                }
                Some(s) if s == "struct" => {
                    // return an empty `struct` type as its children aren't defined in the map
                    Ok(DataType::Struct(vec![]))
                }
                Some(s) if s == "map" => {
                    if let Some(Value::Bool(keys_sorted)) = map.get("keysSorted") {
                        // Return a map with an empty type as its children aren't defined in the map
                        Ok(DataType::Map(Box::new(default_field), *keys_sorted))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a keysSorted for map".to_string(),
                        ))
                    }
                }
                Some(s) if s == "union" => {
                    if let Some(Value::String(mode)) = map.get("mode") {
                        let union_mode = if mode == "SPARSE" {
                            UnionMode::Sparse
                        } else if mode == "DENSE" {
                            UnionMode::Dense
                        } else {
                            return Err(ArrowError::ParseError(format!(
                                "Unknown union mode {:?} for union",
                                mode
                            )));
                        };
                        if let Some(type_ids) = map.get("typeIds") {
                            let type_ids = type_ids
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|t| t.as_i64().unwrap() as i8)
                                .collect::<Vec<_>>();

                            let default_fields = type_ids
                                .iter()
                                .map(|_| default_field.clone())
                                .collect::<Vec<_>>();

                            Ok(DataType::Union(default_fields, type_ids, union_mode))
                        } else {
                            Err(ArrowError::ParseError(
                                "Expecting a typeIds for union ".to_string(),
                            ))
                        }
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a mode for union".to_string(),
                        ))
                    }
                }
                Some(other) => Err(ArrowError::ParseError(format!(
                    "invalid or unsupported type name: {} in {:?}",
                    other, json
                ))),
                None => Err(ArrowError::ParseError("type name missing".to_string())),
            },
            _ => Err(ArrowError::ParseError(
                "invalid json value type".to_string(),
            )),
        }
    }

    /// Generate a JSON representation of the data type.
    pub fn to_json(&self) -> Value {
        match self {
            DataType::Null => json!({"name": "null"}),
            DataType::Boolean => json!({"name": "bool"}),
            DataType::Int8 => json!({"name": "int", "bitWidth": 8, "isSigned": true}),
            DataType::Int16 => json!({"name": "int", "bitWidth": 16, "isSigned": true}),
            DataType::Int32 => json!({"name": "int", "bitWidth": 32, "isSigned": true}),
            DataType::Int64 => json!({"name": "int", "bitWidth": 64, "isSigned": true}),
            DataType::UInt8 => json!({"name": "int", "bitWidth": 8, "isSigned": false}),
            DataType::UInt16 => json!({"name": "int", "bitWidth": 16, "isSigned": false}),
            DataType::UInt32 => json!({"name": "int", "bitWidth": 32, "isSigned": false}),
            DataType::UInt64 => json!({"name": "int", "bitWidth": 64, "isSigned": false}),
            DataType::Float16 => json!({"name": "floatingpoint", "precision": "HALF"}),
            DataType::Float32 => json!({"name": "floatingpoint", "precision": "SINGLE"}),
            DataType::Float64 => json!({"name": "floatingpoint", "precision": "DOUBLE"}),
            DataType::Utf8 => json!({"name": "utf8"}),
            DataType::LargeUtf8 => json!({"name": "largeutf8"}),
            DataType::Binary => json!({"name": "binary"}),
            DataType::LargeBinary => json!({"name": "largebinary"}),
            DataType::FixedSizeBinary(byte_width) => {
                json!({"name": "fixedsizebinary", "byteWidth": byte_width})
            }
            DataType::Struct(_) => json!({"name": "struct"}),
            DataType::Union(_, _, _) => json!({"name": "union"}),
            DataType::List(_) => json!({ "name": "list"}),
            DataType::LargeList(_) => json!({ "name": "largelist"}),
            DataType::FixedSizeList(_, length) => {
                json!({"name":"fixedsizelist", "listSize": length})
            }
            DataType::Time32(unit) => {
                json!({"name": "time", "bitWidth": 32, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Time64(unit) => {
                json!({"name": "time", "bitWidth": 64, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Date32 => {
                json!({"name": "date", "unit": "DAY"})
            }
            DataType::Date64 => {
                json!({"name": "date", "unit": "MILLISECOND"})
            }
            DataType::Timestamp(unit, None) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Timestamp(unit, Some(tz)) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }, "timezone": tz})
            }
            DataType::Interval(unit) => json!({"name": "interval", "unit": match unit {
                IntervalUnit::YearMonth => "YEAR_MONTH",
                IntervalUnit::DayTime => "DAY_TIME",
                IntervalUnit::MonthDayNano => "MONTH_DAY_NANO",
            }}),
            DataType::Duration(unit) => json!({"name": "duration", "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }}),
            DataType::Dictionary(_, _) => json!({ "name": "dictionary"}),
            DataType::Decimal128(precision, scale) => {
                json!({"name": "decimal", "precision": precision, "scale": scale, "bitWidth": 128})
            }
            DataType::Decimal256(precision, scale) => {
                json!({"name": "decimal", "precision": precision, "scale": scale, "bitWidth": 256})
            }
            DataType::Map(_, keys_sorted) => {
                json!({"name": "map", "keysSorted": keys_sorted})
            }
        }
    }

    /// Returns true if this type is numeric: (UInt*, Unit*, or Float*).
    pub fn is_numeric(t: &DataType) -> bool {
        use DataType::*;
        matches!(
            t,
            UInt8
                | UInt16
                | UInt32
                | UInt64
                | Int8
                | Int16
                | Int32
                | Int64
                | Float32
                | Float64
        )
    }

    /// Returns true if this type is temporal: (Date*, Time*, Duration, or Interval).
    pub fn is_temporal(t: &DataType) -> bool {
        use DataType::*;
        matches!(
            t,
            Date32
                | Date64
                | Timestamp(_, _)
                | Time32(_)
                | Time64(_)
                | Duration(_)
                | Interval(_)
        )
    }

    /// Returns true if this type is valid as a dictionary key
    /// (e.g. [`super::ArrowDictionaryKeyType`]
    pub fn is_dictionary_key_type(t: &DataType) -> bool {
        use DataType::*;
        matches!(
            t,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64
        )
    }

    /// Compares the datatype with another, ignoring nested field names
    /// and metadata.
    pub fn equals_datatype(&self, other: &DataType) -> bool {
        match (&self, other) {
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b)) => {
                a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
                a_size == b_size
                    && a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| {
                        a.is_nullable() == b.is_nullable()
                            && a.data_type().equals_datatype(b.data_type())
                    })
            }
            (
                DataType::Map(a_field, a_is_sorted),
                DataType::Map(b_field, b_is_sorted),
            ) => a_field == b_field && a_is_sorted == b_is_sorted,
            _ => self == other,
        }
    }
}
