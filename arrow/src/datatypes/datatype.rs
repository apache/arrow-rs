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

use num::BigInt;
use std::cmp::Ordering;
use std::fmt;

use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value, Value::String as VString};

use crate::error::{ArrowError, Result};
use crate::util::decimal::singed_cmp_le_bytes;

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

// MAX decimal256 value of little-endian format for each precision.
// Each element is the max value of signed 256-bit integer for the specified precision which
// is encoded to the 32-byte width format of little-endian.
pub(crate) const MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION: [[u8; 32]; 76] = [
    [
        9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ],
    [
        99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ],
    [
        231, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ],
    [
        15, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ],
    [
        159, 134, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ],
    [
        63, 66, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ],
    [
        127, 150, 152, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 224, 245, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 201, 154, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 227, 11, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 231, 118, 72, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 15, 165, 212, 232, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 159, 114, 78, 24, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 63, 122, 16, 243, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 127, 198, 164, 126, 141, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 192, 111, 242, 134, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 137, 93, 120, 69, 99, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 99, 167, 179, 182, 224, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 231, 137, 4, 35, 199, 138, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 15, 99, 45, 94, 199, 107, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 159, 222, 197, 173, 201, 53, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 63, 178, 186, 201, 224, 25, 30, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 127, 246, 74, 225, 199, 2, 45, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 160, 237, 204, 206, 27, 194, 211, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 73, 72, 1, 20, 22, 149, 69, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 227, 210, 12, 200, 220, 210, 183, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 231, 60, 128, 208, 159, 60, 46, 59, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 15, 97, 2, 37, 62, 94, 206, 79, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 159, 202, 23, 114, 109, 174, 15, 30, 67, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 63, 234, 237, 116, 70, 208, 156, 44, 159, 12, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 127, 38, 75, 145, 192, 34, 32, 190, 55, 126, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 128, 239, 172, 133, 91, 65, 109, 45, 238, 4, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 9, 91, 193, 56, 147, 141, 68, 198, 77, 49, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 99, 142, 141, 55, 192, 135, 173, 190, 9, 237, 1, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 231, 143, 135, 43, 130, 77, 199, 114, 97, 66, 19, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 15, 159, 75, 179, 21, 7, 201, 123, 206, 151, 192, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 159, 54, 244, 0, 217, 70, 218, 213, 16, 238, 133, 7, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 127, 86, 101, 95, 196, 172, 67, 137, 147, 254, 80, 240, 2, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 96, 245, 185, 171, 191, 164, 92, 195, 241, 41, 99, 29,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 201, 149, 67, 181, 124, 111, 158, 161, 113, 163, 223,
        37, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 227, 217, 163, 20, 223, 90, 48, 80, 112, 98, 188, 122,
        11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 231, 130, 102, 206, 182, 140, 227, 33, 99, 216, 91, 203,
        114, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 15, 29, 1, 16, 36, 127, 227, 82, 223, 115, 150, 241,
        123, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 159, 34, 11, 160, 104, 247, 226, 60, 185, 134, 224, 111,
        215, 44, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 63, 90, 111, 64, 22, 170, 221, 96, 60, 67, 197, 94, 106,
        192, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 127, 134, 89, 132, 222, 164, 168, 200, 91, 160, 180,
        179, 39, 132, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 64, 127, 43, 177, 112, 150, 214, 149, 67, 14, 5,
        141, 41, 175, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 137, 248, 178, 235, 102, 224, 97, 218, 163, 142,
        50, 130, 159, 215, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 99, 181, 253, 52, 5, 196, 210, 135, 102, 146, 249,
        21, 59, 108, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 231, 21, 233, 17, 52, 168, 59, 78, 1, 184, 191,
        219, 78, 58, 172, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 15, 219, 26, 179, 8, 146, 84, 14, 13, 48, 125, 149,
        20, 71, 186, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 159, 142, 12, 255, 86, 180, 77, 143, 130, 224, 227,
        214, 205, 198, 70, 11, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 63, 146, 125, 246, 101, 11, 9, 153, 25, 197, 230,
        100, 10, 196, 195, 112, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 127, 182, 231, 160, 251, 113, 90, 250, 255, 178, 3,
        241, 103, 168, 165, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 32, 13, 73, 212, 115, 136, 199, 255, 253, 36,
        106, 15, 148, 120, 12, 20, 4, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 73, 131, 218, 74, 134, 84, 203, 253, 235, 113,
        37, 154, 200, 181, 124, 200, 40, 0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 227, 32, 137, 236, 62, 77, 241, 233, 55, 115,
        118, 5, 214, 25, 223, 212, 151, 1, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 231, 72, 91, 61, 117, 4, 109, 35, 47, 128,
        160, 54, 92, 2, 183, 80, 238, 15, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 15, 217, 144, 101, 148, 44, 66, 98, 215, 1,
        69, 34, 154, 23, 38, 39, 79, 159, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 159, 122, 168, 247, 203, 189, 149, 214, 105,
        18, 178, 86, 5, 236, 124, 135, 23, 57, 6, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 63, 202, 148, 172, 247, 105, 217, 97, 34, 184,
        244, 98, 53, 56, 225, 74, 235, 58, 62, 0, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 127, 230, 207, 189, 172, 35, 126, 210, 87, 49,
        143, 221, 21, 50, 204, 236, 48, 77, 110, 2, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 0, 31, 106, 191, 100, 237, 56, 110, 237,
        151, 167, 218, 244, 249, 63, 233, 3, 79, 24, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 9, 54, 37, 122, 239, 69, 57, 78, 70, 239,
        139, 138, 144, 195, 127, 28, 39, 22, 243, 0, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 99, 28, 116, 197, 90, 187, 60, 14, 191,
        88, 119, 105, 165, 163, 253, 28, 135, 221, 126, 9, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 231, 27, 137, 182, 139, 81, 95, 142, 118,
        119, 169, 30, 118, 100, 232, 33, 71, 167, 244, 94, 0, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 15, 23, 91, 33, 117, 47, 185, 143, 161,
        170, 158, 50, 157, 236, 19, 83, 199, 136, 142, 181, 3, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 159, 230, 142, 77, 147, 218, 59, 157, 79,
        170, 50, 250, 35, 62, 199, 62, 201, 87, 145, 23, 37, 0, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 63, 2, 149, 7, 193, 137, 86, 36, 28, 167,
        250, 197, 103, 109, 200, 115, 220, 109, 173, 235, 114, 1, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 127, 22, 210, 75, 138, 97, 97, 107, 25,
        135, 202, 187, 13, 70, 212, 133, 156, 74, 198, 52, 125, 14, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 224, 52, 246, 102, 207, 205, 49,
        254, 70, 233, 85, 137, 188, 74, 58, 29, 234, 190, 15, 228, 144, 0, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 201, 16, 158, 5, 26, 10, 242, 237,
        197, 28, 91, 93, 93, 235, 70, 36, 37, 117, 157, 232, 168, 5, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 227, 167, 44, 56, 4, 101, 116, 75,
        187, 31, 143, 165, 165, 49, 197, 106, 115, 147, 38, 22, 153, 56, 0,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 231, 142, 190, 49, 42, 242, 139,
        242, 80, 61, 151, 119, 120, 240, 179, 43, 130, 194, 129, 221, 250, 53, 2,
    ],
    [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 15, 149, 113, 241, 165, 117, 119,
        121, 41, 101, 232, 171, 180, 100, 7, 181, 21, 153, 17, 167, 204, 27, 22,
    ],
];

// MIN decimal256 value of little-endian format for each precision.
// Each element is the min value of signed 256-bit integer for the specified precision which
// is encoded to the 76-byte width format of little-endian.
pub(crate) const MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION: [[u8; 32]; 76] = [
    [
        247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        157, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        25, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        241, 216, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        97, 121, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        193, 189, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        129, 105, 103, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 31, 10, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 54, 101, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 28, 244, 171, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 24, 137, 183, 232, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 240, 90, 43, 23, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 96, 141, 177, 231, 246, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 192, 133, 239, 12, 165, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 128, 57, 91, 129, 114, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 63, 144, 13, 121, 220, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 118, 162, 135, 186, 156, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 156, 88, 76, 73, 31, 242, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 24, 118, 251, 220, 56, 117, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 240, 156, 210, 161, 56, 148, 250, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 96, 33, 58, 82, 54, 202, 201, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 192, 77, 69, 54, 31, 230, 225, 253, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 128, 9, 181, 30, 56, 253, 210, 234, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 95, 18, 51, 49, 228, 61, 44, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 182, 183, 254, 235, 233, 106, 186, 247, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 28, 45, 243, 55, 35, 45, 72, 173, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 24, 195, 127, 47, 96, 195, 209, 196, 252, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 240, 158, 253, 218, 193, 161, 49, 176, 223, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 96, 53, 232, 141, 146, 81, 240, 225, 188, 254, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 192, 21, 18, 139, 185, 47, 99, 211, 96, 243, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 128, 217, 180, 110, 63, 221, 223, 65, 200, 129, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 127, 16, 83, 122, 164, 190, 146, 210, 17, 251, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 246, 164, 62, 199, 108, 114, 187, 57, 178, 206, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 156, 113, 114, 200, 63, 120, 82, 65, 246, 18, 254, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 24, 112, 120, 212, 125, 178, 56, 141, 158, 189, 236, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 240, 96, 180, 76, 234, 248, 54, 132, 49, 104, 63, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 96, 201, 11, 255, 38, 185, 37, 42, 239, 17, 122, 248, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 192, 221, 117, 246, 133, 59, 121, 165, 87, 179, 196, 180, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 128, 169, 154, 160, 59, 83, 188, 118, 108, 1, 175, 15, 253, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 159, 10, 70, 84, 64, 91, 163, 60, 14, 214, 156, 226, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 54, 106, 188, 74, 131, 144, 97, 94, 142, 92, 32, 218, 254, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 28, 38, 92, 235, 32, 165, 207, 175, 143, 157, 67, 133, 244, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 24, 125, 153, 49, 73, 115, 28, 222, 156, 39, 164, 52, 141, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 240, 226, 254, 239, 219, 128, 28, 173, 32, 140, 105, 14, 132, 251,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 96, 221, 244, 95, 151, 8, 29, 195, 70, 121, 31, 144, 40, 211, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 192, 165, 144, 191, 233, 85, 34, 159, 195, 188, 58, 161, 149, 63,
        254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 128, 121, 166, 123, 33, 91, 87, 55, 164, 95, 75, 76, 216, 123,
        238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 191, 128, 212, 78, 143, 105, 41, 106, 188, 241, 250, 114, 214,
        80, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 118, 7, 77, 20, 153, 31, 158, 37, 92, 113, 205, 125, 96, 40,
        249, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 156, 74, 2, 203, 250, 59, 45, 120, 153, 109, 6, 234, 196, 147,
        187, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 24, 234, 22, 238, 203, 87, 196, 177, 254, 71, 64, 36, 177, 197,
        83, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 240, 36, 229, 76, 247, 109, 171, 241, 242, 207, 130, 106, 235,
        184, 69, 229, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 96, 113, 243, 0, 169, 75, 178, 112, 125, 31, 28, 41, 50, 57,
        185, 244, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 192, 109, 130, 9, 154, 244, 246, 102, 230, 58, 25, 155, 245,
        59, 60, 143, 245, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 128, 73, 24, 95, 4, 142, 165, 5, 0, 77, 252, 14, 152, 87, 90,
        152, 151, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 223, 242, 182, 43, 140, 119, 56, 0, 2, 219, 149, 240, 107,
        135, 243, 235, 251, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 182, 124, 37, 181, 121, 171, 52, 2, 20, 142, 218, 101, 55,
        74, 131, 55, 215, 255, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 28, 223, 118, 19, 193, 178, 14, 22, 200, 140, 137, 250, 41,
        230, 32, 43, 104, 254, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 24, 183, 164, 194, 138, 251, 146, 220, 208, 127, 95, 201,
        163, 253, 72, 175, 17, 240, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 240, 38, 111, 154, 107, 211, 189, 157, 40, 254, 186, 221,
        101, 232, 217, 216, 176, 96, 255, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 96, 133, 87, 8, 52, 66, 106, 41, 150, 237, 77, 169, 250, 19,
        131, 120, 232, 198, 249, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 192, 53, 107, 83, 8, 150, 38, 158, 221, 71, 11, 157, 202,
        199, 30, 181, 20, 197, 193, 255, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 128, 25, 48, 66, 83, 220, 129, 45, 168, 206, 112, 34, 234,
        205, 51, 19, 207, 178, 145, 253, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 255, 224, 149, 64, 155, 18, 199, 145, 18, 104, 88, 37,
        11, 6, 192, 22, 252, 176, 231, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 246, 201, 218, 133, 16, 186, 198, 177, 185, 16, 116, 117,
        111, 60, 128, 227, 216, 233, 12, 255, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 156, 227, 139, 58, 165, 68, 195, 241, 64, 167, 136, 150,
        90, 92, 2, 227, 120, 34, 129, 246, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 24, 228, 118, 73, 116, 174, 160, 113, 137, 136, 86, 225,
        137, 155, 23, 222, 184, 88, 11, 161, 255, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 240, 232, 164, 222, 138, 208, 70, 112, 94, 85, 97, 205,
        98, 19, 236, 172, 56, 119, 113, 74, 252, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 96, 25, 113, 178, 108, 37, 196, 98, 176, 85, 205, 5, 220,
        193, 56, 193, 54, 168, 110, 232, 218, 255, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 192, 253, 106, 248, 62, 118, 169, 219, 227, 88, 5, 58,
        152, 146, 55, 140, 35, 146, 82, 20, 141, 254, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 128, 233, 45, 180, 117, 158, 158, 148, 230, 120, 53, 68,
        242, 185, 43, 122, 99, 181, 57, 203, 130, 241, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 31, 203, 9, 153, 48, 50, 206, 1, 185, 22, 170, 118,
        67, 181, 197, 226, 21, 65, 240, 27, 111, 255, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 54, 239, 97, 250, 229, 245, 13, 18, 58, 227, 164, 162,
        162, 20, 185, 219, 218, 138, 98, 23, 87, 250, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 28, 88, 211, 199, 251, 154, 139, 180, 68, 224, 112,
        90, 90, 206, 58, 149, 140, 108, 217, 233, 102, 199, 255,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 24, 113, 65, 206, 213, 13, 116, 13, 175, 194, 104,
        136, 135, 15, 76, 212, 125, 61, 126, 34, 5, 202, 253,
    ],
    [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 240, 106, 142, 14, 90, 138, 136, 134, 214, 154, 23,
        84, 75, 155, 248, 74, 234, 102, 238, 88, 51, 228, 233,
    ],
];

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

/// Validates that the specified `byte_array` of little-endian format
/// value can be properly interpreted as a Decimal256 number with precision `precision`
#[inline]
pub(crate) fn validate_decimal256_precision_with_lt_bytes(
    lt_value: &[u8],
    precision: usize,
) -> Result<()> {
    if precision > DECIMAL256_MAX_PRECISION {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Max precision of a Decimal256 is {}, but got {}",
            DECIMAL256_MAX_PRECISION, precision,
        )));
    }
    let max = MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[precision - 1];
    let min = MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[precision - 1];

    if singed_cmp_le_bytes(lt_value, &max) == Ordering::Greater {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too large to store in a Decimal256 of precision {}. Max is {:?}",
            BigInt::from_signed_bytes_le(lt_value),
            precision,
            BigInt::from_signed_bytes_le(&max)
        )))
    } else if singed_cmp_le_bytes(lt_value, &min) == Ordering::Less {
        Err(ArrowError::InvalidArgumentError(format!(
            "{:?} is too small to store in a Decimal256 of precision {}. Min is {:?}",
            BigInt::from_signed_bytes_le(lt_value),
            precision,
            BigInt::from_signed_bytes_le(&min)
        )))
    } else {
        Ok(())
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

#[cfg(test)]
mod test {
    use crate::datatypes::datatype::{
        MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION,
        MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION,
    };
    use crate::util::decimal::Decimal256;
    use num::{BigInt, Num};

    #[test]
    fn test_decimal256_min_max_for_precision() {
        // The precision from 1 to 76
        let mut max_value = "9".to_string();
        let mut min_value = "-9".to_string();
        for i in 1..77 {
            let max_decimal =
                Decimal256::from(BigInt::from_str_radix(max_value.as_str(), 10).unwrap());
            let min_decimal =
                Decimal256::from(BigInt::from_str_radix(min_value.as_str(), 10).unwrap());
            let max_bytes = MAX_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[i - 1];
            let min_bytes = MIN_DECIMAL_BYTES_FOR_LARGER_EACH_PRECISION[i - 1];
            max_value += "9";
            min_value += "9";
            assert_eq!(max_decimal.raw_value(), max_bytes);
            assert_eq!(min_decimal.raw_value(), min_bytes);
        }
    }
}
