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

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::{ArrowError, Field, FieldRef, Fields, UnionFields};

/// Datatypes supported by this implementation of Apache Arrow.
///
/// The variants of this enum include primitive fixed size types as well as
/// parametric or nested types. See [`Schema.fbs`] for Arrow's specification.
///
/// # Examples
///
/// Primitive types
/// ```
/// # use arrow_schema::DataType;
/// // create a new 32-bit signed integer
/// let data_type = DataType::Int32;
/// ```
///
/// Nested Types
/// ```
/// # use arrow_schema::{DataType, Field};
/// # use std::sync::Arc;
/// // create a new list of 32-bit signed integers directly
/// let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
/// // Create the same list type with constructor
/// let list_data_type2 = DataType::new_list(DataType::Int32, true);
/// assert_eq!(list_data_type, list_data_type2);
/// ```
///
/// Dictionary Types
/// ```
/// # use arrow_schema::{DataType};
/// // String Dictionary (key type Int32 and value type Utf8)
/// let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
/// ```
///
/// Timestamp Types
/// ```
/// # use arrow_schema::{DataType, TimeUnit};
/// // timestamp with millisecond precision without timezone specified
/// let data_type = DataType::Timestamp(TimeUnit::Millisecond, None);
/// // timestamp with nanosecond precision in UTC timezone
/// let data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
///```
///
/// # Display and FromStr
///
/// The `Display` and `FromStr` implementations for `DataType` are
/// human-readable, parseable, and reversible.
///
/// ```
/// # use arrow_schema::DataType;
/// let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
/// let data_type_string = data_type.to_string();
/// assert_eq!(data_type_string, "Dictionary(Int32, Utf8)");
/// // display can be parsed back into the original type
/// let parsed_data_type: DataType = data_type.to_string().parse().unwrap();
/// assert_eq!(data_type, parsed_data_type);
/// ```
///
/// # Nested Support
/// Currently, the Rust implementation supports the following nested types:
///  - `List<T>`
///  - `LargeList<T>`
///  - `FixedSizeList<T>`
///  - `Struct<T, U, V, ...>`
///  - `Union<T, U, V, ...>`
///  - `Map<K, V>`
///
/// Nested types can themselves be nested within other arrays.
/// For more information on these types please see
/// [the physical memory layout of Apache Arrow]
///
/// [`Schema.fbs`]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
/// [the physical memory layout of Apache Arrow]: https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    /// as a signed 64-bit integer.
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
    ///
    /// ```
    /// # use arrow_schema::{DataType, TimeUnit};
    /// DataType::Timestamp(TimeUnit::Second, None);
    /// DataType::Timestamp(TimeUnit::Second, Some("literal".into()));
    /// DataType::Timestamp(TimeUnit::Second, Some("string".to_string().into()));
    /// ```
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// According to the specification (see [Schema.fbs]), this should be treated as the number of
    /// days, in milliseconds, since the UNIX epoch. Therefore, values must be evenly divisible by
    /// `86_400_000` (the number of milliseconds in a standard day).
    ///
    /// The reason for this is for compatibility with other language's native libraries,
    /// such as Java, which historically lacked a dedicated date type
    /// and only supported timestamps.
    ///
    /// Practically, validation that values of this type are evenly divisible by `86_400_000` is not enforced
    /// by this library for performance and usability reasons. Date64 values will be treated similarly to the
    /// `Timestamp(TimeUnit::Millisecond, None)` type, in that its values will be printed showing the time of
    /// day if the value does not represent an exact day, and arithmetic can be done at the millisecond
    /// granularity to change the time represented.
    ///
    /// Users should prefer using Date32 to cleanly represent the number of days, or one of the Timestamp
    /// variants to include time as part of the representation, depending on their use case.
    ///
    /// For more details, see [#5288](https://github.com/apache/arrow-rs/issues/5288).
    ///
    /// [Schema.fbs]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
    Date64,
    /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either seconds or milliseconds.
    Time32(TimeUnit),
    /// A signed 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either microseconds or nanoseconds.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    ///
    /// A single Binary array can store up to [`i32::MAX`] bytes
    /// of binary data in total.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    ///
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes
    /// of binary data in total.
    LargeBinary,
    /// (NOT YET FULLY SUPPORTED) Opaque binary data of variable length.
    ///
    /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
    ///
    /// Logically the same as [`Self::Binary`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    BinaryView,
    /// A variable-length string in Unicode with UTF-8 encoding.
    ///
    /// A single Utf8 array can store up to [`i32::MAX`] bytes
    /// of string data in total.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    ///
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
    /// of string data in total.
    LargeUtf8,
    /// (NOT YET FULLY SUPPORTED)  A variable-length string in Unicode with UTF-8 encoding
    ///
    /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
    ///
    /// Logically the same as [`Self::Utf8`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    Utf8View,
    /// A list of some logical data type with variable length.
    ///
    /// A single List array can store up to [`i32::MAX`] elements in total.
    List(FieldRef),

    /// (NOT YET FULLY SUPPORTED)  A list of some logical data type with variable length.
    ///
    /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
    ///
    /// The ListView layout is defined by three buffers:
    /// a validity bitmap, an offsets buffer, and an additional sizes buffer.
    /// Sizes and offsets are both 32 bits for this type
    ListView(FieldRef),
    /// A list of some logical data type with fixed length.
    FixedSizeList(FieldRef, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// A single LargeList array can store up to [`i64::MAX`] elements in total.
    LargeList(FieldRef),

    /// (NOT YET FULLY SUPPORTED)  A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
    ///
    /// The LargeListView layout is defined by three buffers:
    /// a validity bitmap, an offsets buffer, and an additional sizes buffer.
    /// Sizes and offsets are both 64 bits for this type
    LargeListView(FieldRef),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Fields),
    /// A nested datatype that can represent slots of differing types. Components:
    ///
    /// 1. [`UnionFields`]
    /// 2. The type of union (Sparse or Dense)
    Union(UnionFields, UnionMode),
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
    ///
    /// In certain situations, scale could be negative number. For
    /// negative scale, it is the number of padding 0 to the right
    /// of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal
    /// has precision 3 and scale -2.
    Decimal128(u8, i8),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    ///
    /// In certain situations, scale could be negative number. For
    /// negative scale, it is the number of padding 0 to the right
    /// of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal
    /// has precision 3 and scale -2.
    Decimal256(u8, i8),
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
    Map(FieldRef, bool),
    /// A run-end encoding (REE) is a variation of run-length encoding (RLE). These
    /// encodings are well-suited for representing data containing sequences of the
    /// same value, called runs. Each run is represented as a value and an integer giving
    /// the index in the array where the run ends.
    ///
    /// A run-end encoded array has no buffers by itself, but has two child arrays. The
    /// first child array, called the run ends array, holds either 16, 32, or 64-bit
    /// signed integers. The actual values of each run are held in the second child array.
    ///
    /// These child arrays are prescribed the standard names of "run_ends" and "values"
    /// respectively.
    RunEndEncoded(FieldRef, FieldRef),
}

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UnionMode {
    Sparse,
    Dense,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Parses `str` into a `DataType`.
///
/// This is the reverse of [`DataType`]'s `Display`
/// impl, and maintains the invariant that
/// `DataType::try_from(&data_type.to_string()).unwrap() == data_type`
///
/// # Example
/// ```
/// use arrow_schema::DataType;
///
/// let data_type: DataType = "Int32".parse().unwrap();
/// assert_eq!(data_type, DataType::Int32);
/// ```
impl FromStr for DataType {
    type Err = ArrowError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::datatype_parse::parse_data_type(s)
    }
}

impl TryFrom<&str> for DataType {
    type Error = ArrowError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl DataType {
    /// Returns true if the type is primitive: (numeric, temporal).
    #[inline]
    pub fn is_primitive(&self) -> bool {
        self.is_numeric() || self.is_temporal()
    }

    /// Returns true if this type is numeric: (UInt*, Int*, Float*, Decimal*).
    #[inline]
    pub fn is_numeric(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            UInt8
                | UInt16
                | UInt32
                | UInt64
                | Int8
                | Int16
                | Int32
                | Int64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
        )
    }

    /// Returns true if this type is temporal: (Date*, Time*, Duration, or Interval).
    #[inline]
    pub fn is_temporal(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            Date32 | Date64 | Timestamp(_, _) | Time32(_) | Time64(_) | Duration(_) | Interval(_)
        )
    }

    /// Returns true if this type is floating: (Float*).
    #[inline]
    pub fn is_floating(&self) -> bool {
        use DataType::*;
        matches!(self, Float16 | Float32 | Float64)
    }

    /// Returns true if this type is integer: (Int*, UInt*).
    #[inline]
    pub fn is_integer(&self) -> bool {
        self.is_signed_integer() || self.is_unsigned_integer()
    }

    /// Returns true if this type is signed integer: (Int*).
    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        use DataType::*;
        matches!(self, Int8 | Int16 | Int32 | Int64)
    }

    /// Returns true if this type is unsigned integer: (UInt*).
    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        use DataType::*;
        matches!(self, UInt8 | UInt16 | UInt32 | UInt64)
    }

    /// Returns true if this type is valid as a dictionary key
    #[inline]
    pub fn is_dictionary_key_type(&self) -> bool {
        self.is_integer()
    }

    /// Returns true if this type is valid for run-ends array in RunArray
    #[inline]
    pub fn is_run_ends_type(&self) -> bool {
        use DataType::*;
        matches!(self, Int16 | Int32 | Int64)
    }

    /// Returns true if this type is nested (List, FixedSizeList, LargeList, Struct, Union,
    /// or Map), or a dictionary of a nested type
    #[inline]
    pub fn is_nested(&self) -> bool {
        use DataType::*;
        match self {
            Dictionary(_, v) => DataType::is_nested(v.as_ref()),
            List(_) | FixedSizeList(_, _) | LargeList(_) | Struct(_) | Union(_, _) | Map(_, _) => {
                true
            }
            _ => false,
        }
    }

    /// Returns true if this type is DataType::Null.
    #[inline]
    pub fn is_null(&self) -> bool {
        use DataType::*;
        matches!(self, Null)
    }

    /// Compares the datatype with another, ignoring nested field names
    /// and metadata.
    pub fn equals_datatype(&self, other: &DataType) -> bool {
        match (&self, other) {
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b)) => {
                a.is_nullable() == b.is_nullable() && a.data_type().equals_datatype(b.data_type())
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
            (DataType::Map(a_field, a_is_sorted), DataType::Map(b_field, b_is_sorted)) => {
                a_field.is_nullable() == b_field.is_nullable()
                    && a_field.data_type().equals_datatype(b_field.data_type())
                    && a_is_sorted == b_is_sorted
            }
            (DataType::Dictionary(a_key, a_value), DataType::Dictionary(b_key, b_value)) => {
                a_key.equals_datatype(b_key) && a_value.equals_datatype(b_value)
            }
            (
                DataType::RunEndEncoded(a_run_ends, a_values),
                DataType::RunEndEncoded(b_run_ends, b_values),
            ) => {
                a_run_ends.is_nullable() == b_run_ends.is_nullable()
                    && a_run_ends
                        .data_type()
                        .equals_datatype(b_run_ends.data_type())
                    && a_values.is_nullable() == b_values.is_nullable()
                    && a_values.data_type().equals_datatype(b_values.data_type())
            }
            (
                DataType::Union(a_union_fields, a_union_mode),
                DataType::Union(b_union_fields, b_union_mode),
            ) => {
                a_union_mode == b_union_mode
                    && a_union_fields.len() == b_union_fields.len()
                    && a_union_fields.iter().all(|a| {
                        b_union_fields.iter().any(|b| {
                            a.0 == b.0
                                && a.1.is_nullable() == b.1.is_nullable()
                                && a.1.data_type().equals_datatype(b.1.data_type())
                        })
                    })
            }
            _ => self == other,
        }
    }

    /// Returns the bit width of this type if it is a primitive type
    ///
    /// Returns `None` if not a primitive type
    #[inline]
    pub fn primitive_width(&self) -> Option<usize> {
        match self {
            DataType::Null => None,
            DataType::Boolean => None,
            DataType::Int8 | DataType::UInt8 => Some(1),
            DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => Some(4),
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => Some(8),
            DataType::Timestamp(_, _) => Some(8),
            DataType::Date32 | DataType::Time32(_) => Some(4),
            DataType::Date64 | DataType::Time64(_) => Some(8),
            DataType::Duration(_) => Some(8),
            DataType::Interval(IntervalUnit::YearMonth) => Some(4),
            DataType::Interval(IntervalUnit::DayTime) => Some(8),
            DataType::Interval(IntervalUnit::MonthDayNano) => Some(16),
            DataType::Decimal128(_, _) => Some(16),
            DataType::Decimal256(_, _) => Some(32),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => None,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => None,
            DataType::FixedSizeBinary(_) => None,
            DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _) => None,
            DataType::FixedSizeList(_, _) => None,
            DataType::Struct(_) => None,
            DataType::Union(_, _) => None,
            DataType::Dictionary(_, _) => None,
            DataType::RunEndEncoded(_, _) => None,
        }
    }

    /// Return size of this instance in bytes.
    ///
    /// Includes the size of `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + match self {
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => 0,
                DataType::Timestamp(_, s) => s.as_ref().map(|s| s.len()).unwrap_or_default(),
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _)
                | DataType::LargeList(field)
                | DataType::LargeListView(field)
                | DataType::Map(field, _) => field.size(),
                DataType::Struct(fields) => fields.size(),
                DataType::Union(fields, _) => fields.size(),
                DataType::Dictionary(dt1, dt2) => dt1.size() + dt2.size(),
                DataType::RunEndEncoded(run_ends, values) => {
                    run_ends.size() - std::mem::size_of_val(run_ends) + values.size()
                        - std::mem::size_of_val(values)
                }
            }
    }

    /// Check to see if `self` is a superset of `other`
    ///
    /// If DataType is a nested type, then it will check to see if the nested type is a superset of the other nested type
    /// else it will check to see if the DataType is equal to the other DataType
    pub fn contains(&self, other: &DataType) -> bool {
        match (self, other) {
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2)) => f1.contains(f2),
            (DataType::FixedSizeList(f1, s1), DataType::FixedSizeList(f2, s2)) => {
                s1 == s2 && f1.contains(f2)
            }
            (DataType::Map(f1, s1), DataType::Map(f2, s2)) => s1 == s2 && f1.contains(f2),
            (DataType::Struct(f1), DataType::Struct(f2)) => f1.contains(f2),
            (DataType::Union(f1, s1), DataType::Union(f2, s2)) => {
                s1 == s2
                    && f1
                        .iter()
                        .all(|f1| f2.iter().any(|f2| f1.0 == f2.0 && f1.1.contains(f2.1)))
            }
            (DataType::Dictionary(k1, v1), DataType::Dictionary(k2, v2)) => {
                k1.contains(k2) && v1.contains(v2)
            }
            _ => self == other,
        }
    }

    /// Create a [`DataType::List`] with elements of the specified type
    /// and nullability, and conventionally named inner [`Field`] (`"item"`).
    ///
    /// To specify field level metadata, construct the inner [`Field`]
    /// directly via [`Field::new`] or [`Field::new_list_field`].
    pub fn new_list(data_type: DataType, nullable: bool) -> Self {
        DataType::List(Arc::new(Field::new_list_field(data_type, nullable)))
    }

    /// Create a [`DataType::LargeList`] with elements of the specified type
    /// and nullability, and conventionally named inner [`Field`] (`"item"`).
    ///
    /// To specify field level metadata, construct the inner [`Field`]
    /// directly via [`Field::new`] or [`Field::new_list_field`].
    pub fn new_large_list(data_type: DataType, nullable: bool) -> Self {
        DataType::LargeList(Arc::new(Field::new_list_field(data_type, nullable)))
    }

    /// Create a [`DataType::FixedSizeList`] with elements of the specified type, size
    /// and nullability, and conventionally named inner [`Field`] (`"item"`).
    ///
    /// To specify field level metadata, construct the inner [`Field`]
    /// directly via [`Field::new`] or [`Field::new_list_field`].
    pub fn new_fixed_size_list(data_type: DataType, size: i32, nullable: bool) -> Self {
        DataType::FixedSizeList(Arc::new(Field::new_list_field(data_type, nullable)), size)
    }
}

/// The maximum precision for [DataType::Decimal128] values
pub const DECIMAL128_MAX_PRECISION: u8 = 38;

/// The maximum scale for [DataType::Decimal128] values
pub const DECIMAL128_MAX_SCALE: i8 = 38;

/// The maximum precision for [DataType::Decimal256] values
pub const DECIMAL256_MAX_PRECISION: u8 = 76;

/// The maximum scale for [DataType::Decimal256] values
pub const DECIMAL256_MAX_SCALE: i8 = 76;

/// The default scale for [DataType::Decimal128] and [DataType::Decimal256]
/// values
pub const DECIMAL_DEFAULT_SCALE: i8 = 10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "serde")]
    fn serde_struct_type() {
        use std::collections::HashMap;

        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: HashMap<String, String> = kv_array.iter().cloned().collect();

        // Non-empty map: should be converted as JSON obj { ... }
        let first_name =
            Field::new("first_name", DataType::Utf8, false).with_metadata(field_metadata);

        // Empty map: should be omitted.
        let last_name =
            Field::new("last_name", DataType::Utf8, false).with_metadata(HashMap::default());

        let person = DataType::Struct(Fields::from(vec![
            first_name,
            last_name,
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ])),
                false,
            ),
        ]));

        let serialized = serde_json::to_string(&person).unwrap();

        // NOTE that this is testing the default (derived) serialization format, not the
        // JSON format specified in metadata.md

        assert_eq!(
            "{\"Struct\":[\
             {\"name\":\"first_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{\"k\":\"v\"}},\
             {\"name\":\"last_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},\
             {\"name\":\"address\",\"data_type\":{\"Struct\":\
             [{\"name\":\"street\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},\
             {\"name\":\"zip\",\"data_type\":\"UInt16\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}}\
             ]},\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}}]}",
            serialized
        );

        let deserialized = serde_json::from_str(&serialized).unwrap();

        assert_eq!(person, deserialized);
    }

    #[test]
    fn test_list_datatype_equality() {
        // tests that list type equality is checked while ignoring list names
        let list_a = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_b = DataType::List(Arc::new(Field::new("array", DataType::Int32, true)));
        let list_c = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_d = DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
        assert!(list_a.equals_datatype(&list_b));
        assert!(!list_a.equals_datatype(&list_c));
        assert!(!list_b.equals_datatype(&list_c));
        assert!(!list_a.equals_datatype(&list_d));

        let list_e =
            DataType::FixedSizeList(Arc::new(Field::new("item", list_a.clone(), false)), 3);
        let list_f =
            DataType::FixedSizeList(Arc::new(Field::new("array", list_b.clone(), false)), 3);
        let list_g = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::FixedSizeBinary(3), true)),
            3,
        );
        assert!(list_e.equals_datatype(&list_f));
        assert!(!list_e.equals_datatype(&list_g));
        assert!(!list_f.equals_datatype(&list_g));

        let list_h = DataType::Struct(Fields::from(vec![Field::new("f1", list_e, true)]));
        let list_i = DataType::Struct(Fields::from(vec![Field::new("f1", list_f.clone(), true)]));
        let list_j = DataType::Struct(Fields::from(vec![Field::new("f1", list_f.clone(), false)]));
        let list_k = DataType::Struct(Fields::from(vec![
            Field::new("f1", list_f.clone(), false),
            Field::new("f2", list_g.clone(), false),
            Field::new("f3", DataType::Utf8, true),
        ]));
        let list_l = DataType::Struct(Fields::from(vec![
            Field::new("ff1", list_f.clone(), false),
            Field::new("ff2", list_g.clone(), false),
            Field::new("ff3", DataType::LargeUtf8, true),
        ]));
        let list_m = DataType::Struct(Fields::from(vec![
            Field::new("ff1", list_f, false),
            Field::new("ff2", list_g, false),
            Field::new("ff3", DataType::Utf8, true),
        ]));
        assert!(list_h.equals_datatype(&list_i));
        assert!(!list_h.equals_datatype(&list_j));
        assert!(!list_k.equals_datatype(&list_l));
        assert!(list_k.equals_datatype(&list_m));

        let list_n = DataType::Map(Arc::new(Field::new("f1", list_a.clone(), true)), true);
        let list_o = DataType::Map(Arc::new(Field::new("f2", list_b.clone(), true)), true);
        let list_p = DataType::Map(Arc::new(Field::new("f2", list_b.clone(), true)), false);
        let list_q = DataType::Map(Arc::new(Field::new("f2", list_c.clone(), true)), true);
        let list_r = DataType::Map(Arc::new(Field::new("f1", list_a.clone(), false)), true);

        assert!(list_n.equals_datatype(&list_o));
        assert!(!list_n.equals_datatype(&list_p));
        assert!(!list_n.equals_datatype(&list_q));
        assert!(!list_n.equals_datatype(&list_r));

        let list_s = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(list_a));
        let list_t = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(list_b.clone()));
        let list_u = DataType::Dictionary(Box::new(DataType::Int8), Box::new(list_b));
        let list_v = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(list_c));

        assert!(list_s.equals_datatype(&list_t));
        assert!(!list_s.equals_datatype(&list_u));
        assert!(!list_s.equals_datatype(&list_v));

        let union_a = DataType::Union(
            UnionFields::new(
                vec![1, 2],
                vec![
                    Field::new("f1", DataType::Utf8, false),
                    Field::new("f2", DataType::UInt8, false),
                ],
            ),
            UnionMode::Sparse,
        );
        let union_b = DataType::Union(
            UnionFields::new(
                vec![1, 2],
                vec![
                    Field::new("ff1", DataType::Utf8, false),
                    Field::new("ff2", DataType::UInt8, false),
                ],
            ),
            UnionMode::Sparse,
        );
        let union_c = DataType::Union(
            UnionFields::new(
                vec![2, 1],
                vec![
                    Field::new("fff2", DataType::UInt8, false),
                    Field::new("fff1", DataType::Utf8, false),
                ],
            ),
            UnionMode::Sparse,
        );
        let union_d = DataType::Union(
            UnionFields::new(
                vec![2, 1],
                vec![
                    Field::new("fff1", DataType::Int8, false),
                    Field::new("fff2", DataType::UInt8, false),
                ],
            ),
            UnionMode::Sparse,
        );
        let union_e = DataType::Union(
            UnionFields::new(
                vec![1, 2],
                vec![
                    Field::new("f1", DataType::Utf8, true),
                    Field::new("f2", DataType::UInt8, false),
                ],
            ),
            UnionMode::Sparse,
        );

        assert!(union_a.equals_datatype(&union_b));
        assert!(union_a.equals_datatype(&union_c));
        assert!(!union_a.equals_datatype(&union_d));
        assert!(!union_a.equals_datatype(&union_e));

        let list_w = DataType::RunEndEncoded(
            Arc::new(Field::new("f1", DataType::Int64, true)),
            Arc::new(Field::new("f2", DataType::Utf8, true)),
        );
        let list_x = DataType::RunEndEncoded(
            Arc::new(Field::new("ff1", DataType::Int64, true)),
            Arc::new(Field::new("ff2", DataType::Utf8, true)),
        );
        let list_y = DataType::RunEndEncoded(
            Arc::new(Field::new("ff1", DataType::UInt16, true)),
            Arc::new(Field::new("ff2", DataType::Utf8, true)),
        );
        let list_z = DataType::RunEndEncoded(
            Arc::new(Field::new("f1", DataType::Int64, false)),
            Arc::new(Field::new("f2", DataType::Utf8, true)),
        );

        assert!(list_w.equals_datatype(&list_x));
        assert!(!list_w.equals_datatype(&list_y));
        assert!(!list_w.equals_datatype(&list_z));
    }

    #[test]
    fn create_struct_type() {
        let _person = DataType::Struct(Fields::from(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ])),
                false,
            ),
        ]));
    }

    #[test]
    fn test_nested() {
        let list = DataType::List(Arc::new(Field::new("foo", DataType::Utf8, true)));

        assert!(!DataType::is_nested(&DataType::Boolean));
        assert!(!DataType::is_nested(&DataType::Int32));
        assert!(!DataType::is_nested(&DataType::Utf8));
        assert!(DataType::is_nested(&list));

        assert!(!DataType::is_nested(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Boolean)
        )));
        assert!(!DataType::is_nested(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Int64)
        )));
        assert!(!DataType::is_nested(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::LargeUtf8)
        )));
        assert!(DataType::is_nested(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(list)
        )));
    }

    #[test]
    fn test_integer() {
        // is_integer
        assert!(DataType::is_integer(&DataType::Int32));
        assert!(DataType::is_integer(&DataType::UInt64));
        assert!(!DataType::is_integer(&DataType::Float16));

        // is_signed_integer
        assert!(DataType::is_signed_integer(&DataType::Int32));
        assert!(!DataType::is_signed_integer(&DataType::UInt64));
        assert!(!DataType::is_signed_integer(&DataType::Float16));

        // is_unsigned_integer
        assert!(!DataType::is_unsigned_integer(&DataType::Int32));
        assert!(DataType::is_unsigned_integer(&DataType::UInt64));
        assert!(!DataType::is_unsigned_integer(&DataType::Float16));

        // is_dictionary_key_type
        assert!(DataType::is_dictionary_key_type(&DataType::Int32));
        assert!(DataType::is_dictionary_key_type(&DataType::UInt64));
        assert!(!DataType::is_dictionary_key_type(&DataType::Float16));
    }

    #[test]
    fn test_floating() {
        assert!(DataType::is_floating(&DataType::Float16));
        assert!(!DataType::is_floating(&DataType::Int32));
    }

    #[test]
    fn test_datatype_is_null() {
        assert!(DataType::is_null(&DataType::Null));
        assert!(!DataType::is_null(&DataType::Int32));
    }

    #[test]
    fn size_should_not_regress() {
        assert_eq!(std::mem::size_of::<DataType>(), 24);
    }

    #[test]
    #[should_panic(expected = "duplicate type id: 1")]
    fn test_union_with_duplicated_type_id() {
        let type_ids = vec![1, 1];
        let _union = DataType::Union(
            UnionFields::new(
                type_ids,
                vec![
                    Field::new("f1", DataType::Int32, false),
                    Field::new("f2", DataType::Utf8, false),
                ],
            ),
            UnionMode::Dense,
        );
    }

    #[test]
    fn test_try_from_str() {
        let data_type: DataType = "Int32".try_into().unwrap();
        assert_eq!(data_type, DataType::Int32);
    }

    #[test]
    fn test_from_str() {
        let data_type: DataType = "UInt64".parse().unwrap();
        assert_eq!(data_type, DataType::UInt64);
    }
}
