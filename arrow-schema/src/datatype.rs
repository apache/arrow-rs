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

use crate::field::Field;

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
    ///
    /// A single Binary array can store up to [`i32::MAX`] bytes
    /// of binary data in total
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    ///
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes
    /// of binary data in total
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding
    ///
    /// A single Utf8 array can store up to [`i32::MAX`] bytes
    /// of string data in total
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    ///
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
    /// of string data in total
    LargeUtf8,
    /// A list of some logical data type with variable length.
    ///
    /// A single List array can store up to [`i32::MAX`] elements in total
    List(Box<Field>),
    /// A list of some logical data type with fixed length.
    FixedSizeList(Box<Field>, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// A single LargeList array can store up to [`i64::MAX`] elements in total
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
    Map(Box<Field>, bool),
}

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UnionMode {
    Sparse,
    Dense,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
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
    #[inline]
    pub fn is_dictionary_key_type(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64
        )
    }

    /// Returns true if this type is nested (List, FixedSizeList, LargeList, Struct, Union,
    /// or Map), or a dictionary of a nested type
    pub fn is_nested(&self) -> bool {
        use DataType::*;
        match self {
            Dictionary(_, v) => DataType::is_nested(v.as_ref()),
            List(_)
            | FixedSizeList(_, _)
            | LargeList(_)
            | Struct(_)
            | Union(_, _, _)
            | Map(_, _) => true,
            _ => false,
        }
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
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => 0,
                DataType::Timestamp(_, s) => {
                    s.as_ref().map(|s| s.capacity()).unwrap_or_default()
                }
                DataType::List(field)
                | DataType::FixedSizeList(field, _)
                | DataType::LargeList(field)
                | DataType::Map(field, _) => field.size(),
                DataType::Struct(fields) | DataType::Union(fields, _, _) => {
                    fields
                        .iter()
                        .map(|field| field.size() - std::mem::size_of_val(field))
                        .sum::<usize>()
                        + (std::mem::size_of::<Field>() * fields.capacity())
                }
                DataType::Dictionary(dt1, dt2) => dt1.size() + dt2.size(),
            }
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
        let last_name = Field::new("last_name", DataType::Utf8, false)
            .with_metadata(HashMap::default());

        let person = DataType::Struct(vec![
            first_name,
            last_name,
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);

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
        let list_a = DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_b = DataType::List(Box::new(Field::new("array", DataType::Int32, true)));
        let list_c = DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_d = DataType::List(Box::new(Field::new("item", DataType::UInt32, true)));
        assert!(list_a.equals_datatype(&list_b));
        assert!(!list_a.equals_datatype(&list_c));
        assert!(!list_b.equals_datatype(&list_c));
        assert!(!list_a.equals_datatype(&list_d));

        let list_e =
            DataType::FixedSizeList(Box::new(Field::new("item", list_a, false)), 3);
        let list_f =
            DataType::FixedSizeList(Box::new(Field::new("array", list_b, false)), 3);
        let list_g = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::FixedSizeBinary(3), true)),
            3,
        );
        assert!(list_e.equals_datatype(&list_f));
        assert!(!list_e.equals_datatype(&list_g));
        assert!(!list_f.equals_datatype(&list_g));

        let list_h = DataType::Struct(vec![Field::new("f1", list_e, true)]);
        let list_i = DataType::Struct(vec![Field::new("f1", list_f.clone(), true)]);
        let list_j = DataType::Struct(vec![Field::new("f1", list_f.clone(), false)]);
        let list_k = DataType::Struct(vec![
            Field::new("f1", list_f.clone(), false),
            Field::new("f2", list_g.clone(), false),
            Field::new("f3", DataType::Utf8, true),
        ]);
        let list_l = DataType::Struct(vec![
            Field::new("ff1", list_f.clone(), false),
            Field::new("ff2", list_g.clone(), false),
            Field::new("ff3", DataType::LargeUtf8, true),
        ]);
        let list_m = DataType::Struct(vec![
            Field::new("ff1", list_f, false),
            Field::new("ff2", list_g, false),
            Field::new("ff3", DataType::Utf8, true),
        ]);
        assert!(list_h.equals_datatype(&list_i));
        assert!(!list_h.equals_datatype(&list_j));
        assert!(!list_k.equals_datatype(&list_l));
        assert!(list_k.equals_datatype(&list_m));
    }

    #[test]
    fn create_struct_type() {
        let _person = DataType::Struct(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);
    }

    #[test]
    fn test_nested() {
        let list = DataType::List(Box::new(Field::new("foo", DataType::Utf8, true)));

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
}
