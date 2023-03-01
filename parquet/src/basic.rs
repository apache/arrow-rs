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

//! Contains Rust mappings for Thrift definition.
//! Refer to `parquet.thrift` file to see raw definitions.

use std::{fmt, str};

use crate::format as parquet;

use crate::errors::{ParquetError, Result};

// Re-export crate::format types used in this module
pub use crate::format::{
    BsonType, DateType, DecimalType, EnumType, IntType, JsonType, ListType, MapType,
    NullType, StringType, TimeType, TimeUnit, TimestampType, UUIDType,
};

// ----------------------------------------------------------------------
// Types from the Thrift definition

// ----------------------------------------------------------------------
// Mirrors `parquet::Type`

/// Types supported by Parquet.
/// These physical types are intended to be used in combination with the encodings to
/// control the on disk storage format.
/// For example INT16 is not included as a type since a good encoding of INT32
/// would handle this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum Type {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ConvertedType`

/// Common types (converted types) used by frameworks when using Parquet.
/// This helps map between types in those frameworks to the base types in Parquet.
/// This is only metadata and not needed to read or write the data.
///
/// This struct was renamed from `LogicalType` in version 4.0.0.
/// If targeting Parquet format 2.4.0 or above, please use [LogicalType] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum ConvertedType {
    NONE,
    /// A BYTE_ARRAY actually contains UTF8 encoded chars.
    UTF8,

    /// A map is converted as an optional field containing a repeated key/value pair.
    MAP,

    /// A key/value pair is converted into a group of two fields.
    MAP_KEY_VALUE,

    /// A list is converted into an optional field containing a repeated field for its
    /// values.
    LIST,

    /// An enum is converted into a binary field
    ENUM,

    /// A decimal value.
    /// This may be used to annotate binary or fixed primitive types. The
    /// underlying byte array stores the unscaled value encoded as two's
    /// complement using big-endian byte order (the most significant byte is the
    /// zeroth element).
    ///
    /// This must be accompanied by a (maximum) precision and a scale in the
    /// SchemaElement. The precision specifies the number of digits in the decimal
    /// and the scale stores the location of the decimal point. For example 1.23
    /// would have precision 3 (3 total digits) and scale 2 (the decimal point is
    /// 2 digits over).
    DECIMAL,

    /// A date stored as days since Unix epoch, encoded as the INT32 physical type.
    DATE,

    /// The total number of milliseconds since midnight. The value is stored as an INT32
    /// physical type.
    TIME_MILLIS,

    /// The total number of microseconds since midnight. The value is stored as an INT64
    /// physical type.
    TIME_MICROS,

    /// Date and time recorded as milliseconds since the Unix epoch.
    /// Recorded as a physical type of INT64.
    TIMESTAMP_MILLIS,

    /// Date and time recorded as microseconds since the Unix epoch.
    /// The value is stored as an INT64 physical type.
    TIMESTAMP_MICROS,

    /// An unsigned 8 bit integer value stored as INT32 physical type.
    UINT_8,

    /// An unsigned 16 bit integer value stored as INT32 physical type.
    UINT_16,

    /// An unsigned 32 bit integer value stored as INT32 physical type.
    UINT_32,

    /// An unsigned 64 bit integer value stored as INT64 physical type.
    UINT_64,

    /// A signed 8 bit integer value stored as INT32 physical type.
    INT_8,

    /// A signed 16 bit integer value stored as INT32 physical type.
    INT_16,

    /// A signed 32 bit integer value stored as INT32 physical type.
    INT_32,

    /// A signed 64 bit integer value stored as INT64 physical type.
    INT_64,

    /// A JSON document embedded within a single UTF8 column.
    JSON,

    /// A BSON document embedded within a single BINARY column.
    BSON,

    /// An interval of time.
    ///
    /// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12.
    /// This data is composed of three separate little endian unsigned integers.
    /// Each stores a component of a duration of time. The first integer identifies
    /// the number of months associated with the duration, the second identifies
    /// the number of days associated with the duration and the third identifies
    /// the number of milliseconds associated with the provided duration.
    /// This duration of time is independent of any particular timezone or date.
    INTERVAL,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::LogicalType`

/// Logical types used by version 2.4.0+ of the Parquet format.
///
/// This is an *entirely new* struct as of version
/// 4.0.0. The struct previously named `LogicalType` was renamed to
/// [`ConvertedType`]. Please see the README.md for more details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        is_adjusted_to_u_t_c: bool,
        unit: TimeUnit,
    },
    Timestamp {
        is_adjusted_to_u_t_c: bool,
        unit: TimeUnit,
    },
    Integer {
        bit_width: i8,
        is_signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::FieldRepetitionType`

/// Representation of field types in schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Repetition {
    /// Field is required (can not be null) and each record has exactly 1 value.
    REQUIRED,
    /// Field is optional (can be null) and each record has 0 or 1 values.
    OPTIONAL,
    /// Field is repeated and can contain 0 or more values.
    REPEATED,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::Encoding`

/// Encodings supported by Parquet.
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[allow(non_camel_case_types)]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    PLAIN,

    /// **Deprecated** dictionary encoding.
    ///
    /// The values in the dictionary are encoded using PLAIN encoding.
    /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
    /// PLAIN encoding is used for dictionary page.
    PLAIN_DICTIONARY,

    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    RLE,

    /// Bit packed encoding.
    ///
    /// This can only be used if the data has a known max width.
    /// Usable for definition/repetition levels encoding.
    BIT_PACKED,

    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DELTA_BINARY_PACKED,

    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
    DELTA_LENGTH_BYTE_ARRAY,

    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DELTA_BYTE_ARRAY,

    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RLE_DICTIONARY,

    /// Encoding for floating-point data.
    ///
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of an FP value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards.
    BYTE_STREAM_SPLIT,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::CompressionCodec`

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
    LZ4_RAW,
    QCOM,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::PageType`

/// Available data pages for Parquet file format.
/// Note that some of the page types may not be supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum PageType {
    DATA_PAGE,
    INDEX_PAGE,
    DICTIONARY_PAGE,
    DATA_PAGE_V2,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ColumnOrder`

/// Sort order for page and column statistics.
///
/// Types are associated with sort orders and column stats are aggregated using a sort
/// order, and a sort order should be considered when comparing values with statistics
/// min/max.
///
/// See reference in
/// <https://github.com/apache/parquet-cpp/blob/master/src/parquet/types.h>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum SortOrder {
    /// Signed (either value or legacy byte-wise) comparison.
    SIGNED,
    /// Unsigned (depending on physical type either value or byte-wise) comparison.
    UNSIGNED,
    /// Comparison is undefined.
    UNDEFINED,
}

impl SortOrder {
    /// Returns true if this is [`Self::SIGNED`]
    pub fn is_signed(&self) -> bool {
        matches!(self, Self::SIGNED)
    }
}

/// Column order that specifies what method was used to aggregate min/max values for
/// statistics.
///
/// If column order is undefined, then it is the legacy behaviour and all values should
/// be compared as signed values/bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum ColumnOrder {
    /// Column uses the order defined by its logical or physical type
    /// (if there is no logical type), parquet-format 2.4.0+.
    TYPE_DEFINED_ORDER(SortOrder),
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    UNDEFINED,
}

impl ColumnOrder {
    /// Returns sort order for a physical/logical type.
    pub fn get_sort_order(
        logical_type: Option<LogicalType>,
        converted_type: ConvertedType,
        physical_type: Type,
    ) -> SortOrder {
        // TODO: Should this take converted and logical type, for compatibility?
        match logical_type {
            Some(logical) => match logical {
                LogicalType::String
                | LogicalType::Enum
                | LogicalType::Json
                | LogicalType::Bson => SortOrder::UNSIGNED,
                LogicalType::Integer { is_signed, .. } => match is_signed {
                    true => SortOrder::SIGNED,
                    false => SortOrder::UNSIGNED,
                },
                LogicalType::Map | LogicalType::List => SortOrder::UNDEFINED,
                LogicalType::Decimal { .. } => SortOrder::SIGNED,
                LogicalType::Date => SortOrder::SIGNED,
                LogicalType::Time { .. } => SortOrder::SIGNED,
                LogicalType::Timestamp { .. } => SortOrder::SIGNED,
                LogicalType::Unknown => SortOrder::UNDEFINED,
                LogicalType::Uuid => SortOrder::UNSIGNED,
            },
            // Fall back to converted type
            None => Self::get_converted_sort_order(converted_type, physical_type),
        }
    }

    fn get_converted_sort_order(
        converted_type: ConvertedType,
        physical_type: Type,
    ) -> SortOrder {
        match converted_type {
            // Unsigned byte-wise comparison.
            ConvertedType::UTF8
            | ConvertedType::JSON
            | ConvertedType::BSON
            | ConvertedType::ENUM => SortOrder::UNSIGNED,

            ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32
            | ConvertedType::INT_64 => SortOrder::SIGNED,

            ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64 => SortOrder::UNSIGNED,

            // Signed comparison of the represented value.
            ConvertedType::DECIMAL => SortOrder::SIGNED,

            ConvertedType::DATE => SortOrder::SIGNED,

            ConvertedType::TIME_MILLIS
            | ConvertedType::TIME_MICROS
            | ConvertedType::TIMESTAMP_MILLIS
            | ConvertedType::TIMESTAMP_MICROS => SortOrder::SIGNED,

            ConvertedType::INTERVAL => SortOrder::UNDEFINED,

            ConvertedType::LIST | ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => {
                SortOrder::UNDEFINED
            }

            // Fall back to physical type.
            ConvertedType::NONE => Self::get_default_sort_order(physical_type),
        }
    }

    /// Returns default sort order based on physical type.
    fn get_default_sort_order(physical_type: Type) -> SortOrder {
        match physical_type {
            // Order: false, true
            Type::BOOLEAN => SortOrder::UNSIGNED,
            Type::INT32 | Type::INT64 => SortOrder::SIGNED,
            Type::INT96 => SortOrder::UNDEFINED,
            // Notes to remember when comparing float/double values:
            // If the min is a NaN, it should be ignored.
            // If the max is a NaN, it should be ignored.
            // If the min is +0, the row group may contain -0 values as well.
            // If the max is -0, the row group may contain +0 values as well.
            // When looking for NaN values, min and max should be ignored.
            Type::FLOAT | Type::DOUBLE => SortOrder::SIGNED,
            // Unsigned byte-wise comparison
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => SortOrder::UNSIGNED,
        }
    }

    /// Returns sort order associated with this column order.
    pub fn sort_order(&self) -> SortOrder {
        match *self {
            ColumnOrder::TYPE_DEFINED_ORDER(order) => order,
            ColumnOrder::UNDEFINED => SortOrder::SIGNED,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for ConvertedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for Repetition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

// ----------------------------------------------------------------------
// parquet::Type <=> Type conversion

impl TryFrom<parquet::Type> for Type {
    type Error = ParquetError;

    fn try_from(value: parquet::Type) -> Result<Self> {
        Ok(match value {
            parquet::Type::BOOLEAN => Type::BOOLEAN,
            parquet::Type::INT32 => Type::INT32,
            parquet::Type::INT64 => Type::INT64,
            parquet::Type::INT96 => Type::INT96,
            parquet::Type::FLOAT => Type::FLOAT,
            parquet::Type::DOUBLE => Type::DOUBLE,
            parquet::Type::BYTE_ARRAY => Type::BYTE_ARRAY,
            parquet::Type::FIXED_LEN_BYTE_ARRAY => Type::FIXED_LEN_BYTE_ARRAY,
            _ => return Err(general_err!("unexpected parquet type: {}", value.0)),
        })
    }
}

impl From<Type> for parquet::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::BOOLEAN => parquet::Type::BOOLEAN,
            Type::INT32 => parquet::Type::INT32,
            Type::INT64 => parquet::Type::INT64,
            Type::INT96 => parquet::Type::INT96,
            Type::FLOAT => parquet::Type::FLOAT,
            Type::DOUBLE => parquet::Type::DOUBLE,
            Type::BYTE_ARRAY => parquet::Type::BYTE_ARRAY,
            Type::FIXED_LEN_BYTE_ARRAY => parquet::Type::FIXED_LEN_BYTE_ARRAY,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::ConvertedType <=> ConvertedType conversion

impl TryFrom<Option<parquet::ConvertedType>> for ConvertedType {
    type Error = ParquetError;

    fn try_from(option: Option<parquet::ConvertedType>) -> Result<Self> {
        Ok(match option {
            None => ConvertedType::NONE,
            Some(value) => match value {
                parquet::ConvertedType::UTF8 => ConvertedType::UTF8,
                parquet::ConvertedType::MAP => ConvertedType::MAP,
                parquet::ConvertedType::MAP_KEY_VALUE => ConvertedType::MAP_KEY_VALUE,
                parquet::ConvertedType::LIST => ConvertedType::LIST,
                parquet::ConvertedType::ENUM => ConvertedType::ENUM,
                parquet::ConvertedType::DECIMAL => ConvertedType::DECIMAL,
                parquet::ConvertedType::DATE => ConvertedType::DATE,
                parquet::ConvertedType::TIME_MILLIS => ConvertedType::TIME_MILLIS,
                parquet::ConvertedType::TIME_MICROS => ConvertedType::TIME_MICROS,
                parquet::ConvertedType::TIMESTAMP_MILLIS => {
                    ConvertedType::TIMESTAMP_MILLIS
                }
                parquet::ConvertedType::TIMESTAMP_MICROS => {
                    ConvertedType::TIMESTAMP_MICROS
                }
                parquet::ConvertedType::UINT_8 => ConvertedType::UINT_8,
                parquet::ConvertedType::UINT_16 => ConvertedType::UINT_16,
                parquet::ConvertedType::UINT_32 => ConvertedType::UINT_32,
                parquet::ConvertedType::UINT_64 => ConvertedType::UINT_64,
                parquet::ConvertedType::INT_8 => ConvertedType::INT_8,
                parquet::ConvertedType::INT_16 => ConvertedType::INT_16,
                parquet::ConvertedType::INT_32 => ConvertedType::INT_32,
                parquet::ConvertedType::INT_64 => ConvertedType::INT_64,
                parquet::ConvertedType::JSON => ConvertedType::JSON,
                parquet::ConvertedType::BSON => ConvertedType::BSON,
                parquet::ConvertedType::INTERVAL => ConvertedType::INTERVAL,
                _ => {
                    return Err(general_err!(
                        "unexpected parquet converted type: {}",
                        value.0
                    ))
                }
            },
        })
    }
}

impl From<ConvertedType> for Option<parquet::ConvertedType> {
    fn from(value: ConvertedType) -> Self {
        match value {
            ConvertedType::NONE => None,
            ConvertedType::UTF8 => Some(parquet::ConvertedType::UTF8),
            ConvertedType::MAP => Some(parquet::ConvertedType::MAP),
            ConvertedType::MAP_KEY_VALUE => Some(parquet::ConvertedType::MAP_KEY_VALUE),
            ConvertedType::LIST => Some(parquet::ConvertedType::LIST),
            ConvertedType::ENUM => Some(parquet::ConvertedType::ENUM),
            ConvertedType::DECIMAL => Some(parquet::ConvertedType::DECIMAL),
            ConvertedType::DATE => Some(parquet::ConvertedType::DATE),
            ConvertedType::TIME_MILLIS => Some(parquet::ConvertedType::TIME_MILLIS),
            ConvertedType::TIME_MICROS => Some(parquet::ConvertedType::TIME_MICROS),
            ConvertedType::TIMESTAMP_MILLIS => {
                Some(parquet::ConvertedType::TIMESTAMP_MILLIS)
            }
            ConvertedType::TIMESTAMP_MICROS => {
                Some(parquet::ConvertedType::TIMESTAMP_MICROS)
            }
            ConvertedType::UINT_8 => Some(parquet::ConvertedType::UINT_8),
            ConvertedType::UINT_16 => Some(parquet::ConvertedType::UINT_16),
            ConvertedType::UINT_32 => Some(parquet::ConvertedType::UINT_32),
            ConvertedType::UINT_64 => Some(parquet::ConvertedType::UINT_64),
            ConvertedType::INT_8 => Some(parquet::ConvertedType::INT_8),
            ConvertedType::INT_16 => Some(parquet::ConvertedType::INT_16),
            ConvertedType::INT_32 => Some(parquet::ConvertedType::INT_32),
            ConvertedType::INT_64 => Some(parquet::ConvertedType::INT_64),
            ConvertedType::JSON => Some(parquet::ConvertedType::JSON),
            ConvertedType::BSON => Some(parquet::ConvertedType::BSON),
            ConvertedType::INTERVAL => Some(parquet::ConvertedType::INTERVAL),
        }
    }
}

// ----------------------------------------------------------------------
// parquet::LogicalType <=> LogicalType conversion

impl From<parquet::LogicalType> for LogicalType {
    fn from(value: parquet::LogicalType) -> Self {
        match value {
            parquet::LogicalType::STRING(_) => LogicalType::String,
            parquet::LogicalType::MAP(_) => LogicalType::Map,
            parquet::LogicalType::LIST(_) => LogicalType::List,
            parquet::LogicalType::ENUM(_) => LogicalType::Enum,
            parquet::LogicalType::DECIMAL(t) => LogicalType::Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            parquet::LogicalType::DATE(_) => LogicalType::Date,
            parquet::LogicalType::TIME(t) => LogicalType::Time {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: t.unit,
            },
            parquet::LogicalType::TIMESTAMP(t) => LogicalType::Timestamp {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: t.unit,
            },
            parquet::LogicalType::INTEGER(t) => LogicalType::Integer {
                bit_width: t.bit_width,
                is_signed: t.is_signed,
            },
            parquet::LogicalType::UNKNOWN(_) => LogicalType::Unknown,
            parquet::LogicalType::JSON(_) => LogicalType::Json,
            parquet::LogicalType::BSON(_) => LogicalType::Bson,
            parquet::LogicalType::UUID(_) => LogicalType::Uuid,
        }
    }
}

impl From<LogicalType> for parquet::LogicalType {
    fn from(value: LogicalType) -> Self {
        match value {
            LogicalType::String => parquet::LogicalType::STRING(Default::default()),
            LogicalType::Map => parquet::LogicalType::MAP(Default::default()),
            LogicalType::List => parquet::LogicalType::LIST(Default::default()),
            LogicalType::Enum => parquet::LogicalType::ENUM(Default::default()),
            LogicalType::Decimal { scale, precision } => {
                parquet::LogicalType::DECIMAL(DecimalType { scale, precision })
            }
            LogicalType::Date => parquet::LogicalType::DATE(Default::default()),
            LogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => parquet::LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c,
                unit,
            }),
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => parquet::LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c,
                unit,
            }),
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => parquet::LogicalType::INTEGER(IntType {
                bit_width,
                is_signed,
            }),
            LogicalType::Unknown => parquet::LogicalType::UNKNOWN(Default::default()),
            LogicalType::Json => parquet::LogicalType::JSON(Default::default()),
            LogicalType::Bson => parquet::LogicalType::BSON(Default::default()),
            LogicalType::Uuid => parquet::LogicalType::UUID(Default::default()),
        }
    }
}

// ----------------------------------------------------------------------
// LogicalType <=> ConvertedType conversion

// Note: To prevent type loss when converting from ConvertedType to LogicalType,
// the conversion from ConvertedType -> LogicalType is not implemented.
// Such type loss includes:
// - Not knowing the decimal scale and precision of ConvertedType
// - Time and timestamp nanosecond precision, that is not supported in ConvertedType.

impl From<Option<LogicalType>> for ConvertedType {
    fn from(value: Option<LogicalType>) -> Self {
        match value {
            Some(value) => match value {
                LogicalType::String => ConvertedType::UTF8,
                LogicalType::Map => ConvertedType::MAP,
                LogicalType::List => ConvertedType::LIST,
                LogicalType::Enum => ConvertedType::ENUM,
                LogicalType::Decimal { .. } => ConvertedType::DECIMAL,
                LogicalType::Date => ConvertedType::DATE,
                LogicalType::Time { unit, .. } => match unit {
                    TimeUnit::MILLIS(_) => ConvertedType::TIME_MILLIS,
                    TimeUnit::MICROS(_) => ConvertedType::TIME_MICROS,
                    TimeUnit::NANOS(_) => ConvertedType::NONE,
                },
                LogicalType::Timestamp { unit, .. } => match unit {
                    TimeUnit::MILLIS(_) => ConvertedType::TIMESTAMP_MILLIS,
                    TimeUnit::MICROS(_) => ConvertedType::TIMESTAMP_MICROS,
                    TimeUnit::NANOS(_) => ConvertedType::NONE,
                },
                LogicalType::Integer {
                    bit_width,
                    is_signed,
                } => match (bit_width, is_signed) {
                    (8, true) => ConvertedType::INT_8,
                    (16, true) => ConvertedType::INT_16,
                    (32, true) => ConvertedType::INT_32,
                    (64, true) => ConvertedType::INT_64,
                    (8, false) => ConvertedType::UINT_8,
                    (16, false) => ConvertedType::UINT_16,
                    (32, false) => ConvertedType::UINT_32,
                    (64, false) => ConvertedType::UINT_64,
                    t => panic!("Integer type {t:?} is not supported"),
                },
                LogicalType::Unknown => ConvertedType::NONE,
                LogicalType::Json => ConvertedType::JSON,
                LogicalType::Bson => ConvertedType::BSON,
                LogicalType::Uuid => ConvertedType::NONE,
            },
            None => ConvertedType::NONE,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::FieldRepetitionType <=> Repetition conversion

impl TryFrom<parquet::FieldRepetitionType> for Repetition {
    type Error = ParquetError;

    fn try_from(value: parquet::FieldRepetitionType) -> Result<Self> {
        Ok(match value {
            parquet::FieldRepetitionType::REQUIRED => Repetition::REQUIRED,
            parquet::FieldRepetitionType::OPTIONAL => Repetition::OPTIONAL,
            parquet::FieldRepetitionType::REPEATED => Repetition::REPEATED,
            _ => {
                return Err(general_err!(
                    "unexpected parquet repetition type: {}",
                    value.0
                ))
            }
        })
    }
}

impl From<Repetition> for parquet::FieldRepetitionType {
    fn from(value: Repetition) -> Self {
        match value {
            Repetition::REQUIRED => parquet::FieldRepetitionType::REQUIRED,
            Repetition::OPTIONAL => parquet::FieldRepetitionType::OPTIONAL,
            Repetition::REPEATED => parquet::FieldRepetitionType::REPEATED,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::Encoding <=> Encoding conversion

impl TryFrom<parquet::Encoding> for Encoding {
    type Error = ParquetError;

    fn try_from(value: parquet::Encoding) -> Result<Self> {
        Ok(match value {
            parquet::Encoding::PLAIN => Encoding::PLAIN,
            parquet::Encoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
            parquet::Encoding::RLE => Encoding::RLE,
            parquet::Encoding::BIT_PACKED => Encoding::BIT_PACKED,
            parquet::Encoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                Encoding::DELTA_LENGTH_BYTE_ARRAY
            }
            parquet::Encoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            parquet::Encoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
            parquet::Encoding::BYTE_STREAM_SPLIT => Encoding::BYTE_STREAM_SPLIT,
            _ => return Err(general_err!("unexpected parquet encoding: {}", value.0)),
        })
    }
}

impl From<Encoding> for parquet::Encoding {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::PLAIN => parquet::Encoding::PLAIN,
            Encoding::PLAIN_DICTIONARY => parquet::Encoding::PLAIN_DICTIONARY,
            Encoding::RLE => parquet::Encoding::RLE,
            Encoding::BIT_PACKED => parquet::Encoding::BIT_PACKED,
            Encoding::DELTA_BINARY_PACKED => parquet::Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
            }
            Encoding::DELTA_BYTE_ARRAY => parquet::Encoding::DELTA_BYTE_ARRAY,
            Encoding::RLE_DICTIONARY => parquet::Encoding::RLE_DICTIONARY,
            Encoding::BYTE_STREAM_SPLIT => parquet::Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::CompressionCodec <=> Compression conversion

impl TryFrom<parquet::CompressionCodec> for Compression {
    type Error = ParquetError;

    fn try_from(value: parquet::CompressionCodec) -> Result<Self> {
        Ok(match value {
            parquet::CompressionCodec::UNCOMPRESSED => Compression::UNCOMPRESSED,
            parquet::CompressionCodec::SNAPPY => Compression::SNAPPY,
            parquet::CompressionCodec::GZIP => Compression::GZIP,
            parquet::CompressionCodec::LZO => Compression::LZO,
            parquet::CompressionCodec::BROTLI => Compression::BROTLI,
            parquet::CompressionCodec::LZ4 => Compression::LZ4,
            parquet::CompressionCodec::ZSTD => Compression::ZSTD,
            parquet::CompressionCodec::LZ4_RAW => Compression::LZ4_RAW,
            parquet::CompressionCodec::QCOM => Compression::QCOM,
            _ => {
                return Err(general_err!(
                    "unexpected parquet compression codec: {}",
                    value.0
                ))
            }
        })
    }
}

impl From<Compression> for parquet::CompressionCodec {
    fn from(value: Compression) -> Self {
        match value {
            Compression::UNCOMPRESSED => parquet::CompressionCodec::UNCOMPRESSED,
            Compression::SNAPPY => parquet::CompressionCodec::SNAPPY,
            Compression::GZIP => parquet::CompressionCodec::GZIP,
            Compression::LZO => parquet::CompressionCodec::LZO,
            Compression::BROTLI => parquet::CompressionCodec::BROTLI,
            Compression::LZ4 => parquet::CompressionCodec::LZ4,
            Compression::ZSTD => parquet::CompressionCodec::ZSTD,
            Compression::LZ4_RAW => parquet::CompressionCodec::LZ4_RAW,
            Compression::QCOM => parquet::CompressionCodec::QCOM,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::PageType <=> PageType conversion

impl TryFrom<parquet::PageType> for PageType {
    type Error = ParquetError;

    fn try_from(value: parquet::PageType) -> Result<Self> {
        Ok(match value {
            parquet::PageType::DATA_PAGE => PageType::DATA_PAGE,
            parquet::PageType::INDEX_PAGE => PageType::INDEX_PAGE,
            parquet::PageType::DICTIONARY_PAGE => PageType::DICTIONARY_PAGE,
            parquet::PageType::DATA_PAGE_V2 => PageType::DATA_PAGE_V2,
            _ => return Err(general_err!("unexpected parquet page type: {}", value.0)),
        })
    }
}

impl From<PageType> for parquet::PageType {
    fn from(value: PageType) -> Self {
        match value {
            PageType::DATA_PAGE => parquet::PageType::DATA_PAGE,
            PageType::INDEX_PAGE => parquet::PageType::INDEX_PAGE,
            PageType::DICTIONARY_PAGE => parquet::PageType::DICTIONARY_PAGE,
            PageType::DATA_PAGE_V2 => parquet::PageType::DATA_PAGE_V2,
        }
    }
}

// ----------------------------------------------------------------------
// String conversions for schema parsing.

impl str::FromStr for Repetition {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "REQUIRED" => Ok(Repetition::REQUIRED),
            "OPTIONAL" => Ok(Repetition::OPTIONAL),
            "REPEATED" => Ok(Repetition::REPEATED),
            other => Err(general_err!("Invalid parquet repetition {}", other)),
        }
    }
}

impl str::FromStr for Type {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "INT32" => Ok(Type::INT32),
            "INT64" => Ok(Type::INT64),
            "INT96" => Ok(Type::INT96),
            "FLOAT" => Ok(Type::FLOAT),
            "DOUBLE" => Ok(Type::DOUBLE),
            "BYTE_ARRAY" | "BINARY" => Ok(Type::BYTE_ARRAY),
            "FIXED_LEN_BYTE_ARRAY" => Ok(Type::FIXED_LEN_BYTE_ARRAY),
            other => Err(general_err!("Invalid parquet type {}", other)),
        }
    }
}

impl str::FromStr for ConvertedType {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "NONE" => Ok(ConvertedType::NONE),
            "UTF8" => Ok(ConvertedType::UTF8),
            "MAP" => Ok(ConvertedType::MAP),
            "MAP_KEY_VALUE" => Ok(ConvertedType::MAP_KEY_VALUE),
            "LIST" => Ok(ConvertedType::LIST),
            "ENUM" => Ok(ConvertedType::ENUM),
            "DECIMAL" => Ok(ConvertedType::DECIMAL),
            "DATE" => Ok(ConvertedType::DATE),
            "TIME_MILLIS" => Ok(ConvertedType::TIME_MILLIS),
            "TIME_MICROS" => Ok(ConvertedType::TIME_MICROS),
            "TIMESTAMP_MILLIS" => Ok(ConvertedType::TIMESTAMP_MILLIS),
            "TIMESTAMP_MICROS" => Ok(ConvertedType::TIMESTAMP_MICROS),
            "UINT_8" => Ok(ConvertedType::UINT_8),
            "UINT_16" => Ok(ConvertedType::UINT_16),
            "UINT_32" => Ok(ConvertedType::UINT_32),
            "UINT_64" => Ok(ConvertedType::UINT_64),
            "INT_8" => Ok(ConvertedType::INT_8),
            "INT_16" => Ok(ConvertedType::INT_16),
            "INT_32" => Ok(ConvertedType::INT_32),
            "INT_64" => Ok(ConvertedType::INT_64),
            "JSON" => Ok(ConvertedType::JSON),
            "BSON" => Ok(ConvertedType::BSON),
            "INTERVAL" => Ok(ConvertedType::INTERVAL),
            other => Err(general_err!("Invalid parquet converted type {}", other)),
        }
    }
}

impl str::FromStr for LogicalType {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            // The type is a placeholder that gets updated elsewhere
            "INTEGER" => Ok(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }),
            "MAP" => Ok(LogicalType::Map),
            "LIST" => Ok(LogicalType::List),
            "ENUM" => Ok(LogicalType::Enum),
            "DECIMAL" => Ok(LogicalType::Decimal {
                precision: -1,
                scale: -1,
            }),
            "DATE" => Ok(LogicalType::Date),
            "TIME" => Ok(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(parquet::MilliSeconds {}),
            }),
            "TIMESTAMP" => Ok(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(parquet::MilliSeconds {}),
            }),
            "STRING" => Ok(LogicalType::String),
            "JSON" => Ok(LogicalType::Json),
            "BSON" => Ok(LogicalType::Bson),
            "UUID" => Ok(LogicalType::Uuid),
            "UNKNOWN" => Ok(LogicalType::Unknown),
            "INTERVAL" => Err(general_err!(
                "Interval parquet logical type not yet supported"
            )),
            other => Err(general_err!("Invalid parquet logical type {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_type() {
        assert_eq!(Type::BOOLEAN.to_string(), "BOOLEAN");
        assert_eq!(Type::INT32.to_string(), "INT32");
        assert_eq!(Type::INT64.to_string(), "INT64");
        assert_eq!(Type::INT96.to_string(), "INT96");
        assert_eq!(Type::FLOAT.to_string(), "FLOAT");
        assert_eq!(Type::DOUBLE.to_string(), "DOUBLE");
        assert_eq!(Type::BYTE_ARRAY.to_string(), "BYTE_ARRAY");
        assert_eq!(
            Type::FIXED_LEN_BYTE_ARRAY.to_string(),
            "FIXED_LEN_BYTE_ARRAY"
        );
    }

    #[test]
    fn test_from_type() {
        assert_eq!(
            Type::try_from(parquet::Type::BOOLEAN).unwrap(),
            Type::BOOLEAN
        );
        assert_eq!(Type::try_from(parquet::Type::INT32).unwrap(), Type::INT32);
        assert_eq!(Type::try_from(parquet::Type::INT64).unwrap(), Type::INT64);
        assert_eq!(Type::try_from(parquet::Type::INT96).unwrap(), Type::INT96);
        assert_eq!(Type::try_from(parquet::Type::FLOAT).unwrap(), Type::FLOAT);
        assert_eq!(Type::try_from(parquet::Type::DOUBLE).unwrap(), Type::DOUBLE);
        assert_eq!(
            Type::try_from(parquet::Type::BYTE_ARRAY).unwrap(),
            Type::BYTE_ARRAY
        );
        assert_eq!(
            Type::try_from(parquet::Type::FIXED_LEN_BYTE_ARRAY).unwrap(),
            Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_type() {
        assert_eq!(parquet::Type::BOOLEAN, Type::BOOLEAN.into());
        assert_eq!(parquet::Type::INT32, Type::INT32.into());
        assert_eq!(parquet::Type::INT64, Type::INT64.into());
        assert_eq!(parquet::Type::INT96, Type::INT96.into());
        assert_eq!(parquet::Type::FLOAT, Type::FLOAT.into());
        assert_eq!(parquet::Type::DOUBLE, Type::DOUBLE.into());
        assert_eq!(parquet::Type::BYTE_ARRAY, Type::BYTE_ARRAY.into());
        assert_eq!(
            parquet::Type::FIXED_LEN_BYTE_ARRAY,
            Type::FIXED_LEN_BYTE_ARRAY.into()
        );
    }

    #[test]
    fn test_from_string_into_type() {
        assert_eq!(
            Type::BOOLEAN.to_string().parse::<Type>().unwrap(),
            Type::BOOLEAN
        );
        assert_eq!(
            Type::INT32.to_string().parse::<Type>().unwrap(),
            Type::INT32
        );
        assert_eq!(
            Type::INT64.to_string().parse::<Type>().unwrap(),
            Type::INT64
        );
        assert_eq!(
            Type::INT96.to_string().parse::<Type>().unwrap(),
            Type::INT96
        );
        assert_eq!(
            Type::FLOAT.to_string().parse::<Type>().unwrap(),
            Type::FLOAT
        );
        assert_eq!(
            Type::DOUBLE.to_string().parse::<Type>().unwrap(),
            Type::DOUBLE
        );
        assert_eq!(
            Type::BYTE_ARRAY.to_string().parse::<Type>().unwrap(),
            Type::BYTE_ARRAY
        );
        assert_eq!("BINARY".parse::<Type>().unwrap(), Type::BYTE_ARRAY);
        assert_eq!(
            Type::FIXED_LEN_BYTE_ARRAY
                .to_string()
                .parse::<Type>()
                .unwrap(),
            Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn test_display_converted_type() {
        assert_eq!(ConvertedType::NONE.to_string(), "NONE");
        assert_eq!(ConvertedType::UTF8.to_string(), "UTF8");
        assert_eq!(ConvertedType::MAP.to_string(), "MAP");
        assert_eq!(ConvertedType::MAP_KEY_VALUE.to_string(), "MAP_KEY_VALUE");
        assert_eq!(ConvertedType::LIST.to_string(), "LIST");
        assert_eq!(ConvertedType::ENUM.to_string(), "ENUM");
        assert_eq!(ConvertedType::DECIMAL.to_string(), "DECIMAL");
        assert_eq!(ConvertedType::DATE.to_string(), "DATE");
        assert_eq!(ConvertedType::TIME_MILLIS.to_string(), "TIME_MILLIS");
        assert_eq!(ConvertedType::DATE.to_string(), "DATE");
        assert_eq!(ConvertedType::TIME_MICROS.to_string(), "TIME_MICROS");
        assert_eq!(
            ConvertedType::TIMESTAMP_MILLIS.to_string(),
            "TIMESTAMP_MILLIS"
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MICROS.to_string(),
            "TIMESTAMP_MICROS"
        );
        assert_eq!(ConvertedType::UINT_8.to_string(), "UINT_8");
        assert_eq!(ConvertedType::UINT_16.to_string(), "UINT_16");
        assert_eq!(ConvertedType::UINT_32.to_string(), "UINT_32");
        assert_eq!(ConvertedType::UINT_64.to_string(), "UINT_64");
        assert_eq!(ConvertedType::INT_8.to_string(), "INT_8");
        assert_eq!(ConvertedType::INT_16.to_string(), "INT_16");
        assert_eq!(ConvertedType::INT_32.to_string(), "INT_32");
        assert_eq!(ConvertedType::INT_64.to_string(), "INT_64");
        assert_eq!(ConvertedType::JSON.to_string(), "JSON");
        assert_eq!(ConvertedType::BSON.to_string(), "BSON");
        assert_eq!(ConvertedType::INTERVAL.to_string(), "INTERVAL");
        assert_eq!(ConvertedType::DECIMAL.to_string(), "DECIMAL")
    }

    #[test]
    fn test_from_converted_type() {
        let parquet_conv_none: Option<parquet::ConvertedType> = None;
        assert_eq!(
            ConvertedType::try_from(parquet_conv_none).unwrap(),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::UTF8)).unwrap(),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::MAP)).unwrap(),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::MAP_KEY_VALUE)).unwrap(),
            ConvertedType::MAP_KEY_VALUE
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::LIST)).unwrap(),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::ENUM)).unwrap(),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::DECIMAL)).unwrap(),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::DATE)).unwrap(),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::TIME_MILLIS)).unwrap(),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::TIME_MICROS)).unwrap(),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::TIMESTAMP_MILLIS))
                .unwrap(),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::TIMESTAMP_MICROS))
                .unwrap(),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::UINT_8)).unwrap(),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::UINT_16)).unwrap(),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::UINT_32)).unwrap(),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::UINT_64)).unwrap(),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::INT_8)).unwrap(),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::INT_16)).unwrap(),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::INT_32)).unwrap(),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::INT_64)).unwrap(),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::JSON)).unwrap(),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::BSON)).unwrap(),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::INTERVAL)).unwrap(),
            ConvertedType::INTERVAL
        );
        assert_eq!(
            ConvertedType::try_from(Some(parquet::ConvertedType::DECIMAL)).unwrap(),
            ConvertedType::DECIMAL
        )
    }

    #[test]
    fn test_into_converted_type() {
        let converted_type: Option<parquet::ConvertedType> = None;
        assert_eq!(converted_type, ConvertedType::NONE.into());
        assert_eq!(
            Some(parquet::ConvertedType::UTF8),
            ConvertedType::UTF8.into()
        );
        assert_eq!(Some(parquet::ConvertedType::MAP), ConvertedType::MAP.into());
        assert_eq!(
            Some(parquet::ConvertedType::MAP_KEY_VALUE),
            ConvertedType::MAP_KEY_VALUE.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::LIST),
            ConvertedType::LIST.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::ENUM),
            ConvertedType::ENUM.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::DECIMAL),
            ConvertedType::DECIMAL.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::DATE),
            ConvertedType::DATE.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TIME_MILLIS),
            ConvertedType::TIME_MILLIS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TIME_MICROS),
            ConvertedType::TIME_MICROS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TIMESTAMP_MILLIS),
            ConvertedType::TIMESTAMP_MILLIS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TIMESTAMP_MICROS),
            ConvertedType::TIMESTAMP_MICROS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::UINT_8),
            ConvertedType::UINT_8.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::UINT_16),
            ConvertedType::UINT_16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::UINT_32),
            ConvertedType::UINT_32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::UINT_64),
            ConvertedType::UINT_64.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::INT_8),
            ConvertedType::INT_8.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::INT_16),
            ConvertedType::INT_16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::INT_32),
            ConvertedType::INT_32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::INT_64),
            ConvertedType::INT_64.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::JSON),
            ConvertedType::JSON.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::BSON),
            ConvertedType::BSON.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::INTERVAL),
            ConvertedType::INTERVAL.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::DECIMAL),
            ConvertedType::DECIMAL.into()
        )
    }

    #[test]
    fn test_from_string_into_converted_type() {
        assert_eq!(
            ConvertedType::NONE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::UTF8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::MAP
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::MAP_KEY_VALUE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::MAP_KEY_VALUE
        );
        assert_eq!(
            ConvertedType::LIST
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::ENUM
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::DECIMAL
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::DATE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::TIME_MILLIS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::TIME_MICROS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MILLIS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MICROS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::UINT_8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::UINT_16
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::UINT_32
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::UINT_64
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::INT_8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::INT_16
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::INT_32
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::INT_64
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::JSON
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::BSON
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::INTERVAL
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INTERVAL
        );
        assert_eq!(
            ConvertedType::DECIMAL
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::DECIMAL
        )
    }

    #[test]
    fn test_logical_to_converted_type() {
        let logical_none: Option<LogicalType> = None;
        assert_eq!(ConvertedType::from(logical_none), ConvertedType::NONE);
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Decimal {
                precision: 20,
                scale: 5
            })),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Bson)),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Json)),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::String)),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Date)),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Time {
                unit: TimeUnit::MILLIS(Default::default()),
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Time {
                unit: TimeUnit::MICROS(Default::default()),
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Time {
                unit: TimeUnit::NANOS(Default::default()),
                is_adjusted_to_u_t_c: false,
            })),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::MILLIS(Default::default()),
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::MICROS(Default::default()),
                is_adjusted_to_u_t_c: false,
            })),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::NANOS(Default::default()),
                is_adjusted_to_u_t_c: false,
            })),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false
            })),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true
            })),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false
            })),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true
            })),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false
            })),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: true
            })),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false
            })),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: true
            })),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::List)),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Map)),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Uuid)),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Enum)),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Unknown)),
            ConvertedType::NONE
        );
    }

    #[test]
    fn test_display_repetition() {
        assert_eq!(Repetition::REQUIRED.to_string(), "REQUIRED");
        assert_eq!(Repetition::OPTIONAL.to_string(), "OPTIONAL");
        assert_eq!(Repetition::REPEATED.to_string(), "REPEATED");
    }

    #[test]
    fn test_from_repetition() {
        assert_eq!(
            Repetition::try_from(parquet::FieldRepetitionType::REQUIRED).unwrap(),
            Repetition::REQUIRED
        );
        assert_eq!(
            Repetition::try_from(parquet::FieldRepetitionType::OPTIONAL).unwrap(),
            Repetition::OPTIONAL
        );
        assert_eq!(
            Repetition::try_from(parquet::FieldRepetitionType::REPEATED).unwrap(),
            Repetition::REPEATED
        );
    }

    #[test]
    fn test_into_repetition() {
        assert_eq!(
            parquet::FieldRepetitionType::REQUIRED,
            Repetition::REQUIRED.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::OPTIONAL,
            Repetition::OPTIONAL.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::REPEATED,
            Repetition::REPEATED.into()
        );
    }

    #[test]
    fn test_from_string_into_repetition() {
        assert_eq!(
            Repetition::REQUIRED
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::REQUIRED
        );
        assert_eq!(
            Repetition::OPTIONAL
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::OPTIONAL
        );
        assert_eq!(
            Repetition::REPEATED
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::REPEATED
        );
    }

    #[test]
    fn test_display_encoding() {
        assert_eq!(Encoding::PLAIN.to_string(), "PLAIN");
        assert_eq!(Encoding::PLAIN_DICTIONARY.to_string(), "PLAIN_DICTIONARY");
        assert_eq!(Encoding::RLE.to_string(), "RLE");
        assert_eq!(Encoding::BIT_PACKED.to_string(), "BIT_PACKED");
        assert_eq!(
            Encoding::DELTA_BINARY_PACKED.to_string(),
            "DELTA_BINARY_PACKED"
        );
        assert_eq!(
            Encoding::DELTA_LENGTH_BYTE_ARRAY.to_string(),
            "DELTA_LENGTH_BYTE_ARRAY"
        );
        assert_eq!(Encoding::DELTA_BYTE_ARRAY.to_string(), "DELTA_BYTE_ARRAY");
        assert_eq!(Encoding::RLE_DICTIONARY.to_string(), "RLE_DICTIONARY");
    }

    #[test]
    fn test_from_encoding() {
        assert_eq!(
            Encoding::try_from(parquet::Encoding::PLAIN).unwrap(),
            Encoding::PLAIN
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::PLAIN_DICTIONARY).unwrap(),
            Encoding::PLAIN_DICTIONARY
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::RLE).unwrap(),
            Encoding::RLE
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::BIT_PACKED).unwrap(),
            Encoding::BIT_PACKED
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::DELTA_BINARY_PACKED).unwrap(),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY).unwrap(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY
        );
        assert_eq!(
            Encoding::try_from(parquet::Encoding::DELTA_BYTE_ARRAY).unwrap(),
            Encoding::DELTA_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_encoding() {
        assert_eq!(parquet::Encoding::PLAIN, Encoding::PLAIN.into());
        assert_eq!(
            parquet::Encoding::PLAIN_DICTIONARY,
            Encoding::PLAIN_DICTIONARY.into()
        );
        assert_eq!(parquet::Encoding::RLE, Encoding::RLE.into());
        assert_eq!(parquet::Encoding::BIT_PACKED, Encoding::BIT_PACKED.into());
        assert_eq!(
            parquet::Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_BINARY_PACKED.into()
        );
        assert_eq!(
            parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_LENGTH_BYTE_ARRAY.into()
        );
        assert_eq!(
            parquet::Encoding::DELTA_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY.into()
        );
    }

    #[test]
    fn test_display_compression() {
        assert_eq!(Compression::UNCOMPRESSED.to_string(), "UNCOMPRESSED");
        assert_eq!(Compression::SNAPPY.to_string(), "SNAPPY");
        assert_eq!(Compression::GZIP.to_string(), "GZIP");
        assert_eq!(Compression::LZO.to_string(), "LZO");
        assert_eq!(Compression::BROTLI.to_string(), "BROTLI");
        assert_eq!(Compression::LZ4.to_string(), "LZ4");
        assert_eq!(Compression::ZSTD.to_string(), "ZSTD");
    }

    #[test]
    fn test_from_compression() {
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::UNCOMPRESSED).unwrap(),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::SNAPPY).unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::GZIP).unwrap(),
            Compression::GZIP
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::LZO).unwrap(),
            Compression::LZO
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::BROTLI).unwrap(),
            Compression::BROTLI
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::LZ4).unwrap(),
            Compression::LZ4
        );
        assert_eq!(
            Compression::try_from(parquet::CompressionCodec::ZSTD).unwrap(),
            Compression::ZSTD
        );
    }

    #[test]
    fn test_into_compression() {
        assert_eq!(
            parquet::CompressionCodec::UNCOMPRESSED,
            Compression::UNCOMPRESSED.into()
        );
        assert_eq!(
            parquet::CompressionCodec::SNAPPY,
            Compression::SNAPPY.into()
        );
        assert_eq!(parquet::CompressionCodec::GZIP, Compression::GZIP.into());
        assert_eq!(parquet::CompressionCodec::LZO, Compression::LZO.into());
        assert_eq!(
            parquet::CompressionCodec::BROTLI,
            Compression::BROTLI.into()
        );
        assert_eq!(parquet::CompressionCodec::LZ4, Compression::LZ4.into());
        assert_eq!(parquet::CompressionCodec::ZSTD, Compression::ZSTD.into());
    }

    #[test]
    fn test_display_page_type() {
        assert_eq!(PageType::DATA_PAGE.to_string(), "DATA_PAGE");
        assert_eq!(PageType::INDEX_PAGE.to_string(), "INDEX_PAGE");
        assert_eq!(PageType::DICTIONARY_PAGE.to_string(), "DICTIONARY_PAGE");
        assert_eq!(PageType::DATA_PAGE_V2.to_string(), "DATA_PAGE_V2");
    }

    #[test]
    fn test_from_page_type() {
        assert_eq!(
            PageType::try_from(parquet::PageType::DATA_PAGE).unwrap(),
            PageType::DATA_PAGE
        );
        assert_eq!(
            PageType::try_from(parquet::PageType::INDEX_PAGE).unwrap(),
            PageType::INDEX_PAGE
        );
        assert_eq!(
            PageType::try_from(parquet::PageType::DICTIONARY_PAGE).unwrap(),
            PageType::DICTIONARY_PAGE
        );
        assert_eq!(
            PageType::try_from(parquet::PageType::DATA_PAGE_V2).unwrap(),
            PageType::DATA_PAGE_V2
        );
    }

    #[test]
    fn test_into_page_type() {
        assert_eq!(parquet::PageType::DATA_PAGE, PageType::DATA_PAGE.into());
        assert_eq!(parquet::PageType::INDEX_PAGE, PageType::INDEX_PAGE.into());
        assert_eq!(
            parquet::PageType::DICTIONARY_PAGE,
            PageType::DICTIONARY_PAGE.into()
        );
        assert_eq!(
            parquet::PageType::DATA_PAGE_V2,
            PageType::DATA_PAGE_V2.into()
        );
    }

    #[test]
    fn test_display_sort_order() {
        assert_eq!(SortOrder::SIGNED.to_string(), "SIGNED");
        assert_eq!(SortOrder::UNSIGNED.to_string(), "UNSIGNED");
        assert_eq!(SortOrder::UNDEFINED.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_display_column_order() {
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED).to_string(),
            "TYPE_DEFINED_ORDER(SIGNED)"
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED).to_string(),
            "TYPE_DEFINED_ORDER(UNSIGNED)"
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED).to_string(),
            "TYPE_DEFINED_ORDER(UNDEFINED)"
        );
        assert_eq!(ColumnOrder::UNDEFINED.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_column_order_get_logical_type_sort_order() {
        // Helper to check the order in a list of values.
        // Only logical type is checked.
        fn check_sort_order(types: Vec<LogicalType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(
                        Some(tpe),
                        ConvertedType::NONE,
                        Type::BYTE_ARRAY
                    ),
                    expected_order
                );
            }
        }

        // Unsigned comparison (physical type does not matter)
        let unsigned = vec![
            LogicalType::String,
            LogicalType::Json,
            LogicalType::Bson,
            LogicalType::Enum,
            LogicalType::Uuid,
            LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            },
            LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            },
            LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            },
            LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            },
        ];
        check_sort_order(unsigned, SortOrder::UNSIGNED);

        // Signed comparison (physical type does not matter)
        let signed = vec![
            LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            },
            LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            },
            LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            },
            LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            },
            LogicalType::Decimal {
                scale: 20,
                precision: 4,
            },
            LogicalType::Date,
            LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(Default::default()),
            },
            LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS(Default::default()),
            },
            LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS(Default::default()),
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(Default::default()),
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS(Default::default()),
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS(Default::default()),
            },
        ];
        check_sort_order(signed, SortOrder::SIGNED);

        // Undefined comparison
        let undefined = vec![LogicalType::List, LogicalType::Map];
        check_sort_order(undefined, SortOrder::UNDEFINED);
    }

    #[test]
    fn test_column_order_get_coverted_type_sort_order() {
        // Helper to check the order in a list of values.
        // Only converted type is checked.
        fn check_sort_order(types: Vec<ConvertedType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(None, tpe, Type::BYTE_ARRAY),
                    expected_order
                );
            }
        }

        // Unsigned comparison (physical type does not matter)
        let unsigned = vec![
            ConvertedType::UTF8,
            ConvertedType::JSON,
            ConvertedType::BSON,
            ConvertedType::ENUM,
            ConvertedType::UINT_8,
            ConvertedType::UINT_16,
            ConvertedType::UINT_32,
            ConvertedType::UINT_64,
        ];
        check_sort_order(unsigned, SortOrder::UNSIGNED);

        // Signed comparison (physical type does not matter)
        let signed = vec![
            ConvertedType::INT_8,
            ConvertedType::INT_16,
            ConvertedType::INT_32,
            ConvertedType::INT_64,
            ConvertedType::DECIMAL,
            ConvertedType::DATE,
            ConvertedType::TIME_MILLIS,
            ConvertedType::TIME_MICROS,
            ConvertedType::TIMESTAMP_MILLIS,
            ConvertedType::TIMESTAMP_MICROS,
        ];
        check_sort_order(signed, SortOrder::SIGNED);

        // Undefined comparison
        let undefined = vec![
            ConvertedType::LIST,
            ConvertedType::MAP,
            ConvertedType::MAP_KEY_VALUE,
            ConvertedType::INTERVAL,
        ];
        check_sort_order(undefined, SortOrder::UNDEFINED);

        // Check None logical type
        // This should return a sort order for byte array type.
        check_sort_order(vec![ConvertedType::NONE], SortOrder::UNSIGNED);
    }

    #[test]
    fn test_column_order_get_default_sort_order() {
        // Comparison based on physical type
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::BOOLEAN),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT32),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT64),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT96),
            SortOrder::UNDEFINED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::FLOAT),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::DOUBLE),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::FIXED_LEN_BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_column_order_sort_order() {
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED).sort_order(),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED).sort_order(),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED).sort_order(),
            SortOrder::UNDEFINED
        );
        assert_eq!(ColumnOrder::UNDEFINED.sort_order(), SortOrder::SIGNED);
    }
}
