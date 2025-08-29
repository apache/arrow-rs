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

//! Contains Rust mappings for Thrift definition. This module contains only mappings for thrift
//! enums and unions. Thrift structs are handled elsewhere.
//! Refer to [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
//! file to see raw definitions.

use std::io::Write;
use std::str::FromStr;
use std::{fmt, str};

pub use crate::compression::{BrotliLevel, GzipLevel, ZstdLevel};
use crate::parquet_thrift::{
    ElementType, FieldType, ThriftCompactInputProtocol, ThriftCompactOutputProtocol, WriteThrift,
    WriteThriftField,
};
use crate::{thrift_enum, thrift_struct, thrift_union_all_empty};

use crate::errors::{ParquetError, Result};

// ----------------------------------------------------------------------
// Types from the Thrift definition

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::Type`

thrift_enum!(
/// Types supported by Parquet.
///
/// These physical types are intended to be used in combination with the encodings to
/// control the on disk storage format.
/// For example INT16 is not included as a type since a good encoding of INT32
/// would handle this.
enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;  // deprecated, only used by legacy implementations.
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::ConvertedType`
//
// Cannot use macros because of added field `None`

/// Common types (converted types) used by frameworks when using Parquet.
///
/// This helps map between types in those frameworks to the base types in Parquet.
/// This is only metadata and not needed to read or write the data.
///
/// This struct was renamed from `LogicalType` in version 4.0.0.
/// If targeting Parquet format 2.4.0 or above, please use [LogicalType] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum ConvertedType {
    /// No type conversion.
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

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for ConvertedType {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        let val = prot.read_i32()?;
        Ok(match val {
            0 => Self::UTF8,
            1 => Self::MAP,
            2 => Self::MAP_KEY_VALUE,
            3 => Self::LIST,
            4 => Self::ENUM,
            5 => Self::DECIMAL,
            6 => Self::DATE,
            7 => Self::TIME_MILLIS,
            8 => Self::TIME_MICROS,
            9 => Self::TIMESTAMP_MILLIS,
            10 => Self::TIMESTAMP_MICROS,
            11 => Self::UINT_8,
            12 => Self::UINT_16,
            13 => Self::UINT_32,
            14 => Self::UINT_64,
            15 => Self::INT_8,
            16 => Self::INT_16,
            17 => Self::INT_32,
            18 => Self::INT_64,
            19 => Self::JSON,
            20 => Self::BSON,
            21 => Self::INTERVAL,
            _ => return Err(general_err!("Unexpected ConvertedType {}", val)),
        })
    }
}

impl WriteThrift for ConvertedType {
    const ELEMENT_TYPE: ElementType = ElementType::I32;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        // because we've added NONE, the variant values are off by 1, so correct that here
        writer.write_i32(*self as i32 - 1)
    }
}

impl WriteThriftField for ConvertedType {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::I32, field_id, last_field_id)?;
        self.write_thrift(writer)?;
        Ok(field_id)
    }
}

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::TimeUnit`

thrift_union_all_empty!(
/// Time unit for `Time` and `Timestamp` logical types.
union TimeUnit {
  1: MilliSeconds MILLIS
  2: MicroSeconds MICROS
  3: NanoSeconds NANOS
}
);

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::LogicalType`

// private structs for decoding logical type

thrift_struct!(
struct DecimalType {
  1: required i32 scale
  2: required i32 precision
}
);

thrift_struct!(
struct TimestampType {
  1: required bool is_adjusted_to_u_t_c
  2: required TimeUnit unit
}
);

// they are identical
use TimestampType as TimeType;

thrift_struct!(
struct IntType {
  1: required i8 bit_width
  2: required bool is_signed
}
);

thrift_struct!(
struct VariantType {
  // The version of the variant specification that the variant was
  // written with.
  1: optional i8 specification_version
}
);

thrift_struct!(
struct GeometryType<'a> {
  1: optional string<'a> crs;
}
);

thrift_struct!(
struct GeographyType<'a> {
  1: optional string<'a> crs;
  2: optional EdgeInterpolationAlgorithm algorithm;
}
);

/// Logical types used by version 2.4.0+ of the Parquet format.
///
/// This is an *entirely new* struct as of version
/// 4.0.0. The struct previously named `LogicalType` was renamed to
/// [`ConvertedType`]. Please see the README.md for more details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    /// A UTF8 encoded string.
    String,
    /// A map of key-value pairs.
    Map,
    /// A list of elements.
    List,
    /// A set of predefined values.
    Enum,
    /// A decimal value with a specified scale and precision.
    Decimal {
        /// The number of digits in the decimal.
        scale: i32,
        /// The location of the decimal point.
        precision: i32,
    },
    /// A date stored as days since Unix epoch.
    Date,
    /// A time stored as [`TimeUnit`] since midnight.
    Time {
        /// Whether the time is adjusted to UTC.
        is_adjusted_to_u_t_c: bool,
        /// The unit of time.
        unit: TimeUnit,
    },
    /// A timestamp stored as [`TimeUnit`] since Unix epoch.
    Timestamp {
        /// Whether the timestamp is adjusted to UTC.
        is_adjusted_to_u_t_c: bool,
        /// The unit of time.
        unit: TimeUnit,
    },
    /// An integer with a specified bit width and signedness.
    Integer {
        /// The number of bits in the integer.
        bit_width: i8,
        /// Whether the integer is signed.
        is_signed: bool,
    },
    /// An unknown logical type.
    Unknown,
    /// A JSON document.
    Json,
    /// A BSON document.
    Bson,
    /// A UUID.
    Uuid,
    /// A 16-bit floating point number.
    Float16,
    /// A Variant value.
    Variant {
        /// The version of the variant specification that the variant was written with.
        specification_version: Option<i8>,
    },
    /// A geospatial feature in the Well-Known Binary (WKB) format with linear/planar edges interpolation.
    Geometry {
        /// A custom CRS. If unset the defaults to `OGC:CRS84`.
        crs: Option<String>,
    },
    /// A geospatial feature in the WKB format with an explicit (non-linear/non-planar) edges interpolation.
    Geography {
        /// A custom CRS. If unset the defaults to `OGC:CRS84`.
        crs: Option<String>,
        /// An optional algorithm can be set to correctly interpret edges interpolation
        /// of the geometries. If unset, the algorithm defaults to `SPHERICAL``.
        algorithm: Option<EdgeInterpolationAlgorithm>,
    },
    /// For forward compatibility; used when an unknown union value is encountered.
    _Unknown {
        /// The field id encountered when parsing the unknown logical type.
        field_id: i16,
    },
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for LogicalType {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_struct_begin()?;

        let field_ident = prot.read_field_begin()?;
        if field_ident.field_type == FieldType::Stop {
            return Err(general_err!("received empty union from remote LogicalType"));
        }
        let ret = match field_ident.id {
            1 => {
                prot.skip_empty_struct()?;
                Self::String
            }
            2 => {
                prot.skip_empty_struct()?;
                Self::Map
            }
            3 => {
                prot.skip_empty_struct()?;
                Self::List
            }
            4 => {
                prot.skip_empty_struct()?;
                Self::Enum
            }
            5 => {
                let val = DecimalType::try_from(&mut *prot)?;
                Self::Decimal {
                    scale: val.scale,
                    precision: val.precision,
                }
            }
            6 => {
                prot.skip_empty_struct()?;
                Self::Date
            }
            7 => {
                let val = TimeType::try_from(&mut *prot)?;
                Self::Time {
                    is_adjusted_to_u_t_c: val.is_adjusted_to_u_t_c,
                    unit: val.unit,
                }
            }
            8 => {
                let val = TimestampType::try_from(&mut *prot)?;
                Self::Timestamp {
                    is_adjusted_to_u_t_c: val.is_adjusted_to_u_t_c,
                    unit: val.unit,
                }
            }
            10 => {
                let val = IntType::try_from(&mut *prot)?;
                Self::Integer {
                    is_signed: val.is_signed,
                    bit_width: val.bit_width,
                }
            }
            11 => {
                prot.skip_empty_struct()?;
                Self::Unknown
            }
            12 => {
                prot.skip_empty_struct()?;
                Self::Json
            }
            13 => {
                prot.skip_empty_struct()?;
                Self::Bson
            }
            14 => {
                prot.skip_empty_struct()?;
                Self::Uuid
            }
            15 => {
                prot.skip_empty_struct()?;
                Self::Float16
            }
            16 => {
                let val = VariantType::try_from(&mut *prot)?;
                Self::Variant {
                    specification_version: val.specification_version,
                }
            }
            17 => {
                let val = GeometryType::try_from(&mut *prot)?;
                Self::Geometry {
                    crs: val.crs.map(|s| s.to_owned()),
                }
            }
            18 => {
                let val = GeographyType::try_from(&mut *prot)?;
                Self::Geography {
                    crs: val.crs.map(|s| s.to_owned()),
                    algorithm: val.algorithm,
                }
            }
            _ => {
                prot.skip(field_ident.field_type)?;
                Self::_Unknown {
                    field_id: field_ident.id,
                }
            }
        };
        let field_ident = prot.read_field_begin()?;
        if field_ident.field_type != FieldType::Stop {
            return Err(general_err!(
                "Received multiple fields for union from remote LogicalType"
            ));
        }
        prot.read_struct_end()?;
        Ok(ret)
    }
}

impl WriteThrift for LogicalType {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        match self {
            Self::String => {
                writer.write_empty_struct(1, 0)?;
            }
            Self::Map => {
                writer.write_empty_struct(2, 0)?;
            }
            Self::List => {
                writer.write_empty_struct(3, 0)?;
            }
            Self::Enum => {
                writer.write_empty_struct(4, 0)?;
            }
            Self::Decimal { scale, precision } => {
                DecimalType {
                    scale: *scale,
                    precision: *precision,
                }
                .write_thrift_field(writer, 5, 0)?;
            }
            Self::Date => {
                writer.write_empty_struct(6, 0)?;
            }
            Self::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                TimeType {
                    is_adjusted_to_u_t_c: *is_adjusted_to_u_t_c,
                    unit: *unit,
                }
                .write_thrift_field(writer, 7, 0)?;
            }
            Self::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                TimestampType {
                    is_adjusted_to_u_t_c: *is_adjusted_to_u_t_c,
                    unit: *unit,
                }
                .write_thrift_field(writer, 8, 0)?;
            }
            Self::Integer {
                bit_width,
                is_signed,
            } => {
                IntType {
                    bit_width: *bit_width,
                    is_signed: *is_signed,
                }
                .write_thrift_field(writer, 10, 0)?;
            }
            Self::Unknown => {
                writer.write_empty_struct(11, 0)?;
            }
            Self::Json => {
                writer.write_empty_struct(12, 0)?;
            }
            Self::Bson => {
                writer.write_empty_struct(13, 0)?;
            }
            Self::Uuid => {
                writer.write_empty_struct(14, 0)?;
            }
            Self::Float16 => {
                writer.write_empty_struct(15, 0)?;
            }
            Self::Variant {
                specification_version,
            } => {
                VariantType {
                    specification_version: *specification_version,
                }
                .write_thrift_field(writer, 16, 0)?;
            }
            Self::Geometry { crs } => {
                GeometryType {
                    crs: crs.as_ref().map(|s| s.as_str()),
                }
                .write_thrift_field(writer, 17, 0)?;
            }
            Self::Geography { crs, algorithm } => {
                GeographyType {
                    crs: crs.as_ref().map(|s| s.as_str()),
                    algorithm: *algorithm,
                }
                .write_thrift_field(writer, 18, 0)?;
            }
            _ => return Err(nyi_err!("logical type")),
        }
        writer.write_struct_end()
    }
}

impl WriteThriftField for LogicalType {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Struct, field_id, last_field_id)?;
        self.write_thrift(writer)?;
        Ok(field_id)
    }
}

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::FieldRepetitionType`
//
// Cannot use macro since the name is changed

thrift_enum!(
/// Representation of field types in schema.
enum FieldRepetitionType {
  /// This field is required (can not be null) and each row has exactly 1 value.
  REQUIRED = 0;
  /// The field is optional (can be null) and each row has 0 or 1 values.
  OPTIONAL = 1;
  /// The field is repeated and can contain 0 or more values.
  REPEATED = 2;
}
);

/// Type alias for thrift `FieldRepetitionType`
pub type Repetition = FieldRepetitionType;

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::Encoding`

thrift_enum!(
/// Encodings supported by Parquet.
///
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
///
/// By default this crate uses [Encoding::PLAIN], [Encoding::RLE], and [Encoding::RLE_DICTIONARY].
/// These provide very good encode and decode performance, whilst yielding reasonable storage
/// efficiency and being supported by all major parquet readers.
///
/// The delta encodings are also supported and will be used if a newer [WriterVersion] is
/// configured, however, it should be noted that these sacrifice encode and decode performance for
/// improved storage efficiency. This performance regression is particularly pronounced in the case
/// of record skipping as occurs during predicate push-down. It is recommended users assess the
/// performance impact when evaluating these encodings.
///
/// [WriterVersion]: crate::file::properties::WriterVersion
enum Encoding {
  /// Default encoding.
  /// - BOOLEAN - 1 bit per value. 0 is false; 1 is true.
  /// - INT32 - 4 bytes per value.  Stored as little-endian.
  /// - INT64 - 8 bytes per value.  Stored as little-endian.
  /// - FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
  /// - DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
  /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
  /// - FIXED_LEN_BYTE_ARRAY - Just the bytes.
  PLAIN = 0;
  //  GROUP_VAR_INT = 1;
  /// **Deprecated** dictionary encoding.
  ///
  /// The values in the dictionary are encoded using PLAIN encoding.
  /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
  /// PLAIN encoding is used for dictionary page.
  PLAIN_DICTIONARY = 2;
  /// Group packed run length encoding.
  ///
  /// Usable for definition/repetition levels encoding and boolean values.
  RLE = 3;
  /// **Deprecated** Bit-packed encoding.
  ///
  /// This can only be used if the data has a known max width.
  /// Usable for definition/repetition levels encoding.
  ///
  /// There are compatibility issues with files using this encoding.
  /// The parquet standard specifies the bits to be packed starting from the
  /// most-significant bit, several implementations do not follow this bit order.
  /// Several other implementations also have issues reading this encoding
  /// because of incorrect assumptions about the length of the encoded data.
  ///
  /// The RLE/bit-packing hybrid is more cpu and memory efficient and should be used instead.
  #[deprecated(
      since = "51.0.0",
      note = "Please see documentation for compatibility issues and use the RLE/bit-packing hybrid encoding instead"
  )]
  BIT_PACKED = 4;
  /// Delta encoding for integers, either INT32 or INT64.
  ///
  /// Works best on sorted data.
  DELTA_BINARY_PACKED = 5;
  /// Encoding for byte arrays to separate the length values and the data.
  ///
  /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
  DELTA_LENGTH_BYTE_ARRAY = 6;
  /// Incremental encoding for byte arrays.
  ///
  /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
  /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
  DELTA_BYTE_ARRAY = 7;
  /// Dictionary encoding.
  ///
  /// The ids are encoded using the RLE encoding.
  RLE_DICTIONARY = 8;
  /// Encoding for fixed-width data.
  ///
  /// K byte-streams are created where K is the size in bytes of the data type.
  /// The individual bytes of a value are scattered to the corresponding stream and
  /// the streams are concatenated.
  /// This itself does not reduce the size of the data but can lead to better compression
  /// afterwards. Note that the use of this encoding with FIXED_LEN_BYTE_ARRAY(N) data may
  /// perform poorly for large values of N.
  BYTE_STREAM_SPLIT = 9;
}
);

impl FromStr for Encoding {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PLAIN" | "plain" => Ok(Encoding::PLAIN),
            "PLAIN_DICTIONARY" | "plain_dictionary" => Ok(Encoding::PLAIN_DICTIONARY),
            "RLE" | "rle" => Ok(Encoding::RLE),
            #[allow(deprecated)]
            "BIT_PACKED" | "bit_packed" => Ok(Encoding::BIT_PACKED),
            "DELTA_BINARY_PACKED" | "delta_binary_packed" => Ok(Encoding::DELTA_BINARY_PACKED),
            "DELTA_LENGTH_BYTE_ARRAY" | "delta_length_byte_array" => {
                Ok(Encoding::DELTA_LENGTH_BYTE_ARRAY)
            }
            "DELTA_BYTE_ARRAY" | "delta_byte_array" => Ok(Encoding::DELTA_BYTE_ARRAY),
            "RLE_DICTIONARY" | "rle_dictionary" => Ok(Encoding::RLE_DICTIONARY),
            "BYTE_STREAM_SPLIT" | "byte_stream_split" => Ok(Encoding::BYTE_STREAM_SPLIT),
            _ => Err(general_err!("unknown encoding: {}", s)),
        }
    }
}

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::CompressionCodec`

/// Supported block compression algorithms.
///
/// Block compression can yield non-trivial improvements to storage efficiency at the expense
/// of potentially significantly worse encode and decode performance. Many applications,
/// especially those making use of high-throughput and low-cost commodity object storage,
/// may find storage efficiency less important than decode throughput, and therefore may
/// wish to not make use of block compression.
///
/// The writers in this crate default to no block compression for this reason.
///
/// Applications that do still wish to use block compression, will find [`Compression::ZSTD`]
/// to provide a good balance of compression, performance, and ecosystem support. Alternatively,
/// [`Compression::LZ4_RAW`] provides much faster decompression speeds, at the cost of typically
/// worse compression ratios. However, it is not as widely supported by the ecosystem, with the
/// Hadoop ecosystem historically favoring the non-standard and now deprecated [`Compression::LZ4`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Compression {
    /// No compression.
    UNCOMPRESSED,
    /// [Snappy compression](https://en.wikipedia.org/wiki/Snappy_(compression))
    SNAPPY,
    /// [Gzip compression](https://www.ietf.org/rfc/rfc1952.txt)
    GZIP(GzipLevel),
    /// [LZO compression](https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Oberhumer)
    LZO,
    /// [Brotli compression](https://datatracker.ietf.org/doc/html/rfc7932)
    BROTLI(BrotliLevel),
    /// [LZ4 compression](https://lz4.org/), [(deprecated)](https://issues.apache.org/jira/browse/PARQUET-2032)
    LZ4,
    /// [ZSTD compression](https://datatracker.ietf.org/doc/html/rfc8878)
    ZSTD(ZstdLevel),
    /// [LZ4 compression](https://lz4.org/).
    LZ4_RAW,
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for Compression {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        let val = prot.read_i32()?;
        Ok(match val {
            0 => Self::UNCOMPRESSED,
            1 => Self::SNAPPY,
            2 => Self::GZIP(Default::default()),
            3 => Self::LZO,
            4 => Self::BROTLI(Default::default()),
            5 => Self::LZ4,
            6 => Self::ZSTD(Default::default()),
            7 => Self::LZ4_RAW,
            _ => return Err(general_err!("Unexpected CompressionCodec {}", val)),
        })
    }
}

// FIXME
// ugh...why did we add compression level to some variants if we don't use them????
impl WriteThrift for Compression {
    const ELEMENT_TYPE: ElementType = ElementType::I32;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        let id: i32 = match *self {
            Self::UNCOMPRESSED => 0,
            Self::SNAPPY => 1,
            Self::GZIP(_) => 2,
            Self::LZO => 3,
            Self::BROTLI(_) => 4,
            Self::LZ4 => 5,
            Self::ZSTD(_) => 6,
            Self::LZ4_RAW => 7,
        };
        writer.write_i32(id)
    }
}

impl WriteThriftField for Compression {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::I32, field_id, last_field_id)?;
        self.write_thrift(writer)?;
        Ok(field_id)
    }
}

impl Compression {
    /// Returns the codec type of this compression setting as a string, without the compression
    /// level.
    pub(crate) fn codec_to_string(self) -> String {
        format!("{self:?}").split('(').next().unwrap().to_owned()
    }
}

fn split_compression_string(str_setting: &str) -> Result<(&str, Option<u32>), ParquetError> {
    let split_setting = str_setting.split_once('(');

    match split_setting {
        Some((codec, level_str)) => {
            let level = &level_str[..level_str.len() - 1]
                .parse::<u32>()
                .map_err(|_| {
                    ParquetError::General(format!("invalid compression level: {level_str}"))
                })?;
            Ok((codec, Some(*level)))
        }
        None => Ok((str_setting, None)),
    }
}

fn check_level_is_none(level: &Option<u32>) -> Result<(), ParquetError> {
    if level.is_some() {
        return Err(ParquetError::General(
            "compression level is not supported".to_string(),
        ));
    }

    Ok(())
}

fn require_level(codec: &str, level: Option<u32>) -> Result<u32, ParquetError> {
    level.ok_or(ParquetError::General(format!(
        "{codec} requires a compression level",
    )))
}

impl FromStr for Compression {
    type Err = ParquetError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (codec, level) = split_compression_string(s)?;

        let c = match codec {
            "UNCOMPRESSED" | "uncompressed" => {
                check_level_is_none(&level)?;
                Compression::UNCOMPRESSED
            }
            "SNAPPY" | "snappy" => {
                check_level_is_none(&level)?;
                Compression::SNAPPY
            }
            "GZIP" | "gzip" => {
                let level = require_level(codec, level)?;
                Compression::GZIP(GzipLevel::try_new(level)?)
            }
            "LZO" | "lzo" => {
                check_level_is_none(&level)?;
                Compression::LZO
            }
            "BROTLI" | "brotli" => {
                let level = require_level(codec, level)?;
                Compression::BROTLI(BrotliLevel::try_new(level)?)
            }
            "LZ4" | "lz4" => {
                check_level_is_none(&level)?;
                Compression::LZ4
            }
            "ZSTD" | "zstd" => {
                let level = require_level(codec, level)?;
                Compression::ZSTD(ZstdLevel::try_new(level as i32)?)
            }
            "LZ4_RAW" | "lz4_raw" => {
                check_level_is_none(&level)?;
                Compression::LZ4_RAW
            }
            _ => {
                return Err(ParquetError::General(format!(
                    "unsupport compression {codec}"
                )));
            }
        };

        Ok(c)
    }
}

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::PageType`

thrift_enum!(
/// Available data pages for Parquet file format.
/// Note that some of the page types may not be supported.
enum PageType {
  DATA_PAGE = 0;
  INDEX_PAGE = 1;
  DICTIONARY_PAGE = 2;
  DATA_PAGE_V2 = 3;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::BoundaryOrder`

thrift_enum!(
/// Enum to annotate whether lists of min/max elements inside ColumnIndex
/// are ordered and if so, in which direction.
enum BoundaryOrder {
  UNORDERED = 0;
  ASCENDING = 1;
  DESCENDING = 2;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift enum `crate::format::EdgeInterpolationAlgorithm`

thrift_enum!(
/// Edge interpolation algorithm for Geography logical type
enum EdgeInterpolationAlgorithm {
  SPHERICAL = 0;
  VINCENTY = 1;
  THOMAS = 2;
  ANDOYER = 3;
  KARNEY = 4;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::BloomFilterAlgorithm`

thrift_union_all_empty!(
/// The algorithm used in Bloom filter.
union BloomFilterAlgorithm {
  /** Block-based Bloom filter. **/
  1: SplitBlockAlgorithm BLOCK;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::BloomFilterHash`

thrift_union_all_empty!(
/// The hash function used in Bloom filter. This function takes the hash of a column value
/// using plain encoding.
union BloomFilterHash {
  /** xxHash Strategy. **/
  1: XxHash XXHASH;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::BloomFilterCompression`

thrift_union_all_empty!(
/// The compression used in the Bloom filter.
union BloomFilterCompression {
  1: Uncompressed UNCOMPRESSED;
}
);

// ----------------------------------------------------------------------
// Mirrors thrift union `crate::format::ColumnOrder`

/// Sort order for page and column statistics.
///
/// Types are associated with sort orders and column stats are aggregated using a sort
/// order, and a sort order should be considered when comparing values with statistics
/// min/max.
///
/// See reference in
/// <https://github.com/apache/arrow/blob/main/cpp/src/parquet/types.h>
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
    // The following are not defined in the Parquet spec and should always be last.
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    UNDEFINED,
    /// An unknown but present ColumnOrder. Statistics with an unknown `ColumnOrder`
    /// will be ignored.
    UNKNOWN,
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
                LogicalType::String | LogicalType::Enum | LogicalType::Json | LogicalType::Bson => {
                    SortOrder::UNSIGNED
                }
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
                LogicalType::Float16 => SortOrder::SIGNED,
                LogicalType::Variant { .. }
                | LogicalType::Geometry { .. }
                | LogicalType::Geography { .. }
                | LogicalType::_Unknown { .. } => SortOrder::UNDEFINED,
            },
            // Fall back to converted type
            None => Self::get_converted_sort_order(converted_type, physical_type),
        }
    }

    fn get_converted_sort_order(converted_type: ConvertedType, physical_type: Type) -> SortOrder {
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
            ColumnOrder::UNKNOWN => SortOrder::UNDEFINED,
        }
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for ColumnOrder {
    type Error = ParquetError;

    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_struct_begin()?;
        let field_ident = prot.read_field_begin()?;
        if field_ident.field_type == FieldType::Stop {
            return Err(general_err!("Received empty union from remote ColumnOrder"));
        }
        let ret = match field_ident.id {
            1 => {
                // NOTE: the sort order needs to be set correctly after parsing.
                prot.skip_empty_struct()?;
                Self::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            }
            _ => {
                prot.skip(field_ident.field_type)?;
                Self::UNKNOWN
            }
        };
        let field_ident = prot.read_field_begin()?;
        if field_ident.field_type != FieldType::Stop {
            return Err(general_err!(
                "Received multiple fields for union from remote ColumnOrder"
            ));
        }
        prot.read_struct_end()?;
        Ok(ret)
    }
}

impl WriteThrift for ColumnOrder {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        match *self {
            Self::TYPE_DEFINED_ORDER(_) => {
                writer.write_field_begin(FieldType::Struct, 1, 0)?;
                writer.write_struct_end()?;
            }
            _ => return Err(general_err!("Attempt to write undefined ColumnOrder")),
        }
        // write end of struct for this union
        writer.write_struct_end()
    }
}

// ----------------------------------------------------------------------
// Display handlers

impl fmt::Display for ConvertedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for Compression {
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
// crate::format::ConvertedType <=> ConvertedType conversion

impl TryFrom<Option<crate::format::ConvertedType>> for ConvertedType {
    type Error = ParquetError;

    fn try_from(option: Option<crate::format::ConvertedType>) -> Result<Self> {
        Ok(match option {
            None => ConvertedType::NONE,
            Some(value) => match value {
                crate::format::ConvertedType::UTF8 => ConvertedType::UTF8,
                crate::format::ConvertedType::MAP => ConvertedType::MAP,
                crate::format::ConvertedType::MAP_KEY_VALUE => ConvertedType::MAP_KEY_VALUE,
                crate::format::ConvertedType::LIST => ConvertedType::LIST,
                crate::format::ConvertedType::ENUM => ConvertedType::ENUM,
                crate::format::ConvertedType::DECIMAL => ConvertedType::DECIMAL,
                crate::format::ConvertedType::DATE => ConvertedType::DATE,
                crate::format::ConvertedType::TIME_MILLIS => ConvertedType::TIME_MILLIS,
                crate::format::ConvertedType::TIME_MICROS => ConvertedType::TIME_MICROS,
                crate::format::ConvertedType::TIMESTAMP_MILLIS => ConvertedType::TIMESTAMP_MILLIS,
                crate::format::ConvertedType::TIMESTAMP_MICROS => ConvertedType::TIMESTAMP_MICROS,
                crate::format::ConvertedType::UINT_8 => ConvertedType::UINT_8,
                crate::format::ConvertedType::UINT_16 => ConvertedType::UINT_16,
                crate::format::ConvertedType::UINT_32 => ConvertedType::UINT_32,
                crate::format::ConvertedType::UINT_64 => ConvertedType::UINT_64,
                crate::format::ConvertedType::INT_8 => ConvertedType::INT_8,
                crate::format::ConvertedType::INT_16 => ConvertedType::INT_16,
                crate::format::ConvertedType::INT_32 => ConvertedType::INT_32,
                crate::format::ConvertedType::INT_64 => ConvertedType::INT_64,
                crate::format::ConvertedType::JSON => ConvertedType::JSON,
                crate::format::ConvertedType::BSON => ConvertedType::BSON,
                crate::format::ConvertedType::INTERVAL => ConvertedType::INTERVAL,
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

impl From<ConvertedType> for Option<crate::format::ConvertedType> {
    fn from(value: ConvertedType) -> Self {
        match value {
            ConvertedType::NONE => None,
            ConvertedType::UTF8 => Some(crate::format::ConvertedType::UTF8),
            ConvertedType::MAP => Some(crate::format::ConvertedType::MAP),
            ConvertedType::MAP_KEY_VALUE => Some(crate::format::ConvertedType::MAP_KEY_VALUE),
            ConvertedType::LIST => Some(crate::format::ConvertedType::LIST),
            ConvertedType::ENUM => Some(crate::format::ConvertedType::ENUM),
            ConvertedType::DECIMAL => Some(crate::format::ConvertedType::DECIMAL),
            ConvertedType::DATE => Some(crate::format::ConvertedType::DATE),
            ConvertedType::TIME_MILLIS => Some(crate::format::ConvertedType::TIME_MILLIS),
            ConvertedType::TIME_MICROS => Some(crate::format::ConvertedType::TIME_MICROS),
            ConvertedType::TIMESTAMP_MILLIS => Some(crate::format::ConvertedType::TIMESTAMP_MILLIS),
            ConvertedType::TIMESTAMP_MICROS => Some(crate::format::ConvertedType::TIMESTAMP_MICROS),
            ConvertedType::UINT_8 => Some(crate::format::ConvertedType::UINT_8),
            ConvertedType::UINT_16 => Some(crate::format::ConvertedType::UINT_16),
            ConvertedType::UINT_32 => Some(crate::format::ConvertedType::UINT_32),
            ConvertedType::UINT_64 => Some(crate::format::ConvertedType::UINT_64),
            ConvertedType::INT_8 => Some(crate::format::ConvertedType::INT_8),
            ConvertedType::INT_16 => Some(crate::format::ConvertedType::INT_16),
            ConvertedType::INT_32 => Some(crate::format::ConvertedType::INT_32),
            ConvertedType::INT_64 => Some(crate::format::ConvertedType::INT_64),
            ConvertedType::JSON => Some(crate::format::ConvertedType::JSON),
            ConvertedType::BSON => Some(crate::format::ConvertedType::BSON),
            ConvertedType::INTERVAL => Some(crate::format::ConvertedType::INTERVAL),
        }
    }
}

// ----------------------------------------------------------------------
// crate::format::LogicalType <=> LogicalType conversion

impl From<crate::format::LogicalType> for LogicalType {
    fn from(value: crate::format::LogicalType) -> Self {
        match value {
            crate::format::LogicalType::STRING(_) => LogicalType::String,
            crate::format::LogicalType::MAP(_) => LogicalType::Map,
            crate::format::LogicalType::LIST(_) => LogicalType::List,
            crate::format::LogicalType::ENUM(_) => LogicalType::Enum,
            crate::format::LogicalType::DECIMAL(t) => LogicalType::Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            crate::format::LogicalType::DATE(_) => LogicalType::Date,
            crate::format::LogicalType::TIME(t) => LogicalType::Time {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: t.unit.into(),
            },
            crate::format::LogicalType::TIMESTAMP(t) => LogicalType::Timestamp {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: t.unit.into(),
            },
            crate::format::LogicalType::INTEGER(t) => LogicalType::Integer {
                bit_width: t.bit_width,
                is_signed: t.is_signed,
            },
            crate::format::LogicalType::UNKNOWN(_) => LogicalType::Unknown,
            crate::format::LogicalType::JSON(_) => LogicalType::Json,
            crate::format::LogicalType::BSON(_) => LogicalType::Bson,
            crate::format::LogicalType::UUID(_) => LogicalType::Uuid,
            crate::format::LogicalType::FLOAT16(_) => LogicalType::Float16,
            crate::format::LogicalType::VARIANT(vt) => LogicalType::Variant {
                specification_version: vt.specification_version,
            },
            crate::format::LogicalType::GEOMETRY(gt) => LogicalType::Geometry { crs: gt.crs },
            crate::format::LogicalType::GEOGRAPHY(gt) => LogicalType::Geography {
                crs: gt.crs,
                algorithm: gt.algorithm.map(|a| a.try_into().unwrap()),
            },
        }
    }
}

impl From<LogicalType> for crate::format::LogicalType {
    fn from(value: LogicalType) -> Self {
        match value {
            LogicalType::String => crate::format::LogicalType::STRING(Default::default()),
            LogicalType::Map => crate::format::LogicalType::MAP(Default::default()),
            LogicalType::List => crate::format::LogicalType::LIST(Default::default()),
            LogicalType::Enum => crate::format::LogicalType::ENUM(Default::default()),
            LogicalType::Decimal { scale, precision } => {
                crate::format::LogicalType::DECIMAL(crate::format::DecimalType { scale, precision })
            }
            LogicalType::Date => crate::format::LogicalType::DATE(Default::default()),
            LogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => crate::format::LogicalType::TIME(crate::format::TimeType {
                is_adjusted_to_u_t_c,
                unit: unit.into(),
            }),
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => crate::format::LogicalType::TIMESTAMP(crate::format::TimestampType {
                is_adjusted_to_u_t_c,
                unit: unit.into(),
            }),
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => crate::format::LogicalType::INTEGER(crate::format::IntType {
                bit_width,
                is_signed,
            }),
            LogicalType::Unknown => crate::format::LogicalType::UNKNOWN(Default::default()),
            LogicalType::Json => crate::format::LogicalType::JSON(Default::default()),
            LogicalType::Bson => crate::format::LogicalType::BSON(Default::default()),
            LogicalType::Uuid => crate::format::LogicalType::UUID(Default::default()),
            LogicalType::Float16 => crate::format::LogicalType::FLOAT16(Default::default()),
            LogicalType::Variant {
                specification_version,
            } => crate::format::LogicalType::VARIANT(crate::format::VariantType {
                specification_version,
            }),
            LogicalType::Geometry { crs } => {
                crate::format::LogicalType::GEOMETRY(crate::format::GeometryType { crs })
            }
            LogicalType::Geography { crs, algorithm } => {
                crate::format::LogicalType::GEOGRAPHY(crate::format::GeographyType {
                    crs,
                    algorithm: algorithm.map(|a| a.into()),
                })
            }
            LogicalType::_Unknown { .. } => {
                panic!("Trying to convert unknown LogicalType to thrift");
            }
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
                    TimeUnit::MILLIS => ConvertedType::TIME_MILLIS,
                    TimeUnit::MICROS => ConvertedType::TIME_MICROS,
                    TimeUnit::NANOS => ConvertedType::NONE,
                },
                LogicalType::Timestamp { unit, .. } => match unit {
                    TimeUnit::MILLIS => ConvertedType::TIMESTAMP_MILLIS,
                    TimeUnit::MICROS => ConvertedType::TIMESTAMP_MICROS,
                    TimeUnit::NANOS => ConvertedType::NONE,
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
                LogicalType::Json => ConvertedType::JSON,
                LogicalType::Bson => ConvertedType::BSON,
                LogicalType::Uuid
                | LogicalType::Float16
                | LogicalType::Variant { .. }
                | LogicalType::Geometry { .. }
                | LogicalType::Geography { .. }
                | LogicalType::_Unknown { .. }
                | LogicalType::Unknown => ConvertedType::NONE,
            },
            None => ConvertedType::NONE,
        }
    }
}

// ----------------------------------------------------------------------
// crate::format::CompressionCodec <=> Compression conversion

impl TryFrom<crate::format::CompressionCodec> for Compression {
    type Error = ParquetError;

    fn try_from(value: crate::format::CompressionCodec) -> Result<Self> {
        Ok(match value {
            crate::format::CompressionCodec::UNCOMPRESSED => Compression::UNCOMPRESSED,
            crate::format::CompressionCodec::SNAPPY => Compression::SNAPPY,
            crate::format::CompressionCodec::GZIP => Compression::GZIP(Default::default()),
            crate::format::CompressionCodec::LZO => Compression::LZO,
            crate::format::CompressionCodec::BROTLI => Compression::BROTLI(Default::default()),
            crate::format::CompressionCodec::LZ4 => Compression::LZ4,
            crate::format::CompressionCodec::ZSTD => Compression::ZSTD(Default::default()),
            crate::format::CompressionCodec::LZ4_RAW => Compression::LZ4_RAW,
            _ => {
                return Err(general_err!(
                    "unexpected parquet compression codec: {}",
                    value.0
                ))
            }
        })
    }
}

impl From<Compression> for crate::format::CompressionCodec {
    fn from(value: Compression) -> Self {
        match value {
            Compression::UNCOMPRESSED => crate::format::CompressionCodec::UNCOMPRESSED,
            Compression::SNAPPY => crate::format::CompressionCodec::SNAPPY,
            Compression::GZIP(_) => crate::format::CompressionCodec::GZIP,
            Compression::LZO => crate::format::CompressionCodec::LZO,
            Compression::BROTLI(_) => crate::format::CompressionCodec::BROTLI,
            Compression::LZ4 => crate::format::CompressionCodec::LZ4,
            Compression::ZSTD(_) => crate::format::CompressionCodec::ZSTD,
            Compression::LZ4_RAW => crate::format::CompressionCodec::LZ4_RAW,
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
                unit: TimeUnit::MILLIS,
            }),
            "TIMESTAMP" => Ok(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS,
            }),
            "STRING" => Ok(LogicalType::String),
            "JSON" => Ok(LogicalType::Json),
            "BSON" => Ok(LogicalType::Bson),
            "UUID" => Ok(LogicalType::Uuid),
            "UNKNOWN" => Ok(LogicalType::Unknown),
            "INTERVAL" => Err(general_err!(
                "Interval parquet logical type not yet supported"
            )),
            "FLOAT16" => Ok(LogicalType::Float16),
            other => Err(general_err!("Invalid parquet logical type {}", other)),
        }
    }
}

#[cfg(test)]
#[allow(deprecated)] // allow BIT_PACKED encoding for the whole test module
mod tests {
    use super::*;
    use crate::parquet_thrift::tests::test_roundtrip;

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
            Type::try_from(crate::format::Type::BOOLEAN).unwrap(),
            Type::BOOLEAN
        );
        assert_eq!(
            Type::try_from(crate::format::Type::INT32).unwrap(),
            Type::INT32
        );
        assert_eq!(
            Type::try_from(crate::format::Type::INT64).unwrap(),
            Type::INT64
        );
        assert_eq!(
            Type::try_from(crate::format::Type::INT96).unwrap(),
            Type::INT96
        );
        assert_eq!(
            Type::try_from(crate::format::Type::FLOAT).unwrap(),
            Type::FLOAT
        );
        assert_eq!(
            Type::try_from(crate::format::Type::DOUBLE).unwrap(),
            Type::DOUBLE
        );
        assert_eq!(
            Type::try_from(crate::format::Type::BYTE_ARRAY).unwrap(),
            Type::BYTE_ARRAY
        );
        assert_eq!(
            Type::try_from(crate::format::Type::FIXED_LEN_BYTE_ARRAY).unwrap(),
            Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_type() {
        assert_eq!(crate::format::Type::BOOLEAN, Type::BOOLEAN.into());
        assert_eq!(crate::format::Type::INT32, Type::INT32.into());
        assert_eq!(crate::format::Type::INT64, Type::INT64.into());
        assert_eq!(crate::format::Type::INT96, Type::INT96.into());
        assert_eq!(crate::format::Type::FLOAT, Type::FLOAT.into());
        assert_eq!(crate::format::Type::DOUBLE, Type::DOUBLE.into());
        assert_eq!(crate::format::Type::BYTE_ARRAY, Type::BYTE_ARRAY.into());
        assert_eq!(
            crate::format::Type::FIXED_LEN_BYTE_ARRAY,
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
    fn test_converted_type_roundtrip() {
        test_roundtrip(ConvertedType::UTF8);
        test_roundtrip(ConvertedType::MAP);
        test_roundtrip(ConvertedType::MAP_KEY_VALUE);
        test_roundtrip(ConvertedType::LIST);
        test_roundtrip(ConvertedType::ENUM);
        test_roundtrip(ConvertedType::DECIMAL);
        test_roundtrip(ConvertedType::DATE);
        test_roundtrip(ConvertedType::TIME_MILLIS);
        test_roundtrip(ConvertedType::TIME_MICROS);
        test_roundtrip(ConvertedType::TIMESTAMP_MILLIS);
        test_roundtrip(ConvertedType::TIMESTAMP_MICROS);
        test_roundtrip(ConvertedType::UINT_8);
        test_roundtrip(ConvertedType::UINT_16);
        test_roundtrip(ConvertedType::UINT_32);
        test_roundtrip(ConvertedType::UINT_64);
        test_roundtrip(ConvertedType::INT_8);
        test_roundtrip(ConvertedType::INT_16);
        test_roundtrip(ConvertedType::INT_32);
        test_roundtrip(ConvertedType::INT_64);
        test_roundtrip(ConvertedType::JSON);
        test_roundtrip(ConvertedType::BSON);
        test_roundtrip(ConvertedType::INTERVAL);
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
        let parquet_conv_none: Option<crate::format::ConvertedType> = None;
        assert_eq!(
            ConvertedType::try_from(parquet_conv_none).unwrap(),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::UTF8)).unwrap(),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::MAP)).unwrap(),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::MAP_KEY_VALUE)).unwrap(),
            ConvertedType::MAP_KEY_VALUE
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::LIST)).unwrap(),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::ENUM)).unwrap(),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::DECIMAL)).unwrap(),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::DATE)).unwrap(),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::TIME_MILLIS)).unwrap(),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::TIME_MICROS)).unwrap(),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::TIMESTAMP_MILLIS)).unwrap(),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::TIMESTAMP_MICROS)).unwrap(),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::UINT_8)).unwrap(),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::UINT_16)).unwrap(),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::UINT_32)).unwrap(),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::UINT_64)).unwrap(),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::INT_8)).unwrap(),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::INT_16)).unwrap(),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::INT_32)).unwrap(),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::INT_64)).unwrap(),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::JSON)).unwrap(),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::BSON)).unwrap(),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::INTERVAL)).unwrap(),
            ConvertedType::INTERVAL
        );
        assert_eq!(
            ConvertedType::try_from(Some(crate::format::ConvertedType::DECIMAL)).unwrap(),
            ConvertedType::DECIMAL
        )
    }

    #[test]
    fn test_into_converted_type() {
        let converted_type: Option<crate::format::ConvertedType> = None;
        assert_eq!(converted_type, ConvertedType::NONE.into());
        assert_eq!(
            Some(crate::format::ConvertedType::UTF8),
            ConvertedType::UTF8.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::MAP),
            ConvertedType::MAP.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::MAP_KEY_VALUE),
            ConvertedType::MAP_KEY_VALUE.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::LIST),
            ConvertedType::LIST.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::ENUM),
            ConvertedType::ENUM.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::DECIMAL),
            ConvertedType::DECIMAL.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::DATE),
            ConvertedType::DATE.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::TIME_MILLIS),
            ConvertedType::TIME_MILLIS.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::TIME_MICROS),
            ConvertedType::TIME_MICROS.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::TIMESTAMP_MILLIS),
            ConvertedType::TIMESTAMP_MILLIS.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::TIMESTAMP_MICROS),
            ConvertedType::TIMESTAMP_MICROS.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::UINT_8),
            ConvertedType::UINT_8.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::UINT_16),
            ConvertedType::UINT_16.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::UINT_32),
            ConvertedType::UINT_32.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::UINT_64),
            ConvertedType::UINT_64.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::INT_8),
            ConvertedType::INT_8.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::INT_16),
            ConvertedType::INT_16.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::INT_32),
            ConvertedType::INT_32.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::INT_64),
            ConvertedType::INT_64.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::JSON),
            ConvertedType::JSON.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::BSON),
            ConvertedType::BSON.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::INTERVAL),
            ConvertedType::INTERVAL.into()
        );
        assert_eq!(
            Some(crate::format::ConvertedType::DECIMAL),
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
                unit: TimeUnit::MILLIS,
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Time {
                unit: TimeUnit::MICROS,
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Time {
                unit: TimeUnit::NANOS,
                is_adjusted_to_u_t_c: false,
            })),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::MILLIS,
                is_adjusted_to_u_t_c: true,
            })),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::MICROS,
                is_adjusted_to_u_t_c: false,
            })),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Timestamp {
                unit: TimeUnit::NANOS,
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
            ConvertedType::from(Some(LogicalType::Float16)),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::Unknown)),
            ConvertedType::NONE
        );
    }

    #[test]
    fn test_logical_type_roundtrip() {
        test_roundtrip(LogicalType::String);
        test_roundtrip(LogicalType::Map);
        test_roundtrip(LogicalType::List);
        test_roundtrip(LogicalType::Enum);
        test_roundtrip(LogicalType::Decimal {
            scale: 0,
            precision: 20,
        });
        test_roundtrip(LogicalType::Date);
        test_roundtrip(LogicalType::Time {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MICROS,
        });
        test_roundtrip(LogicalType::Time {
            is_adjusted_to_u_t_c: false,
            unit: TimeUnit::MILLIS,
        });
        test_roundtrip(LogicalType::Time {
            is_adjusted_to_u_t_c: false,
            unit: TimeUnit::NANOS,
        });
        test_roundtrip(LogicalType::Timestamp {
            is_adjusted_to_u_t_c: false,
            unit: TimeUnit::MICROS,
        });
        test_roundtrip(LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MILLIS,
        });
        test_roundtrip(LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::NANOS,
        });
        test_roundtrip(LogicalType::Integer {
            bit_width: 8,
            is_signed: true,
        });
        test_roundtrip(LogicalType::Integer {
            bit_width: 16,
            is_signed: false,
        });
        test_roundtrip(LogicalType::Integer {
            bit_width: 32,
            is_signed: true,
        });
        test_roundtrip(LogicalType::Integer {
            bit_width: 64,
            is_signed: false,
        });
        test_roundtrip(LogicalType::Json);
        test_roundtrip(LogicalType::Bson);
        test_roundtrip(LogicalType::Uuid);
        test_roundtrip(LogicalType::Float16);
        test_roundtrip(LogicalType::Variant {
            specification_version: Some(1),
        });
        test_roundtrip(LogicalType::Variant {
            specification_version: None,
        });
        test_roundtrip(LogicalType::Geometry {
            crs: Some("foo".to_owned()),
        });
        test_roundtrip(LogicalType::Geometry { crs: None });
        test_roundtrip(LogicalType::Geography {
            crs: Some("foo".to_owned()),
            algorithm: Some(EdgeInterpolationAlgorithm::ANDOYER),
        });
        test_roundtrip(LogicalType::Geography {
            crs: None,
            algorithm: Some(EdgeInterpolationAlgorithm::KARNEY),
        });
        test_roundtrip(LogicalType::Geography {
            crs: Some("foo".to_owned()),
            algorithm: None,
        });
        test_roundtrip(LogicalType::Geography {
            crs: None,
            algorithm: None,
        });
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
            Repetition::try_from(crate::format::FieldRepetitionType::REQUIRED).unwrap(),
            Repetition::REQUIRED
        );
        assert_eq!(
            Repetition::try_from(crate::format::FieldRepetitionType::OPTIONAL).unwrap(),
            Repetition::OPTIONAL
        );
        assert_eq!(
            Repetition::try_from(crate::format::FieldRepetitionType::REPEATED).unwrap(),
            Repetition::REPEATED
        );
    }

    #[test]
    fn test_into_repetition() {
        assert_eq!(
            crate::format::FieldRepetitionType::REQUIRED,
            Repetition::REQUIRED.into()
        );
        assert_eq!(
            crate::format::FieldRepetitionType::OPTIONAL,
            Repetition::OPTIONAL.into()
        );
        assert_eq!(
            crate::format::FieldRepetitionType::REPEATED,
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
            Encoding::try_from(crate::format::Encoding::PLAIN).unwrap(),
            Encoding::PLAIN
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::PLAIN_DICTIONARY).unwrap(),
            Encoding::PLAIN_DICTIONARY
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::RLE).unwrap(),
            Encoding::RLE
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::BIT_PACKED).unwrap(),
            Encoding::BIT_PACKED
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::DELTA_BINARY_PACKED).unwrap(),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::DELTA_LENGTH_BYTE_ARRAY).unwrap(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY
        );
        assert_eq!(
            Encoding::try_from(crate::format::Encoding::DELTA_BYTE_ARRAY).unwrap(),
            Encoding::DELTA_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_encoding() {
        assert_eq!(crate::format::Encoding::PLAIN, Encoding::PLAIN.into());
        assert_eq!(
            crate::format::Encoding::PLAIN_DICTIONARY,
            Encoding::PLAIN_DICTIONARY.into()
        );
        assert_eq!(crate::format::Encoding::RLE, Encoding::RLE.into());
        assert_eq!(
            crate::format::Encoding::BIT_PACKED,
            Encoding::BIT_PACKED.into()
        );
        assert_eq!(
            crate::format::Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_BINARY_PACKED.into()
        );
        assert_eq!(
            crate::format::Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_LENGTH_BYTE_ARRAY.into()
        );
        assert_eq!(
            crate::format::Encoding::DELTA_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY.into()
        );
    }

    #[test]
    fn test_compression_codec_to_string() {
        assert_eq!(Compression::UNCOMPRESSED.codec_to_string(), "UNCOMPRESSED");
        assert_eq!(
            Compression::ZSTD(ZstdLevel::default()).codec_to_string(),
            "ZSTD"
        );
    }

    #[test]
    fn test_display_compression() {
        assert_eq!(Compression::UNCOMPRESSED.to_string(), "UNCOMPRESSED");
        assert_eq!(Compression::SNAPPY.to_string(), "SNAPPY");
        assert_eq!(
            Compression::GZIP(Default::default()).to_string(),
            "GZIP(GzipLevel(6))"
        );
        assert_eq!(Compression::LZO.to_string(), "LZO");
        assert_eq!(
            Compression::BROTLI(Default::default()).to_string(),
            "BROTLI(BrotliLevel(1))"
        );
        assert_eq!(Compression::LZ4.to_string(), "LZ4");
        assert_eq!(
            Compression::ZSTD(Default::default()).to_string(),
            "ZSTD(ZstdLevel(1))"
        );
    }

    #[test]
    fn test_from_compression() {
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::UNCOMPRESSED).unwrap(),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::SNAPPY).unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::GZIP).unwrap(),
            Compression::GZIP(Default::default())
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::LZO).unwrap(),
            Compression::LZO
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::BROTLI).unwrap(),
            Compression::BROTLI(Default::default())
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::LZ4).unwrap(),
            Compression::LZ4
        );
        assert_eq!(
            Compression::try_from(crate::format::CompressionCodec::ZSTD).unwrap(),
            Compression::ZSTD(Default::default())
        );
    }

    #[test]
    fn test_into_compression() {
        assert_eq!(
            crate::format::CompressionCodec::UNCOMPRESSED,
            Compression::UNCOMPRESSED.into()
        );
        assert_eq!(
            crate::format::CompressionCodec::SNAPPY,
            Compression::SNAPPY.into()
        );
        assert_eq!(
            crate::format::CompressionCodec::GZIP,
            Compression::GZIP(Default::default()).into()
        );
        assert_eq!(
            crate::format::CompressionCodec::LZO,
            Compression::LZO.into()
        );
        assert_eq!(
            crate::format::CompressionCodec::BROTLI,
            Compression::BROTLI(Default::default()).into()
        );
        assert_eq!(
            crate::format::CompressionCodec::LZ4,
            Compression::LZ4.into()
        );
        assert_eq!(
            crate::format::CompressionCodec::ZSTD,
            Compression::ZSTD(Default::default()).into()
        );
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
            PageType::try_from(crate::format::PageType::DATA_PAGE).unwrap(),
            PageType::DATA_PAGE
        );
        assert_eq!(
            PageType::try_from(crate::format::PageType::INDEX_PAGE).unwrap(),
            PageType::INDEX_PAGE
        );
        assert_eq!(
            PageType::try_from(crate::format::PageType::DICTIONARY_PAGE).unwrap(),
            PageType::DICTIONARY_PAGE
        );
        assert_eq!(
            PageType::try_from(crate::format::PageType::DATA_PAGE_V2).unwrap(),
            PageType::DATA_PAGE_V2
        );
    }

    #[test]
    fn test_into_page_type() {
        assert_eq!(
            crate::format::PageType::DATA_PAGE,
            PageType::DATA_PAGE.into()
        );
        assert_eq!(
            crate::format::PageType::INDEX_PAGE,
            PageType::INDEX_PAGE.into()
        );
        assert_eq!(
            crate::format::PageType::DICTIONARY_PAGE,
            PageType::DICTIONARY_PAGE.into()
        );
        assert_eq!(
            crate::format::PageType::DATA_PAGE_V2,
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
    fn test_column_order_roundtrip() {
        // SortOrder::SIGNED is the default on read.
        test_roundtrip(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED))
    }

    #[test]
    fn test_column_order_get_logical_type_sort_order() {
        // Helper to check the order in a list of values.
        // Only logical type is checked.
        fn check_sort_order(types: Vec<LogicalType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(Some(tpe), ConvertedType::NONE, Type::BYTE_ARRAY),
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
                unit: TimeUnit::MILLIS,
            },
            LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS,
            },
            LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS,
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS,
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS,
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS,
            },
            LogicalType::Float16,
        ];
        check_sort_order(signed, SortOrder::SIGNED);

        // Undefined comparison
        let undefined = vec![LogicalType::List, LogicalType::Map];
        check_sort_order(undefined, SortOrder::UNDEFINED);
    }

    #[test]
    fn test_column_order_get_converted_type_sort_order() {
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

    #[test]
    fn test_parse_encoding() {
        let mut encoding: Encoding = "PLAIN".parse().unwrap();
        assert_eq!(encoding, Encoding::PLAIN);
        encoding = "PLAIN_DICTIONARY".parse().unwrap();
        assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
        encoding = "RLE".parse().unwrap();
        assert_eq!(encoding, Encoding::RLE);
        encoding = "BIT_PACKED".parse().unwrap();
        assert_eq!(encoding, Encoding::BIT_PACKED);
        encoding = "DELTA_BINARY_PACKED".parse().unwrap();
        assert_eq!(encoding, Encoding::DELTA_BINARY_PACKED);
        encoding = "DELTA_LENGTH_BYTE_ARRAY".parse().unwrap();
        assert_eq!(encoding, Encoding::DELTA_LENGTH_BYTE_ARRAY);
        encoding = "DELTA_BYTE_ARRAY".parse().unwrap();
        assert_eq!(encoding, Encoding::DELTA_BYTE_ARRAY);
        encoding = "RLE_DICTIONARY".parse().unwrap();
        assert_eq!(encoding, Encoding::RLE_DICTIONARY);
        encoding = "BYTE_STREAM_SPLIT".parse().unwrap();
        assert_eq!(encoding, Encoding::BYTE_STREAM_SPLIT);

        // test lowercase
        encoding = "byte_stream_split".parse().unwrap();
        assert_eq!(encoding, Encoding::BYTE_STREAM_SPLIT);

        // test unknown string
        match "plain_xxx".parse::<Encoding>() {
            Ok(e) => {
                panic!("Should not be able to parse {e:?}");
            }
            Err(e) => {
                assert_eq!(e.to_string(), "Parquet error: unknown encoding: plain_xxx");
            }
        }
    }

    #[test]
    fn test_parse_compression() {
        let mut compress: Compression = "snappy".parse().unwrap();
        assert_eq!(compress, Compression::SNAPPY);
        compress = "lzo".parse().unwrap();
        assert_eq!(compress, Compression::LZO);
        compress = "zstd(3)".parse().unwrap();
        assert_eq!(compress, Compression::ZSTD(ZstdLevel::try_new(3).unwrap()));
        compress = "LZ4_RAW".parse().unwrap();
        assert_eq!(compress, Compression::LZ4_RAW);
        compress = "uncompressed".parse().unwrap();
        assert_eq!(compress, Compression::UNCOMPRESSED);
        compress = "snappy".parse().unwrap();
        assert_eq!(compress, Compression::SNAPPY);
        compress = "gzip(9)".parse().unwrap();
        assert_eq!(compress, Compression::GZIP(GzipLevel::try_new(9).unwrap()));
        compress = "lzo".parse().unwrap();
        assert_eq!(compress, Compression::LZO);
        compress = "brotli(3)".parse().unwrap();
        assert_eq!(
            compress,
            Compression::BROTLI(BrotliLevel::try_new(3).unwrap())
        );
        compress = "lz4".parse().unwrap();
        assert_eq!(compress, Compression::LZ4);

        // test unknown compression
        let mut err = "plain_xxx".parse::<Encoding>().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: unknown encoding: plain_xxx"
        );

        // test invalid compress level
        err = "gzip(-10)".parse::<Encoding>().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: unknown encoding: gzip(-10)"
        );
    }

    #[test]
    fn test_display_boundary_order() {
        assert_eq!(BoundaryOrder::ASCENDING.to_string(), "ASCENDING");
        assert_eq!(BoundaryOrder::DESCENDING.to_string(), "DESCENDING");
        assert_eq!(BoundaryOrder::UNORDERED.to_string(), "UNORDERED");
    }

    #[test]
    fn test_from_boundary_order() {
        assert_eq!(
            BoundaryOrder::try_from(crate::format::BoundaryOrder::ASCENDING).unwrap(),
            BoundaryOrder::ASCENDING
        );
        assert_eq!(
            BoundaryOrder::try_from(crate::format::BoundaryOrder::DESCENDING).unwrap(),
            BoundaryOrder::DESCENDING
        );
        assert_eq!(
            BoundaryOrder::try_from(crate::format::BoundaryOrder::UNORDERED).unwrap(),
            BoundaryOrder::UNORDERED
        );
    }

    #[test]
    fn test_into_boundary_order() {
        assert_eq!(
            crate::format::BoundaryOrder::ASCENDING,
            BoundaryOrder::ASCENDING.into()
        );
        assert_eq!(
            crate::format::BoundaryOrder::DESCENDING,
            BoundaryOrder::DESCENDING.into()
        );
        assert_eq!(
            crate::format::BoundaryOrder::UNORDERED,
            BoundaryOrder::UNORDERED.into()
        );
    }

    #[test]
    fn test_display_edge_algo() {
        assert_eq!(
            EdgeInterpolationAlgorithm::SPHERICAL.to_string(),
            "SPHERICAL"
        );
        assert_eq!(EdgeInterpolationAlgorithm::VINCENTY.to_string(), "VINCENTY");
        assert_eq!(EdgeInterpolationAlgorithm::THOMAS.to_string(), "THOMAS");
        assert_eq!(EdgeInterpolationAlgorithm::ANDOYER.to_string(), "ANDOYER");
        assert_eq!(EdgeInterpolationAlgorithm::KARNEY.to_string(), "KARNEY");
    }

    #[test]
    fn test_from_edge_algo() {
        assert_eq!(
            EdgeInterpolationAlgorithm::try_from(
                crate::format::EdgeInterpolationAlgorithm::SPHERICAL
            )
            .unwrap(),
            EdgeInterpolationAlgorithm::SPHERICAL
        );
        assert_eq!(
            EdgeInterpolationAlgorithm::try_from(
                crate::format::EdgeInterpolationAlgorithm::VINCENTY
            )
            .unwrap(),
            EdgeInterpolationAlgorithm::VINCENTY
        );
        assert_eq!(
            EdgeInterpolationAlgorithm::try_from(crate::format::EdgeInterpolationAlgorithm::THOMAS)
                .unwrap(),
            EdgeInterpolationAlgorithm::THOMAS
        );
        assert_eq!(
            EdgeInterpolationAlgorithm::try_from(
                crate::format::EdgeInterpolationAlgorithm::ANDOYER
            )
            .unwrap(),
            EdgeInterpolationAlgorithm::ANDOYER
        );
        assert_eq!(
            EdgeInterpolationAlgorithm::try_from(crate::format::EdgeInterpolationAlgorithm::KARNEY)
                .unwrap(),
            EdgeInterpolationAlgorithm::KARNEY
        );
    }

    #[test]
    fn test_into_edge_algo() {
        assert_eq!(
            crate::format::EdgeInterpolationAlgorithm::SPHERICAL,
            EdgeInterpolationAlgorithm::SPHERICAL.into()
        );
        assert_eq!(
            crate::format::EdgeInterpolationAlgorithm::VINCENTY,
            EdgeInterpolationAlgorithm::VINCENTY.into()
        );
        assert_eq!(
            crate::format::EdgeInterpolationAlgorithm::THOMAS,
            EdgeInterpolationAlgorithm::THOMAS.into()
        );
        assert_eq!(
            crate::format::EdgeInterpolationAlgorithm::ANDOYER,
            EdgeInterpolationAlgorithm::ANDOYER.into()
        );
        assert_eq!(
            crate::format::EdgeInterpolationAlgorithm::KARNEY,
            EdgeInterpolationAlgorithm::KARNEY.into()
        );
    }
}
