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
use crate::ShortString;
use crate::utils::{
    array_from_slice, overflow_error, slice_from_slice_at_offset, string_from_slice,
};

use arrow_schema::ArrowError;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

/// The basic type of a [`Variant`] value, encoded in the first two bits of the
/// header byte.
///
/// See the [Variant Encoding specification] for details
///
/// [`Variant`]: crate::Variant
/// [Variant Encoding specification]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VariantBasicType {
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
}

/// The type of [`VariantBasicType::Primitive`], for a primitive [`Variant`]
/// value.
///
/// See the [Variant Encoding specification] for details
///
/// [`Variant`]: crate::Variant
/// [Variant Encoding specification]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VariantPrimitiveType {
    Null = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Decimal4 = 8,
    Decimal8 = 9,
    Decimal16 = 10,
    Date = 11,
    TimestampMicros = 12,
    TimestampNtzMicros = 13,
    Float = 14,
    Binary = 15,
    String = 16,
    Time = 17,
    TimestampNanos = 18,
    TimestampNtzNanos = 19,
    Uuid = 20,
}

/// Extracts the basic type from a header byte
pub(crate) fn get_basic_type(header: u8) -> VariantBasicType {
    // See https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#value-encoding
    let basic_type = header & 0x03; // Basic type is encoded in the first 2 bits
    match basic_type {
        0 => VariantBasicType::Primitive,
        1 => VariantBasicType::ShortString,
        2 => VariantBasicType::Object,
        3 => VariantBasicType::Array,
        _ => {
            //NOTE:  A 2-bit value has a max of 4 different values (0-3), hence this is unreachable as we
            // masked `basic_type` with 0x03 above.
            unreachable!();
        }
    }
}

impl TryFrom<u8> for VariantPrimitiveType {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(VariantPrimitiveType::Null),
            1 => Ok(VariantPrimitiveType::BooleanTrue),
            2 => Ok(VariantPrimitiveType::BooleanFalse),
            3 => Ok(VariantPrimitiveType::Int8),
            4 => Ok(VariantPrimitiveType::Int16),
            5 => Ok(VariantPrimitiveType::Int32),
            6 => Ok(VariantPrimitiveType::Int64),
            7 => Ok(VariantPrimitiveType::Double),
            8 => Ok(VariantPrimitiveType::Decimal4),
            9 => Ok(VariantPrimitiveType::Decimal8),
            10 => Ok(VariantPrimitiveType::Decimal16),
            11 => Ok(VariantPrimitiveType::Date),
            12 => Ok(VariantPrimitiveType::TimestampMicros),
            13 => Ok(VariantPrimitiveType::TimestampNtzMicros),
            14 => Ok(VariantPrimitiveType::Float),
            15 => Ok(VariantPrimitiveType::Binary),
            16 => Ok(VariantPrimitiveType::String),
            17 => Ok(VariantPrimitiveType::Time),
            18 => Ok(VariantPrimitiveType::TimestampNanos),
            19 => Ok(VariantPrimitiveType::TimestampNtzNanos),
            20 => Ok(VariantPrimitiveType::Uuid),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "unknown primitive type: {value}",
            ))),
        }
    }
}

/// Used to unpack offset array entries such as metadata dictionary offsets or object/array value
/// offsets. Also used to unpack object field ids. These are always derived from a two-bit
/// `XXX_size_minus_one` field in the corresponding header byte.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OffsetSizeBytes {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
}

impl OffsetSizeBytes {
    /// Build from the `offset_size_minus_one` bits (see spec).
    pub(crate) fn try_new(offset_size_minus_one: u8) -> Result<Self, ArrowError> {
        use OffsetSizeBytes::*;
        let result = match offset_size_minus_one {
            0 => One,
            1 => Two,
            2 => Three,
            3 => Four,
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "offset_size_minus_one must be 0–3".to_string(),
                ));
            }
        };
        Ok(result)
    }

    /// Return one unsigned little-endian value from `bytes`.
    ///
    /// * `bytes` – the byte buffer to index
    /// * `index` – 0-based index into the buffer
    ///
    /// Each value is `self as u32` bytes wide (1, 2, 3 or 4), zero-extended to 32 bits as needed.
    pub(crate) fn unpack_u32(&self, bytes: &[u8], index: usize) -> Result<u32, ArrowError> {
        self.unpack_u32_at_offset(bytes, 0, index)
    }

    /// Return one unsigned little-endian value from `bytes`.
    ///
    /// * `bytes` – the byte buffer to index
    /// * `byte_offset` – number of bytes to skip **before** reading the first
    ///   value (e.g. `1` to move past a header byte).
    /// * `offset_index` – 0-based index **after** the skipped bytes
    ///   (`0` is the first value, `1` the next, …).
    ///
    /// Each value is `self as u32` bytes wide (1, 2, 3 or 4), zero-extended to 32 bits as needed.
    pub(crate) fn unpack_u32_at_offset(
        &self,
        bytes: &[u8],
        byte_offset: usize,  // how many bytes to skip
        offset_index: usize, // which offset in an array of offsets
    ) -> Result<u32, ArrowError> {
        use OffsetSizeBytes::*;

        // Index into the byte array:
        // byte_offset + (*self as usize) * offset_index
        let offset = offset_index
            .checked_mul(*self as usize)
            .and_then(|n| n.checked_add(byte_offset))
            .ok_or_else(|| overflow_error("unpacking offset array value"))?;
        let value = match self {
            One => u8::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Two => u16::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Three => {
                // Let's grab the three byte le-chunk first
                let b3_chunks: [u8; 3] = array_from_slice(bytes, offset)?;
                // Let's pad it and construct a padded u32 from it.
                let mut buf = [0u8; 4];
                buf[..3].copy_from_slice(&b3_chunks);
                u32::from_le_bytes(buf)
            }
            Four => u32::from_le_bytes(array_from_slice(bytes, offset)?),
        };
        Ok(value)
    }
}

/// Converts a byte buffer to offset values based on the specific offset size
pub(crate) fn map_bytes_to_offsets(
    buffer: &[u8],
    offset_size: OffsetSizeBytes,
) -> impl Iterator<Item = usize> + use<'_> {
    buffer
        .chunks_exact(offset_size as usize)
        .map(move |chunk| match offset_size {
            OffsetSizeBytes::One => chunk[0] as usize,
            OffsetSizeBytes::Two => u16::from_le_bytes([chunk[0], chunk[1]]) as usize,
            OffsetSizeBytes::Three => {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], 0]) as usize
            }
            OffsetSizeBytes::Four => {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize
            }
        })
}

/// Extract the primitive type from a Variant value-metadata byte
pub(crate) fn get_primitive_type(metadata: u8) -> Result<VariantPrimitiveType, ArrowError> {
    // last 6 bits contain the primitive-type, see spec
    VariantPrimitiveType::try_from(metadata >> 2)
}

/// Decodes an Int8 from the value section of a variant.
pub(crate) fn decode_int8(data: &[u8]) -> Result<i8, ArrowError> {
    Ok(i8::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes an Int16 from the value section of a variant.
pub(crate) fn decode_int16(data: &[u8]) -> Result<i16, ArrowError> {
    Ok(i16::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes an Int32 from the value section of a variant.
pub(crate) fn decode_int32(data: &[u8]) -> Result<i32, ArrowError> {
    Ok(i32::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes an Int64 from the value section of a variant.
pub(crate) fn decode_int64(data: &[u8]) -> Result<i64, ArrowError> {
    Ok(i64::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes a Decimal4 from the value section of a variant.
pub(crate) fn decode_decimal4(data: &[u8]) -> Result<(i32, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i32::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

/// Decodes a Decimal8 from the value section of a variant.
pub(crate) fn decode_decimal8(data: &[u8]) -> Result<(i64, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i64::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

/// Decodes a Decimal16 from the value section of a variant.
pub(crate) fn decode_decimal16(data: &[u8]) -> Result<(i128, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i128::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

/// Decodes a Float from the value section of a variant.
pub(crate) fn decode_float(data: &[u8]) -> Result<f32, ArrowError> {
    Ok(f32::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes a Double from the value section of a variant.
pub(crate) fn decode_double(data: &[u8]) -> Result<f64, ArrowError> {
    Ok(f64::from_le_bytes(array_from_slice(data, 0)?))
}

/// Decodes a Date from the value section of a variant.
pub(crate) fn decode_date(data: &[u8]) -> Result<NaiveDate, ArrowError> {
    let days_since_epoch = i32::from_le_bytes(array_from_slice(data, 0)?);
    let value = DateTime::UNIX_EPOCH + Duration::days(i64::from(days_since_epoch));
    Ok(value.date_naive())
}

/// Decodes a TimestampMicros from the value section of a variant.
pub(crate) fn decode_timestamp_micros(data: &[u8]) -> Result<DateTime<Utc>, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch).ok_or_else(|| {
        ArrowError::CastError(format!(
            "Could not cast `{micros_since_epoch}` microseconds into a DateTime<Utc>"
        ))
    })
}

/// Decodes a TimestampNtzMicros from the value section of a variant.
pub(crate) fn decode_timestampntz_micros(data: &[u8]) -> Result<NaiveDateTime, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch)
        .ok_or_else(|| {
            ArrowError::CastError(format!(
                "Could not cast `{micros_since_epoch}` microseconds into a NaiveDateTime"
            ))
        })
        .map(|v| v.naive_utc())
}

pub(crate) fn decode_time_ntz(data: &[u8]) -> Result<NaiveTime, ArrowError> {
    let micros_since_epoch = u64::from_le_bytes(array_from_slice(data, 0)?);

    let case_error = ArrowError::CastError(format!(
        "Could not cast {micros_since_epoch} microseconds into a NaiveTime"
    ));

    if micros_since_epoch >= 86_400_000_000 {
        return Err(case_error);
    }

    let nanos_since_midnight = micros_since_epoch * 1_000;
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nanos_since_midnight / 1_000_000_000) as u32,
        (nanos_since_midnight % 1_000_000_000) as u32,
    )
    .ok_or(case_error)
}

/// Decodes a TimestampNanos from the value section of a variant.
pub(crate) fn decode_timestamp_nanos(data: &[u8]) -> Result<DateTime<Utc>, ArrowError> {
    let nanos_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);

    // DateTime::from_timestamp_nanos would never fail
    Ok(DateTime::from_timestamp_nanos(nanos_since_epoch))
}

/// Decodes a TimestampNtzNanos from the value section of a variant.
pub(crate) fn decode_timestampntz_nanos(data: &[u8]) -> Result<NaiveDateTime, ArrowError> {
    decode_timestamp_nanos(data).map(|v| v.naive_utc())
}

/// Decodes a UUID from the value section of a variant.
pub(crate) fn decode_uuid(data: &[u8]) -> Result<Uuid, ArrowError> {
    Uuid::from_slice(&data[0..16])
        .map_err(|_| ArrowError::CastError(format!("Cant decode uuid from {:?}", &data[0..16])))
}

/// Decodes a Binary from the value section of a variant.
pub(crate) fn decode_binary(data: &[u8]) -> Result<&[u8], ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    slice_from_slice_at_offset(data, 4, 0..len)
}

/// Decodes a long string from the value section of a variant.
pub(crate) fn decode_long_string(data: &[u8]) -> Result<&str, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    string_from_slice(data, 4, 0..len)
}

/// Decodes a short string from the value section of a variant.
pub(crate) fn decode_short_string(
    metadata: u8,
    data: &[u8],
) -> Result<ShortString<'_>, ArrowError> {
    let len = (metadata >> 2) as usize;
    let string = string_from_slice(data, 0, 0..len)?;
    ShortString::try_new(string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paste::paste;

    macro_rules! test_decoder_bounds {
        ($test_name:ident, $data:expr, $decode_fn:ident, $expected:expr) => {
            paste! {
                #[test]
                fn [<$test_name _exact_length>]() {
                    let result = $decode_fn(&$data).unwrap();
                    assert_eq!(result, $expected);
                }

                #[test]
                fn [<$test_name _truncated_length>]() {
                    // Remove the last byte of data so that there is not enough to decode
                    let truncated_data = &$data[.. $data.len() - 1];
                    let result = $decode_fn(truncated_data);
                    assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
                }
            }
        };
    }

    mod integer {
        use super::*;

        test_decoder_bounds!(test_i8, [0x2a], decode_int8, 42);
        test_decoder_bounds!(test_i16, [0xd2, 0x04], decode_int16, 1234);
        test_decoder_bounds!(test_i32, [0x40, 0xe2, 0x01, 0x00], decode_int32, 123456);
        test_decoder_bounds!(
            test_i64,
            [0x15, 0x81, 0xe9, 0x7d, 0xf4, 0x10, 0x22, 0x11],
            decode_int64,
            1234567890123456789
        );
    }

    mod decimal {
        use super::*;

        test_decoder_bounds!(
            test_decimal4,
            [
                0x02, // Scale
                0xd2, 0x04, 0x00, 0x00, // Unscaled Value
            ],
            decode_decimal4,
            (1234, 2)
        );

        test_decoder_bounds!(
            test_decimal8,
            [
                0x02, // Scale
                0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00, // Unscaled Value
            ],
            decode_decimal8,
            (1234567890, 2)
        );

        test_decoder_bounds!(
            test_decimal16,
            [
                0x02, // Scale
                0xd2, 0xb6, 0x23, 0xc0, 0xf4, 0x10, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, // Unscaled Value
            ],
            decode_decimal16,
            (1234567891234567890, 2)
        );
    }

    mod float {
        use super::*;

        test_decoder_bounds!(
            test_float,
            [0x06, 0x2c, 0x93, 0x4e],
            decode_float,
            1234567890.1234
        );

        test_decoder_bounds!(
            test_double,
            [0xc9, 0xe5, 0x87, 0xb4, 0x80, 0x65, 0xd2, 0x41],
            decode_double,
            1234567890.1234
        );
    }

    mod datetime {
        use super::*;

        test_decoder_bounds!(
            test_date,
            [0xe2, 0x4e, 0x0, 0x0],
            decode_date,
            NaiveDate::from_ymd_opt(2025, 4, 16).unwrap()
        );

        test_decoder_bounds!(
            test_timestamp_micros,
            [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
            decode_timestamp_micros,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestampntz_micros,
            [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
            decode_timestampntz_micros,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
        );

        test_decoder_bounds!(
            test_timestamp_nanos,
            [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
            decode_timestamp_nanos,
            NaiveDate::from_ymd_opt(2025, 8, 14)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestamp_nanos_before_epoch,
            [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
            decode_timestamp_nanos,
            NaiveDate::from_ymd_opt(1957, 11, 7)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestampntz_nanos,
            [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
            decode_timestampntz_nanos,
            NaiveDate::from_ymd_opt(2025, 8, 14)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
        );

        test_decoder_bounds!(
            test_timestampntz_nanos_before_epoch,
            [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
            decode_timestampntz_nanos,
            NaiveDate::from_ymd_opt(1957, 11, 7)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
        );
    }

    #[test]
    fn test_uuid() {
        let data = [
            0xf2, 0x4f, 0x9b, 0x64, 0x81, 0xfa, 0x49, 0xd1, 0xb7, 0x4e, 0x8c, 0x09, 0xa6, 0xe3,
            0x1c, 0x56,
        ];
        let result = decode_uuid(&data).unwrap();
        assert_eq!(
            Uuid::parse_str("f24f9b64-81fa-49d1-b74e-8c09a6e31c56").unwrap(),
            result
        );
    }

    mod time {
        use super::*;

        test_decoder_bounds!(
            test_timentz,
            [0x53, 0x1f, 0x8e, 0xdf, 0x2, 0, 0, 0],
            decode_time_ntz,
            NaiveTime::from_num_seconds_from_midnight_opt(12340, 567_891_000).unwrap()
        );

        #[test]
        fn test_decode_time_ntz_invalid() {
            let invalid_second = u64::MAX;
            let data = invalid_second.to_le_bytes();
            let result = decode_time_ntz(&data);
            assert!(matches!(result, Err(ArrowError::CastError(_))));
        }
    }

    #[test]
    fn test_binary_exact_length() {
        let data = [
            0x09, 0, 0, 0, // Length of binary data, 4-byte little-endian
            0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe,
        ];
        let result = decode_binary(&data).unwrap();
        assert_eq!(
            result,
            [0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]
        );
    }

    #[test]
    fn test_binary_truncated_length() {
        let data = [
            0x09, 0, 0, 0, // Length of binary data, 4-byte little-endian
            0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca,
        ];
        let result = decode_binary(&data);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn test_short_string_exact_length() {
        let data = [b'H', b'e', b'l', b'l', b'o', b'o'];
        let result = decode_short_string(1 | 5 << 2, &data).unwrap();
        assert_eq!(result.0, "Hello");
    }

    #[test]
    fn test_short_string_truncated_length() {
        let data = [b'H', b'e', b'l'];
        let result = decode_short_string(1 | 5 << 2, &data);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn test_string_exact_length() {
        let data = [
            0x05, 0, 0, 0, // Length of string, 4-byte little-endian
            b'H', b'e', b'l', b'l', b'o', b'o',
        ];
        let result = decode_long_string(&data).unwrap();
        assert_eq!(result, "Hello");
    }

    #[test]
    fn test_string_truncated_length() {
        let data = [
            0x05, 0, 0, 0, // Length of string, 4-byte little-endian
            b'H', b'e', b'l',
        ];
        let result = decode_long_string(&data);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn test_offset() {
        assert_eq!(OffsetSizeBytes::try_new(0).unwrap(), OffsetSizeBytes::One);
        assert_eq!(OffsetSizeBytes::try_new(1).unwrap(), OffsetSizeBytes::Two);
        assert_eq!(OffsetSizeBytes::try_new(2).unwrap(), OffsetSizeBytes::Three);
        assert_eq!(OffsetSizeBytes::try_new(3).unwrap(), OffsetSizeBytes::Four);

        // everything outside 0-3 must error
        assert!(OffsetSizeBytes::try_new(4).is_err());
        assert!(OffsetSizeBytes::try_new(255).is_err());
    }

    #[test]
    fn unpack_u32_all_widths() {
        // One-byte offsets
        let buf_one = [0x01u8, 0xAB, 0xCD];
        assert_eq!(OffsetSizeBytes::One.unpack_u32(&buf_one, 0).unwrap(), 0x01);
        assert_eq!(OffsetSizeBytes::One.unpack_u32(&buf_one, 2).unwrap(), 0xCD);

        // Two-byte offsets (little-endian 0x1234, 0x5678)
        let buf_two = [0x34, 0x12, 0x78, 0x56];
        assert_eq!(
            OffsetSizeBytes::Two.unpack_u32(&buf_two, 0).unwrap(),
            0x1234
        );
        assert_eq!(
            OffsetSizeBytes::Two.unpack_u32(&buf_two, 1).unwrap(),
            0x5678
        );

        // Three-byte offsets (0x030201 and 0x0000FF)
        let buf_three = [0x01, 0x02, 0x03, 0xFF, 0x00, 0x00];
        assert_eq!(
            OffsetSizeBytes::Three.unpack_u32(&buf_three, 0).unwrap(),
            0x030201
        );
        assert_eq!(
            OffsetSizeBytes::Three.unpack_u32(&buf_three, 1).unwrap(),
            0x0000FF
        );

        // Four-byte offsets (0x12345678, 0x90ABCDEF)
        let buf_four = [0x78, 0x56, 0x34, 0x12, 0xEF, 0xCD, 0xAB, 0x90];
        assert_eq!(
            OffsetSizeBytes::Four.unpack_u32(&buf_four, 0).unwrap(),
            0x1234_5678
        );
        assert_eq!(
            OffsetSizeBytes::Four.unpack_u32(&buf_four, 1).unwrap(),
            0x90AB_CDEF
        );
    }

    #[test]
    fn unpack_u32_out_of_bounds() {
        let tiny = [0x00u8]; // deliberately too short
        assert!(OffsetSizeBytes::Two.unpack_u32(&tiny, 0).is_err());
        assert!(OffsetSizeBytes::Three.unpack_u32(&tiny, 0).is_err());
    }

    #[test]
    fn unpack_simple() {
        let buf = [
            0x41, // header
            0x02, 0x00, // dictionary_size = 2
            0x00, 0x00, // offset[0] = 0
            0x05, 0x00, // offset[1] = 5
            0x09, 0x00, // offset[2] = 9
        ];

        let width = OffsetSizeBytes::Two;

        // dictionary_size starts immediately after the header byte
        let dict_size = width.unpack_u32_at_offset(&buf, 1, 0).unwrap();
        assert_eq!(dict_size, 2);

        // offset array immediately follows the dictionary size
        let first = width.unpack_u32_at_offset(&buf, 1, 1).unwrap();
        assert_eq!(first, 0);

        let second = width.unpack_u32_at_offset(&buf, 1, 2).unwrap();
        assert_eq!(second, 5);

        let third = width.unpack_u32_at_offset(&buf, 1, 3).unwrap();
        assert_eq!(third, 9);

        let err = width.unpack_u32_at_offset(&buf, 1, 4);
        assert!(err.is_err())
    }
}
