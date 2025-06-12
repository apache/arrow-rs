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
use arrow_schema::ArrowError;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use std::array::TryFromSliceError;

use crate::utils::{array_from_slice, slice_from_slice, string_from_slice};

#[derive(Debug, Clone, Copy)]
pub enum VariantBasicType {
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
}

#[derive(Debug, Clone, Copy)]
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
    TimestampNTZMicros = 13,
    Float = 14,
    Binary = 15,
    String = 16,
}

/// Extracts the basic type from a header byte
pub(crate) fn get_basic_type(header: u8) -> Result<VariantBasicType, ArrowError> {
    // See https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#value-encoding
    let basic_type = header & 0x03; // Basic type is encoded in the first 2 bits
    let basic_type = match basic_type {
        0 => VariantBasicType::Primitive,
        1 => VariantBasicType::ShortString,
        2 => VariantBasicType::Object,
        3 => VariantBasicType::Array,
        _ => {
            //NOTE:  A 2-bit value has a max of 4 different values (0-3), hence this is unreachable as we
            // masked `basic_type` with 0x03 above.
            unreachable!();
        }
    };
    Ok(basic_type)
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
            13 => Ok(VariantPrimitiveType::TimestampNTZMicros),
            14 => Ok(VariantPrimitiveType::Float),
            15 => Ok(VariantPrimitiveType::Binary),
            16 => Ok(VariantPrimitiveType::String),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "unknown primitive type: {}",
                value
            ))),
        }
    }
}
/// Extract the primitive type from a Variant value-metadata byte
pub(crate) fn get_primitive_type(metadata: u8) -> Result<VariantPrimitiveType, ArrowError> {
    // last 6 bits contain the primitive-type, see spec
    VariantPrimitiveType::try_from(metadata >> 2)
}

/// To be used in `map_err` when unpacking an integer from a slice of bytes.
fn map_try_from_slice_error(e: TryFromSliceError) -> ArrowError {
    ArrowError::InvalidArgumentError(e.to_string())
}

/// Decodes an Int8 from the value section of a variant.
pub(crate) fn decode_int8(data: &[u8]) -> Result<i8, ArrowError> {
    let value = i8::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
}

/// Decodes an Int16 from the value section of a variant.
pub(crate) fn decode_int16(data: &[u8]) -> Result<i16, ArrowError> {
    let value = i16::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
}

/// Decodes an Int32 from the value section of a variant.
pub(crate) fn decode_int32(data: &[u8]) -> Result<i32, ArrowError> {
    let value = i32::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
}

/// Decodes an Int64 from the value section of a variant.
pub(crate) fn decode_int64(data: &[u8]) -> Result<i64, ArrowError> {
    let value = i64::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
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
    let value = f32::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
}

/// Decodes a Double from the value section of a variant.
pub(crate) fn decode_double(data: &[u8]) -> Result<f64, ArrowError> {
    let value = f64::from_le_bytes(array_from_slice(data, 0)?);
    Ok(value)
}

/// Decodes a Date from the value section of a variant.
pub(crate) fn decode_date(data: &[u8]) -> Result<NaiveDate, ArrowError> {
    let days_since_epoch = i32::from_le_bytes(array_from_slice(data, 0)?);
    let value = (DateTime::UNIX_EPOCH + Duration::days(days_since_epoch as i64)).date_naive();
    Ok(value)
}

/// Decodes a TimestampMicros from the value section of a variant.
pub(crate) fn decode_timestamp_micros(data: &[u8]) -> Result<DateTime<Utc>, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    if let Some(value) = DateTime::from_timestamp_micros(micros_since_epoch) {
        Ok(value)
    } else {
        Err(ArrowError::CastError(format!(
            "Could not cast `{micros_since_epoch}` microseconds into a DateTime<Utc>"
        )))
    }
}

/// Decodes a TimestampNTZMicros from the value section of a variant.
pub(crate) fn decode_timestampntz_micros(data: &[u8]) -> Result<NaiveDateTime, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    if let Some(value) = DateTime::from_timestamp_micros(micros_since_epoch) {
        Ok(value.naive_utc())
    } else {
        Err(ArrowError::CastError(format!(
            "Could not cast `{micros_since_epoch}` microseconds into a NaiveDateTime"
        )))
    }
}

/// Decodes a Binary from the value section of a variant.
pub(crate) fn decode_binary(data: &[u8]) -> Result<&[u8], ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    let value = slice_from_slice(data, 4..4 + len)?;
    Ok(value)
}

/// Decodes a long string from the value section of a variant.
pub(crate) fn decode_long_string(data: &[u8]) -> Result<&str, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    let string = string_from_slice(data, 4..4 + len)?;
    Ok(string)
}

/// Decodes a short string from the value section of a variant.
pub(crate) fn decode_short_string(metadata: u8, data: &[u8]) -> Result<&str, ArrowError> {
    let len = (metadata >> 2) as usize;

    let string = string_from_slice(data, 0..len)?;
    Ok(string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i8() -> Result<(), ArrowError> {
        let data = [0x2a];
        let result = decode_int8(&data)?;
        assert_eq!(result, 42);
        Ok(())
    }

    #[test]
    fn test_i16() -> Result<(), ArrowError> {
        let data = [0xd2, 0x04];
        let result = decode_int16(&data)?;
        assert_eq!(result, 1234);
        Ok(())
    }

    #[test]
    fn test_i32() -> Result<(), ArrowError> {
        let data = [0x40, 0xe2, 0x01, 0x00];
        let result = decode_int32(&data)?;
        assert_eq!(result, 123456);
        Ok(())
    }

    #[test]
    fn test_i64() -> Result<(), ArrowError> {
        let data = [0x15, 0x81, 0xe9, 0x7d, 0xf4, 0x10, 0x22, 0x11];
        let result = decode_int64(&data)?;
        assert_eq!(result, 1234567890123456789);
        Ok(())
    }

    #[test]
    fn test_decimal4() -> Result<(), ArrowError> {
        let data = [
            0x02, // Scale
            0xd2, 0x04, 0x00, 0x00, // Integer
        ];
        let result = decode_decimal4(&data)?;
        assert_eq!(result, (1234, 2));
        Ok(())
    }

    #[test]
    fn test_decimal8() -> Result<(), ArrowError> {
        let data = [
            0x02, // Scale
            0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00, // Integer
        ];
        let result = decode_decimal8(&data)?;
        assert_eq!(result, (1234567890, 2));
        Ok(())
    }

    #[test]
    fn test_decimal16() -> Result<(), ArrowError> {
        let data = [
            0x02, // Scale
            0xd2, 0xb6, 0x23, 0xc0, 0xf4, 0x10, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // Integer
        ];
        let result = decode_decimal16(&data)?;
        assert_eq!(result, (1234567891234567890, 2));
        Ok(())
    }

    #[test]
    fn test_float() -> Result<(), ArrowError> {
        let data = [0x06, 0x2c, 0x93, 0x4e];
        let result = decode_float(&data)?;
        assert_eq!(result, 1234567890.1234);
        Ok(())
    }

    #[test]
    fn test_double() -> Result<(), ArrowError> {
        let data = [0xc9, 0xe5, 0x87, 0xb4, 0x80, 0x65, 0xd2, 0x41];
        let result = decode_double(&data)?;
        assert_eq!(result, 1234567890.1234);
        Ok(())
    }

    #[test]
    fn test_date() -> Result<(), ArrowError> {
        let data = [0xe2, 0x4e, 0x0, 0x0];
        let result = decode_date(&data)?;
        assert_eq!(result, NaiveDate::from_ymd_opt(2025, 4, 16).unwrap());
        Ok(())
    }

    #[test]
    fn test_timestamp_micros() -> Result<(), ArrowError> {
        let data = [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00];
        let result = decode_timestamp_micros(&data)?;
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
                .and_utc()
        );
        Ok(())
    }

    #[test]
    fn test_timestampntz_micros() -> Result<(), ArrowError> {
        let data = [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00];
        let result = decode_timestampntz_micros(&data)?;
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
        );
        Ok(())
    }

    #[test]
    fn test_binary() -> Result<(), ArrowError> {
        let data = [
            9, 0, 0, 0, // Length of binary data, 4-byte little-endian
            0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe,
        ];
        let result = decode_binary(&data)?;
        assert_eq!(
            result,
            [0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]
        );
        Ok(())
    }

    #[test]
    fn test_short_string() -> Result<(), ArrowError> {
        let data = [b'H', b'e', b'l', b'l', b'o', b'o'];
        let result = decode_short_string(1 | 5 << 2, &data)?;
        assert_eq!(result, "Hello");
        Ok(())
    }

    #[test]
    fn test_string() -> Result<(), ArrowError> {
        let data = [
            5, 0, 0, 0, // Length of string, 4-byte little-endian
            b'H', b'e', b'l', b'l', b'o', b'o',
        ];
        let result = decode_long_string(&data)?;
        assert_eq!(result, "Hello");
        Ok(())
    }
}
