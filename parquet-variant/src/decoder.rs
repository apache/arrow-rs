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
use chrono::{DateTime, Duration, NaiveDate};
use std::array::TryFromSliceError;

use crate::utils::{array_from_slice, first_byte_from_slice, slice_from_slice, string_from_slice};

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
    // TODO: Add types for the rest of primitives, once API is agreed upon
    Date = 11,
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
            // TODO: Add types for the rest, once API is agreed upon
            11 => Ok(VariantPrimitiveType::Date),
            15 => Ok(VariantPrimitiveType::Binary),
            16 => Ok(VariantPrimitiveType::String),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "unknown primitive type: {}",
                value
            ))),
        }
    }
}
/// Extract the primitive type from a Variant value-header byte
pub(crate) fn get_primitive_type(header: u8) -> Result<VariantPrimitiveType, ArrowError> {
    // last 6 bits contain the primitive-type, see spec
    VariantPrimitiveType::try_from(header >> 2)
}

/// To be used in `map_err` when unpacking an integer from a slice of bytes.
fn map_try_from_slice_error(e: TryFromSliceError) -> ArrowError {
    ArrowError::InvalidArgumentError(e.to_string())
}

/// Decodes an Int8 from the value section of a variant.
pub(crate) fn decode_int8(value: &[u8]) -> Result<i8, ArrowError> {
    let value = i8::from_le_bytes(array_from_slice(value, 1)?);
    Ok(value)
}

/// Decodes a Date from the value section of a variant.
pub(crate) fn decode_date(value: &[u8]) -> Result<NaiveDate, ArrowError> {
    let days_since_epoch = i32::from_le_bytes(array_from_slice(value, 1)?);
    let value = (DateTime::UNIX_EPOCH + Duration::days(days_since_epoch as i64)).date_naive();
    Ok(value)
}

/// Decodes a Binary from the value section of a variant.
pub(crate) fn decode_binary(value: &[u8]) -> Result<&[u8], ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(value, 1)?) as usize;
    let value = slice_from_slice(value, 5..5 + len)?;
    Ok(value)
}

/// Decodes a long string from the value section of a variant.
pub(crate) fn decode_long_string(value: &[u8]) -> Result<&str, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(value, 1)?) as usize;
    let string = string_from_slice(value, 5..5 + len)?;
    Ok(string)
}

/// Decodes a short string from the value section of a variant.
pub(crate) fn decode_short_string(value: &[u8]) -> Result<&str, ArrowError> {
    let len = (first_byte_from_slice(value)? >> 2) as usize;

    let string = string_from_slice(value, 1..1 + len)?;
    Ok(string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i8() -> Result<(), ArrowError> {
        let value = [
            3 << 2, // Primitive type for i8
            42,
        ];
        let result = decode_int8(&value)?;
        assert_eq!(result, 42);
        Ok(())
    }

    #[test]
    fn test_date() -> Result<(), ArrowError> {
        let value = [
            (VariantPrimitiveType::Date as u8) << 2, // Basic type
            0xe2,
            0x4e,
            0x0,
            0x0, // Data
        ];
        let result = decode_date(&value)?;
        assert_eq!(result, NaiveDate::from_ymd_opt(2025, 4, 16).unwrap());
        Ok(())
    }

    #[test]
    fn test_binary() -> Result<(), ArrowError> {
        let value = [
            (VariantPrimitiveType::Binary as u8) << 2, // Basic type
            9,
            0,
            0,
            0, // Length of binary data, 4-byte little-endian
            0x03,
            0x13,
            0x37,
            0xde,
            0xad,
            0xbe,
            0xef,
            0xca,
            0xfe, // Data
        ];
        let result = decode_binary(&value)?;
        assert_eq!(
            result,
            [0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]
        );
        Ok(())
    }

    #[test]
    fn test_short_string() -> Result<(), ArrowError> {
        let value = [
            1 | 5 << 2, // Basic type for short string | length of short string
            b'H',
            b'e',
            b'l',
            b'l',
            b'o',
            b'o',
        ];
        let result = decode_short_string(&value)?;
        assert_eq!(result, "Hello");
        Ok(())
    }

    #[test]
    fn test_string() -> Result<(), ArrowError> {
        let value = [
            16 << 2, // Basic type for short string | length of short string
            5,
            0,
            0,
            0, // Length of string
            b'H',
            b'e',
            b'l',
            b'l',
            b'o',
            b'o',
        ];
        let result = decode_long_string(&value)?;
        assert_eq!(result, "Hello");
        Ok(())
    }
}
