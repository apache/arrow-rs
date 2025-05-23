use arrow_schema::ArrowError;
use std::{array::TryFromSliceError, str};

use crate::utils::{array_from_slice, first_byte_from_slice, invalid_utf8_err, slice_from_slice};

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

/// Decodes a long string from the value section of a variant.
pub(crate) fn decode_long_string(value: &[u8]) -> Result<&str, ArrowError> {
    let len = u32::from_le_bytes(
        slice_from_slice(value, 1..5)?
            .try_into()
            .map_err(map_try_from_slice_error)?,
    ) as usize;
    let string =
        str::from_utf8(slice_from_slice(value, 5..5 + len)?).map_err(|_| invalid_utf8_err())?;
    Ok(string)
}

/// Decodes a short string from the value section of a variant.
pub(crate) fn decode_short_string(value: &[u8]) -> Result<&str, ArrowError> {
    let len = (first_byte_from_slice(value)? >> 2) as usize;

    let string =
        str::from_utf8(slice_from_slice(value, 1..1 + len)?).map_err(|_| invalid_utf8_err())?;
    Ok(string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i8() -> Result<(), ArrowError> {
        let value = [
            0 | 3 << 2, // Primitive type for i8
            42,
        ];
        let result = decode_int8(&value)?;
        assert_eq!(result, 42);
        Ok(())
    }

    #[test]
    fn test_short_string() -> Result<(), ArrowError> {
        let value = [
            1 | 5 << 2, // Basic type for short string | length of short string
            'H' as u8,
            'e' as u8,
            'l' as u8,
            'l' as u8,
            'o' as u8,
            'o' as u8,
        ];
        let result = decode_short_string(&value)?;
        assert_eq!(result, "Hello");
        Ok(())
    }

    #[test]
    fn test_string() -> Result<(), ArrowError> {
        let value = [
            0 | 16 << 2, // Basic type for short string | length of short string
            5,
            0,
            0,
            0, // Length of string
            'H' as u8,
            'e' as u8,
            'l' as u8,
            'l' as u8,
            'o' as u8,
            'o' as u8,
        ];
        let result = decode_long_string(&value)?;
        assert_eq!(result, "Hello");
        Ok(())
    }
}
