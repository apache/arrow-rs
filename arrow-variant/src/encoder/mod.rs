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

//! Core encoding primitives for the Variant binary format

use arrow_schema::ArrowError;
use std::io::Write;

/// Maximum value that can be stored in a single byte (2^8 - 1)
pub const MAX_1BYTE_VALUE: usize = 255;

/// Maximum value that can be stored in two bytes (2^16 - 1)
pub const MAX_2BYTE_VALUE: usize = 65535;

/// Maximum value that can be stored in three bytes (2^24 - 1)
pub const MAX_3BYTE_VALUE: usize = 16777215;

/// Maximum length of a short string in bytes (used in short string encoding)
pub const MAX_SHORT_STRING_LENGTH: usize = 64;

/// Maximum scale allowed for decimal values
pub const MAX_DECIMAL_SCALE: u8 = 38;

/// Calculate the minimum number of bytes required to represent a value.
///
/// Returns a value between 1 and 4, representing the minimum number of
/// bytes needed to store the given value.
///
/// # Arguments
///
/// * `value` - The value to determine the size for
///
/// # Returns
///
/// The number of bytes (1, 2, 3, or 4) needed to represent the value
pub(crate) fn min_bytes_needed(value: usize) -> usize {
    if value <= MAX_1BYTE_VALUE {
        1
    } else if value <= MAX_2BYTE_VALUE {
        2
    } else if value <= MAX_3BYTE_VALUE {
        3
    } else {
        4
    }
}

/// Variant basic types as defined in the Arrow Variant specification
/// 
/// See the official specification: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
///
/// Basic Type	ID	Description
/// Primitive	0	One of the primitive types
/// Short string	1	A string with a length less than 64 bytes
/// Object	2	A collection of (string-key, variant-value) pairs
/// Array	3	An ordered sequence of variant values
#[derive(Debug, Clone, Copy)]
pub enum VariantBasicType {
    /// Primitive type (0)
    Primitive = 0,
    /// Short string (1)
    ShortString = 1,
    /// Object (2)
    Object = 2,
    /// Array (3)
    Array = 3,
}

/// Variant primitive types as defined in the Arrow Variant specification
/// 
/// See the official specification: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
///
/// Equivalence Class	Variant Physical Type	Type ID	Equivalent Parquet Type	Binary format
/// NullType	null	0	UNKNOWN	none
/// Boolean	boolean (True)	1	BOOLEAN	none
/// Boolean	boolean (False)	2	BOOLEAN	none
/// Exact Numeric	int8	3	INT(8, signed)	1 byte
/// Exact Numeric	int16	4	INT(16, signed)	2 byte little-endian
/// Exact Numeric	int32	5	INT(32, signed)	4 byte little-endian
/// Exact Numeric	int64	6	INT(64, signed)	8 byte little-endian
/// Double	double	7	DOUBLE	IEEE little-endian
/// Exact Numeric	decimal4	8	DECIMAL(precision, scale)	1 byte scale in range [0, 38], followed by little-endian unscaled value
/// Exact Numeric	decimal8	9	DECIMAL(precision, scale)	1 byte scale in range [0, 38], followed by little-endian unscaled value
/// Exact Numeric	decimal16	10	DECIMAL(precision, scale)	1 byte scale in range [0, 38], followed by little-endian unscaled value
/// Date	date	11	DATE	4 byte little-endian
/// Timestamp	timestamp	12	TIMESTAMP(isAdjustedToUTC=true, MICROS)	8-byte little-endian
/// TimestampNTZ	timestamp without time zone	13	TIMESTAMP(isAdjustedToUTC=false, MICROS)	8-byte little-endian
/// Float	float	14	FLOAT	IEEE little-endian
/// Binary	binary	15	BINARY	4 byte little-endian size, followed by bytes
/// String	string	16	STRING	4 byte little-endian size, followed by UTF-8 encoded bytes
/// TimeNTZ	time without time zone	17	TIME(isAdjustedToUTC=false, MICROS)	8-byte little-endian
/// Timestamp	timestamp with time zone	18	TIMESTAMP(isAdjustedToUTC=true, NANOS)	8-byte little-endian
/// TimestampNTZ	timestamp without time zone	19	TIMESTAMP(isAdjustedToUTC=false, NANOS)	8-byte little-endian
/// UUID	uuid	20	UUID	16-byte big-endian
#[derive(Debug, Clone, Copy)]
pub enum VariantPrimitiveType {
    /// Null type (0)
    Null = 0,
    /// Boolean true (1)
    BooleanTrue = 1,
    /// Boolean false (2)
    BooleanFalse = 2,
    /// 8-bit signed integer (3)
    Int8 = 3,
    /// 16-bit signed integer (4)
    Int16 = 4,
    /// 32-bit signed integer (5)
    Int32 = 5,
    /// 64-bit signed integer (6)
    Int64 = 6,
    /// 64-bit floating point (7)
    Double = 7,
    /// 32-bit decimal (8)
    Decimal4 = 8,
    /// 64-bit decimal (9)
    Decimal8 = 9,
    /// 128-bit decimal (10)
    Decimal16 = 10,
    /// Date (11)
    Date = 11,
    /// Timestamp with timezone (12)
    Timestamp = 12,
    /// Timestamp without timezone (13)
    TimestampNTZ = 13,
    /// 32-bit floating point (14)
    Float = 14,
    /// Binary data (15)
    Binary = 15,
    /// UTF-8 string (16)
    String = 16,
    /// Time without timezone (17)
    TimeNTZ = 17,
    /// Timestamp with timezone (nanos) (18)
    TimestampNanos = 18,
    /// Timestamp without timezone (nanos) (19)
    TimestampNTZNanos = 19,
    /// UUID (20)
    Uuid = 20,
}

/// Trait for encoding primitive types in variant binary format
pub trait Encoder {
    /// Get the type ID for the header
    fn type_id(&self) -> u8;

    /// Encode a simple value into variant binary format
    /// 
    /// # Arguments
    /// 
    /// * `value` - The byte slice containing the raw value data
    /// * `output` - The output buffer to write the encoded value
    fn encode_simple(&self, value: &[u8], output: &mut Vec<u8>) {
        // Write the header byte for the type
        output.push(primitive_header(self.type_id()));
        
        // Write the value bytes if any
        if !value.is_empty() {
            output.extend_from_slice(value);
        }
    }
    
    /// Encode a value that needs a prefix and suffix (for decimal types)
    /// 
    /// This is a more efficient version that avoids intermediate allocations
    /// 
    /// # Arguments
    /// 
    /// * `prefix` - A prefix to add before the value (e.g., scale for decimal)
    /// * `value` - The byte slice containing the raw value data
    /// * `output` - The output buffer to write the encoded value
    fn encode_with_prefix(&self, prefix: &[u8], value: &[u8], output: &mut Vec<u8>) {
        // Write the header
        output.push(primitive_header(self.type_id()));
        
        // Write prefix + value directly to output (no temporary buffer)
        output.extend_from_slice(prefix);
        output.extend_from_slice(value);
    }
    
    /// Encode a length-prefixed value (for string and binary types)
    /// 
    /// # Arguments
    /// 
    /// * `len` - The length to encode as a prefix
    /// * `value` - The byte slice containing the raw value data
    /// * `output` - The output buffer to write the encoded value
    fn encode_length_prefixed(&self, len: u32, value: &[u8], output: &mut Vec<u8>) {
        // Write the header
        output.push(primitive_header(self.type_id()));
        
        // Write the length as 4-byte little-endian
        output.extend_from_slice(&len.to_le_bytes());
        
        // Write the value bytes
        output.extend_from_slice(value);
    }
}

impl Encoder for VariantPrimitiveType {
    #[inline]
    fn type_id(&self) -> u8 {
        *self as u8
    }
}

/// Creates a header byte for a primitive type value
///
/// The header byte contains:
/// - Basic type (2 bits) in the lower bits
/// - Type ID (6 bits) in the upper bits
fn primitive_header(type_id: u8) -> u8 {
    (type_id << 2) | VariantBasicType::Primitive as u8
}

/// Creates a header byte for a short string value
///
/// The header byte contains:
/// - Basic type (2 bits) in the lower bits
/// - String length (6 bits) in the upper bits
fn short_str_header(size: u8) -> u8 {
    (size << 2) | VariantBasicType::ShortString as u8
}

/// Creates a header byte for an object value
///
/// The header byte contains:
/// - Basic type (2 bits) in the lower bits
/// - is_large (1 bit) at position 6
/// - field_id_size_minus_one (2 bits) at positions 4-5
/// - field_offset_size_minus_one (2 bits) at positions 2-3
pub(crate) fn object_header(is_large: bool, id_size: u8, offset_size: u8) -> u8 {
    ((is_large as u8) << 6)
        | ((id_size - 1) << 4)
        | ((offset_size - 1) << 2)
        | VariantBasicType::Object as u8
}

/// Creates a header byte for an array value
///
/// The header byte contains:
/// - Basic type (2 bits) in the lower bits
/// - is_large (1 bit) at position 4
/// - field_offset_size_minus_one (2 bits) at positions 2-3
pub(crate) fn array_header(is_large: bool, offset_size: u8) -> u8 {
    ((is_large as u8) << 4) | ((offset_size - 1) << 2) | VariantBasicType::Array as u8
}

/// Encodes a null value
pub(crate) fn encode_null(output: &mut Vec<u8>) {
    VariantPrimitiveType::Null.encode_simple(&[], output);
}

/// Encodes a boolean value
pub(crate) fn encode_boolean(value: bool, output: &mut Vec<u8>) {
    let type_id = if value {
        VariantPrimitiveType::BooleanTrue
    } else {
        VariantPrimitiveType::BooleanFalse
    };
    type_id.encode_simple(&[], output);
}

/// Encodes an integer value, choosing the smallest sufficient type
pub(crate) fn encode_integer(value: i64, output: &mut Vec<u8>) {
    if value >= i8::MIN.into() && value <= i8::MAX.into() {
        // Int8
        VariantPrimitiveType::Int8.encode_simple(&[value as u8], output);
    } else if value >= i16::MIN.into() && value <= i16::MAX.into() {
        // Int16
        VariantPrimitiveType::Int16.encode_simple(&(value as i16).to_le_bytes(), output);
    } else if value >= i32::MIN.into() && value <= i32::MAX.into() {
        // Int32
        VariantPrimitiveType::Int32.encode_simple(&(value as i32).to_le_bytes(), output);
    } else {
        // Int64
        VariantPrimitiveType::Int64.encode_simple(&value.to_le_bytes(), output);
    }
}

/// Encodes a float value
pub(crate) fn encode_float(value: f64, output: &mut Vec<u8>) {
    VariantPrimitiveType::Double.encode_simple(&value.to_le_bytes(), output);
}

/// Encodes a string value
pub(crate) fn encode_string(value: &str, output: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    let len = bytes.len();

    if len < MAX_SHORT_STRING_LENGTH {
        // Short string format - encode length in header
        let header = short_str_header(len as u8);
        output.push(header);
        output.extend_from_slice(bytes);
    } else {
        // Long string format (using primitive string type with length prefix)
        // Directly encode to output without intermediate buffer
        VariantPrimitiveType::String.encode_length_prefixed(len as u32, bytes, output);
    }
}

/// Encodes a binary value
pub(crate) fn encode_binary(value: &[u8], output: &mut Vec<u8>) {
    // Use primitive + binary type with length prefix
    // Directly encode to output without intermediate buffer
    VariantPrimitiveType::Binary.encode_length_prefixed(value.len() as u32, value, output);
}

/// Encodes a date value (days since epoch)
pub(crate) fn encode_date(value: i32, output: &mut Vec<u8>) {
    VariantPrimitiveType::Date.encode_simple(&value.to_le_bytes(), output);
}

/// General function for encoding timestamp-like values with a specified type
pub(crate) fn encode_timestamp_with_type(value: i64, type_id: VariantPrimitiveType, output: &mut Vec<u8>) {
    type_id.encode_simple(&value.to_le_bytes(), output);
}

/// Encodes a timestamp value (milliseconds since epoch)
pub(crate) fn encode_timestamp(value: i64, output: &mut Vec<u8>) {
    encode_timestamp_with_type(value, VariantPrimitiveType::Timestamp, output);
}

/// Encodes a timestamp without timezone value (milliseconds since epoch)
pub(crate) fn encode_timestamp_ntz(value: i64, output: &mut Vec<u8>) {
    encode_timestamp_with_type(value, VariantPrimitiveType::TimestampNTZ, output);
}

/// Encodes a time without timezone value (milliseconds)
pub(crate) fn encode_time_ntz(value: i64, output: &mut Vec<u8>) {
    encode_timestamp_with_type(value, VariantPrimitiveType::TimeNTZ, output);
}

/// Encodes a timestamp with nanosecond precision
pub(crate) fn encode_timestamp_nanos(value: i64, output: &mut Vec<u8>) {
    encode_timestamp_with_type(value, VariantPrimitiveType::TimestampNanos, output);
}

/// Encodes a timestamp without timezone with nanosecond precision
pub(crate) fn encode_timestamp_ntz_nanos(value: i64, output: &mut Vec<u8>) {
    encode_timestamp_with_type(value, VariantPrimitiveType::TimestampNTZNanos, output);
}

/// Encodes a UUID value
pub(crate) fn encode_uuid(value: &[u8; 16], output: &mut Vec<u8>) {
    VariantPrimitiveType::Uuid.encode_simple(value, output);
}

/// Generic decimal encoding function 
fn encode_decimal_generic<T: AsRef<[u8]>>(
    scale: u8, 
    unscaled_value: T, 
    type_id: VariantPrimitiveType,
    output: &mut Vec<u8>
) {
    if scale > MAX_DECIMAL_SCALE {
        panic!("Decimal scale must be in range [0, {}], got {}", MAX_DECIMAL_SCALE, scale);
    }

    type_id.encode_with_prefix(&[scale], unscaled_value.as_ref(), output);
}

/// Encodes a decimal value with 32-bit precision (decimal4)
///
/// According to the Variant Binary Format specification, decimal values are encoded as:
/// 1. A 1-byte scale value in range [0, 38]
/// 2. Followed by the little-endian unscaled value
///
/// # Arguments
///
/// * `scale` - The scale of the decimal value (number of decimal places)
/// * `unscaled_value` - The unscaled integer value
/// * `output` - The destination to write to
pub(crate) fn encode_decimal4(scale: u8, unscaled_value: i32, output: &mut Vec<u8>) {
    encode_decimal_generic(scale, &unscaled_value.to_le_bytes(), VariantPrimitiveType::Decimal4, output);
}

/// Encodes a decimal value with 64-bit precision (decimal8)
///
/// According to the Variant Binary Format specification, decimal values are encoded as:
/// 1. A 1-byte scale value in range [0, 38]
/// 2. Followed by the little-endian unscaled value
///
/// # Arguments
///
/// * `scale` - The scale of the decimal value (number of decimal places)
/// * `unscaled_value` - The unscaled integer value
/// * `output` - The destination to write to
pub(crate) fn encode_decimal8(scale: u8, unscaled_value: i64, output: &mut Vec<u8>) {
    encode_decimal_generic(scale, &unscaled_value.to_le_bytes(), VariantPrimitiveType::Decimal8, output);
}

/// Encodes a decimal value with 128-bit precision (decimal16)
///
/// According to the Variant Binary Format specification, decimal values are encoded as:
/// 1. A 1-byte scale value in range [0, 38]
/// 2. Followed by the little-endian unscaled value
///
/// # Arguments
///
/// * `scale` - The scale of the decimal value (number of decimal places)
/// * `unscaled_value` - The unscaled integer value
/// * `output` - The destination to write to
pub(crate) fn encode_decimal16(scale: u8, unscaled_value: i128, output: &mut Vec<u8>) {
    encode_decimal_generic(scale, &unscaled_value.to_le_bytes(), VariantPrimitiveType::Decimal16, output);
}

/// Writes an integer value using the specified number of bytes (1-4).
///
/// This is a helper function to write integers with variable byte length,
/// used for offsets, field IDs, and other values in the variant format.
///
/// # Arguments
///
/// * `value` - The integer value to write
/// * `num_bytes` - The number of bytes to use (1, 2, 3, or 4)
/// * `output` - The destination to write to
///
/// # Returns
///
/// An arrow error if writing fails
pub(crate) fn write_int_with_size(
    value: u32,
    num_bytes: usize,
    output: &mut impl Write,
) -> Result<(), ArrowError> {
    match num_bytes {
        1 => output.write_all(&[value as u8])?,
        2 => output.write_all(&(value as u16).to_le_bytes())?,
        3 => {
            output.write_all(&[value as u8])?;
            output.write_all(&[(value >> 8) as u8])?;
            output.write_all(&[(value >> 16) as u8])?;
        }
        4 => output.write_all(&value.to_le_bytes())?,
        _ => {
            return Err(ArrowError::VariantError(format!(
                "Invalid byte size: {}",
                num_bytes
            )))
        }
    }
    Ok(())
}

/// Encodes a pre-encoded array to the Variant binary format
///
/// This function takes an array of pre-encoded values and writes a properly formatted
/// array according to the Arrow Variant encoding specification.
///
/// # Arguments
///
/// * `values` - A slice of byte slices containing pre-encoded variant values
/// * `output` - The destination to write the encoded array
pub(crate) fn encode_array_from_pre_encoded(
    values: &[&[u8]],
    output: &mut impl Write,
) -> Result<(), ArrowError> {
    let len = values.len();

    // Determine if we need large size encoding
    let is_large = len > MAX_1BYTE_VALUE;

    // Calculate total value size to determine offset_size
    let mut data_size = 0;
    for value in values {
        data_size += value.len();
    }

    // Determine minimum offset size
    let offset_size = min_bytes_needed(data_size);

    // Write array header with correct flags
    let header = array_header(is_large, offset_size as u8);
    output.write_all(&[header])?;

    // Write length as 1 or 4 bytes
    if is_large {
        output.write_all(&(len as u32).to_le_bytes())?;
    } else {
        output.write_all(&[len as u8])?;
    }

    // Calculate and write offsets
    let mut offsets = Vec::with_capacity(len + 1);
    let mut current_offset = 0u32;

    offsets.push(current_offset);
    for value in values {
        current_offset += value.len() as u32;
        offsets.push(current_offset);
    }

    // Write offsets using the helper function
    for offset in &offsets {
        write_int_with_size(*offset, offset_size, output)?;
    }

    // Write values
    for value in values {
        output.write_all(value)?;
    }

    Ok(())
}

/// Encodes a pre-encoded object to the Variant binary format
///
/// This function takes a collection of field IDs and pre-encoded values and writes a properly
/// formatted object according to the Arrow Variant encoding specification.
///
/// # Arguments
///
/// * `field_ids` - A slice of field IDs corresponding to keys in the dictionary
/// * `field_values` - A slice of byte slices containing pre-encoded variant values
/// * `output` - The destination to write the encoded object
pub(crate) fn encode_object_from_pre_encoded(
    field_ids: &[usize],
    field_values: &[&[u8]],
    output: &mut impl Write,
) -> Result<(), ArrowError> {
    let len = field_ids.len();

    // Determine if we need large size encoding
    let is_large = len > MAX_1BYTE_VALUE;

    // Calculate total value size to determine offset_size
    let mut data_size = 0;
    for value in field_values {
        data_size += value.len();
    }

    // Determine minimum sizes needed
    let id_size = if field_ids.is_empty() {
        1
    } else {
        let max_id = field_ids.iter().max().unwrap_or(&0);
        min_bytes_needed(*max_id)
    };

    let offset_size = min_bytes_needed(data_size);

    // Write object header with correct flags
    let header = object_header(is_large, id_size as u8, offset_size as u8);
    output.write_all(&[header])?;

    // Write length as 1 or 4 bytes
    if is_large {
        output.write_all(&(len as u32).to_le_bytes())?;
    } else {
        output.write_all(&[len as u8])?;
    }

    // Write field IDs using the helper function
    for id in field_ids {
        write_int_with_size(*id as u32, id_size, output)?;
    }

    // Calculate and write offsets
    let mut offsets = Vec::with_capacity(len + 1);
    let mut current_offset = 0u32;

    offsets.push(current_offset);
    for value in field_values {
        current_offset += value.len() as u32;
        offsets.push(current_offset);
    }

    // Write offsets using the helper function
    for offset in &offsets {
        write_int_with_size(*offset, offset_size, output)?;
    }

    // Write values
    for value in field_values {
        output.write_all(value)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_integers() {
        // Test Int8
        let mut output = Vec::new();
        encode_integer(42, &mut output);
        assert_eq!(
            output,
            vec![primitive_header(VariantPrimitiveType::Int8 as u8), 42]
        );

        // Test Int16
        output.clear();
        encode_integer(1000, &mut output);
        assert_eq!(
            output,
            vec![primitive_header(VariantPrimitiveType::Int16 as u8), 232, 3]
        );

        // Test Int32
        output.clear();
        encode_integer(100000, &mut output);
        let mut expected = vec![primitive_header(VariantPrimitiveType::Int32 as u8)];
        expected.extend_from_slice(&(100000i32).to_le_bytes());
        assert_eq!(output, expected);

        // Test Int64
        output.clear();
        encode_integer(3000000000, &mut output);
        let mut expected = vec![primitive_header(VariantPrimitiveType::Int64 as u8)];
        expected.extend_from_slice(&(3000000000i64).to_le_bytes());
        assert_eq!(output, expected);
    }

    #[test]
    fn test_encode_float() {
        let mut output = Vec::new();
        encode_float(3.14159, &mut output);
        let mut expected = vec![primitive_header(VariantPrimitiveType::Double as u8)];
        expected.extend_from_slice(&(3.14159f64).to_le_bytes());
        assert_eq!(output, expected);
    }

    #[test]
    fn test_encode_string() {
        let mut output = Vec::new();

        // Test short string
        let short_str = "Hello";
        encode_string(short_str, &mut output);

        // Check header byte
        assert_eq!(output[0], short_str_header(short_str.len() as u8));

        // Check string content
        assert_eq!(&output[1..], short_str.as_bytes());

        // Test longer string
        output.clear();
        let long_str = "This is a longer string that definitely won't fit in the small format because it needs to be at least 64 bytes long to test the long string format";
        encode_string(long_str, &mut output);

        // Check header byte
        assert_eq!(
            output[0],
            primitive_header(VariantPrimitiveType::String as u8)
        );

        // Check length bytes
        assert_eq!(&output[1..5], &(long_str.len() as u32).to_le_bytes());

        // Check string content
        assert_eq!(&output[5..], long_str.as_bytes());
    }

    #[test]
    fn test_encode_null() {
        let mut output = Vec::new();
        encode_null(&mut output);
        assert_eq!(
            output,
            vec![primitive_header(VariantPrimitiveType::Null as u8)]
        );
    }

    #[test]
    fn test_encode_boolean() {
        // Test true
        let mut output = Vec::new();
        encode_boolean(true, &mut output);
        assert_eq!(
            output,
            vec![primitive_header(VariantPrimitiveType::BooleanTrue as u8)]
        );

        // Test false
        output.clear();
        encode_boolean(false, &mut output);
        assert_eq!(
            output,
            vec![primitive_header(VariantPrimitiveType::BooleanFalse as u8)]
        );
    }

    #[test]
    fn test_encode_decimal() {
        // Test Decimal4
        let mut output = Vec::new();
        encode_decimal4(2, 12345, &mut output);

        // Verify header
        assert_eq!(
            output[0],
            primitive_header(VariantPrimitiveType::Decimal4 as u8)
        );
        // Verify scale
        assert_eq!(output[1], 2);
        // Verify unscaled value
        let unscaled_bytes = &output[2..6];
        let unscaled_value = i32::from_le_bytes([
            unscaled_bytes[0],
            unscaled_bytes[1],
            unscaled_bytes[2],
            unscaled_bytes[3],
        ]);
        assert_eq!(unscaled_value, 12345);

        // Test Decimal8
        output.clear();
        encode_decimal8(6, 9876543210, &mut output);

        // Verify header
        assert_eq!(
            output[0],
            primitive_header(VariantPrimitiveType::Decimal8 as u8)
        );
        // Verify scale
        assert_eq!(output[1], 6);
        // Verify unscaled value
        let unscaled_bytes = &output[2..10];
        let unscaled_value = i64::from_le_bytes([
            unscaled_bytes[0],
            unscaled_bytes[1],
            unscaled_bytes[2],
            unscaled_bytes[3],
            unscaled_bytes[4],
            unscaled_bytes[5],
            unscaled_bytes[6],
            unscaled_bytes[7],
        ]);
        assert_eq!(unscaled_value, 9876543210);

        // Test Decimal16
        output.clear();
        let large_value = 1234567890123456789012345678901234_i128;
        encode_decimal16(10, large_value, &mut output);

        // Verify header
        assert_eq!(
            output[0],
            primitive_header(VariantPrimitiveType::Decimal16 as u8)
        );
        // Verify scale
        assert_eq!(output[1], 10);
        // Verify unscaled value
        let unscaled_bytes = &output[2..18];
        let unscaled_value = i128::from_le_bytes([
            unscaled_bytes[0],
            unscaled_bytes[1],
            unscaled_bytes[2],
            unscaled_bytes[3],
            unscaled_bytes[4],
            unscaled_bytes[5],
            unscaled_bytes[6],
            unscaled_bytes[7],
            unscaled_bytes[8],
            unscaled_bytes[9],
            unscaled_bytes[10],
            unscaled_bytes[11],
            unscaled_bytes[12],
            unscaled_bytes[13],
            unscaled_bytes[14],
            unscaled_bytes[15],
        ]);
        assert_eq!(unscaled_value, large_value);
    }
}
