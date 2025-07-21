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

//! Low-level binary format parsing for variant objects

use arrow::error::ArrowError;

/// Basic variant type enumeration for the first 2 bits of header
#[derive(Debug, Clone, PartialEq)]
pub enum VariantBasicType {
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
}

/// Primitive type variants
#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveType {
    Null,
    True,
    False,
    Int8,
    Int16,
    Int32,
    Int64,
    Double,
    Decimal4,
    Decimal8,
    Decimal16,
    Date,
    TimestampNtz,
    TimestampLtz,
    Float,
    Binary,
    String,
}

/// Variant type enumeration covering all possible types
#[derive(Debug, Clone, PartialEq)]
pub enum VariantType {
    Primitive(PrimitiveType),
    ShortString(ShortStringHeader),
}

/// Short string header structure
#[derive(Debug, Clone, PartialEq)]
pub struct ShortStringHeader {
    pub length: usize,
}



/// Low-level parser for variant binary format
pub struct VariantParser;

impl VariantParser {


    /// Parse primitive type header
    pub fn parse_primitive_header(header_byte: u8) -> Result<PrimitiveType, ArrowError> {
        let primitive_type = header_byte >> 2;

        match primitive_type {
            0 => Ok(PrimitiveType::Null),
            1 => Ok(PrimitiveType::True),
            2 => Ok(PrimitiveType::False),
            3 => Ok(PrimitiveType::Int8),
            4 => Ok(PrimitiveType::Int16),
            5 => Ok(PrimitiveType::Int32),
            6 => Ok(PrimitiveType::Int64),
            7 => Ok(PrimitiveType::Double),
            8 => Ok(PrimitiveType::Decimal4),
            9 => Ok(PrimitiveType::Decimal8),
            10 => Ok(PrimitiveType::Decimal16),
            11 => Ok(PrimitiveType::Date),
            12 => Ok(PrimitiveType::TimestampNtz),
            13 => Ok(PrimitiveType::TimestampLtz),
            14 => Ok(PrimitiveType::Float),
            15 => Ok(PrimitiveType::Binary),
            16 => Ok(PrimitiveType::String),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid primitive type: {}",
                primitive_type
            ))),
        }
    }

    /// Get the basic type from header byte
    pub fn get_basic_type(header_byte: u8) -> VariantBasicType {
        match header_byte & 0x03 {
            0 => VariantBasicType::Primitive,
            1 => VariantBasicType::ShortString,
            2 => VariantBasicType::Object,
            3 => VariantBasicType::Array,
            _ => panic!("Invalid basic type: {}", header_byte & 0x03),
        }
    }



    /// Parse short string header
    pub fn parse_short_string_header(header_byte: u8) -> Result<ShortStringHeader, ArrowError> {
        let length = (header_byte >> 2) as usize;

        // Short strings can be up to 64 bytes (6-bit value: 0-63)
        if length > 63 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Short string length {} exceeds maximum of 63",
                length
            )));
        }

        Ok(ShortStringHeader { length })
    }



    /// Unpack integer from bytes
    pub fn unpack_int(bytes: &[u8], size: usize) -> Result<usize, ArrowError> {
        if bytes.len() < size {
            return Err(ArrowError::InvalidArgumentError(
                "Not enough bytes to unpack integer".to_string(),
            ));
        }

        match size {
            1 => Ok(bytes[0] as usize),
            2 => Ok(u16::from_le_bytes([bytes[0], bytes[1]]) as usize),
            3 => Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize),
            4 => Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid integer size: {}",
                size
            ))),
        }
    }

    /// Calculate the size needed to store an integer
    pub fn calculate_int_size(value: usize) -> usize {
        if value <= u8::MAX as usize {
            1
        } else if value <= u16::MAX as usize {
            2
        } else if value <= 0xFFFFFF {
            3
        } else {
            4
        }
    }

    /// Build object header byte
    pub fn build_object_header(
        is_large: bool,
        field_id_size: usize,
        field_offset_size: usize,
    ) -> u8 {
        let large_bit = if is_large { 1 } else { 0 };
        (large_bit << 6)
            | (((field_id_size - 1) as u8) << 4)
            | (((field_offset_size - 1) as u8) << 2)
            | 2
    }

    /// Write integer bytes to buffer
    pub fn write_int_bytes(buffer: &mut Vec<u8>, value: usize, size: usize) {
        match size {
            1 => buffer.push(value as u8),
            2 => buffer.extend_from_slice(&(value as u16).to_le_bytes()),
            3 => {
                let bytes = (value as u32).to_le_bytes();
                buffer.extend_from_slice(&bytes[..3]);
            }
            4 => buffer.extend_from_slice(&(value as u32).to_le_bytes()),
            _ => panic!("Invalid size: {}", size),
        }
    }

    /// Get the data length for a primitive type
    /// Returns Some(len) for fixed-length types, None for variable-length types
    pub fn get_primitive_data_length(primitive_type: &PrimitiveType) -> Option<usize> {
        match primitive_type {
            PrimitiveType::Null | PrimitiveType::True | PrimitiveType::False => Some(0),
            PrimitiveType::Int8 => Some(1),
            PrimitiveType::Int16 => Some(2),
            PrimitiveType::Int32
            | PrimitiveType::Float
            | PrimitiveType::Decimal4
            | PrimitiveType::Date => Some(4),
            PrimitiveType::Int64
            | PrimitiveType::Double
            | PrimitiveType::Decimal8
            | PrimitiveType::TimestampNtz
            | PrimitiveType::TimestampLtz => Some(8),
            PrimitiveType::Decimal16 => Some(16),
            PrimitiveType::Binary | PrimitiveType::String => None, // Variable length, need to read from data
        }
    }



    // Legacy type checking functions - kept for backwards compatibility but consider using Variant pattern matching instead
    
    /// Check if value bytes represent a primitive
    /// NOTE: Consider using `matches!(variant, Variant::Int32(_) | Variant::String(_) | ...)` instead
    pub fn is_primitive(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == VariantBasicType::Primitive
    }

    /// Check if value bytes represent a short string
    /// NOTE: Consider using `matches!(variant, Variant::ShortString(_))` instead
    pub fn is_short_string(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == VariantBasicType::ShortString
    }

    /// Check if value bytes represent an object
    /// NOTE: Consider using `variant.as_object().is_some()` instead
    pub fn is_object(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == VariantBasicType::Object
    }

    /// Check if value bytes represent an array
    /// NOTE: Consider using `variant.as_list().is_some()` instead
    pub fn is_array(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == VariantBasicType::Array
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unpack_int() {
        assert_eq!(VariantParser::unpack_int(&[42], 1).unwrap(), 42);
        assert_eq!(VariantParser::unpack_int(&[0, 1], 2).unwrap(), 256);
        assert_eq!(VariantParser::unpack_int(&[0, 0, 1, 0], 4).unwrap(), 65536);
    }

    #[test]
    fn test_calculate_int_size() {
        assert_eq!(VariantParser::calculate_int_size(255), 1);
        assert_eq!(VariantParser::calculate_int_size(256), 2);
        assert_eq!(VariantParser::calculate_int_size(65536), 3);
        assert_eq!(VariantParser::calculate_int_size(16777216), 4);
    }

    #[test]
    fn test_write_int_bytes() {
        let mut buffer = Vec::new();
        VariantParser::write_int_bytes(&mut buffer, 42, 1);
        assert_eq!(buffer, vec![42]);

        let mut buffer = Vec::new();
        VariantParser::write_int_bytes(&mut buffer, 256, 2);
        assert_eq!(buffer, vec![0, 1]);
    }

    #[test]
    fn test_parse_primitive_header() {
        // Test null (primitive type 0)
        assert_eq!(
            VariantParser::parse_primitive_header(0b00000000).unwrap(),
            PrimitiveType::Null
        );

        // Test true (primitive type 1)
        assert_eq!(
            VariantParser::parse_primitive_header(0b00000100).unwrap(),
            PrimitiveType::True
        );

        // Test false (primitive type 2)
        assert_eq!(
            VariantParser::parse_primitive_header(0b00001000).unwrap(),
            PrimitiveType::False
        );

        // Test int32 (primitive type 5)
        assert_eq!(
            VariantParser::parse_primitive_header(0b00010100).unwrap(),
            PrimitiveType::Int32
        );

        // Test double (primitive type 7)
        assert_eq!(
            VariantParser::parse_primitive_header(0b00011100).unwrap(),
            PrimitiveType::Double
        );
    }

    #[test]
    fn test_parse_short_string_header() {
        // Test 0-length short string
        assert_eq!(
            VariantParser::parse_short_string_header(0b00000001).unwrap(),
            ShortStringHeader { length: 0 }
        );

        // Test 5-length short string
        assert_eq!(
            VariantParser::parse_short_string_header(0b00010101).unwrap(),
            ShortStringHeader { length: 5 }
        );

        // Test 63-length short string (maximum for 6-bit value)
        assert_eq!(
            VariantParser::parse_short_string_header(0b11111101).unwrap(),
            ShortStringHeader { length: 63 }
        );

        // Test that all values 0-63 are valid
        for length in 0..=63 {
            let header_byte = (length << 2) | 1; // short string type
            assert!(VariantParser::parse_short_string_header(header_byte as u8).is_ok());
        }
    }



    #[test]
    fn test_basic_type_checks() {
        // Test primitive type check
        assert!(VariantParser::is_primitive(&[0b00000000])); // Null
        assert!(VariantParser::is_primitive(&[0b00000100])); // True
        assert!(!VariantParser::is_primitive(&[0b00000001])); // Not primitive

        // Test short string type check
        assert!(VariantParser::is_short_string(&[0b00000001])); // 0-length short string
        assert!(VariantParser::is_short_string(&[0b00010101])); // 5-length short string
        assert!(!VariantParser::is_short_string(&[0b00000000])); // Not short string

        // Test object type check
        assert!(VariantParser::is_object(&[0b00000010])); // Basic object
        assert!(!VariantParser::is_object(&[0b00000001])); // Not object

        // Test array type check
        assert!(VariantParser::is_array(&[0b00000011])); // Basic array
        assert!(!VariantParser::is_array(&[0b00000010])); // Not array
    }

    #[test]
    fn test_get_primitive_data_length() {
        // Test fixed-length 0-byte types
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Null),
            Some(0)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::True),
            Some(0)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::False),
            Some(0)
        );
        
        // Test fixed-length types with specific byte counts
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Int8),
            Some(1)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Int16),
            Some(2)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Int32),
            Some(4)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Int64),
            Some(8)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Double),
            Some(8)
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Decimal16),
            Some(16)
        );
        
        // Test variable-length types (should return None)
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::Binary),
            None
        );
        assert_eq!(
            VariantParser::get_primitive_data_length(&PrimitiveType::String),
            None
        );
    }


}
