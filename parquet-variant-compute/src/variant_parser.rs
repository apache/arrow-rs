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

/// Variant type enumeration covering all possible types
#[derive(Debug, Clone, PartialEq)]
pub enum VariantType {
    Primitive(PrimitiveType),
    ShortString(ShortStringHeader),
    Object(ObjectHeader),
    Array(ArrayHeader),
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

/// Short string header structure
#[derive(Debug, Clone, PartialEq)]
pub struct ShortStringHeader {
    pub length: usize,
}

/// Object header structure for variant objects
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectHeader {
    pub num_elements_size: usize,
    pub field_id_size: usize,
    pub field_offset_size: usize,
    pub is_large: bool,
}

/// Array header structure for variant objects
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayHeader {
    pub num_elements_size: usize,
    pub element_offset_size: usize,
    pub is_large: bool,
}

/// Object byte offsets structure
#[derive(Debug, Clone)]
pub struct ObjectOffsets {
    pub field_ids_start: usize,
    pub field_offsets_start: usize,
    pub values_start: usize,
}

/// Array byte offsets structure
#[derive(Debug, Clone)]
pub struct ArrayOffsets {
    pub element_offsets_start: usize,
    pub elements_start: usize,
}

/// Low-level parser for variant binary format
pub struct VariantParser;

impl VariantParser {
    /// General dispatch function to parse any variant header
    pub fn parse_variant_header(header_byte: u8) -> Result<VariantType, ArrowError> {
        let basic_type = header_byte & 0x03;
        
        match basic_type {
            0 => Ok(VariantType::Primitive(Self::parse_primitive_header(header_byte)?)),
            1 => Ok(VariantType::ShortString(Self::parse_short_string_header(header_byte)?)),
            2 => Ok(VariantType::Object(Self::parse_object_header(header_byte)?)),
            3 => Ok(VariantType::Array(Self::parse_array_header(header_byte)?)),
            _ => Err(ArrowError::InvalidArgumentError(
                format!("Invalid basic type: {}", basic_type)
            )),
        }
    }
    
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
            _ => Err(ArrowError::InvalidArgumentError(
                format!("Invalid primitive type: {}", primitive_type)
            )),
        }
    }
    
    /// Parse short string header
    pub fn parse_short_string_header(header_byte: u8) -> Result<ShortStringHeader, ArrowError> {
        let length = (header_byte >> 2) as usize;
        
        if length > 13 {
            return Err(ArrowError::InvalidArgumentError(
                format!("Short string length {} exceeds maximum of 13", length)
            ));
        }
        
        Ok(ShortStringHeader { length })
    }
    
    /// Parse object header from header byte
    pub fn parse_object_header(header_byte: u8) -> Result<ObjectHeader, ArrowError> {
        let value_header = header_byte >> 2;
        let field_offset_size_minus_one = value_header & 0x03;
        let field_id_size_minus_one = (value_header >> 2) & 0x03;
        let is_large = (value_header & 0x10) != 0;
        
        let num_elements_size = if is_large { 4 } else { 1 };
        let field_id_size = (field_id_size_minus_one + 1) as usize;
        let field_offset_size = (field_offset_size_minus_one + 1) as usize;
        
        Ok(ObjectHeader {
            num_elements_size,
            field_id_size,
            field_offset_size,
            is_large,
        })
    }
    
    /// Parse array header from header byte
    pub fn parse_array_header(header_byte: u8) -> Result<ArrayHeader, ArrowError> {
        let value_header = header_byte >> 2;
        let element_offset_size_minus_one = value_header & 0x03;
        let is_large = (value_header & 0x10) != 0;
        
        let num_elements_size = if is_large { 4 } else { 1 };
        let element_offset_size = (element_offset_size_minus_one + 1) as usize;
        
        Ok(ArrayHeader {
            num_elements_size,
            element_offset_size,
            is_large,
        })
    }
    
    /// Unpack integer from bytes
    pub fn unpack_int(bytes: &[u8], size: usize) -> Result<usize, ArrowError> {
        if bytes.len() < size {
            return Err(ArrowError::InvalidArgumentError(
                "Not enough bytes to unpack integer".to_string()
            ));
        }
        
        match size {
            1 => Ok(bytes[0] as usize),
            2 => Ok(u16::from_le_bytes([bytes[0], bytes[1]]) as usize),
            3 => Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize),
            4 => Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize),
            _ => Err(ArrowError::InvalidArgumentError(
                format!("Invalid integer size: {}", size)
            )),
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
    pub fn build_object_header(is_large: bool, field_id_size: usize, field_offset_size: usize) -> u8 {
        let large_bit = if is_large { 1 } else { 0 };
        (large_bit << 6) | (((field_id_size - 1) as u8) << 4) | (((field_offset_size - 1) as u8) << 2) | 2
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
    
    /// Get the basic type from header byte
    pub fn get_basic_type(header_byte: u8) -> u8 {
        header_byte & 0x03
    }
    
    /// Check if value bytes represent a primitive
    pub fn is_primitive(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == 0
    }
    
    /// Check if value bytes represent a short string
    pub fn is_short_string(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == 1
    }
    
    /// Check if value bytes represent an object
    pub fn is_object(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == 2
    }
    
    /// Check if value bytes represent an array
    pub fn is_array(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        Self::get_basic_type(value_bytes[0]) == 3
    }
    
    /// Get the data length for a primitive type
    pub fn get_primitive_data_length(primitive_type: &PrimitiveType) -> usize {
        match primitive_type {
            PrimitiveType::Null | PrimitiveType::True | PrimitiveType::False => 0,
            PrimitiveType::Int8 => 1,
            PrimitiveType::Int16 => 2,
            PrimitiveType::Int32 | PrimitiveType::Float | PrimitiveType::Decimal4 | PrimitiveType::Date => 4,
            PrimitiveType::Int64 | PrimitiveType::Double | PrimitiveType::Decimal8 | PrimitiveType::TimestampNtz | PrimitiveType::TimestampLtz => 8,
            PrimitiveType::Decimal16 => 16,
            PrimitiveType::Binary | PrimitiveType::String => 0, // Variable length, need to read from data
        }
    }
    
    /// Extract short string data from value bytes
    pub fn extract_short_string_data(value_bytes: &[u8]) -> Result<&[u8], ArrowError> {
        if value_bytes.is_empty() {
            return Err(ArrowError::InvalidArgumentError("Empty value bytes".to_string()));
        }
        
        let header = Self::parse_short_string_header(value_bytes[0])?;
        
        if value_bytes.len() < 1 + header.length {
            return Err(ArrowError::InvalidArgumentError(
                format!("Short string data length {} exceeds available bytes", header.length)
            ));
        }
        
        Ok(&value_bytes[1..1 + header.length])
    }
    
    /// Extract primitive data from value bytes
    pub fn extract_primitive_data(value_bytes: &[u8]) -> Result<&[u8], ArrowError> {
        if value_bytes.is_empty() {
            return Err(ArrowError::InvalidArgumentError("Empty value bytes".to_string()));
        }
        
        let primitive_type = Self::parse_primitive_header(value_bytes[0])?;
        let data_length = Self::get_primitive_data_length(&primitive_type);
        
        if data_length == 0 {
            // Handle variable length types and null/boolean
            match primitive_type {
                PrimitiveType::Null | PrimitiveType::True | PrimitiveType::False => Ok(&[]),
                PrimitiveType::Binary | PrimitiveType::String => {
                    // These require reading length from the data
                    if value_bytes.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Not enough bytes for variable length primitive".to_string()
                        ));
                    }
                    let length = u32::from_le_bytes([value_bytes[1], value_bytes[2], value_bytes[3], value_bytes[4]]) as usize;
                    if value_bytes.len() < 5 + length {
                        return Err(ArrowError::InvalidArgumentError(
                            "Variable length primitive data exceeds available bytes".to_string()
                        ));
                    }
                    Ok(&value_bytes[5..5 + length])
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    format!("Unhandled primitive type: {:?}", primitive_type)
                )),
            }
        } else {
            if value_bytes.len() < 1 + data_length {
                return Err(ArrowError::InvalidArgumentError(
                    format!("Primitive data length {} exceeds available bytes", data_length)
                ));
            }
            Ok(&value_bytes[1..1 + data_length])
        }
    }
    
    /// Calculate byte offsets for array elements
    pub fn calculate_array_offsets(header: &ArrayHeader, num_elements: usize) -> ArrayOffsets {
        let element_offsets_start = 1 + header.num_elements_size;
        let elements_start = element_offsets_start + ((num_elements + 1) * header.element_offset_size);
        
        ArrayOffsets {
            element_offsets_start,
            elements_start,
        }
    }
    
    /// Calculate byte offsets for object fields
    pub fn calculate_object_offsets(header: &ObjectHeader, num_elements: usize) -> ObjectOffsets {
        let field_ids_start = 1 + header.num_elements_size;
        let field_offsets_start = field_ids_start + (num_elements * header.field_id_size);
        let values_start = field_offsets_start + ((num_elements + 1) * header.field_offset_size);
        
        ObjectOffsets {
            field_ids_start,
            field_offsets_start,
            values_start,
        }
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
        assert_eq!(VariantParser::parse_primitive_header(0b00000000).unwrap(), PrimitiveType::Null);
        
        // Test true (primitive type 1)
        assert_eq!(VariantParser::parse_primitive_header(0b00000100).unwrap(), PrimitiveType::True);
        
        // Test false (primitive type 2)
        assert_eq!(VariantParser::parse_primitive_header(0b00001000).unwrap(), PrimitiveType::False);
        
        // Test int32 (primitive type 5)
        assert_eq!(VariantParser::parse_primitive_header(0b00010100).unwrap(), PrimitiveType::Int32);
        
        // Test double (primitive type 7)
        assert_eq!(VariantParser::parse_primitive_header(0b00011100).unwrap(), PrimitiveType::Double);
    }

    #[test]
    fn test_parse_short_string_header() {
        // Test 0-length short string
        assert_eq!(VariantParser::parse_short_string_header(0b00000001).unwrap(), ShortStringHeader { length: 0 });
        
        // Test 5-length short string
        assert_eq!(VariantParser::parse_short_string_header(0b00010101).unwrap(), ShortStringHeader { length: 5 });
        
        // Test 13-length short string (maximum)
        assert_eq!(VariantParser::parse_short_string_header(0b00110101).unwrap(), ShortStringHeader { length: 13 });
        
        // Test invalid length > 13
        assert!(VariantParser::parse_short_string_header(0b00111001).is_err());
    }

    #[test]
    fn test_parse_variant_header_dispatch() {
        // Test primitive dispatch
        let primitive_header = 0b00000100; // True primitive
        match VariantParser::parse_variant_header(primitive_header).unwrap() {
            VariantType::Primitive(PrimitiveType::True) => {},
            _ => panic!("Expected primitive True"),
        }
        
        // Test short string dispatch
        let short_string_header = 0b00010101; // 5-length short string
        match VariantParser::parse_variant_header(short_string_header).unwrap() {
            VariantType::ShortString(ShortStringHeader { length: 5 }) => {},
            _ => panic!("Expected short string with length 5"),
        }
        
        // Test object dispatch
        let object_header = 0b00000010; // Basic object
        match VariantParser::parse_variant_header(object_header).unwrap() {
            VariantType::Object(_) => {},
            _ => panic!("Expected object"),
        }
        
        // Test array dispatch
        let array_header = 0b00000011; // Basic array
        match VariantParser::parse_variant_header(array_header).unwrap() {
            VariantType::Array(_) => {},
            _ => panic!("Expected array"),
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
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Null), 0);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::True), 0);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::False), 0);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Int8), 1);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Int16), 2);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Int32), 4);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Int64), 8);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Double), 8);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Decimal16), 16);
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::Binary), 0); // Variable length
        assert_eq!(VariantParser::get_primitive_data_length(&PrimitiveType::String), 0); // Variable length
    }

    #[test]
    fn test_extract_short_string_data() {
        // Test 0-length short string
        let data = &[0b00000001]; // 0-length short string header
        assert_eq!(VariantParser::extract_short_string_data(data).unwrap(), &[] as &[u8]);
        
        // Test 5-length short string
        let data = &[0b00010101, b'H', b'e', b'l', b'l', b'o']; // 5-length short string + "Hello"
        assert_eq!(VariantParser::extract_short_string_data(data).unwrap(), b"Hello");
        
        // Test insufficient data
        let data = &[0b00010101, b'H', b'i']; // Claims 5 bytes but only has 2
        assert!(VariantParser::extract_short_string_data(data).is_err());
    }

    #[test]
    fn test_extract_primitive_data() {
        // Test null (no data)
        let data = &[0b00000000]; // Null header
        assert_eq!(VariantParser::extract_primitive_data(data).unwrap(), &[] as &[u8]);
        
        // Test true (no data)
        let data = &[0b00000100]; // True header
        assert_eq!(VariantParser::extract_primitive_data(data).unwrap(), &[] as &[u8]);
        
        // Test int32 (4 bytes)
        let data = &[0b00010100, 0x2A, 0x00, 0x00, 0x00]; // Int32 header + 42 in little endian
        assert_eq!(VariantParser::extract_primitive_data(data).unwrap(), &[0x2A, 0x00, 0x00, 0x00]);
        
        // Test insufficient data for int32
        let data = &[0b00010100, 0x2A, 0x00]; // Int32 header but only 2 bytes
        assert!(VariantParser::extract_primitive_data(data).is_err());
    }
} 