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

/// Object header structure for variant objects
#[derive(Debug, Clone)]
pub struct ObjectHeader {
    pub num_elements_size: usize,
    pub field_id_size: usize,
    pub field_offset_size: usize,
    pub is_large: bool,
}

/// Array header structure for variant objects
#[derive(Debug, Clone)]
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
    
    /// Check if value bytes represent an object
    pub fn is_object(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        
        let header_byte = value_bytes[0];
        let basic_type = header_byte & 0x03; // Basic type is in first 2 bits
        basic_type == 2 // Object type
    }
    
    /// Get the basic type from header byte
    pub fn get_basic_type(header_byte: u8) -> u8 {
        header_byte & 0x03 // Basic type is in first 2 bits
    }
    
    /// Check if value bytes represent an array
    pub fn is_array(value_bytes: &[u8]) -> bool {
        if value_bytes.is_empty() {
            return false;
        }
        
        let header_byte = value_bytes[0];
        let basic_type = header_byte & 0x03; // Basic type is in first 2 bits
        basic_type == 3 // Array type
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
} 