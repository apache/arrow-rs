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

//! Encoder module for converting JSON values to Variant binary format

use serde_json::Value;
use std::collections::HashMap;
use crate::error::Error;

/// Variant basic types as defined in the Arrow Variant specification
/// 
/// Basic Type	ID	Description
/// Primitive	0	One of the primitive types
/// Short string	1	A string with a length less than 64 bytes
/// Object	2	A collection of (string-key, variant-value) pairs
/// Array	3	An ordered sequence of variant values
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
fn object_header(is_large: bool, id_size: u8, offset_size: u8) -> u8 {
    ((is_large as u8) << 6) | 
    ((id_size - 1) << 4) | 
    ((offset_size - 1) << 2) | 
    VariantBasicType::Object as u8
}

/// Creates a header byte for an array value
/// 
/// The header byte contains:
/// - Basic type (2 bits) in the lower bits
/// - is_large (1 bit) at position 4
/// - field_offset_size_minus_one (2 bits) at positions 2-3
fn array_header(is_large: bool, offset_size: u8) -> u8 {
    ((is_large as u8) << 4) | 
    ((offset_size - 1) << 2) | 
    VariantBasicType::Array as u8
}

/// Encodes a null value
fn encode_null(output: &mut Vec<u8>) {
    output.push(primitive_header(VariantPrimitiveType::Null as u8));
}

/// Encodes a boolean value
fn encode_boolean(value: bool, output: &mut Vec<u8>) {
    if value {
        output.push(primitive_header(VariantPrimitiveType::BooleanTrue as u8));
    } else {
        output.push(primitive_header(VariantPrimitiveType::BooleanFalse as u8));
    }
}

/// Encodes an integer value, choosing the smallest sufficient type
fn encode_integer(value: i64, output: &mut Vec<u8>) {
    if value >= -128 && value <= 127 {
        // Int8
        output.push(primitive_header(VariantPrimitiveType::Int8 as u8));
        output.push(value as u8);
    } else if value >= -32768 && value <= 32767 {
        // Int16
        output.push(primitive_header(VariantPrimitiveType::Int16 as u8));
        output.extend_from_slice(&(value as i16).to_le_bytes());
    } else if value >= -2147483648 && value <= 2147483647 {
        // Int32
        output.push(primitive_header(VariantPrimitiveType::Int32 as u8));
        output.extend_from_slice(&(value as i32).to_le_bytes());
    } else {
        // Int64
        output.push(primitive_header(VariantPrimitiveType::Int64 as u8));
        output.extend_from_slice(&value.to_le_bytes());
    }
}

/// Encodes a float value
fn encode_float(value: f64, output: &mut Vec<u8>) {
    output.push(primitive_header(VariantPrimitiveType::Double as u8));
    output.extend_from_slice(&value.to_le_bytes());
}

/// Encodes a string value
fn encode_string(value: &str, output: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    let len = bytes.len();
    
    if len < 64 {
        // Short string format - encode length in header
        let header = short_str_header(len as u8);
        output.push(header);
        output.extend_from_slice(bytes);
    } else {
        // Long string format (using primitive string type)
        let header = primitive_header(VariantPrimitiveType::String as u8);
        output.push(header);
        
        // Write length as 4-byte little-endian
        output.extend_from_slice(&(len as u32).to_le_bytes());
        
        // Write string bytes
        output.extend_from_slice(bytes);
    }
}

/// Encodes an array value
fn encode_array(array: &[Value], output: &mut Vec<u8>, key_mapping: &HashMap<String, usize>) -> Result<(), Error> {
    let len = array.len();
    
    // Determine if we need large size encoding
    let is_large = len > 255;
    
    // First pass to calculate offsets and collect encoded values
    let mut temp_outputs = Vec::with_capacity(len);
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    
    let mut max_offset = 0;
    for value in array {
        let mut temp_output = Vec::new();
        encode_value(value, &mut temp_output, key_mapping)?;
        max_offset += temp_output.len();
        offsets.push(max_offset);
        temp_outputs.push(temp_output);
    }
    
    // Determine minimum offset size
    let offset_size = if max_offset <= 255 { 1 } 
                      else if max_offset <= 65535 { 2 }
                      else { 3 };
    
    // Write array header
    output.push(array_header(is_large, offset_size));
    
    // Write length as 1 or 4 bytes
    if is_large {
        output.extend_from_slice(&(len as u32).to_le_bytes());
    } else {
        output.push(len as u8);
    }
    
    // Write offsets
    for offset in &offsets {
        match offset_size {
            1 => output.push(*offset as u8),
            2 => output.extend_from_slice(&(*offset as u16).to_le_bytes()),
            3 => {
                output.push((*offset & 0xFF) as u8);
                output.push(((*offset >> 8) & 0xFF) as u8);
                output.push(((*offset >> 16) & 0xFF) as u8);
            },
            _ => unreachable!(),
        }
    }
    
    // Write values
    for temp_output in temp_outputs {
        output.extend_from_slice(&temp_output);
    }
    
    Ok(())
}

/// Encodes an object value
fn encode_object(obj: &serde_json::Map<String, Value>, output: &mut Vec<u8>, key_mapping: &HashMap<String, usize>) -> Result<(), Error> {
    let len = obj.len();
    
    // Determine if we need large size encoding
    let is_large = len > 255;
    
    // Collect and sort fields by key
    let mut fields: Vec<_> = obj.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    
    // First pass to calculate offsets and collect encoded values
    let mut field_ids = Vec::with_capacity(len);
    let mut temp_outputs = Vec::with_capacity(len);
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    
    let mut data_size = 0;
    for (key, value) in &fields {
        let field_id = key_mapping.get(key.as_str())
            .ok_or_else(|| Error::VariantCreation(format!("Key not found in mapping: {}", key)))?;
        field_ids.push(*field_id);
        
        let mut temp_output = Vec::new();
        encode_value(value, &mut temp_output, key_mapping)?;
        data_size += temp_output.len();
        offsets.push(data_size);
        temp_outputs.push(temp_output);
    }
    
    // Determine minimum sizes needed - use size 1 for empty objects
    let id_size = if field_ids.is_empty() { 1 }
                  else if field_ids.iter().max().unwrap() <= &255 { 1 }
                  else if field_ids.iter().max().unwrap() <= &65535 { 2 }
                  else if field_ids.iter().max().unwrap() <= &16777215 { 3 }
                  else { 4 };
                  
    let offset_size = if data_size <= 255 { 1 }
                      else if data_size <= 65535 { 2 }
                      else { 3 };
    
    // Write object header
    output.push(object_header(is_large, id_size, offset_size));
    
    // Write length as 1 or 4 bytes
    if is_large {
        output.extend_from_slice(&(len as u32).to_le_bytes());
    } else {
        output.push(len as u8);
    }
    
    // Write field IDs
    for id in &field_ids {
        match id_size {
            1 => output.push(*id as u8),
            2 => output.extend_from_slice(&(*id as u16).to_le_bytes()),
            3 => {
                output.push((*id & 0xFF) as u8);
                output.push(((*id >> 8) & 0xFF) as u8);
                output.push(((*id >> 16) & 0xFF) as u8);
            },
            4 => output.extend_from_slice(&(*id as u32).to_le_bytes()),
            _ => unreachable!(),
        }
    }
    
    // Write offsets
    for offset in &offsets {
        match offset_size {
            1 => output.push(*offset as u8),
            2 => output.extend_from_slice(&(*offset as u16).to_le_bytes()),
            3 => {
                output.push((*offset & 0xFF) as u8);
                output.push(((*offset >> 8) & 0xFF) as u8);
                output.push(((*offset >> 16) & 0xFF) as u8);
            },
            4 => output.extend_from_slice(&(*offset as u32).to_le_bytes()),
            _ => unreachable!(),
        }
    }
    
    // Write values
    for temp_output in temp_outputs {
        output.extend_from_slice(&temp_output);
    }
    
    Ok(())
}

/// Encodes a JSON value to Variant binary format
pub fn encode_value(value: &Value, output: &mut Vec<u8>, key_mapping: &HashMap<String, usize>) -> Result<(), Error> {
    match value {
        Value::Null => encode_null(output),
        Value::Bool(b) => encode_boolean(*b, output),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                encode_integer(i, output);
            } else if let Some(f) = n.as_f64() {
                encode_float(f, output);
            } else {
                return Err(Error::VariantCreation("Unsupported number format".to_string()));
            }
        },
        Value::String(s) => encode_string(s, output),
        Value::Array(a) => encode_array(a, output, key_mapping)?,
        Value::Object(o) => encode_object(o, output, key_mapping)?,
    }
    
    Ok(())
}

/// Encodes a JSON value to a complete Variant binary value
pub fn encode_json(json: &Value, key_mapping: &HashMap<String, usize>) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    encode_value(json, &mut output, key_mapping)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    fn setup_key_mapping() -> HashMap<String, usize> {
        let mut mapping = HashMap::new();
        mapping.insert("name".to_string(), 0);
        mapping.insert("age".to_string(), 1);
        mapping.insert("active".to_string(), 2);
        mapping.insert("scores".to_string(), 3);
        mapping.insert("address".to_string(), 4);
        mapping.insert("street".to_string(), 5);
        mapping.insert("city".to_string(), 6);
        mapping.insert("zip".to_string(), 7);
        mapping.insert("tags".to_string(), 8);
        mapping
    }
    
    #[test]
    fn test_encode_integers() {
        // Test Int8
        let mut output = Vec::new();
        encode_integer(42, &mut output);
        assert_eq!(output, vec![primitive_header(VariantPrimitiveType::Int8 as u8), 42]);
        
        // Test Int16
        output.clear();
        encode_integer(1000, &mut output);
        assert_eq!(output, vec![primitive_header(VariantPrimitiveType::Int16 as u8), 232, 3]);
        
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
        assert_eq!(output[0], primitive_header(VariantPrimitiveType::String as u8));
        
        // Check length bytes
        assert_eq!(&output[1..5], &(long_str.len() as u32).to_le_bytes());
        
        // Check string content
        assert_eq!(&output[5..], long_str.as_bytes());
    }
    
    #[test]
    fn test_encode_array() -> Result<(), Error> {
        let key_mapping = setup_key_mapping();
        let json = json!([1, "text", true, null]);
        
        let mut output = Vec::new();
        encode_array(json.as_array().unwrap(), &mut output, &key_mapping)?;
        
        // Validate array header
        assert_eq!(output[0], array_header(false, 1));
        assert_eq!(output[1], 4); // 4 elements
        
        // Array should contain encoded versions of the 4 values
        Ok(())
    }
    
    #[test]
    fn test_encode_object() -> Result<(), Error> {
        let key_mapping = setup_key_mapping();
        let json = json!({
            "name": "John",
            "age": 30,
            "active": true
        });
        
        let mut output = Vec::new();
        encode_object(json.as_object().unwrap(), &mut output, &key_mapping)?;
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id = 2 < 255)
        // - field_offset_size_minus_one = 0 (offset_size = 1, small offsets)
        assert_eq!(output[0], 0b00000010); // Object header
        
        // Verify num_elements (1 byte)
        assert_eq!(output[1], 3);
        
        // Verify field_ids (in lexicographical order: active, age, name)
        assert_eq!(output[2], 2); // active
        assert_eq!(output[3], 1); // age
        assert_eq!(output[4], 0); // name
        
        // Test empty object
        let empty_obj = json!({});
        output.clear();
        encode_object(empty_obj.as_object().unwrap(), &mut output, &key_mapping)?;
        
        // Verify header byte for empty object
        assert_eq!(output[0], 0b00000010); // Object header with minimum sizes
        assert_eq!(output[1], 0); // Zero elements
        
        // Test case 2: Object with large values requiring larger offsets
        let obj = json!({
            "name": "This is a very long string that will definitely require more than 255 bytes to encode. Let me add some more text to make sure it exceeds the limit. The string needs to be long enough to trigger the use of 2-byte offsets. Adding more content to ensure we go over the threshold. This is just padding text to make the string longer. Almost there, just a bit more to go. And finally, some more text to push us over the edge.",
            "age": 30,
            "active": true
        });
        
        output.clear();
        encode_object(obj.as_object().unwrap(), &mut output, &key_mapping)?;
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id = 2 < 255)
        // - field_offset_size_minus_one = 1 (offset_size = 2, large offsets)
        assert_eq!(output[0], 0b00000110); // Object header with 2-byte offsets
        
        // Test case 3: Object with nested objects
        let obj = json!({
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            },
            "scores": [95, 87, 92]
        });
        
        output.clear();
        encode_object(obj.as_object().unwrap(), &mut output, &key_mapping)?;
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id < 255)
        // - field_offset_size_minus_one = 0 (offset_size = 1, determined by data size)
        assert_eq!(output[0], 0b00000010); // Object header with 1-byte offsets
        
        // Verify num_elements (1 byte)
        assert_eq!(output[1], 3);
        
        // Verify field_ids (in lexicographical order: address, name, scores)
        assert_eq!(output[2], 4); // address
        assert_eq!(output[3], 0); // name
        assert_eq!(output[4], 3); // scores
        
        Ok(())
    }
    
    #[test]
    fn test_encode_null() {
        let mut output = Vec::new();
        encode_null(&mut output);
        assert_eq!(output, vec![primitive_header(VariantPrimitiveType::Null as u8)]);
        
        // Test that the encoded value can be decoded correctly
        let keys = Vec::<String>::new();
        let result = crate::decoder::decode_value(&output, &keys).unwrap();
        assert!(result.is_null());
    }
    
    #[test]
    fn test_encode_boolean() {
        // Test true
        let mut output = Vec::new();
        encode_boolean(true, &mut output);
        assert_eq!(output, vec![primitive_header(VariantPrimitiveType::BooleanTrue as u8)]);
        
        // Test that the encoded value can be decoded correctly
        let keys = Vec::<String>::new();
        let result = crate::decoder::decode_value(&output, &keys).unwrap();
        assert_eq!(result, serde_json::json!(true));
        
        // Test false
        output.clear();
        encode_boolean(false, &mut output);
        assert_eq!(output, vec![primitive_header(VariantPrimitiveType::BooleanFalse as u8)]);
        
        // Test that the encoded value can be decoded correctly
        let result = crate::decoder::decode_value(&output, &keys).unwrap();
        assert_eq!(result, serde_json::json!(false));
    }

    #[test]
    fn test_object_encoding() {
        let key_mapping = setup_key_mapping();
        let json = json!({
            "name": "John",
            "age": 30,
            "active": true
        });
        
        let mut output = Vec::new();
        encode_object(json.as_object().unwrap(), &mut output, &key_mapping).unwrap();
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id = 2 < 255)
        // - field_offset_size_minus_one = 0 (offset_size = 1, small offsets)
        assert_eq!(output[0], 0b00000010); // Object header
        
        // Verify num_elements (1 byte)
        assert_eq!(output[1], 3);
        
        // Verify field_ids (in lexicographical order: active, age, name)
        assert_eq!(output[2], 2); // active
        assert_eq!(output[3], 1); // age
        assert_eq!(output[4], 0); // name
        
        // Test case 2: Object with large values requiring larger offsets
        let obj = json!({
            "name": "This is a very long string that will definitely require more than 255 bytes to encode. Let me add some more text to make sure it exceeds the limit. The string needs to be long enough to trigger the use of 2-byte offsets. Adding more content to ensure we go over the threshold. This is just padding text to make the string longer. Almost there, just a bit more to go. And finally, some more text to push us over the edge.",
            "age": 30,
            "active": true
        });
        
        output.clear();
        encode_object(obj.as_object().unwrap(), &mut output, &key_mapping).unwrap();
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id = 2 < 255)
        // - field_offset_size_minus_one = 1 (offset_size = 2, large offsets)
        assert_eq!(output[0], 0b00000110); // Object header with 2-byte offsets
        
        
        // Test case 3: Object with nested objects
        let obj = json!({
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            },
            "scores": [95, 87, 92]
        });
        
        output.clear();
        encode_object(obj.as_object().unwrap(), &mut output, &key_mapping).unwrap();
        
        // Verify header byte
        // - basic_type = 2 (Object)
        // - is_large = 0 (3 elements < 255)
        // - field_id_size_minus_one = 0 (max field_id < 255)
        // - field_offset_size_minus_one = 0 (offset_size = 1, determined by data size)
        assert_eq!(output[0], 0b00000010); // Object header with 1-byte offsets
        
        // Verify num_elements (1 byte)
        assert_eq!(output[1], 3);
        
        // Verify field_ids (in lexicographical order: address, name, scores)
        assert_eq!(output[2], 4); // address
        assert_eq!(output[3], 0); // name
        assert_eq!(output[4], 3); // scores
        
    }
} 