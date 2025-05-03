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

//! Decoder module for converting Variant binary format to JSON values
#[allow(unused_imports)]
use serde_json::{json, Value, Map};
use std::str;
use crate::error::Error;
use crate::encoder::{VariantBasicType, VariantPrimitiveType};
#[allow(unused_imports)]
use std::collections::HashMap;


/// Decodes a Variant binary value to a JSON value
pub fn decode_value(value: &[u8], keys: &[String]) -> Result<Value, Error> {
    println!("Decoding value of length: {}", value.len());
    let mut pos = 0;
    let result = decode_value_internal(value, &mut pos, keys)?;
    println!("Decoded value: {:?}", result);
    Ok(result)
}

/// Extracts the basic type from a header byte
fn get_basic_type(header: u8) -> VariantBasicType {
    match header & 0x03 {
        0 => VariantBasicType::Primitive,
        1 => VariantBasicType::ShortString,
        2 => VariantBasicType::Object,
        3 => VariantBasicType::Array,
        _ => unreachable!(),
    }
}

/// Extracts the primitive type from a header byte
fn get_primitive_type(header: u8) -> VariantPrimitiveType {
    match (header >> 2) & 0x3F {
        0 => VariantPrimitiveType::Null,
        1 => VariantPrimitiveType::BooleanTrue,
        2 => VariantPrimitiveType::BooleanFalse,
        3 => VariantPrimitiveType::Int8,
        4 => VariantPrimitiveType::Int16,
        5 => VariantPrimitiveType::Int32,
        6 => VariantPrimitiveType::Int64,
        7 => VariantPrimitiveType::Double,
        8 => VariantPrimitiveType::Decimal4,
        9 => VariantPrimitiveType::Decimal8,
        10 => VariantPrimitiveType::Decimal16,
        11 => VariantPrimitiveType::Date,
        12 => VariantPrimitiveType::Timestamp,
        13 => VariantPrimitiveType::TimestampNTZ,
        14 => VariantPrimitiveType::Float,
        15 => VariantPrimitiveType::Binary,
        16 => VariantPrimitiveType::String,
        17 => VariantPrimitiveType::TimeNTZ,
        18 => VariantPrimitiveType::TimestampNanos,
        19 => VariantPrimitiveType::TimestampNTZNanos,
        20 => VariantPrimitiveType::Uuid,
        _ => unreachable!(),
    }
}

/// Extracts object header information
fn get_object_header_info(header: u8) -> (bool, u8, u8) {
    let header = (header >> 2) & 0x3F;               // Get header bits
    let is_large = (header >> 4) & 0x01 != 0;        // is_large from bit 4
    let id_size = ((header >> 2) & 0x03) + 1;        // field_id_size from bits 2-3
    let offset_size = (header & 0x03) + 1;           // offset_size from bits 0-1
    (is_large, id_size, offset_size)
}

/// Extracts array header information
fn get_array_header_info(header: u8) -> (bool, u8) {
    let header = (header >> 2) & 0x3F;               // Get header bits
    let is_large = (header >> 2) & 0x01 != 0;        // is_large from bit 2
    let offset_size = (header & 0x03) + 1;           // offset_size from bits 0-1
    (is_large, offset_size)
}

/// Reads an unsigned integer of the specified size
fn read_unsigned(data: &[u8], pos: &mut usize, size: u8) -> Result<usize, Error> {
    if *pos + (size as usize - 1) >= data.len() {
        return Err(Error::VariantRead(format!("Unexpected end of data for {} byte unsigned integer", size)));
    }
    
    let mut value = 0usize;
    for i in 0..size {
        value |= (data[*pos + i as usize] as usize) << (8 * i);
    }
    *pos += size as usize;
    
    Ok(value)
}

/// Internal recursive function to decode a value at the current position
fn decode_value_internal(data: &[u8], pos: &mut usize, keys: &[String]) -> Result<Value, Error> {
    if *pos >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data".to_string()));
    }
    
    let header = data[*pos];
    println!("Decoding at position {}: header byte = 0x{:02X}", *pos, header);
    *pos += 1;
    
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            match get_primitive_type(header) {
                VariantPrimitiveType::Null => Ok(Value::Null),
                VariantPrimitiveType::BooleanTrue => Ok(Value::Bool(true)),
                VariantPrimitiveType::BooleanFalse => Ok(Value::Bool(false)),
                VariantPrimitiveType::Int8 => decode_int8(data, pos),
                VariantPrimitiveType::Int16 => decode_int16(data, pos),
                VariantPrimitiveType::Int32 => decode_int32(data, pos),
                VariantPrimitiveType::Int64 => decode_int64(data, pos),
                VariantPrimitiveType::Double => decode_double(data, pos),
                VariantPrimitiveType::Decimal4 => decode_decimal4(data, pos),
                VariantPrimitiveType::Decimal8 => decode_decimal8(data, pos),
                VariantPrimitiveType::Decimal16 => decode_decimal16(data, pos),
                VariantPrimitiveType::Date => decode_date(data, pos),
                VariantPrimitiveType::Timestamp => decode_timestamp(data, pos),
                VariantPrimitiveType::TimestampNTZ => decode_timestamp_ntz(data, pos),
                VariantPrimitiveType::Float => decode_float(data, pos),
                VariantPrimitiveType::Binary => decode_binary(data, pos),
                VariantPrimitiveType::String => decode_long_string(data, pos),
                VariantPrimitiveType::TimeNTZ => decode_time_ntz(data, pos),
                VariantPrimitiveType::TimestampNanos => decode_timestamp_nanos(data, pos),
                VariantPrimitiveType::TimestampNTZNanos => decode_timestamp_ntz_nanos(data, pos),
                VariantPrimitiveType::Uuid => decode_uuid(data, pos),
            }
        },
        VariantBasicType::ShortString => {
            let len = (header >> 2) & 0x3F;
            println!("Short string with length: {}", len);
            if *pos + len as usize > data.len() {
                return Err(Error::VariantRead("Unexpected end of data for short string".to_string()));
            }
            
            let string_bytes = &data[*pos..*pos + len as usize];
            *pos += len as usize;
            
            let string = str::from_utf8(string_bytes)
                .map_err(|e| Error::InvalidMetadata(format!("Invalid UTF-8 string: {}", e)))?;
            
            Ok(Value::String(string.to_string()))
        },
        VariantBasicType::Object => {
            let (is_large, id_size, offset_size) = get_object_header_info(header);
            println!("Object header: is_large={}, id_size={}, offset_size={}", is_large, id_size, offset_size);
            
            // Read number of elements
            let num_elements = if is_large {
                read_unsigned(data, pos, 4)?
            } else {
                read_unsigned(data, pos, 1)?
            };
            println!("Object has {} elements", num_elements);
            
            // Read field IDs
            let mut field_ids = Vec::with_capacity(num_elements);
            for _ in 0..num_elements {
                field_ids.push(read_unsigned(data, pos, id_size)?);
            }
            println!("Field IDs: {:?}", field_ids);
            
            // Read offsets
            let mut offsets = Vec::with_capacity(num_elements + 1);
            for _ in 0..=num_elements {
                offsets.push(read_unsigned(data, pos, offset_size)?);
            }
            println!("Offsets: {:?}", offsets);
            
            // Create object and save position after offsets
            let mut obj = Map::new();
            let base_pos = *pos;
            
            // Process each field
            for i in 0..num_elements {
                let field_id = field_ids[i];
                if field_id >= keys.len() {
                    return Err(Error::VariantRead(format!("Field ID out of range: {}", field_id)));
                }
                
                let field_name = &keys[field_id];
                let start_offset = offsets[i];
                let end_offset = offsets[i + 1];
                
                println!("Field {}: {} (ID: {}), range: {}..{}", i, field_name, field_id, base_pos + start_offset, base_pos + end_offset);
                
                if base_pos + end_offset > data.len() {
                    return Err(Error::VariantRead("Unexpected end of data for object field".to_string()));
                }
                
                // Create a slice just for this field and decode it
                let field_data = &data[base_pos + start_offset..base_pos + end_offset];
                let mut field_pos = 0;
                let value = decode_value_internal(field_data, &mut field_pos, keys)?;
                
                obj.insert(field_name.clone(), value);
            }
            
            // Update position to end of object data
            *pos = base_pos + offsets[num_elements];
            Ok(Value::Object(obj))
        },
        VariantBasicType::Array => {
            let (is_large, offset_size) = get_array_header_info(header);
            println!("Array header: is_large={}, offset_size={}", is_large, offset_size);
            
            // Read number of elements
            let num_elements = if is_large {
                read_unsigned(data, pos, 4)?
            } else {
                read_unsigned(data, pos, 1)?
            };
            println!("Array has {} elements", num_elements);
            
            // Read offsets
            let mut offsets = Vec::with_capacity(num_elements + 1);
            for _ in 0..=num_elements {
                offsets.push(read_unsigned(data, pos, offset_size)?);
            }
            println!("Offsets: {:?}", offsets);
            
            // Create array and save position after offsets
            let mut array = Vec::with_capacity(num_elements);
            let base_pos = *pos;
            
            // Process each element
            for i in 0..num_elements {
                let start_offset = offsets[i];
                let end_offset = offsets[i + 1];
                
                println!("Element {}: range: {}..{}", i, base_pos + start_offset, base_pos + end_offset);
                
                if base_pos + end_offset > data.len() {
                    return Err(Error::VariantRead("Unexpected end of data for array element".to_string()));
                }
                
                // Create a slice just for this element and decode it
                let elem_data = &data[base_pos + start_offset..base_pos + end_offset];
                let mut elem_pos = 0;
                let value = decode_value_internal(elem_data, &mut elem_pos, keys)?;
                
                array.push(value);
            }
            
            // Update position to end of array data
            *pos = base_pos + offsets[num_elements];
            Ok(Value::Array(array))
        },
    }
}

/// Decodes a null value
#[allow(dead_code)]
fn decode_null() -> Result<Value, Error> {
    Ok(Value::Null)
}

/// Decodes a primitive value
#[allow(dead_code)]
fn decode_primitive(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for primitive".to_string()));
    }
    
    // Read the primitive type header
    let header = data[*pos];
    *pos += 1;
    
    // Extract primitive type ID
    let type_id = header & 0x1F;
    
    // Decode based on primitive type
    match type_id {
        0 => decode_null(),
        1 => Ok(Value::Bool(true)),
        2 => Ok(Value::Bool(false)),
        3 => decode_int8(data, pos),
        4 => decode_int16(data, pos),
        5 => decode_int32(data, pos),
        6 => decode_int64(data, pos),
        7 => decode_double(data, pos),
        8 => decode_decimal4(data, pos),
        9 => decode_decimal8(data, pos),
        10 => decode_decimal16(data, pos),
        11 => decode_date(data, pos),
        12 => decode_timestamp(data, pos),
        13 => decode_timestamp_ntz(data, pos),
        14 => decode_float(data, pos),
        15 => decode_binary(data, pos),
        16 => decode_long_string(data, pos),
        17 => decode_time_ntz(data, pos),
        18 => decode_timestamp_nanos(data, pos),
        19 => decode_timestamp_ntz_nanos(data, pos),
        20 => decode_uuid(data, pos),
        _ => Err(Error::InvalidMetadata(format!("Unknown primitive type ID: {}", type_id)))
    }
}

/// Decodes a short string value
#[allow(dead_code)]
fn decode_short_string(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for short string length".to_string()));
    }
    
    // Read the string length (1 byte)
    let len = data[*pos] as usize;
    *pos += 1;
    
    // Read the string bytes
    if *pos + len > data.len() {
        return Err(Error::VariantRead("Unexpected end of data for short string content".to_string()));
    }
    
    let string_bytes = &data[*pos..*pos + len];
    *pos += len;
    
    // Convert to UTF-8 string
    let string = str::from_utf8(string_bytes)
        .map_err(|e| Error::InvalidMetadata(format!("Invalid UTF-8 string: {}", e)))?;
    
    Ok(Value::String(string.to_string()))
}

/// Decodes an int8 value
fn decode_int8(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for int8".to_string()));
    }
    
    let value = data[*pos] as i8 as i64;
    *pos += 1;
    
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int16 value
fn decode_int16(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 1 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for int16".to_string()));
    }
    
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&data[*pos..*pos+2]);
    *pos += 2;
    
    let value = i16::from_le_bytes(buf) as i64;
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int32 value
fn decode_int32(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 3 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for int32".to_string()));
    }
    
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos+4]);
    *pos += 4;
    
    let value = i32::from_le_bytes(buf) as i64;
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int64 value
fn decode_int64(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for int64".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let value = i64::from_le_bytes(buf);
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes a double value
fn decode_double(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for double".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let value = f64::from_le_bytes(buf);
    
    // Create a Number from the float
    let number = serde_json::Number::from_f64(value)
        .ok_or_else(|| Error::InvalidMetadata(format!("Invalid float value: {}", value)))?;
    
    Ok(Value::Number(number))
}

/// Decodes a decimal4 value
fn decode_decimal4(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 4 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for decimal4".to_string()));
    }
    
    // Read scale (1 byte)
    let scale = data[*pos] as i32;
    *pos += 1;
    
    // Read unscaled value (3 bytes)
    let mut buf = [0u8; 4];
    buf[0] = data[*pos];
    buf[1] = data[*pos + 1];
    buf[2] = data[*pos + 2];
    buf[3] = 0; // Sign extend
    *pos += 3;
    
    let unscaled = i32::from_le_bytes(buf);
    
    // Convert to decimal string
    let decimal = format!("{}.{}", unscaled, scale);
    
    Ok(Value::String(decimal))
}

/// Decodes a decimal8 value
fn decode_decimal8(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 8 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for decimal8".to_string()));
    }
    
    // Read scale (1 byte)
    let scale = data[*pos] as i32;
    *pos += 1;
    
    // Read unscaled value (7 bytes)
    let mut buf = [0u8; 8];
    buf[0..7].copy_from_slice(&data[*pos..*pos+7]);
    buf[7] = 0; // Sign extend
    *pos += 7;
    
    let unscaled = i64::from_le_bytes(buf);
    
    // Convert to decimal string
    let decimal = format!("{}.{}", unscaled, scale);
    
    Ok(Value::String(decimal))
}

/// Decodes a decimal16 value
fn decode_decimal16(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 16 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for decimal16".to_string()));
    }
    
    // Read scale (1 byte)
    let scale = data[*pos] as i32;
    *pos += 1;
    
    // Read unscaled value (15 bytes)
    let mut buf = [0u8; 16];
    buf[0..15].copy_from_slice(&data[*pos..*pos+15]);
    buf[15] = 0; // Sign extend
    *pos += 15;
    
    // Convert to decimal string (simplified for now)
    let decimal = format!("decimal16.{}", scale);
    
    Ok(Value::String(decimal))
}

/// Decodes a date value
fn decode_date(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 3 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for date".to_string()));
    }
    
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos+4]);
    *pos += 4;
    
    let days = i32::from_le_bytes(buf);
    
    // Convert to ISO date string (simplified)
    let date = format!("date-{}", days);
    
    Ok(Value::String(date))
}

/// Decodes a timestamp value
fn decode_timestamp(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for timestamp".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let micros = i64::from_le_bytes(buf);
    
    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp-{}", micros);
    
    Ok(Value::String(timestamp))
}

/// Decodes a timestamp without timezone value
fn decode_timestamp_ntz(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for timestamp_ntz".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let micros = i64::from_le_bytes(buf);
    
    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_ntz-{}", micros);
    
    Ok(Value::String(timestamp))
}

/// Decodes a float value
fn decode_float(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 3 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for float".to_string()));
    }
    
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos+4]);
    *pos += 4;
    
    let value = f32::from_le_bytes(buf);
    
    // Create a Number from the float
    let number = serde_json::Number::from_f64(value as f64)
        .ok_or_else(|| Error::InvalidMetadata(format!("Invalid float value: {}", value)))?;
    
    Ok(Value::Number(number))
}

/// Decodes a binary value
fn decode_binary(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 3 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for binary length".to_string()));
    }
    
    // Read the binary length (4 bytes)
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos+4]);
    *pos += 4;
    
    let len = u32::from_le_bytes(buf) as usize;
    
    // Read the binary bytes
    if *pos + len > data.len() {
        return Err(Error::VariantRead("Unexpected end of data for binary content".to_string()));
    }
    
    let binary_bytes = &data[*pos..*pos + len];
    *pos += len;
    
    // Convert to hex string instead of base64
    let hex = binary_bytes.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<String>>()
        .join("");
    
    Ok(Value::String(format!("binary:{}", hex)))
}

/// Decodes a string value
fn decode_long_string(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 3 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for string length".to_string()));
    }
    
    // Read the string length (4 bytes)
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos+4]);
    *pos += 4;
    
    let len = u32::from_le_bytes(buf) as usize;
    
    // Read the string bytes
    if *pos + len > data.len() {
        return Err(Error::VariantRead("Unexpected end of data for string content".to_string()));
    }
    
    let string_bytes = &data[*pos..*pos + len];
    *pos += len;
    
    // Convert to UTF-8 string
    let string = str::from_utf8(string_bytes)
        .map_err(|e| Error::InvalidMetadata(format!("Invalid UTF-8 string: {}", e)))?;
    
    Ok(Value::String(string.to_string()))
}

/// Decodes a time without timezone value
fn decode_time_ntz(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for time_ntz".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let micros = i64::from_le_bytes(buf);
    
    // Convert to ISO time string (simplified)
    let time = format!("time_ntz-{}", micros);
    
    Ok(Value::String(time))
}

/// Decodes a timestamp with timezone (nanos) value
fn decode_timestamp_nanos(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for timestamp_nanos".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let nanos = i64::from_le_bytes(buf);
    
    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_nanos-{}", nanos);
    
    Ok(Value::String(timestamp))
}

/// Decodes a timestamp without timezone (nanos) value
fn decode_timestamp_ntz_nanos(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 7 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for timestamp_ntz_nanos".to_string()));
    }
    
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos+8]);
    *pos += 8;
    
    let nanos = i64::from_le_bytes(buf);
    
    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_ntz_nanos-{}", nanos);
    
    Ok(Value::String(timestamp))
}

/// Decodes a UUID value
fn decode_uuid(data: &[u8], pos: &mut usize) -> Result<Value, Error> {
    if *pos + 15 >= data.len() {
        return Err(Error::VariantRead("Unexpected end of data for uuid".to_string()));
    }
    
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[*pos..*pos+16]);
    *pos += 16;
    
    // Convert to UUID string (simplified)
    let uuid = format!("uuid-{:?}", buf);
    
    Ok(Value::String(uuid))
}

/// Decodes a Variant binary to a JSON value using the given metadata
pub fn decode_json(binary: &[u8], metadata: &[u8]) -> Result<Value, Error> {
    let keys = parse_metadata_keys(metadata)?;
    decode_value(binary, &keys)
}

/// Parses metadata to extract the key list
fn parse_metadata_keys(metadata: &[u8]) -> Result<Vec<String>, Error> {
    if metadata.is_empty() {
        return Err(Error::InvalidMetadata("Empty metadata".to_string()));
    }
    
    // Parse header
    let header = metadata[0];
    let version = header & 0x0F;
    let _sorted = (header >> 4) & 0x01 != 0;
    let offset_size_minus_one = (header >> 6) & 0x03;
    let offset_size = (offset_size_minus_one + 1) as usize;
    
    if version != 1 {
        return Err(Error::InvalidMetadata(format!("Unsupported version: {}", version)));
    }
    
    if metadata.len() < 1 + offset_size {
        return Err(Error::InvalidMetadata("Metadata too short for dictionary size".to_string()));
    }
    
    // Parse dictionary_size
    let mut dictionary_size = 0u32;
    for i in 0..offset_size {
        dictionary_size |= (metadata[1 + i] as u32) << (8 * i);
    }
    
    // Parse offsets
    let offset_start = 1 + offset_size;
    let offset_end = offset_start + (dictionary_size as usize + 1) * offset_size;
    
    if metadata.len() < offset_end {
        return Err(Error::InvalidMetadata("Metadata too short for offsets".to_string()));
    }
    
    let mut offsets = Vec::with_capacity(dictionary_size as usize + 1);
    for i in 0..=dictionary_size {
        let offset_pos = offset_start + (i as usize * offset_size);
        let mut offset = 0u32;
        for j in 0..offset_size {
            offset |= (metadata[offset_pos + j] as u32) << (8 * j);
        }
        offsets.push(offset as usize);
    }
    
    // Parse dictionary strings
    let mut keys = Vec::with_capacity(dictionary_size as usize);
    for i in 0..dictionary_size as usize {
        let start = offset_end + offsets[i];
        let end = offset_end + offsets[i + 1];
        
        if end > metadata.len() {
            return Err(Error::InvalidMetadata("Invalid string offset".to_string()));
        }
        
        let key = str::from_utf8(&metadata[start..end])
            .map_err(|e| Error::InvalidMetadata(format!("Invalid UTF-8: {}", e)))?
            .to_string();
            
        keys.push(key);
    }
    
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::create_metadata;
    use crate::encoder::encode_json;
    
    fn encode_and_decode(value: Value) -> Result<Value, Error> {
        // Create metadata for this value
        let metadata = create_metadata(&value, false)?;
        
        // Parse metadata to get key mapping
        let keys = parse_metadata_keys(&metadata)?;
        let key_mapping: HashMap<String, usize> = keys.iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), i))
            .collect();
        
        // Encode to binary
        let binary = encode_json(&value, &key_mapping)?;
        
        // Decode back to value
        decode_value(&binary, &keys)
    }
    
    #[test]
    fn test_decode_primitives() -> Result<(), Error> {
        // Test null
        let null_value = Value::Null;
        let decoded = encode_and_decode(null_value.clone())?;
        assert_eq!(decoded, null_value);
        
        // Test boolean
        let true_value = Value::Bool(true);
        let decoded = encode_and_decode(true_value.clone())?;
        assert_eq!(decoded, true_value);
        
        let false_value = Value::Bool(false);
        let decoded = encode_and_decode(false_value.clone())?;
        assert_eq!(decoded, false_value);
        
        // Test integer
        let int_value = json!(42);
        let decoded = encode_and_decode(int_value.clone())?;
        assert_eq!(decoded, int_value);
        
        // Test float
        let float_value = json!(3.14159);
        let decoded = encode_and_decode(float_value.clone())?;
        assert_eq!(decoded, float_value);
        
        // Test string
        let string_value = json!("Hello, World!");
        let decoded = encode_and_decode(string_value.clone())?;
        assert_eq!(decoded, string_value);
        
        Ok(())
    }
    
    #[test]
    fn test_decode_array() -> Result<(), Error> {
        let array_value = json!([1, 2, 3, 4, 5]);
        let decoded = encode_and_decode(array_value.clone())?;
        assert_eq!(decoded, array_value);
        
        let mixed_array = json!([1, "text", true, null]);
        let decoded = encode_and_decode(mixed_array.clone())?;
        assert_eq!(decoded, mixed_array);
        
        let nested_array = json!([[1, 2], [3, 4]]);
        let decoded = encode_and_decode(nested_array.clone())?;
        assert_eq!(decoded, nested_array);
        
        Ok(())
    }
    
    #[test]
    fn test_decode_object() -> Result<(), Error> {
        let object_value = json!({"name": "John", "age": 30});
        let decoded = encode_and_decode(object_value.clone())?;
        assert_eq!(decoded, object_value);
        
        let complex_object = json!({
            "name": "John",
            "age": 30,
            "is_active": true,
            "email": null
        });
        let decoded = encode_and_decode(complex_object.clone())?;
        assert_eq!(decoded, complex_object);
        
        let nested_object = json!({
            "person": {
                "name": "John",
                "age": 30
            },
            "company": {
                "name": "ACME Inc.",
                "location": "New York"
            }
        });
        let decoded = encode_and_decode(nested_object.clone())?;
        assert_eq!(decoded, nested_object);
        
        Ok(())
    }
    
    #[test]
    fn test_decode_complex() -> Result<(), Error> {
        let complex_value = json!({
            "name": "John Doe",
            "age": 30,
            "is_active": true,
            "scores": [95, 87, 92],
            "null_value": null,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "zip": 12345
            },
            "contacts": [
                {
                    "type": "email",
                    "value": "john@example.com"
                },
                {
                    "type": "phone",
                    "value": "555-1234"
                }
            ]
        });
        
        let decoded = encode_and_decode(complex_value.clone())?;
        assert_eq!(decoded, complex_value);
        
        Ok(())
    }
    
    #[test]
    fn test_decode_null_function() {
        let result = decode_null().unwrap();
        assert_eq!(result, Value::Null);
    }
    
    #[test]
    fn test_decode_primitive_function() -> Result<(), Error> {
        // Test with null type
        let mut pos = 0;
        let data = [0x00]; // Null type
        let result = decode_primitive(&data, &mut pos)?;
        assert_eq!(result, Value::Null);
        
        // Test with boolean true
        let mut pos = 0;
        let data = [0x01]; // Boolean true
        let result = decode_primitive(&data, &mut pos)?;
        assert_eq!(result, Value::Bool(true));
        
        // Test with boolean false
        let mut pos = 0;
        let data = [0x02]; // Boolean false
        let result = decode_primitive(&data, &mut pos)?;
        assert_eq!(result, Value::Bool(false));
        
        // Test with int8
        let mut pos = 0;
        let data = [0x03, 42]; // Int8 type, value 42
        let result = decode_primitive(&data, &mut pos)?;
        assert_eq!(result, json!(42));
        
        // Test with string
        let mut pos = 0;
        let data = [0x10, 0x05, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6C, 0x6C, 0x6F]; 
        // String type, length 5, "Hello"
        let result = decode_primitive(&data, &mut pos)?;
        assert_eq!(result, json!("Hello"));
        
        Ok(())
    }
    
    #[test]
    fn test_decode_short_string_function() -> Result<(), Error> {
        let mut pos = 0;
        let data = [0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F]; // Length 5, "Hello"
        let result = decode_short_string(&data, &mut pos)?;
        assert_eq!(result, json!("Hello"));
        
        // Test with empty string
        let mut pos = 0;
        let data = [0x00]; // Length 0, ""
        let result = decode_short_string(&data, &mut pos)?;
        assert_eq!(result, json!(""));
        
        // Test with error case - unexpected end of data
        let mut pos = 0;
        let data = [0x05, 0x48, 0x65]; // Length 5 but only 3 bytes available
        let result = decode_short_string(&data, &mut pos);
        assert!(result.is_err());
        
        Ok(())
    }
    
    #[test]
    fn test_decode_string_function() -> Result<(), Error> {
        let mut pos = 0;
        let data = [0x05, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6C, 0x6C, 0x6F]; 
        // Length 5, "Hello"
        let result = decode_long_string(&data, &mut pos)?;
        assert_eq!(result, json!("Hello"));
        
        // Test with empty string
        let mut pos = 0;
        let data = [0x00, 0x00, 0x00, 0x00]; // Length 0, ""
        let result = decode_long_string(&data, &mut pos)?;
        assert_eq!(result, json!(""));
        
        // Test with error case - unexpected end of data
        let mut pos = 0;
        let data = [0x05, 0x00, 0x00, 0x00, 0x48, 0x65]; 
        // Length 5 but only 2 bytes available
        let result = decode_long_string(&data, &mut pos);
        assert!(result.is_err());
        
        Ok(())
    }
} 