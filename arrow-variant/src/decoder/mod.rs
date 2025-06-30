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
use crate::encoder::{VariantBasicType, VariantPrimitiveType};
use arrow_schema::ArrowError;
use indexmap::IndexMap;
#[allow(unused_imports)]
use serde_json::{json, Map, Value};
#[allow(unused_imports)]
use std::collections::HashMap;
use std::str;

/// Decodes a Variant binary value to a JSON value
pub fn decode_value(value: &[u8], keys: &[String]) -> Result<Value, ArrowError> {
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
    let header = (header >> 2) & 0x3F; // Get header bits
    let is_large = (header >> 4) & 0x01 != 0; // is_large from bit 4
    let id_size = ((header >> 2) & 0x03) + 1; // field_id_size from bits 2-3
    let offset_size = (header & 0x03) + 1; // offset_size from bits 0-1
    (is_large, id_size, offset_size)
}

/// Extracts array header information
fn get_array_header_info(header: u8) -> (bool, u8) {
    let header = (header >> 2) & 0x3F; // Get header bits
    let is_large = (header >> 2) & 0x01 != 0; // is_large from bit 2
    let offset_size = (header & 0x03) + 1; // offset_size from bits 0-1
    (is_large, offset_size)
}

/// Reads an unsigned integer of the specified size
fn read_unsigned(data: &[u8], pos: &mut usize, size: u8) -> Result<usize, ArrowError> {
    if *pos + (size as usize - 1) >= data.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Unexpected end of data for {} byte unsigned integer",
            size
        )));
    }

    let mut value = 0usize;
    for i in 0..size {
        value |= (data[*pos + i as usize] as usize) << (8 * i);
    }
    *pos += size as usize;

    Ok(value)
}

/// Internal recursive function to decode a value at the current position
fn decode_value_internal(
    data: &[u8],
    pos: &mut usize,
    keys: &[String],
) -> Result<Value, ArrowError> {
    if *pos >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data".to_string(),
        ));
    }

    let header = data[*pos];
    println!(
        "Decoding at position {}: header byte = 0x{:02X}",
        *pos, header
    );
    *pos += 1;

    match get_basic_type(header) {
        VariantBasicType::Primitive => match get_primitive_type(header) {
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
        },
        VariantBasicType::ShortString => {
            let len = (header >> 2) & 0x3F;
            println!("Short string with length: {}", len);
            if *pos + len as usize > data.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "Unexpected end of data for short string".to_string(),
                ));
            }

            let string_bytes = &data[*pos..*pos + len as usize];
            *pos += len as usize;

            let string = str::from_utf8(string_bytes)
                .map_err(|e| ArrowError::SchemaError(format!("Invalid UTF-8 string: {}", e)))?;

            Ok(Value::String(string.to_string()))
        }
        VariantBasicType::Object => {
            let (is_large, id_size, offset_size) = get_object_header_info(header);
            println!(
                "Object header: is_large={}, id_size={}, offset_size={}",
                is_large, id_size, offset_size
            );

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
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Field ID out of range: {}",
                        field_id
                    )));
                }

                let field_name = &keys[field_id];
                let start_offset = offsets[i];
                let end_offset = offsets[i + 1];

                println!(
                    "Field {}: {} (ID: {}), range: {}..{}",
                    i,
                    field_name,
                    field_id,
                    base_pos + start_offset,
                    base_pos + end_offset
                );

                if base_pos + end_offset > data.len() {
                    return Err(ArrowError::SchemaError(
                        "Unexpected end of data for object field".to_string(),
                    ));
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
        }
        VariantBasicType::Array => {
            let (is_large, offset_size) = get_array_header_info(header);
            println!(
                "Array header: is_large={}, offset_size={}",
                is_large, offset_size
            );

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

                println!(
                    "Element {}: range: {}..{}",
                    i,
                    base_pos + start_offset,
                    base_pos + end_offset
                );

                if base_pos + end_offset > data.len() {
                    return Err(ArrowError::SchemaError(
                        "Unexpected end of data for array element".to_string(),
                    ));
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
        }
    }
}

/// Decodes a null value
#[allow(dead_code)]
fn decode_null() -> Result<Value, ArrowError> {
    Ok(Value::Null)
}

/// Decodes a primitive value
#[allow(dead_code)]
fn decode_primitive(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for primitive".to_string(),
        ));
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
        _ => Err(ArrowError::SchemaError(format!(
            "Unknown primitive type ID: {}",
            type_id
        ))),
    }
}

/// Decodes a short string value
#[allow(dead_code)]
fn decode_short_string(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for short string length".to_string(),
        ));
    }

    // Read the string length (1 byte)
    let len = data[*pos] as usize;
    *pos += 1;

    // Read the string bytes
    if *pos + len > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for short string content".to_string(),
        ));
    }

    let string_bytes = &data[*pos..*pos + len];
    *pos += len;

    // Convert to UTF-8 string
    let string = str::from_utf8(string_bytes)
        .map_err(|e| ArrowError::SchemaError(format!("Invalid UTF-8 string: {}", e)))?;

    Ok(Value::String(string.to_string()))
}

/// Decodes an int8 value
fn decode_int8(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for int8".to_string(),
        ));
    }

    let value = data[*pos] as i8 as i64;
    *pos += 1;

    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int16 value
fn decode_int16(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 1 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for int16".to_string(),
        ));
    }

    let mut buf = [0u8; 2];
    buf.copy_from_slice(&data[*pos..*pos + 2]);
    *pos += 2;

    let value = i16::from_le_bytes(buf) as i64;
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int32 value
fn decode_int32(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 3 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for int32".to_string(),
        ));
    }

    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let value = i32::from_le_bytes(buf) as i64;
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes an int64 value
fn decode_int64(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for int64".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let value = i64::from_le_bytes(buf);
    Ok(Value::Number(serde_json::Number::from(value)))
}

/// Decodes a double value
fn decode_double(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for double".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let value = f64::from_le_bytes(buf);

    // Create a Number from the float
    let number = serde_json::Number::from_f64(value)
        .ok_or_else(|| ArrowError::SchemaError(format!("Invalid float value: {}", value)))?;

    Ok(Value::Number(number))
}

/// Decodes a decimal4 value
fn decode_decimal4(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 4 > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for decimal4".to_string(),
        ));
    }

    // Read scale (1 byte)
    let scale = data[*pos];
    *pos += 1;

    // Read unscaled value (4 bytes)
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let unscaled = i32::from_le_bytes(buf);

    // Correctly scale the value: divide by 10^scale
    let scaled = (unscaled as f64) / 10f64.powi(scale as i32);

    // Format as JSON number
    let number = serde_json::Number::from_f64(scaled)
        .ok_or_else(|| ArrowError::SchemaError(format!("Invalid decimal value: {}", scaled)))?;

    Ok(Value::Number(number))
}

/// Decodes a decimal8 value
fn decode_decimal8(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 8 > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for decimal8".to_string(),
        ));
    }

    let scale = data[*pos] as i32;
    *pos += 1;

    let mut buf = [0u8; 8];
    buf[..7].copy_from_slice(&data[*pos..*pos + 7]);
    buf[7] = if (buf[6] & 0x80) != 0 { 0xFF } else { 0x00 };
    *pos += 7;

    let unscaled = i64::from_le_bytes(buf);
    let value = (unscaled as f64) / 10f64.powi(scale);

    Ok(Value::Number(
        serde_json::Number::from_f64(value)
            .ok_or_else(|| ArrowError::ParseError("Invalid f64 from decimal8".to_string()))?,
    ))
}

/// Decodes a decimal16 value
fn decode_decimal16(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 16 > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for decimal16".to_string(),
        ));
    }

    let scale = data[*pos] as i32;
    *pos += 1;

    let mut buf = [0u8; 16];
    buf[..15].copy_from_slice(&data[*pos..*pos + 15]);
    buf[15] = if (buf[14] & 0x80) != 0 { 0xFF } else { 0x00 };
    *pos += 15;

    let unscaled = i128::from_le_bytes(buf);
    let s = format!(
        "{}.{:0>width$}",
        unscaled / 10i128.pow(scale as u32),
        (unscaled.abs() % 10i128.pow(scale as u32)),
        width = scale as usize
    );

    Ok(Value::String(s))
}

/// Decodes a date value
fn decode_date(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 3 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for date".to_string(),
        ));
    }

    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let days = i32::from_le_bytes(buf);

    // Convert to ISO date string (simplified)
    let date = format!("date-{}", days);

    Ok(Value::String(date))
}

/// Decodes a timestamp value
fn decode_timestamp(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for timestamp".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let micros = i64::from_le_bytes(buf);

    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp-{}", micros);

    Ok(Value::String(timestamp))
}

/// Decodes a timestamp without timezone value
fn decode_timestamp_ntz(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for timestamp_ntz".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let micros = i64::from_le_bytes(buf);

    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_ntz-{}", micros);

    Ok(Value::String(timestamp))
}

/// Decodes a float value
fn decode_float(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 3 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for float".to_string(),
        ));
    }

    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let value = f32::from_le_bytes(buf);

    // Create a Number from the float
    let number = serde_json::Number::from_f64(value as f64)
        .ok_or_else(|| ArrowError::SchemaError(format!("Invalid float value: {}", value)))?;

    Ok(Value::Number(number))
}

/// Decodes a binary value
fn decode_binary(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 3 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for binary length".to_string(),
        ));
    }

    // Read the binary length (4 bytes)
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let len = u32::from_le_bytes(buf) as usize;

    // Read the binary bytes
    if *pos + len > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for binary content".to_string(),
        ));
    }

    let binary_bytes = &data[*pos..*pos + len];
    *pos += len;

    // Convert to hex string instead of base64
    let hex = binary_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<String>>()
        .join("");

    Ok(Value::String(format!("binary:{}", hex)))
}

/// Decodes a string value
fn decode_long_string(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 3 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for string length".to_string(),
        ));
    }

    // Read the string length (4 bytes)
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;

    let len = u32::from_le_bytes(buf) as usize;

    // Read the string bytes
    if *pos + len > data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for string content".to_string(),
        ));
    }

    let string_bytes = &data[*pos..*pos + len];
    *pos += len;

    // Convert to UTF-8 string
    let string = str::from_utf8(string_bytes)
        .map_err(|e| ArrowError::SchemaError(format!("Invalid UTF-8 string: {}", e)))?;

    Ok(Value::String(string.to_string()))
}

/// Decodes a time without timezone value
fn decode_time_ntz(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for time_ntz".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let micros = i64::from_le_bytes(buf);

    // Convert to ISO time string (simplified)
    let time = format!("time_ntz-{}", micros);

    Ok(Value::String(time))
}

/// Decodes a timestamp with timezone (nanos) value
fn decode_timestamp_nanos(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for timestamp_nanos".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let nanos = i64::from_le_bytes(buf);

    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_nanos-{}", nanos);

    Ok(Value::String(timestamp))
}

/// Decodes a timestamp without timezone (nanos) value
fn decode_timestamp_ntz_nanos(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 7 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for timestamp_ntz_nanos".to_string(),
        ));
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;

    let nanos = i64::from_le_bytes(buf);

    // Convert to ISO timestamp string (simplified)
    let timestamp = format!("timestamp_ntz_nanos-{}", nanos);

    Ok(Value::String(timestamp))
}

/// Decodes a UUID value
fn decode_uuid(data: &[u8], pos: &mut usize) -> Result<Value, ArrowError> {
    if *pos + 15 >= data.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Unexpected end of data for uuid".to_string(),
        ));
    }

    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[*pos..*pos + 16]);
    *pos += 16;

    // Convert to UUID string (simplified)
    let uuid = format!("uuid-{:?}", buf);

    Ok(Value::String(uuid))
}

/// Decodes a Variant binary to a JSON value using the given metadata
pub fn decode_json(binary: &[u8], metadata: &[u8]) -> Result<Value, ArrowError> {
    let keys = parse_metadata_keys(metadata)?;
    decode_value(binary, &keys)
}

/// A helper struct to simplify metadata dictionary handling
struct MetadataDictionary {
    keys: Vec<String>,
    key_to_id: IndexMap<String, usize>,
}

impl MetadataDictionary {
    fn new(metadata: &[u8]) -> Result<Self, ArrowError> {
        let keys = parse_metadata_keys(metadata)?;

        // Build key to id mapping for faster lookups
        let mut key_to_id = IndexMap::new();
        for (i, key) in keys.iter().enumerate() {
            key_to_id.insert(key.clone(), i);
        }

        Ok(Self { keys, key_to_id })
    }

    fn get_field_id(&self, key: &str) -> Option<usize> {
        self.key_to_id.get(key).copied()
    }

    fn get_key(&self, id: usize) -> Option<&str> {
        self.keys.get(id).map(|s| s.as_str())
    }
}

/// Parses metadata to extract the key list
pub fn parse_metadata_keys(metadata: &[u8]) -> Result<Vec<String>, ArrowError> {
    if metadata.is_empty() {
        // Return empty key list if no metadata
        return Ok(Vec::new());
    }

    // Parse header
    let header = metadata[0];
    let version = header & 0x0F;
    let _sorted = (header >> 4) & 0x01 != 0;
    let offset_size_minus_one = (header >> 6) & 0x03;
    let offset_size = (offset_size_minus_one + 1) as usize;

    if version != 1 {
        return Err(ArrowError::SchemaError(format!(
            "Unsupported version: {}",
            version
        )));
    }

    if metadata.len() < 1 + offset_size {
        return Err(ArrowError::SchemaError(
            "Metadata too short for dictionary size".to_string(),
        ));
    }

    // Parse dictionary_size
    let mut dictionary_size = 0u32;
    for i in 0..offset_size {
        dictionary_size |= (metadata[1 + i] as u32) << (8 * i);
    }

    // Early return if dictionary is empty
    if dictionary_size == 0 {
        return Ok(Vec::new());
    }

    // Parse offsets
    let offset_start = 1 + offset_size;
    let offset_end = offset_start + (dictionary_size as usize + 1) * offset_size;

    if metadata.len() < offset_end {
        return Err(ArrowError::SchemaError(
            "Metadata too short for offsets".to_string(),
        ));
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
            return Err(ArrowError::SchemaError(format!(
                "Invalid string offset: start={}, end={}, metadata_len={}",
                start,
                end,
                metadata.len()
            )));
        }

        let key = str::from_utf8(&metadata[start..end])
            .map_err(|e| ArrowError::SchemaError(format!("Invalid UTF-8: {}", e)))?
            .to_string();

        keys.push(key);
    }

    println!("Parsed metadata keys: {:?}", keys);

    Ok(keys)
}

/// Validates that the binary data represents a valid Variant
/// Returns error if the format is invalid
pub fn validate_variant(value: &[u8], metadata: &[u8]) -> Result<(), ArrowError> {
    // Check if metadata is valid
    let keys = parse_metadata_keys(metadata)?;

    // Try to decode the value using the metadata to validate the format
    let mut pos = 0;
    decode_value_internal(value, &mut pos, &keys)?;

    Ok(())
}

/// Checks if the variant is an object
pub fn is_object(value: &[u8]) -> Result<bool, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value data".to_string(),
        ));
    }

    let header = value[0];
    let basic_type = get_basic_type(header);

    Ok(matches!(basic_type, VariantBasicType::Object))
}

/// Checks if the variant is an array
pub fn is_array(value: &[u8]) -> Result<bool, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value data".to_string(),
        ));
    }

    let header = value[0];
    let basic_type = get_basic_type(header);

    Ok(matches!(basic_type, VariantBasicType::Array))
}

/// Formats a variant value as a string for debugging purposes
pub fn format_variant_value(value: &[u8], metadata: &[u8]) -> Result<String, ArrowError> {
    if value.is_empty() {
        return Ok("null".to_string());
    }

    let keys = parse_metadata_keys(metadata)?;
    let mut pos = 0;
    let json_value = decode_value_internal(value, &mut pos, &keys)?;

    // Return the JSON string representation
    Ok(json_value.to_string())
}

/// Gets a field value range from an object variant
pub fn get_field_value_range(
    value: &[u8],
    metadata: &[u8],
    key: &str,
) -> Result<Option<(usize, usize)>, ArrowError> {
    // First check if this is an object
    if !is_object(value)? {
        return Ok(None);
    }

    // Parse the metadata dictionary to get all keys
    let dict = MetadataDictionary::new(metadata)?;

    // Get the field ID for this key
    let field_id = match dict.get_field_id(key) {
        Some(id) => id,
        None => {
            println!("Key '{}' not found in metadata dictionary", key);
            return Ok(None); // Key not found in metadata dictionary
        }
    };

    println!("Looking for field '{}' with ID {}", key, field_id);

    // Read object header
    let header = value[0];
    let (is_large, id_size, offset_size) = get_object_header_info(header);

    // Parse the number of elements
    let mut pos = 1; // Skip header
    let num_elements = if is_large {
        read_unsigned(value, &mut pos, 4)?
    } else {
        read_unsigned(value, &mut pos, 1)?
    };

    // Read all field IDs to find our target
    let field_ids_start = pos;

    // First scan to print all fields (for debugging)
    let mut debug_pos = pos;
    let mut found_fields = Vec::new();
    for i in 0..num_elements {
        let id = read_unsigned(value, &mut debug_pos, id_size)?;
        found_fields.push(id);
        if let Some(name) = dict.get_key(id) {
            println!("Field {} has ID {} and name '{}'", i, id, name);
        } else {
            println!("Field {} has ID {} but no name in dictionary", i, id);
        }
    }

    // Find the index of our target field ID
    // Binary search can be used because field keys (not IDs) are in lexicographical order
    let mut field_index = None;

    // Binary search
    let mut low = 0;
    let mut high = (num_elements as i64) - 1;

    while low <= high {
        let mid = ((low + high) / 2) as usize;
        let pos = field_ids_start + (mid * id_size as usize);

        if pos + id_size as usize <= value.len() {
            let mut temp_pos = pos;
            let id = read_unsigned(value, &mut temp_pos, id_size)?;

            // Get key for this ID and compare it with our target key
            if let Some(field_key) = dict.get_key(id) {
                match field_key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        low = mid as i64 + 1;
                    }
                    std::cmp::Ordering::Greater => {
                        high = mid as i64 - 1;
                    }
                    std::cmp::Ordering::Equal => {
                        field_index = Some(mid);
                        break;
                    }
                }
            } else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Field ID {} not found in metadata dictionary",
                    id
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Field ID position out of bounds: {} + {}",
                pos, id_size
            )));
        }
    }

    // If field ID not found in this object, return None
    let idx = match field_index {
        Some(idx) => idx,
        None => {
            println!(
                "Field ID {} not found in object fields: {:?}",
                field_id, found_fields
            );
            return Ok(None);
        }
    };

    // Calculate positions for offsets
    let offsets_start = field_ids_start + (num_elements * id_size as usize);

    // Read the start and end offsets for this field
    let start_offset_pos = offsets_start + (idx * offset_size as usize);
    let end_offset_pos = offsets_start + ((idx + 1) * offset_size as usize);

    // Read offsets directly at their positions
    let mut pos = start_offset_pos;
    let start_offset = read_unsigned(value, &mut pos, offset_size)?;

    pos = end_offset_pos;
    let end_offset = read_unsigned(value, &mut pos, offset_size)?;

    // Calculate data section start (after all offsets)
    let data_start = offsets_start + ((num_elements + 1) * offset_size as usize);

    // Calculate absolute positions
    let field_start = data_start + start_offset;
    let field_end = data_start + end_offset;

    println!("Field {} value range: {}..{}", key, field_start, field_end);

    // Validate offsets
    if field_end > value.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Field offset out of bounds: {} > {}",
            field_end,
            value.len()
        )));
    }

    // Return the field value range
    Ok(Some((field_start, field_end)))
}

/// Gets a field value from an object variant
pub fn get_field_value(
    value: &[u8],
    metadata: &[u8],
    key: &str,
) -> Result<Option<Vec<u8>>, ArrowError> {
    let range = get_field_value_range(value, metadata, key)?;
    Ok(range.map(|(start, end)| value[start..end].to_vec()))
}

/// Gets an array element range
pub fn get_array_element_range(
    value: &[u8],
    index: usize,
) -> Result<Option<(usize, usize)>, ArrowError> {
    // Check that the value is an array
    if !is_array(value)? {
        return Ok(None);
    }

    // Parse array header
    let header = value[0];
    let (is_large, offset_size) = get_array_header_info(header);

    // Parse the number of elements
    let mut pos = 1; // Skip header
    let num_elements = if is_large {
        read_unsigned(value, &mut pos, 4)?
    } else {
        read_unsigned(value, &mut pos, 1)?
    };

    // Check if index is out of bounds
    if index >= num_elements as usize {
        return Ok(None);
    }

    // Calculate positions for offsets
    let offsets_start = pos;

    // Read the start and end offsets for this element
    let start_offset_pos = offsets_start + (index * offset_size as usize);
    let end_offset_pos = offsets_start + ((index + 1) * offset_size as usize);

    let mut pos = start_offset_pos;
    let start_offset = read_unsigned(value, &mut pos, offset_size)?;

    pos = end_offset_pos;
    let end_offset = read_unsigned(value, &mut pos, offset_size)?;

    // Calculate data section start (after all offsets)
    let data_start = offsets_start + ((num_elements + 1) * offset_size as usize);

    // Calculate absolute positions
    let elem_start = data_start + start_offset;
    let elem_end = data_start + end_offset;

    println!("Element {} range: {}..{}", index, elem_start, elem_end);

    // Validate offsets
    if elem_end > value.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Element offset out of bounds: {} > {}",
            elem_end,
            value.len()
        )));
    }

    // Return the element value range
    Ok(Some((elem_start, elem_end)))
}

/// Gets an array element value
pub fn get_array_element(value: &[u8], index: usize) -> Result<Option<Vec<u8>>, ArrowError> {
    let range = get_array_element_range(value, index)?;
    Ok(range.map(|(start, end)| value[start..end].to_vec()))
}

/// Decode a string value
pub fn decode_string(value: &[u8]) -> Result<String, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    // Check header byte
    let header = value[0];

    match get_basic_type(header) {
        VariantBasicType::ShortString => {
            // Short string format - length is encoded in the header
            let len = (header >> 2) & 0x3F; // Extract 6 bits of length
            if value.len() < 1 + len as usize {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Buffer too short for short string: expected {} bytes",
                    1 + len
                )));
            }

            // Extract the string bytes and convert to String
            let string_bytes = &value[1..1 + len as usize];
            String::from_utf8(string_bytes.to_vec()).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("Invalid UTF-8 in string: {}", e))
            })
        }
        VariantBasicType::Primitive => {
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::String => {
                    // Long string format
                    if value.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for long string header".to_string(),
                        ));
                    }

                    let len = u32::from_le_bytes([value[1], value[2], value[3], value[4]]) as usize;
                    if value.len() < 5 + len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Buffer too short for long string: expected {} bytes",
                            5 + len
                        )));
                    }

                    // Extract the string bytes and convert to String
                    let string_bytes = &value[5..5 + len];
                    String::from_utf8(string_bytes.to_vec()).map_err(|e| {
                        ArrowError::InvalidArgumentError(format!("Invalid UTF-8 in string: {}", e))
                    })
                }
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Not a string value, primitive type: {:?}",
                    primitive_type
                ))),
            }
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not a string value, header: {:#x}",
            header
        ))),
    }
}

/// Decode an i32 value
pub fn decode_i32(value: &[u8]) -> Result<i32, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    // Parse header
    let header = value[0];

    // Check if it's a primitive type and handle accordingly
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            // Handle small positive integers (0, 1, 2, 3)
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::Int8 => {
                    if value.len() < 2 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int8".to_string(),
                        ));
                    }
                    Ok(value[1] as i8 as i32)
                }
                VariantPrimitiveType::Int16 => {
                    if value.len() < 3 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int16".to_string(),
                        ));
                    }
                    Ok(i16::from_le_bytes([value[1], value[2]]) as i32)
                }
                VariantPrimitiveType::Int32 => {
                    if value.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int32".to_string(),
                        ));
                    }
                    Ok(i32::from_le_bytes([value[1], value[2], value[3], value[4]]))
                }
                VariantPrimitiveType::Int64 => {
                    if value.len() < 9 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int64".to_string(),
                        ));
                    }
                    let v = i64::from_le_bytes([
                        value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                        value[8],
                    ]);
                    // Check if the i64 value can fit into an i32
                    if v > i32::MAX as i64 || v < i32::MIN as i64 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "i64 value {} is out of range for i32",
                            v
                        )));
                    }
                    Ok(v as i32)
                }
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Not an integer value, primitive type: {:?}",
                    primitive_type
                ))),
            }
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not an integer value, header: {:#x}",
            header
        ))),
    }
}

/// Decode an i64 value
pub fn decode_i64(value: &[u8]) -> Result<i64, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    // Parse header
    let header = value[0];

    // Check if it's a primitive type and handle accordingly
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            // Handle small positive integers (0, 1, 2, 3)
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::Int8 => {
                    if value.len() < 2 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int8".to_string(),
                        ));
                    }
                    Ok(value[1] as i8 as i64)
                }
                VariantPrimitiveType::Int16 => {
                    if value.len() < 3 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int16".to_string(),
                        ));
                    }
                    Ok(i16::from_le_bytes([value[1], value[2]]) as i64)
                }
                VariantPrimitiveType::Int32 => {
                    if value.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int32".to_string(),
                        ));
                    }
                    Ok(i32::from_le_bytes([value[1], value[2], value[3], value[4]]) as i64)
                }
                VariantPrimitiveType::Int64 => {
                    if value.len() < 9 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int64".to_string(),
                        ));
                    }
                    Ok(i64::from_le_bytes([
                        value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                        value[8],
                    ]))
                }
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Not an integer value, primitive type: {:?}",
                    primitive_type
                ))),
            }
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not an integer value, header: {:#x}",
            header
        ))),
    }
}

/// Decode a boolean value
pub fn decode_bool(value: &[u8]) -> Result<bool, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    // Parse header
    let header = value[0];

    // Check if it's a primitive type and handle accordingly
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::BooleanTrue => Ok(true),
                VariantPrimitiveType::BooleanFalse => Ok(false),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Not a boolean value, primitive type: {:?}",
                    primitive_type
                ))),
            }
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not a boolean value, header: {:#x}",
            header
        ))),
    }
}

/// Decode a double (f64) value
pub fn decode_f64(value: &[u8]) -> Result<f64, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    // Parse header
    let header = value[0];

    // Check if it's a primitive type and handle accordingly
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::Double => {
                    if value.len() < 9 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for double".to_string(),
                        ));
                    }
                    let bytes = [
                        value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                        value[8],
                    ];
                    Ok(f64::from_le_bytes(bytes))
                }
                VariantPrimitiveType::Float => {
                    if value.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for float".to_string(),
                        ));
                    }
                    let bytes = [value[1], value[2], value[3], value[4]];
                    Ok(f32::from_le_bytes(bytes) as f64)
                }
                // Also handle integers
                VariantPrimitiveType::Int8 => {
                    if value.len() < 2 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int8".to_string(),
                        ));
                    }
                    Ok((value[1] as i8) as f64)
                }
                VariantPrimitiveType::Int16 => {
                    if value.len() < 3 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int16".to_string(),
                        ));
                    }
                    Ok(i16::from_le_bytes([value[1], value[2]]) as f64)
                }
                VariantPrimitiveType::Int32 => {
                    if value.len() < 5 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int32".to_string(),
                        ));
                    }
                    Ok(i32::from_le_bytes([value[1], value[2], value[3], value[4]]) as f64)
                }
                VariantPrimitiveType::Int64 => {
                    if value.len() < 9 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Buffer too short for int64".to_string(),
                        ));
                    }
                    Ok(i64::from_le_bytes([
                        value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                        value[8],
                    ]) as f64)
                }
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Not a double value, primitive type: {:?}",
                    primitive_type
                ))),
            }
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not a double value, header: {:#x}",
            header
        ))),
    }
}

/// Check if a value is null
pub fn is_null(value: &[u8]) -> Result<bool, ArrowError> {
    if value.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Empty value buffer".to_string(),
        ));
    }

    let header = value[0];

    // Check if it's a primitive type and handle accordingly
    match get_basic_type(header) {
        VariantBasicType::Primitive => {
            let primitive_type = get_primitive_type(header);
            match primitive_type {
                VariantPrimitiveType::Null => Ok(true),
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_null() -> Result<(), ArrowError> {
        // Test decoding a null value
        let null_result = decode_null()?;
        assert_eq!(null_result, Value::Null);
        Ok(())
    }

    #[test]
    fn test_primitive_decode() -> Result<(), ArrowError> {
        // Test decoding an int8
        let data = [42]; // Value 42
        let mut pos = 0;
        let result = decode_int8(&data, &mut pos)?;

        // Convert to i64 for comparison
        let expected = Value::Number(serde_json::Number::from(42i64));
        assert_eq!(result, expected);
        assert_eq!(pos, 1); // Should have advanced by 1 byte

        Ok(())
    }

    #[test]
    fn test_short_string_decoding() -> Result<(), ArrowError> {
        // Create a header byte for a short string of length 5
        // Short string has basic type 1 and length in the upper 6 bits
        let header = 0x01 | (5 << 2); // 0x15

        // Create the test data with header and "Hello" bytes
        let mut data = vec![header];
        data.extend_from_slice(b"Hello");

        let mut pos = 0;
        let result = decode_value_internal(&data, &mut pos, &[])?;

        assert_eq!(result, Value::String("Hello".to_string()));
        assert_eq!(pos, 6); // Header (1) + string length (5)

        Ok(())
    }
}
