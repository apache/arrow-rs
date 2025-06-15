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

//! Module for converting Variant data to JSON format

use arrow_schema::ArrowError;
use base64::{Engine as _, engine::general_purpose};
use serde_json::Value;
use std::io::Write;

use crate::variant::{Variant, VariantArray, VariantObject};

/// Converts a Variant to JSON and writes it to the provided buffer
///
/// # Arguments
///
/// * `json_buffer` - Writer to output JSON to
/// * `variant` - The Variant value to convert
///
/// # Returns
///
/// * `Ok(())` if successful
/// * `Err` with error details if conversion fails
///
/// # Example
///
/// ```rust
/// use parquet_variant::{Variant, variant_to_json};
/// use arrow_schema::ArrowError;
/// 
/// fn example() -> Result<(), ArrowError> {
///     let variant = Variant::Int8(42);
///     let mut buffer = Vec::new();
///     variant_to_json(&mut buffer, &variant)?;
///     assert_eq!(String::from_utf8(buffer).unwrap(), "42");
///     Ok(())
/// }
/// example().unwrap();
/// ```
pub fn variant_to_json<W: Write>(
    json_buffer: &mut W,
    variant: &Variant,
) -> Result<(), ArrowError> {
    match variant {
        Variant::Null => {
            write!(json_buffer, "null")?;
        }
        Variant::BooleanTrue => {
            write!(json_buffer, "true")?;
        }
        Variant::BooleanFalse => {
            write!(json_buffer, "false")?;
        }
        Variant::Int8(i) => {
            write!(json_buffer, "{}", i)?;
        }
        Variant::Int16(i) => {
            write!(json_buffer, "{}", i)?;
        }
        Variant::Int32(i) => {
            write!(json_buffer, "{}", i)?;
        }
        Variant::Int64(i) => {
            write!(json_buffer, "{}", i)?;
        }
        Variant::Float(f) => {
            write!(json_buffer, "{}", f)?;
        }
        Variant::Double(f) => {
            write!(json_buffer, "{}", f)?;
        }
        Variant::Decimal4 { integer, scale } => {
            // Convert decimal to string representation
            let divisor = 10_i32.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            write!(json_buffer, "{}", decimal_value)?;
        }
        Variant::Decimal8 { integer, scale } => {
            // Convert decimal to string representation
            let divisor = 10_i64.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            write!(json_buffer, "{}", decimal_value)?;
        }
        Variant::Decimal16 { integer, scale } => {
            // Convert decimal to string representation
            let divisor = 10_i128.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            write!(json_buffer, "{}", decimal_value)?;
        }
        Variant::Date(date) => {
            write!(json_buffer, "\"{}\"", date.format("%Y-%m-%d"))?;
        }
        Variant::TimestampMicros(ts) => {
            write!(json_buffer, "\"{}\"", ts.to_rfc3339())?;
        }
        Variant::TimestampNtzMicros(ts) => {
            write!(json_buffer, "\"{}\"", ts.format("%Y-%m-%dT%H:%M:%S%.6f"))?;
        }
        Variant::Binary(bytes) => {
            // Encode binary as base64 string
            let base64_str = general_purpose::STANDARD.encode(bytes);
            let json_str = serde_json::to_string(&base64_str)
                .map_err(|e| ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e)))?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::String(s) | Variant::ShortString(s) => {
            // Use serde_json to properly escape the string
            let json_str = serde_json::to_string(s)
                .map_err(|e| ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e)))?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::Object(obj) => {
            convert_object_to_json(json_buffer, obj)?;
        }
        Variant::Array(arr) => {
            convert_array_to_json(json_buffer, arr)?;
        }
    }
    Ok(())
}

/// Convert object fields to JSON
fn convert_object_to_json<W: Write>(
    buffer: &mut W,
    obj: &VariantObject,
) -> Result<(), ArrowError> {
    write!(buffer, "{{")?;
    
    // Get all fields from the object
    let fields = obj.fields()?;
    let mut first = true;
    
    for (key, value) in fields {
        if !first {
            write!(buffer, ",")?;
        }
        first = false;
        
        // Write the key (properly escaped)
        let json_key = serde_json::to_string(key)
            .map_err(|e| ArrowError::InvalidArgumentError(format!("JSON key encoding error: {}", e)))?;
        write!(buffer, "{}:", json_key)?;
        
        // Recursively convert the value
        variant_to_json(buffer, &value)?;
    }
    
    write!(buffer, "}}")?;
    Ok(())
}

/// Convert array elements to JSON
fn convert_array_to_json<W: Write>(
    buffer: &mut W,
    arr: &VariantArray,
) -> Result<(), ArrowError> {
    write!(buffer, "[")?;
    
    let len = arr.len();
    for i in 0..len {
        if i > 0 {
            write!(buffer, ",")?;
        }
        
        let element = arr.get(i)?;
        variant_to_json(buffer, &element)?;
    }
    
    write!(buffer, "]")?;
    Ok(())
}

/// Convert Variant to JSON string
///
/// # Arguments
///
/// * `variant` - The Variant value to convert
///
/// # Returns
///
/// * `Ok(String)` containing the JSON representation
/// * `Err` with error details if conversion fails
///
/// # Example
///
/// ```rust
/// use parquet_variant::{Variant, variant_to_json_string};
/// use arrow_schema::ArrowError;
/// 
/// fn example() -> Result<(), ArrowError> {
///     let variant = Variant::String("hello");
///     let json = variant_to_json_string(&variant)?;
///     assert_eq!(json, "\"hello\"");
///     Ok(())
/// }
/// example().unwrap();
/// ```
pub fn variant_to_json_string(variant: &Variant) -> Result<String, ArrowError> {
    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, variant)?;
    String::from_utf8(buffer)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("UTF-8 conversion error: {}", e)))
}

/// Convert Variant to serde_json::Value
///
/// # Arguments
///
/// * `variant` - The Variant value to convert
///
/// # Returns
///
/// * `Ok(Value)` containing the JSON value
/// * `Err` with error details if conversion fails
///
/// # Example
///
/// ```rust
/// use parquet_variant::{Variant, variant_to_json_value};
/// use serde_json::Value;
/// use arrow_schema::ArrowError;
/// 
/// fn example() -> Result<(), ArrowError> {
///     let variant = Variant::Int8(42);
///     let json_value = variant_to_json_value(&variant)?;
///     assert_eq!(json_value, Value::Number(42.into()));
///     Ok(())
/// }
/// example().unwrap();
/// ```
pub fn variant_to_json_value(variant: &Variant) -> Result<Value, ArrowError> {
    match variant {
        Variant::Null => Ok(Value::Null),
        Variant::BooleanTrue => Ok(Value::Bool(true)),
        Variant::BooleanFalse => Ok(Value::Bool(false)),
        Variant::Int8(i) => Ok(Value::Number((*i).into())),
        Variant::Int16(i) => Ok(Value::Number((*i).into())),
        Variant::Int32(i) => Ok(Value::Number((*i).into())),
        Variant::Int64(i) => Ok(Value::Number((*i).into())),
        Variant::Float(f) => {
            serde_json::Number::from_f64(*f as f64)
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid float value".to_string()))
        }
        Variant::Double(f) => {
            serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid double value".to_string()))
        }
        Variant::Decimal4 { integer, scale } => {
            let divisor = 10_i32.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid decimal value".to_string()))
        }
        Variant::Decimal8 { integer, scale } => {
            let divisor = 10_i64.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid decimal value".to_string()))
        }
        Variant::Decimal16 { integer, scale } => {
            let divisor = 10_i128.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid decimal value".to_string()))
        }
        Variant::Date(date) => Ok(Value::String(date.format("%Y-%m-%d").to_string())),
        Variant::TimestampMicros(ts) => Ok(Value::String(ts.to_rfc3339())),
        Variant::TimestampNtzMicros(ts) => Ok(Value::String(ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())),
        Variant::Binary(bytes) => Ok(Value::String(general_purpose::STANDARD.encode(bytes))),
        Variant::String(s) | Variant::ShortString(s) => Ok(Value::String(s.to_string())),
        Variant::Object(obj) => {
            let mut map = serde_json::Map::new();
            let fields = obj.fields()?;
            
            for (key, value) in fields {
                let json_value = variant_to_json_value(&value)?;
                map.insert(key.to_string(), json_value);
            }
            
            Ok(Value::Object(map))
        }
        Variant::Array(arr) => {
            let mut vec = Vec::new();
            let len = arr.len();
            
            for i in 0..len {
                let element = arr.get(i)?;
                let json_value = variant_to_json_value(&element)?;
                vec.push(json_value);
            }
            
            Ok(Value::Array(vec))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Variant;

    #[test]
    fn test_null_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Null;
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "null");
        
        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Null);
        Ok(())
    }

    #[test]
    fn test_boolean_to_json() -> Result<(), ArrowError> {
        let variant_true = Variant::BooleanTrue;
        let json_true = variant_to_json_string(&variant_true)?;
        assert_eq!(json_true, "true");
        
        let variant_false = Variant::BooleanFalse;
        let json_false = variant_to_json_string(&variant_false)?;
        assert_eq!(json_false, "false");
        
        let json_value_true = variant_to_json_value(&variant_true)?;
        assert_eq!(json_value_true, Value::Bool(true));
        
        let json_value_false = variant_to_json_value(&variant_false)?;
        assert_eq!(json_value_false, Value::Bool(false));
        Ok(())
    }

    #[test]
    fn test_int8_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Int8(42);
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "42");
        
        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Number(42.into()));
        Ok(())
    }

    #[test]
    fn test_string_to_json() -> Result<(), ArrowError> {
        let variant = Variant::String("hello world");
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "\"hello world\"");
        
        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::String("hello world".to_string()));
        Ok(())
    }

    #[test]
    fn test_short_string_to_json() -> Result<(), ArrowError> {
        let variant = Variant::ShortString("short");
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "\"short\"");
        
        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::String("short".to_string()));
        Ok(())
    }

    #[test]
    fn test_string_escaping() -> Result<(), ArrowError> {
        let variant = Variant::String("hello\nworld\t\"quoted\"");
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "\"hello\\nworld\\t\\\"quoted\\\"\"");
        
        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::String("hello\nworld\t\"quoted\"".to_string()));
        Ok(())
    }

    // TODO: Add tests for objects and arrays once the implementation is complete
    // These will be added in the next steps when we implement the missing methods
    // in VariantObject and VariantArray

    #[test]
    fn test_json_buffer_writing() -> Result<(), ArrowError> {
        let variant = Variant::Int8(123);
        let mut buffer = Vec::new();
        variant_to_json(&mut buffer, &variant)?;
        
        let result = String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        assert_eq!(result, "123");
        Ok(())
    }
} 