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
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;
use std::io::Write;

use crate::variant::{Variant, VariantList, VariantObject};

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
pub fn variant_to_json(json_buffer: &mut impl Write, variant: &Variant) -> Result<(), ArrowError> {
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
            let json_str = serde_json::to_string(&base64_str).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e))
            })?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::String(s) | Variant::ShortString(s) => {
            // Use serde_json to properly escape the string
            let json_str = serde_json::to_string(s).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e))
            })?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::Object(obj) => {
            convert_object_to_json(json_buffer, obj)?;
        }
        Variant::List(arr) => {
            convert_array_to_json(json_buffer, arr)?;
        }
    }
    Ok(())
}

/// Convert object fields to JSON
fn convert_object_to_json(buffer: &mut impl Write, obj: &VariantObject) -> Result<(), ArrowError> {
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
        let json_key = serde_json::to_string(key).map_err(|e| {
            ArrowError::InvalidArgumentError(format!("JSON key encoding error: {}", e))
        })?;
        write!(buffer, "{}:", json_key)?;

        // Recursively convert the value
        variant_to_json(buffer, &value)?;
    }

    write!(buffer, "}}")?;
    Ok(())
}

/// Convert array elements to JSON
fn convert_array_to_json(buffer: &mut impl Write, arr: &VariantList) -> Result<(), ArrowError> {
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
        Variant::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid float value".to_string())),
        Variant::Double(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid double value".to_string())),
        Variant::Decimal4 { integer, scale } => {
            let divisor = 10_i32.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid decimal value".to_string())
                })
        }
        Variant::Decimal8 { integer, scale } => {
            let divisor = 10_i64.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid decimal value".to_string())
                })
        }
        Variant::Decimal16 { integer, scale } => {
            let divisor = 10_i128.pow(*scale as u32);
            let decimal_value = *integer as f64 / divisor as f64;
            serde_json::Number::from_f64(decimal_value)
                .map(Value::Number)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid decimal value".to_string())
                })
        }
        Variant::Date(date) => Ok(Value::String(date.format("%Y-%m-%d").to_string())),
        Variant::TimestampMicros(ts) => Ok(Value::String(ts.to_rfc3339())),
        Variant::TimestampNtzMicros(ts) => Ok(Value::String(
            ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
        )),
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
        Variant::List(arr) => {
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
    use chrono::{DateTime, NaiveDate, Utc};

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
    fn test_int16_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Int16(32767);
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "32767");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Number(32767.into()));

        // Test negative value
        let negative_variant = Variant::Int16(-32768);
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-32768");
        Ok(())
    }

    #[test]
    fn test_int32_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Int32(2147483647);
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "2147483647");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Number(2147483647.into()));

        // Test negative value
        let negative_variant = Variant::Int32(-2147483648);
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-2147483648");
        Ok(())
    }

    #[test]
    fn test_int64_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Int64(9223372036854775807);
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "9223372036854775807");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Number(9223372036854775807i64.into()));

        // Test negative value
        let negative_variant = Variant::Int64(-9223372036854775808);
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-9223372036854775808");
        Ok(())
    }

    #[test]
    fn test_float_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Float(std::f32::consts::PI);
        let json = variant_to_json_string(&variant)?;
        assert!(json.starts_with("3.14159"));

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test zero
        let zero_variant = Variant::Float(0.0);
        let zero_json = variant_to_json_string(&zero_variant)?;
        assert_eq!(zero_json, "0");

        // Test negative
        let negative_variant = Variant::Float(-1.5);
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-1.5");
        Ok(())
    }

    #[test]
    fn test_double_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Double(std::f64::consts::E);
        let json = variant_to_json_string(&variant)?;
        assert!(json.starts_with("2.718281828459045"));

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test zero
        let zero_variant = Variant::Double(0.0);
        let zero_json = variant_to_json_string(&zero_variant)?;
        assert_eq!(zero_json, "0");

        // Test negative
        let negative_variant = Variant::Double(-2.5);
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-2.5");
        Ok(())
    }

    #[test]
    fn test_decimal4_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Decimal4 {
            integer: 12345,
            scale: 2,
        };
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "123.45");

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test zero scale
        let no_scale_variant = Variant::Decimal4 {
            integer: 42,
            scale: 0,
        };
        let no_scale_json = variant_to_json_string(&no_scale_variant)?;
        assert_eq!(no_scale_json, "42");

        // Test negative
        let negative_variant = Variant::Decimal4 {
            integer: -12345,
            scale: 3,
        };
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-12.345");
        Ok(())
    }

    #[test]
    fn test_decimal8_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Decimal8 {
            integer: 1234567890,
            scale: 3,
        };
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "1234567.89");

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test large scale
        let large_scale_variant = Variant::Decimal8 {
            integer: 123456789,
            scale: 6,
        };
        let large_scale_json = variant_to_json_string(&large_scale_variant)?;
        assert_eq!(large_scale_json, "123.456789");
        Ok(())
    }

    #[test]
    fn test_decimal16_to_json() -> Result<(), ArrowError> {
        let variant = Variant::Decimal16 {
            integer: 123456789012345,
            scale: 4,
        };
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "12345678901.2345");

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test very large number
        let large_variant = Variant::Decimal16 {
            integer: 999999999999999999,
            scale: 2,
        };
        let large_json = variant_to_json_string(&large_variant)?;
        // Due to f64 precision limits, very large numbers may lose precision
        assert!(
            large_json.starts_with("9999999999999999")
                || large_json.starts_with("10000000000000000")
        );
        Ok(())
    }

    #[test]
    fn test_date_to_json() -> Result<(), ArrowError> {
        let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
        let variant = Variant::Date(date);
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "\"2023-12-25\"");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::String("2023-12-25".to_string()));

        // Test leap year date
        let leap_date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let leap_variant = Variant::Date(leap_date);
        let leap_json = variant_to_json_string(&leap_variant)?;
        assert_eq!(leap_json, "\"2024-02-29\"");
        Ok(())
    }

    #[test]
    fn test_timestamp_micros_to_json() -> Result<(), ArrowError> {
        let timestamp = DateTime::parse_from_rfc3339("2023-12-25T10:30:45Z")
            .unwrap()
            .with_timezone(&Utc);
        let variant = Variant::TimestampMicros(timestamp);
        let json = variant_to_json_string(&variant)?;
        assert!(json.contains("2023-12-25T10:30:45"));
        assert!(json.starts_with('"') && json.ends_with('"'));

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_timestamp_ntz_micros_to_json() -> Result<(), ArrowError> {
        let naive_timestamp = DateTime::from_timestamp(1703505045, 123456)
            .unwrap()
            .naive_utc();
        let variant = Variant::TimestampNtzMicros(naive_timestamp);
        let json = variant_to_json_string(&variant)?;
        assert!(json.contains("2023-12-25"));
        assert!(json.starts_with('"') && json.ends_with('"'));

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_binary_to_json() -> Result<(), ArrowError> {
        let binary_data = b"Hello, World!";
        let variant = Variant::Binary(binary_data);
        let json = variant_to_json_string(&variant)?;

        // Should be base64 encoded and quoted
        assert!(json.starts_with('"') && json.ends_with('"'));
        assert!(json.len() > 2); // Should have content

        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::String(_)));

        // Test empty binary
        let empty_variant = Variant::Binary(b"");
        let empty_json = variant_to_json_string(&empty_variant)?;
        assert_eq!(empty_json, "\"\"");

        // Test binary with special bytes
        let special_variant = Variant::Binary(&[0, 255, 128, 64]);
        let special_json = variant_to_json_string(&special_variant)?;
        assert!(special_json.starts_with('"') && special_json.ends_with('"'));
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
        assert_eq!(
            json_value,
            Value::String("hello\nworld\t\"quoted\"".to_string())
        );
        Ok(())
    }

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

    #[test]
    fn test_comprehensive_type_coverage() -> Result<(), ArrowError> {
        // Test all supported types to ensure no compilation errors
        let test_variants = vec![
            Variant::Null,
            Variant::BooleanTrue,
            Variant::BooleanFalse,
            Variant::Int8(1),
            Variant::Int16(2),
            Variant::Int32(3),
            Variant::Int64(4),
            Variant::Float(5.0),
            Variant::Double(6.0),
            Variant::Decimal4 {
                integer: 7,
                scale: 0,
            },
            Variant::Decimal8 {
                integer: 8,
                scale: 0,
            },
            Variant::Decimal16 {
                integer: 9,
                scale: 0,
            },
            Variant::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
            Variant::TimestampMicros(
                DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            Variant::TimestampNtzMicros(DateTime::from_timestamp(0, 0).unwrap().naive_utc()),
            Variant::Binary(b"test"),
            Variant::String("test"),
            Variant::ShortString("test"),
        ];

        for variant in test_variants {
            // Ensure all types can be converted without panicking
            let _json_string = variant_to_json_string(&variant)?;
            let _json_value = variant_to_json_value(&variant)?;
        }

        Ok(())
    }

    #[test]
    fn test_simple_object_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        // Create a simple object with various field types
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("name", "Alice");
            obj.append_value("age", 30i32);
            obj.append_value("active", true);
            obj.append_value("score", 95.5f64);
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        // Parse the JSON to verify structure - handle JSON parsing errors manually
        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        if let Value::Object(obj) = parsed {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(obj.get("age"), Some(&Value::Number(30.into())));
            assert_eq!(obj.get("active"), Some(&Value::Bool(true)));
            assert!(matches!(obj.get("score"), Some(Value::Number(_))));
            assert_eq!(obj.len(), 4);
        } else {
            panic!("Expected JSON object");
        }

        // Test variant_to_json_value as well
        let json_value = variant_to_json_value(&variant)?;
        assert!(matches!(json_value, Value::Object(_)));

        Ok(())
    }

    #[test]
    fn test_empty_object_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let obj = builder.new_object();
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "{}");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Object(serde_json::Map::new()));

        Ok(())
    }

    #[test]
    fn test_object_with_special_characters_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("message", "Hello \"World\"\nWith\tTabs");
            obj.append_value("path", "C:\\Users\\Alice\\Documents");
            obj.append_value("unicode", "😀 Smiley");
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        // Verify that special characters are properly escaped
        assert!(json.contains("Hello \\\"World\\\"\\nWith\\tTabs"));
        assert!(json.contains("C:\\\\Users\\\\Alice\\\\Documents"));
        assert!(json.contains("😀 Smiley"));

        // Verify that the JSON can be parsed back
        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        assert!(matches!(parsed, Value::Object(_)));

        Ok(())
    }

    #[test]
    fn test_simple_list_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut list = builder.new_list();
            list.append_value(1i32);
            list.append_value(2i32);
            list.append_value(3i32);
            list.append_value(4i32);
            list.append_value(5i32);
            list.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "[1,2,3,4,5]");

        let json_value = variant_to_json_value(&variant)?;
        if let Value::Array(arr) = json_value {
            assert_eq!(arr.len(), 5);
            assert_eq!(arr[0], Value::Number(1.into()));
            assert_eq!(arr[4], Value::Number(5.into()));
        } else {
            panic!("Expected JSON array");
        }

        Ok(())
    }

    #[test]
    fn test_empty_list_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let list = builder.new_list();
            list.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;
        assert_eq!(json, "[]");

        let json_value = variant_to_json_value(&variant)?;
        assert_eq!(json_value, Value::Array(vec![]));

        Ok(())
    }

    #[test]
    fn test_mixed_type_list_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut list = builder.new_list();
            list.append_value("hello");
            list.append_value(42i32);
            list.append_value(true);
            list.append_value(()); // null
            list.append_value(std::f64::consts::PI);
            list.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        if let Value::Array(arr) = parsed {
            assert_eq!(arr.len(), 5);
            assert_eq!(arr[0], Value::String("hello".to_string()));
            assert_eq!(arr[1], Value::Number(42.into()));
            assert_eq!(arr[2], Value::Bool(true));
            assert_eq!(arr[3], Value::Null);
            assert!(matches!(arr[4], Value::Number(_)));
        } else {
            panic!("Expected JSON array");
        }

        Ok(())
    }

    #[test]
    fn test_object_field_ordering_in_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            // Add fields in non-alphabetical order
            obj.append_value("zebra", "last");
            obj.append_value("alpha", "first");
            obj.append_value("beta", "second");
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        // Parse and verify all fields are present
        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        if let Value::Object(obj) = parsed {
            assert_eq!(obj.len(), 3);
            assert_eq!(obj.get("alpha"), Some(&Value::String("first".to_string())));
            assert_eq!(obj.get("beta"), Some(&Value::String("second".to_string())));
            assert_eq!(obj.get("zebra"), Some(&Value::String("last".to_string())));
        } else {
            panic!("Expected JSON object");
        }

        Ok(())
    }

    #[test]
    fn test_list_with_various_primitive_types_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut list = builder.new_list();
            list.append_value("string_value");
            list.append_value(42i32);
            list.append_value(true);
            list.append_value(std::f64::consts::PI);
            list.append_value(false);
            list.append_value(()); // null
            list.append_value(100i64);
            list.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        if let Value::Array(arr) = parsed {
            assert_eq!(arr.len(), 7);
            assert_eq!(arr[0], Value::String("string_value".to_string()));
            assert_eq!(arr[1], Value::Number(42.into()));
            assert_eq!(arr[2], Value::Bool(true));
            assert!(matches!(arr[3], Value::Number(_))); // float
            assert_eq!(arr[4], Value::Bool(false));
            assert_eq!(arr[5], Value::Null);
            assert_eq!(arr[6], Value::Number(100.into()));
        } else {
            panic!("Expected JSON array");
        }

        Ok(())
    }

    #[test]
    fn test_object_with_various_primitive_types_to_json() -> Result<(), ArrowError> {
        use crate::builder::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("string_field", "test_string");
            obj.append_value("int_field", 123i32);
            obj.append_value("bool_field", true);
            obj.append_value("float_field", 2.71f64);
            obj.append_value("null_field", ());
            obj.append_value("long_field", 999i64);
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant_to_json_string(&variant)?;

        let parsed: Value = serde_json::from_str(&json)
            .map_err(|e| ArrowError::ParseError(format!("JSON parse error: {}", e)))?;
        if let Value::Object(obj) = parsed {
            assert_eq!(obj.len(), 6);
            assert_eq!(
                obj.get("string_field"),
                Some(&Value::String("test_string".to_string()))
            );
            assert_eq!(obj.get("int_field"), Some(&Value::Number(123.into())));
            assert_eq!(obj.get("bool_field"), Some(&Value::Bool(true)));
            assert!(matches!(obj.get("float_field"), Some(Value::Number(_))));
            assert_eq!(obj.get("null_field"), Some(&Value::Null));
            assert_eq!(obj.get("long_field"), Some(&Value::Number(999.into())));
        } else {
            panic!("Expected JSON object");
        }

        Ok(())
    }
}
