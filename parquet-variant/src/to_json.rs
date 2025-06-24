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

// Format string constants to avoid duplication and reduce errors
const DATE_FORMAT: &str = "%Y-%m-%d";
const TIMESTAMP_NTZ_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";

// Helper functions for consistent formatting
fn format_date_string(date: &chrono::NaiveDate) -> String {
    date.format(DATE_FORMAT).to_string()
}

fn format_timestamp_ntz_string(ts: &chrono::NaiveDateTime) -> String {
    ts.format(TIMESTAMP_NTZ_FORMAT).to_string()
}

fn format_binary_base64(bytes: &[u8]) -> String {
    general_purpose::STANDARD.encode(bytes)
}

/// Converts a Variant to JSON and writes it to the provided `Write`
///
/// This function writes JSON directly to any type that implements [`Write`](std::io::Write),
/// making it efficient for streaming or when you want to control the output destination.
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
/// # Examples
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json};
/// # use arrow_schema::ArrowError;
/// let variant = Variant::Int32(42);
/// let mut buffer = Vec::new();
/// variant_to_json(&mut buffer, &variant)?;
/// assert_eq!(String::from_utf8(buffer).unwrap(), "42");
/// # Ok::<(), ArrowError>(())
/// ```
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json};
/// # use arrow_schema::ArrowError;
/// let variant = Variant::String("Hello, World!");
/// let mut buffer = Vec::new();
/// variant_to_json(&mut buffer, &variant)?;
/// assert_eq!(String::from_utf8(buffer).unwrap(), "\"Hello, World!\"");
/// # Ok::<(), ArrowError>(())
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
            // Convert decimal to string representation using integer arithmetic
            if *scale == 0 {
                write!(json_buffer, "{}", integer)?;
            } else {
                let divisor = 10_i32.pow(*scale as u32);
                let quotient = integer / divisor;
                let remainder = (integer % divisor).abs();
                let formatted_remainder = format!("{:0width$}", remainder, width = *scale as usize);
                let trimmed_remainder = formatted_remainder.trim_end_matches('0');
                if trimmed_remainder.is_empty() {
                    write!(json_buffer, "{}", quotient)?;
                } else {
                    write!(json_buffer, "{}.{}", quotient, trimmed_remainder)?;
                }
            }
        }
        Variant::Decimal8 { integer, scale } => {
            // Convert decimal to string representation using integer arithmetic
            if *scale == 0 {
                write!(json_buffer, "{}", integer)?;
            } else {
                let divisor = 10_i64.pow(*scale as u32);
                let quotient = integer / divisor;
                let remainder = (integer % divisor).abs();
                let formatted_remainder = format!("{:0width$}", remainder, width = *scale as usize);
                let trimmed_remainder = formatted_remainder.trim_end_matches('0');
                if trimmed_remainder.is_empty() {
                    write!(json_buffer, "{}", quotient)?;
                } else {
                    write!(json_buffer, "{}.{}", quotient, trimmed_remainder)?;
                }
            }
        }
        Variant::Decimal16 { integer, scale } => {
            // Convert decimal to string representation using integer arithmetic
            if *scale == 0 {
                write!(json_buffer, "{}", integer)?;
            } else {
                let divisor = 10_i128.pow(*scale as u32);
                let quotient = integer / divisor;
                let remainder = (integer % divisor).abs();
                let formatted_remainder = format!("{:0width$}", remainder, width = *scale as usize);
                let trimmed_remainder = formatted_remainder.trim_end_matches('0');
                if trimmed_remainder.is_empty() {
                    write!(json_buffer, "{}", quotient)?;
                } else {
                    write!(json_buffer, "{}.{}", quotient, trimmed_remainder)?;
                }
            }
        }
        Variant::Date(date) => {
            write!(json_buffer, "\"{}\"", format_date_string(date))?;
        }
        Variant::TimestampMicros(ts) => {
            write!(json_buffer, "\"{}\"", ts.to_rfc3339())?;
        }
        Variant::TimestampNtzMicros(ts) => {
            write!(json_buffer, "\"{}\"", format_timestamp_ntz_string(ts))?;
        }
        Variant::Binary(bytes) => {
            // Encode binary as base64 string
            let base64_str = format_binary_base64(bytes);
            let json_str = serde_json::to_string(&base64_str).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e))
            })?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::String(s) => {
            // Use serde_json to properly escape the string
            let json_str = serde_json::to_string(s).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {}", e))
            })?;
            write!(json_buffer, "{}", json_str)?;
        }
        Variant::ShortString(s) => {
            // Use serde_json to properly escape the string
            let json_str = serde_json::to_string(s.as_str()).map_err(|e| {
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
    let mut first = true;

    for (key, value) in obj.iter() {
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
/// This is a convenience function that converts a Variant to a JSON string.
/// This is the same as calling variant_to_json with a Vec
/// It's the simplest way to get a JSON representation when you just need a String result.
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
/// # Examples
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json_string};
/// # use arrow_schema::ArrowError;
/// let variant = Variant::Int32(42);
/// let json = variant_to_json_string(&variant)?;
/// assert_eq!(json, "42");
/// # Ok::<(), ArrowError>(())
/// ```
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json_string};
/// # use arrow_schema::ArrowError;
/// let variant = Variant::String("Hello, World!");
/// let json = variant_to_json_string(&variant)?;
/// assert_eq!(json, "\"Hello, World!\"");
/// # Ok::<(), ArrowError>(())
/// ```
///
/// # Example: Create a [`Variant::Object`] and convert to JSON
/// 
/// This example shows how to create an object with two fields and convert it to JSON:
/// ```json
/// {
///   "first_name": "Jiaying",
///   "last_name": "Li"
/// }
/// ```
/// 
/// ```rust
/// # use parquet_variant::{Variant, VariantBuilder, variant_to_json_string};
/// # use arrow_schema::ArrowError;
/// let mut builder = VariantBuilder::new();
/// // Create an object builder that will write fields to the object
/// let mut object_builder = builder.new_object();
/// object_builder.append_value("first_name", "Jiaying");
/// object_builder.append_value("last_name", "Li");
/// object_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // Create the Variant and convert to JSON
/// let variant = Variant::try_new(&metadata, &value)?;
/// let json = variant_to_json_string(&variant)?;
/// assert!(json.contains("\"first_name\":\"Jiaying\""));
/// assert!(json.contains("\"last_name\":\"Li\""));
/// # Ok::<(), ArrowError>(())
/// ```
pub fn variant_to_json_string(variant: &Variant) -> Result<String, ArrowError> {
    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, variant)?;
    String::from_utf8(buffer)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("UTF-8 conversion error: {}", e)))
}

/// Convert Variant to serde_json::Value
///
/// This function converts a Variant to a [`serde_json::Value`], which is useful
/// when you need to work with the JSON data programmatically or integrate with
/// other serde-based JSON processing.
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
/// # Examples
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json_value};
/// # use serde_json::Value;
/// # use arrow_schema::ArrowError;
/// let variant = Variant::Int32(42);
/// let json_value = variant_to_json_value(&variant)?;
/// assert_eq!(json_value, Value::Number(42.into()));
/// # Ok::<(), ArrowError>(())
/// ```
///
/// ```rust
/// # use parquet_variant::{Variant, variant_to_json_value};
/// # use serde_json::Value;
/// # use arrow_schema::ArrowError;
/// let variant = Variant::String("hello");
/// let json_value = variant_to_json_value(&variant)?;
/// assert_eq!(json_value, Value::String("hello".to_string()));
/// # Ok::<(), ArrowError>(())
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
        Variant::Date(date) => Ok(Value::String(format_date_string(date))),
        Variant::TimestampMicros(ts) => Ok(Value::String(ts.to_rfc3339())),
        Variant::TimestampNtzMicros(ts) => Ok(Value::String(format_timestamp_ntz_string(ts))),
        Variant::Binary(bytes) => Ok(Value::String(format_binary_base64(bytes))),
        Variant::String(s) => Ok(Value::String(s.to_string())),
        Variant::ShortString(s) => Ok(Value::String(s.to_string())),
        Variant::Object(obj) => {
            let mut map = serde_json::Map::new();

            for (key, value) in obj.iter() {
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
    fn test_decimal_edge_cases() -> Result<(), ArrowError> {
        // Test negative decimal
        let negative_variant = Variant::Decimal4 {
            integer: -12345,
            scale: 3,
        };
        let negative_json = variant_to_json_string(&negative_variant)?;
        assert_eq!(negative_json, "-12.345");
        
        // Test large scale decimal  
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
        use crate::variant::ShortString;
        let short_string = ShortString::try_new("short")?;
        let variant = Variant::ShortString(short_string);
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

    /// Reusable test structure for JSON conversion testing
    struct JsonTest {
        variant: Variant<'static, 'static>,
        expected_json: &'static str,
        expected_value: Value,
    }

    impl JsonTest {
        fn run(self) {
            let json_string = variant_to_json_string(&self.variant)
                .expect("variant_to_json_string should succeed");
            assert_eq!(json_string, self.expected_json, 
                "JSON string mismatch for variant: {:?}", self.variant);
            
            let json_value = variant_to_json_value(&self.variant)
                .expect("variant_to_json_value should succeed");
            
            // For floating point numbers, we need special comparison due to JSON number representation
            match (&json_value, &self.expected_value) {
                (Value::Number(actual), Value::Number(expected)) => {
                    let actual_f64 = actual.as_f64().unwrap_or(0.0);
                    let expected_f64 = expected.as_f64().unwrap_or(0.0);
                    assert!((actual_f64 - expected_f64).abs() < f64::EPSILON,
                        "JSON value mismatch for variant: {:?}, got {}, expected {}", 
                        self.variant, actual_f64, expected_f64);
                }
                _ => {
                    assert_eq!(json_value, self.expected_value, 
                        "JSON value mismatch for variant: {:?}", self.variant);
                }
            }
            
            // Verify roundtrip: JSON string should parse to same value
            let parsed: Value = serde_json::from_str(&json_string)
                .expect("Generated JSON should be valid");
            // Same floating point handling for roundtrip
            match (&parsed, &self.expected_value) {
                (Value::Number(actual), Value::Number(expected)) => {
                    let actual_f64 = actual.as_f64().unwrap_or(0.0);
                    let expected_f64 = expected.as_f64().unwrap_or(0.0);
                    assert!((actual_f64 - expected_f64).abs() < f64::EPSILON,
                        "Parsed JSON mismatch for variant: {:?}, got {}, expected {}", 
                        self.variant, actual_f64, expected_f64);
                }
                _ => {
                    assert_eq!(parsed, self.expected_value, 
                        "Parsed JSON mismatch for variant: {:?}", self.variant);
                }
            }
        }
    }

    #[test]
    fn test_primitive_json_conversion() {
        use crate::variant::ShortString;
        
        // Null
        JsonTest {
            variant: Variant::Null,
            expected_json: "null",
            expected_value: Value::Null,
        }.run();
        
        // Booleans
        JsonTest {
            variant: Variant::BooleanTrue,
            expected_json: "true", 
            expected_value: Value::Bool(true),
        }.run();
        
        JsonTest {
            variant: Variant::BooleanFalse,
            expected_json: "false",
            expected_value: Value::Bool(false),
        }.run();
        
        // Integers - positive and negative edge cases
        JsonTest {
            variant: Variant::Int8(42),
            expected_json: "42",
            expected_value: Value::Number(42.into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int8(-128),
            expected_json: "-128",
            expected_value: Value::Number((-128).into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int16(32767),
            expected_json: "32767",
            expected_value: Value::Number(32767.into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int16(-32768),
            expected_json: "-32768",
            expected_value: Value::Number((-32768).into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int32(2147483647),
            expected_json: "2147483647",
            expected_value: Value::Number(2147483647.into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int32(-2147483648),
            expected_json: "-2147483648",
            expected_value: Value::Number((-2147483648).into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int64(9223372036854775807),
            expected_json: "9223372036854775807",
            expected_value: Value::Number(9223372036854775807i64.into()),
        }.run();
        
        JsonTest {
            variant: Variant::Int64(-9223372036854775808),
            expected_json: "-9223372036854775808",
            expected_value: Value::Number((-9223372036854775808i64).into()),
        }.run();
        
        // Floats
        JsonTest {
            variant: Variant::Float(3.5),
            expected_json: "3.5",
            expected_value: serde_json::Number::from_f64(3.5).map(Value::Number).unwrap(),
        }.run();
        
        JsonTest {
            variant: Variant::Float(0.0),
            expected_json: "0",
            expected_value: Value::Number(0.into()), // Use integer 0 to match JSON parsing
        }.run();
        
        JsonTest {
            variant: Variant::Float(-1.5),
            expected_json: "-1.5",
            expected_value: serde_json::Number::from_f64(-1.5).map(Value::Number).unwrap(),
        }.run();
        
        JsonTest {
            variant: Variant::Double(2.718281828459045),
            expected_json: "2.718281828459045",
            expected_value: serde_json::Number::from_f64(2.718281828459045).map(Value::Number).unwrap(),
        }.run();
        
        // Decimals
        JsonTest {
            variant: Variant::Decimal4 { integer: 12345, scale: 2 },
            expected_json: "123.45",
            expected_value: serde_json::Number::from_f64(123.45).map(Value::Number).unwrap(),
        }.run();
        
        JsonTest {
            variant: Variant::Decimal4 { integer: 42, scale: 0 },
            expected_json: "42",
            expected_value: serde_json::Number::from_f64(42.0).map(Value::Number).unwrap(),
        }.run();
        
        JsonTest {
            variant: Variant::Decimal8 { integer: 1234567890, scale: 3 },
            expected_json: "1234567.89",
            expected_value: serde_json::Number::from_f64(1234567.89).map(Value::Number).unwrap(),
        }.run();
        
        JsonTest {
            variant: Variant::Decimal16 { integer: 123456789012345, scale: 4 },
            expected_json: "12345678901.2345",
            expected_value: serde_json::Number::from_f64(12345678901.2345).map(Value::Number).unwrap(),
        }.run();
        
        // Strings
        JsonTest {
            variant: Variant::String("hello world"),
            expected_json: "\"hello world\"",
            expected_value: Value::String("hello world".to_string()),
        }.run();
        
        JsonTest {
            variant: Variant::String(""),
            expected_json: "\"\"",
            expected_value: Value::String("".to_string()),
        }.run();
        
        JsonTest {
            variant: Variant::ShortString(ShortString::try_new("test").unwrap()),
            expected_json: "\"test\"",
            expected_value: Value::String("test".to_string()),
        }.run();
        
        // Date and timestamps
        JsonTest {
            variant: Variant::Date(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap()),
            expected_json: "\"2023-12-25\"",
            expected_value: Value::String("2023-12-25".to_string()),
        }.run();
        
        // Binary data (base64 encoded)
        JsonTest {
            variant: Variant::Binary(b"test"),
            expected_json: "\"dGVzdA==\"", // base64 encoded "test"
            expected_value: Value::String("dGVzdA==".to_string()),
        }.run();
        
        JsonTest {
            variant: Variant::Binary(b""),
            expected_json: "\"\"", // empty base64
            expected_value: Value::String("".to_string()),
        }.run();
        
        JsonTest {
            variant: Variant::Binary(b"binary data"),
            expected_json: "\"YmluYXJ5IGRhdGE=\"", // base64 encoded "binary data"
            expected_value: Value::String("YmluYXJ5IGRhdGE=".to_string()),
        }.run();
    }

    #[test]
    fn test_string_escaping_comprehensive() {
        // Test comprehensive string escaping scenarios
        JsonTest {
            variant: Variant::String("line1\nline2\ttab\"quote\"\\backslash"),
            expected_json: "\"line1\\nline2\\ttab\\\"quote\\\"\\\\backslash\"",
            expected_value: Value::String("line1\nline2\ttab\"quote\"\\backslash".to_string()),
        }.run();
        
        JsonTest {
            variant: Variant::String("Hello 世界 🌍"),
            expected_json: "\"Hello 世界 🌍\"",
            expected_value: Value::String("Hello 世界 🌍".to_string()),
        }.run();
    }

    #[test]
    fn test_buffer_writing_variants() -> Result<(), ArrowError> {
        use crate::variant_to_json;
        
        let variant = Variant::String("test buffer writing");
        
        // Test writing to a Vec<u8>
        let mut buffer = Vec::new();
        variant_to_json(&mut buffer, &variant)?;
        let result = String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        assert_eq!(result, "\"test buffer writing\"");
        
        // Test writing to vec![]
        let mut buffer = vec![];
        variant_to_json(&mut buffer, &variant)?;
        let result = String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        assert_eq!(result, "\"test buffer writing\"");
        
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
