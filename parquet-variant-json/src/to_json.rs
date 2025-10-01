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
use chrono::Timelike;
use parquet_variant::{Variant, VariantList, VariantObject};
use serde_json::Value;
use std::io::Write;

/// Extension trait for converting Variants to JSON
pub trait VariantToJson {
    ///
    /// This function writes JSON directly to any type that implements [`Write`],
    /// making it efficient for streaming or when you want to control the output destination.
    ///
    /// See [`VariantToJson::to_json_string`] for a convenience function that returns a
    /// JSON string.
    ///
    /// # Arguments
    ///
    /// * `writer` - Writer to output JSON to
    /// * `variant` - The Variant value to convert
    ///
    /// # Returns
    ///
    /// * `Ok(())` if successful
    /// * `Err` with error details if conversion fails
    ///
    /// # Examples
    ///
    ///
    /// ```rust
    /// # use parquet_variant::{Variant};
    /// # use parquet_variant_json::VariantToJson;
    /// # use arrow_schema::ArrowError;
    /// let variant = Variant::from("Hello, World!");
    /// let mut buffer = Vec::new();
    /// variant.to_json(&mut buffer)?;
    /// assert_eq!(String::from_utf8(buffer).unwrap(), "\"Hello, World!\"");
    /// # Ok::<(), ArrowError>(())
    /// ```
    ///
    /// # Example: Create a [`Variant::Object`] and convert to JSON
    /// ```rust
    /// # use parquet_variant::{Variant, VariantBuilder};
    /// # use parquet_variant_json::VariantToJson;
    /// # use arrow_schema::ArrowError;
    /// let mut builder = VariantBuilder::new();
    /// // Create an object builder that will write fields to the object
    /// let mut object_builder = builder.new_object();
    /// object_builder.insert("first_name", "Jiaying");
    /// object_builder.insert("last_name", "Li");
    /// object_builder.finish();
    /// // Finish the builder to get the metadata and value
    /// let (metadata, value) = builder.finish();
    /// // Create the Variant and convert to JSON
    /// let variant = Variant::try_new(&metadata, &value)?;
    /// let mut writer = Vec::new();
    /// variant.to_json(&mut writer)?;
    /// assert_eq!(br#"{"first_name":"Jiaying","last_name":"Li"}"#, writer.as_slice());
    /// # Ok::<(), ArrowError>(())
    /// ```
    fn to_json(&self, buffer: &mut impl Write) -> Result<(), ArrowError>;

    /// Convert [`Variant`] to JSON [`String`]
    ///
    /// This is a convenience function that converts a Variant to a JSON string.
    /// This is the same as calling [`VariantToJson::to_json`] with a [`Vec`].
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
    /// # use parquet_variant::{Variant};
    /// # use parquet_variant_json::VariantToJson;
    /// # use arrow_schema::ArrowError;
    /// let variant = Variant::Int32(42);
    /// let json = variant.to_json_string()?;
    /// assert_eq!(json, "42");
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
    /// # use parquet_variant::{Variant, VariantBuilder};
    /// # use parquet_variant_json::VariantToJson;
    /// # use arrow_schema::ArrowError;
    /// let mut builder = VariantBuilder::new();
    /// // Create an object builder that will write fields to the object
    /// let mut object_builder = builder.new_object();
    /// object_builder.insert("first_name", "Jiaying");
    /// object_builder.insert("last_name", "Li");
    /// object_builder.finish();
    /// // Finish the builder to get the metadata and value
    /// let (metadata, value) = builder.finish();
    /// // Create the Variant and convert to JSON
    /// let variant = Variant::try_new(&metadata, &value)?;
    /// let json = variant.to_json_string()?;
    /// assert_eq!(r#"{"first_name":"Jiaying","last_name":"Li"}"#, json);
    /// # Ok::<(), ArrowError>(())
    /// ```
    fn to_json_string(&self) -> Result<String, ArrowError>;

    /// Convert [`Variant`] to [`serde_json::Value`]
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
    /// # use parquet_variant::{Variant};
    /// # use parquet_variant_json::VariantToJson;
    /// # use serde_json::Value;
    /// # use arrow_schema::ArrowError;
    /// let variant = Variant::from("hello");
    /// let json_value = variant.to_json_value()?;
    /// assert_eq!(json_value, Value::String("hello".to_string()));
    /// # Ok::<(), ArrowError>(())
    /// ```
    fn to_json_value(&self) -> Result<Value, ArrowError>;
}

impl<'m, 'v> VariantToJson for Variant<'m, 'v> {
    fn to_json(&self, buffer: &mut impl Write) -> Result<(), ArrowError> {
        match self {
            Variant::Null => write!(buffer, "null")?,
            Variant::BooleanTrue => write!(buffer, "true")?,
            Variant::BooleanFalse => write!(buffer, "false")?,
            Variant::Int8(i) => write!(buffer, "{i}")?,
            Variant::Int16(i) => write!(buffer, "{i}")?,
            Variant::Int32(i) => write!(buffer, "{i}")?,
            Variant::Int64(i) => write!(buffer, "{i}")?,
            Variant::Float(f) => write!(buffer, "{f}")?,
            Variant::Double(f) => write!(buffer, "{f}")?,
            Variant::Decimal4(decimal) => write!(buffer, "{decimal}")?,
            Variant::Decimal8(decimal) => write!(buffer, "{decimal}")?,
            Variant::Decimal16(decimal) => write!(buffer, "{decimal}")?,
            Variant::Date(date) => write!(buffer, "\"{}\"", format_date_string(date))?,
            Variant::TimestampMicros(ts) | Variant::TimestampNanos(ts) => {
                write!(buffer, "\"{}\"", ts.to_rfc3339())?
            }
            Variant::TimestampNtzMicros(ts) => {
                write!(buffer, "\"{}\"", format_timestamp_ntz_string(ts, 6))?
            }
            Variant::TimestampNtzNanos(ts) => {
                write!(buffer, "\"{}\"", format_timestamp_ntz_string(ts, 9))?
            }
            Variant::Time(time) => write!(buffer, "\"{}\"", format_time_ntz_str(time))?,
            Variant::Binary(bytes) => {
                // Encode binary as base64 string
                let base64_str = format_binary_base64(bytes);
                let json_str = serde_json::to_string(&base64_str).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
                })?;
                write!(buffer, "{json_str}")?
            }
            Variant::String(s) => {
                // Use serde_json to properly escape the string
                let json_str = serde_json::to_string(s).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
                })?;
                write!(buffer, "{json_str}")?
            }
            Variant::ShortString(s) => {
                // Use serde_json to properly escape the string
                let json_str = serde_json::to_string(s.as_str()).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
                })?;
                write!(buffer, "{json_str}")?
            }
            Variant::Uuid(uuid) => {
                write!(buffer, "\"{uuid}\"")?;
            }
            Variant::Object(obj) => {
                convert_object_to_json(buffer, obj)?;
            }
            Variant::List(arr) => {
                convert_array_to_json(buffer, arr)?;
            }
        }
        Ok(())
    }

    fn to_json_string(&self) -> Result<String, ArrowError> {
        let mut buffer = Vec::new();
        self.to_json(&mut buffer)?;
        String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(format!("UTF-8 conversion error: {e}")))
    }

    fn to_json_value(&self) -> Result<Value, ArrowError> {
        match self {
            Variant::Null => Ok(Value::Null),
            Variant::BooleanTrue => Ok(Value::Bool(true)),
            Variant::BooleanFalse => Ok(Value::Bool(false)),
            Variant::Int8(i) => Ok(Value::Number((*i).into())),
            Variant::Int16(i) => Ok(Value::Number((*i).into())),
            Variant::Int32(i) => Ok(Value::Number((*i).into())),
            Variant::Int64(i) => Ok(Value::Number((*i).into())),
            Variant::Float(f) => serde_json::Number::from_f64((*f).into())
                .map(Value::Number)
                .ok_or_else(|| ArrowError::InvalidArgumentError("Invalid float value".to_string())),
            Variant::Double(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Invalid double value".to_string())
                }),
            Variant::Decimal4(decimal4) => {
                let scale = decimal4.scale();
                let integer = decimal4.integer();

                let integer = if scale == 0 {
                    integer
                } else {
                    let divisor = 10_i32.pow(scale as u32);
                    if integer % divisor != 0 {
                        // fall back to floating point
                        return Ok(Value::from(integer as f64 / divisor as f64));
                    }
                    integer / divisor
                };
                Ok(Value::from(integer))
            }
            Variant::Decimal8(decimal8) => {
                let scale = decimal8.scale();
                let integer = decimal8.integer();

                let integer = if scale == 0 {
                    integer
                } else {
                    let divisor = 10_i64.pow(scale as u32);
                    if integer % divisor != 0 {
                        // fall back to floating point
                        return Ok(Value::from(integer as f64 / divisor as f64));
                    }
                    integer / divisor
                };
                Ok(Value::from(integer))
            }
            Variant::Decimal16(decimal16) => {
                let scale = decimal16.scale();
                let integer = decimal16.integer();

                let integer = if scale == 0 {
                    integer
                } else {
                    let divisor = 10_i128.pow(scale as u32);
                    if integer % divisor != 0 {
                        // fall back to floating point
                        return Ok(Value::from(integer as f64 / divisor as f64));
                    }
                    integer / divisor
                };
                // i128 has higher precision than any 64-bit type. Try a lossless narrowing cast to
                // i64 or u64 first, falling back to a lossy narrowing cast to f64 if necessary.
                let value = i64::try_from(integer)
                    .map(Value::from)
                    .or_else(|_| u64::try_from(integer).map(Value::from))
                    .unwrap_or_else(|_| Value::from(integer as f64));
                Ok(value)
            }
            Variant::Date(date) => Ok(Value::String(format_date_string(date))),
            Variant::TimestampMicros(ts) | Variant::TimestampNanos(ts) => {
                Ok(Value::String(ts.to_rfc3339()))
            }
            Variant::TimestampNtzMicros(ts) => {
                Ok(Value::String(format_timestamp_ntz_string(ts, 6)))
            }
            Variant::TimestampNtzNanos(ts) => Ok(Value::String(format_timestamp_ntz_string(ts, 9))),
            Variant::Time(time) => Ok(Value::String(format_time_ntz_str(time))),
            Variant::Binary(bytes) => Ok(Value::String(format_binary_base64(bytes))),
            Variant::String(s) => Ok(Value::String(s.to_string())),
            Variant::ShortString(s) => Ok(Value::String(s.to_string())),
            Variant::Uuid(uuid) => Ok(Value::String(uuid.to_string())),
            Variant::Object(obj) => {
                let map = obj
                    .iter()
                    .map(|(k, v)| v.to_json_value().map(|json_val| (k.to_string(), json_val)))
                    .collect::<Result<_, _>>()?;
                Ok(Value::Object(map))
            }
            Variant::List(arr) => {
                let vec = arr
                    .iter()
                    .map(|element| element.to_json_value())
                    .collect::<Result<_, _>>()?;
                Ok(Value::Array(vec))
            }
        }
    }
}

// Format string constants to avoid duplication and reduce errors
const DATE_FORMAT: &str = "%Y-%m-%d";

// Helper functions for consistent formatting
fn format_date_string(date: &chrono::NaiveDate) -> String {
    date.format(DATE_FORMAT).to_string()
}

fn format_timestamp_ntz_string(ts: &chrono::NaiveDateTime, precision: usize) -> String {
    let format_str = format!(
        "{}",
        ts.format(&format!("%Y-%m-%dT%H:%M:%S%.{}f", precision))
    );
    ts.format(format_str.as_str()).to_string()
}

fn format_binary_base64(bytes: &[u8]) -> String {
    general_purpose::STANDARD.encode(bytes)
}

fn format_time_ntz_str(time: &chrono::NaiveTime) -> String {
    let base = time.format("%H:%M:%S").to_string();
    let micros = time.nanosecond() / 1000;
    match micros {
        0 => format!("{}.{}", base, 0),
        _ => {
            let micros_str = format!("{:06}", micros);
            let micros_str_trimmed = micros_str.trim_matches('0');
            format!("{}.{}", base, micros_str_trimmed)
        }
    }
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
            ArrowError::InvalidArgumentError(format!("JSON key encoding error: {e}"))
        })?;
        write!(buffer, "{json_key}:")?;

        // Recursively convert the value
        value.to_json(buffer)?;
    }

    write!(buffer, "}}")?;
    Ok(())
}

/// Convert array elements to JSON
fn convert_array_to_json(buffer: &mut impl Write, arr: &VariantList) -> Result<(), ArrowError> {
    write!(buffer, "[")?;

    let mut first = true;
    for element in arr.iter() {
        if !first {
            write!(buffer, ",")?;
        }
        first = false;

        element.to_json(buffer)?;
    }

    write!(buffer, "]")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use parquet_variant::{VariantDecimal4, VariantDecimal8, VariantDecimal16};

    #[test]
    fn test_decimal_edge_cases() -> Result<(), ArrowError> {
        // Test negative decimal
        let negative_variant = Variant::from(VariantDecimal4::try_new(-12345, 3)?);
        let negative_json = negative_variant.to_json_string()?;
        assert_eq!(negative_json, "-12.345");

        // Test large scale decimal
        let large_scale_variant = Variant::from(VariantDecimal8::try_new(123456789, 6)?);
        let large_scale_json = large_scale_variant.to_json_string()?;
        assert_eq!(large_scale_json, "123.456789");

        Ok(())
    }

    #[test]
    fn test_decimal16_to_json() -> Result<(), ArrowError> {
        let variant = Variant::from(VariantDecimal16::try_new(123456789012345, 4)?);
        let json = variant.to_json_string()?;
        assert_eq!(json, "12345678901.2345");

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::Number(_)));

        // Test very large number
        let large_variant = Variant::from(VariantDecimal16::try_new(999999999999999999, 2)?);
        let large_json = large_variant.to_json_string()?;
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
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"2023-12-25\"");

        let json_value = variant.to_json_value()?;
        assert_eq!(json_value, Value::String("2023-12-25".to_string()));

        // Test leap year date
        let leap_date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let leap_variant = Variant::Date(leap_date);
        let leap_json = leap_variant.to_json_string()?;
        assert_eq!(leap_json, "\"2024-02-29\"");
        Ok(())
    }

    #[test]
    fn test_timestamp_micros_to_json() -> Result<(), ArrowError> {
        let timestamp = DateTime::parse_from_rfc3339("2023-12-25T10:30:45Z")
            .unwrap()
            .with_timezone(&Utc);
        let variant = Variant::TimestampMicros(timestamp);
        let json = variant.to_json_string()?;
        assert!(json.contains("2023-12-25T10:30:45"));
        assert!(json.starts_with('"') && json.ends_with('"'));

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_timestamp_ntz_micros_to_json() -> Result<(), ArrowError> {
        let naive_timestamp = DateTime::from_timestamp(1703505045, 123456)
            .unwrap()
            .naive_utc();
        let variant = Variant::TimestampNtzMicros(naive_timestamp);
        let json = variant.to_json_string()?;
        assert!(json.contains("2023-12-25"));
        assert!(json.starts_with('"') && json.ends_with('"'));

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_time_to_json() -> Result<(), ArrowError> {
        let naive_time = NaiveTime::from_num_seconds_from_midnight_opt(12345, 123460708).unwrap();
        let variant = Variant::Time(naive_time);
        let json = variant.to_json_string()?;
        assert_eq!("\"03:25:45.12346\"", json);

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_timestamp_nanos_to_json() -> Result<(), ArrowError> {
        let timestamp = DateTime::parse_from_rfc3339("2023-12-25T10:30:45.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let variant = Variant::TimestampNanos(timestamp);
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"2023-12-25T10:30:45.123456789+00:00\"");

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_timestamp_ntz_nanos_to_json() -> Result<(), ArrowError> {
        let naive_timestamp = DateTime::from_timestamp(1703505045, 123456789)
            .unwrap()
            .naive_utc();
        let variant = Variant::TimestampNtzNanos(naive_timestamp);
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"2023-12-25T11:50:45.123456789\"");

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));
        Ok(())
    }

    #[test]
    fn test_binary_to_json() -> Result<(), ArrowError> {
        let binary_data = b"Hello, World!";
        let variant = Variant::Binary(binary_data);
        let json = variant.to_json_string()?;

        // Should be base64 encoded and quoted
        assert!(json.starts_with('"') && json.ends_with('"'));
        assert!(json.len() > 2); // Should have content

        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::String(_)));

        // Test empty binary
        let empty_variant = Variant::Binary(b"");
        let empty_json = empty_variant.to_json_string()?;
        assert_eq!(empty_json, "\"\"");

        // Test binary with special bytes
        let special_variant = Variant::Binary(&[0, 255, 128, 64]);
        let special_json = special_variant.to_json_string()?;
        assert!(special_json.starts_with('"') && special_json.ends_with('"'));
        Ok(())
    }

    #[test]
    fn test_string_to_json() -> Result<(), ArrowError> {
        let variant = Variant::from("hello world");
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"hello world\"");

        let json_value = variant.to_json_value()?;
        assert_eq!(json_value, Value::String("hello world".to_string()));
        Ok(())
    }

    #[test]
    fn test_short_string_to_json() -> Result<(), ArrowError> {
        use parquet_variant::ShortString;
        let short_string = ShortString::try_new("short")?;
        let variant = Variant::ShortString(short_string);
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"short\"");

        let json_value = variant.to_json_value()?;
        assert_eq!(json_value, Value::String("short".to_string()));
        Ok(())
    }

    #[test]
    fn test_uuid_to_json() -> Result<(), ArrowError> {
        let uuid = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let variant = Variant::Uuid(uuid);
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"123e4567-e89b-12d3-a456-426614174000\"");

        let json_value = variant.to_json_value()?;
        assert_eq!(
            json_value,
            Value::String("123e4567-e89b-12d3-a456-426614174000".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_string_escaping() -> Result<(), ArrowError> {
        let variant = Variant::from("hello\nworld\t\"quoted\"");
        let json = variant.to_json_string()?;
        assert_eq!(json, "\"hello\\nworld\\t\\\"quoted\\\"\"");

        let json_value = variant.to_json_value()?;
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
        variant.to_json(&mut buffer)?;

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
            let json_string = self
                .variant
                .to_json_string()
                .expect("variant_to_json_string should succeed");
            assert_eq!(
                json_string, self.expected_json,
                "JSON string mismatch for variant: {:?}",
                self.variant
            );

            let json_value = self
                .variant
                .to_json_value()
                .expect("variant_to_json_value should succeed");

            // For floating point numbers, we need special comparison due to JSON number representation
            match (&json_value, &self.expected_value) {
                (Value::Number(actual), Value::Number(expected)) => {
                    let actual_f64 = actual.as_f64().unwrap_or(0.0);
                    let expected_f64 = expected.as_f64().unwrap_or(0.0);
                    assert!(
                        (actual_f64 - expected_f64).abs() < f64::EPSILON,
                        "JSON value mismatch for variant: {:?}, got {}, expected {}",
                        self.variant,
                        actual_f64,
                        expected_f64
                    );
                }
                _ => {
                    assert_eq!(
                        json_value, self.expected_value,
                        "JSON value mismatch for variant: {:?}",
                        self.variant
                    );
                }
            }

            // Verify roundtrip: JSON string should parse to same value
            let parsed: Value =
                serde_json::from_str(&json_string).expect("Generated JSON should be valid");
            // Same floating point handling for roundtrip
            match (&parsed, &self.expected_value) {
                (Value::Number(actual), Value::Number(expected)) => {
                    let actual_f64 = actual.as_f64().unwrap_or(0.0);
                    let expected_f64 = expected.as_f64().unwrap_or(0.0);
                    assert!(
                        (actual_f64 - expected_f64).abs() < f64::EPSILON,
                        "Parsed JSON mismatch for variant: {:?}, got {}, expected {}",
                        self.variant,
                        actual_f64,
                        expected_f64
                    );
                }
                _ => {
                    assert_eq!(
                        parsed, self.expected_value,
                        "Parsed JSON mismatch for variant: {:?}",
                        self.variant
                    );
                }
            }
        }
    }

    #[test]
    fn test_primitive_json_conversion() {
        use parquet_variant::ShortString;

        // Null
        JsonTest {
            variant: Variant::Null,
            expected_json: "null",
            expected_value: Value::Null,
        }
        .run();

        // Booleans
        JsonTest {
            variant: Variant::BooleanTrue,
            expected_json: "true",
            expected_value: Value::Bool(true),
        }
        .run();

        JsonTest {
            variant: Variant::BooleanFalse,
            expected_json: "false",
            expected_value: Value::Bool(false),
        }
        .run();

        // Integers - positive and negative edge cases
        JsonTest {
            variant: Variant::Int8(42),
            expected_json: "42",
            expected_value: Value::Number(42.into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int8(-128),
            expected_json: "-128",
            expected_value: Value::Number((-128).into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int16(32767),
            expected_json: "32767",
            expected_value: Value::Number(32767.into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int16(-32768),
            expected_json: "-32768",
            expected_value: Value::Number((-32768).into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int32(2147483647),
            expected_json: "2147483647",
            expected_value: Value::Number(2147483647.into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int32(-2147483648),
            expected_json: "-2147483648",
            expected_value: Value::Number((-2147483648).into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int64(9223372036854775807),
            expected_json: "9223372036854775807",
            expected_value: Value::Number(9223372036854775807i64.into()),
        }
        .run();

        JsonTest {
            variant: Variant::Int64(-9223372036854775808),
            expected_json: "-9223372036854775808",
            expected_value: Value::Number((-9223372036854775808i64).into()),
        }
        .run();

        // Floats
        JsonTest {
            variant: Variant::Float(3.5),
            expected_json: "3.5",
            expected_value: serde_json::Number::from_f64(3.5)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        JsonTest {
            variant: Variant::Float(0.0),
            expected_json: "0",
            expected_value: Value::Number(0.into()), // Use integer 0 to match JSON parsing
        }
        .run();

        JsonTest {
            variant: Variant::Float(-1.5),
            expected_json: "-1.5",
            expected_value: serde_json::Number::from_f64(-1.5)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        JsonTest {
            variant: Variant::Double(std::f64::consts::E),
            expected_json: "2.718281828459045",
            expected_value: serde_json::Number::from_f64(std::f64::consts::E)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        // Decimals
        JsonTest {
            variant: Variant::from(VariantDecimal4::try_new(12345, 2).unwrap()),
            expected_json: "123.45",
            expected_value: serde_json::Number::from_f64(123.45)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        JsonTest {
            variant: Variant::from(VariantDecimal4::try_new(42, 0).unwrap()),
            expected_json: "42",
            expected_value: serde_json::Number::from_f64(42.0)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        JsonTest {
            variant: Variant::from(VariantDecimal8::try_new(1234567890, 3).unwrap()),
            expected_json: "1234567.89",
            expected_value: serde_json::Number::from_f64(1234567.89)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        JsonTest {
            variant: Variant::from(VariantDecimal16::try_new(123456789012345, 4).unwrap()),
            expected_json: "12345678901.2345",
            expected_value: serde_json::Number::from_f64(12345678901.2345)
                .map(Value::Number)
                .unwrap(),
        }
        .run();

        // Strings
        JsonTest {
            variant: Variant::from("hello world"),
            expected_json: "\"hello world\"",
            expected_value: Value::String("hello world".to_string()),
        }
        .run();

        JsonTest {
            variant: Variant::from(""),
            expected_json: "\"\"",
            expected_value: Value::String("".to_string()),
        }
        .run();

        JsonTest {
            variant: Variant::ShortString(ShortString::try_new("test").unwrap()),
            expected_json: "\"test\"",
            expected_value: Value::String("test".to_string()),
        }
        .run();

        // Date and timestamps
        JsonTest {
            variant: Variant::Date(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap()),
            expected_json: "\"2023-12-25\"",
            expected_value: Value::String("2023-12-25".to_string()),
        }
        .run();

        // Binary data (base64 encoded)
        JsonTest {
            variant: Variant::Binary(b"test"),
            expected_json: "\"dGVzdA==\"", // base64 encoded "test"
            expected_value: Value::String("dGVzdA==".to_string()),
        }
        .run();

        JsonTest {
            variant: Variant::Binary(b""),
            expected_json: "\"\"", // empty base64
            expected_value: Value::String("".to_string()),
        }
        .run();

        JsonTest {
            variant: Variant::Binary(b"binary data"),
            expected_json: "\"YmluYXJ5IGRhdGE=\"", // base64 encoded "binary data"
            expected_value: Value::String("YmluYXJ5IGRhdGE=".to_string()),
        }
        .run();
    }

    #[test]
    fn test_string_escaping_comprehensive() {
        // Test comprehensive string escaping scenarios
        JsonTest {
            variant: Variant::from("line1\nline2\ttab\"quote\"\\backslash"),
            expected_json: "\"line1\\nline2\\ttab\\\"quote\\\"\\\\backslash\"",
            expected_value: Value::String("line1\nline2\ttab\"quote\"\\backslash".to_string()),
        }
        .run();

        JsonTest {
            variant: Variant::from("Hello 世界 🌍"),
            expected_json: "\"Hello 世界 🌍\"",
            expected_value: Value::String("Hello 世界 🌍".to_string()),
        }
        .run();
    }

    #[test]
    fn test_buffer_writing_variants() -> Result<(), ArrowError> {
        let variant = Variant::from("test buffer writing");

        // Test writing to a Vec<u8>
        let mut buffer = Vec::new();
        variant.to_json(&mut buffer)?;
        let result = String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        assert_eq!(result, "\"test buffer writing\"");

        // Test writing to vec![]
        let mut buffer = vec![];
        variant.to_json(&mut buffer)?;
        let result = String::from_utf8(buffer)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        assert_eq!(result, "\"test buffer writing\"");

        Ok(())
    }

    #[test]
    fn test_simple_object_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        // Create a simple object with various field types
        let mut builder = VariantBuilder::new();

        builder
            .new_object()
            .with_field("name", "Alice")
            .with_field("age", 30i32)
            .with_field("active", true)
            .with_field("score", 95.5f64)
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        // Parse the JSON to verify structure - handle JSON parsing errors manually
        let parsed: Value = serde_json::from_str(&json).unwrap();
        let obj = parsed.as_object().expect("expected JSON object");
        assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(obj.get("age"), Some(&Value::Number(30.into())));
        assert_eq!(obj.get("active"), Some(&Value::Bool(true)));
        assert!(matches!(obj.get("score"), Some(Value::Number(_))));
        assert_eq!(obj.len(), 4);

        // Test variant_to_json_value as well
        let json_value = variant.to_json_value()?;
        assert!(matches!(json_value, Value::Object(_)));

        Ok(())
    }

    #[test]
    fn test_empty_object_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let obj = builder.new_object();
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;
        assert_eq!(json, "{}");

        let json_value = variant.to_json_value()?;
        assert_eq!(json_value, Value::Object(serde_json::Map::new()));

        Ok(())
    }

    #[test]
    fn test_object_with_special_characters_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        builder
            .new_object()
            .with_field("message", "Hello \"World\"\nWith\tTabs")
            .with_field("path", "C:\\Users\\Alice\\Documents")
            .with_field("unicode", "😀 Smiley")
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        // Verify that special characters are properly escaped
        assert!(json.contains("Hello \\\"World\\\"\\nWith\\tTabs"));
        assert!(json.contains("C:\\\\Users\\\\Alice\\\\Documents"));
        assert!(json.contains("😀 Smiley"));

        // Verify that the JSON can be parsed back
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Value::Object(_)));

        Ok(())
    }

    #[test]
    fn test_simple_list_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value(1i32)
            .with_value(2i32)
            .with_value(3i32)
            .with_value(4i32)
            .with_value(5i32)
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;
        assert_eq!(json, "[1,2,3,4,5]");

        let json_value = variant.to_json_value()?;
        let arr = json_value.as_array().expect("expected JSON array");
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0], Value::Number(1.into()));
        assert_eq!(arr[4], Value::Number(5.into()));

        Ok(())
    }

    #[test]
    fn test_empty_list_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let list = builder.new_list();
            list.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;
        assert_eq!(json, "[]");

        let json_value = variant.to_json_value()?;
        assert_eq!(json_value, Value::Array(vec![]));

        Ok(())
    }

    #[test]
    fn test_mixed_type_list_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value("hello")
            .with_value(42i32)
            .with_value(true)
            .with_value(()) // null
            .with_value(std::f64::consts::PI)
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        let parsed: Value = serde_json::from_str(&json).unwrap();
        let arr = parsed.as_array().expect("expected JSON array");
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0], Value::String("hello".to_string()));
        assert_eq!(arr[1], Value::Number(42.into()));
        assert_eq!(arr[2], Value::Bool(true));
        assert_eq!(arr[3], Value::Null);
        assert!(matches!(arr[4], Value::Number(_)));

        Ok(())
    }

    #[test]
    fn test_object_field_ordering_in_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            // Add fields in non-alphabetical order
            obj.insert("zebra", "last");
            obj.insert("alpha", "first");
            obj.insert("beta", "second");
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        // Parse and verify all fields are present
        let parsed: Value = serde_json::from_str(&json).unwrap();
        let obj = parsed.as_object().expect("expected JSON object");
        assert_eq!(obj.len(), 3);
        assert_eq!(obj.get("alpha"), Some(&Value::String("first".to_string())));
        assert_eq!(obj.get("beta"), Some(&Value::String("second".to_string())));
        assert_eq!(obj.get("zebra"), Some(&Value::String("last".to_string())));

        Ok(())
    }

    #[test]
    fn test_list_with_various_primitive_types_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value("string_value")
            .with_value(42i32)
            .with_value(true)
            .with_value(std::f64::consts::PI)
            .with_value(false)
            .with_value(()) // null
            .with_value(100i64)
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        let parsed: Value = serde_json::from_str(&json).unwrap();
        let arr = parsed.as_array().expect("expected JSON array");
        assert_eq!(arr.len(), 7);
        assert_eq!(arr[0], Value::String("string_value".to_string()));
        assert_eq!(arr[1], Value::Number(42.into()));
        assert_eq!(arr[2], Value::Bool(true));
        assert!(matches!(arr[3], Value::Number(_))); // float
        assert_eq!(arr[4], Value::Bool(false));
        assert_eq!(arr[5], Value::Null);
        assert_eq!(arr[6], Value::Number(100.into()));

        Ok(())
    }

    #[test]
    fn test_object_with_various_primitive_types_to_json() -> Result<(), ArrowError> {
        use parquet_variant::VariantBuilder;

        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.insert("string_field", "test_string");
            obj.insert("int_field", 123i32);
            obj.insert("bool_field", true);
            obj.insert("float_field", 2.71f64);
            obj.insert("null_field", ());
            obj.insert("long_field", 999i64);
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let json = variant.to_json_string()?;

        let parsed: Value = serde_json::from_str(&json).unwrap();
        let obj = parsed.as_object().expect("expected JSON object");
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

        Ok(())
    }

    #[test]
    fn test_decimal_precision_behavior() -> Result<(), ArrowError> {
        // Test case that demonstrates f64 precision limits
        // This is a 63-bit precision decimal8 value that f64 cannot represent exactly
        let high_precision_decimal8 = Variant::from(VariantDecimal8::try_new(
            9007199254740993, // 2^53 + 1, exceeds f64 precision
            6,
        )?);

        let json_string = high_precision_decimal8.to_json_string()?;
        let json_value = high_precision_decimal8.to_json_value()?;

        // Due to f64 precision limits, we expect precision loss for values > 2^53
        // Both functions should produce consistent results (even if not exact)
        let parsed: Value = serde_json::from_str(&json_string).unwrap();
        assert_eq!(parsed, json_value);

        // Test a case that can be exactly represented (integer result)
        let exact_decimal = Variant::from(VariantDecimal8::try_new(
            1234567890000, // Should result in 1234567.89 (trailing zeros trimmed)
            6,
        )?);

        let json_string_exact = exact_decimal.to_json_string()?;
        assert_eq!(json_string_exact, "1234567.89");

        // Test integer case (should be exact)
        let integer_decimal = Variant::from(VariantDecimal8::try_new(
            42000000, // Should result in 42 (integer)
            6,
        )?);

        let json_string_integer = integer_decimal.to_json_string()?;
        assert_eq!(json_string_integer, "42");

        Ok(())
    }

    #[test]
    fn test_float_nan_inf_handling() -> Result<(), ArrowError> {
        // Test NaN handling - should return an error since JSON doesn't support NaN
        let nan_variant = Variant::Float(f32::NAN);
        let nan_result = nan_variant.to_json_value();
        assert!(nan_result.is_err());
        assert!(
            nan_result
                .unwrap_err()
                .to_string()
                .contains("Invalid float value")
        );

        // Test positive infinity - should return an error since JSON doesn't support Infinity
        let pos_inf_variant = Variant::Float(f32::INFINITY);
        let pos_inf_result = pos_inf_variant.to_json_value();
        assert!(pos_inf_result.is_err());
        assert!(
            pos_inf_result
                .unwrap_err()
                .to_string()
                .contains("Invalid float value")
        );

        // Test negative infinity - should return an error since JSON doesn't support -Infinity
        let neg_inf_variant = Variant::Float(f32::NEG_INFINITY);
        let neg_inf_result = neg_inf_variant.to_json_value();
        assert!(neg_inf_result.is_err());
        assert!(
            neg_inf_result
                .unwrap_err()
                .to_string()
                .contains("Invalid float value")
        );

        // Test the same for Double variants
        let nan_double_variant = Variant::Double(f64::NAN);
        let nan_double_result = nan_double_variant.to_json_value();
        assert!(nan_double_result.is_err());
        assert!(
            nan_double_result
                .unwrap_err()
                .to_string()
                .contains("Invalid double value")
        );

        let pos_inf_double_variant = Variant::Double(f64::INFINITY);
        let pos_inf_double_result = pos_inf_double_variant.to_json_value();
        assert!(pos_inf_double_result.is_err());
        assert!(
            pos_inf_double_result
                .unwrap_err()
                .to_string()
                .contains("Invalid double value")
        );

        let neg_inf_double_variant = Variant::Double(f64::NEG_INFINITY);
        let neg_inf_double_result = neg_inf_double_variant.to_json_value();
        assert!(neg_inf_double_result.is_err());
        assert!(
            neg_inf_double_result
                .unwrap_err()
                .to_string()
                .contains("Invalid double value")
        );

        // Test normal float values still work
        let normal_float = Variant::Float(std::f32::consts::PI);
        let normal_result = normal_float.to_json_value()?;
        assert!(matches!(normal_result, Value::Number(_)));

        let normal_double = Variant::Double(std::f64::consts::E);
        let normal_double_result = normal_double.to_json_value()?;
        assert!(matches!(normal_double_result, Value::Number(_)));

        Ok(())
    }
}
