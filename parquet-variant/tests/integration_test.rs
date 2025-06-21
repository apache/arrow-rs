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

//! Integration tests for JSON conversion functionality

use chrono::{DateTime, NaiveDate, Utc};
use parquet_variant::{variant_to_json_string, variant_to_json_value, Variant};
use serde_json::Value;

#[test]
fn test_primitive_values_to_json() {
    // Test null
    let null_variant = Variant::Null;
    assert_eq!(variant_to_json_string(&null_variant).unwrap(), "null");
    assert_eq!(variant_to_json_value(&null_variant).unwrap(), Value::Null);

    // Test boolean
    let bool_true = Variant::BooleanTrue;
    let bool_false = Variant::BooleanFalse;
    assert_eq!(variant_to_json_string(&bool_true).unwrap(), "true");
    assert_eq!(variant_to_json_string(&bool_false).unwrap(), "false");
    assert_eq!(
        variant_to_json_value(&bool_true).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        variant_to_json_value(&bool_false).unwrap(),
        Value::Bool(false)
    );

    // Test integers
    let int8_variant = Variant::Int8(42);
    assert_eq!(variant_to_json_string(&int8_variant).unwrap(), "42");
    assert_eq!(
        variant_to_json_value(&int8_variant).unwrap(),
        Value::Number(42.into())
    );

    let negative_int8 = Variant::Int8(-123);
    assert_eq!(variant_to_json_string(&negative_int8).unwrap(), "-123");
    assert_eq!(
        variant_to_json_value(&negative_int8).unwrap(),
        Value::Number((-123).into())
    );

    // Test strings
    let string_variant = Variant::String("hello world");
    assert_eq!(
        variant_to_json_string(&string_variant).unwrap(),
        "\"hello world\""
    );
    assert_eq!(
        variant_to_json_value(&string_variant).unwrap(),
        Value::String("hello world".to_string())
    );

    let short_string = Variant::ShortString(parquet_variant::ShortString::try_new("test").unwrap());
    assert_eq!(variant_to_json_string(&short_string).unwrap(), "\"test\"");
    assert_eq!(
        variant_to_json_value(&short_string).unwrap(),
        Value::String("test".to_string())
    );
}

#[test]
fn test_integer_types_to_json() {
    // Test Int16
    let int16_variant = Variant::Int16(32767);
    assert_eq!(variant_to_json_string(&int16_variant).unwrap(), "32767");
    assert_eq!(
        variant_to_json_value(&int16_variant).unwrap(),
        Value::Number(32767.into())
    );

    let negative_int16 = Variant::Int16(-32768);
    assert_eq!(variant_to_json_string(&negative_int16).unwrap(), "-32768");
    assert_eq!(
        variant_to_json_value(&negative_int16).unwrap(),
        Value::Number((-32768).into())
    );

    // Test Int32
    let int32_variant = Variant::Int32(2147483647);
    assert_eq!(
        variant_to_json_string(&int32_variant).unwrap(),
        "2147483647"
    );
    assert_eq!(
        variant_to_json_value(&int32_variant).unwrap(),
        Value::Number(2147483647.into())
    );

    let negative_int32 = Variant::Int32(-2147483648);
    assert_eq!(
        variant_to_json_string(&negative_int32).unwrap(),
        "-2147483648"
    );
    assert_eq!(
        variant_to_json_value(&negative_int32).unwrap(),
        Value::Number((-2147483648).into())
    );

    // Test Int64
    let int64_variant = Variant::Int64(9223372036854775807);
    assert_eq!(
        variant_to_json_string(&int64_variant).unwrap(),
        "9223372036854775807"
    );
    assert_eq!(
        variant_to_json_value(&int64_variant).unwrap(),
        Value::Number(9223372036854775807i64.into())
    );

    let negative_int64 = Variant::Int64(-9223372036854775808);
    assert_eq!(
        variant_to_json_string(&negative_int64).unwrap(),
        "-9223372036854775808"
    );
    assert_eq!(
        variant_to_json_value(&negative_int64).unwrap(),
        Value::Number((-9223372036854775808i64).into())
    );
}

#[test]
fn test_floating_point_types_to_json() {
    // Test Float (f32)
    let float_variant = Variant::Float(std::f32::consts::PI);
    let float_json = variant_to_json_string(&float_variant).unwrap();
    assert!(float_json.starts_with("3.14159"));

    let float_value = variant_to_json_value(&float_variant).unwrap();
    assert!(matches!(float_value, Value::Number(_)));

    // Test Double (f64)
    let double_variant = Variant::Double(std::f64::consts::E);
    let double_json = variant_to_json_string(&double_variant).unwrap();
    assert!(double_json.starts_with("2.718281828459045"));

    let double_value = variant_to_json_value(&double_variant).unwrap();
    assert!(matches!(double_value, Value::Number(_)));

    // Test special float values
    let zero_float = Variant::Float(0.0);
    assert_eq!(variant_to_json_string(&zero_float).unwrap(), "0");

    let negative_float = Variant::Float(-1.5);
    assert_eq!(variant_to_json_string(&negative_float).unwrap(), "-1.5");
}

#[test]
fn test_decimal_types_to_json() {
    // Test Decimal4 (i32 with scale)
    let decimal4_variant = Variant::Decimal4 {
        integer: 12345,
        scale: 2,
    };
    assert_eq!(variant_to_json_string(&decimal4_variant).unwrap(), "123.45");

    let decimal4_value = variant_to_json_value(&decimal4_variant).unwrap();
    assert!(matches!(decimal4_value, Value::Number(_)));

    // Test Decimal8 (i64 with scale)
    let decimal8_variant = Variant::Decimal8 {
        integer: 1234567890,
        scale: 3,
    };
    assert_eq!(
        variant_to_json_string(&decimal8_variant).unwrap(),
        "1234567.89"
    );

    let decimal8_value = variant_to_json_value(&decimal8_variant).unwrap();
    assert!(matches!(decimal8_value, Value::Number(_)));

    // Test Decimal16 (i128 with scale)
    let decimal16_variant = Variant::Decimal16 {
        integer: 123456789012345,
        scale: 4,
    };
    assert_eq!(
        variant_to_json_string(&decimal16_variant).unwrap(),
        "12345678901.2345"
    );

    let decimal16_value = variant_to_json_value(&decimal16_variant).unwrap();
    assert!(matches!(decimal16_value, Value::Number(_)));

    // Test zero scale decimal
    let no_scale_decimal = Variant::Decimal4 {
        integer: 42,
        scale: 0,
    };
    assert_eq!(variant_to_json_string(&no_scale_decimal).unwrap(), "42");
}

#[test]
fn test_date_and_timestamp_types_to_json() {
    // Test Date
    let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
    let date_variant = Variant::Date(date);
    assert_eq!(
        variant_to_json_string(&date_variant).unwrap(),
        "\"2023-12-25\""
    );
    assert_eq!(
        variant_to_json_value(&date_variant).unwrap(),
        Value::String("2023-12-25".to_string())
    );

    // Test TimestampMicros (UTC)
    let timestamp_utc = DateTime::parse_from_rfc3339("2023-12-25T10:30:45Z")
        .unwrap()
        .with_timezone(&Utc);
    let timestamp_variant = Variant::TimestampMicros(timestamp_utc);
    let timestamp_json = variant_to_json_string(&timestamp_variant).unwrap();
    assert!(timestamp_json.contains("2023-12-25T10:30:45"));
    assert!(timestamp_json.starts_with('"') && timestamp_json.ends_with('"'));

    // Test TimestampNtzMicros (naive datetime)
    let naive_timestamp = DateTime::from_timestamp(1703505045, 123456)
        .unwrap()
        .naive_utc();
    let naive_timestamp_variant = Variant::TimestampNtzMicros(naive_timestamp);
    let naive_json = variant_to_json_string(&naive_timestamp_variant).unwrap();
    assert!(naive_json.contains("2023-12-25"));
    assert!(naive_json.starts_with('"') && naive_json.ends_with('"'));
}

#[test]
fn test_binary_type_to_json() {
    // Test Binary data (encoded as base64)
    let binary_data = b"Hello, World!";
    let binary_variant = Variant::Binary(binary_data);

    let binary_json = variant_to_json_string(&binary_variant).unwrap();
    // Should be base64 encoded and quoted
    assert!(binary_json.starts_with('"') && binary_json.ends_with('"'));

    let binary_value = variant_to_json_value(&binary_variant).unwrap();
    assert!(matches!(binary_value, Value::String(_)));

    // Test empty binary
    let empty_binary = Variant::Binary(b"");
    let empty_json = variant_to_json_string(&empty_binary).unwrap();
    assert_eq!(empty_json, "\"\""); // Empty base64 string

    // Test binary with special bytes
    let special_binary = Variant::Binary(&[0, 255, 128, 64]);
    let special_json = variant_to_json_string(&special_binary).unwrap();
    assert!(special_json.starts_with('"') && special_json.ends_with('"'));
    assert!(special_json.len() > 2); // Should have content between quotes
}

#[test]
fn test_comprehensive_roundtrip_compatibility() {
    // Test that our JSON output can be parsed back by serde_json for all types
    let test_cases = vec![
        Variant::Null,
        Variant::BooleanTrue,
        Variant::BooleanFalse,
        Variant::Int8(42),
        Variant::Int16(1000),
        Variant::Int32(100000),
        Variant::Int64(10000000000),
        Variant::Float(3.5), // Use a value that can be represented exactly in f32
        Variant::Double(std::f64::consts::E),
        Variant::Decimal4 {
            integer: 12345,
            scale: 2,
        },
        Variant::Decimal8 {
            integer: 1234567890,
            scale: 3,
        },
        Variant::Decimal16 {
            integer: 123456789012345,
            scale: 4,
        },
        Variant::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
        Variant::String("test string"),
        Variant::ShortString(parquet_variant::ShortString::try_new("short").unwrap()),
        Variant::Binary(b"binary data"),
    ];

    for variant in test_cases {
        let json_string = variant_to_json_string(&variant).unwrap();

        // Ensure the JSON can be parsed back
        let parsed: Value = serde_json::from_str(&json_string).unwrap();

        // Ensure our direct Value conversion matches
        let direct_value = variant_to_json_value(&variant).unwrap();
        assert_eq!(parsed, direct_value, "Mismatch for variant: {:?}", variant);
    }
}

#[test]
fn test_string_escaping_edge_cases() {
    // Test various escape sequences
    let escaped_string = Variant::String("line1\nline2\ttab\"quote\"\\backslash");
    let expected_json = "\"line1\\nline2\\ttab\\\"quote\\\"\\\\backslash\"";
    assert_eq!(
        variant_to_json_string(&escaped_string).unwrap(),
        expected_json
    );

    // Test Unicode characters
    let unicode_string = Variant::String("Hello 世界 🌍");
    let json_result = variant_to_json_string(&unicode_string).unwrap();
    assert!(json_result.contains("Hello 世界 🌍"));
    assert!(json_result.starts_with('"') && json_result.ends_with('"'));
}

#[test]
fn test_json_roundtrip_compatibility() {
    // Test that our JSON output can be parsed back by serde_json
    let test_cases = vec![
        Variant::Null,
        Variant::BooleanTrue,
        Variant::BooleanFalse,
        Variant::Int8(0),
        Variant::Int8(127),
        Variant::Int8(-128),
        Variant::String(""),
        Variant::String("simple string"),
        Variant::String("string with\nnewlines\tand\ttabs"),
        Variant::ShortString(parquet_variant::ShortString::try_new("short").unwrap()),
    ];

    for variant in test_cases {
        let json_string = variant_to_json_string(&variant).unwrap();

        // Ensure the JSON can be parsed back
        let parsed: Value = serde_json::from_str(&json_string).unwrap();

        // Ensure our direct Value conversion matches
        let direct_value = variant_to_json_value(&variant).unwrap();
        assert_eq!(parsed, direct_value, "Mismatch for variant: {:?}", variant);
    }
}

#[test]
fn test_buffer_writing() {
    use parquet_variant::variant_to_json;
    use std::io::Write;

    let variant = Variant::String("test buffer writing");

    // Test writing to a Vec<u8>
    let mut buffer = Vec::new();
    variant_to_json(&mut buffer, &variant).unwrap();
    let result = String::from_utf8(buffer).unwrap();
    assert_eq!(result, "\"test buffer writing\"");

    // Test writing to a custom writer
    struct CustomWriter {
        data: Vec<u8>,
    }

    impl Write for CustomWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut custom_writer = CustomWriter { data: Vec::new() };
    variant_to_json(&mut custom_writer, &variant).unwrap();
    let result = String::from_utf8(custom_writer.data).unwrap();
    assert_eq!(result, "\"test buffer writing\"");
}

// Note: Tests for arrays and objects would require actual Variant data structures
// to be created, which would need the builder API to be implemented first.
// These tests demonstrate the primitive functionality that's currently working.
