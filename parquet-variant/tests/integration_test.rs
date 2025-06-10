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
    assert_eq!(variant_to_json_value(&bool_true).unwrap(), Value::Bool(true));
    assert_eq!(variant_to_json_value(&bool_false).unwrap(), Value::Bool(false));

    // Test integers
    let int_variant = Variant::Int8(42);
    assert_eq!(variant_to_json_string(&int_variant).unwrap(), "42");
    assert_eq!(variant_to_json_value(&int_variant).unwrap(), Value::Number(42.into()));

    let negative_int = Variant::Int8(-123);
    assert_eq!(variant_to_json_string(&negative_int).unwrap(), "-123");
    assert_eq!(variant_to_json_value(&negative_int).unwrap(), Value::Number((-123).into()));

    // Test strings
    let string_variant = Variant::String("hello world");
    assert_eq!(variant_to_json_string(&string_variant).unwrap(), "\"hello world\"");
    assert_eq!(variant_to_json_value(&string_variant).unwrap(), Value::String("hello world".to_string()));

    let short_string = Variant::ShortString("test");
    assert_eq!(variant_to_json_string(&short_string).unwrap(), "\"test\"");
    assert_eq!(variant_to_json_value(&short_string).unwrap(), Value::String("test".to_string()));
}

#[test]
fn test_string_escaping_edge_cases() {
    // Test various escape sequences
    let escaped_string = Variant::String("line1\nline2\ttab\"quote\"\\backslash");
    let expected_json = "\"line1\\nline2\\ttab\\\"quote\\\"\\\\backslash\"";
    assert_eq!(variant_to_json_string(&escaped_string).unwrap(), expected_json);

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
        Variant::ShortString("short"),
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