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

//! Integration tests and utilities for the arrow-variant crate

use arrow_array::{Array, StructArray};
use arrow_schema::extension::Variant;
use serde_json::{json, Value};

use crate::error::Error;
use crate::reader::{from_json, from_json_array};
use crate::writer::{to_json, to_json_array};

/// Creates a test Variant from a JSON value
pub fn create_test_variant(json_value: Value) -> Result<Variant, Error> {
    let json_str = json_value.to_string();
    from_json(&json_str)
}

/// Creates a test StructArray with variant data from a list of JSON values
pub fn create_test_variant_array(json_values: Vec<Value>) -> Result<StructArray, Error> {
    let json_strings: Vec<String> = json_values.into_iter().map(|v| v.to_string()).collect();
    let str_refs: Vec<&str> = json_strings.iter().map(|s| s.as_str()).collect();
    from_json_array(&str_refs)
}

/// Validates that a JSON value can be roundtripped through Variant
pub fn validate_variant_roundtrip(json_value: Value) -> Result<(), Error> {
    let json_str = json_value.to_string();
    
    // Convert JSON to Variant
    let variant = from_json(&json_str)?;
    
    // Convert Variant back to JSON
    let result_json = to_json(&variant)?;
    
    // Parse both to Value to compare them structurally
    let original: Value = serde_json::from_str(&json_str).unwrap();
    let result: Value = serde_json::from_str(&result_json).unwrap();
    
    assert_eq!(original, result, "Original and result JSON should be equal");
    
    Ok(())
}

/// Creates a sample Variant with a complex JSON structure
pub fn create_sample_variant() -> Result<Variant, Error> {
    let json = json!({
        "name": "John Doe",
        "age": 30,
        "is_active": true,
        "scores": [95, 87, 92],
        "null_value": null,
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zip": 12345
        }
    });
    
    create_test_variant(json)
}

/// Creates a sample StructArray with variant data containing multiple entries
pub fn create_sample_variant_array() -> Result<StructArray, Error> {
    let json_values = vec![
        json!({
            "name": "John",
            "age": 30,
            "tags": ["developer", "rust"]
        }),
        json!({
            "name": "Jane",
            "age": 28,
            "tags": ["designer", "ui/ux"]
        }),
        json!({
            "name": "Bob",
            "age": 22,
            "tags": ["intern", "student"]
        })
    ];
    
    create_test_variant_array(json_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_roundtrip_primitives() -> Result<(), Error> {
        // Test null
        validate_variant_roundtrip(json!(null))?;
        
        // Test boolean
        validate_variant_roundtrip(json!(true))?;
        validate_variant_roundtrip(json!(false))?;
        
        // Test integers
        validate_variant_roundtrip(json!(0))?;
        validate_variant_roundtrip(json!(42))?;
        validate_variant_roundtrip(json!(-1000))?;
        validate_variant_roundtrip(json!(12345678))?;
        
        // Test floating point
        validate_variant_roundtrip(json!(3.14159))?;
        validate_variant_roundtrip(json!(-2.71828))?;
        
        // Test string
        validate_variant_roundtrip(json!("Hello, World!"))?;
        validate_variant_roundtrip(json!(""))?;
        
        Ok(())
    }
    
    #[test]
    fn test_roundtrip_arrays() -> Result<(), Error> {
        // Empty array
        validate_variant_roundtrip(json!([]))?;
        
        // Homogeneous arrays
        validate_variant_roundtrip(json!([1, 2, 3, 4, 5]))?;
        validate_variant_roundtrip(json!(["a", "b", "c"]))?;
        validate_variant_roundtrip(json!([true, false, true]))?;
        
        // Heterogeneous arrays
        validate_variant_roundtrip(json!([1, "text", true, null]))?;
        
        // Nested arrays
        validate_variant_roundtrip(json!([[1, 2], [3, 4], [5, 6]]))?;
        
        Ok(())
    }
    
    #[test]
    fn test_roundtrip_objects() -> Result<(), Error> {
        // Empty object
        validate_variant_roundtrip(json!({}))?;

        
        // Simple object
        validate_variant_roundtrip(json!({"name": "John", "age": 30}))?;
        
        // Object with different types
        validate_variant_roundtrip(json!({
            "name": "John",
            "age": 30,
            "is_active": true,
            "email": null
        }))?;
        
        // Nested object
        validate_variant_roundtrip(json!({
            "person": {
                "name": "John",
                "age": 30
            },
            "company": {
                "name": "ACME Inc.",
                "location": "New York"
            }
        }))?;
        
        Ok(())
    }
    
    #[test]
    fn test_roundtrip_complex() -> Result<(), Error> {
        // Complex nested structure
        validate_variant_roundtrip(json!({
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
            ],
            "metadata": {
                "created_at": "2023-01-01",
                "updated_at": null,
                "tags": ["user", "customer"]
            }
        }))?;
        
        Ok(())
    }
    
    #[test]
    fn test_variant_array_roundtrip() -> Result<(), Error> {
        // Create variant array
        let variant_array = create_sample_variant_array()?;
        
        // Convert to JSON strings
        let json_strings = to_json_array(&variant_array)?;
        
        // Convert back to variant array
        let str_refs: Vec<&str> = json_strings.iter().map(|s| s.as_str()).collect();
        let round_trip_array = from_json_array(&str_refs)?;
        
        // Verify length
        assert_eq!(variant_array.len(), round_trip_array.len());
        
        // Convert both arrays to JSON and compare
        let original_json = to_json_array(&variant_array)?;
        let result_json = to_json_array(&round_trip_array)?;
        
        for (i, (original, result)) in original_json.iter().zip(result_json.iter()).enumerate() {
            let original_value: Value = serde_json::from_str(original).unwrap();
            let result_value: Value = serde_json::from_str(result).unwrap();
            
            assert_eq!(
                original_value, 
                result_value,
                "JSON values at index {} should be equal", 
                i
            );
        }
        
        Ok(())
    }
} 