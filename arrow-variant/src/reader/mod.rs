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

//! Reading JSON and converting to Variant
//! 
use arrow_array::{Array, StructArray};
use arrow_schema::extension::Variant;
use serde_json::Value;
use crate::error::Error;
use crate::metadata::{create_metadata, parse_metadata};
use crate::encoder::encode_json;
use crate::variant_utils::create_variant_array;
#[allow(unused_imports)]
use crate::decoder::decode_value;
#[allow(unused_imports)]
use std::collections::HashMap;

/// Converts a JSON string to a Variant
///
/// # Example
///
/// ```
/// use arrow_variant::from_json;
///
/// let json_str = r#"{"name": "John", "age": 30, "city": "New York"}"#;
/// let variant = from_json(json_str).unwrap();
///
/// // Access variant metadata and value
/// println!("Metadata length: {}", variant.metadata().len());
/// println!("Value length: {}", variant.value().len());
/// ```
pub fn from_json(json_str: &str) -> Result<Variant, Error> {
    // Parse the JSON string
    let value: Value = serde_json::from_str(json_str)?;
    
    // Use the value-based function
    from_json_value(&value)
}

/// Converts an array of JSON strings to a StructArray with variant extension type
///
/// # Example
///
/// ```
/// use arrow_variant::from_json_array;
/// use arrow_array::array::Array;
///
/// let json_strings = vec![
///     r#"{"name": "John", "age": 30}"#,
///     r#"{"name": "Jane", "age": 28}"#,
/// ];
///
/// let variant_array = from_json_array(&json_strings).unwrap();
/// assert_eq!(variant_array.len(), 2);
/// ```
pub fn from_json_array(json_strings: &[&str]) -> Result<StructArray, Error> {
    if json_strings.is_empty() {
        return Err(Error::EmptyInput);
    }
    
    // Parse each JSON string to a Value
    let values: Result<Vec<_>, _> = json_strings
        .iter()
        .map(|json_str| serde_json::from_str::<Value>(json_str).map_err(Error::from))
        .collect();
    
    // Convert the values to a StructArray with variant extension type
    from_json_value_array(&values?)
}

/// Converts a JSON Value object directly to a Variant
///
/// # Example
///
/// ```
/// use arrow_variant::from_json_value;
/// use serde_json::json;
///
/// let value = json!({"name": "John", "age": 30, "city": "New York"});
/// let variant = from_json_value(&value).unwrap();
///
/// // Access variant metadata and value
/// println!("Metadata length: {}", variant.metadata().len());
/// println!("Value length: {}", variant.value().len());
/// ```
pub fn from_json_value(value: &Value) -> Result<Variant, Error> {
    // Create metadata from the JSON value
    let metadata = create_metadata(value, false)?;
    
    // Parse the metadata to get a key-to-id mapping
    let key_mapping = parse_metadata(&metadata)?;
    
    // Encode the JSON value to binary format
    let value_bytes = encode_json(value, &key_mapping)?;
    
    // Create the Variant with metadata and value
    Ok(Variant::new(metadata, value_bytes))
}

/// Converts an array of JSON Value objects to a StructArray with variant extension type
///
/// # Example
///
/// ```
/// use arrow_variant::from_json_value_array;
/// use serde_json::json;
/// use arrow_array::array::Array;
///
/// let values = vec![
///     json!({"name": "John", "age": 30}),
///     json!({"name": "Jane", "age": 28}),
/// ];
///
/// let variant_array = from_json_value_array(&values).unwrap();
/// assert_eq!(variant_array.len(), 2);
/// ```
pub fn from_json_value_array(values: &[Value]) -> Result<StructArray, Error> {
    if values.is_empty() {
        return Err(Error::EmptyInput);
    }
    
    // Convert each JSON value to a Variant
    let variants: Result<Vec<_>, _> = values
        .iter()
        .map(|value| from_json_value(value))
        .collect();
    
    let variants = variants?;
    
    // Create a StructArray with the variants
    create_variant_array(variants)
        .map_err(|e| Error::VariantArrayCreation(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant_utils::get_variant;
    
    #[test]
    fn test_from_json() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let variant = from_json(json_str).unwrap();
        
        // Verify the metadata has the expected keys
        assert!(!variant.metadata().is_empty());
        
        // Verify the value is not empty
        assert!(!variant.value().is_empty());
        
        // Verify the first byte is an object header
        // Object type (2) with default sizes
        assert_eq!(variant.value()[0], 0b00000010);
    }
    
    #[test]
    fn test_from_json_array() {
        let json_strings = vec![
            r#"{"name": "John", "age": 30}"#,
            r#"{"name": "Jane", "age": 28}"#,
        ];
        
        let variant_array = from_json_array(&json_strings).unwrap();
        
        // Verify array length
        assert_eq!(variant_array.len(), 2);
        
        // Verify the values are properly encoded
        for i in 0..variant_array.len() {
            let variant = get_variant(&variant_array, i).unwrap();
            assert!(!variant.value().is_empty());
            // First byte should be an object header
            assert_eq!(variant.value()[0], 0b00000010);
        }
    }
    
    #[test]
    fn test_from_json_error() {
        let invalid_json = r#"{"name": "John", "age": }"#; // Missing value
        let result = from_json(invalid_json);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_complex_json() {
        let json_str = r#"{
            "name": "John",
            "age": 30,
            "active": true,
            "scores": [85, 90, 78],
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "zip": 12345
            },
            "tags": ["developer", "rust"]
        }"#;
        
        let variant = from_json(json_str).unwrap();
        
        // Verify the metadata has the expected keys
        assert!(!variant.metadata().is_empty());
        
        // Verify the value is not empty
        assert!(!variant.value().is_empty());
        
        // Verify the first byte is an object header
        // Object type (2) with default sizes
        assert_eq!(variant.value()[0], 0b00000010);
    }
} 