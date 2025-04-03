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
use arrow_array::{Array, VariantArray};
use arrow_schema::extension::Variant;
use serde_json::Value;
use std::sync::Arc;

use crate::error::Error;
use crate::metadata::create_metadata;

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
/// println!("Value: {}", std::str::from_utf8(variant.value()).unwrap());
/// ```
pub fn from_json(json_str: &str) -> Result<Variant, Error> {
    // Parse the JSON string
    let value: Value = serde_json::from_str(json_str)?;
    
    // Create metadata from the JSON value
    let metadata = create_metadata(&value)?;
    
    // Use the original JSON string as the value
    let value_bytes = json_str.as_bytes().to_vec();
    
    // Create the Variant with metadata and value
    Ok(Variant::new(metadata, value_bytes))
}

/// Converts an array of JSON strings to a VariantArray
///
/// # Example
///
/// ```
/// use arrow_variant::from_json_array;
///
/// let json_strings = vec![
///     r#"{"name": "John", "age": 30}"#,
///     r#"{"name": "Jane", "age": 28}"#,
/// ];
///
/// let variant_array = from_json_array(&json_strings).unwrap();
/// assert_eq!(variant_array.len(), 2);
/// ```
pub fn from_json_array(json_strings: &[&str]) -> Result<VariantArray, Error> {
    if json_strings.is_empty() {
        return Err(Error::EmptyInput);
    }
    
    // Convert each JSON string to a Variant
    let variants: Result<Vec<_>, _> = json_strings
        .iter()
        .map(|json_str| from_json(json_str))
        .collect();
    
    let variants = variants?;
    
    // Use the metadata from the first variant for the VariantArray
    let variant_type = Variant::new(variants[0].metadata().to_vec(), vec![]);
    
    // Create the VariantArray
    VariantArray::from_variants(variant_type, variants)
        .map_err(|e| Error::VariantArrayCreation(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_from_json() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let variant = from_json(json_str).unwrap();
        
        // Verify the metadata has the expected keys
        assert!(!variant.metadata().is_empty());
        
        // Verify the value contains the original JSON string
        let value_str = std::str::from_utf8(variant.value()).unwrap();
        assert_eq!(value_str, json_str);
    }
    
    #[test]
    fn test_metadata_from_json_array() {
        let json_strings = vec![
            r#"{"name": "John", "age": 30}"#,
            r#"{"name": "Jane", "age": 28}"#,
        ];
        
        let variant_array = from_json_array(&json_strings).unwrap();
        
        // Verify array length
        assert_eq!(variant_array.len(), 2);
        
        // Verify the values
        for (i, json_str) in json_strings.iter().enumerate() {
            let variant = variant_array.value(i).unwrap();
            let value_str = std::str::from_utf8(variant.value()).unwrap();
            assert_eq!(value_str, *json_str);
        }
    }
    
    #[test]
    fn test_from_json_error() {
        let invalid_json = r#"{"name": "John", "age": }"#; // Missing value
        let result = from_json(invalid_json);
        assert!(result.is_err());
    }
} 