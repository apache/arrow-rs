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

//! Writing Variant data to JSON

use arrow_array::{Array, StructArray};
use arrow_schema::extension::Variant;
use serde_json::Value;
use crate::error::Error;
use crate::decoder::decode_json;
use crate::variant_utils::get_variant;

/// Converts a Variant to a JSON Value
///
/// # Examples
///
/// ```
/// use arrow_variant::reader::from_json;
/// use arrow_variant::writer::to_json_value;
/// use serde_json::json;
///
/// let json_str = r#"{"name":"John","age":30}"#;
/// let variant = from_json(json_str).unwrap();
/// let value = to_json_value(&variant).unwrap();
/// assert_eq!(value, json!({"name":"John","age":30}));
/// ```
pub fn to_json_value(variant: &Variant) -> Result<Value, Error> {
    // Decode the variant binary data to a JSON value
    decode_json(variant.value(), variant.metadata())
}

/// Converts a StructArray with variant extension type to an array of JSON Values
///
/// # Example
///
/// ```
/// use arrow_variant::{from_json_array, to_json_value_array};
/// use serde_json::json;
///
/// let json_strings = vec![
///     r#"{"name": "John", "age": 30}"#,
///     r#"{"name": "Jane", "age": 28}"#,
/// ];
///
/// let variant_array = from_json_array(&json_strings).unwrap();
/// let values = to_json_value_array(&variant_array).unwrap();
/// assert_eq!(values, vec![
///     json!({"name": "John", "age": 30}),
///     json!({"name": "Jane", "age": 28})
/// ]);
/// ```
pub fn to_json_value_array(variant_array: &StructArray) -> Result<Vec<Value>, Error> {
    let mut result = Vec::with_capacity(variant_array.len());
    for i in 0..variant_array.len() {
        if variant_array.is_null(i) {
            result.push(Value::Null);
            continue;
        }
        
        let variant = get_variant(variant_array, i)
            .map_err(|e| Error::VariantRead(e.to_string()))?;
        result.push(to_json_value(&variant)?);
    }
    Ok(result)
}

/// Converts a Variant to a JSON string
///
/// # Examples
///
/// ```
/// use arrow_variant::reader::from_json;
/// use arrow_variant::writer::to_json;
///
/// let json_str = r#"{"name":"John","age":30}"#;
/// let variant = from_json(json_str).unwrap();
/// let result = to_json(&variant).unwrap();
/// assert_eq!(serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(json_str).unwrap()).unwrap(),
///            serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&result).unwrap()).unwrap());
/// ```
pub fn to_json(variant: &Variant) -> Result<String, Error> {
    // Use the value-based function and convert to string
    let value = to_json_value(variant)?;
    Ok(value.to_string())
}

/// Converts a StructArray with variant extension type to an array of JSON strings
///
/// # Example
///
/// ```
/// use arrow_variant::{from_json_array, to_json_array};
///
/// let json_strings = vec![
///     r#"{"name": "John", "age": 30}"#,
///     r#"{"name": "Jane", "age": 28}"#,
/// ];
///
/// let variant_array = from_json_array(&json_strings).unwrap();
/// let json_array = to_json_array(&variant_array).unwrap();
///
/// // Note that the output JSON strings may have different formatting
/// // but they are semantically equivalent
/// ```
pub fn to_json_array(variant_array: &StructArray) -> Result<Vec<String>, Error> {
    // Use the value-based function and convert each value to a string
    to_json_value_array(variant_array).map(|values| 
        values.into_iter().map(|v| v.to_string()).collect()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::from_json;
    use serde_json::json;
    
    #[test]
    fn test_to_json() {
        let json_str = r#"{"name":"John","age":30}"#;
        let variant = from_json(json_str).unwrap();
        
        let result = to_json(&variant).unwrap();
        
        // Parse both to Value to compare them structurally
        let original: Value = serde_json::from_str(json_str).unwrap();
        let result_value: Value = serde_json::from_str(&result).unwrap();
        
        assert_eq!(original, result_value);
    }
    
    #[test]
    fn test_to_json_array() {
        let json_strings = vec![
            r#"{"name":"John","age":30}"#,
            r#"{"name":"Jane","age":28}"#,
        ];
        
        // Create variant array from JSON strings
        let variant_array = crate::reader::from_json_array(&json_strings).unwrap();
        
        // Convert back to JSON
        let result = to_json_array(&variant_array).unwrap();
        
        // Verify the result
        assert_eq!(result.len(), 2);
        
        // Parse both to Value to compare them structurally
        for (i, (original, result)) in json_strings.iter().zip(result.iter()).enumerate() {
            let original_value: Value = serde_json::from_str(original).unwrap();
            let result_value: Value = serde_json::from_str(result).unwrap();
            
            assert_eq!(
                original_value, 
                result_value,
                "JSON values at index {} should be equal", 
                i
            );
        }
    }
    
    #[test]
    fn test_roundtrip() {
        let complex_json = json!({
            "array": [1, 2, 3],
            "nested": {"a": true, "b": null},
            "string": "value"
        });
        
        let complex_str = complex_json.to_string();
        
        let variant = from_json(&complex_str).unwrap();
        let json = to_json(&variant).unwrap();
        
        // Parse both to Value to compare them structurally
        let original: Value = serde_json::from_str(&complex_str).unwrap();
        let result: Value = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, result);
    }
    
    #[test]
    fn test_special_characters() {
        // Test with JSON containing special characters
        let special_json = json!({
            "unicode": "こんにちは世界",  // Hello world in Japanese
            "escaped": "Line 1\nLine 2\t\"quoted\"",
            "emoji": "🚀🌟⭐"
        });
        
        let special_str = special_json.to_string();
        
        let variant = from_json(&special_str).unwrap();
        let json = to_json(&variant).unwrap();
        
        // Parse both to Value to compare them structurally
        let original: Value = serde_json::from_str(&special_str).unwrap();
        let result: Value = serde_json::from_str(&json).unwrap();
        
        assert_eq!(original, result);
    }
} 