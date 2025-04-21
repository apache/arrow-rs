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

//! Utilities for working with Variant metadata

use crate::error::Error;
use serde_json::Value;
use std::collections::HashMap;
use arrow_array::{
    Array, ArrayRef, BinaryArray, StructArray,
};
use arrow_array::builder::{BinaryBuilder, LargeBinaryBuilder};

/// Creates a metadata binary vector for a JSON value according to the Arrow Variant specification
///
/// Metadata format:
///   - header: 1 byte (<version> | <sorted_strings> << 4 | (<offset_size_minus_one> << 6))
///   - dictionary_size: `offset_size` bytes (unsigned little-endian)
///   - offsets: `dictionary_size + 1` entries of `offset_size` bytes each
///   - bytes: UTF-8 encoded dictionary string values
///
/// # Arguments
///
/// * `json_value` - The JSON value to create metadata for
/// * `sort_keys` - If true, keys will be sorted lexicographically; if false, keys will be used in their original order
pub fn create_metadata(json_value: &Value, sort_keys: bool) -> Result<Vec<u8>, Error> {
    // Extract all keys from the JSON value (including nested)
    let keys = extract_all_keys(json_value);
    
    // Convert keys to a vector and optionally sort them
    let mut keys: Vec<_> = keys.into_iter().collect();
    if sort_keys {
        keys.sort();
    }
    
    // Calculate the total size of all dictionary strings
    let mut dictionary_string_size = 0u32;
    for key in &keys {
        dictionary_string_size += key.len() as u32;
    }
    
    // Determine the minimum integer size required for offsets
    // The largest offset is the one-past-the-end value, which is total string size
    let max_size = std::cmp::max(dictionary_string_size, (keys.len() + 1) as u32);
    let offset_size = get_min_integer_size(max_size as usize);
    let offset_size_minus_one = offset_size - 1;
    
    // Set sorted_strings based on whether keys are sorted in metadata
    let sorted_strings = if sort_keys { 1 } else { 0 };
    
    // Create header: version=1, sorted_strings based on parameter, offset_size based on calculation
    let header = 0x01 | (sorted_strings << 4) | ((offset_size_minus_one as u8) << 6);
    
    // Start building the metadata
    let mut metadata = Vec::new();
    metadata.push(header);
    
    // Add dictionary_size (this is the number of keys)
    // Write the dictionary size using the calculated offset_size
    for i in 0..offset_size {
        metadata.push(((keys.len() >> (8 * i)) & 0xFF) as u8);
    }
    
    // Pre-calculate offsets and prepare bytes
    let mut bytes = Vec::new();
    let mut offsets = Vec::with_capacity(keys.len() + 1);
    let mut current_offset = 0u32;
    
    offsets.push(current_offset);
    
    for key in keys {
        bytes.extend_from_slice(key.as_bytes());
        current_offset += key.len() as u32;
        offsets.push(current_offset);
    }
    
    // Add all offsets using the calculated offset_size
    for offset in &offsets {
        for i in 0..offset_size {
            metadata.push(((*offset >> (8 * i)) & 0xFF) as u8);
        }
    }
    
    // Add dictionary bytes
    metadata.extend_from_slice(&bytes);
    
    Ok(metadata)
}

/// Determines the minimum integer size required to represent a value
fn get_min_integer_size(value: usize) -> usize {
    if value <= 255 {
        1
    } else if value <= 65535 {
        2
    } else if value <= 16777215 {
        3
    } else {
        4
    }
}

/// Extracts all keys from a JSON value, including nested objects
fn extract_all_keys(json_value: &Value) -> Vec<String> {
    let mut keys = Vec::new();
    
    match json_value {
        Value::Object(map) => {
            for (key, value) in map {
                keys.push(key.clone());
                keys.extend(extract_all_keys(value));
            }
        }
        Value::Array(arr) => {
            for value in arr {
                keys.extend(extract_all_keys(value));
            }
        }
        _ => {} // No keys for primitive values
    }
    
    keys
}

/// Parses metadata binary into a map of keys to their indices
pub fn parse_metadata(metadata: &[u8]) -> Result<HashMap<String, usize>, Error> {
    if metadata.is_empty() {
        return Err(Error::InvalidMetadata("Empty metadata".to_string()));
    }
    
    // Parse header
    let header = metadata[0];
    let version = header & 0x0F;
    let _sorted_strings = (header >> 4) & 0x01 != 0;
    let offset_size_minus_one = (header >> 6) & 0x03;
    let offset_size = (offset_size_minus_one + 1) as usize;
    
    if version != 1 {
        return Err(Error::InvalidMetadata(format!("Unsupported version: {}", version)));
    }
    
    if metadata.len() < 1 + offset_size {
        return Err(Error::InvalidMetadata("Metadata too short for dictionary size".to_string()));
    }
    
    // Parse dictionary_size
    let mut dictionary_size = 0u32;
    for i in 0..offset_size {
        dictionary_size |= (metadata[1 + i] as u32) << (8 * i);
    }
    
    // Parse offsets
    let offset_start = 1 + offset_size;
    let offset_end = offset_start + (dictionary_size as usize + 1) * offset_size;
    
    if metadata.len() < offset_end {
        return Err(Error::InvalidMetadata("Metadata too short for offsets".to_string()));
    }
    
    let mut offsets = Vec::with_capacity(dictionary_size as usize + 1);
    for i in 0..=dictionary_size {
        let offset_pos = offset_start + (i as usize * offset_size);
        let mut offset = 0u32;
        for j in 0..offset_size {
            offset |= (metadata[offset_pos + j] as u32) << (8 * j);
        }
        offsets.push(offset as usize);
    }
    
    // Parse dictionary strings
    let mut result = HashMap::new();
    for i in 0..dictionary_size as usize {
        let start = offset_end + offsets[i];
        let end = offset_end + offsets[i + 1];
        
        if end > metadata.len() {
            return Err(Error::InvalidMetadata("Invalid string offset".to_string()));
        }
        
        let key = std::str::from_utf8(&metadata[start..end])
            .map_err(|e| Error::InvalidMetadata(format!("Invalid UTF-8: {}", e)))?
            .to_string();
            
        result.insert(key, i);
    }
    
    Ok(result)
}

/// Creates simple metadata for testing purposes
///
/// This creates valid metadata with a single key "key"
pub fn create_test_metadata() -> Vec<u8> {
    vec![
        0x01,  // header: version=1, sorted=0, offset_size=1
        0x01,  // dictionary_size = 1
        0x00,  // offset 0
        0x03,  // offset 3
        b'k', b'e', b'y'  // dictionary bytes
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_object() {
        let value = json!({
            "a": 1,
            "b": 2,
            "c": 3
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 3 keys
        assert_eq!(metadata[1], 3);
        
        // Offsets: [0, 1, 2, 3] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // Third offset
        assert_eq!(metadata[5], 3); // One-past-the-end offset
        
        // Dictionary bytes: "abc"
        assert_eq!(&metadata[6..9], b"abc");
    }

    #[test]
    fn test_normal_object() {
        let value = json!({
            "a": 1,
            "b": 2,
            "c": 3
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 3 keys
        assert_eq!(metadata[1], 3);
        
        // Offsets: [0, 1, 2, 3] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // Third offset
        assert_eq!(metadata[5], 3); // One-past-the-end offset
        
        // Dictionary bytes: "abc"
        assert_eq!(&metadata[6..9], b"abc");
    }

    #[test]
    fn test_complex_object() {
        let value = json!({
            "first_name": "John",
            "last_name": "Smith",
            "email": "john.smith@example.com"
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 3 keys
        assert_eq!(metadata[1], 3);
        
        // Offsets: [0, 5, 15, 24] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset for "email"
        assert_eq!(metadata[3], 5); // Second offset for "first_name"
        assert_eq!(metadata[4], 15); // Third offset for "last_name"
        assert_eq!(metadata[5], 24); // One-past-the-end offset
        
        // Dictionary bytes: "emailfirst_namelast_name"
        assert_eq!(&metadata[6..30], b"emailfirst_namelast_name");
    }

    #[test]
    fn test_nested_object() {
        let value = json!({
            "a": {
                "b": 1,
                "c": 2
            },
            "d": 3
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 4 keys (a, b, c, d)
        assert_eq!(metadata[1], 4);
        
        // Offsets: [0, 1, 2, 3, 4] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // Third offset
        assert_eq!(metadata[5], 3); // Fourth offset
        assert_eq!(metadata[6], 4); // One-past-the-end offset
        
        // Dictionary bytes: "abcd"
        assert_eq!(&metadata[7..11], b"abcd");
    }

    #[test]
    fn test_nested_array() {
        let value = json!({
            "a": [1, 2, 3],
            "b": 4
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 2 keys (a, b)
        assert_eq!(metadata[1], 2);
        
        // Offsets: [0, 1, 2] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // One-past-the-end offset
        
        // Dictionary bytes: "ab"
        assert_eq!(&metadata[5..7], b"ab");
    }

    #[test]
    fn test_complex_nested() {
        let value = json!({
            "a": {
                "b": [1, 2, 3],
                "c": 4
            },
            "d": 5
        });

        let metadata = create_metadata(&value, false).unwrap();
        
        // Header: version=1, sorted_strings=0, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x01);
        
        // Dictionary size: 4 keys (a, b, c, d)
        assert_eq!(metadata[1], 4);
        
        // Offsets: [0, 1, 2, 3, 4] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // Third offset
        assert_eq!(metadata[5], 3); // Fourth offset
        assert_eq!(metadata[6], 4); // One-past-the-end offset
        
        // Dictionary bytes: "abcd"
        assert_eq!(&metadata[7..11], b"abcd");
    }

    #[test]
    fn test_sorted_keys() {
        let value = json!({
            "c": 3,
            "a": 1,
            "b": 2
        });

        let metadata = create_metadata(&value, true).unwrap();
        
        // Header: version=1, sorted_strings=1, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x11);
        
        // Dictionary size: 3 keys
        assert_eq!(metadata[1], 3);
        
        // Offsets: [0, 1, 2, 3] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset
        assert_eq!(metadata[3], 1); // Second offset
        assert_eq!(metadata[4], 2); // Third offset
        assert_eq!(metadata[5], 3); // One-past-the-end offset
        
        // Dictionary bytes: "abc" (sorted)
        assert_eq!(&metadata[6..9], b"abc");
    }

    #[test]
    fn test_sorted_complex_object() {
        let value = json!({
            "first_name": "John",
            "email": "john.smith@example.com",
            "last_name": "Smith"
        });

        let metadata = create_metadata(&value, true).unwrap();
        
        // Header: version=1, sorted_strings=1, offset_size=1 (1 byte)
        assert_eq!(metadata[0], 0x11);
        
        // Dictionary size: 3 keys
        assert_eq!(metadata[1], 3);
        
        // Offsets: [0, 5, 15, 24] (1 byte each)
        assert_eq!(metadata[2], 0); // First offset for "email"
        assert_eq!(metadata[3], 5); // Second offset for "first_name"
        assert_eq!(metadata[4], 15); // Third offset for "last_name"
        assert_eq!(metadata[5], 24); // One-past-the-end offset
        
        // Dictionary bytes: "emailfirst_namelast_name"
        assert_eq!(&metadata[6..30], b"emailfirst_namelast_name");
    }
} 