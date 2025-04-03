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
use std::collections::{HashSet, HashMap};

/// Creates a metadata binary vector for a JSON value according to the Arrow Variant specification
///
/// Metadata format:
///   - header: 1 byte (<version> | <sorted_strings> << 4 | (<offset_size_minus_one> << 6))
///   - dictionary_size: `offset_size` bytes (unsigned little-endian)
///   - offsets: `dictionary_size + 1` entries of `offset_size` bytes each
///   - bytes: UTF-8 encoded dictionary string values
pub fn create_metadata(json_value: &Value) -> Result<Vec<u8>, Error> {
    // Extract all keys from the JSON value (including nested)
    let keys = extract_all_keys(json_value);
    
    // For simplicity, we'll use 1 byte for offset_size
    let offset_size = 1;
    let offset_size_minus_one = offset_size - 1;
    
    // Create header: version=1, sorted=0, offset_size=1 (1 byte)
    let header = 0x01 | ((offset_size_minus_one as u8) << 6);
    
    // Start building the metadata
    let mut metadata = Vec::new();
    metadata.push(header);
    
    // Add dictionary_size (this is the number of keys)
    if keys.len() > 255 {
        return Err(Error::InvalidMetadata(
            "Too many keys for 1-byte offset_size".to_string(),
        ));
    }
    metadata.push(keys.len() as u8);
    
    // Pre-calculate offsets and prepare bytes
    let mut bytes = Vec::new();
    let mut offsets = Vec::with_capacity(keys.len() + 1);
    let mut current_offset = 0u32;
    
    offsets.push(current_offset);
    
    // Sort keys to ensure consistent ordering
    let mut sorted_keys: Vec<_> = keys.into_iter().collect();
    sorted_keys.sort();
    
    for key in sorted_keys {
        bytes.extend_from_slice(key.as_bytes());
        current_offset += key.len() as u32;
        offsets.push(current_offset);
    }
    
    // Add all offsets
    for offset in &offsets {
        metadata.push(*offset as u8);
    }
    
    // Add dictionary bytes
    metadata.extend_from_slice(&bytes);
    
    Ok(metadata)
}

/// Extracts all keys from a JSON value, including nested objects
fn extract_all_keys(json_value: &Value) -> HashSet<String> {
    let mut keys = HashSet::new();
    
    match json_value {
        Value::Object(map) => {
            for (key, value) in map {
                keys.insert(key.clone());
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
    let _sorted = (header >> 4) & 0x01 != 0;
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
        let json = json!({"a": 1, "b": 2});
        let metadata = create_metadata(&json).unwrap();
        
        // Expected structure:
        // header: 0x01 (version=1, sorted=0, offset_size=1)
        // dictionary_size: 2
        // offsets: [0, 1, 2] (3 offsets for 2 strings)
        // bytes: "ab"
        
        assert_eq!(metadata[0], 0x01); // header
        assert_eq!(metadata[1], 0x02); // dictionary_size
        assert_eq!(metadata[2], 0x00); // first offset
        assert_eq!(metadata[3], 0x01); // second offset
        assert_eq!(metadata[4], 0x02); // third offset (total length)
        assert_eq!(metadata[5], b'a'); // first key
        assert_eq!(metadata[6], b'b'); // second key
    }

    #[test]
    fn test_normal_object() {
        let json = json!({
            "first_name": "John",
            "last_name": "Smith",
            "email": "john.smith@example.com"
        });
        let metadata = create_metadata(&json).unwrap();
        
        // Expected structure:
        // header: 0x01 (version=1, sorted=0, offset_size=1)
        // dictionary_size: 3
        // offsets: [0, 5, 15, 24] (4 offsets for 3 strings)
        // bytes: "emailfirst_namelast_name"
        
        assert_eq!(metadata[0], 0x01); // header
        assert_eq!(metadata[1], 0x03); // dictionary_size
        assert_eq!(metadata[2], 0x00); // offset for "email"
        assert_eq!(metadata[3], 0x05); // offset for "first_name"
        assert_eq!(metadata[4], 0x0F); // offset for "last_name"
        assert_eq!(metadata[5], 0x18); // total length
        assert_eq!(&metadata[6..], b"emailfirst_namelast_name"); // dictionary bytes
    }

    #[test]
    fn test_nested_object() {
        let json = json!({
            "a": {
                "b": {
                    "c": {
                        "d": 1,
                        "e": 2
                    },
                    "f": 3
                },
                "g": 4
            },
            "h": 5
        });
        let metadata = create_metadata(&json).unwrap();
        
        // Expected structure:
        // header: 0x01
        // dictionary_size: 8 (a, b, c, d, e, f, g, h)
        // offsets: [0, 1, 2, 3, 4, 5, 6, 7, 8]
        // bytes: "abcdefgh"
        
        assert_eq!(metadata[0], 0x01); // header
        assert_eq!(metadata[1], 0x08); // dictionary_size = 8
        assert_eq!(metadata[2], 0x00); // offset for "a"
        assert_eq!(metadata[3], 0x01); // offset for "b"
        assert_eq!(metadata[4], 0x02); // offset for "c"
        assert_eq!(metadata[5], 0x03); // offset for "d"
        assert_eq!(metadata[6], 0x04); // offset for "e"
        assert_eq!(metadata[7], 0x05); // offset for "f"
        assert_eq!(metadata[8], 0x06); // offset for "g"
        assert_eq!(metadata[9], 0x07); // offset for "h"
        assert_eq!(metadata[10], 0x08); // total length
        assert_eq!(&metadata[11..19], b"abcdefgh"); // dictionary bytes
    }

    #[test]
    fn test_nested_array() {
        let json = json!({
            "arr": [
                {"x": 1, "y": 2},
                {"z": 3}
            ]
        });
        let metadata = create_metadata(&json).unwrap();
    
        // header: 0x01 (version=1, sorted=0, offset_size=1)
        // dictionary_size: 4
        // offsets: [0, 3, 4, 5, 6]
        // bytes: "arrxyz"
    
        assert_eq!(metadata[0], 0x01); // header
        assert_eq!(metadata[1], 0x04); // dictionary_size = 4
    
        assert_eq!(metadata[2], 0x00); // offset for "arr"
        assert_eq!(metadata[3], 0x03); // offset for "x"
        assert_eq!(metadata[4], 0x04); // offset for "y"
        assert_eq!(metadata[5], 0x05); // offset for "z"
        assert_eq!(metadata[6], 0x06); // total length of bytes
    
        assert_eq!(&metadata[7..13], b"arrxyz"); // dictionary bytes
    }
    
    #[test]
    fn test_complex_nested() {
        let json = json!({
            "outer": {
                "middle": {
                    "inner": 1
                },
                "array": [
                    {"key": "value"},
                    {"another": true}
                ]
            }
        });
        let metadata = create_metadata(&json).unwrap();
    
        // dictionary key: another, array, inner, key, middle, outer
        // bytes: "anotherarrayinnerkeymiddleouter"
        // offsets: [0, 7, 12, 17, 20, 26, 31]
    
        assert_eq!(metadata[0], 0x01); // header
        assert_eq!(metadata[1], 0x06); // dictionary_size = 6
    
        assert_eq!(metadata[2], 0x00); // "another"
        assert_eq!(metadata[3], 0x07); // "array"
        assert_eq!(metadata[4], 0x0C); // "inner"
        assert_eq!(metadata[5], 0x11); // "key"
        assert_eq!(metadata[6], 0x14); // "middle"
        assert_eq!(metadata[7], 0x1A); // "outer"
        assert_eq!(metadata[8], 0x1F); // total bytes length = 31
    
        assert_eq!(
            &metadata[9..40],
            b"anotherarrayinnerkeymiddleouter"
        );
    }
    
} 