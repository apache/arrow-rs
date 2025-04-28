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

//! Builder API for creating Variant binary values.
//!
//! This module provides a builder-style API for creating Variant values in the
//! Arrow binary format. The API is modeled after the Arrow array builder APIs.
//!
//! # Example
//!
//! ```
//! use std::io::Cursor;
//! use arrow_variant::builder::{VariantBuilder, PrimitiveValue};
//!
//! // Create a builder for variant values
//! let mut metadata_buffer = vec![];
//! let mut builder = VariantBuilder::new(&mut metadata_buffer);
//!
//! // Create an object
//! let mut value_buffer = vec![];
//! let mut object_builder = builder.new_object(&mut value_buffer);
//! object_builder.append_value("foo", 1);
//! object_builder.append_value("bar", 100);
//! object_builder.finish();
//!
//! // value_buffer now contains a valid variant value
//! // builder contains metadata with fields "foo" and "bar"
//!
//! // Create another object reusing the same metadata
//! let mut value_buffer2 = vec![];
//! let mut object_builder2 = builder.new_object(&mut value_buffer2);
//! object_builder2.append_value("foo", 2);
//! object_builder2.append_value("bar", 200);
//! object_builder2.finish();
//!
//! // Finalize the metadata
//! builder.finish();
//! // metadata_buffer now contains valid variant metadata bytes
//! ```

use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::Write;

use crate::encoder::{
    encode_array_from_pre_encoded, encode_binary, encode_boolean, encode_date, encode_decimal16,
    encode_decimal4, encode_decimal8, encode_float, encode_integer, encode_null,
    encode_object_from_pre_encoded, encode_string, encode_time_ntz, encode_timestamp,
    encode_timestamp_nanos, encode_timestamp_ntz, encode_timestamp_ntz_nanos, encode_uuid,
    min_bytes_needed, write_int_with_size,
};
use arrow_schema::ArrowError;

/// Values that can be stored in a Variant.
#[derive(Debug, Clone)]
pub enum PrimitiveValue {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 8-bit integer
    Int8(i8),
    /// 16-bit integer
    Int16(i16),
    /// 32-bit integer
    Int32(i32),
    /// 64-bit integer
    Int64(i64),
    /// Single-precision floating point
    Float(f32),
    /// Double-precision floating point
    Double(f64),
    /// UTF-8 string
    String(String),
    /// Binary data
    Binary(Vec<u8>),
    /// Date value (days since epoch)
    Date(i32),
    /// Timestamp (milliseconds since epoch)
    Timestamp(i64),
    /// Timestamp without timezone (milliseconds since epoch)
    TimestampNTZ(i64),
    /// Time without timezone (milliseconds)
    TimeNTZ(i64),
    /// Timestamp with nanosecond precision
    TimestampNanos(i64),
    /// Timestamp without timezone with nanosecond precision
    TimestampNTZNanos(i64),
    /// UUID as 16 bytes
    Uuid([u8; 16]),
    /// Decimal with scale and 32-bit unscaled value (precision 1-9)
    Decimal4(u8, i32),
    /// Decimal with scale and 64-bit unscaled value (precision 10-18)
    Decimal8(u8, i64),
    /// Decimal with scale and 128-bit unscaled value (precision 19-38)
    Decimal16(u8, i128),
}

impl From<i32> for PrimitiveValue {
    fn from(value: i32) -> Self {
        PrimitiveValue::Int32(value)
    }
}

impl From<i64> for PrimitiveValue {
    fn from(value: i64) -> Self {
        PrimitiveValue::Int64(value)
    }
}

impl From<i16> for PrimitiveValue {
    fn from(value: i16) -> Self {
        PrimitiveValue::Int16(value)
    }
}

impl From<i8> for PrimitiveValue {
    fn from(value: i8) -> Self {
        PrimitiveValue::Int8(value)
    }
}

impl From<f32> for PrimitiveValue {
    fn from(value: f32) -> Self {
        PrimitiveValue::Float(value)
    }
}

impl From<f64> for PrimitiveValue {
    fn from(value: f64) -> Self {
        PrimitiveValue::Double(value)
    }
}

impl From<bool> for PrimitiveValue {
    fn from(value: bool) -> Self {
        PrimitiveValue::Boolean(value)
    }
}

impl From<String> for PrimitiveValue {
    fn from(value: String) -> Self {
        PrimitiveValue::String(value)
    }
}

impl From<&str> for PrimitiveValue {
    fn from(value: &str) -> Self {
        PrimitiveValue::String(value.to_string())
    }
}

impl From<Vec<u8>> for PrimitiveValue {
    fn from(value: Vec<u8>) -> Self {
        PrimitiveValue::Binary(value)
    }
}

impl From<&[u8]> for PrimitiveValue {
    fn from(value: &[u8]) -> Self {
        PrimitiveValue::Binary(value.to_vec())
    }
}

impl<T: Into<PrimitiveValue>> From<Option<T>> for PrimitiveValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => PrimitiveValue::Null,
        }
    }
}

/// Builder for Variant values.
///
/// This builder creates Variant values in the Arrow binary format.
/// It manages metadata and helps create nested objects and arrays.
///
/// The builder follows a pattern similar to other Arrow array builders,
/// but is specialized for creating Variant binary values.
pub struct VariantBuilder<'a> {
    /// Dictionary mapping field names to indexes
    dictionary: HashMap<String, usize>,
    /// Whether keys should be sorted in metadata
    sort_keys: bool,
    /// Whether the metadata is finalized
    is_finalized: bool,
    /// The output destination for metadata
    metadata_output: Box<dyn Write + 'a>,
}

impl<'a> std::fmt::Debug for VariantBuilder<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariantBuilder")
            .field("dictionary", &self.dictionary)
            .field("sort_keys", &self.sort_keys)
            .field("is_finalized", &self.is_finalized)
            .field("metadata_output", &"<dyn Write>")
            .finish()
    }
}

impl<'a> VariantBuilder<'a> {
    /// Creates a new VariantBuilder.
    ///
    /// # Arguments
    ///
    /// * `metadata_output` - The destination for metadata
    pub fn new(metadata_output: impl Write + 'a) -> Self {
        Self::new_with_sort(metadata_output, false)
    }

    /// Creates a new VariantBuilder with optional key sorting.
    ///
    /// # Arguments
    ///
    /// * `metadata_output` - The destination for metadata
    /// * `sort_keys` - Whether keys should be sorted in metadata
    pub fn new_with_sort(metadata_output: impl Write + 'a, sort_keys: bool) -> Self {
        Self {
            dictionary: HashMap::new(),
            sort_keys,
            is_finalized: false,
            metadata_output: Box::new(metadata_output),
        }
    }

    /// Creates a new ObjectBuilder for building an object variant.
    ///
    /// # Arguments
    ///
    /// * `output` - The destination for the object value
    pub fn new_object<'b>(&'b mut self, output: &'b mut Vec<u8>) -> ObjectBuilder<'b, 'a>
    where
        'a: 'b,
    {
        if self.is_finalized {
            panic!("Cannot create a new object after the builder has been finalized");
        }

        ObjectBuilder::new(output, self)
    }

    /// Creates a new ArrayBuilder for building an array variant.
    ///
    /// # Arguments
    ///
    /// * `output` - The destination for the array value
    pub fn new_array<'b>(&'b mut self, output: &'b mut Vec<u8>) -> ArrayBuilder<'b, 'a>
    where
        'a: 'b,
    {
        if self.is_finalized {
            panic!("Cannot create a new array after the builder has been finalized");
        }

        ArrayBuilder::new(output, self)
    }

    /// Adds a key to the dictionary if it doesn't already exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add
    ///
    /// # Returns
    ///
    /// The index of the key in the dictionary
    pub(crate) fn add_key(&mut self, key: &str) -> Result<usize, ArrowError> {
        if self.is_finalized {
            return Err(ArrowError::VariantError(
                "Cannot add keys after metadata has been finalized".to_string(),
            ));
        }

        if let Some(idx) = self.dictionary.get(key) {
            return Ok(*idx);
        }

        let idx = self.dictionary.len();
        self.dictionary.insert(key.to_string(), idx);
        Ok(idx)
    }

    /// Finalizes the metadata and writes it to the output.
    pub fn finish(&mut self) {
        if self.is_finalized {
            return;
        }

        // Get keys in sorted or insertion order
        let mut keys: Vec<_> = self.dictionary.keys().cloned().collect();
        if self.sort_keys {
            keys.sort();

            // Re-index keys based on sorted order
            for (i, key) in keys.iter().enumerate() {
                self.dictionary.insert(key.clone(), i);
            }
        }

        // Calculate total size of dictionary strings
        let total_string_size: usize = keys.iter().map(|k| k.len()).sum();

        // Determine offset size based on max possible offset value
        let max_offset = std::cmp::max(total_string_size, keys.len() + 1);
        let offset_size = min_bytes_needed(max_offset);
        let offset_size_minus_one = offset_size - 1;

        // Construct header byte
        let sorted_bit = if self.sort_keys { 1 } else { 0 };
        let header = 0x01 | (sorted_bit << 4) | ((offset_size_minus_one as u8) << 6);

        // Write header byte
        if let Err(e) = self.metadata_output.write_all(&[header]) {
            panic!("Failed to write metadata header: {}", e);
        }

        // Write dictionary size (number of keys)
        let dict_size = keys.len() as u32;
        if let Err(e) = write_int_with_size(dict_size, offset_size, &mut self.metadata_output) {
            panic!("Failed to write dictionary size: {}", e);
        }

        // Calculate and write offsets
        let mut current_offset = 0u32;
        let mut offsets = Vec::with_capacity(keys.len() + 1);

        offsets.push(current_offset);
        for key in &keys {
            current_offset += key.len() as u32;
            offsets.push(current_offset);
        }

        // Write offsets using the helper function
        for offset in offsets {
            if let Err(e) = write_int_with_size(offset, offset_size, &mut self.metadata_output) {
                panic!("Failed to write offset: {}", e);
            }
        }

        // Write dictionary strings
        for key in keys {
            if let Err(e) = self.metadata_output.write_all(key.as_bytes()) {
                panic!("Failed to write dictionary string: {}", e);
            }
        }

        self.is_finalized = true;
    }

    /// Returns whether the builder has been finalized.
    pub fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

/// Builder for Variant object values.
pub struct ObjectBuilder<'a, 'b> {
    /// Destination for the object value
    output: &'a mut Vec<u8>,
    /// Reference to the variant builder
    variant_builder: &'a mut VariantBuilder<'b>,
    /// Temporary buffer for field values - stored as key_index -> value_buffer
    /// Using IndexMap for O(1) access with ability to sort by key
    value_buffers: IndexMap<usize, Vec<u8>>,
    /// Whether the object has been finalized
    is_finalized: bool,
}

impl<'a, 'b> std::fmt::Debug for ObjectBuilder<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectBuilder")
            .field("variant_builder", &self.variant_builder)
            .field("value_buffers", &self.value_buffers)
            .field("is_finalized", &self.is_finalized)
            .finish()
    }
}

impl<'a, 'b> ObjectBuilder<'a, 'b> {
    /// Creates a new ObjectBuilder.
    ///
    /// # Arguments
    ///
    /// * `output` - The destination for the object value
    /// * `variant_builder` - The parent variant builder
    fn new(output: &'a mut Vec<u8>, variant_builder: &'a mut VariantBuilder<'b>) -> Self {
        Self {
            output,
            variant_builder,
            value_buffers: IndexMap::new(),
            is_finalized: false,
        }
    }

    /// Adds a primitive value to the object.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the value
    /// * `value` - The primitive value to add
    pub fn append_value<T: Into<PrimitiveValue>>(&mut self, key: &str, value: T) {
        if self.is_finalized {
            panic!("Cannot append to a finalized object");
        }

        // Add the key to metadata and get its index
        let key_index = match self.variant_builder.add_key(key) {
            Ok(idx) => idx,
            Err(e) => panic!("Failed to add key: {}", e),
        };

        // Create a buffer for this value
        let mut buffer = Vec::new();

        // Convert the value to PrimitiveValue and write it
        let primitive_value = value.into();
        if let Err(e) = write_value(&mut buffer, &primitive_value) {
            panic!("Failed to write value: {}", e);
        }

        // Store the buffer for this field - will overwrite if key already exists
        self.value_buffers.insert(key_index, buffer);
    }

    /// Creates a nested object builder.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the nested object
    pub fn append_object<'c>(&'c mut self, key: &str) -> ObjectBuilder<'c, 'b>
    where
        'a: 'c,
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized object");
        }

        // Add the key to metadata and get its index
        let key_index = match self.variant_builder.add_key(key) {
            Ok(idx) => idx,
            Err(e) => panic!("Failed to add key: {}", e),
        };

        // Create a temporary buffer for the nested object and store it
        let nested_buffer = Vec::new();
        self.value_buffers.insert(key_index, nested_buffer);

        // Get a mutable reference to the value buffer we just inserted
        let nested_buffer = self.value_buffers.get_mut(&key_index).unwrap();

        // Create a new object builder for this nested buffer
        ObjectBuilder::new(nested_buffer, self.variant_builder)
    }

    /// Creates a nested array builder.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the nested array
    pub fn append_array<'c>(&'c mut self, key: &str) -> ArrayBuilder<'c, 'b>
    where
        'a: 'c,
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized object");
        }

        // Add the key to metadata and get its index
        let key_index = match self.variant_builder.add_key(key) {
            Ok(idx) => idx,
            Err(e) => panic!("Failed to add key: {}", e),
        };

        // Create a temporary buffer for the nested array and store it
        let nested_buffer = Vec::new();
        self.value_buffers.insert(key_index, nested_buffer);

        // Get a mutable reference to the value buffer we just inserted
        let nested_buffer = self.value_buffers.get_mut(&key_index).unwrap();

        // Create a new array builder for this nested buffer
        ArrayBuilder::new(nested_buffer, self.variant_builder)
    }

    /// Finalizes the object and writes it to the output.
    pub fn finish(&mut self) {
        if self.is_finalized {
            return;
        }

        // Sort the entries by key index
        self.value_buffers.sort_keys();

        // Prepare field IDs and values for encoding
        let field_ids: Vec<usize> = self.value_buffers.keys().copied().collect();
        let field_values: Vec<&[u8]> = self.value_buffers.values().map(|v| v.as_slice()).collect();

        // Encode the object directly to output
        if let Err(e) = encode_object_from_pre_encoded(&field_ids, &field_values, self.output) {
            panic!("Failed to encode object: {}", e);
        }

        self.is_finalized = true;
    }
}

/// Builder for Variant array values.
pub struct ArrayBuilder<'a, 'b> {
    /// Destination for the array value
    output: &'a mut Vec<u8>,
    /// Reference to the variant builder
    variant_builder: &'a mut VariantBuilder<'b>,
    /// Temporary buffers for array elements
    value_buffers: Vec<Vec<u8>>,
    /// Whether the array has been finalized
    is_finalized: bool,
}

impl<'a, 'b> std::fmt::Debug for ArrayBuilder<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayBuilder")
            .field("variant_builder", &self.variant_builder)
            .field("value_buffers", &self.value_buffers)
            .field("is_finalized", &self.is_finalized)
            .finish()
    }
}

impl<'a, 'b> ArrayBuilder<'a, 'b> {
    /// Creates a new ArrayBuilder.
    ///
    /// # Arguments
    ///
    /// * `output` - The destination for the array value
    /// * `variant_builder` - The parent variant builder
    fn new(output: &'a mut Vec<u8>, variant_builder: &'a mut VariantBuilder<'b>) -> Self {
        Self {
            output,
            variant_builder,
            value_buffers: Vec::new(),
            is_finalized: false,
        }
    }

    /// Adds a primitive value to the array.
    ///
    /// # Arguments
    ///
    /// * `value` - The primitive value to add
    pub fn append_value<T: Into<PrimitiveValue>>(&mut self, value: T) {
        if self.is_finalized {
            panic!("Cannot append to a finalized array");
        }

        // Create a buffer for this value
        let mut buffer = Vec::new();

        // Convert the value to PrimitiveValue and write it
        let primitive_value = value.into();
        if let Err(e) = write_value(&mut buffer, &primitive_value) {
            panic!("Failed to write value: {}", e);
        }

        // Store the buffer for this element
        self.value_buffers.push(buffer);
    }

    /// Creates a nested object builder.
    ///
    /// # Returns the index of the nested object in the array
    pub fn append_object<'c>(&'c mut self) -> ObjectBuilder<'c, 'b>
    where
        'a: 'c,
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized array");
        }

        // Create a temporary buffer for the nested object
        let nested_buffer = Vec::new();
        self.value_buffers.push(nested_buffer);

        // Get a mutable reference to the value buffer we just inserted
        let nested_buffer = self.value_buffers.last_mut().unwrap();

        // Create a new object builder for this nested buffer
        ObjectBuilder::new(nested_buffer, self.variant_builder)
    }

    /// Creates a nested array builder.
    ///
    /// # Returns the index of the nested array in the array
    pub fn append_array<'c>(&'c mut self) -> ArrayBuilder<'c, 'b>
    where
        'a: 'c,
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized array");
        }

        // Create a temporary buffer for the nested array
        let nested_buffer = Vec::new();
        self.value_buffers.push(nested_buffer);

        // Get a mutable reference to the value buffer we just inserted
        let nested_buffer = self.value_buffers.last_mut().unwrap();

        // Create a new array builder for this nested buffer
        ArrayBuilder::new(nested_buffer, self.variant_builder)
    }

    /// Finalizes the array and writes it to the output.
    pub fn finish(&mut self) {
        if self.is_finalized {
            return;
        }

        // Prepare slices for values
        let values: Vec<&[u8]> = self.value_buffers.iter().map(|v| v.as_slice()).collect();

        // Encode the array directly to output
        if let Err(e) = encode_array_from_pre_encoded(&values, self.output) {
            panic!("Failed to encode array: {}", e);
        }

        self.is_finalized = true;
    }
}

/// Writes a primitive value to a buffer using the Variant format.
///
/// This function handles the correct encoding of primitive values by utilizing
/// the encoder module functionality.
fn write_value(buffer: &mut Vec<u8>, value: &PrimitiveValue) -> Result<(), ArrowError> {
    match value {
        PrimitiveValue::Null => {
            encode_null(buffer);
        }
        PrimitiveValue::Boolean(val) => {
            encode_boolean(*val, buffer);
        }
        PrimitiveValue::Int8(val) => {
            encode_integer(*val as i64, buffer);
        }
        PrimitiveValue::Int16(val) => {
            encode_integer(*val as i64, buffer);
        }
        PrimitiveValue::Int32(val) => {
            encode_integer(*val as i64, buffer);
        }
        PrimitiveValue::Int64(val) => {
            encode_integer(*val, buffer);
        }
        PrimitiveValue::Float(val) => {
            encode_float(*val as f64, buffer);
        }
        PrimitiveValue::Double(val) => {
            encode_float(*val, buffer);
        }
        PrimitiveValue::String(val) => {
            encode_string(val, buffer);
        }
        PrimitiveValue::Binary(val) => {
            encode_binary(val, buffer);
        }
        PrimitiveValue::Date(val) => {
            encode_date(*val, buffer);
        }
        PrimitiveValue::Timestamp(val) => {
            encode_timestamp(*val, buffer);
        }
        PrimitiveValue::TimestampNTZ(val) => {
            encode_timestamp_ntz(*val, buffer);
        }
        PrimitiveValue::TimeNTZ(val) => {
            encode_time_ntz(*val, buffer);
        }
        PrimitiveValue::TimestampNanos(val) => {
            encode_timestamp_nanos(*val, buffer);
        }
        PrimitiveValue::TimestampNTZNanos(val) => {
            encode_timestamp_ntz_nanos(*val, buffer);
        }
        PrimitiveValue::Uuid(val) => {
            encode_uuid(val, buffer);
        }
        PrimitiveValue::Decimal4(scale, unscaled_value) => {
            encode_decimal4(*scale, *unscaled_value, buffer);
        }
        PrimitiveValue::Decimal8(scale, unscaled_value) => {
            encode_decimal8(*scale, *unscaled_value, buffer);
        }
        PrimitiveValue::Decimal16(scale, unscaled_value) => {
            encode_decimal16(*scale, *unscaled_value, buffer);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::VariantBasicType;
    use arrow_schema::extension::Variant;

    // Helper function to extract keys from metadata for testing
    fn get_metadata_keys(metadata: &[u8]) -> Vec<String> {
        // Simple implementation to extract keys from metadata buffer
        // This avoids dependency on VariantReader which might not be accessible

        // Skip the header byte
        let mut pos = 1;

        // Get offset size from header byte
        let offset_size = ((metadata[0] >> 6) & 0x03) + 1;

        // Read dictionary size
        let mut dict_size = 0usize;
        for i in 0..offset_size {
            dict_size |= (metadata[pos + i as usize] as usize) << (i * 8);
        }
        pos += offset_size as usize;

        if dict_size == 0 {
            return vec![];
        }

        // Read offsets
        let mut offsets = Vec::with_capacity(dict_size + 1);
        for _ in 0..=dict_size {
            let mut offset = 0usize;
            for i in 0..offset_size {
                offset |= (metadata[pos + i as usize] as usize) << (i * 8);
            }
            offsets.push(offset);
            pos += offset_size as usize;
        }

        // Extract keys using offsets
        let mut keys = Vec::with_capacity(dict_size);
        for i in 0..dict_size {
            let start = offsets[i];
            let end = offsets[i + 1];
            let key_bytes = &metadata[pos + start..pos + end];
            keys.push(String::from_utf8_lossy(key_bytes).to_string());
        }

        keys
    }

    // =========================================================================
    // Basic builder functionality tests
    // =========================================================================

    #[test]
    fn test_basic_object_builder() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object_builder = builder.new_object(&mut value_buffer);

            // Test various primitive types
            object_builder.append_value("null", Option::<i32>::None);
            object_builder.append_value("bool_true", true);
            object_builder.append_value("bool_false", false);
            object_builder.append_value("int8", 42i8);
            object_builder.append_value("int16", 1000i16);
            object_builder.append_value("int32", 100000i32);
            object_builder.append_value("int64", 1000000000i64);
            object_builder.append_value("float", 3.14f32);
            object_builder.append_value("double", 2.71828f64);
            object_builder.append_value("string", "hello world");
            object_builder.append_value("binary", vec![1u8, 2u8, 3u8]);

            object_builder.finish();
            builder.finish();
        }

        // Verify object encoding
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);

        // Verify metadata contains all keys
        let keys = get_metadata_keys(&metadata_buffer);
        assert_eq!(keys.len(), 11, "Should have 11 keys in metadata");
        assert!(keys.contains(&"null".to_string()), "Missing 'null' key");
        assert!(
            keys.contains(&"bool_true".to_string()),
            "Missing 'bool_true' key"
        );
        assert!(keys.contains(&"string".to_string()), "Missing 'string' key");

        // Verify object has the correct number of entries
        // First byte after header is the number of fields (if small object)
        assert!(value_buffer.len() > 1, "Value buffer too small");
        let num_fields = value_buffer[1];
        assert_eq!(num_fields as usize, 11, "Object should have 11 fields");

        let _variant = Variant::new(metadata_buffer, value_buffer);
    }

    #[test]
    fn test_basic_array_builder() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        let num_elements = 11; // Number of elements we'll add

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut array_builder = builder.new_array(&mut value_buffer);

            // Test various primitive types
            array_builder.append_value(Option::<i32>::None);
            array_builder.append_value(true);
            array_builder.append_value(false);
            array_builder.append_value(42i8);
            array_builder.append_value(1000i16);
            array_builder.append_value(100000i32);
            array_builder.append_value(1000000000i64);
            array_builder.append_value(3.14f32);
            array_builder.append_value(2.71828f64);
            array_builder.append_value("hello world");
            array_builder.append_value(vec![1u8, 2u8, 3u8]);

            array_builder.finish();
            builder.finish();
        }

        // Verify array encoding
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Array as u8);

        // Verify array length
        // First byte after header is the array length (if small array)
        assert!(value_buffer.len() > 1, "Value buffer too small");
        let array_length = value_buffer[1];
        assert_eq!(
            array_length as usize, num_elements,
            "Array should have exactly {num_elements} elements"
        );

        // Verify metadata format is valid (version 1)
        assert_eq!(
            metadata_buffer[0] & 0x0F,
            0x01,
            "Metadata should be version 1"
        );

        // Metadata should have dictionary size of 0 (no keys in a plain array)
        // Second and potentially following bytes are dictionary size depending on offset size
        let offset_size = ((metadata_buffer[0] >> 6) & 0x03) + 1;
        let dict_size_bytes = &metadata_buffer[1..1 + offset_size as usize];
        if offset_size == 1 {
            assert_eq!(
                dict_size_bytes[0], 0,
                "Dictionary should be empty for array"
            );
        }

        // Create variant and verify it's structurally valid
        let variant = Variant::new(metadata_buffer, value_buffer);
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }

    // =========================================================================
    // Nested structure tests
    // =========================================================================

    #[test]
    fn test_nested_objects() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut root = builder.new_object(&mut value_buffer);

            // Add primitive values
            root.append_value("name", "Test User");
            root.append_value("age", 30);

            // Add nested object
            {
                let mut address = root.append_object("address");
                address.append_value("street", "123 Main St");
                address.append_value("city", "Anytown");
                address.append_value("zip", 12345);

                // Add deeply nested object
                {
                    let mut geo = address.append_object("geo");
                    geo.append_value("lat", 40.7128);
                    geo.append_value("lng", -74.0060);
                    geo.finish();
                }

                address.finish();
            }

            root.finish();
            builder.finish();
        }

        // Verify metadata contains the correct keys
        let keys = get_metadata_keys(&metadata_buffer);
        assert_eq!(keys.len(), 9, "Should have 9 keys in metadata");

        // Check all required keys exist
        let required_keys = [
            "name", "age", "address", "street", "city", "zip", "geo", "lat", "lng",
        ];
        for key in required_keys.iter() {
            assert!(keys.contains(&key.to_string()), "Missing '{key}' key");
        }

        // Verify object structure - first byte should be object type
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);

        // Create variant and verify it's valid
        let variant = Variant::new(metadata_buffer, value_buffer);
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }

    #[test]
    fn test_nested_arrays() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut root = builder.new_object(&mut value_buffer);

            // Add array of primitives with expected length 3
            {
                let mut scores = root.append_array("scores");
                scores.append_value(95);
                scores.append_value(87);
                scores.append_value(91);
                scores.finish();
            }

            // Add array of objects with expected length 2
            {
                let mut contacts = root.append_array("contacts");

                // First contact
                {
                    let mut contact = contacts.append_object();
                    contact.append_value("name", "Alice");
                    contact.append_value("phone", "555-1234");
                    contact.finish();
                }

                // Second contact
                {
                    let mut contact = contacts.append_object();
                    contact.append_value("name", "Bob");
                    contact.append_value("phone", "555-5678");
                    contact.finish();
                }

                contacts.finish();
            }

            root.finish();
            builder.finish();
        }

        // Verify metadata contains the expected keys
        let keys = get_metadata_keys(&metadata_buffer);
        assert_eq!(keys.len(), 4, "Should have 4 keys in metadata");

        // Check required keys
        let required_keys = ["scores", "contacts", "name", "phone"];
        for key in required_keys.iter() {
            assert!(keys.contains(&key.to_string()), "Missing '{key}' key");
        }

        // Create variant
        let variant = Variant::new(metadata_buffer, value_buffer);
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }

    // =========================================================================
    // Advanced feature tests
    // =========================================================================

    #[test]
    fn test_metadata_reuse() {
        let mut metadata_buffer = vec![];

        // Create multiple value buffers
        let mut value_buffer1 = vec![];
        let mut value_buffer2 = vec![];
        let mut value_buffer3 = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);

            // First object with all keys
            {
                let mut object = builder.new_object(&mut value_buffer1);
                object.append_value("foo", 1);
                object.append_value("bar", 100);
                object.append_value("baz", "hello");
                object.finish();
            }

            // Second object with subset of keys
            {
                let mut object = builder.new_object(&mut value_buffer2);
                object.append_value("foo", 2);
                object.append_value("bar", 200);
                // No "baz" key
                object.finish();
            }

            // Third object with different subset and order
            {
                let mut object = builder.new_object(&mut value_buffer3);
                // Different order
                object.append_value("baz", "world");
                object.append_value("foo", 3);
                // No "bar" key
                object.finish();
            }

            builder.finish();
        }

        // Verify metadata has expected number of keys
        let keys = get_metadata_keys(&metadata_buffer);
        assert_eq!(keys.len(), 3, "Should have 3 keys in metadata");

        // Create variants with same metadata
        let variant1 = Variant::new(metadata_buffer.clone(), value_buffer1);
        let variant2 = Variant::new(metadata_buffer.clone(), value_buffer2);
        let variant3 = Variant::new(metadata_buffer, value_buffer3);

        // Verify shared metadata has identical bytes
        assert_eq!(
            variant1.metadata(),
            variant2.metadata(),
            "Metadata should be exactly the same"
        );
        assert_eq!(
            variant2.metadata(),
            variant3.metadata(),
            "Metadata should be exactly the same"
        );

        // Verify different values
        assert_ne!(
            variant1.value(),
            variant2.value(),
            "Values should be different"
        );
        assert_ne!(
            variant2.value(),
            variant3.value(),
            "Values should be different"
        );
        assert_ne!(
            variant1.value(),
            variant3.value(),
            "Values should be different"
        );
    }

    #[test]
    fn test_sorted_keys() {
        // Test sorted keys vs unsorted
        let mut sorted_metadata = vec![];
        let mut unsorted_metadata = vec![];
        let mut value_buffer1 = vec![];
        let mut value_buffer2 = vec![];

        // Define keys in a non-alphabetical order
        let keys = ["zoo", "apple", "banana"];

        // Build with sorted keys
        {
            let mut builder = VariantBuilder::new_with_sort(&mut sorted_metadata, true);
            let mut object = builder.new_object(&mut value_buffer1);

            // Add keys in random order
            for (i, key) in keys.iter().enumerate() {
                object.append_value(key, (i + 1) as i32);
            }

            object.finish();
            builder.finish();
        }

        // Build with unsorted keys
        {
            let mut builder = VariantBuilder::new_with_sort(&mut unsorted_metadata, false);
            let mut object = builder.new_object(&mut value_buffer2);

            // Add keys in same order
            for (i, key) in keys.iter().enumerate() {
                object.append_value(key, (i + 1) as i32);
            }

            object.finish();
            builder.finish();
        }

        // Verify sort flag in metadata header (bit 4)
        assert_eq!(sorted_metadata[0] & 0x10, 0x10, "Sorted flag should be set");
        assert_eq!(
            unsorted_metadata[0] & 0x10,
            0,
            "Sorted flag should not be set"
        );

        // Verify actual sorting of keys
        let sorted_keys = get_metadata_keys(&sorted_metadata);
        let unsorted_keys = get_metadata_keys(&unsorted_metadata);

        // Verify number of keys
        assert_eq!(sorted_keys.len(), 3, "Should have 3 keys");
        assert_eq!(unsorted_keys.len(), 3, "Should have 3 keys");

        // Verify sorted keys are in alphabetical order
        let mut expected_sorted = keys.to_vec();
        expected_sorted.sort();

        // Convert to Vec to make comparison easier
        let sorted_keys_vec: Vec<_> = sorted_keys.iter().collect();

        // Verify first key is alphabetically first
        assert_eq!(
            sorted_keys_vec[0], "apple",
            "First key should be 'apple' in sorted metadata"
        );
    }

    // =========================================================================
    // Encoding validation tests
    // =========================================================================

    #[test]
    fn test_object_encoding() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object = builder.new_object(&mut value_buffer);

            // Add a few values
            object.append_value("name", "Test User");
            object.append_value("age", 30);
            object.append_value("active", true);

            object.finish();
            builder.finish();
        }

        // Validate object encoding format
        // First byte should have Object type in lower 2 bits
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);

        // Check field ID and offset sizes from header
        let is_large = (value_buffer[0] & 0x40) != 0;
        // Verify correct sizes based on our data
        assert!(!is_large, "Should not need large format for 3 fields");
        // Validate number of fields
        let num_fields = value_buffer[1];
        assert_eq!(num_fields, 3, "Should have 3 fields");

        // Verify metadata contains the correct keys
        let keys = get_metadata_keys(&metadata_buffer);
        assert_eq!(keys.len(), 3, "Should have 3 keys in metadata");

        // Check all keys exist
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"active".to_string()));
    }

    #[test]
    fn test_array_encoding() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        let expected_len = 4; // We'll add 4 elements

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut array = builder.new_array(&mut value_buffer);

            // Add a few values
            array.append_value(1);
            array.append_value(2);
            array.append_value("hello");
            array.append_value(true);

            array.finish();
            builder.finish();
        }

        // Validate array encoding format
        // First byte should have Array type in lower 2 bits
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Array as u8);

        // Check if large format and offset size from header
        let is_large = (value_buffer[0] & 0x10) != 0;
        let offset_size = ((value_buffer[0] >> 2) & 0x03) + 1;

        // Verify correct sizes based on our data
        assert!(!is_large, "Should not need large format for 4 elements");

        // Validate array length
        let array_length = value_buffer[1];
        assert_eq!(
            array_length, expected_len,
            "Array should have {expected_len} elements"
        );

        // Verify offsets section exists
        // The offsets start after the header (1 byte) and length (1 byte if small)
        // and there should be n+1 offsets where n is the array length
        let offsets_section_size = (expected_len as usize + 1) * (offset_size as usize);
        assert!(
            value_buffer.len() > 2 + offsets_section_size,
            "Value buffer should contain offsets section of size {offsets_section_size}"
        );
    }

    #[test]
    fn test_metadata_encoding() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new_with_sort(&mut metadata_buffer, true);
            let mut object = builder.new_object(&mut value_buffer);

            // Add keys in non-alphabetical order
            object.append_value("zzz", 3);
            object.append_value("aaa", 1);
            object.append_value("mmm", 2);

            object.finish();
            builder.finish();
        }

        // Validate metadata encoding
        // First byte should have metadata version and sorted flag
        assert_eq!(
            metadata_buffer[0] & 0x0F,
            0x01,
            "Metadata should be version 1"
        );
        assert_eq!(metadata_buffer[0] & 0x10, 0x10, "Sorted flag should be set");

        // Get offset size from header
        let offset_size = ((metadata_buffer[0] >> 6) & 0x03) + 1;

        // Read dictionary size based on offset size
        let mut dict_size = 0usize;
        for i in 0..offset_size {
            dict_size |= (metadata_buffer[1 + i as usize] as usize) << (i * 8);
        }

        assert_eq!(dict_size, 3, "Dictionary should have 3 entries");

        // Verify key ordering by reading keys
        let keys = get_metadata_keys(&metadata_buffer);

        // Convert to Vec to make validation easier
        let keys_vec: Vec<_> = keys.iter().collect();

        // Verify keys are in alphabetical order
        assert_eq!(keys_vec[0], "aaa", "First key should be 'aaa'");
        assert_eq!(keys_vec[1], "mmm", "Second key should be 'mmm'");
        assert_eq!(keys_vec[2], "zzz", "Third key should be 'zzz'");
    }

    #[test]
    fn test_primitive_type_encoding() {
        // Test encoding of each primitive type
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object = builder.new_object(&mut value_buffer);

            // Add one of each primitive type
            object.append_value("null", Option::<i32>::None);
            object.append_value("bool_true", true);
            object.append_value("bool_false", false);
            object.append_value("int8", 42i8);
            object.append_value("int16", 1000i16);
            object.append_value("int32", 100000i32);
            object.append_value("int64", 1000000000i64);
            object.append_value("float", 3.14f32);
            object.append_value("double", 2.71828f64);
            object.append_value("string_short", "abc"); // Short string
            object.append_value("string_long", "a".repeat(64)); // Long string
            object.append_value("binary", vec![1u8, 2u8, 3u8]);

            object.finish();
            builder.finish();
        }

        // Verify object encoding
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);

        // Verify number of fields
        let num_fields = value_buffer[1];
        assert_eq!(num_fields, 12, "Object should have 12 fields");

        // Create variant
        let variant = Variant::new(metadata_buffer, value_buffer);
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }

    // =========================================================================
    // Error handling and edge cases
    // =========================================================================

    #[test]
    #[should_panic(expected = "Cannot create a new object after the builder has been finalized")]
    fn test_error_after_finalize() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        let mut builder = VariantBuilder::new(&mut metadata_buffer);

        // Finalize the builder
        builder.finish();

        // This should panic - creating object after finalize
        let mut _object = builder.new_object(&mut value_buffer);
    }

    #[test]
    #[should_panic(expected = "Cannot append to a finalized object")]
    fn test_error_append_after_finish() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        let mut builder = VariantBuilder::new(&mut metadata_buffer);
        let mut object = builder.new_object(&mut value_buffer);

        // Finish the object
        object.finish();

        // This should panic - appending after finish
        object.append_value("test", 1);
    }

    #[test]
    fn test_empty_object_and_array() {
        // Test empty object
        let mut metadata_buffer = vec![];
        let mut obj_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object = builder.new_object(&mut obj_buffer);
            // Don't add any fields
            object.finish();
            builder.finish();
        }

        let obj_variant = Variant::new(metadata_buffer.clone(), obj_buffer);
        assert!(!obj_variant.metadata().is_empty());
        assert!(!obj_variant.value().is_empty());

        // Check object has 0 fields
        assert_eq!(
            obj_variant.value()[1],
            0,
            "Empty object should have 0 fields"
        );

        // Test empty array
        let mut arr_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut array = builder.new_array(&mut arr_buffer);
            // Don't add any elements
            array.finish();
            builder.finish();
        }

        let arr_variant = Variant::new(metadata_buffer, arr_buffer);
        assert!(!arr_variant.metadata().is_empty());
        assert!(!arr_variant.value().is_empty());

        // Check array has 0 elements
        assert_eq!(
            arr_variant.value()[1],
            0,
            "Empty array should have 0 elements"
        );
    }

    #[test]
    fn test_decimal_values() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];

        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object_builder = builder.new_object(&mut value_buffer);

            // Test using PrimitiveValue directly
            object_builder.append_value("decimal4", PrimitiveValue::Decimal4(2, 12345));
            object_builder.append_value("decimal8", PrimitiveValue::Decimal8(4, 9876543210));
            object_builder.append_value(
                "decimal16",
                PrimitiveValue::Decimal16(10, 1234567890123456789012345678901_i128),
            );

            object_builder.finish();
            builder.finish();
        }

        // Verify object was created successfully
        let variant = Variant::new(metadata_buffer, value_buffer);
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());

        // Verify basics about the object
        let object_byte = variant.value()[0];
        assert_eq!(object_byte & 0x03, VariantBasicType::Object as u8);

        // Check number of fields is correct
        assert_eq!(variant.value()[1], 3, "Should have 3 decimal fields");
    }
}
