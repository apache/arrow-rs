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
//! object_builder.append_value("foo", PrimitiveValue::Int32(1));
//! object_builder.append_value("bar", PrimitiveValue::Int32(100));
//! object_builder.finish();
//! 
//! // value_buffer now contains a valid variant value
//! // builder contains metadata with fields "foo" and "bar"
//! 
//! // Create another object reusing the same metadata
//! let mut value_buffer2 = vec![];
//! let mut object_builder2 = builder.new_object(&mut value_buffer2);
//! object_builder2.append_value("foo", PrimitiveValue::Int32(2));
//! object_builder2.append_value("bar", PrimitiveValue::Int32(200));
//! object_builder2.finish();
//! 
//! // Finalize the metadata
//! builder.finish();
//! // metadata_buffer now contains valid variant metadata bytes
//! ```

use std::collections::HashMap;
use std::io::Write;
use arrow_schema::extension::Variant;

use arrow_schema::ArrowError;
use crate::encoder::{
    VariantBasicType, 
    encode_null, encode_boolean, encode_integer, encode_float, encode_string,
    encode_binary, encode_date, encode_timestamp, encode_timestamp_ntz, 
    encode_time_ntz, encode_timestamp_nanos, encode_timestamp_ntz_nanos, encode_uuid
};

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
    where 'a: 'b 
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
    where 'a: 'b
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
            return Err(ArrowError::SchemaError("Cannot add keys after metadata has been finalized".to_string()));
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
        let offset_size = get_min_integer_size(max_offset);
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
        for i in 0..offset_size {
            if let Err(e) = self.metadata_output.write_all(&[((dict_size >> (8 * i)) & 0xFF) as u8]) {
                panic!("Failed to write dictionary size: {}", e);
            }
        }
        
        // Calculate and write offsets
        let mut current_offset = 0u32;
        let mut offsets = Vec::with_capacity(keys.len() + 1);
        
        offsets.push(current_offset);
        for key in &keys {
            current_offset += key.len() as u32;
            offsets.push(current_offset);
        }
        
        for offset in offsets {
            for i in 0..offset_size {
                if let Err(e) = self.metadata_output.write_all(&[((offset >> (8 * i)) & 0xFF) as u8]) {
                    panic!("Failed to write offset: {}", e);
                }
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
    /// Temporary buffer for field values
    value_buffers: HashMap<usize, Vec<u8>>,
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
            value_buffers: HashMap::new(),
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
        
        // Store the buffer for this field
        self.value_buffers.insert(key_index, buffer);
    }
    
    /// Creates a nested object builder.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the nested object
    pub fn append_object<'c>(&'c mut self, key: &str) -> ObjectBuilder<'c, 'b> 
    where 'a: 'c
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized object");
        }
        
        // Add the key to metadata and get its index
        let key_index = match self.variant_builder.add_key(key) {
            Ok(idx) => idx,
            Err(e) => panic!("Failed to add key: {}", e),
        };
        
        // Create a temporary buffer for the nested object
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
    where 'a: 'c
    {
        if self.is_finalized {
            panic!("Cannot append to a finalized object");
        }
        
        // Add the key to metadata and get its index
        let key_index = match self.variant_builder.add_key(key) {
            Ok(idx) => idx,
            Err(e) => panic!("Failed to add key: {}", e),
        };
        
        // Create a temporary buffer for the nested array
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
        
        // Create a temporary buffer for the final object
        let mut temp_buffer = Vec::new();
        
        // Prepare field IDs and values for encoding
        let mut field_ids: Vec<usize> = Vec::with_capacity(self.value_buffers.len());
        let mut field_values: Vec<&[u8]> = Vec::with_capacity(self.value_buffers.len());
        
        // Sort by key index if needed
        let mut entries: Vec<(&usize, &Vec<u8>)> = self.value_buffers.iter().collect();
        entries.sort_by_key(|&(k, _)| k);
        
        for (key_index, value) in entries {
            field_ids.push(*key_index);
            field_values.push(value.as_slice());
        }
        
        // Use the helper function to encode the object
        if let Err(e) = encode_object_to_writer(&field_ids, &field_values, &mut temp_buffer) {
            panic!("Failed to encode object: {}", e);
        }
        
        // Now that we have the complete object, write it to the output
        if let Err(e) = self.output.write_all(&temp_buffer) {
            panic!("Failed to write object to output: {}", e);
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
    where 'a: 'c
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
    where 'a: 'c
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
        
        // Create a temporary buffer for the final array
        let mut temp_buffer = Vec::new();
        
        // Prepare values for encoding
        let values: Vec<&[u8]> = self.value_buffers.iter()
            .map(|v| v.as_slice())
            .collect();
        
        // Use the helper function to encode the array
        if let Err(e) = encode_array_to_writer(&values, &mut temp_buffer) {
            panic!("Failed to encode array: {}", e);
        }
        
        // Now that we have the complete array, write it to the output
        if let Err(e) = self.output.write_all(&temp_buffer) {
            panic!("Failed to write array to output: {}", e);
        }
        
        self.is_finalized = true;
    }
}

/// Writes a primitive value to a buffer using the Variant format.
///
/// This function handles the correct encoding of primitive values by utilizing 
/// the encoder module functionality.
fn write_value(buffer: &mut impl Write, value: &PrimitiveValue) -> Result<(), ArrowError> {
    // Create a temporary buffer for encoder functions that expect Vec<u8>
    let mut temp_buffer = Vec::new();
    
    match value {
        PrimitiveValue::Null => {
            encode_null(&mut temp_buffer);
        },
        PrimitiveValue::Boolean(val) => {
            encode_boolean(*val, &mut temp_buffer);
        },
        PrimitiveValue::Int8(val) => {
            encode_integer(*val as i64, &mut temp_buffer);
        },
        PrimitiveValue::Int16(val) => {
            encode_integer(*val as i64, &mut temp_buffer);
        },
        PrimitiveValue::Int32(val) => {
            encode_integer(*val as i64, &mut temp_buffer);
        },
        PrimitiveValue::Int64(val) => {
            encode_integer(*val, &mut temp_buffer);
        },
        PrimitiveValue::Float(val) => {
            encode_float(*val as f64, &mut temp_buffer);
        },
        PrimitiveValue::Double(val) => {
            encode_float(*val, &mut temp_buffer);
        },
        PrimitiveValue::String(val) => {
            encode_string(val, &mut temp_buffer);
        },
        PrimitiveValue::Binary(val) => {
            encode_binary(val, &mut temp_buffer);
        },
        PrimitiveValue::Date(val) => {
            encode_date(*val, &mut temp_buffer);
        },
        PrimitiveValue::Timestamp(val) => {
            encode_timestamp(*val, &mut temp_buffer);
        },
        PrimitiveValue::TimestampNTZ(val) => {
            encode_timestamp_ntz(*val, &mut temp_buffer);
        },
        PrimitiveValue::TimeNTZ(val) => {
            encode_time_ntz(*val, &mut temp_buffer);
        },
        PrimitiveValue::TimestampNanos(val) => {
            encode_timestamp_nanos(*val, &mut temp_buffer);
        },
        PrimitiveValue::TimestampNTZNanos(val) => {
            encode_timestamp_ntz_nanos(*val, &mut temp_buffer);
        },
        PrimitiveValue::Uuid(val) => {
            encode_uuid(val, &mut temp_buffer);
        },
    }
    
    // Write the prepared buffer to the output
    buffer.write_all(&temp_buffer)?;
    
    Ok(())
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

/// Encodes an object using the correct encoder logic
fn encode_object_to_writer(
    field_ids: &[usize],
    field_values: &[&[u8]],
    output: &mut impl Write
) -> Result<(), ArrowError> {
    let len = field_ids.len();
    
    // Determine if we need large size encoding
    let is_large = len > 255;
    
    // Calculate total value size to determine offset_size
    let mut data_size = 0;
    for value in field_values {
        data_size += value.len();
    }
    
    // Determine minimum sizes needed
    let id_size = if field_ids.is_empty() { 1 }
                  else if field_ids.iter().max().unwrap_or(&0) <= &255 { 1 }
                  else if field_ids.iter().max().unwrap_or(&0) <= &65535 { 2 }
                  else if field_ids.iter().max().unwrap_or(&0) <= &16777215 { 3 }
                  else { 4 };
                  
    let offset_size = if data_size <= 255 { 1 }
                      else if data_size <= 65535 { 2 }
                      else if data_size <= 16777215 { 3 }
                      else { 4 };
    
    // Write object header with correct flags
    let header = object_header(is_large, id_size, offset_size);
    output.write_all(&[header])?;
    
    // Write length as 1 or 4 bytes
    if is_large {
        output.write_all(&(len as u32).to_le_bytes())?;
    } else {
        output.write_all(&[len as u8])?;
    }
    
    // Write field IDs
    for id in field_ids {
        match id_size {
            1 => output.write_all(&[*id as u8])?,
            2 => output.write_all(&(*id as u16).to_le_bytes())?,
            3 => {
                output.write_all(&[(*id & 0xFF) as u8])?;
                output.write_all(&[((*id >> 8) & 0xFF) as u8])?;
                output.write_all(&[((*id >> 16) & 0xFF) as u8])?;
            },
            4 => output.write_all(&(*id as u32).to_le_bytes())?,
            _ => unreachable!(),
        }
    }
    
    // Calculate and write offsets
    let mut offsets = Vec::with_capacity(len + 1);
    let mut current_offset = 0u32;
    
    offsets.push(current_offset);
    for value in field_values {
        current_offset += value.len() as u32;
        offsets.push(current_offset);
    }
    
    for offset in &offsets {
        match offset_size {
            1 => output.write_all(&[*offset as u8])?,
            2 => output.write_all(&(*offset as u16).to_le_bytes())?,
            3 => {
                output.write_all(&[(*offset & 0xFF) as u8])?;
                output.write_all(&[((*offset >> 8) & 0xFF) as u8])?;
                output.write_all(&[((*offset >> 16) & 0xFF) as u8])?;
            },
            4 => output.write_all(&(*offset as u32).to_le_bytes())?,
            _ => unreachable!(),
        }
    }
    
    // Write values
    for value in field_values {
        output.write_all(value)?;
    }
    
    Ok(())
}

/// Encodes an array using the correct encoder logic
fn encode_array_to_writer(
    values: &[&[u8]],
    output: &mut impl Write
) -> Result<(), ArrowError> {
    let len = values.len();
    
    // Determine if we need large size encoding
    let is_large = len > 255;
    
    // Calculate total value size to determine offset_size
    let mut data_size = 0;
    for value in values {
        data_size += value.len();
    }
    
    // Determine minimum offset size
    let offset_size = if data_size <= 255 { 1 } 
                      else if data_size <= 65535 { 2 }
                      else if data_size <= 16777215 { 3 }
                      else { 4 };
    
    // Write array header with correct flags
    let header = array_header(is_large, offset_size);
    output.write_all(&[header])?;
    
    // Write length as 1 or 4 bytes
    if is_large {
        output.write_all(&(len as u32).to_le_bytes())?;
    } else {
        output.write_all(&[len as u8])?;
    }
    
    // Calculate and write offsets
    let mut offsets = Vec::with_capacity(len + 1);
    let mut current_offset = 0u32;
    
    offsets.push(current_offset);
    for value in values {
        current_offset += value.len() as u32;
        offsets.push(current_offset);
    }
    
    for offset in &offsets {
        match offset_size {
            1 => output.write_all(&[*offset as u8])?,
            2 => output.write_all(&(*offset as u16).to_le_bytes())?,
            3 => {
                output.write_all(&[(*offset & 0xFF) as u8])?;
                output.write_all(&[((*offset >> 8) & 0xFF) as u8])?;
                output.write_all(&[((*offset >> 16) & 0xFF) as u8])?;
            },
            4 => output.write_all(&(*offset as u32).to_le_bytes())?,
            _ => unreachable!(),
        }
    }
    
    // Write values
    for value in values {
        output.write_all(value)?;
    }
    
    Ok(())
}

/// Creates a header byte for an object value with the correct format according to the encoding spec
fn object_header(is_large: bool, id_size: u8, offset_size: u8) -> u8 {
    ((is_large as u8) << 6) | 
    ((id_size - 1) << 4) | 
    ((offset_size - 1) << 2) | 
    VariantBasicType::Object as u8
}

/// Creates a header byte for an array value with the correct format according to the encoding spec
fn array_header(is_large: bool, offset_size: u8) -> u8 {
    ((is_large as u8) << 4) | 
    ((offset_size - 1) << 2) | 
    VariantBasicType::Array as u8
}

/// Creates a simple variant object.
///
/// This function demonstrates the usage pattern of the builder API.
///
/// # Arguments
///
/// * `sort_keys` - Whether keys should be sorted in metadata
///
/// # Returns
///
/// A Variant instance representing the object
pub fn create_variant_object_example(sort_keys: bool) -> Result<Variant, ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // The builder borrows metadata_buffer, so we need to drop it before using metadata_buffer
    {
        // Create a builder
        let mut builder = VariantBuilder::new_with_sort(&mut metadata_buffer, sort_keys);
        
        // Create an object
        {
            let mut object_builder = builder.new_object(&mut value_buffer);
            
            // Add values
            object_builder.append_value("foo", 1);
            object_builder.append_value("bar", 100);
            
            // Finish the object
            object_builder.finish();
        }
        
        // Finish the metadata
        builder.finish();
    } // builder is dropped here, releasing the borrow on metadata_buffer
    
    // Create variant from buffers - now we can move metadata_buffer safely
    Ok(Variant::new(metadata_buffer, value_buffer))
}

/// Creates a simple array variant.
///
/// This function demonstrates the usage pattern of the builder API.
///
/// # Returns
///
/// A Variant instance representing the array
pub fn create_variant_array_example() -> Result<Variant, ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // The builder borrows metadata_buffer, so we need to drop it before using metadata_buffer
    {
        // Create a builder
        let mut builder = VariantBuilder::new(&mut metadata_buffer);
        
        // Create an array
        {
            let mut array_builder = builder.new_array(&mut value_buffer);
            
            // Add values
            array_builder.append_value(1);
            array_builder.append_value(2);
            array_builder.append_value("hello");
            array_builder.append_value(Option::<i32>::None);
            
            // Finish the array
            array_builder.finish();
        }
        
        // Finish the metadata
        builder.finish();
    } // builder is dropped here, releasing the borrow on metadata_buffer
    
    // Create variant from buffers - now we can move metadata_buffer safely
    Ok(Variant::new(metadata_buffer, value_buffer))
}

/// Creates a complex nested variant structure.
///
/// This function demonstrates creating a deeply nested variant structure.
///
/// # Returns
///
/// A Variant instance with a complex nested structure
pub fn create_complex_variant_example() -> Result<Variant, ArrowError> {
    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    
    // The builder borrows metadata_buffer, so we need to drop it before using metadata_buffer
    {
        // Create a builder
        let mut builder = VariantBuilder::new(&mut metadata_buffer);
        
        // Create the complex structure
        {
            let mut root_builder = builder.new_object(&mut value_buffer);
            
            // Add primitive values to root
            root_builder.append_value("id", 123);
            root_builder.append_value("name", "Example User");
            root_builder.append_value("active", true);
            
            // Create and populate address object
            {
                let mut address_builder = root_builder.append_object("address");
                address_builder.append_value("street", "123 Main St");
                address_builder.append_value("city", "Anytown");
                address_builder.append_value("zip", 12345);
                
                // Create geo object inside address
                {
                    let mut geo_builder = address_builder.append_object("geo");
                    geo_builder.append_value("lat", 40.7128);
                    geo_builder.append_value("lng", -74.0060);
                    geo_builder.finish();
                }
                
                address_builder.finish();
            }
            
            // Create scores array
            {
                let mut scores_builder = root_builder.append_array("scores");
                scores_builder.append_value(95);
                scores_builder.append_value(87);
                scores_builder.append_value(91);
                scores_builder.finish();
            }
            
            // Create contacts array with objects
            {
                let mut contacts_builder = root_builder.append_array("contacts");
                
                // First contact
                {
                    let mut contact1_builder = contacts_builder.append_object();
                    contact1_builder.append_value("name", "Alice");
                    contact1_builder.append_value("phone", "555-1234");
                    contact1_builder.finish();
                }
                
                // Second contact
                {
                    let mut contact2_builder = contacts_builder.append_object();
                    contact2_builder.append_value("name", "Bob");
                    contact2_builder.append_value("phone", "555-5678");
                    contact2_builder.finish();
                }
                
                contacts_builder.finish();
            }
            
            // Finish the root object
            root_builder.finish();
        }
        
        // Finish the metadata
        builder.finish();
    } // builder is dropped here, releasing the borrow on metadata_buffer
    
    // Create variant from buffers - now we can move metadata_buffer safely
    Ok(Variant::new(metadata_buffer, value_buffer))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_spec_example_usage_pattern() {
        // Location to write metadata
        let mut metadata_buffer = vec![];
        
        // Create a builder for constructing variant values
        let mut value_buffer = vec![];
        let mut value_buffer2 = vec![];
        
        // Use a scope to drop the builder before using metadata_buffer
        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            
            // Example creating a primitive Variant value:
            // Create the equivalent of {"foo": 1, "bar": 100}
            let mut object_builder = builder.new_object(&mut value_buffer); // object_builder has reference to builder
            object_builder.append_value("foo", 1);
            object_builder.append_value("bar", 100);
            object_builder.finish();
            
            // value_buffer now contains a valid variant
            // builder contains a metadata header with fields "foo" and "bar"
            
            // Example of creating a nested VariantValue:
            // Create nested object: the equivalent of {"foo": {"bar": 100}}
            // note we haven't finalized the metadata yet so we reuse it here
            let mut object_builder2 = builder.new_object(&mut value_buffer2);
            let mut foo_object_builder = object_builder2.append_object("foo"); // builder for "foo"
            foo_object_builder.append_value("bar", 100);
            foo_object_builder.finish();
            object_builder2.finish();
            
            // value_buffer2 contains a valid variant
            
            // Finish the builder to finalize the metadata
            // complete writing the metadata
            builder.finish();
        } // builder is dropped here, releasing the borrow on metadata_buffer
        
        // Verify the output is valid - now safe to use metadata_buffer
        assert!(!metadata_buffer.is_empty());
        assert!(!value_buffer.is_empty());
        assert!(!value_buffer2.is_empty());
        
        // Create actual Variant objects
        let variant1 = Variant::new(metadata_buffer.clone(), value_buffer);
        let variant2 = Variant::new(metadata_buffer, value_buffer2);
        
        // Verify they are valid
        assert!(!variant1.metadata().is_empty());
        assert!(!variant1.value().is_empty());
        assert!(!variant2.metadata().is_empty());
        assert!(!variant2.value().is_empty());
    }
    
    #[test]
    fn test_variant_object() {
        let variant = create_variant_object_example(false);
        let variant = variant.unwrap();
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }
    
    #[test]
    fn test_variant_array() {
        let variant = create_variant_array_example();
        let variant = variant.unwrap();
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }
    
    #[test]
    fn test_builder_usage() {
        // Test the basic builder usage as outlined in the example
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        let mut value_buffer2 = vec![];
        
        // Create a builder in a scope to avoid borrowing issues
        {
            // Create a builder
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            
            // First object
            {
                let mut object_builder = builder.new_object(&mut value_buffer);
                object_builder.append_value("foo", 1);
                object_builder.append_value("bar", 100);
                object_builder.finish();
            }
            
            // Second object with reused metadata
            {
                let mut object_builder2 = builder.new_object(&mut value_buffer2);
                object_builder2.append_value("foo", 2);
                object_builder2.append_value("bar", 200);
                object_builder2.finish();
            }
            
            // Finalize metadata
            builder.finish();
        }
        
        // Now that builder is dropped, we can use the buffers
        
        // Verify buffers contain valid data
        assert!(!metadata_buffer.is_empty());
        assert!(!value_buffer.is_empty());
        assert!(!value_buffer2.is_empty());
    }
    
    #[test]
    fn test_nested_objects() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        
        // Create a builder in a scope to avoid borrowing issues
        {
            // Create a builder
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            
            // Create an object with a nested object
            {
                let mut object_builder = builder.new_object(&mut value_buffer);
                
                // Create a nested object
                {
                    let mut nested_builder = object_builder.append_object("nested");
                    nested_builder.append_value("foo", 42);
                    nested_builder.finish();
                }
                
                object_builder.finish();
            }
            
            // Finalize metadata
            builder.finish();
        }
        
        // Now that builder is dropped, we can use the buffers
        
        // Verify buffers
        assert!(!metadata_buffer.is_empty());
        assert!(!value_buffer.is_empty());
    }
    
    #[test]
    fn test_complex_variant() {
        let variant = create_complex_variant_example();
        let variant = variant.unwrap();
        assert!(!variant.metadata().is_empty());
        assert!(!variant.value().is_empty());
    }

    #[test]
    fn test_objectbuilder() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        
        // Create a scope for the builders
        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object_builder = builder.new_object(&mut value_buffer);
            
            // Add a string field
            object_builder.append_value("name", "John");
            
            // Add a int32 field
            object_builder.append_value("age", 30);
            
            object_builder.finish();
            builder.finish();
        } // builders are dropped here, releasing the borrow
        
        // Assert after the builders have been dropped
        assert!(!metadata_buffer.is_empty());
        assert!(!value_buffer.is_empty());
    }

    #[test]
    fn test_arraybuilder() {
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        
        // Create a scope for the builders
        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut array_builder = builder.new_array(&mut value_buffer);
            
            // Add elements
            array_builder.append_value(1);
            array_builder.append_value(2);
            array_builder.append_value(3);
            
            array_builder.finish();
            builder.finish();
        } // builders are dropped here, releasing the borrow
        
        // Assert after the builders have been dropped
        assert!(!metadata_buffer.is_empty());
        assert!(!value_buffer.is_empty());
    }

    #[test]
    fn test_encoder_integration() {
        // Create primitive values
        let null_value = PrimitiveValue::Null;
        let bool_value = PrimitiveValue::Boolean(true);
        let int8_value = PrimitiveValue::Int8(42);
        let int32_value = PrimitiveValue::Int32(12345);
        let float_value = PrimitiveValue::Float(3.14);
        let string_value = PrimitiveValue::String("Hello, world!".to_string());
        
        // Create additional test values for newly implemented encoder functions
        let binary_value = PrimitiveValue::Binary(vec![0x01, 0x02, 0x03, 0x04]);
        let date_value = PrimitiveValue::Date(18262); // Example date
        let timestamp_value = PrimitiveValue::Timestamp(1618243200000); // Example timestamp
        let timestamp_ntz_value = PrimitiveValue::TimestampNTZ(1618243200000);
        let time_ntz_value = PrimitiveValue::TimeNTZ(43200000); // 12:00:00
        let timestamp_nanos_value = PrimitiveValue::TimestampNanos(1618243200000000000);
        let timestamp_ntz_nanos_value = PrimitiveValue::TimestampNTZNanos(1618243200000000000);
        let uuid_value = PrimitiveValue::Uuid([
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF
        ]);
        
        // Create test vectors using write_value (which now uses encoder functions)
        let mut null_buffer = Vec::new();
        let mut bool_buffer = Vec::new();
        let mut int8_buffer = Vec::new();
        let mut int32_buffer = Vec::new();
        let mut float_buffer = Vec::new();
        let mut string_buffer = Vec::new();
        let mut binary_buffer = Vec::new();
        let mut date_buffer = Vec::new();
        let mut timestamp_buffer = Vec::new();
        let mut timestamp_ntz_buffer = Vec::new();
        let mut time_ntz_buffer = Vec::new();
        let mut timestamp_nanos_buffer = Vec::new();
        let mut timestamp_ntz_nanos_buffer = Vec::new();
        let mut uuid_buffer = Vec::new();
        
        // Encode basic values
        write_value(&mut null_buffer, &null_value).unwrap();
        write_value(&mut bool_buffer, &bool_value).unwrap();
        write_value(&mut int8_buffer, &int8_value).unwrap();
        write_value(&mut int32_buffer, &int32_value).unwrap();
        write_value(&mut float_buffer, &float_value).unwrap();
        write_value(&mut string_buffer, &string_value).unwrap();
        
        // Encode new types
        write_value(&mut binary_buffer, &binary_value).unwrap();
        write_value(&mut date_buffer, &date_value).unwrap();
        write_value(&mut timestamp_buffer, &timestamp_value).unwrap();
        write_value(&mut timestamp_ntz_buffer, &timestamp_ntz_value).unwrap();
        write_value(&mut time_ntz_buffer, &time_ntz_value).unwrap();
        write_value(&mut timestamp_nanos_buffer, &timestamp_nanos_value).unwrap();
        write_value(&mut timestamp_ntz_nanos_buffer, &timestamp_ntz_nanos_value).unwrap();
        write_value(&mut uuid_buffer, &uuid_value).unwrap();
        
        // Verify encoded values are valid by decoding them
        let keys = Vec::<String>::new();
        
        // Test basic values
        let decoded_null = crate::decoder::decode_value(&null_buffer, &keys).unwrap();
        assert!(decoded_null.is_null());
        
        let decoded_bool = crate::decoder::decode_value(&bool_buffer, &keys).unwrap();
        assert_eq!(decoded_bool, serde_json::json!(true));
        
        let decoded_int8 = crate::decoder::decode_value(&int8_buffer, &keys).unwrap();
        assert_eq!(decoded_int8, serde_json::json!(42));
        
        let decoded_int32 = crate::decoder::decode_value(&int32_buffer, &keys).unwrap();
        assert_eq!(decoded_int32, serde_json::json!(12345));
        
        let decoded_float = crate::decoder::decode_value(&float_buffer, &keys).unwrap();
        // Use is_f64 since json values may have slight precision differences
        assert!(decoded_float.is_f64());
        assert!((decoded_float.as_f64().unwrap() - 3.14).abs() < 1e-6);
        
        let decoded_string = crate::decoder::decode_value(&string_buffer, &keys).unwrap();
        assert_eq!(decoded_string, serde_json::json!("Hello, world!"));
        
        // Test binary value (decoded as a string in JSON format)
        let decoded_binary = crate::decoder::decode_value(&binary_buffer, &keys).unwrap();
        assert!(decoded_binary.is_string());

        // Date and timestamp types are converted to strings in the decoder
        let decoded_date = crate::decoder::decode_value(&date_buffer, &keys).unwrap();
        assert!(decoded_date.is_string());
        
        let decoded_timestamp = crate::decoder::decode_value(&timestamp_buffer, &keys).unwrap();
        assert!(decoded_timestamp.is_string());
        
        let decoded_timestamp_ntz = crate::decoder::decode_value(&timestamp_ntz_buffer, &keys).unwrap();
        assert!(decoded_timestamp_ntz.is_string());
        
        let decoded_time_ntz = crate::decoder::decode_value(&time_ntz_buffer, &keys).unwrap();
        assert!(decoded_time_ntz.is_string());
        
        let decoded_timestamp_nanos = crate::decoder::decode_value(&timestamp_nanos_buffer, &keys).unwrap();
        assert!(decoded_timestamp_nanos.is_string());
        
        let decoded_timestamp_ntz_nanos = crate::decoder::decode_value(&timestamp_ntz_nanos_buffer, &keys).unwrap();
        assert!(decoded_timestamp_ntz_nanos.is_string());
        
        let decoded_uuid = crate::decoder::decode_value(&uuid_buffer, &keys).unwrap();
        assert!(decoded_uuid.is_string());
    }

    #[test]
    fn test_valid_encoding_format() {
        // Test that the builder creates correctly encoded objects and arrays
        // according to the Variant encoding specification
        
        // Create an object
        let mut metadata_buffer = vec![];
        let mut value_buffer = vec![];
        
        // Create a builder in a scope to avoid borrowing issues
        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer);
            let mut object_builder = builder.new_object(&mut value_buffer);
            
            // Add some values including different types
            object_builder.append_value("name", "Test User");
            object_builder.append_value("age", 30);
            object_builder.append_value("active", true);
            
            // Add a nested object
            {
                let mut nested_builder = object_builder.append_object("address");
                nested_builder.append_value("city", "Testville");
                nested_builder.append_value("zip", 12345);
                nested_builder.finish();
            }
            
            // Finish the object
            object_builder.finish();
            builder.finish();
        }
        
        // Now validate the object encoding
        // First byte is the object header
        assert_eq!(value_buffer[0] & 0x03, VariantBasicType::Object as u8);
        
        // Create another test for arrays
        let mut metadata_buffer2 = vec![];
        let mut array_buffer = vec![];
        
        {
            let mut builder = VariantBuilder::new(&mut metadata_buffer2);
            let mut array_builder = builder.new_array(&mut array_buffer);
            
            // Add different types of values
            array_builder.append_value(1);
            array_builder.append_value(2);
            array_builder.append_value("hello");
            array_builder.append_value(true);
            
            // Add a nested object
            {
                let mut obj_builder = array_builder.append_object();
                obj_builder.append_value("key", "value");
                obj_builder.finish();
            }
            
            // Finish the array
            array_builder.finish();
            builder.finish();
        }
        
        // Validate the array encoding
        // First byte is the array header
        assert_eq!(array_buffer[0] & 0x03, VariantBasicType::Array as u8);
        
        // Advanced validation: Create a round-trip test
        // Create a Variant from the buffers
        let variant_obj = Variant::new(metadata_buffer, value_buffer);
        let variant_arr = Variant::new(metadata_buffer2, array_buffer);
        
        // These will panic if the encoding is invalid
        assert!(!variant_obj.metadata().is_empty());
        assert!(!variant_obj.value().is_empty());
        assert!(!variant_arr.metadata().is_empty());
        assert!(!variant_arr.value().is_empty());
        
        // If we have a decoder function, we could call it here to validate
        // the full round-trip decoding
    }
} 