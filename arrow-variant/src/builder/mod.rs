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
use crate::encoder::{VariantBasicType, VariantPrimitiveType};

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
        
        // Write object type tag (basic type = Object)
        let header = (VariantBasicType::Object as u8) & 0x03;
        if let Err(e) = temp_buffer.write_all(&[header]) {
            panic!("Failed to write object header: {}", e);
        }
        
        // Write the number of fields
        let field_count = self.value_buffers.len() as u32;
        if let Err(e) = temp_buffer.write_all(&field_count.to_le_bytes()) {
            panic!("Failed to write field count: {}", e);
        }
        
        // Write each field and value
        for (key_index, value_buffer) in &self.value_buffers {
            // Write key index as u32
            if let Err(e) = temp_buffer.write_all(&(*key_index as u32).to_le_bytes()) {
                panic!("Failed to write key index: {}", e);
            }
            
            // Write value
            if let Err(e) = temp_buffer.write_all(value_buffer) {
                panic!("Failed to write value: {}", e);
            }
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
        
        // Write array type tag (basic type = Array)
        let header = (VariantBasicType::Array as u8) & 0x03;
        if let Err(e) = temp_buffer.write_all(&[header]) {
            panic!("Failed to write array header: {}", e);
        }
        
        // Write the number of elements
        let element_count = self.value_buffers.len() as u32;
        if let Err(e) = temp_buffer.write_all(&element_count.to_le_bytes()) {
            panic!("Failed to write element count: {}", e);
        }
        
        // Write each element
        for value_buffer in &self.value_buffers {
            if let Err(e) = temp_buffer.write_all(value_buffer) {
                panic!("Failed to write array element: {}", e);
            }
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
/// This function handles the correct encoding of primitive values.
fn write_value(buffer: &mut impl Write, value: &PrimitiveValue) -> Result<(), ArrowError> {
    match value {
        PrimitiveValue::Null => {
            // Basic type = Primitive, Primitive type = Null
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Null as u8) << 2);
            buffer.write_all(&[header])?;
        },
        PrimitiveValue::Boolean(val) => {
            // Basic type = Primitive, Primitive type = BooleanTrue/BooleanFalse
            let prim_type = if *val { 
                VariantPrimitiveType::BooleanTrue 
            } else { 
                VariantPrimitiveType::BooleanFalse 
            };
            
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((prim_type as u8) << 2);
            buffer.write_all(&[header])?;
        },
        PrimitiveValue::Int8(val) => {
            // Basic type = Primitive, Primitive type = Int8
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Int8 as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&[*val as u8])?;
        },
        PrimitiveValue::Int16(val) => {
            // Basic type = Primitive, Primitive type = Int16
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Int16 as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Int32(val) => {
            // Basic type = Primitive, Primitive type = Int32
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Int32 as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Int64(val) => {
            // Basic type = Primitive, Primitive type = Int64
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Int64 as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Float(val) => {
            // Basic type = Primitive, Primitive type = Float
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Float as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Double(val) => {
            // Basic type = Primitive, Primitive type = Double
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Double as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::String(val) => {
            // For short strings (fits in a single byte), use ShortString type
            // Otherwise use Primitive + String type
            if val.len() <= 63 {
                // Basic type = ShortString
                let header = (VariantBasicType::ShortString as u8) & 0x03 | 
                             ((val.len() as u8) << 2);
                buffer.write_all(&[header])?;
                buffer.write_all(val.as_bytes())?;
            } else {
                // Basic type = Primitive, Primitive type = String
                let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                             ((VariantPrimitiveType::String as u8) << 2);
                buffer.write_all(&[header])?;
                
                // Write length followed by bytes
                let bytes = val.as_bytes();
                let len = bytes.len() as u32;
                buffer.write_all(&len.to_le_bytes())?;
                buffer.write_all(bytes)?;
            }
        },
        PrimitiveValue::Binary(val) => {
            // Basic type = Primitive, Primitive type = Binary
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Binary as u8) << 2);
            buffer.write_all(&[header])?;
            
            // Write length followed by bytes
            let len = val.len() as u32;
            buffer.write_all(&len.to_le_bytes())?;
            buffer.write_all(val)?;
        },
        PrimitiveValue::Date(val) => {
            // Basic type = Primitive, Primitive type = Date
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Date as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Timestamp(val) => {
            // Basic type = Primitive, Primitive type = Timestamp
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Timestamp as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::TimestampNTZ(val) => {
            // Basic type = Primitive, Primitive type = TimestampNTZ
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::TimestampNTZ as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::TimeNTZ(val) => {
            // Basic type = Primitive, Primitive type = TimeNTZ
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::TimeNTZ as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::TimestampNanos(val) => {
            // Basic type = Primitive, Primitive type = TimestampNanos
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::TimestampNanos as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::TimestampNTZNanos(val) => {
            // Basic type = Primitive, Primitive type = TimestampNTZNanos
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::TimestampNTZNanos as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(&val.to_le_bytes())?;
        },
        PrimitiveValue::Uuid(val) => {
            // Basic type = Primitive, Primitive type = Uuid
            let header = ((VariantBasicType::Primitive as u8) & 0x03) | 
                         ((VariantPrimitiveType::Uuid as u8) << 2);
            buffer.write_all(&[header])?;
            buffer.write_all(val)?;
        },
    }
    
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
} 