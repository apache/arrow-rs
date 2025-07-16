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

//! [`VariantArray`] implementation with hybrid byte-level and high-level APIs

use crate::field_operations::FieldOperations;
use arrow::array::{Array, ArrayData, BinaryViewArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use parquet_variant::{Variant, VariantMetadata};
use std::any::Any;
use std::sync::Arc;

/// Array implementation for variant data with hybrid byte-level and high-level APIs
#[derive(Debug)]
pub struct VariantArray {
    /// StructArray of up to three fields:
    ///
    /// 1. A required field named `metadata` which is binary, large_binary, or
    ///    binary_view
    ///
    /// 2. An optional field named `value` that is binary, large_binary, or
    ///    binary_view
    ///
    /// 3. An optional field named `typed_value` which can be any primitive type
    ///    or be a list, large_list, list_view or struct
    ///
    /// NOTE: It is also permissible for the metadata field to be
    /// Dictionary-Encoded, preferably (but not required) with an index type of
    /// int8.
    inner: StructArray,

    /// Reference to the metadata column of inner
    metadata_ref: ArrayRef,

    /// Reference to the value column of inner
    value_ref: ArrayRef,
}

impl VariantArray {
    /// Create a new VariantArray from a StructArray
    pub fn try_new(inner: Arc<StructArray>) -> Result<Self, ArrowError> {
        // Validate that the struct has the expected format
        if inner.num_columns() != 2 {
            return Err(ArrowError::InvalidArgumentError(
                "Expected struct with exactly 2 columns (metadata, value)".to_string(),
            ));
        };
        // Ensure the StructArray has a metadata field of BinaryView

        let Some(metadata_field) = VariantArray::find_metadata_field(inner) else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'metadata' field".to_string(),
            ));
        };
        if metadata_field.data_type() != &DataType::BinaryView {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'metadata' field must be BinaryView, got {}",
                metadata_field.data_type()
            )));
        }
        let Some(value_field) = VariantArray::find_value_field(inner) else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: StructArray must contain a 'value' field".to_string(),
            ));
        };
        if value_field.data_type() != &DataType::BinaryView {
            return Err(ArrowError::NotYetImplemented(format!(
                "VariantArray 'value' field must be BinaryView, got {}",
                value_field.data_type()
            )));
        }

        Ok(Self {
            inner: inner.clone(),
            metadata_ref: metadata_field,
            value_ref: value_field,
        })
    }

    /// Returns a reference to the underlying [`StructArray`].
    pub fn inner(&self) -> &StructArray {
        &self.inner
    }

    /// Returns the inner [`StructArray`], consuming self
    pub fn into_inner(self) -> StructArray {
        self.inner
    }

    /// Return the [`Variant`] instance stored at the given row
    ///
    /// Panics if the index is out of bounds.
    ///
    /// Note: Does not do deep validation of the [`Variant`], so it is up to the
    /// caller to ensure that the metadata and value were constructed correctly.
    pub fn value(&self, index: usize) -> Variant {
        let metadata = self.metadata_field().as_binary_view().value(index);
        let value = self.value_field().as_binary_view().value(index);
        Variant::new(metadata, value)
    }

    fn find_metadata_field(array: &StructArray) -> Option<ArrayRef> {
        array.column_by_name("metadata").cloned()
    }

    fn find_value_field(array: &StructArray) -> Option<ArrayRef> {
        array.column_by_name("value").cloned()
    }
    /// Extract a field from the variant at the specified row using a path.
    ///
    /// This method provides direct access to nested fields without reconstructing
    /// the entire variant, which is critical for performance with shredded variants.
    ///
    /// # Arguments
    /// * `index` - The row index in the array
    /// * `path` - The path to the field to extract
    ///
    /// # Returns
    /// * `Some(Variant)` if the field exists at the specified path
    /// * `None` if the field doesn't exist or the path is invalid
    ///
    /// # Example
    /// ```
    /// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantPath};
    /// # use parquet_variant::VariantBuilder;
    /// # let mut builder = VariantArrayBuilder::new(1);
    /// # let mut variant_builder = VariantBuilder::new();
    /// # let mut obj = variant_builder.new_object();
    /// # obj.insert("name", "Alice");
    /// # obj.finish().unwrap();
    /// # let (metadata, value) = variant_builder.finish();
    /// # builder.append_variant_buffers(&metadata, &value);
    /// # let variant_array = builder.build();
    /// let path = VariantPath::field("name");
    /// let name_variant = variant_array.get_path(0, &path);
    /// ```
    pub fn get_path(&self, index: usize, path: &VariantPath) -> Option<Variant> {
        if path.is_empty() {
            return Some(self.value(index));
        }

        // Start with the root variant
        let mut current = self.value(index);
        
        Ok(Self { inner })
    }
    
    /// Get the metadata field as a BinaryViewArray
    pub fn metadata_field(&self) -> &BinaryViewArray {
        self.inner.column(0)
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .expect("Expected metadata field to be BinaryViewArray")
    }
    
    /// Get the value field as a BinaryViewArray
    pub fn value_field(&self) -> &BinaryViewArray {
        self.inner.column(1)
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .expect("Expected value field to be BinaryViewArray")
    }
    
    /// Get the metadata bytes for a specific index
    pub fn metadata(&self, index: usize) -> &[u8] {
        self.metadata_field().value(index).as_ref()
    }
    
    /// Get the value bytes for a specific index
    pub fn value_bytes(&self, index: usize) -> &[u8] {
        self.value_field().value(index).as_ref()
    }
    
    /// Get the parsed variant at a specific index
    pub fn value(&self, index: usize) -> Variant {
        if index >= self.len() {
            panic!("Index {} out of bounds for array of length {}", index, self.len());
        }
        
        if self.is_null(index) {
            return Variant::Null;
        }
        
        let metadata = self.metadata(index);
        let value = self.value_bytes(index);
        
        let variant_metadata = VariantMetadata::try_new(metadata)
            .expect("Failed to parse variant metadata");
        Variant::try_new_with_metadata(variant_metadata, value)
            .expect("Failed to create variant from metadata and value")
    }
    
    /// Get value at a specific path for the variant at the given index
    /// 
    /// Uses high-level Variant API for convenience. Returns a Variant object that can be
    /// directly used with standard variant operations.
    pub fn get_path(&self, index: usize, path: &crate::field_operations::VariantPath) -> Option<parquet_variant::Variant> {
        if index >= self.len() || self.is_null(index) {
            return None;
        }
        
        let mut current_variant = self.value(index);
        
        for element in path.elements() {
            match element {
                crate::field_operations::VariantPathElement::Field(field_name) => {
                    current_variant = current_variant.get_object_field(field_name)?;
                }
                crate::field_operations::VariantPathElement::Index(idx) => {
                    current_variant = current_variant.get_list_element(*idx)?;
                }
            }
        }
        
        Some(current_variant)
    }
    
    /// Get values at multiple paths for the variant at the given index
    /// 
    /// Convenience method that applies `get_path()` to multiple paths at once.
    /// Useful for extracting multiple fields from a single variant row.
    pub fn get_paths(&self, index: usize, paths: &[crate::field_operations::VariantPath]) -> Vec<Option<parquet_variant::Variant>> {
        let mut results = Vec::new();
        for path in paths {
            results.push(self.get_path(index, path));
        }
        results
    }
    
    /// Get the field names for an object at the given index
    pub fn get_field_names(&self, index: usize) -> Vec<String> {
        if index >= self.len() {
            return vec![];
        }
        
        if self.is_null(index) {
            return vec![];
        }
        
        let variant = self.value(index);
        if let Some(obj) = variant.as_object() {
            let mut paths = Vec::new();
            for i in 0..obj.len() {
                if let Some(field_name) = obj.field_name(i) {
                    paths.push(field_name.to_string());
                }
            }
            paths
        } else {
            vec![]
        }
    }
    
    /// Extract field values by path from all variants in the array
    /// 
    /// Applies `get_path()` to a single path across all rows in the array.
    /// Useful for extracting a column of values from nested variant data.
    pub fn extract_field_by_path(&self, path: &crate::field_operations::VariantPath) -> Vec<Option<parquet_variant::Variant>> {
        let mut results = Vec::new();
        for i in 0..self.len() {
            results.push(self.get_path(i, path));
        }
        results
    }

    /// Return a reference to the metadata field of the [`StructArray`]
    pub fn metadata_field(&self) -> &ArrayRef {
        // spec says fields order is not guaranteed, so we search by name
        &self.metadata_ref
    }

    /// Return a reference to the value field of the `StructArray`
    pub fn value_field(&self) -> &ArrayRef {
        // spec says fields order is not guaranteed, so we search by name
        &self.value_ref
    }
    
    /// Create a new VariantArray with a field removed from all variants
    pub fn with_field_removed(&self, field_name: &str) -> Result<Self, ArrowError> {
        let mut builder = crate::variant_array_builder::VariantArrayBuilder::new(self.len());
        
        for i in 0..self.len() {
            if self.is_null(i) {
                builder.append_null();
            } else {
                match FieldOperations::remove_field_bytes(self.metadata(i), self.value_bytes(i), field_name)? {
                    Some(new_value) => {
                        builder.append_variant_buffers(self.metadata(i), &new_value);
                    }
                    None => {
                        // Field didn't exist, use original value
                        builder.append_variant_buffers(self.metadata(i), self.value_bytes(i));
                    }
                }
            }
        }
        
        Ok(builder.build())
    }
    
    /// Create a new VariantArray with multiple fields removed from all variants
    pub fn with_fields_removed(&self, field_names: &[&str]) -> Result<Self, ArrowError> {
        let mut builder = crate::variant_array_builder::VariantArrayBuilder::new(self.len());
        
        for i in 0..self.len() {
            if self.is_null(i) {
                builder.append_null();
            } else {
                match FieldOperations::remove_fields_bytes(self.metadata(i), self.value_bytes(i), field_names)? {
                    Some(new_value) => {
                        builder.append_variant_buffers(self.metadata(i), &new_value);
                    }
                    None => {
                        // No fields existed, use original value
                        builder.append_variant_buffers(self.metadata(i), self.value_bytes(i));
                    }
                }
            }
        }
        
        Ok(builder.build())
    }
}

impl Array for VariantArray {
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }
    
    fn into_data(self) -> ArrayData {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.into_data(),
            Err(inner) => inner.to_data(),
        }
    }
    
    fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let slice = self.inner.slice(offset, length);
        let met = self.metadata_ref.slice(offset, length);
        let val = self.value_ref.slice(offset, length);
        Arc::new(Self {
            inner: slice,
            metadata_ref: met,
            value_ref: val,
        })
    }
    
    fn len(&self) -> usize {
        self.inner.len()
    }
    
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    
    fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }
    
    fn offset(&self) -> usize {
        self.inner.offset()
    }
    
    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }
    
    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant_array_builder::VariantArrayBuilder;
    use parquet_variant::VariantBuilder;

    fn create_test_variant_array() -> VariantArray {
        let mut builder = VariantArrayBuilder::new(2);
        
        // Create variant 1: {"name": "Alice", "age": 30}
        let mut builder1 = VariantBuilder::new();
        {
            let mut obj = builder1.new_object();
            obj.insert("name", "Alice");
            obj.insert("age", 30i32);
            obj.finish().unwrap();
        }
        let (metadata1, value1) = builder1.finish();
        builder.append_variant_buffers(&metadata1, &value1);
        
        // Create variant 2: {"name": "Bob", "age": 25, "city": "NYC"}
        let mut builder2 = VariantBuilder::new();
        {
            let mut obj = builder2.new_object();
            obj.insert("name", "Bob");
            obj.insert("age", 25i32);
            obj.insert("city", "NYC");
            obj.finish().unwrap();
        }
        let (metadata2, value2) = builder2.finish();
        builder.append_variant_buffers(&metadata2, &value2);
        
        builder.build()
    }

    #[test]
    fn test_variant_array_basic() {
        let array = create_test_variant_array();
        assert_eq!(array.len(), 2);
        assert!(!array.is_empty());
        
        // Test accessing variants
        let variant1 = array.value(0);
        assert_eq!(variant1.get_object_field("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(variant1.get_object_field("age").unwrap().as_int32(), Some(30));
        
        let variant2 = array.value(1);
        assert_eq!(variant2.get_object_field("name").unwrap().as_string(), Some("Bob"));
        assert_eq!(variant2.get_object_field("age").unwrap().as_int32(), Some(25));
        assert_eq!(variant2.get_object_field("city").unwrap().as_string(), Some("NYC"));
    }

    #[test]
    fn test_get_field_names() {
        let array = create_test_variant_array();
        
        let paths1 = array.get_field_names(0);
        assert_eq!(paths1.len(), 2);
        assert!(paths1.contains(&"name".to_string()));
        assert!(paths1.contains(&"age".to_string()));
        
        let paths2 = array.get_field_names(1);
        assert_eq!(paths2.len(), 3);
        assert!(paths2.contains(&"name".to_string()));
        assert!(paths2.contains(&"age".to_string()));
        assert!(paths2.contains(&"city".to_string()));
    }

    #[test]
    fn test_get_path() {
        let array = create_test_variant_array();
        
        // Test field access
        let name_path = crate::field_operations::VariantPath::field("name");
        let alice_name = array.get_path(0, &name_path).unwrap();
        assert_eq!(alice_name.as_string(), Some("Alice"));
        
        // Test non-existent field
        let nonexistent_path = crate::field_operations::VariantPath::field("nonexistent");
        let result = array.get_path(0, &nonexistent_path);
        assert!(result.is_none());
    }

    #[test]
    fn test_with_field_removed() {
        let array = create_test_variant_array();
        
        let new_array = array.with_field_removed("age").unwrap();
        
        // Check that age field was removed from all variants
        let variant1 = new_array.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.len(), 1);
        assert!(obj1.get("name").is_some());
        assert!(obj1.get("age").is_none());
        
        let variant2 = new_array.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.len(), 2);
        assert!(obj2.get("name").is_some());
        assert!(obj2.get("age").is_none());
        assert!(obj2.get("city").is_some());
    }

    #[test]
    fn test_metadata_and_value_fields() {
        let array = create_test_variant_array();
        
        let metadata_field = array.metadata_field();
        let value_field = array.value_field();
        
        // Check that we got the expected arrays
        assert_eq!(metadata_field.len(), 2);
        assert_eq!(value_field.len(), 2);
        
        // Check that metadata and value bytes are non-empty
        assert!(!metadata_field.value(0).is_empty());
        assert!(!value_field.value(0).is_empty());
        assert!(!metadata_field.value(1).is_empty());
        assert!(!value_field.value(1).is_empty());
    }
}

