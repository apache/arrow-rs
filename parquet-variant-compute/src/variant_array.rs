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

//! [`VariantArray`] implementation

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType};
use parquet_variant::Variant;
use std::any::Any;
use std::sync::Arc;

/// Path element for accessing nested variant fields
#[derive(Debug, Clone, PartialEq)]
pub enum VariantPathElement {
    /// Access a field in an object by name
    Field(String),
    /// Access an element in an array by index
    Index(usize),
}

/// A path specification for accessing nested variant data
#[derive(Debug, Clone, PartialEq)]
pub struct VariantPath {
    elements: Vec<VariantPathElement>,
}

impl VariantPath {
    /// Create a new empty path
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Create a path from a single field name
    pub fn field(name: impl Into<String>) -> Self {
        Self {
            elements: vec![VariantPathElement::Field(name.into())],
        }
    }

    /// Create a path from a single array index
    pub fn index(idx: usize) -> Self {
        Self {
            elements: vec![VariantPathElement::Index(idx)],
        }
    }

    /// Add a field access to this path
    pub fn push_field(mut self, name: impl Into<String>) -> Self {
        self.elements.push(VariantPathElement::Field(name.into()));
        self
    }

    /// Add an array index access to this path
    pub fn push_index(mut self, idx: usize) -> Self {
        self.elements.push(VariantPathElement::Index(idx));
        self
    }

    /// Get the path elements
    pub fn elements(&self) -> &[VariantPathElement] {
        &self.elements
    }

    /// Check if this path is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl Default for VariantPath {
    fn default() -> Self {
        Self::new()
    }
}

/// An array of Parquet [`Variant`] values
///
/// A [`VariantArray`] wraps an Arrow [`StructArray`] that stores the underlying
/// `metadata` and `value` fields, and adds convenience methods to access
/// the `Variant`s
///
/// See [`VariantArrayBuilder`] for constructing a `VariantArray`.
///
/// [`VariantArrayBuilder`]: crate::VariantArrayBuilder
///
/// # Specification
///
/// 1. This code follows the conventions for storing variants in Arrow `StructArray`
///    defined by [Extension Type for Parquet Variant arrow] and this [document].
///    At the time of this writing, this is not yet a standardized Arrow extension type.
///
/// [Extension Type for Parquet Variant arrow]: https://github.com/apache/arrow/issues/46908
/// [document]: https://docs.google.com/document/d/1pw0AWoMQY3SjD7R4LgbPvMjG_xSCtXp3rZHkVp9jpZ4/edit?usp=sharing
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
    /// Creates a new `VariantArray` from a [`StructArray`].
    ///
    /// # Arguments
    /// - `inner` - The underlying [`StructArray`] that contains the variant data.
    ///
    /// # Returns
    /// - A new instance of `VariantArray`.
    ///
    /// # Errors:
    /// - If the `StructArray` does not contain the required fields
    ///
    /// # Current support
    /// This structure does not (yet) support the full Arrow Variant Array specification.
    ///
    /// Only `StructArrays` with `metadata` and `value` fields that are
    /// [`BinaryViewArray`] are supported. Shredded values are not currently supported
    /// nor are using types other than `BinaryViewArray`
    ///
    /// [`BinaryViewArray`]: arrow::array::BinaryViewArray
    pub fn try_new(inner: ArrayRef) -> Result<Self, ArrowError> {
        let Some(inner) = inner.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid VariantArray: requires StructArray as input".to_string(),
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
        
        // Navigate through the path elements
        for element in path.elements() {
            match element {
                VariantPathElement::Field(field_name) => {
                    current = current.get_object_field(field_name)?;
                }
                VariantPathElement::Index(idx) => {
                    current = current.get_list_element(*idx)?;
                }
            }
        }

        Some(current)
    }

    /// Extract multiple fields from the variant at the specified row using paths.
    ///
    /// This method is more efficient than calling `get_path` multiple times
    /// for the same row, as it avoids repeated work.
    ///
    /// # Arguments
    /// * `index` - The row index in the array
    /// * `paths` - The paths to the fields to extract
    ///
    /// # Returns
    /// A vector of `Option<Variant>` where each element corresponds to the
    /// field at the same index in the paths vector.
    ///
    /// # Example
    /// ```
    /// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantPath};
    /// # use parquet_variant::VariantBuilder;
    /// # let mut builder = VariantArrayBuilder::new(1);
    /// # let mut variant_builder = VariantBuilder::new();
    /// # let mut obj = variant_builder.new_object();
    /// # obj.insert("name", "Alice");
    /// # obj.insert("email", "alice@example.com");
    /// # obj.insert("timestamp", 1234567890i64);
    /// # obj.finish().unwrap();
    /// # let (metadata, value) = variant_builder.finish();
    /// # builder.append_variant_buffers(&metadata, &value);
    /// # let variant_array = builder.build();
    /// let paths = vec![
    ///     VariantPath::field("name"),
    ///     VariantPath::field("email"),
    ///     VariantPath::field("timestamp"),
    /// ];
    /// let fields = variant_array.get_paths(0, &paths);
    /// ```
    pub fn get_paths(&self, index: usize, paths: &[VariantPath]) -> Vec<Option<Variant>> {
        paths.iter().map(|path| self.get_path(index, path)).collect()
    }

    /// Extract a specific field from all rows in the array.
    ///
    /// This method is optimized for extracting the same field from many rows,
    /// which is a common operation in analytical queries.
    ///
    /// # Arguments
    /// * `path` - The path to the field to extract from all rows
    ///
    /// # Returns
    /// A vector of `Option<Variant>` where each element corresponds to the
    /// field value at the same row index.
    ///
    /// # Example
    /// ```
    /// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantPath};
    /// # use parquet_variant::VariantBuilder;
    /// # let mut builder = VariantArrayBuilder::new(1);
    /// # let mut variant_builder = VariantBuilder::new();
    /// # let mut obj = variant_builder.new_object();
    /// # obj.insert("id", 123i32);
    /// # obj.finish().unwrap();
    /// # let (metadata, value) = variant_builder.finish();
    /// # builder.append_variant_buffers(&metadata, &value);
    /// # let variant_array = builder.build();
    /// let path = VariantPath::field("id");
    /// let user_ids = variant_array.extract_field(&path);
    /// ```
    pub fn extract_field(&self, path: &VariantPath) -> Vec<Option<Variant>> {
        (0..self.len())
            .map(|i| self.get_path(i, path))
            .collect()
    }

    /// Extract multiple fields from all rows in the array.
    ///
    /// This method is optimized for extracting multiple fields from many rows,
    /// which is essential for efficient shredding operations.
    ///
    /// # Arguments
    /// * `paths` - The paths to the fields to extract from all rows
    ///
    /// # Returns
    /// A vector of vectors where the outer vector corresponds to rows and
    /// the inner vector corresponds to the fields at the same index in paths.
    ///
    /// # Example
    /// ```
    /// # use parquet_variant_compute::{VariantArrayBuilder, VariantArray, VariantPath};
    /// # use parquet_variant::VariantBuilder;
    /// # let mut builder = VariantArrayBuilder::new(1);
    /// # let mut variant_builder = VariantBuilder::new();
    /// # let mut obj = variant_builder.new_object();
    /// # obj.insert("name", "Alice");
    /// # obj.insert("age", 30i32);
    /// # obj.finish().unwrap();
    /// # let (metadata, value) = variant_builder.finish();
    /// # builder.append_variant_buffers(&metadata, &value);
    /// # let variant_array = builder.build();
    /// let paths = vec![
    ///     VariantPath::field("name"),
    ///     VariantPath::field("age"),
    /// ];
    /// let extracted_data = variant_array.extract_fields(&paths);
    /// ```
    pub fn extract_fields(&self, paths: &[VariantPath]) -> Vec<Vec<Option<Variant>>> {
        (0..self.len())
            .map(|i| self.get_paths(i, paths))
            .collect()
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
}

impl Array for VariantArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.inner.into_data()
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

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{BinaryArray, BinaryViewArray};
    use arrow_schema::{Field, Fields};
    use parquet_variant::{Variant, VariantBuilder};

    #[test]
    fn invalid_not_a_struct_array() {
        let array = make_binary_view_array();
        // Should fail because the input is not a StructArray
        let err = VariantArray::try_new(array);
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: requires StructArray as input"
        );
    }

    #[test]
    fn invalid_missing_metadata() {
        let fields = Fields::from(vec![Field::new("value", DataType::BinaryView, true)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);
        // Should fail because the StructArray does not contain a 'metadata' field
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'metadata' field"
        );
    }

    #[test]
    fn invalid_missing_value() {
        let fields = Fields::from(vec![Field::new("metadata", DataType::BinaryView, false)]);
        let array = StructArray::new(fields, vec![make_binary_view_array()], None);
        // Should fail because the StructArray does not contain a 'value' field
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Invalid argument error: Invalid VariantArray: StructArray must contain a 'value' field"
        );
    }

    #[test]
    fn invalid_metadata_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, true), // Not yet supported
            Field::new("value", DataType::BinaryView, true),
        ]);
        let array = StructArray::new(
            fields,
            vec![make_binary_array(), make_binary_view_array()],
            None,
        );
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'metadata' field must be BinaryView, got Binary"
        );
    }

    #[test]
    fn invalid_value_field_type() {
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, true),
            Field::new("value", DataType::Binary, true), // Not yet supported
        ]);
        let array = StructArray::new(
            fields,
            vec![make_binary_view_array(), make_binary_array()],
            None,
        );
        let err = VariantArray::try_new(Arc::new(array));
        assert_eq!(
            err.unwrap_err().to_string(),
            "Not yet implemented: VariantArray 'value' field must be BinaryView, got Binary"
        );
    }

    #[test]
    fn test_variant_path_creation() {
        let path = VariantPath::field("user")
            .push_field("profile")
            .push_field("name");
        
        assert_eq!(path.elements().len(), 3);
        assert_eq!(path.elements()[0], VariantPathElement::Field("user".to_string()));
        assert_eq!(path.elements()[1], VariantPathElement::Field("profile".to_string()));
        assert_eq!(path.elements()[2], VariantPathElement::Field("name".to_string()));
    }

    #[test]
    fn test_variant_path_with_index() {
        let path = VariantPath::field("users")
            .push_index(0)
            .push_field("name");
        
        assert_eq!(path.elements().len(), 3);
        assert_eq!(path.elements()[0], VariantPathElement::Field("users".to_string()));
        assert_eq!(path.elements()[1], VariantPathElement::Index(0));
        assert_eq!(path.elements()[2], VariantPathElement::Field("name".to_string()));
    }

    #[test]
    fn test_get_path_simple_field() {
        let variant_array = create_test_variant_array();
        
        let path = VariantPath::field("name");
        let result = variant_array.get_path(0, &path);
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Variant::from("Alice"));
    }

    #[test]
    fn test_get_path_nested_field() {
        let variant_array = create_test_variant_array();
        
        let path = VariantPath::field("details").push_field("age");
        let result = variant_array.get_path(0, &path);
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Variant::from(30i32));
    }

    #[test]
    fn test_get_path_array_index() {
        let variant_array = create_test_variant_array();
        
        let path = VariantPath::field("hobbies").push_index(1);
        let result = variant_array.get_path(0, &path);
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Variant::from("cooking"));
    }

    #[test]
    fn test_get_path_nonexistent_field() {
        let variant_array = create_test_variant_array();
        
        let path = VariantPath::field("nonexistent");
        let result = variant_array.get_path(0, &path);
        
        assert!(result.is_none());
    }

    #[test]
    fn test_get_path_empty_path() {
        let variant_array = create_test_variant_array();
        
        let path = VariantPath::new();
        let result = variant_array.get_path(0, &path);
        
        assert!(result.is_some());
        // Should return the full variant
        let variant = result.unwrap();
        assert!(variant.as_object().is_some());
    }

    #[test]
    fn test_get_paths_multiple() {
        let variant_array = create_test_variant_array();
        
        let paths = vec![
            VariantPath::field("name"),
            VariantPath::field("details").push_field("age"),
            VariantPath::field("hobbies").push_index(0),
        ];
        
        let results = variant_array.get_paths(0, &paths);
        
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(Variant::from("Alice")));
        assert_eq!(results[1], Some(Variant::from(30i32)));
        assert_eq!(results[2], Some(Variant::from("reading")));
    }

    #[test]
    fn test_extract_field_all_rows() {
        let variant_array = create_test_variant_array_multiple_rows();
        
        let path = VariantPath::field("name");
        let results = variant_array.extract_field(&path);
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(Variant::from("Alice")));
        assert_eq!(results[1], Some(Variant::from("Bob")));
    }

    #[test]
    fn test_extract_fields_all_rows() {
        let variant_array = create_test_variant_array_multiple_rows();
        
        let paths = vec![
            VariantPath::field("name"),
            VariantPath::field("details").push_field("age"),
        ];
        
        let results = variant_array.extract_fields(&paths);
        
        assert_eq!(results.len(), 2); // 2 rows
        assert_eq!(results[0].len(), 2); // 2 fields per row
        assert_eq!(results[1].len(), 2); // 2 fields per row
        
        // Row 0
        assert_eq!(results[0][0], Some(Variant::from("Alice")));
        assert_eq!(results[0][1], Some(Variant::from(30i32)));
        
        // Row 1
        assert_eq!(results[1][0], Some(Variant::from("Bob")));
        assert_eq!(results[1][1], Some(Variant::from(25i32)));
    }

    fn make_binary_view_array() -> ArrayRef {
        Arc::new(BinaryViewArray::from(vec![b"test" as &[u8]]))
    }

    fn make_binary_array() -> ArrayRef {
        Arc::new(BinaryArray::from(vec![b"test" as &[u8]]))
    }

    /// Create a test VariantArray with a single row containing:
    /// {
    ///   "name": "Alice",
    ///   "details": {
    ///     "age": 30,
    ///     "city": "New York"
    ///   },
    ///   "hobbies": ["reading", "cooking", "hiking"]
    /// }
    fn create_test_variant_array() -> VariantArray {
        let mut builder = VariantBuilder::new();
        let mut obj = builder.new_object();
        
        obj.insert("name", "Alice");
        
        // Create details object
        {
            let mut details = obj.new_object("details");
            details.insert("age", 30i32);
            details.insert("city", "New York");
            let _ = details.finish();
        }
        
        // Create hobbies list
        {
            let mut hobbies = obj.new_list("hobbies");
            hobbies.append_value("reading");
            hobbies.append_value("cooking");
            hobbies.append_value("hiking");
            hobbies.finish();
        }
        
        obj.finish().unwrap();
        
        let (metadata, value) = builder.finish();
        
        // Create VariantArray
        let metadata_array = Arc::new(BinaryViewArray::from(vec![metadata.as_slice()]));
        let value_array = Arc::new(BinaryViewArray::from(vec![value.as_slice()]));
        
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ]);
        
        let struct_array = StructArray::new(fields, vec![metadata_array, value_array], None);
        
        VariantArray::try_new(Arc::new(struct_array)).unwrap()
    }

    /// Create a test VariantArray with multiple rows
    fn create_test_variant_array_multiple_rows() -> VariantArray {
        let mut metadata_vec = Vec::new();
        let mut value_vec = Vec::new();
        
        // Row 0: Alice
        {
            let mut builder = VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("name", "Alice");
            
            // Create details object
            {
                let mut details = obj.new_object("details");
                details.insert("age", 30i32);
                let _ = details.finish();
            }
            
            obj.finish().unwrap();
            let (metadata, value) = builder.finish();
            metadata_vec.push(metadata);
            value_vec.push(value);
        }
        
        // Row 1: Bob
        {
            let mut builder = VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("name", "Bob");
            
            // Create details object
            {
                let mut details = obj.new_object("details");
                details.insert("age", 25i32);
                let _ = details.finish();
            }
            
            obj.finish().unwrap();
            let (metadata, value) = builder.finish();
            metadata_vec.push(metadata);
            value_vec.push(value);
        }
        
        // Create VariantArray
        let metadata_array = Arc::new(BinaryViewArray::from(
            metadata_vec.iter().map(|m| m.as_slice()).collect::<Vec<_>>()
        ));
        let value_array = Arc::new(BinaryViewArray::from(
            value_vec.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
        ));
        
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ]);
        
        let struct_array = StructArray::new(fields, vec![metadata_array, value_array], None);
        
        VariantArray::try_new(Arc::new(struct_array)).unwrap()
    }
}
