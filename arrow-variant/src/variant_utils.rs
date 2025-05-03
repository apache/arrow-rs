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

//! Utilities for working with Variant as a StructArray

use arrow_array::{Array, ArrayRef, BinaryArray, StructArray};
use arrow_array::builder::BinaryBuilder;
use arrow_schema::{ArrowError, DataType, Field};
use arrow_schema::extension::Variant;
use std::sync::Arc;

/// Validate that a struct array can be used as a variant array
pub fn validate_struct_array(array: &StructArray) -> Result<(), ArrowError> {
    // Check that the struct has both metadata and value fields
    let fields = array.fields();
    
    if fields.len() != 2 {
        return Err(ArrowError::InvalidArgumentError(
            "Variant struct must have exactly two fields".to_string(),
        ));
    }

    let metadata_field = fields
        .iter()
        .find(|f| f.name() == "metadata")
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Variant struct must have a field named 'metadata'".to_string(),
            )
        })?;

    let value_field = fields
        .iter()
        .find(|f| f.name() == "value")
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Variant struct must have a field named 'value'".to_string(),
            )
        })?;

    // Check field types
    match (metadata_field.data_type(), value_field.data_type()) {
        (DataType::Binary, DataType::Binary) | (DataType::LargeBinary, DataType::LargeBinary) => {
            Ok(())
        }
        _ => Err(ArrowError::InvalidArgumentError(
            "Variant struct fields must both be Binary or LargeBinary".to_string(),
        )),
    }
}

/// Extract a Variant object from a struct array at the given index
pub fn get_variant(array: &StructArray, index: usize) -> Result<Variant, ArrowError> {
    // Verify index is valid
    if index >= array.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Index out of bounds".to_string(),
        ));
    }

    // Skip if null
    if array.is_null(index) {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot extract variant from null value".to_string(),
        ));
    }

    // Get metadata and value columns
    let metadata_array = array
        .column_by_name("metadata")
        .ok_or_else(|| ArrowError::InvalidArgumentError("Missing metadata field".to_string()))?;

    let value_array = array
        .column_by_name("value")
        .ok_or_else(|| ArrowError::InvalidArgumentError("Missing value field".to_string()))?;

    // Extract binary data
    let metadata = extract_binary_data(metadata_array, index)?;
    let value = extract_binary_data(value_array, index)?;

    Ok(Variant::new(metadata, value))
}

/// Extract binary data from a binary array at the specified index
fn extract_binary_data(array: &ArrayRef, index: usize) -> Result<Vec<u8>, ArrowError> {
    match array.data_type() {
        DataType::Binary => {
            let binary_array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast binary array".to_string())
                })?;
            Ok(binary_array.value(index).to_vec())
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported binary type: {}",
            array.data_type()
        ))),
    }
}

/// Create a variant struct array from a collection of variants
pub fn create_variant_array(
    variants: Vec<Variant>
) -> Result<StructArray, ArrowError> {
    if variants.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot create variant array from empty variants".to_string(),
        ));
    }

    // Create binary builders for metadata and value
    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = BinaryBuilder::new();

    // Add variants to builders
    for variant in &variants {
        metadata_builder.append_value(variant.metadata());
        value_builder.append_value(variant.value());
    }

    // Create arrays
    let metadata_array = metadata_builder.finish();
    let value_array = value_builder.finish();

    // Create fields
    let fields = vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ];

    // Create arrays vector
    let arrays: Vec<ArrayRef> = vec![Arc::new(metadata_array), Arc::new(value_array)];

    // Build struct array
    let struct_array = StructArray::try_new(fields.into(), arrays, None)?;

    Ok(struct_array)
}

/// Create an empty variant struct array with given capacity
pub fn create_empty_variant_array(capacity: usize) -> Result<StructArray, ArrowError> {
    // Create binary builders for metadata and value
    let mut metadata_builder = BinaryBuilder::with_capacity(capacity, 0);
    let mut value_builder = BinaryBuilder::with_capacity(capacity, 0);

    // Create arrays
    let metadata_array = metadata_builder.finish();
    let value_array = value_builder.finish();

    // Create fields
    let fields = vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ];

    // Create arrays vector
    let arrays: Vec<ArrayRef> = vec![Arc::new(metadata_array), Arc::new(value_array)];

    // Build struct array
    StructArray::try_new(fields.into(), arrays, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use crate::metadata::create_test_metadata;

    #[test]
    fn test_variant_array_creation() {
        // Create metadata and value for each variant
        let metadata = create_test_metadata();

        // Create variants with different values
        let variants = vec![
            Variant::new(metadata.clone(), b"null".to_vec()),
            Variant::new(metadata.clone(), b"true".to_vec()),
            Variant::new(metadata.clone(), b"{\"a\": 1}".to_vec()),
        ];

        // Create a VariantArray
        let variant_array = create_variant_array(variants.clone()).unwrap();

        // Access variants from the array
        assert_eq!(variant_array.len(), 3);
        
        let retrieved = get_variant(&variant_array, 0).unwrap();
        assert_eq!(retrieved.metadata(), &metadata);
        assert_eq!(retrieved.value(), b"null");

        let retrieved = get_variant(&variant_array, 1).unwrap();
        assert_eq!(retrieved.metadata(), &metadata);
        assert_eq!(retrieved.value(), b"true");
    }

    #[test]
    fn test_validate_struct_array() {
        // Create metadata and value for each variant
        let metadata = create_test_metadata();

        // Create variants with different values
        let variants = vec![
            Variant::new(metadata.clone(), b"null".to_vec()),
            Variant::new(metadata.clone(), b"true".to_vec()),
        ];

        // Create a VariantArray
        let variant_array = create_variant_array(variants.clone()).unwrap();

        // Validate it
        assert!(validate_struct_array(&variant_array).is_ok());
    }

    #[test]
    fn test_get_variant_error() {
        // Create an empty array
        let empty_array = create_empty_variant_array(0).unwrap();
        
        // Should error when trying to get a variant from an empty array
        let result = get_variant(&empty_array, 0);
        assert!(result.is_err());
    }
} 