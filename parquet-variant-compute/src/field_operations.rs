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

//! Field extraction and removal operations for variant objects

use crate::variant_parser::{ObjectHeader, ObjectOffsets, VariantParser};
use arrow::error::ArrowError;
use parquet_variant::{VariantMetadata, VariantPath, VariantPathElement};
use std::collections::HashSet;

/// Field operations for variant objects
pub struct FieldOperations;

impl FieldOperations {
    /// Extract field bytes from a single variant object
    pub fn extract_field_bytes(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        field_name: &str,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        if !VariantParser::is_object(value_bytes) {
            return Ok(None);
        }

        let header_byte = value_bytes[0];
        let header = VariantParser::parse_object_header(header_byte)?;
        let num_elements = VariantParser::unpack_int(&value_bytes[1..], header.num_elements_size)?;
        let offsets = VariantParser::calculate_object_offsets(&header, num_elements);

        // Find field ID for the target field name
        let target_field_id = Self::find_field_id(metadata_bytes, field_name)?;
        let target_field_id = match target_field_id {
            Some(id) => id,
            None => return Ok(None), // Field not found
        };

        // Search for the field in the object
        for i in 0..num_elements {
            let field_id_offset = offsets.field_ids_start + (i * header.field_id_size);
            let field_id =
                VariantParser::unpack_int(&value_bytes[field_id_offset..], header.field_id_size)?;

            if field_id == target_field_id {
                return Self::extract_field_value_at_index(
                    value_bytes,
                    &header,
                    &offsets,
                    i,
                    num_elements,
                );
            }
        }

        Ok(None)
    }

    /// Remove field from a single variant object
    pub fn remove_field_bytes(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        field_name: &str,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        Self::remove_fields_bytes(metadata_bytes, value_bytes, &[field_name])
    }

    /// Remove multiple fields from a single variant object
    pub fn remove_fields_bytes(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        field_names: &[&str],
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        if !VariantParser::is_object(value_bytes) {
            return Ok(Some(value_bytes.to_vec()));
        }

        let header_byte = value_bytes[0];
        let header = VariantParser::parse_object_header(header_byte)?;
        let num_elements = VariantParser::unpack_int(&value_bytes[1..], header.num_elements_size)?;
        let offsets = VariantParser::calculate_object_offsets(&header, num_elements);

        // Find field IDs for target field names
        let target_field_ids = Self::find_field_ids(metadata_bytes, field_names)?;

        if target_field_ids.is_empty() {
            return Ok(Some(value_bytes.to_vec())); // No fields to remove
        }

        // Collect fields to keep
        let fields_to_keep = Self::collect_fields_to_keep(
            value_bytes,
            &header,
            &offsets,
            num_elements,
            &target_field_ids,
        )?;

        if fields_to_keep.len() == num_elements {
            return Ok(Some(value_bytes.to_vec())); // No fields were removed
        }

        // Sort fields by name for proper variant object ordering
        let sorted_fields = Self::sort_fields_by_name(metadata_bytes, fields_to_keep)?;

        // Reconstruct object with remaining fields
        Self::reconstruct_object(sorted_fields)
    }

    /// Find field ID for a given field name
    fn find_field_id(metadata_bytes: &[u8], field_name: &str) -> Result<Option<usize>, ArrowError> {
        let metadata = VariantMetadata::try_new(metadata_bytes)?;

        for dict_idx in 0..metadata.len() {
            if let Ok(name) = metadata.get(dict_idx) {
                if name == field_name {
                    return Ok(Some(dict_idx));
                }
            }
        }

        Ok(None)
    }

    /// Find field IDs for multiple field names
    fn find_field_ids(
        metadata_bytes: &[u8],
        field_names: &[&str],
    ) -> Result<HashSet<usize>, ArrowError> {
        let metadata = VariantMetadata::try_new(metadata_bytes)?;
        let mut target_field_ids = HashSet::new();

        for field_name in field_names {
            for dict_idx in 0..metadata.len() {
                if let Ok(name) = metadata.get(dict_idx) {
                    if name == *field_name {
                        target_field_ids.insert(dict_idx);
                        break;
                    }
                }
            }
        }

        Ok(target_field_ids)
    }

    /// Extract field value at a specific index
    fn extract_field_value_at_index(
        value_bytes: &[u8],
        header: &ObjectHeader,
        offsets: &ObjectOffsets,
        field_index: usize,
        num_elements: usize,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        // Get all field offsets
        let mut field_offsets = Vec::new();
        for i in 0..=num_elements {
            let offset_idx = offsets.field_offsets_start + (i * header.field_offset_size);
            let offset_val =
                VariantParser::unpack_int(&value_bytes[offset_idx..], header.field_offset_size)?;
            field_offsets.push(offset_val);
        }

        let field_start = field_offsets[field_index];

        // To find the end offset, we need to find the next field in byte order
        // Since fields are stored in alphabetical order, we can't just use field_index + 1
        // We need to find the smallest offset that's greater than field_start
        let mut field_end = field_offsets[num_elements]; // Default to final offset

        for i in 0..num_elements {
            if i != field_index {
                let other_offset = field_offsets[i];
                if other_offset > field_start && other_offset < field_end {
                    field_end = other_offset;
                }
            }
        }

        let field_start_absolute = offsets.values_start + field_start;
        let field_end_absolute = offsets.values_start + field_end;

        if field_start_absolute <= field_end_absolute && field_end_absolute <= value_bytes.len() {
            let field_value_bytes = &value_bytes[field_start_absolute..field_end_absolute];
            Ok(Some(field_value_bytes.to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Collect fields to keep (those not being removed)
    fn collect_fields_to_keep(
        value_bytes: &[u8],
        header: &ObjectHeader,
        offsets: &ObjectOffsets,
        num_elements: usize,
        target_field_ids: &HashSet<usize>,
    ) -> Result<Vec<(usize, Vec<u8>)>, ArrowError> {
        let mut fields_to_keep = Vec::new();

        for i in 0..num_elements {
            let field_id_offset = offsets.field_ids_start + (i * header.field_id_size);
            let field_id =
                VariantParser::unpack_int(&value_bytes[field_id_offset..], header.field_id_size)?;

            if !target_field_ids.contains(&field_id) {
                if let Some(field_value) = Self::extract_field_value_at_index(
                    value_bytes,
                    header,
                    offsets,
                    i,
                    num_elements,
                )? {
                    fields_to_keep.push((field_id, field_value));
                }
            }
        }

        Ok(fields_to_keep)
    }

    /// Sort fields by their names (variant objects must be sorted alphabetically)
    fn sort_fields_by_name(
        metadata_bytes: &[u8],
        mut fields: Vec<(usize, Vec<u8>)>,
    ) -> Result<Vec<(usize, Vec<u8>)>, ArrowError> {
        let metadata = VariantMetadata::try_new(metadata_bytes)?;

        fields.sort_by(|a, b| {
            let name_a = metadata.get(a.0).unwrap_or("");
            let name_b = metadata.get(b.0).unwrap_or("");
            name_a.cmp(name_b)
        });

        Ok(fields)
    }

    /// Reconstruct variant object from sorted fields
    fn reconstruct_object(fields: Vec<(usize, Vec<u8>)>) -> Result<Option<Vec<u8>>, ArrowError> {
        let new_num_elements = fields.len();
        let new_is_large = new_num_elements > 255;

        // Calculate sizes for new object
        let max_field_id = fields.iter().map(|(id, _)| *id).max().unwrap_or(0);
        let new_field_id_size = VariantParser::calculate_int_size(max_field_id);

        let total_values_size: usize = fields.iter().map(|(_, value)| value.len()).sum();
        let new_field_offset_size = VariantParser::calculate_int_size(total_values_size);

        // Build new object
        let mut new_value_bytes = Vec::new();

        // Write header
        let new_header = VariantParser::build_object_header(
            new_is_large,
            new_field_id_size,
            new_field_offset_size,
        );
        new_value_bytes.push(new_header);

        // Write num_elements
        if new_is_large {
            new_value_bytes.extend_from_slice(&(new_num_elements as u32).to_le_bytes());
        } else {
            new_value_bytes.push(new_num_elements as u8);
        }

        // Write field IDs
        for (field_id, _) in &fields {
            VariantParser::write_int_bytes(&mut new_value_bytes, *field_id, new_field_id_size);
        }

        // Write field offsets
        let mut current_offset = 0;
        for (_, field_value) in &fields {
            VariantParser::write_int_bytes(
                &mut new_value_bytes,
                current_offset,
                new_field_offset_size,
            );
            current_offset += field_value.len();
        }
        // Write final offset
        VariantParser::write_int_bytes(&mut new_value_bytes, current_offset, new_field_offset_size);

        // Write field values
        for (_, field_value) in &fields {
            new_value_bytes.extend_from_slice(field_value);
        }

        Ok(Some(new_value_bytes))
    }

    /// Get the bytes at a specific path through the variant data
    pub fn get_path_bytes(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        path: &VariantPath,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        let mut current_value = value_bytes.to_vec();

        for element in path.iter() {
            match element {
                VariantPathElement::Field { name } => {
                    if let Some(field_bytes) =
                        Self::get_field_bytes(metadata_bytes, &current_value, name)?
                    {
                        current_value = field_bytes;
                    } else {
                        return Ok(None);
                    }
                }
                VariantPathElement::Index { index } => {
                    if let Some(element_bytes) =
                        Self::get_array_element_bytes(metadata_bytes, &current_value, *index)?
                    {
                        current_value = element_bytes;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some(current_value))
    }

    /// Get the value at a specific path and return its type and data
    pub fn get_path_with_type(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        path: &VariantPath,
    ) -> Result<Option<(crate::variant_parser::VariantType, Vec<u8>)>, ArrowError> {
        if let Some(value_bytes) = Self::get_path_bytes(metadata_bytes, value_bytes, path)? {
            if !value_bytes.is_empty() {
                let variant_type = VariantParser::parse_variant_header(value_bytes[0])?;
                return Ok(Some((variant_type, value_bytes)));
            }
        }
        Ok(None)
    }

    /// Get field bytes from an object at the byte level
    fn get_field_bytes(
        metadata_bytes: &[u8],
        value_bytes: &[u8],
        field_name: &str,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        // Use the general dispatch parser to ensure we're dealing with an object
        if !value_bytes.is_empty() {
            match VariantParser::parse_variant_header(value_bytes[0])? {
                crate::variant_parser::VariantType::Object(_) => {
                    Self::extract_field_bytes(metadata_bytes, value_bytes, field_name)
                }
                _ => Ok(None), // Not an object, can't extract fields
            }
        } else {
            Ok(None)
        }
    }

    /// Get array element bytes at the byte level
    fn get_array_element_bytes(
        _metadata_bytes: &[u8],
        value_bytes: &[u8],
        index: usize,
    ) -> Result<Option<Vec<u8>>, ArrowError> {
        // Use the general dispatch parser to ensure we're dealing with an array
        if value_bytes.is_empty() {
            return Ok(None);
        }

        match VariantParser::parse_variant_header(value_bytes[0])? {
            crate::variant_parser::VariantType::Array(array_header) => {
                let num_elements =
                    VariantParser::unpack_int(&value_bytes[1..], array_header.num_elements_size)?;

                // Check bounds
                if index >= num_elements {
                    return Ok(None);
                }

                // Calculate array offsets
                let offsets = VariantParser::calculate_array_offsets(&array_header, num_elements);

                // Get element offset
                let element_offset_start =
                    offsets.element_offsets_start + index * array_header.element_offset_size;
                let element_offset_end = element_offset_start + array_header.element_offset_size;

                if element_offset_end > value_bytes.len() {
                    return Err(ArrowError::InvalidArgumentError(
                        "Element offset exceeds value buffer".to_string(),
                    ));
                }

                let element_offset = VariantParser::unpack_int(
                    &value_bytes[element_offset_start..element_offset_end],
                    array_header.element_offset_size,
                )?;

                // Get next element offset (or end of data)
                let next_offset = if index + 1 < num_elements {
                    let next_element_offset_start = offsets.element_offsets_start
                        + (index + 1) * array_header.element_offset_size;
                    let next_element_offset_end =
                        next_element_offset_start + array_header.element_offset_size;
                    VariantParser::unpack_int(
                        &value_bytes[next_element_offset_start..next_element_offset_end],
                        array_header.element_offset_size,
                    )?
                } else {
                    value_bytes.len()
                };

                // Extract element bytes
                let element_start = offsets.elements_start + element_offset;
                let element_end = offsets.elements_start + next_offset;

                if element_end > value_bytes.len() {
                    return Err(ArrowError::InvalidArgumentError(
                        "Element data exceeds value buffer".to_string(),
                    ));
                }

                Ok(Some(value_bytes[element_start..element_end].to_vec()))
            }
            _ => Ok(None), // Not an array, can't extract elements
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet_variant::VariantBuilder;

    fn create_test_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("name", "Alice")
            .with_field("age", 30i32)
            .with_field("city", "NYC")
            .finish()
            .unwrap();
        builder.finish()
    }

    #[test]
    fn test_extract_field_bytes() {
        let (metadata, value) = create_test_object();

        let name_bytes = FieldOperations::extract_field_bytes(&metadata, &value, "name").unwrap();
        assert!(name_bytes.is_some());

        let nonexistent_bytes =
            FieldOperations::extract_field_bytes(&metadata, &value, "nonexistent").unwrap();
        assert!(nonexistent_bytes.is_none());
    }

    #[test]
    fn test_remove_field_bytes() {
        let (metadata, value) = create_test_object();

        let result = FieldOperations::remove_field_bytes(&metadata, &value, "city").unwrap();
        assert!(result.is_some());

        // Verify the field was removed by checking we can't extract it
        let new_value = result.unwrap();
        let city_bytes =
            FieldOperations::extract_field_bytes(&metadata, &new_value, "city").unwrap();
        assert!(city_bytes.is_none());

        // Verify other fields are still there
        let name_bytes =
            FieldOperations::extract_field_bytes(&metadata, &new_value, "name").unwrap();
        assert!(name_bytes.is_some());
    }
}
