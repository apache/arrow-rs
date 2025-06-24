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
use crate::decoder::OffsetSizeBytes;
use crate::utils::{
    first_byte_from_slice, overflow_error, slice_from_slice, try_binary_search_range_by,
    validate_fallible_iterator,
};
use crate::variant::{Variant, VariantMetadata};

use arrow_schema::ArrowError;

// The value header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: usize = 1;

/// Header structure for [`VariantObject`]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantObjectHeader {
    field_offset_size: OffsetSizeBytes,
    field_id_size: OffsetSizeBytes,
    is_large: bool,
}

impl VariantObjectHeader {
    pub(crate) fn try_new(header_byte: u8) -> Result<Self, ArrowError> {
        // Parse the header byte to get object parameters
        let value_header = header_byte >> 2;
        let field_offset_size_minus_one = value_header & 0x03; // Last 2 bits
        let field_id_size_minus_one = (value_header >> 2) & 0x03; // Next 2 bits
        let is_large = (value_header & 0x10) != 0; // 5th bit

        Ok(Self {
            field_offset_size: OffsetSizeBytes::try_new(field_offset_size_minus_one)?,
            field_id_size: OffsetSizeBytes::try_new(field_id_size_minus_one)?,
            is_large,
        })
    }
}

/// A [`Variant`] Object (struct with named fields).
#[derive(Clone, Debug, PartialEq)]
pub struct VariantObject<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantObjectHeader,
    num_elements: usize,
    field_ids_start_byte: usize,
    field_offsets_start_byte: usize,
    values_start_byte: usize,
}

impl<'m, 'v> VariantObject<'m, 'v> {
    /// Attempts to interpret `value` as a variant object value.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` points to a valid variant object value. In
    /// particular, that all field ids exist in `metadata`, and all offsets are in-bounds and point
    /// to valid objects.
    // TODO: How to make the validation non-recursive while still making iterators safely infallible??
    // See https://github.com/apache/arrow-rs/issues/7711
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(value)?;
        let header = VariantObjectHeader::try_new(header_byte)?;

        // Determine num_elements size based on is_large flag and fetch the value
        let num_elements_size = if header.is_large {
            OffsetSizeBytes::Four
        } else {
            OffsetSizeBytes::One
        };
        let num_elements = num_elements_size.unpack_usize(value, NUM_HEADER_BYTES, 0)?;

        // Calculate byte offsets for different sections with overflow protection
        let field_ids_start_byte = NUM_HEADER_BYTES
            .checked_add(num_elements_size as usize)
            .ok_or_else(|| overflow_error("offset of variant object field ids"))?;

        let field_offsets_start_byte = num_elements
            .checked_mul(header.field_id_size as usize)
            .and_then(|n| n.checked_add(field_ids_start_byte))
            .ok_or_else(|| overflow_error("offset of variant object field offsets"))?;

        let values_start_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.field_offset_size as usize))
            .and_then(|n| n.checked_add(field_offsets_start_byte))
            .ok_or_else(|| overflow_error("offset of variant object field values"))?;

        // Spec says: "The last field_offset points to the byte after the end of the last value"
        //
        // Use the last offset as a bounds check. The iterator check below doesn't use it -- offsets
        // are not monotonic -- so we have to check separately here.
        let end_offset = header
            .field_offset_size
            .unpack_usize(value, field_offsets_start_byte, num_elements)?
            .checked_add(values_start_byte)
            .ok_or_else(|| overflow_error("end of variant object field values"))?;
        if end_offset > value.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last field offset value {} is outside the value slice of length {}",
                end_offset,
                value.len()
            )));
        }

        let new_self = Self {
            metadata,
            value,
            header,
            num_elements,
            field_ids_start_byte,
            field_offsets_start_byte,
            values_start_byte,
        };

        // Iterate over all fields of this object in order to validate the field_id and field_offset
        // arrays, and also to prove the field values are all in bounds. Otherwise, `iter` might
        // panic on `unwrap`.
        validate_fallible_iterator(new_self.iter_checked())?;
        Ok(new_self)
    }

    /// Returns the number of key-value pairs in this object
    pub fn len(&self) -> usize {
        self.num_elements
    }

    /// Returns true if the object contains no key-value pairs
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a field's value by index in `0..self.len()`
    pub fn field(&self, i: usize) -> Option<Variant<'m, 'v>> {
        if i >= self.num_elements {
            return None;
        }

        match self.try_field(i) {
            Ok(field) => Some(field),
            Err(err) => panic!("validation error: {}", err),
        }
    }

    /// Fallible version of `field`. Returns field value by index, capturing validation errors
    fn try_field(&self, i: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        let start_offset = self.header.field_offset_size.unpack_usize(
            self.value,
            self.field_offsets_start_byte,
            i,
        )?;
        let value_start = self
            .values_start_byte
            .checked_add(start_offset)
            .ok_or_else(|| overflow_error("offset of variant object field"))?;
        let value_bytes = slice_from_slice(self.value, value_start..)?;
        Variant::try_new_with_metadata(self.metadata, value_bytes)
    }

    /// Get a field's name by index in `0..self.len()`
    pub fn field_name(&self, i: usize) -> Option<&'m str> {
        if i >= self.num_elements {
            return None;
        }

        match self.try_field_name(i) {
            Ok(field_name) => Some(field_name),
            Err(err) => panic!("validation error: {}", err),
        }
    }

    /// Fallible version of `field_name`. Returns field name by index, capturing validation errors
    fn try_field_name(&self, i: usize) -> Result<&'m str, ArrowError> {
        let field_id =
            self.header
                .field_id_size
                .unpack_usize(self.value, self.field_ids_start_byte, i)?;
        self.metadata.get(field_id)
    }

    /// Returns an iterator of (name, value) pairs over the fields of this object.
    pub fn iter(&self) -> impl Iterator<Item = (&'m str, Variant<'m, 'v>)> + '_ {
        // NOTE: It is safe to unwrap because the constructor already made a successful traversal.
        self.iter_checked().map(Result::unwrap)
    }

    // Fallible iteration over the fields of this object. The constructor traverses the iterator to
    // prove it has no errors, so that all other use sites can blindly `unwrap` the result.
    fn iter_checked(
        &self,
    ) -> impl Iterator<Item = Result<(&'m str, Variant<'m, 'v>), ArrowError>> + '_ {
        (0..self.num_elements).map(move |i| Ok((self.try_field_name(i)?, self.try_field(i)?)))
    }

    /// Returns the value of the field with the specified name, if any.
    ///
    /// `Ok(None)` means the field does not exist; `Err` means the search encountered an error.
    pub fn get(&self, name: &str) -> Option<Variant<'m, 'v>> {
        // Binary search through the field IDs of this object to find the requested field name.
        //
        // NOTE: This does not require a sorted metadata dictionary, because the variant spec
        // requires object field ids to be lexically sorted by their corresponding string values,
        // and probing the dictionary for a field id is always O(1) work.
        let i = try_binary_search_range_by(0..self.num_elements, &name, |i| self.field_name(i))?
            .ok()?;

        self.field(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_object_simple() {
        // Create metadata with field names: "age", "name", "active" (sorted)
        // Header: version=1, sorted=1, offset_size=1 (offset_size_minus_one=0)
        // So header byte = 00_0_1_0001 = 0x10
        let metadata_bytes = vec![
            0b0001_0001,
            3, // dictionary size
            0, // "active"
            6, // "age"
            9, // "name"
            13,
            b'a',
            b'c',
            b't',
            b'i',
            b'v',
            b'e',
            b'a',
            b'g',
            b'e',
            b'n',
            b'a',
            b'm',
            b'e',
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create object value data for: {"active": true, "age": 42, "name": "hello"}
        // Field IDs in sorted order: [0, 1, 2] (active, age, name)
        // Header: basic_type=2, field_offset_size_minus_one=0, field_id_size_minus_one=0, is_large=0
        // value_header = 0000_00_00 = 0x00
        // So header byte = (0x00 << 2) | 2 = 0x02
        let object_value = vec![
            0x02, // header: basic_type=2, value_header=0x00
            3,    // num_elements = 3
            // Field IDs (1 byte each): active=0, age=1, name=2
            0, 1, 2,
            // Field offsets (1 byte each): 4 offsets total
            0, // offset to first value (boolean true)
            1, // offset to second value (int8)
            3, // offset to third value (short string)
            9, // end offset
            // Values:
            0x04, // boolean true: primitive_header=1, basic_type=0 -> (1 << 2) | 0 = 0x04
            0x0C,
            42, // int8: primitive_header=3, basic_type=0 -> (3 << 2) | 0 = 0x0C, then value 42
            0x15, b'h', b'e', b'l', b'l',
            b'o', // short string: length=5, basic_type=1 -> (5 << 2) | 1 = 0x15
        ];

        let variant_obj = VariantObject::try_new(metadata, &object_value).unwrap();

        // Test basic properties
        assert_eq!(variant_obj.len(), 3);
        assert!(!variant_obj.is_empty());

        // Test field access
        let active_field = variant_obj.get("active");
        assert!(active_field.is_some());
        assert_eq!(active_field.unwrap().as_boolean(), Some(true));

        let age_field = variant_obj.get("age");
        assert!(age_field.is_some());
        assert_eq!(age_field.unwrap().as_int8(), Some(42));

        let name_field = variant_obj.get("name");
        assert!(name_field.is_some());
        assert_eq!(name_field.unwrap().as_string(), Some("hello"));

        // Test non-existent field
        let missing_field = variant_obj.get("missing");
        assert!(missing_field.is_none());

        // Fixme: This assertion will panic! That is not good
        // let missing_field_name = variant_obj.field_name(3);
        // assert!(missing_field_name.is_none());

        let missing_field_name = variant_obj.field_name(300);
        assert!(missing_field_name.is_none());

        let missing_field_value = variant_obj.field(3);
        assert!(missing_field_value.is_none());

        let missing_field_value = variant_obj.field(300);
        assert!(missing_field_value.is_none());

        // Test fields iterator
        let fields: Vec<_> = variant_obj.iter().collect();
        assert_eq!(fields.len(), 3);

        // Fields should be in sorted order: active, age, name
        assert_eq!(fields[0].0, "active");
        assert_eq!(fields[0].1.as_boolean(), Some(true));

        assert_eq!(fields[1].0, "age");
        assert_eq!(fields[1].1.as_int8(), Some(42));

        assert_eq!(fields[2].0, "name");
        assert_eq!(fields[2].1.as_string(), Some("hello"));

        // Test field access by index
        // Fields should be in sorted order: active, age, name
        assert_eq!(variant_obj.field_name(0), Some("active"));
        assert_eq!(variant_obj.field(0).unwrap().as_boolean(), Some(true));

        assert_eq!(variant_obj.field_name(1), Some("age"));
        assert_eq!(variant_obj.field(1).unwrap().as_int8(), Some(42));

        assert_eq!(variant_obj.field_name(2), Some("name"));
        assert_eq!(variant_obj.field(2).unwrap().as_string(), Some("hello"));
    }

    #[test]
    fn test_variant_object_empty() {
        // Create metadata with no fields
        let metadata_bytes = vec![
            0x11, // header: version=1, sorted=0, offset_size_minus_one=0
            0,    // dictionary_size = 0
            0,    // offset[0] = 0 (end of dictionary)
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create empty object value data: {}
        let object_value = vec![
            0x02, // header: basic_type=2, value_header=0x00
            0,    // num_elements = 0
            0,    // single offset pointing to end
                  // No field IDs, no values
        ];

        let variant_obj = VariantObject::try_new(metadata, &object_value).unwrap();

        // Test basic properties
        assert_eq!(variant_obj.len(), 0);
        assert!(variant_obj.is_empty());

        // Test field access on empty object
        let missing_field = variant_obj.get("anything");
        assert!(missing_field.is_none());

        // Test fields iterator on empty object
        let fields: Vec<_> = variant_obj.iter().collect();
        assert_eq!(fields.len(), 0);
    }
}
