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
use crate::decoder::{map_bytes_to_offsets, OffsetSizeBytes};
use crate::utils::{
    first_byte_from_slice, overflow_error, slice_from_slice, try_binary_search_range_by,
};
use crate::variant::{Variant, VariantMetadata};

use arrow_schema::ArrowError;

// The value header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: u32 = 1;

/// Header structure for [`VariantObject`]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VariantObjectHeader {
    num_elements_size: OffsetSizeBytes,
    field_id_size: OffsetSizeBytes,
    field_offset_size: OffsetSizeBytes,
}

impl VariantObjectHeader {
    // Hide the ugly casting
    const fn num_elements_size(&self) -> u32 {
        self.num_elements_size as _
    }
    const fn field_id_size(&self) -> u32 {
        self.field_id_size as _
    }
    const fn field_offset_size(&self) -> u32 {
        self.field_offset_size as _
    }

    // Avoid materializing this offset, since it's cheaply and safely computable
    const fn field_ids_start_byte(&self) -> u32 {
        NUM_HEADER_BYTES + self.num_elements_size()
    }

    pub(crate) fn try_new(header_byte: u8) -> Result<Self, ArrowError> {
        // Parse the header byte to get object parameters
        let value_header = header_byte >> 2;
        let field_offset_size_minus_one = value_header & 0x03; // Last 2 bits
        let field_id_size_minus_one = (value_header >> 2) & 0x03; // Next 2 bits
        let is_large = (value_header & 0x10) != 0; // 5th bit
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };
        Ok(Self {
            num_elements_size,
            field_id_size: OffsetSizeBytes::try_new(field_id_size_minus_one)?,
            field_offset_size: OffsetSizeBytes::try_new(field_offset_size_minus_one)?,
        })
    }
}

/// A [`Variant`] Object (struct with named fields).
///
/// See the [Variant spec] file for more information.
///
/// # Validation
///
/// Every instance of variant object is either _valid_ or _invalid_. depending on whether the
/// underlying bytes are a valid encoding of a variant object subtype (see below).
///
/// Instances produced by [`Self::try_new`] or [`Self::with_full_validation`] are fully (and recursively)
/// _validated_. They always contain _valid_ data, and infallible accesses such as iteration and
/// indexing are panic-free. The validation cost is linear in the number of underlying bytes.
///
/// Instances produced by [`Self::new`] are _unvalidated_ and so they may contain either _valid_ or
/// _invalid_ data. Infallible accesses such as iteration and indexing will panic if the underlying
/// bytes are _invalid_, and fallible alternatives such as [`Self::iter_try`] and [`Self::get`] are
/// provided as panic-free alternatives. [`Self::with_full_validation`] can also be used to _validate_ an
/// _unvalidated_ instance, if desired.
///
/// _Unvalidated_ instances can be constructed in constant time. They can be useful if the caller
/// knows the underlying bytes were already validated previously, or if the caller intends to
/// perform a small number of (fallible) field accesses against a large object.
///
/// A _validated_ instance guarantees that:
///
/// - header byte is valid
/// - num_elements is in bounds
/// - field id array is in bounds
/// - field offset array is in bounds
/// - field value array is in bounds
/// - all field ids are valid metadata dictionary entries (*)
/// - field ids are lexically ordered according by their corresponding string values (*)
/// - all field offsets are in bounds (*)
/// - all field values are (recursively) _valid_ variant values (*)
/// - the associated variant metadata is [valid] (*)
///
/// NOTE: [`Self::new`] only skips expensive (non-constant cost) validation checks (marked by `(*)`
/// in the list above); it panics any of the other checks fails.
///
/// # Safety
///
/// Even an _invalid_ variant object instance is still _safe_ to use in the Rust sense. Accessing it
/// with infallible methods may cause panics but will never lead to undefined behavior.
///
/// [valid]: VariantMetadata#Validation
/// [Variant spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#value-data-for-object-basic_type2
#[derive(Debug, Clone, PartialEq)]
pub struct VariantObject<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantObjectHeader,
    num_elements: u32,
    first_field_offset_byte: u32,
    first_value_byte: u32,
    validated: bool,
}

// We don't want this to grow because it could increase the size of `Variant` and hurt performance.
const _: () = crate::utils::expect_size_of::<VariantObject>(64);

impl<'m, 'v> VariantObject<'m, 'v> {
    pub fn new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Self {
        Self::try_new_with_shallow_validation(metadata, value).expect("Invalid variant object")
    }

    /// Attempts to interpet `metadata` and `value` as a variant object.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` points to a valid variant object value. In
    /// particular, that all field ids exist in `metadata`, and all offsets are in-bounds and point
    /// to valid objects.
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        Self::try_new_with_shallow_validation(metadata, value)?.with_full_validation()
    }

    /// Attempts to interpet `metadata` and `value` as a variant object, performing only basic
    /// (constant-cost) [validation].
    ///
    /// [validation]: Self#Validation
    pub(crate) fn try_new_with_shallow_validation(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(value)?;
        let header = VariantObjectHeader::try_new(header_byte)?;

        // Determine num_elements size based on is_large flag and fetch the value
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, NUM_HEADER_BYTES as _, 0)?;

        // Calculate byte offsets for field offsets and values with overflow protection, and verify
        // they're in bounds
        let first_field_offset_byte = num_elements
            .checked_mul(header.field_id_size())
            .and_then(|n| n.checked_add(header.field_ids_start_byte()))
            .ok_or_else(|| overflow_error("offset of variant object field offsets"))?;

        let first_value_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.field_offset_size()))
            .and_then(|n| n.checked_add(first_field_offset_byte))
            .ok_or_else(|| overflow_error("offset of variant object field values"))?;

        let mut new_self = Self {
            metadata,
            value,
            header,
            num_elements,
            first_field_offset_byte,
            first_value_byte,
            validated: false,
        };

        // Spec says: "The last field_offset points to the byte after the end of the last value"
        //
        // Use it to upper-bound the value bytes, which also verifies that the field id and field
        // offset arrays are in bounds.
        let last_offset = new_self
            .get_offset(num_elements as _)?
            .checked_add(first_value_byte)
            .ok_or_else(|| overflow_error("variant object size"))?;
        new_self.value = slice_from_slice(value, ..last_offset as _)?;
        Ok(new_self)
    }

    /// True if this instance is fully [validated] for panic-free infallible accesses.
    ///
    /// [validated]: Self#Validation
    pub fn is_fully_validated(&self) -> bool {
        self.validated
    }

    /// Performs a full [validation] of this variant object.
    ///
    /// [validation]: Self#Validation
    pub fn with_full_validation(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Validate the metadata dictionary first, if not already validated, because we pass it
            // by value to all the children (who would otherwise re-validate it repeatedly).
            self.metadata = self.metadata.with_full_validation()?;

            let field_id_buffer = slice_from_slice(
                self.value,
                self.header.field_ids_start_byte() as _..self.first_field_offset_byte as _,
            )?;

            let field_ids = map_bytes_to_offsets(field_id_buffer, self.header.field_id_size)
                .collect::<Vec<_>>();

            // Validate all field ids exist in the metadata dictionary and the corresponding field names are lexicographically sorted
            if self.metadata.is_sorted() {
                // Since the metadata dictionary has unique and sorted field names, we can also guarantee this object's field names
                // are lexicographically sorted by their field id ordering
                if !field_ids.is_sorted() {
                    return Err(ArrowError::InvalidArgumentError(
                        "field names not sorted".to_string(),
                    ));
                }

                // Since field ids are sorted, if the last field is smaller than the dictionary size,
                // we also know all field ids are smaller than the dictionary size and in-bounds.
                if let Some(&last_field_id) = field_ids.last() {
                    if last_field_id >= self.metadata.dictionary_size() {
                        return Err(ArrowError::InvalidArgumentError(
                            "field id is not valid".to_string(),
                        ));
                    }
                }
            } else {
                // The metadata dictionary can't guarantee uniqueness or sortedness, so we have to parse out the corresponding field names
                // to check lexicographical order
                let are_field_names_sorted = field_ids
                    .iter()
                    .map(|&i| self.metadata.get(i))
                    .collect::<Result<Vec<_>, _>>()?
                    .is_sorted();

                if !are_field_names_sorted {
                    return Err(ArrowError::InvalidArgumentError(
                        "field names not sorted".to_string(),
                    ));
                }

                // Since field ids are not guaranteed to be sorted, scan over all field ids
                // and check that field ids are less than dictionary size

                let are_field_ids_in_bounds = field_ids
                    .iter()
                    .all(|&id| id < self.metadata.dictionary_size());

                if !are_field_ids_in_bounds {
                    return Err(ArrowError::InvalidArgumentError(
                        "field id is not valid".to_string(),
                    ));
                }
            }

            // Validate whether values are valid variant objects
            let field_offset_buffer = slice_from_slice(
                self.value,
                self.first_field_offset_byte as _..self.first_value_byte as _,
            )?;
            let num_offsets = field_offset_buffer.len() / self.header.field_offset_size() as usize;

            let value_buffer = slice_from_slice(self.value, self.first_value_byte as _..)?;

            map_bytes_to_offsets(field_offset_buffer, self.header.field_offset_size)
                .take(num_offsets.saturating_sub(1))
                .try_for_each(|offset| {
                    let value_bytes = slice_from_slice(value_buffer, offset..)?;
                    Variant::try_new_with_metadata(self.metadata.clone(), value_bytes)?;

                    Ok::<_, ArrowError>(())
                })?;

            self.validated = true;
        }
        Ok(self)
    }

    /// Returns the number of key-value pairs in this object
    pub fn len(&self) -> usize {
        self.num_elements as _
    }

    /// Returns true if the object contains no key-value pairs
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a field's value by index in `0..self.len()`
    ///
    /// # Panics
    ///
    /// If the index is out of bounds. Also if variant object is corrupted (e.g., invalid offsets or
    /// field IDs). The latter can only happen when working with an unvalidated object produced by
    /// [`Self::new`].
    pub fn field(&self, i: usize) -> Option<Variant<'m, 'v>> {
        (i < self.len()).then(|| {
            self.try_field_with_shallow_validation(i)
                .expect("Invalid object field value")
        })
    }

    /// Fallible version of `field`. Returns field value by index, capturing validation errors
    pub fn try_field(&self, i: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        self.try_field_with_shallow_validation(i)?
            .with_full_validation()
    }

    // Attempts to retrieve the ith field value from the value region of the byte buffer; it
    // performs only basic (constant-cost) validation.
    fn try_field_with_shallow_validation(&self, i: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        let value_bytes = slice_from_slice(self.value, self.first_value_byte as _..)?;
        let value_bytes = slice_from_slice(value_bytes, self.get_offset(i)? as _..)?;
        Variant::try_new_with_metadata_and_shallow_validation(self.metadata.clone(), value_bytes)
    }

    // Attempts to retrieve the ith offset from the field offset region of the byte buffer.
    fn get_offset(&self, i: usize) -> Result<u32, ArrowError> {
        let byte_range = self.first_field_offset_byte as _..self.first_value_byte as _;
        let field_offsets = slice_from_slice(self.value, byte_range)?;
        self.header.field_offset_size.unpack_u32(field_offsets, i)
    }

    /// Get a field's name by index in `0..self.len()`
    ///
    /// # Panics
    /// If the variant object is corrupted (e.g., invalid offsets or field IDs).
    /// This should never happen since the constructor validates all data upfront.
    pub fn field_name(&self, i: usize) -> Option<&'m str> {
        (i < self.len()).then(|| {
            self.try_field_name(i)
                .expect("Invalid variant object field name")
        })
    }

    /// Fallible version of `field_name`. Returns field name by index, capturing validation errors
    fn try_field_name(&self, i: usize) -> Result<&'m str, ArrowError> {
        let byte_range = self.header.field_ids_start_byte() as _..self.first_field_offset_byte as _;
        let field_id_bytes = slice_from_slice(self.value, byte_range)?;
        let field_id = self.header.field_id_size.unpack_u32(field_id_bytes, i)?;
        self.metadata.get(field_id as _)
    }

    /// Returns an iterator of (name, value) pairs over the fields of this object.
    pub fn iter(&self) -> impl Iterator<Item = (&'m str, Variant<'m, 'v>)> + '_ {
        self.iter_try_with_shallow_validation()
            .map(|result| result.expect("Invalid variant object field value"))
    }

    /// Fallible iteration over the fields of this object.
    pub fn iter_try(
        &self,
    ) -> impl Iterator<Item = Result<(&'m str, Variant<'m, 'v>), ArrowError>> + '_ {
        self.iter_try_with_shallow_validation().map(|result| {
            let (name, value) = result?;
            Ok((name, value.with_full_validation()?))
        })
    }

    // Fallible iteration over the fields of this object that performs only shallow (constant-cost)
    // validation of field values.
    fn iter_try_with_shallow_validation(
        &self,
    ) -> impl Iterator<Item = Result<(&'m str, Variant<'m, 'v>), ArrowError>> + '_ {
        (0..self.len()).map(|i| {
            let field = self.try_field_with_shallow_validation(i)?;
            Ok((self.try_field_name(i)?, field))
        })
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
        let i = try_binary_search_range_by(0..self.len(), &name, |i| self.field_name(i))?.ok()?;

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

        let missing_field_name = variant_obj.field_name(3);
        assert!(missing_field_name.is_none());

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

    #[test]
    fn test_variant_object_invalid_metadata_end_offset() {
        // Create metadata with field names: "age", "name" (sorted)
        let metadata_bytes = vec![
            0b0001_0001, // header: version=1, sorted=1, offset_size_minus_one=0
            2,           // dictionary size
            0,           // "age"
            3,           // "name"
            8,           // Invalid end offset (should be 7)
            b'a',
            b'g',
            b'e',
            b'n',
            b'a',
            b'm',
            b'e',
        ];
        let err = VariantMetadata::try_new(&metadata_bytes);
        let err = err.unwrap_err();
        assert!(matches!(
            err,
            ArrowError::InvalidArgumentError(ref msg) if msg.contains("Tried to extract byte(s) ..13 from 12-byte buffer")
        ));
    }

    #[test]
    fn test_variant_object_invalid_end_offset() {
        // Create metadata with field names: "age", "name" (sorted)
        let metadata_bytes = vec![
            0b0001_0001, // header: version=1, sorted=1, offset_size_minus_one=0
            2,           // dictionary size
            0,           // "age"
            3,           // "name"
            7,
            b'a',
            b'g',
            b'e',
            b'n',
            b'a',
            b'm',
            b'e',
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create object value data for: {"age": 42, "name": "hello"}
        // Field IDs in sorted order: [0, 1] (age, name)
        // Header: basic_type=2, field_offset_size_minus_one=0, field_id_size_minus_one=0, is_large=0
        // value_header = 0000_00_00 = 0x00
        let object_value = vec![
            0x02, // header: basic_type=2, value_header=0x00
            2,    // num_elements = 2
            // Field IDs (1 byte each): age=0, name=1
            0, 1,
            // Field offsets (1 byte each): 3 offsets total
            0, // offset to first value (int8)
            2, // offset to second value (short string)
            9, // invalid end offset (correct would be 8)
            // Values:
            0x0C,
            42, // int8: primitive_header=3, basic_type=0 -> (3 << 2) | 0 = 0x0C, then value 42
            0x15, b'h', b'e', b'l', b'l',
            b'o', // short string: length=5, basic_type=1 -> (5 << 2) | 1 = 0x15
        ];

        let err = VariantObject::try_new(metadata, &object_value);
        let err = err.unwrap_err();
        assert!(matches!(
            err,
            ArrowError::InvalidArgumentError(ref msg) if msg.contains("Tried to extract byte(s) ..16 from 15-byte buffer")
        ));
    }
}
