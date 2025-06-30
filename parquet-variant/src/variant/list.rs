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
    first_byte_from_slice, overflow_error, slice_from_slice, slice_from_slice_at_offset,
    validate_fallible_iterator,
};
use crate::variant::{Variant, VariantMetadata};

use arrow_schema::ArrowError;

// The value header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: usize = 1;

/// A parsed version of the variant array value header byte.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantListHeader {
    num_elements_size: OffsetSizeBytes,
    offset_size: OffsetSizeBytes,
}

impl VariantListHeader {
    // Hide the ugly casting
    const fn num_elements_size(&self) -> usize {
        self.num_elements_size as _
    }
    const fn offset_size(&self) -> usize {
        self.offset_size as _
    }

    // Avoid materializing this offset, since it's cheaply and safely computable
    const fn first_offset_byte(&self) -> usize {
        NUM_HEADER_BYTES + self.num_elements_size()
    }

    pub(crate) fn try_new(header_byte: u8) -> Result<Self, ArrowError> {
        // The 6 first bits to the left are the value_header and the 2 bits
        // to the right are the basic type, so we shift to get only the value_header
        let value_header = header_byte >> 2;
        let is_large = (value_header & 0x04) != 0; // 3rd bit from the right
        let field_offset_size_minus_one = value_header & 0x03; // Last two bits

        // The size of the num_elements entry in the array value_data is 4 bytes if
        // is_large is true, otherwise 1 byte.
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };
        let offset_size = OffsetSizeBytes::try_new(field_offset_size_minus_one)?;

        Ok(Self {
            num_elements_size,
            offset_size,
        })
    }
}

/// [`Variant`] Array.
///
/// See the [Variant spec] for details.
///
/// NOTE: The "list" naming differs from the variant spec -- which calls it "array" -- in order to be
/// consistent with Parquet and Arrow type naming. Otherwise, the name would conflict with the
/// `VariantArray : Array` we must eventually define for variant-typed arrow arrays.
///
/// # Validation
///
/// Every instance of variant list is either _valid_ or _invalid_. depending on whether the
/// underlying bytes are a valid encoding of a variant array (see below).
///
/// Instances produced by [`Self::try_new`] or [`Self::validate`] are fully _validated_. They always
/// contain _valid_ data, and infallible accesses such as iteration and indexing are panic-free. The
/// validation cost is linear in the number of underlying bytes.
///
/// Instances produced by [`Self::new`] are _unvalidated_ and so they may contain either _valid_ or
/// _invalid_ data. Infallible accesses such as iteration and indexing will panic if the underlying
/// bytes are _invalid_, and fallible alternatives such as [`Self::iter_try`] and [`Self::get`] are
/// provided as panic-free alternatives. [`Self::validate`] can also be used to _validate_ an
/// _unvalidated_ instance, if desired.
///
/// _Unvalidated_ instances can be constructed in constant time. This can be useful if the caller
/// knows the underlying bytes were already validated previously, or if the caller intends to
/// perform a small number of (fallible) accesses to a large list.
///
/// A _validated_ variant list instance guarantees that:
///
/// - header byte is valid
/// - num_elements is in bounds
/// - offset array content is in-bounds
/// - first offset is zero
/// - last offset is in-bounds
/// - all other offsets are in-bounds (*)
/// - all offsets are monotonically increasing (*)
/// - all values are (recursively) valid variant objects (*)
/// - the associated variant metadata is [valid] (*)
///
/// NOTE: [`Self::new`] only skips expensive (non-constant cost) validation checks (marked by `(*)`
/// in the list above); it panics any of the other checks fails.
///
/// # Safety
///
/// Even an _invalid_ variant list instance is still _safe_ to use in the Rust sense. Accessing
/// it with infallible methods may cause panics but will never lead to undefined behavior.
///
/// [valid]: VariantMetadata#Validation
/// [Variant spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#value-data-for-array-basic_type3
#[derive(Clone, Debug, PartialEq)]
pub struct VariantList<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantListHeader,
    num_elements: usize,
    first_value_byte: usize,
    validated: bool,
}

impl<'m, 'v> VariantList<'m, 'v> {
    /// Attempts to interpret `value` as a variant array value.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` points to a valid variant array value. In particular,
    /// that all offsets are in-bounds and point to valid (recursively validated) objects.
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        Self::try_new_impl(metadata, value)?.validate()
    }

    pub fn new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Self {
        Self::try_new_impl(metadata, value).expect("Invalid variant list value")
    }

    /// Attempts to interpet `metadata` and `value` as a variant array, performing only basic
    /// (constant-cost) [validation].
    ///
    /// [validation]: Self#Validation
    pub(crate) fn try_new_impl(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(value)?;
        let header = VariantListHeader::try_new(header_byte)?;

        // Skip the header byte to read the num_elements; the offset array immediately follows
        let num_elements =
            header
                .num_elements_size
                .unpack_usize_at_offset(value, NUM_HEADER_BYTES, 0)?;

        // (num_elements + 1) * offset_size + first_offset_byte
        let first_value_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.offset_size()))
            .and_then(|n| n.checked_add(header.first_offset_byte()))
            .ok_or_else(|| overflow_error("offset of variant list values"))?;

        let mut new_self = Self {
            metadata,
            value,
            header,
            num_elements,
            first_value_byte,
            validated: false,
        };

        // Validate just the first and last offset, ignoring the other offsets and all value bytes.
        let first_offset = new_self.get_offset(0)?;
        if first_offset != 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "First offset is not zero: {first_offset}"
            )));
        }

        // Use the last offset to upper-bound the value buffer
        let last_offset = new_self
            .get_offset(num_elements)?
            .checked_add(first_value_byte)
            .ok_or_else(|| overflow_error("variant array size"))?;
        new_self.value = slice_from_slice(value, ..last_offset)?;
        Ok(new_self)
    }

    /// True if this instance is fully [validated] for panic-free infallible accesses.
    ///
    /// [validated]: Self#Validation
    pub fn is_validated(&self) -> bool {
        self.validated
    }

    /// Performs a full [validation] of this variant array and returns the result.
    ///
    /// [validation]: Self#Validation
    pub fn validate(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Validate the metadata dictionary, if not already validated.
            self.metadata = self.metadata.validate()?;

            // Iterate over all string keys in this dictionary in order to prove that the offset
            // array is valid, all offsets are in bounds, and all string bytes are valid utf-8.
            validate_fallible_iterator(self.iter_try())?;
            self.validated = true;
        }
        Ok(self)
    }

    /// Return the length of this array
    pub fn len(&self) -> usize {
        self.num_elements
    }

    /// Is the array of zero length
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns element by index in `0..self.len()`, if any. May panic if this list is [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get(&self, index: usize) -> Option<Variant<'m, 'v>> {
        (index < self.num_elements).then(|| {
            self.try_get_impl(index)
                .and_then(Variant::validate)
                .expect("Invalid variant array element")
        })
    }

    /// Fallible version of `get`. Returns element by index, capturing validation errors
    pub fn try_get(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        self.try_get_impl(index)?.validate()
    }

    /// Fallible iteration over the elements of this list.
    pub fn iter_try(&self) -> impl Iterator<Item = Result<Variant<'m, 'v>, ArrowError>> + '_ {
        (0..self.len()).map(move |i| self.try_get_impl(i))
    }

    /// Iterates over the values of this list. When working with [unvalidated] input, consider
    /// [`Self::iter_try`] to avoid panics due to invalid data.
    ///
    /// [unvalidated]: Self#Validation
    pub fn iter(&self) -> impl Iterator<Item = Variant<'m, 'v>> + '_ {
        self.iter_try()
            .map(|result| result.expect("Invalid variant list entry"))
    }

    // Attempts to retrieve the ith offset from the offset array region of the byte buffer.
    fn get_offset(&self, index: usize) -> Result<usize, ArrowError> {
        let byte_range = self.header.first_offset_byte()..self.first_value_byte;
        let offset_bytes = slice_from_slice(self.value, byte_range)?;
        self.header.offset_size.unpack_usize(offset_bytes, index)
    }

    // Fallible version of `get`, performing only basic (constant-time) validation.
    fn try_get_impl(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        // Fetch the value bytes between the two offsets for this index, from the value array region
        // of the byte buffer
        let byte_range = self.get_offset(index)?..self.get_offset(index + 1)?;
        let value_bytes =
            slice_from_slice_at_offset(self.value, self.first_value_byte, byte_range)?;
        Variant::try_new_with_metadata(self.metadata, value_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_list_simple() {
        // Create simple metadata (empty dictionary for this test)
        let metadata_bytes = vec![
            0x01, // header: version=1, sorted=0, offset_size_minus_one=0
            0,    // dictionary_size = 0
            0,    // offset[0] = 0 (end of dictionary)
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create list value data for: [42, true, "hi"]
        // Header: basic_type=3 (array), field_offset_size_minus_one=0, is_large=0
        // value_header = 0000_0_0_00 = 0x00
        // So header byte = (0x00 << 2) | 3 = 0x03
        let list_value = vec![
            0x03, // header: basic_type=3, value_header=0x00
            3,    // num_elements = 3
            // Offsets (1 byte each): 4 offsets total
            0, // offset to first value (int8)
            2, // offset to second value (boolean true)
            3, // offset to third value (short string)
            6, // end offset
            // Values:
            0x0C,
            42,   // int8: primitive_header=3, basic_type=0 -> (3 << 2) | 0 = 0x0C, then value 42
            0x04, // boolean true: primitive_header=1, basic_type=0 -> (1 << 2) | 0 = 0x04
            0x09, b'h', b'i', // short string: length=2, basic_type=1 -> (2 << 2) | 1 = 0x09
        ];

        let variant_list = VariantList::try_new(metadata, &list_value).unwrap();

        // Test basic properties
        assert_eq!(variant_list.len(), 3);
        assert!(!variant_list.is_empty());

        // Test individual element access
        let elem0 = variant_list.get(0).unwrap();
        assert_eq!(elem0.as_int8(), Some(42));

        let elem1 = variant_list.get(1).unwrap();
        assert_eq!(elem1.as_boolean(), Some(true));

        let elem2 = variant_list.get(2).unwrap();
        assert_eq!(elem2.as_string(), Some("hi"));

        // Test out of bounds access
        let out_of_bounds = variant_list.get(3);
        assert!(out_of_bounds.is_none());

        // Test values iterator
        let values: Vec<_> = variant_list.iter().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_int8(), Some(42));
        assert_eq!(values[1].as_boolean(), Some(true));
        assert_eq!(values[2].as_string(), Some("hi"));
    }

    #[test]
    fn test_variant_list_empty() {
        // Create simple metadata (empty dictionary)
        let metadata_bytes = vec![
            0x01, // header: version=1, sorted=0, offset_size_minus_one=0
            0,    // dictionary_size = 0
            0,    // offset[0] = 0 (end of dictionary)
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create empty list value data: []
        let list_value = vec![
            0x03, // header: basic_type=3, value_header=0x00
            0,    // num_elements = 0
            0,    // single offset pointing to end
                  // No values
        ];

        let variant_list = VariantList::try_new(metadata, &list_value).unwrap();

        // Test basic properties
        assert_eq!(variant_list.len(), 0);
        assert!(variant_list.is_empty());

        // Test out of bounds access on empty list
        let out_of_bounds = variant_list.get(0);
        assert!(out_of_bounds.is_none());

        // Test values iterator on empty list
        let values: Vec<_> = variant_list.iter().collect();
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_variant_list_large() {
        // Create simple metadata (empty dictionary)
        let metadata_bytes = vec![
            0x01, // header: version=1, sorted=0, offset_size_minus_one=0
            0,    // dictionary_size = 0
            0,    // offset[0] = 0 (end of dictionary)
        ];
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create large list value data with 2-byte offsets: [null, false]
        // Header: is_large=1, field_offset_size_minus_one=1, basic_type=3 (array)
        let list_bytes = vec![
            0x17, // header = 000_1_01_11 = 0x17
            2, 0, 0, 0, // num_elements = 2 (4 bytes because is_large=1)
            // Offsets (2 bytes each): 3 offsets total
            0x00, 0x00, 0x01, 0x00, // first value (null)
            0x02, 0x00, // second value (boolean false)
            // Values:
            0x00, // null: primitive_header=0, basic_type=0 -> (0 << 2) | 0 = 0x00
            0x08, // boolean false: primitive_header=2, basic_type=0 -> (2 << 2) | 0 = 0x08
        ];

        let variant_list = VariantList::try_new(metadata, &list_bytes).unwrap();

        // Test basic properties
        assert_eq!(variant_list.len(), 2);
        assert!(!variant_list.is_empty());

        // Test individual element access
        let elem0 = variant_list.get(0).unwrap();
        assert_eq!(elem0.as_null(), Some(()));

        let elem1 = variant_list.get(1).unwrap();
        assert_eq!(elem1.as_boolean(), Some(false));
    }
}
