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
use crate::decoder::{OffsetSizeBytes, map_bytes_to_offsets};
use crate::utils::{
    first_byte_from_slice, overflow_error, slice_from_slice, slice_from_slice_at_offset,
};
use crate::variant::{Variant, VariantMetadata};

use arrow_schema::ArrowError;

// The value header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: u32 = 1;

/// A parsed version of the variant array value header byte.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VariantListHeader {
    num_elements_size: OffsetSizeBytes,
    offset_size: OffsetSizeBytes,
}

impl VariantListHeader {
    // Hide the ugly casting
    const fn num_elements_size(&self) -> u32 {
        self.num_elements_size as _
    }
    const fn offset_size(&self) -> u32 {
        self.offset_size as _
    }

    // Avoid materializing this offset, since it's cheaply and safely computable
    const fn first_offset_byte(&self) -> u32 {
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
/// Instances produced by [`Self::try_new`] or [`Self::with_full_validation`] are fully _validated_. They always
/// contain _valid_ data, and infallible accesses such as iteration and indexing are panic-free. The
/// validation cost is linear in the number of underlying bytes.
///
/// Instances produced by [`Self::new`] are _unvalidated_ and so they may contain either _valid_ or
/// _invalid_ data. Infallible accesses such as iteration and indexing will panic if the underlying
/// bytes are _invalid_, and fallible alternatives such as [`Self::iter_try`] and [`Self::get`] are
/// provided as panic-free alternatives. [`Self::with_full_validation`] can also be used to _validate_ an
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
#[derive(Debug, Clone)]
pub struct VariantList<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantListHeader,
    num_elements: u32,
    first_value_byte: u32,
    validated: bool,
}

// We don't want this to grow because it could increase the size of `Variant` and hurt performance.
const _: () = crate::utils::expect_size_of::<VariantList>(64);

impl<'m, 'v> VariantList<'m, 'v> {
    /// Attempts to interpret `value` as a variant array value.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` points to a valid variant array value. In particular,
    /// that all offsets are in-bounds and point to valid (recursively validated) objects.
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        Self::try_new_with_shallow_validation(metadata, value)?.with_full_validation()
    }

    pub fn new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Self {
        Self::try_new_with_shallow_validation(metadata, value).expect("Invalid variant list value")
    }

    /// Attempts to interpet `metadata` and `value` as a variant array, performing only basic
    /// (constant-cost) [validation].
    ///
    /// [validation]: Self#Validation
    pub(crate) fn try_new_with_shallow_validation(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(value)?;
        let header = VariantListHeader::try_new(header_byte)?;

        // Skip the header byte to read the num_elements; the offset array immediately follows
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, NUM_HEADER_BYTES as _, 0)?;

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
            .get_offset(num_elements as _)?
            .checked_add(first_value_byte)
            .ok_or_else(|| overflow_error("variant array size"))?;
        new_self.value = slice_from_slice(value, ..last_offset as _)?;
        Ok(new_self)
    }

    /// True if this instance is fully [validated] for panic-free infallible accesses.
    ///
    /// [validated]: Self#Validation
    pub fn is_fully_validated(&self) -> bool {
        self.validated
    }

    /// Performs a full [validation] of this variant array and returns the result.
    ///
    /// [validation]: Self#Validation
    pub fn with_full_validation(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Validate the metadata dictionary first, if not already validated, because we pass it
            // by value to all the children (who would otherwise re-validate it repeatedly).
            self.metadata = self.metadata.with_full_validation()?;

            let offset_buffer = slice_from_slice(
                self.value,
                self.header.first_offset_byte() as _..self.first_value_byte as _,
            )?;

            let value_buffer = slice_from_slice(self.value, self.first_value_byte as _..)?;

            // Validate whether values are valid variant objects
            //
            // Since we use offsets to slice into the value buffer, this also verifies all offsets are in-bounds
            // and monotonically increasing
            let mut offset_iter = map_bytes_to_offsets(offset_buffer, self.header.offset_size);
            let mut current_offset = offset_iter.next().unwrap_or(0);

            for next_offset in offset_iter {
                let value_bytes = slice_from_slice(value_buffer, current_offset..next_offset)?;
                Variant::try_new_with_metadata(self.metadata.clone(), value_bytes)?;
                current_offset = next_offset;
            }

            self.validated = true;
        }
        Ok(self)
    }

    /// Return the length of this array
    pub fn len(&self) -> usize {
        self.num_elements as _
    }

    /// Is the array of zero length
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns element by index in `0..self.len()`, if any. May panic if this list is [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get(&self, index: usize) -> Option<Variant<'m, 'v>> {
        (index < self.len()).then(|| {
            self.try_get_with_shallow_validation(index)
                .expect("Invalid variant array element")
        })
    }

    /// Fallible version of `get`. Returns element by index, capturing validation errors
    pub fn try_get(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        self.try_get_with_shallow_validation(index)?
            .with_full_validation()
    }

    // Fallible version of `get`, performing only basic (constant-time) validation.
    fn try_get_with_shallow_validation(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        // Fetch the value bytes between the two offsets for this index, from the value array region
        // of the byte buffer
        let byte_range = self.get_offset(index)? as _..self.get_offset(index + 1)? as _;
        let value_bytes =
            slice_from_slice_at_offset(self.value, self.first_value_byte as _, byte_range)?;
        Variant::try_new_with_metadata_and_shallow_validation(self.metadata.clone(), value_bytes)
    }

    /// Iterates over the values of this list. When working with [unvalidated] input, consider
    /// [`Self::iter_try`] to avoid panics due to invalid data.
    ///
    /// [unvalidated]: Self#Validation
    pub fn iter(&self) -> impl Iterator<Item = Variant<'m, 'v>> + '_ {
        self.iter_try_with_shallow_validation()
            .map(|result| result.expect("Invalid variant list entry"))
    }

    /// Fallible iteration over the elements of this list.
    pub fn iter_try(&self) -> impl Iterator<Item = Result<Variant<'m, 'v>, ArrowError>> + '_ {
        self.iter_try_with_shallow_validation()
            .map(|result| result?.with_full_validation())
    }

    // Fallible iteration that only performs basic (constant-time) validation.
    fn iter_try_with_shallow_validation(
        &self,
    ) -> impl Iterator<Item = Result<Variant<'m, 'v>, ArrowError>> + '_ {
        (0..self.len()).map(|i| self.try_get_with_shallow_validation(i))
    }

    // Attempts to retrieve the ith offset from the offset array region of the byte buffer.
    fn get_offset(&self, index: usize) -> Result<u32, ArrowError> {
        let byte_range = self.header.first_offset_byte() as _..self.first_value_byte as _;
        let offset_bytes = slice_from_slice(self.value, byte_range)?;
        self.header.offset_size.unpack_u32(offset_bytes, index)
    }
}

// Custom implementation of PartialEq for variant arrays
//
// Instead of comparing the raw bytes of 2 variant lists, this implementation recursively
// checks whether their elements are equal.
impl<'m, 'v> PartialEq for VariantList<'m, 'v> {
    fn eq(&self, other: &Self) -> bool {
        if self.num_elements != other.num_elements {
            return false;
        }

        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VariantBuilder;
    use std::iter::repeat_n;
    use std::ops::Range;

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

    #[test]
    fn test_large_variant_list_with_total_child_length_between_2_pow_8_and_2_pow_16() {
        // all the tests below will set the total child size to ~500,
        // which is larger than 2^8 but less than 2^16.
        // total child size = list_size * single_child_item_len

        let mut list_size: usize = 1;
        let mut single_child_item_len: usize = 500;

        // offset size will be OffSizeBytes::Two as the total child length between 2^8 and 2^16
        let expected_offset_size = OffsetSizeBytes::Two;

        test_large_variant_list_with_child_length(
            list_size,             // the elements in the list
            single_child_item_len, // this will control the total child size in the list
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 255;
        single_child_item_len = 2;
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 256;
        single_child_item_len = 2;
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );

        list_size = 300;
        single_child_item_len = 2;
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );
    }

    #[test]
    fn test_large_variant_list_with_total_child_length_between_2_pow_16_and_2_pow_24() {
        // all the tests below will set the total child size to ~70,000,
        // which is larger than 2^16 but less than 2^24.
        // total child size = list_size * single_child_item_len

        let mut list_size: usize = 1;
        let mut single_child_item_len: usize = 70000;

        // offset size will be OffSizeBytes::Two as the total child length between 2^16 and 2^24
        let expected_offset_size = OffsetSizeBytes::Three;

        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 255;
        single_child_item_len = 275;
        // total child size = 255 * 275 = 70,125
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 256;
        single_child_item_len = 274;
        // total child size = 256 * 274 = 70,144
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );

        list_size = 300;
        single_child_item_len = 234;
        // total child size = 300 * 234 = 70,200
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );
    }

    #[test]
    fn test_large_variant_list_with_total_child_length_between_2_pow_24_and_2_pow_32() {
        // all the tests below will set the total child size to ~20,000,000,
        // which is larger than 2^24 but less than 2^32.
        // total child size = list_size * single_child_item_len

        let mut list_size: usize = 1;
        let mut single_child_item_len: usize = 20000000;

        // offset size will be OffSizeBytes::Two as the total child length between 2^24 and 2^32
        let expected_offset_size = OffsetSizeBytes::Four;

        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 255;
        single_child_item_len = 78432;
        // total child size = 255 * 78,432 = 20,000,160
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::One, // will be OffsetSizeBytes::One as the size of the list is less than 256
            expected_offset_size,
        );

        list_size = 256;
        single_child_item_len = 78125;
        // total child size = 256 * 78,125 = 20,000,000
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );

        list_size = 300;
        single_child_item_len = 66667;
        // total child size = 300 * 66,667 = 20,000,100
        test_large_variant_list_with_child_length(
            list_size,
            single_child_item_len,
            OffsetSizeBytes::Four, // will be OffsetSizeBytes::Four as the size of the list is bigger than 255
            expected_offset_size,
        );
    }

    // this function will create a large variant list from VariantBuilder
    // with specified size and each child item with the given length.
    // and verify the content and some meta for the variant list in the final.
    fn test_large_variant_list_with_child_length(
        list_size: usize,
        single_child_item_len: usize,
        expected_num_element_size: OffsetSizeBytes,
        expected_offset_size_bytes: OffsetSizeBytes,
    ) {
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();

        let mut expected_list = vec![];
        for i in 0..list_size {
            let random_string: String =
                repeat_n(char::from((i % 256) as u8), single_child_item_len).collect();

            list_builder.append_value(Variant::String(random_string.as_str()));
            expected_list.push(random_string);
        }

        list_builder.finish();
        // Finish the builder to get the metadata and value
        let (metadata, value) = builder.finish();
        // use the Variant API to verify the result
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let variant_list = variant.as_list().unwrap();

        // verify that the head is expected
        assert_eq!(expected_offset_size_bytes, variant_list.header.offset_size);
        assert_eq!(
            expected_num_element_size,
            variant_list.header.num_elements_size
        );
        assert_eq!(list_size, variant_list.num_elements as usize);

        // verify the data in the variant
        assert_eq!(list_size, variant_list.len());
        for i in 0..list_size {
            let item = variant_list.get(i).unwrap();
            let item_str = item.as_string().unwrap();
            assert_eq!(expected_list.get(i).unwrap(), item_str);
        }
    }

    #[test]
    fn test_variant_list_equality() {
        // Create two lists with the same values (0..10)
        let (metadata1, value1) = make_listi32(0..10);
        let list1 = Variant::new(&metadata1, &value1);
        let (metadata2, value2) = make_listi32(0..10);
        let list2 = Variant::new(&metadata2, &value2);
        // They should be equal
        assert_eq!(list1, list2);
    }

    #[test]
    fn test_variant_list_equality_different_length() {
        // Create two lists with different lengths
        let (metadata1, value1) = make_listi32(0..10);
        let list1 = Variant::new(&metadata1, &value1);
        let (metadata2, value2) = make_listi32(0..5);
        let list2 = Variant::new(&metadata2, &value2);
        // They should not be equal
        assert_ne!(list1, list2);
    }

    #[test]
    fn test_variant_list_equality_different_values() {
        // Create two lists with different values
        let (metadata1, value1) = make_listi32(0..10);
        let list1 = Variant::new(&metadata1, &value1);
        let (metadata2, value2) = make_listi32(5..15);
        let list2 = Variant::new(&metadata2, &value2);
        // They should not be equal
        assert_ne!(list1, list2);
    }

    #[test]
    fn test_variant_list_equality_different_types() {
        // Create two lists with different types
        let (metadata1, value1) = make_listi32(0i32..10i32);
        let list1 = Variant::new(&metadata1, &value1);
        let (metadata2, value2) = make_listi64(0..10);
        let list2 = Variant::new(&metadata2, &value2);
        // They should not be equal due to type mismatch
        assert_ne!(list1, list2);
    }

    #[test]
    fn test_variant_list_equality_slices() {
        // Make an object like this and make sure equality works
        // when the lists are sub fields
        //
        // {
        //   "list1": [0, 1, 2, ..., 9],
        //   "list2": [0, 1, 2, ..., 9],
        //   "list3": [10, 11, 12, ..., 19],
        //  }
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut object_builder = builder.new_object();
            // list1 (0..10)
            let (metadata1, value1) = make_listi32(0i32..10i32);
            object_builder.insert("list1", Variant::new(&metadata1, &value1));

            // list2 (0..10)
            let (metadata2, value2) = make_listi32(0i32..10i32);
            object_builder.insert("list2", Variant::new(&metadata2, &value2));

            // list3 (10..20)
            let (metadata3, value3) = make_listi32(10i32..20i32);
            object_builder.insert("list3", Variant::new(&metadata3, &value3));
            object_builder.finish();
            builder.finish()
        };

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let object = variant.as_object().unwrap();
        // Check that list1 and list2 are equal
        assert_eq!(object.get("list1").unwrap(), object.get("list2").unwrap());
        // Check that list1 and list3 are not equal
        assert_ne!(object.get("list1").unwrap(), object.get("list3").unwrap());
    }

    /// return metadata/value for a simple variant list with values in a range
    fn make_listi32(range: Range<i32>) -> (Vec<u8>, Vec<u8>) {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.extend(range);
        list_builder.finish();
        variant_builder.finish()
    }

    /// return metadata/value for a simple variant list with values in a range
    fn make_listi64(range: Range<i64>) -> (Vec<u8>, Vec<u8>) {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.extend(range);
        list_builder.finish();
        variant_builder.finish()
    }
}
