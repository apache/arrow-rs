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
use crate::utils::{first_byte_from_slice, slice_from_slice_at_offset, validate_fallible_iterator};
use crate::variant::{Variant, VariantMetadata};

use arrow_schema::ArrowError;

// The value header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: usize = 1;

/// A parsed version of the variant array value header byte.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantListHeader {
    offset_size: OffsetSizeBytes,
    is_large: bool,
}

impl VariantListHeader {
    pub(crate) fn try_new(header_byte: u8) -> Result<Self, ArrowError> {
        // The 6 first bits to the left are the value_header and the 2 bits
        // to the right are the basic type, so we shift to get only the value_header
        let value_header = header_byte >> 2;
        let is_large = (value_header & 0x04) != 0; // 3rd bit from the right
        let field_offset_size_minus_one = value_header & 0x03; // Last two bits
        let offset_size = OffsetSizeBytes::try_new(field_offset_size_minus_one)?;

        Ok(Self {
            offset_size,
            is_large,
        })
    }
}

/// [`Variant`] Array.
///
/// NOTE: The "list" naming differs from the variant spec -- which calls it "array" -- in order to be
/// consistent with Parquet and Arrow type naming. Otherwise, the name would conflict with the
/// `VariantArray : Array` we must eventually define for variant-typed arrow arrays.
#[derive(Clone, Debug, PartialEq)]
pub struct VariantList<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantListHeader,
    num_elements: usize,
    first_offset_byte: usize,
    first_value_byte: usize,
}

impl<'m, 'v> VariantList<'m, 'v> {
    /// Attempts to interpret `value` as a variant array value.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` points to a valid variant array value. In particular,
    /// that all offsets are in-bounds and point to valid objects.
    // TODO: How to make the validation non-recursive while still making iterators safely infallible??
    // See https://github.com/apache/arrow-rs/issues/7711
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(value)?;
        let header = VariantListHeader::try_new(header_byte)?;

        // The size of the num_elements entry in the array value_data is 4 bytes if
        // is_large is true, otherwise 1 byte.
        let num_elements_size = match header.is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };

        // Skip the header byte to read the num_elements; the offset array immediately follows
        let num_elements = num_elements_size.unpack_usize(value, NUM_HEADER_BYTES, 0)?;
        let first_offset_byte = NUM_HEADER_BYTES + num_elements_size as usize;

        // (num_elements + 1) * offset_size + first_offset_byte
        let first_value_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.offset_size as usize))
            .and_then(|n| n.checked_add(first_offset_byte))
            .ok_or_else(|| ArrowError::InvalidArgumentError("Integer overflow computing first_value_byte".into()))?;

        let new_self = Self {
            metadata,
            value,
            header,
            num_elements,
            first_offset_byte,
            first_value_byte,
        };

        // Iterate over all values of this array in order to validate the field_offset array and
        // prove that the field values are all in bounds. Otherwise, `iter` might panic on `unwrap`.
        validate_fallible_iterator(new_self.iter_checked())?;
        Ok(new_self)
    }

    /// Return the length of this array
    pub fn len(&self) -> usize {
        self.num_elements
    }

    /// Is the array of zero length
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        if index >= self.num_elements {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index {} out of bounds for list of length {}",
                index, self.num_elements,
            )));
        }

        // Skip header and num_elements bytes to read the offsets
        let unpack = |i| {
            self.header
                .offset_size
                .unpack_usize(self.value, self.first_offset_byte, i)
        };

        // Read the value bytes from the offsets
        let variant_value_bytes = slice_from_slice_at_offset(
            self.value,
            self.first_value_byte,
            unpack(index)?..unpack(index + 1)?,
        )?;
        let variant = Variant::try_new_with_metadata(self.metadata, variant_value_bytes)?;
        Ok(variant)
    }

    /// Iterates over the values of this list
    pub fn iter(&self) -> impl Iterator<Item = Variant<'m, 'v>> + '_ {
        // NOTE: It is safe to unwrap because the constructor already made a successful traversal.
        self.iter_checked().map(Result::unwrap)
    }

    // Fallible iteration over the fields of this dictionary. The constructor traverses the iterator
    // to prove it has no errors, so that all other use sites can blindly `unwrap` the result.
    fn iter_checked(&self) -> impl Iterator<Item = Result<Variant<'m, 'v>, ArrowError>> + '_ {
        (0..self.len()).map(move |i| self.get(i))
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
        assert!(out_of_bounds.is_err());
        assert!(matches!(
            out_of_bounds.unwrap_err(),
            ArrowError::InvalidArgumentError(ref msg) if msg.contains("out of bounds")
        ));

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
        assert!(out_of_bounds.is_err());

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
