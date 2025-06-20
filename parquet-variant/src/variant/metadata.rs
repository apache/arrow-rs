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
    first_byte_from_slice, slice_from_slice, string_from_slice, validate_fallible_iterator,
};

use arrow_schema::ArrowError;

/// Header structure for [`VariantMetadata`]
#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) struct VariantMetadataHeader {
    version: u8,
    is_sorted: bool,
    /// Note: This is `offset_size_minus_one` + 1
    offset_size: OffsetSizeBytes,
}

// According to the spec this is currently always = 1, and so we store this const for validation
// purposes and to make that visible.
const CORRECT_VERSION_VALUE: u8 = 1;

impl VariantMetadataHeader {
    /// Tries to construct the variant metadata header, which has the form
    ///
    /// ```text
    ///              7     6  5   4  3             0
    ///             +-------+---+---+---------------+
    /// header      |       |   |   |    version    |
    ///             +-------+---+---+---------------+
    ///                 ^         ^
    ///                 |         +-- sorted_strings
    ///                 +-- offset_size_minus_one
    /// ```
    ///
    /// The version is a 4-bit value that must always contain the value 1.
    /// - sorted_strings is a 1-bit value indicating whether dictionary strings are sorted and unique.
    /// - offset_size_minus_one is a 2-bit value providing the number of bytes per dictionary size and offset field.
    /// - The actual number of bytes, offset_size, is offset_size_minus_one + 1
    pub(crate) fn try_new(header_byte: u8) -> Result<Self, ArrowError> {
        let version = header_byte & 0x0F; // First four bits
        if version != CORRECT_VERSION_VALUE {
            let err_msg = format!(
                "The version bytes in the header is not {CORRECT_VERSION_VALUE}, got {:b}",
                version
            );
            return Err(ArrowError::InvalidArgumentError(err_msg));
        }
        let is_sorted = (header_byte & 0x10) != 0; // Fifth bit
        let offset_size_minus_one = header_byte >> 6; // Last two bits
        Ok(Self {
            version,
            is_sorted,
            offset_size: OffsetSizeBytes::try_new(offset_size_minus_one)?,
        })
    }
}

/// [`Variant`] Metadata
///
/// See the [Variant Spec] file for more information
///
/// [`Variant`]: crate::Variant
/// [Variant Spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#metadata-encoding
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VariantMetadata<'m> {
    bytes: &'m [u8],
    header: VariantMetadataHeader,
    dict_size: usize,
    dictionary_key_start_byte: usize,
}

impl<'m> VariantMetadata<'m> {
    /// View the raw bytes (needed by very low-level decoders)
    #[inline]
    pub const fn as_bytes(&self) -> &'m [u8] {
        self.bytes
    }

    /// Attempts to interpret `bytes` as a variant metadata instance.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `bytes` points to a valid variant metadata instance. In
    /// particular, all offsets are in-bounds and point to valid utf8 strings.
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(bytes)?;
        let header = VariantMetadataHeader::try_new(header_byte)?;

        // Offset 1, index 0 because first element after header is dictionary size
        let dict_size = header.offset_size.unpack_usize(bytes, 1, 0)?;

        // Calculate the starting offset of the dictionary string bytes.
        //
        // Value header, dict_size (offset_size bytes), and dict_size+1 offsets
        // = 1 + offset_size + (dict_size + 1) * offset_size
        // = (dict_size + 2) * offset_size + 1
        let dictionary_key_start_byte = dict_size
            .checked_add(2)
            .and_then(|n| n.checked_mul(header.offset_size as usize))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| ArrowError::InvalidArgumentError("metadata length overflow".into()))?;
        println!("dictionary_key_start_byte: {dictionary_key_start_byte}");
        let new_self = Self {
            bytes,
            header,
            dict_size,
            dictionary_key_start_byte,
        };

        // Iterate over all string keys in this dictionary in order to validate the offset array and
        // prove that the string bytes are all in bounds. Otherwise, `iter` might panic on `unwrap`.
        validate_fallible_iterator(new_self.iter_checked())?;
        Ok(new_self)
    }

    /// Whether the dictionary keys are sorted and unique
    pub fn is_sorted(&self) -> bool {
        self.header.is_sorted
    }

    /// Get the dictionary size
    pub fn dictionary_size(&self) -> usize {
        self.dict_size
    }

    /// The variant protocol version
    pub fn version(&self) -> u8 {
        self.header.version
    }

    /// Gets an offset array entry by index.
    ///
    /// This offset is an index into the dictionary, at the boundary between string `i-1` and string
    /// `i`. See [`Self::get`] to retrieve a specific dictionary entry.
    fn get_offset(&self, i: usize) -> Result<usize, ArrowError> {
        // Skipping the header byte (setting byte_offset = 1) and the dictionary_size (setting offset_index +1)
        let bytes = slice_from_slice(self.bytes, ..self.dictionary_key_start_byte)?;
        self.header.offset_size.unpack_usize(bytes, 1, i + 1)
    }

    /// Gets a dictionary entry by index
    pub fn get(&self, i: usize) -> Result<&'m str, ArrowError> {
        let dictionary_keys_bytes = slice_from_slice(self.bytes, self.dictionary_key_start_byte..)?;
        let byte_range = self.get_offset(i)?..self.get_offset(i + 1)?;
        string_from_slice(dictionary_keys_bytes, byte_range)
    }

    /// Get all dictionary entries as an Iterator of strings
    pub fn iter(&self) -> impl Iterator<Item = &'m str> + '_ {
        // NOTE: It is safe to unwrap because the constructor already made a successful traversal.
        self.iter_checked().map(Result::unwrap)
    }

    // Fallible iteration over the fields of this dictionary. The constructor traverses the iterator
    // to prove it has no errors, so that all other use sites can blindly `unwrap` the result.
    fn iter_checked(&self) -> impl Iterator<Item = Result<&'m str, ArrowError>> + '_ {
        (0..self.dict_size).map(move |i| self.get(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `"cat"`, `"dog"` – valid metadata
    #[test]
    fn try_new_ok_inline() {
        let bytes = &[
            0b0000_0001, // header, offset_size_minus_one=0 and version=1
            0x02,        // dictionary_size (2 strings)
            0x00,
            0x03,
            0x06,
            b'c',
            b'a',
            b't',
            b'd',
            b'o',
            b'g',
        ];

        let md = VariantMetadata::try_new(bytes).expect("should parse");
        assert_eq!(md.dictionary_size(), 2);
        // Fields
        assert_eq!(md.get(0).unwrap(), "cat");
        assert_eq!(md.get(1).unwrap(), "dog");

        // Offsets
        assert_eq!(md.get_offset(0).unwrap(), 0x00);
        assert_eq!(md.get_offset(1).unwrap(), 0x03);
        assert_eq!(md.get_offset(2).unwrap(), 0x06);

        let err = md.get_offset(3).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "unexpected error: {err:?}"
        );

        let fields: Vec<(usize, &str)> = md.iter().enumerate().collect();
        assert_eq!(fields, vec![(0usize, "cat"), (1usize, "dog")]);
    }

    /// Too short buffer test (missing one required offset).
    /// Should error with "metadata shorter than dictionary_size implies".
    #[test]
    fn try_new_missing_last_value() {
        let bytes = &[
            0b0000_0001, // header, offset_size_minus_one=0 and version=1
            0x02,        // dictionary_size = 2
            0x00,
            0x01,
            0x02,
            b'a',
            b'b', // <-- we'll remove this
        ];

        let working_md = VariantMetadata::try_new(bytes).expect("should parse");
        assert_eq!(working_md.dictionary_size(), 2);
        assert_eq!(working_md.get(0).unwrap(), "a");
        assert_eq!(working_md.get(1).unwrap(), "b");

        let truncated = &bytes[..bytes.len() - 1];

        let err = VariantMetadata::try_new(truncated).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn try_new_fails_non_monotonic() {
        // 'cat', 'dog', 'lamb'
        let bytes = &[
            0b0000_0001, // header, offset_size_minus_one=0 and version=1
            0x03,        // dictionary_size
            0x00,
            0x02,
            0x01, // Doesn't increase monotonically
            0x10,
            b'c',
            b'a',
            b't',
            b'd',
            b'o',
            b'g',
            b'l',
            b'a',
            b'm',
            b'b',
        ];

        let err = VariantMetadata::try_new(bytes).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn try_new_truncated_offsets_inline() {
        // Missing final offset
        let bytes = &[0b0000_0001, 0x02, 0x00, 0x01];

        let err = VariantMetadata::try_new(bytes).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "unexpected error: {err:?}"
        );
    }
}
