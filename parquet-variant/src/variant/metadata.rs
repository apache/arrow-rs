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
    string_from_slice, validate_fallible_iterator,
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

// The metadata header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: usize = 1;

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
                "The version bytes in the header is not {CORRECT_VERSION_VALUE}, got {version:b}",
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
/// # Validation
///
/// Every instance of variant metadata is either _valid_ or _invalid_. depending on whether the
/// underlying bytes are a valid encoding of variant metadata (see below).
///
/// Instances produced by [`Self::try_new`] or [`Self::validate`] are fully _validated_, and always
/// contain _valid_ data.
///
/// Instances produced by [`Self::new`] are _unvalidated_ and may contain either _valid_ or
/// _invalid_ data. Infallible accesses such as iteration and indexing may panic if the underlying
/// bytes are _invalid_, and fallible alternatives such as [`Self::iter_try`] and [`Self::get`] are
/// strongly recommended. [`Self::validate`] can also be used to _validate_ an _unvalidated_
/// instance, if desired.
///
/// A _validated_ instance guarantees that:
///
/// - header byte is valid
/// - dictionary size is in bounds
/// - offset array content is in-bounds
/// - first offset is zero
/// - last offset is in-bounds
/// - all other offsets are in-bounds (*)
/// - all offsets are monotonically increasing (*)
/// - all values are valid utf-8 (*)
///
/// NOTE: [`Self::new`] only skips expensive validation checks (marked by `(*)` in the list above);
/// it panics any of the other checks fails.
///
/// [`Variant`]: crate::Variant
/// [Variant Spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#metadata-encoding
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VariantMetadata<'m> {
    bytes: &'m [u8],
    header: VariantMetadataHeader,
    dictionary_size: usize,
    dictionary_key_start_byte: usize,
    validated: bool,
}

impl<'m> VariantMetadata<'m> {
    /// Attempts to interpret `bytes` as a variant metadata instance, with full [validation] of all
    /// dictionary entries.
    ///
    /// [validation]: Self#Validation
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        Self::try_new_impl(bytes)?.validate()
    }

    /// Interprets `bytes` as a variant metadata instance, without attempting to [validate] dictionary
    /// entries. Panics if basic sanity checking fails, and subsequent infallible accesses such as
    /// indexing and iteration could also panic due to invalid bytes.
    ///
    /// This constructor can be a useful lightweight alternative to [`Self::try_new`] if the bytes
    /// were already validated previously by other means, or if the caller expects a small number of
    /// accesses to a large dictionary (preferring to use a small number of fallible accesses as
    /// needed, instead of paying expensive full validation up front).
    ///
    /// [validate]: Self#Validation
    pub fn new(bytes: &'m [u8]) -> Self {
        Self::try_new_impl(bytes).unwrap()
    }

    // The actual constructor, which performs only basic sanity checking.
    fn try_new_impl(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(bytes)?;
        let header = VariantMetadataHeader::try_new(header_byte)?;

        // First element after header is dictionary size
        let dictionary_size = header
            .offset_size
            .unpack_usize(bytes, NUM_HEADER_BYTES, 0)?;

        // Calculate the starting offset of the dictionary string bytes.
        //
        // Value header, dict_size (offset_size bytes), and dict_size+1 offsets
        // = NUM_HEADER_BYTES + offset_size + (dict_size + 1) * offset_size
        // = (dict_size + 2) * offset_size + NUM_HEADER_BYTES
        let dictionary_key_start_byte = dictionary_size
            .checked_add(2)
            .and_then(|n| n.checked_mul(header.offset_size as usize))
            .and_then(|n| n.checked_add(NUM_HEADER_BYTES))
            .ok_or_else(|| overflow_error("offset of variant metadata dictionary"))?;

        let new_self = Self {
            bytes,
            header,
            dictionary_size,
            dictionary_key_start_byte,
            validated: false,
        };

        // Validate just the first and last offset, ignoring the other offsets and all value bytes.
        let first_offset = new_self.get_offset(0)?;
        if first_offset != 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "First offset is not zero: {first_offset}"
            )));
        }
        // Bounds check the last offset by requesting the empty slice it points to.
        let last_offset = new_self.get_offset(dictionary_size)?;
        let _ =
            slice_from_slice_at_offset(bytes, dictionary_key_start_byte, last_offset..last_offset)?;
        Ok(new_self)
    }

    /// True if this instance is fully [validated] for panic-free infallible accesses.
    ///
    /// [validated]: Self#Validation
    pub fn is_validated(&self) -> bool {
        self.validated
    }

    /// Performs a full [validation] of this metadata dictionary.
    ///
    /// [validation]: Self#Validation
    pub fn validate(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Iterate over all string keys in this dictionary in order to prove that the offset
            // array is valid, all offsets are in bounds, and all string bytes are valid utf-8.
            validate_fallible_iterator(self.iter_try())?;
            self.validated = true;
        }
        Ok(self)
    }

    /// Whether the dictionary keys are sorted and unique
    pub fn is_sorted(&self) -> bool {
        self.header.is_sorted
    }

    /// Get the dictionary size
    pub fn dictionary_size(&self) -> usize {
        self.dictionary_size
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
        // Skip the header byte and the dictionary_size entry (by offset_index + 1)
        let bytes = slice_from_slice(self.bytes, ..self.dictionary_key_start_byte)?;
        self.header
            .offset_size
            .unpack_usize(bytes, NUM_HEADER_BYTES, i + 1)
    }

    /// Attempts to retrieve a dictionary entry by index, failing if out of bounds or if the
    /// underlying bytes are [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get(&self, i: usize) -> Result<&'m str, ArrowError> {
        let byte_range = self.get_offset(i)?..self.get_offset(i + 1)?;
        string_from_slice(self.bytes, self.dictionary_key_start_byte, byte_range)
    }

    /// Returns an iterator that attempts to visit all dictionary entries, producing `Err` if the
    /// iterator encounters [invalid] data.
    ///
    /// [invalid]: Self#Validation
    pub fn iter_try(&self) -> impl Iterator<Item = Result<&'m str, ArrowError>> + '_ {
        (0..self.dictionary_size).map(move |i| self.get(i))
    }

    /// Iterates oer all dictionary entries. When working with [unvalidated] input, prefer
    /// [`Self::iter_try`] to avoid panics due to invalid data.
    ///
    /// [unvalidated]: Self#Validation
    pub fn iter(&self) -> impl Iterator<Item = &'m str> + '_ {
        self.iter_try().map(Result::unwrap)
    }
}

/// Retrieves the ith dictionary entry, panicking if the index is out of bounds. Accessing
/// [unvalidated] input could also panic if the underlying bytes are invalid.
///
/// [unvalidated]: Self#Validation
impl std::ops::Index<usize> for VariantMetadata<'_> {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        self.get(i).unwrap()
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
        assert_eq!(&md[0], "cat");
        assert_eq!(&md[1], "dog");

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
        assert_eq!(&working_md[0], "a");
        assert_eq!(&working_md[1], "b");

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
