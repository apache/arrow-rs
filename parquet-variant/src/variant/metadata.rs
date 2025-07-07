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
use crate::utils::{first_byte_from_slice, overflow_error, slice_from_slice, string_from_slice};

use arrow_schema::ArrowError;

/// Header structure for [`VariantMetadata`]
#[derive(Debug, Clone, Copy, PartialEq)]
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
    // Hide the cast
    const fn offset_size(&self) -> usize {
        self.offset_size as usize
    }

    // Avoid materializing this offset, since it's cheaply and safely computable
    const fn first_offset_byte(&self) -> usize {
        NUM_HEADER_BYTES + self.offset_size()
    }

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
/// perform a small number of (fallible) accesses to a large dictionary.
///
/// A _validated_ variant [metadata instance guarantees that:
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
/// NOTE: [`Self::new`] only skips expensive (non-constant cost) validation checks (marked by `(*)`
/// in the list above); it panics any of the other checks fails.
///
/// # Safety
///
/// Even an _invalid_ variant metadata instance is still _safe_ to use in the Rust sense. Accessing
/// it with infallible methods may cause panics but will never lead to undefined behavior.
///
/// [`Variant`]: crate::Variant
/// [Variant Spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#metadata-encoding
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantMetadata<'m> {
    bytes: &'m [u8],
    header: VariantMetadataHeader,
    dictionary_size: usize,
    first_value_byte: usize,
    validated: bool,
}

impl<'m> VariantMetadata<'m> {
    /// Attempts to interpret `bytes` as a variant metadata instance, with full [validation] of all
    /// dictionary entries.
    ///
    /// [validation]: Self#Validation
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        Self::try_new_with_shallow_validation(bytes)?.with_full_validation()
    }

    /// Interprets `bytes` as a variant metadata instance, without attempting to [validate] dictionary
    /// entries. Panics if basic sanity checking fails, and subsequent infallible accesses such as
    /// indexing and iteration could also panic if the underlying bytes are invalid.
    ///
    /// This constructor can be a useful lightweight alternative to [`Self::try_new`] if the bytes
    /// were already validated previously by other means, or if the caller expects a small number of
    /// accesses to a large dictionary (preferring to use a small number of fallible accesses as
    /// needed, instead of paying expensive full validation up front).
    ///
    /// [validate]: Self#Validation
    pub fn new(bytes: &'m [u8]) -> Self {
        Self::try_new_with_shallow_validation(bytes).expect("Invalid variant metadata")
    }

    // The actual constructor, which performs only basic (constant-const) validation.
    pub(crate) fn try_new_with_shallow_validation(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header_byte = first_byte_from_slice(bytes)?;
        let header = VariantMetadataHeader::try_new(header_byte)?;

        // First element after header is dictionary size; the offset array immediately follows.
        let dictionary_size =
            header
                .offset_size
                .unpack_usize_at_offset(bytes, NUM_HEADER_BYTES, 0)?;

        // Calculate the starting offset of the dictionary string bytes.
        //
        // There are dict_size + 1 offsets, and the value bytes immediately follow
        // = (dict_size + 1) * offset_size + header.first_offset_byte()
        let first_value_byte = dictionary_size
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.offset_size()))
            .and_then(|n| n.checked_add(header.first_offset_byte()))
            .ok_or_else(|| overflow_error("offset of variant metadata dictionary"))?;

        let mut new_self = Self {
            bytes,
            header,
            dictionary_size,
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

        // Use the last offset to upper-bound the byte slice
        let last_offset = new_self
            .get_offset(dictionary_size)?
            .checked_add(first_value_byte)
            .ok_or_else(|| overflow_error("variant metadata size"))?;
        new_self.bytes = slice_from_slice(bytes, ..last_offset)?;
        Ok(new_self)
    }

    /// The number of metadata dictionary entries
    pub fn len(&self) -> usize {
        self.dictionary_size
    }

    /// True if this metadata dictionary contains no entries
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True if this instance is fully [validated] for panic-free infallible accesses.
    ///
    /// [validated]: Self#Validation
    pub fn is_fully_validated(&self) -> bool {
        self.validated
    }

    /// Performs a full [validation] of this metadata dictionary and returns the result.
    ///
    /// [validation]: Self#Validation
    pub fn with_full_validation(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Iterate over all string keys in this dictionary in order to prove that the offset
            // array is valid, all offsets are in bounds, and all string bytes are valid utf-8.

            /*
            As scovich pointed out, here are the things full validation must do:

            (1) all other offsets are in-bounds (*)
            (2) all offsets are monotonically increasing (*)
            (3) all values are valid utf-8 (*)

            I propose we add another validation check
            (4) if sorted dictionary, check if dictionary fields are sorted

                Doing this check will help us in objects with sorted dictionaries,
                since we can guarantee sortedness by checking if field ids are increasing.
            */

            // (1) (2)
            // Since shallow validation already computes the first and last offset
            // if we guarantee monotonicity for all offsets, then we know they are all in-bounds

            // notice how we do this ceremony only once!
            let offset_byte_range = self.header.first_offset_byte()..self.first_value_byte;
            let offset_bytes = slice_from_slice(self.bytes, offset_byte_range)?;

            let offsets = offset_bytes
                .chunks_exact(self.header.offset_size())
                .map(|chunk| {
                    // at this point, we know for a _fact_ that chunk will have `offset_size` bytes

                    match self.header.offset_size {
                        OffsetSizeBytes::One => chunk[0].into(),
                        OffsetSizeBytes::Two => u16::from_le_bytes([chunk[0], chunk[1]]).into(),
                        OffsetSizeBytes::Three => {
                            u32::from_le_bytes([chunk[0], chunk[1], chunk[2], 0]) as usize
                        }
                        OffsetSizeBytes::Four => {
                            u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize
                        }
                    }
                })
                .collect::<Vec<usize>>();

            let offsets_monotonically_increasing = offsets.is_sorted_by(|a, b| a < b);

            if !offsets_monotonically_increasing {
                return Err(ArrowError::InvalidArgumentError(
                    "offsets not monotonically increasing".to_string(),
                ));
            }

            // (3)
            // We don't take advantage of the values being packed side by side.
            // For every value, we rerequest the entire value buffer and then slice into _just_ that value
            // and parse into a str

            // This looks like a great place to do vectorized utf8 validation
            // plus, all subsequent attempts at parsing out values as a &str can be done using
            // String::from_utf8_unchecked

            let value_bytes = slice_from_slice(self.bytes, self.first_value_byte..)?;
            let value_str = simdutf8::basic::from_utf8(value_bytes)
                .map_err(|e| ArrowError::InvalidArgumentError(format!("{e:?}")))?;

            // (4)
            // if the metadata header marked this variant as having a sorted dictionary,
            // we must check whether the fields are actually sorted

            if self.header.is_sorted {
                let mut prev_field_name = None;
                let mut is_sorted = true;

                for i in 0..offsets.len() - 1 {
                    if !is_sorted {
                        return Err(ArrowError::InvalidArgumentError(
                            "variant marked as having sorted dictionary but is unsorted"
                                .to_string(),
                        ));
                    }

                    let offset_range = offsets[i]..offsets[i + 1];

                    let field_name = value_str
                        .get(offset_range)
                        .ok_or_else(|| overflow_error("overflowed"))?;

                    if let Some(prev_field_name) = prev_field_name {
                        is_sorted = is_sorted && prev_field_name < field_name;
                    }

                    prev_field_name = Some(field_name);
                }
            }

            self.validated = true;
        }
        Ok(self)
    }

    /// Whether the dictionary keys are sorted and unique
    pub fn is_sorted(&self) -> bool {
        self.header.is_sorted
    }

    /// Get the dictionary size
    pub const fn dictionary_size(&self) -> usize {
        self.dictionary_size
    }

    /// The variant protocol version
    pub const fn version(&self) -> u8 {
        self.header.version
    }

    /// Gets an offset array entry by index.
    ///
    /// This offset is an index into the dictionary, at the boundary between string `i-1` and string
    /// `i`. See [`Self::get`] to retrieve a specific dictionary entry.
    fn get_offset(&self, i: usize) -> Result<usize, ArrowError> {
        let offset_byte_range = self.header.first_offset_byte()..self.first_value_byte;
        let bytes = slice_from_slice(self.bytes, offset_byte_range)?;
        self.header.offset_size.unpack_usize(bytes, i)
    }

    /// Attempts to retrieve a dictionary entry by index, failing if out of bounds or if the
    /// underlying bytes are [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get(&self, i: usize) -> Result<&'m str, ArrowError> {
        let byte_range = self.get_offset(i)?..self.get_offset(i + 1)?;
        string_from_slice(self.bytes, self.first_value_byte, byte_range)
    }

    /// Returns an iterator that attempts to visit all dictionary entries, producing `Err` if the
    /// iterator encounters [invalid] data.
    ///
    /// [invalid]: Self#Validation
    pub fn iter_try(&self) -> impl Iterator<Item = Result<&'m str, ArrowError>> + '_ {
        (0..self.dictionary_size).map(move |i| self.get(i))
    }

    /// Iterates over all dictionary entries. When working with [unvalidated] input, consider
    /// [`Self::iter_try`] to avoid panics due to invalid data.
    ///
    /// [unvalidated]: Self#Validation
    pub fn iter(&self) -> impl Iterator<Item = &'m str> + '_ {
        self.iter_try()
            .map(|result| result.expect("Invalid metadata dictionary entry"))
    }
}

/// Retrieves the ith dictionary entry, panicking if the index is out of bounds. Accessing
/// [unvalidated] input could also panic if the underlying bytes are invalid.
///
/// [unvalidated]: Self#Validation
impl std::ops::Index<usize> for VariantMetadata<'_> {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        self.get(i).expect("Invalid metadata dictionary entry")
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
    fn try_new_fails_non_monotonic2() {
        // this test case checks whether offsets are monotonic in the full validation logic.

        // 'cat', 'dog', 'lamb', "eel"
        let bytes = &[
            0b0000_0001, // header, offset_size_minus_one=0 and version=1
            4,           // dictionary_size
            0x00,
            0x02,
            0x01, // Doesn't increase monotonically
            0x10,
            13,
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
            b'e',
            b'e',
            b'l',
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
