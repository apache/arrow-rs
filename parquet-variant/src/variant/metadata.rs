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
use crate::utils::{overflow_error, slice_from_slice, str_from_bytes, string_from_slice};
use crate::variant::view::MetadataView;

use arrow_schema::ArrowError;

/// Header structure for [`VariantMetadata`]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct VariantMetadataHeader {
    version: u8,
    pub(crate) is_sorted: bool,
    /// Note: This is `offset_size_minus_one` + 1
    pub(crate) offset_size: OffsetSizeBytes,
}

// According to the spec this is currently always = 1, and so we store this const for validation
// purposes and to make that visible.
const CORRECT_VERSION_VALUE: u8 = 1;

// The metadata header occupies one byte; use a named constant for readability
const NUM_HEADER_BYTES: u32 = 1;

impl VariantMetadataHeader {
    // Hide the cast
    pub(crate) const fn offset_size(&self) -> u32 {
        self.offset_size as u32
    }

    // Avoid materializing this offset, since it's cheaply and safely computable
    pub(crate) const fn first_offset_byte(&self) -> u32 {
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
    #[inline]
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
/// - all offsets are non-decreasing (*)
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
#[derive(Debug, Clone, PartialEq)]
pub struct VariantMetadata<'m> {
    /// (Only) the bytes that make up this metadata instance.
    pub(crate) bytes: &'m [u8],
    header: VariantMetadataHeader,
    dictionary_size: u32,
    first_value_byte: u32,
    validated: bool,
}

// We don't want this to grow because it increases the size of VariantList and VariantObject, which
// could increase the size of Variant. All those size increases could hurt performance.
#[cfg(target_pointer_width = "64")]
const _: () = crate::utils::expect_size_of::<VariantMetadata>(32);

#[cfg(target_pointer_width = "32")]
const _: () = crate::utils::expect_size_of::<VariantMetadata>(20);

/// The canonical byte slice corresponding to an empty metadata dictionary.
///
/// ```
/// # use parquet_variant::{EMPTY_VARIANT_METADATA_BYTES, VariantMetadata, WritableMetadataBuilder};
/// let mut metadata_builder = WritableMetadataBuilder::default();
/// metadata_builder.finish();
/// let metadata_bytes = metadata_builder.into_inner();
/// assert_eq!(&metadata_bytes, EMPTY_VARIANT_METADATA_BYTES);
/// ```
pub const EMPTY_VARIANT_METADATA_BYTES: &[u8] = &[1, 0, 0];

/// The empty metadata dictionary.
///
/// ```
/// # use parquet_variant::{EMPTY_VARIANT_METADATA, VariantMetadata, WritableMetadataBuilder};
/// let mut metadata_builder = WritableMetadataBuilder::default();
/// metadata_builder.finish();
/// let metadata_bytes = metadata_builder.into_inner();
/// let empty_metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();
/// assert_eq!(empty_metadata, EMPTY_VARIANT_METADATA);
/// ```
pub const EMPTY_VARIANT_METADATA: VariantMetadata = VariantMetadata {
    bytes: EMPTY_VARIANT_METADATA_BYTES,
    header: VariantMetadataHeader {
        version: CORRECT_VERSION_VALUE,
        is_sorted: false,
        offset_size: OffsetSizeBytes::One,
    },
    dictionary_size: 0,
    first_value_byte: 3,
    validated: true,
};

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
    ///
    /// # Panics
    ///
    /// Panics if basic sanity checking fails. Use [`Self::try_new`] for a fallible version.
    pub fn new(bytes: &'m [u8]) -> Self {
        Self::try_new_with_shallow_validation(bytes).expect("Invalid variant metadata")
    }

    // The actual constructor, which performs only basic (constant-const) validation.
    pub(crate) fn try_new_with_shallow_validation(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        MetadataView::try_new(bytes)?.into_metadata()
    }

    // Promotes a view by validating just the first and last offset, ignoring the other offsets
    // and all value bytes.
    #[inline]
    pub(crate) fn try_from_view(view: MetadataView<'m>) -> Result<Self, ArrowError> {
        let first_offset = view.offset(0)?;
        if first_offset != 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "First offset is not zero: {first_offset}"
            )));
        }

        // Use the last offset to upper-bound the byte slice
        let last_offset = view
            .offset(view.len())?
            .checked_add(view.first_value_byte)
            .ok_or_else(|| overflow_error("variant metadata size"))?;
        Ok(Self {
            bytes: slice_from_slice(view.bytes, ..last_offset as _)?,
            header: view.header,
            dictionary_size: view.dictionary_size,
            first_value_byte: view.first_value_byte,
            validated: false,
        })
    }

    /// A view of this dictionary, which reads only the entries it is asked for.
    #[doc(hidden)]
    #[inline]
    pub fn view(&self) -> MetadataView<'m> {
        MetadataView {
            bytes: self.bytes,
            header: self.header,
            dictionary_size: self.dictionary_size,
            first_value_byte: self.first_value_byte,
        }
    }

    /// The number of metadata dictionary entries
    pub fn len(&self) -> usize {
        self.dictionary_size as _
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
            let offset_bytes = slice_from_slice(
                self.bytes,
                self.header.first_offset_byte() as _..self.first_value_byte as _,
            )?;

            // Verify the string values in the dictionary are UTF-8 encoded strings.
            let value_buffer =
                string_from_slice(self.bytes, 0, self.first_value_byte as _..self.bytes.len())?;

            let mut offsets = map_bytes_to_offsets(offset_bytes, self.header.offset_size);
            let mut current_offset = offsets.next().unwrap_or(0);

            if self.header.is_sorted {
                // Validate the dictionary values are unique and lexicographically sorted
                //
                // Since we use the offsets to access dictionary values, this also validates
                // offsets are in-bounds and monotonically increasing
                let mut prev_value: Option<&str> = None;
                for next_offset in offsets {
                    let current_value = value_buffer.get(current_offset..next_offset).ok_or_else(
                        || {
                            ArrowError::InvalidArgumentError(format!(
                                "range {current_offset}..{next_offset} is invalid or out of bounds"
                            ))
                        },
                    )?;

                    if let Some(prev_val) = prev_value
                        && current_value <= prev_val
                    {
                        return Err(ArrowError::InvalidArgumentError(
                            "dictionary values are not unique and ordered".to_string(),
                        ));
                    }

                    prev_value = Some(current_value);
                    current_offset = next_offset;
                }
            } else {
                // Slicing each dictionary value validates that offsets are in-bounds, non-decreasing,
                // and land on UTF-8 character boundaries. Equal offsets are legal: they encode an
                // empty dictionary entry.
                for next_offset in offsets {
                    value_buffer
                        .get(current_offset..next_offset)
                        .ok_or_else(|| {
                            ArrowError::InvalidArgumentError(format!(
                                "range {current_offset}..{next_offset} is invalid or out of bounds"
                            ))
                        })?;
                    current_offset = next_offset;
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

    /// The variant protocol version
    pub const fn version(&self) -> u8 {
        self.header.version
    }

    /// Returns the total size, in bytes, of the metadata.
    ///
    /// Note this value may be smaller than what was passed to [`Self::new`] or
    /// [`Self::try_new`] if the input was larger than necessary to encode the
    /// metadata dictionary.
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// Attempts to retrieve a dictionary entry by index, failing if out of bounds or if the
    /// underlying bytes are [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get(&self, i: usize) -> Result<&'m str, ArrowError> {
        let value_bytes = self.view().entry(i)?;
        if !self.validated {
            return str_from_bytes(value_bytes);
        }

        // Full validation already proved every dictionary entry is valid UTF-8, so validating
        // again here would charge that cost once per field access instead of once per buffer.
        debug_assert!(std::str::from_utf8(value_bytes).is_ok());

        // SAFETY: `validated` is set only by `with_full_validation`, which proved that every
        // dictionary entry, including this one, is valid UTF-8.
        Ok(unsafe { str::from_utf8_unchecked(value_bytes) })
    }

    // Helper method used by our `impl Index` and also by `get_entry`. Panics if the underlying
    // bytes are invalid. Needed because the `Index` trait forces the returned result to have the
    // lifetime of `self` instead of the string's own (longer) lifetime `'m`.
    fn get_impl(&self, i: usize) -> &'m str {
        self.get(i).expect("Invalid metadata dictionary entry")
    }

    /// Attempts to retrieve a dictionary entry and its field id, returning None if the requested field
    /// name is not present. The search cost is logarithmic if [`Self::is_sorted`] and linear
    /// otherwise.
    ///
    /// # Panics
    ///
    /// Panics if the underlying bytes are [invalid].
    ///
    /// [invalid]: Self#Validation
    pub fn get_entry(&self, field_name: &str) -> Option<(u32, &'m str)> {
        let field_id = self
            .view()
            .find(field_name.as_bytes())
            .expect("Invalid metadata dictionary entry")?;
        Some((field_id, self.get_impl(field_id as _)))
    }

    /// Returns an iterator that attempts to visit all dictionary entries, producing `Err` if the
    /// iterator encounters [invalid] data.
    ///
    /// [invalid]: Self#Validation
    pub fn iter_try(&self) -> impl Iterator<Item = Result<&'m str, ArrowError>> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }

    /// Iterates over all dictionary entries. When working with [unvalidated] input, consider
    /// [`Self::iter_try`] to avoid panics due to invalid data.
    ///
    /// [unvalidated]: Self#Validation
    ///
    /// # Panics
    ///
    /// Panics if the underlying bytes are invalid. Use [`Self::iter_try`] for a
    /// fallible version.
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
        self.get_impl(i)
    }
}

#[cfg(test)]
mod tests {

    use crate::VariantBuilder;

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
        assert_eq!(md.len(), 2);
        // Fields
        assert_eq!(&md[0], "cat");
        assert_eq!(&md[1], "dog");

        // Offsets
        assert_eq!(md.view().offset(0).unwrap(), 0x00);
        assert_eq!(md.view().offset(1).unwrap(), 0x03);
        assert_eq!(md.view().offset(2).unwrap(), 0x06);

        let err = md.get(2).unwrap_err();
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
        assert_eq!(working_md.len(), 2);
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

    #[test]
    fn empty_string_is_valid() {
        let bytes = &[
            0b0001_0001, // header: offset_size_minus_one=0, ordered=1, version=1
            1,
            0x00,
            0x00,
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert_eq!(&metadata[0], "");

        let bytes = &[
            0b0001_0001, // header: offset_size_minus_one=0, ordered=1, version=1
            2,
            0x00,
            0x00,
            0x02,
            b'h',
            b'i',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert_eq!(&metadata[0], "");
        assert_eq!(&metadata[1], "hi");

        let bytes = &[
            0b0001_0001, // header: offset_size_minus_one=0, ordered=1, version=1
            2,
            0x00,
            0x02,
            0x02, // empty string is allowed, but must be first in a sorted dict
            b'h',
            b'i',
        ];
        let err = VariantMetadata::try_new(bytes).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "unexpected error: {err:?}"
        );

        let bytes = &[
            0b0000_0001, // header: offset_size_minus_one=0, ordered=0, version=1
            2,
            0x00,
            0x02,
            0x02, // an unsorted dict may hold an empty string anywhere
            b'h',
            b'i',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert_eq!(&metadata[0], "hi");
        assert_eq!(&metadata[1], "");
    }

    /// Builds a metadata dictionary containing `field_names`, in the order given.
    fn metadata_bytes_for(field_names: &[&str]) -> Vec<u8> {
        let mut builder = VariantBuilder::new().with_field_names(field_names.iter().copied());
        let mut object = builder.new_object();
        for name in field_names {
            object.insert(name, 1i32);
        }
        object.finish();
        builder.finish().0
    }

    /// Every entry of `metadata` must resolve to its own field id, whether the name is borrowed from
    /// the dictionary or owned.
    fn assert_lookups_agree(metadata: &VariantMetadata<'_>) {
        for i in 0..metadata.len() {
            let borrowed = metadata.get(i).unwrap();
            let owned = borrowed.to_string();
            assert_eq!(
                metadata.get_entry(borrowed),
                Some((i as u32, borrowed)),
                "get_entry of borrowed {borrowed:?} (field id {i})"
            );
            assert_eq!(
                metadata.get_entry(owned.as_str()),
                Some((i as u32, borrowed)),
                "get_entry of owned {borrowed:?} (field id {i})"
            );
        }
    }

    #[test]
    fn test_get_entry_sorted_dictionary() {
        // More than 10 entries, so `get_entry` uses its binary search path.
        let names: Vec<String> = (0..64).map(|i| format!("field_{i:03}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let bytes = metadata_bytes_for(&names);
        let metadata = VariantMetadata::try_new(&bytes).unwrap();

        assert!(metadata.is_sorted());
        assert_eq!(metadata.len(), 64);
        assert_lookups_agree(&metadata);

        assert_eq!(metadata.get_entry("field_999"), None);
    }

    #[test]
    fn test_get_entry_unsorted_dictionary() {
        let bytes = metadata_bytes_for(&["zebra", "apple", "mango", "kiwi"]);
        let metadata = VariantMetadata::try_new(&bytes).unwrap();

        assert!(!metadata.is_sorted());
        assert_eq!(metadata.len(), 4);
        // An unsorted dictionary is searched linearly.
        assert_lookups_agree(&metadata);
    }

    /// A dictionary that is not marked sorted may legally repeat a string. A name must still map to
    /// a single field id, because `ObjectBuilder` detects duplicate fields by field id.
    #[test]
    fn test_get_entry_unsorted_dictionary_with_duplicate_entries() {
        let bytes = &[
            0b0000_0001, // header: offset_size_minus_one=0, sorted=0, version=1
            2,           // dictionary_size
            0x00,
            0x01,
            0x02,
            b'a',
            b'a',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert!(!metadata.is_sorted());
        assert_eq!(metadata.get(0).unwrap(), "a");
        assert_eq!(metadata.get(1).unwrap(), "a");

        // Whichever entry a name was borrowed from, it resolves to the first entry naming it.
        let owned = String::from("a");
        for name in [metadata.get(0).unwrap(), metadata.get(1).unwrap(), &owned] {
            assert_eq!(metadata.get_entry(name), Some((0, "a")));
        }
    }

    #[test]
    fn test_get_entry_ignores_names_from_another_dictionary() {
        let this_bytes = metadata_bytes_for(&["alpha", "beta", "gamma"]);
        let this = VariantMetadata::try_new(&this_bytes).unwrap();

        // A different dictionary that shares some names, with different field ids for them.
        let other_bytes = metadata_bytes_for(&["delta", "gamma", "beta", "alpha"]);
        let other = VariantMetadata::try_new(&other_bytes).unwrap();

        for i in 0..other.len() {
            let name = other.get(i).unwrap();
            // A copy that borrows from neither dictionary, to compare results against.
            let unborrowed_name: String = name.chars().collect();
            assert_eq!(this.get_entry(name), this.get_entry(&unborrowed_name));
        }

        assert_eq!(this.get_entry(other.get(3).unwrap()), Some((0, "alpha")));
        assert_eq!(this.get_entry(other.get(0).unwrap()), None);
    }

    #[test]
    fn test_get_entry_rejects_substring_of_an_entry() {
        // Dictionary ["x", "yy"], stored as the bytes "xyy". A slice of an entry is not the entry.
        let bytes = &[
            0b0001_0001, // header: offset_size_minus_one=0, ordered=1, version=1
            2,           // dictionary_size
            0x00,
            0x01,
            0x03,
            b'x',
            b'y',
            b'y',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert!(metadata.is_sorted());
        assert_eq!(metadata.get(0).unwrap(), "x");
        assert_eq!(metadata.get(1).unwrap(), "yy");
        assert_eq!(
            metadata.get_entry(metadata.get(1).unwrap()),
            Some((1, "yy"))
        );

        let prefix_of_entry_1 = &metadata.get(1).unwrap()[..1];
        assert_eq!(prefix_of_entry_1, "y");
        assert_eq!(metadata.get_entry(prefix_of_entry_1), None);

        let suffix_of_entry_1 = &metadata.get(1).unwrap()[1..];
        assert_eq!(metadata.get_entry(suffix_of_entry_1), None);
    }

    #[test]
    fn test_get_entry_with_empty_field_name() {
        let bytes = &[
            0b0000_0001, // header: offset_size_minus_one=0, ordered=0, version=1
            2,           // dictionary_size
            0x00,
            0x02,
            0x02, // an unsorted dict may hold an empty string anywhere
            b'h',
            b'i',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert!(!metadata.is_sorted());
        assert_eq!(metadata.get(0).unwrap(), "hi");
        assert_eq!(metadata.get(1).unwrap(), "");

        assert_eq!(
            metadata.get_entry(metadata.get(0).unwrap()),
            Some((0, "hi"))
        );
        assert_eq!(metadata.get_entry(metadata.get(1).unwrap()), Some((1, "")));
        assert_eq!(metadata.get_entry(""), Some((1, "")));
    }

    #[test]
    fn test_get_entry_with_leading_empty_field_name() {
        // A sorted dictionary can only hold an empty string as its first entry, where it shares a
        // start offset with the entry after it.
        let bytes = &[
            0b0001_0001, // header: offset_size_minus_one=0, ordered=1, version=1
            2,           // dictionary_size
            0x00,
            0x00,
            0x02,
            b'h',
            b'i',
        ];
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        assert!(metadata.is_sorted());
        assert_eq!(metadata.get(0).unwrap(), "");
        assert_eq!(metadata.get(1).unwrap(), "hi");

        assert_lookups_agree(&metadata);
        assert_eq!(metadata.get_entry(""), Some((0, "")));
    }

    #[test]
    fn test_compare_sorted_dictionary_with_unsorted_dictionary() {
        // create a sorted object
        let mut b = VariantBuilder::new();
        let mut o = b.new_object();

        o.insert("a", false);
        o.insert("b", false);

        o.finish();

        let (m, _) = b.finish();

        let m1 = VariantMetadata::new(&m);
        assert!(m1.is_sorted());

        // Create metadata with an unsorted dictionary (field names are "a", "a", "b")
        // Since field names are not unique, it is considered not sorted.
        let metadata_bytes = vec![
            0b0000_0001,
            3, // dictionary size
            0, // "a"
            1, // "a"
            2, // "b"
            3,
            b'a',
            b'a',
            b'b',
        ];
        let m2 = VariantMetadata::try_new(&metadata_bytes).unwrap();
        assert!(!m2.is_sorted());

        assert_ne!(m1, m2);
    }

    #[test]
    fn test_compare_sorted_dictionary_with_sorted_dictionary() {
        // create a sorted object
        let mut b = VariantBuilder::new();
        let mut o = b.new_object();

        o.insert("a", false);
        o.insert("b", false);

        o.finish();

        let (m, _) = b.finish();

        let m1 = VariantMetadata::new(&m);
        let m2 = VariantMetadata::new(&m);

        assert_eq!(m1, m2);
    }

    #[test]
    fn test_empty_string_field_names() {
        // Field names are added to the dictionary in insertion order, so this dictionary is
        // unsorted, and the empty field name makes its last two offsets equal.
        let mut b = VariantBuilder::new().with_field_names(["b", "a", ""]);
        let mut o = b.new_object();

        o.insert("b", false);
        o.insert("a", false);
        o.insert("", false);

        o.finish();

        let (m, _) = b.finish();

        let metadata = VariantMetadata::try_new(&m).unwrap();
        assert!(!metadata.is_sorted());
        assert_eq!(metadata.iter().collect::<Vec<_>>(), vec!["b", "a", ""]);
    }

    /// Builds a metadata buffer directly, so that offsets which a builder would never emit can be
    /// exercised. `values` is the raw dictionary value region.
    fn raw_metadata(is_sorted: bool, offsets: &[u8], values: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x01 | (u8::from(is_sorted) << 4)];
        bytes.push(offsets.len() as u8 - 1); // dictionary_size
        bytes.extend_from_slice(offsets);
        bytes.extend_from_slice(values);
        bytes
    }

    /// `get` skips UTF-8 revalidation once the metadata is fully validated, so full validation has
    /// to reject offsets that split a multi-byte character. Otherwise a validated instance could
    /// hand out a `&str` over a partial character.
    #[test]
    fn full_validation_rejects_offsets_splitting_a_character() {
        // "é" is two bytes, so an offset of 1 lands inside it. The value region as a whole is
        // still valid UTF-8, so only the per-entry check can catch this.
        for is_sorted in [false, true] {
            let bytes = raw_metadata(is_sorted, &[0, 1, 2], "é".as_bytes());

            // Shallow validation looks only at the first and last offset, so it accepts.
            let shallow = VariantMetadata::try_new_with_shallow_validation(&bytes).unwrap();
            assert!(!shallow.is_fully_validated());
            // ... and the fallible accessor reports the bad entry rather than panicking.
            assert!(shallow.get(0).is_err());

            // Full validation must reject the buffer outright.
            assert!(
                VariantMetadata::try_new(&bytes).is_err(),
                "is_sorted={is_sorted}: full validation accepted offsets splitting a character"
            );
        }
    }

    /// Validated and unvalidated instances must agree on every entry, including multi-byte
    /// characters and empty entries, since they take different code paths inside `get`.
    #[test]
    fn validated_and_unvalidated_get_agree() {
        // Unsorted so that the dictionary order below is preserved verbatim.
        let values = "aé€\u{10348}"; // 1, 2, 3 and 4 byte characters
        let offsets: &[u8] = &[0, 1, 1, 3, 6, 10]; // note the repeated 1: an empty entry
        let bytes = raw_metadata(false, offsets, values.as_bytes());

        let validated = VariantMetadata::try_new(&bytes).unwrap();
        assert!(validated.is_fully_validated());
        let unvalidated = VariantMetadata::try_new_with_shallow_validation(&bytes).unwrap();
        assert!(!unvalidated.is_fully_validated());

        let expected = ["a", "", "é", "€", "\u{10348}"];
        assert_eq!(validated.len(), expected.len());
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(validated.get(i).unwrap(), *want, "validated get({i})");
            assert_eq!(unvalidated.get(i).unwrap(), *want, "unvalidated get({i})");
        }

        // Out of bounds stays an error on both paths, rather than reading past the offset array.
        assert!(validated.get(expected.len()).is_err());
        assert!(unvalidated.get(expected.len()).is_err());
    }

    /// An unvalidated instance still reports invalid UTF-8 through the fallible accessor.
    #[test]
    fn unvalidated_get_still_reports_invalid_utf8() {
        let bytes = raw_metadata(false, &[0, 1], &[0xFF]);
        let unvalidated = VariantMetadata::try_new_with_shallow_validation(&bytes).unwrap();
        assert!(unvalidated.get(0).is_err());
        assert!(VariantMetadata::try_new(&bytes).is_err());
    }
}
