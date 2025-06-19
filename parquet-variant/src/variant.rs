use std::ops::Deref;

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
pub use self::list::VariantList;
pub use self::metadata::VariantMetadata;
pub use self::object::VariantObject;
use crate::decoder::{
    self, get_basic_type, get_primitive_type, VariantBasicType, VariantPrimitiveType,
};
use crate::utils::{first_byte_from_slice, slice_from_slice};

use arrow_schema::ArrowError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

mod list;
mod metadata;
mod object;

const MAX_SHORT_STRING_BYTES: usize = 0x3F;

    /// Return one unsigned little-endian value from `bytes`.
    ///
    /// * `bytes` – the Variant-metadata buffer.
    /// * `byte_offset` – number of bytes to skip **before** reading the first
    ///   value (usually `1` to move past the header byte).
    /// * `offset_index` – 0-based index **after** the skip
    ///   (`0` is the first value, `1` the next, …).
    ///
    /// Each value is `self as usize` bytes wide (1, 2, 3 or 4).
    /// Three-byte values are zero-extended to 32 bits before the final
    /// fallible cast to `usize`.
    fn unpack_usize(
        &self,
        bytes: &[u8],
        byte_offset: usize,  // how many bytes to skip
        offset_index: usize, // which offset in an array of offsets
    ) -> Result<usize, ArrowError> {
        use OffsetSizeBytes::*;
        let offset = byte_offset + (*self as usize) * offset_index;
        let result = match self {
            One => u8::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Two => u16::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Three => {
                // Let's grab the three byte le-chunk first
                let b3_chunks: [u8; 3] = array_from_slice(bytes, offset)?;
                // Let's pad it and construct a padded u32 from it.
                let mut buf = [0u8; 4];
                buf[..3].copy_from_slice(&b3_chunks);
                u32::from_le_bytes(buf)
                    .try_into()
                    .map_err(|e: TryFromIntError| ArrowError::InvalidArgumentError(e.to_string()))?
            }
            Four => u32::from_le_bytes(array_from_slice(bytes, offset)?)
                .try_into()
                .map_err(|e: TryFromIntError| ArrowError::InvalidArgumentError(e.to_string()))?,
        };
        Ok(result)
    }
}

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
    ///              7     6  5   4  3             0
    ///             +-------+---+---+---------------+
    /// header      |       |   |   |    version    |
    ///             +-------+---+---+---------------+
    ///                 ^         ^
    ///                 |         +-- sorted_strings
    ///                 +-- offset_size_minus_one
    /// The version is a 4-bit value that must always contain the value 1.
    /// - sorted_strings is a 1-bit value indicating whether dictionary strings are sorted and unique.
    /// - offset_size_minus_one is a 2-bit value providing the number of bytes per dictionary size and offset field.
    /// - The actual number of bytes, offset_size, is offset_size_minus_one + 1
    pub(crate) fn try_new(bytes: &[u8]) -> Result<Self, ArrowError> {
        let header = first_byte_from_slice(bytes)?;

        let version = header & 0x0F; // First four bits
        if version != CORRECT_VERSION_VALUE {
            let err_msg = format!(
                "The version bytes in the header is not {CORRECT_VERSION_VALUE}, got {:b}",
                version
            );
            return Err(ArrowError::InvalidArgumentError(err_msg));
        }
        let is_sorted = (header & 0x10) != 0; // Fifth bit
        let offset_size_minus_one = header >> 6; // Last two bits
        Ok(Self {
            version,
            is_sorted,
            offset_size: OffsetSizeBytes::try_new(offset_size_minus_one)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// Encodes the Variant Metadata, see the Variant spec file for more information
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

    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header = VariantMetadataHeader::try_new(bytes)?;
        // Offset 1, index 0 because first element after header is dictionary size
        let dict_size = header.offset_size.unpack_usize(bytes, 1, 0)?;

        // Check that we have the correct metadata length according to dictionary_size, or return
        // error early.
        // Minimum number of bytes the metadata buffer must contain:
        // 1 byte header
        // + offset_size-byte `dictionary_size` field
        // + (dict_size + 1) offset entries, each `offset_size` bytes. (Table size, essentially)
        // 1 + offset_size + (dict_size + 1) * offset_size
        // = (dict_size + 2) * offset_size + 1
        let offset_size = header.offset_size as usize; // Cheap to copy

        let dictionary_key_start_byte = dict_size
            .checked_add(2)
            .and_then(|n| n.checked_mul(offset_size))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| ArrowError::InvalidArgumentError("metadata length overflow".into()))?;

        if bytes.len() < dictionary_key_start_byte {
            return Err(ArrowError::InvalidArgumentError(
                "Metadata shorter than dictionary_size implies".to_string(),
            ));
        }

        // Check that all offsets are monotonically increasing
        let mut offsets = (0..=dict_size).map(|i| header.offset_size.unpack_usize(bytes, 1, i + 1));
        let Some(Ok(mut end @ 0)) = offsets.next() else {
            return Err(ArrowError::InvalidArgumentError(
                "First offset is non-zero".to_string(),
            ));
        };

        for offset in offsets {
            let offset = offset?;
            if end >= offset {
                return Err(ArrowError::InvalidArgumentError(
                    "Offsets are not monotonically increasing".to_string(),
                ));
            }
            end = offset;
        }

        // Verify the buffer covers the whole dictionary-string section
        if end > bytes.len() - dictionary_key_start_byte {
            // `prev` holds the last offset seen still
            return Err(ArrowError::InvalidArgumentError(
                "Last offset does not equal dictionary length".to_string(),
            ));
        }

        Ok(Self {
            bytes,
            header,
            dict_size,
            dictionary_key_start_byte,
        })
    }

    /// Whether the dictionary keys are sorted and unique
    pub fn is_sorted(&self) -> bool {
        self.header.is_sorted
    }

    /// Get the dictionary size
    pub fn dictionary_size(&self) -> usize {
        self.dict_size
    }
    pub fn version(&self) -> u8 {
        self.header.version
    }

    /// Helper method to get the offset start and end range for a key by index.
    fn get_offsets_for_key_by(&self, index: usize) -> Result<Range<usize>, ArrowError> {
        if index >= self.dict_size {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index {} out of bounds for dictionary of length {}",
                index, self.dict_size
            )));
        }

        // Skipping the header byte (setting byte_offset = 1) and the dictionary_size (setting offset_index +1)
        let unpack = |i| self.header.offset_size.unpack_usize(self.bytes, 1, i + 1);
        Ok(unpack(index)?..unpack(index + 1)?)
    }

    /// Get a single offset by index
    pub fn get_offset_by(&self, index: usize) -> Result<usize, ArrowError> {
        if index >= self.dict_size {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index {} out of bounds for dictionary of length {}",
                index, self.dict_size
            )));
        }

        // Skipping the header byte (setting byte_offset = 1) and the dictionary_size (setting offset_index +1)
        let unpack = |i| self.header.offset_size.unpack_usize(self.bytes, 1, i + 1);
        unpack(index)
    }

    /// Get the key-name by index
    pub fn get_field_by(&self, index: usize) -> Result<&'m str, ArrowError> {
        let offset_range = self.get_offsets_for_key_by(index)?;
        self.get_field_by_offset(offset_range)
    }

    /// Gets the field using an offset (Range) - helper method to keep consistent API.
    pub(crate) fn get_field_by_offset(&self, offset: Range<usize>) -> Result<&'m str, ArrowError> {
        let dictionary_keys_bytes =
            slice_from_slice(self.bytes, self.dictionary_key_start_byte..self.bytes.len())?;
        let result = string_from_slice(dictionary_keys_bytes, offset)?;

        Ok(result)
    }

    #[allow(unused)]
    pub(crate) fn header(&self) -> VariantMetadataHeader {
        self.header
    }

    /// Get the offsets as an iterator
    pub fn offsets(&self) -> impl Iterator<Item = Result<Range<usize>, ArrowError>> + 'm {
        let offset_size = self.header.offset_size; // `Copy`
        let bytes = self.bytes;

        (0..self.dict_size).map(move |i| {
            // This wont be out of bounds as long as dict_size and offsets have been validated
            // during construction via `try_new`, as it calls unpack_usize for the
            // indices `1..dict_size+1` already.
            let start = offset_size.unpack_usize(bytes, 1, i + 1);
            let end = offset_size.unpack_usize(bytes, 1, i + 2);

            match (start, end) {
                (Ok(s), Ok(e)) => Ok(s..e),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        })
    }

    /// Get all key-names as an Iterator of strings
    pub fn fields(
        &'m self,
    ) -> Result<impl Iterator<Item = Result<&'m str, ArrowError>>, ArrowError> {
        let iterator = self
            .offsets()
            .map(move |offset_range| self.get_field_by_offset(offset_range?));
        Ok(iterator)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantObjectHeader {
    field_offset_size: OffsetSizeBytes,
    field_id_size: OffsetSizeBytes,
    num_elements: usize,
    field_ids_start_byte: usize,
    field_offsets_start_byte: usize,
    values_start_byte: usize,
}

impl VariantObjectHeader {
    pub(crate) fn try_new(value: &[u8]) -> Result<Self, ArrowError> {
        // Parse the header byte to get object parameters
        let header = first_byte_from_slice(value)?;
        let value_header = header >> 2;

        let field_offset_size_minus_one = value_header & 0x03; // Last 2 bits
        let field_id_size_minus_one = (value_header >> 2) & 0x03; // Next 2 bits
        let is_large = value_header & 0x10; // 5th bit

        let field_offset_size = OffsetSizeBytes::try_new(field_offset_size_minus_one)?;
        let field_id_size = OffsetSizeBytes::try_new(field_id_size_minus_one)?;

        // Determine num_elements size based on is_large flag
        let num_elements_size = if is_large != 0 {
            OffsetSizeBytes::Four
        } else {
            OffsetSizeBytes::One
        };

        // Parse num_elements
        let num_elements = num_elements_size.unpack_usize(value, 1, 0)?;

        // Calculate byte offsets for different sections
        let field_ids_start_byte = 1 + num_elements_size as usize;
        let field_offsets_start_byte = field_ids_start_byte + num_elements * field_id_size as usize;
        let values_start_byte =
            field_offsets_start_byte + (num_elements + 1) * field_offset_size as usize;

        // Verify that the last field offset array entry is inside the value slice
        let last_field_offset_byte =
            field_offsets_start_byte + (num_elements + 1) * field_offset_size as usize;
        if last_field_offset_byte > value.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last field offset array entry at offset {} with length {} is outside the value slice of length {}",
                last_field_offset_byte,
                field_offset_size as usize,
                value.len()
            )));
        }

        // Verify that the value of the last field offset array entry fits inside the value slice
        let last_field_offset =
            field_offset_size.unpack_usize(value, field_offsets_start_byte, num_elements)?;
        if values_start_byte + last_field_offset > value.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last field offset value {} at offset {} is outside the value slice of length {}",
                last_field_offset,
                values_start_byte,
                value.len()
            )));
        }
        Ok(Self {
            field_offset_size,
            field_id_size,
            num_elements,
            field_ids_start_byte,
            field_offsets_start_byte,
            values_start_byte,
        })
    }

    /// Returns the number of key-value pairs in this object
    pub(crate) fn num_elements(&self) -> usize {
        self.num_elements
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariantObject<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantObjectHeader,
}

impl<'m, 'v> VariantObject<'m, 'v> {
    pub fn try_new(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        Ok(Self {
            metadata,
            value,
            header: VariantObjectHeader::try_new(value)?,
        })
    }

    /// Returns the number of key-value pairs in this object
    pub fn len(&self) -> usize {
        self.header.num_elements()
    }

    /// Returns true if the object contains no key-value pairs
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn fields(&self) -> Result<impl Iterator<Item = (&'m str, Variant<'m, 'v>)>, ArrowError> {
        let field_list = self.parse_field_list()?;
        Ok(field_list.into_iter())
    }
    
    pub fn values(&self) -> Result<impl Iterator<Item = Variant<'m, 'v>>, ArrowError> {
        let fields = self.parse_field_list()?;
        let values: Vec<_> = fields.into_iter().map(|(_, variant)| variant).collect();
        Ok(values.into_iter())
    }

    pub fn field(&self, name: &str) -> Result<Option<Variant<'m, 'v>>, ArrowError> {
        // Binary search through the field IDs of this object to find the requested field name.
        //
        // NOTE: This does not require a sorted metadata dictionary, because the variant spec
        // requires object field ids to be lexically sorted by their corresponding string values,
        // and probing the dictionary for a field id is always O(1) work.
        let (field_ids, field_offsets) = self.parse_field_arrays()?;
        let search_result = try_binary_search_by(&field_ids, &name, |&field_id| {
            self.metadata.get_field_by(field_id)
        })?;

        let Ok(index) = search_result else {
            return Ok(None);
        };
        let start_offset = field_offsets[index];
        let end_offset = field_offsets[index + 1];
        let value_bytes = slice_from_slice(
            self.value,
            self.header.values_start_byte + start_offset
                ..self.header.values_start_byte + end_offset,
        )?;
        let variant = Variant::try_new_with_metadata(self.metadata, value_bytes)?;
        Ok(Some(variant))
    }

    /// Parse field IDs and field offsets arrays using the cached header
    fn parse_field_arrays(&self) -> Result<(Vec<usize>, Vec<usize>), ArrowError> {
        // Parse field IDs
        let field_ids = (0..self.header.num_elements)
            .map(|i| {
                self.header.field_id_size.unpack_usize(
                    self.value,
                    self.header.field_ids_start_byte,
                    i,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug_assert_eq!(field_ids.len(), self.header.num_elements);

        // Parse field offsets (num_elements + 1 entries)
        let field_offsets = (0..=self.header.num_elements)
            .map(|i| {
                self.header.field_offset_size.unpack_usize(
                    self.value,
                    self.header.field_offsets_start_byte,
                    i,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug_assert_eq!(field_offsets.len(), self.header.num_elements + 1);

        Ok((field_ids, field_offsets))
    }

    /// Parse all fields into a vector for iteration
    fn parse_field_list(&self) -> Result<Vec<(&'m str, Variant<'m, 'v>)>, ArrowError> {
        let (field_ids, field_offsets) = self.parse_field_arrays()?;

        let mut fields = Vec::with_capacity(self.header.num_elements);

        for i in 0..self.header.num_elements {
            let field_id = field_ids[i];
            let field_name = self.metadata.get_field_by(field_id)?;

            let start_offset = field_offsets[i];
            let value_bytes =
                slice_from_slice(self.value, self.header.values_start_byte + start_offset..)?;
            let variant = Variant::try_new_with_metadata(self.metadata, value_bytes)?;

            fields.push((field_name, variant));
        }

        Ok(fields)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantListHeader {
    offset_size: OffsetSizeBytes,
    is_large: bool,
    num_elements: usize,
    first_offset_byte: usize,
    first_value_byte: usize,
}

impl VariantListHeader {
    pub(crate) fn try_new(value: &[u8]) -> Result<Self, ArrowError> {
        // The 6 first bits to the left are the value_header and the 2 bits
        // to the right are the basic type, so we shift to get only the value_header
        let value_header = first_byte_from_slice(value)? >> 2;
        let is_large = (value_header & 0x04) != 0; // 3rd bit from the right
        let field_offset_size_minus_one = value_header & 0x03; // Last two bits
        let offset_size = OffsetSizeBytes::try_new(field_offset_size_minus_one)?;

        // The size of the num_elements entry in the array value_data is 4 bytes if
        // is_large is true, otherwise 1 byte.
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };

        // Skip the header byte to read the num_elements
        let num_elements = num_elements_size.unpack_usize(value, 1, 0)?;
        let first_offset_byte = 1 + num_elements_size as usize;

        let overflow =
            || ArrowError::InvalidArgumentError("Variant value_byte_length overflow".into());

        // 1.  num_elements + 1
        let n_offsets = num_elements.checked_add(1).ok_or_else(overflow)?;

        // 2.  (num_elements + 1) * offset_size
        let value_bytes = n_offsets
            .checked_mul(offset_size as usize)
            .ok_or_else(overflow)?;

        // 3.  first_offset_byte + ...
        let first_value_byte = first_offset_byte
            .checked_add(value_bytes)
            .ok_or_else(overflow)?;

        // Verify that the last offset array entry is inside the value slice
        let last_offset_byte = first_offset_byte + n_offsets * offset_size as usize;
        if last_offset_byte > value.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last offset array entry at offset {} with length {} is outside the value slice of length {}",
                last_offset_byte,
                offset_size as usize,
                value.len()
            )));
        }

        // Verify that the value of the last offset array entry fits inside the value slice
        let last_offset = offset_size.unpack_usize(value, first_offset_byte, num_elements)?;
        if first_value_byte + last_offset > value.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last offset value {} at offset {} is outside the value slice of length {}",
                last_offset,
                first_value_byte,
                value.len()
            )));
        }

        Ok(Self {
            offset_size,
            is_large,
            num_elements,
            first_offset_byte,
            first_value_byte,
        })
    }

    /// Returns the number of elements in this list
    pub(crate) fn num_elements(&self) -> usize {
        self.num_elements
    }

    /// Returns the offset size in bytes
    #[allow(unused)]
    pub(crate) fn offset_size(&self) -> usize {
        self.offset_size as _
    }

    /// Returns whether this is a large list
    #[allow(unused)]
    pub(crate) fn is_large(&self) -> bool {
        self.is_large
    }

    /// Returns the byte offset where the offset array starts
    pub(crate) fn first_offset_byte(&self) -> usize {
        self.first_offset_byte
    }

    /// Returns the byte offset where the values start
    pub(crate) fn first_value_byte(&self) -> usize {
        self.first_value_byte
    }
}

/// Represents a variant array.
///
/// This implementation is a zero cost wrapper over `&str` that ensures
/// the length of the underlying string is a valid Variant short string (63 bytes or less)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShortString<'a>(pub(crate) &'a str);

/// Represents a 4-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 32-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is limited to 9 digits.
///
/// For valid precision and scale values, see the Variant specification:
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal4 {
    pub(crate) integer: i32,
    pub(crate) scale: u8,
}

impl VariantDecimal4 {
    pub fn try_new(integer: i32, scale: u8) -> Result<Self, ArrowError> {
        const PRECISION_MAX: u32 = 9;

        // Validate that scale doesn't exceed precision
        if scale as u32 > PRECISION_MAX {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} cannot be greater than precision  9 for 4-byte decimal",
                scale
            )));
        }

        Ok(VariantDecimal4 { integer, scale })
    }
}

/// Represents an 8-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 64-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 10 and 18 digits.
///
/// For valid precision and scale values, see the Variant specification:
///
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal8 {
    pub(crate) integer: i64,
    pub(crate) scale: u8,
}

impl VariantDecimal8 {
    pub fn try_new(integer: i64, scale: u8) -> Result<Self, ArrowError> {
        const PRECISION_MAX: u32 = 18;

        // Validate that scale doesn't exceed precision
        if scale as u32 > PRECISION_MAX {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} cannot be greater than precision  18 for 8-byte decimal",
                scale
            )));
        }

        Ok(VariantDecimal8 { integer, scale })
    }
}

/// Represents an 16-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 128-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 19 and 38 digits.
///
/// For valid precision and scale values, see the Variant specification:
///
/// <https://github.com/apache/parquet-format/blob/87f2c8bf77eefb4c43d0ebaeea1778bd28ac3609/VariantEncoding.md?plain=1#L418-L420>
///
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal16 {
    pub(crate) integer: i128,
    pub(crate) scale: u8,
}

impl VariantDecimal16 {
    pub fn try_new(integer: i128, scale: u8) -> Result<Self, ArrowError> {
        const PRECISION_MAX: u32 = 38;

        // Validate that scale doesn't exceed precision
        if scale as u32 > PRECISION_MAX {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Scale {} cannot be greater than precision 38 for 16-byte decimal",
                scale
            )));
        }

        Ok(VariantDecimal16 { integer, scale })
    }
}

impl<'a> ShortString<'a> {
    /// Attempts to interpret `value` as a variant short string value.
    ///
    /// # Validation
    ///
    /// This constructor verifies that `value` is shorter than or equal to `MAX_SHORT_STRING_BYTES`
    pub fn try_new(value: &'a str) -> Result<Self, ArrowError> {
        if value.len() > MAX_SHORT_STRING_BYTES {
            return Err(ArrowError::InvalidArgumentError(format!(
                "value is larger than {MAX_SHORT_STRING_BYTES} bytes"
            )));
        }

        Ok(Self(value))
    }

    /// Returns the underlying Variant short string as a &str
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

impl<'a> From<ShortString<'a>> for &'a str {
    fn from(value: ShortString<'a>) -> Self {
        value.0
    }
}

impl<'a> TryFrom<&'a str> for ShortString<'a> {
    type Error = ArrowError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl AsRef<str> for ShortString<'_> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Deref for ShortString<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

/// Represents a [Parquet Variant]
///
/// The lifetimes `'m` and `'v` are for metadata and value buffers, respectively.
///
/// # Background
///
/// The [specification] says:
///
/// The Variant Binary Encoding allows representation of semi-structured data
/// (e.g. JSON) in a form that can be efficiently queried by path. The design is
/// intended to allow efficient access to nested data even in the presence of
/// very wide or deep structures.
///
/// Another motivation for the representation is that (aside from metadata) each
/// nested Variant value is contiguous and self-contained. For example, in a
/// Variant containing an Array of Variant values, the representation of an
/// inner Variant value, when paired with the metadata of the full variant, is
/// itself a valid Variant.
///
/// When stored in Parquet files, Variant fields can also be *shredded*. Shredding
/// refers to extracting some elements of the variant into separate columns for
/// more efficient extraction/filter pushdown. The [Variant Shredding
/// specification] describes the details of shredding Variant values as typed
/// Parquet columns.
///
/// A Variant represents a type that contains one of:
///
/// * Primitive: A type and corresponding value (e.g. INT, STRING)
///
/// * Array: An ordered list of Variant values
///
/// * Object: An unordered collection of string/Variant pairs (i.e. key/value
///   pairs). An object may not contain duplicate keys.
///
/// # Encoding
///
/// A Variant is encoded with 2 binary values, the value and the metadata. The
/// metadata stores a header and an optional dictionary of field names which are
/// referred to by offset in the value. The value is a binary representation of
/// the actual data, and varies depending on the type.
///
/// # Design Goals
///
/// The design goals of the Rust API are as follows:
/// 1. Speed / Zero copy access (no `clone`ing is required)
/// 2. Safety
/// 3. Follow standard Rust conventions
///
/// [Parquet Variant]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
/// [specification]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
/// [Variant Shredding specification]: https://github.com/apache/parquet-format/blob/master/VariantShredding.md
///
/// # Examples:
///
/// ## Creating `Variant` from Rust Types
/// ```
/// use parquet_variant::Variant;
/// // variants can be directly constructed
/// let variant = Variant::Int32(123);
/// // or constructed via `From` impls
/// assert_eq!(variant, Variant::from(123i32));
/// ```
/// ## Creating `Variant` from metadata and value
/// ```
/// # use parquet_variant::{Variant, VariantMetadata};
/// let metadata = [0x01, 0x00, 0x00];
/// let value = [0x09, 0x48, 0x49];
/// // parse the header metadata
/// assert_eq!(
///   Variant::from("HI"),
///   Variant::try_new(&metadata, &value).unwrap()
/// );
/// ```
///
/// ## Using `Variant` values
/// ```
/// # use parquet_variant::Variant;
/// # let variant = Variant::Int32(123);
/// // variants can be used in match statements like normal enums
/// match variant {
///   Variant::Int32(i) => println!("Integer: {}", i),
///   Variant::String(s) => println!("String: {}", s),
///   _ => println!("Other variant"),
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Variant<'m, 'v> {
    /// Primitive type: Null
    Null,
    /// Primitive (type_id=1): INT(8, SIGNED)
    Int8(i8),
    /// Primitive (type_id=1): INT(16, SIGNED)
    Int16(i16),
    /// Primitive (type_id=1): INT(32, SIGNED)
    Int32(i32),
    /// Primitive (type_id=1): INT(64, SIGNED)
    Int64(i64),
    /// Primitive (type_id=1): DATE
    Date(NaiveDate),
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=true, MICROS)
    TimestampMicros(DateTime<Utc>),
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=false, MICROS)
    TimestampNtzMicros(NaiveDateTime),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 32-bits
    Decimal4(VariantDecimal4),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 64-bits
    Decimal8(VariantDecimal8),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 128-bits
    Decimal16(VariantDecimal16),
    /// Primitive (type_id=1): FLOAT
    Float(f32),
    /// Primitive (type_id=1): DOUBLE
    Double(f64),
    /// Primitive (type_id=1): BOOLEAN (true)
    BooleanTrue,
    /// Primitive (type_id=1): BOOLEAN (false)
    BooleanFalse,
    // Note: only need the *value* buffer for these types
    /// Primitive (type_id=1): BINARY
    Binary(&'v [u8]),
    /// Primitive (type_id=1): STRING
    String(&'v str),
    /// Short String (type_id=2): STRING
    ShortString(ShortString<'v>),
    // need both metadata & value
    /// Object (type_id=3): N/A
    Object(VariantObject<'m, 'v>),
    /// Array (type_id=4): N/A
    List(VariantList<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    /// Create a new `Variant` from metadata and value.
    ///
    /// # Example
    /// ```
    /// use parquet_variant::{Variant, VariantMetadata};
    /// let metadata = [0x01, 0x00, 0x00];
    /// let value = [0x09, 0x48, 0x49];
    /// // parse the header metadata
    /// assert_eq!(
    ///   Variant::from("HI"),
    ///   Variant::try_new(&metadata, &value).unwrap()
    /// );
    /// ```
    pub fn try_new(metadata: &'m [u8], value: &'v [u8]) -> Result<Self, ArrowError> {
        let metadata = VariantMetadata::try_new(metadata)?;
        Self::try_new_with_metadata(metadata, value)
    }

    /// Create a new variant with existing metadata
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantMetadata};
    /// let metadata = [0x01, 0x00, 0x00];
    /// let value = [0x09, 0x48, 0x49];
    /// // parse the header metadata first
    /// let metadata = VariantMetadata::try_new(&metadata).unwrap();
    /// assert_eq!(
    ///   Variant::from("HI"),
    ///   Variant::try_new_with_metadata(metadata, &value).unwrap()
    /// );
    /// ```
    pub fn try_new_with_metadata(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let value_metadata = first_byte_from_slice(value)?;
        let value_data = slice_from_slice(value, 1..)?;
        let new_self = match get_basic_type(value_metadata) {
            VariantBasicType::Primitive => match get_primitive_type(value_metadata)? {
                VariantPrimitiveType::Null => Variant::Null,
                VariantPrimitiveType::Int8 => Variant::Int8(decoder::decode_int8(value_data)?),
                VariantPrimitiveType::Int16 => Variant::Int16(decoder::decode_int16(value_data)?),
                VariantPrimitiveType::Int32 => Variant::Int32(decoder::decode_int32(value_data)?),
                VariantPrimitiveType::Int64 => Variant::Int64(decoder::decode_int64(value_data)?),
                VariantPrimitiveType::Decimal4 => {
                    let (integer, scale) = decoder::decode_decimal4(value_data)?;
                    Variant::Decimal4(VariantDecimal4 { integer, scale })
                }
                VariantPrimitiveType::Decimal8 => {
                    let (integer, scale) = decoder::decode_decimal8(value_data)?;
                    Variant::Decimal8(VariantDecimal8 { integer, scale })
                }
                VariantPrimitiveType::Decimal16 => {
                    let (integer, scale) = decoder::decode_decimal16(value_data)?;
                    Variant::Decimal16(VariantDecimal16 { integer, scale })
                }
                VariantPrimitiveType::Float => Variant::Float(decoder::decode_float(value_data)?),
                VariantPrimitiveType::Double => {
                    Variant::Double(decoder::decode_double(value_data)?)
                }
                VariantPrimitiveType::BooleanTrue => Variant::BooleanTrue,
                VariantPrimitiveType::BooleanFalse => Variant::BooleanFalse,
                VariantPrimitiveType::Date => Variant::Date(decoder::decode_date(value_data)?),
                VariantPrimitiveType::TimestampMicros => {
                    Variant::TimestampMicros(decoder::decode_timestamp_micros(value_data)?)
                }
                VariantPrimitiveType::TimestampNtzMicros => {
                    Variant::TimestampNtzMicros(decoder::decode_timestampntz_micros(value_data)?)
                }
                VariantPrimitiveType::Binary => {
                    Variant::Binary(decoder::decode_binary(value_data)?)
                }
                VariantPrimitiveType::String => {
                    Variant::String(decoder::decode_long_string(value_data)?)
                }
            },
            VariantBasicType::ShortString => {
                Variant::ShortString(decoder::decode_short_string(value_metadata, value_data)?)
            }
            VariantBasicType::Object => Variant::Object(VariantObject::try_new(metadata, value)?),
            VariantBasicType::Array => Variant::List(VariantList::try_new(metadata, value)?),
        };
        Ok(new_self)
    }

    /// Converts this variant to `()` if it is null.
    ///
    /// Returns `Some(())` for null variants,
    /// `None` for non-null variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract `()` from a null variant
    /// let v1 = Variant::from(());
    /// assert_eq!(v1.as_null(), Some(()));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_null(), None);
    /// ```
    pub fn as_null(&self) -> Option<()> {
        matches!(self, Variant::Null).then_some(())
    }

    /// Converts this variant to a `bool` if possible.
    ///
    /// Returns `Some(bool)` for boolean variants,
    /// `None` for non-boolean variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a bool from the true variant
    /// let v1 = Variant::from(true);
    /// assert_eq!(v1.as_boolean(), Some(true));
    ///
    /// // and the false variant
    /// let v2 = Variant::from(false);
    /// assert_eq!(v2.as_boolean(), Some(false));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_boolean(), None);
    /// ```
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Variant::BooleanTrue => Some(true),
            Variant::BooleanFalse => Some(false),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDate` if possible.
    ///
    /// Returns `Some(NaiveDate)` for date variants,
    /// `None` for non-date variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDate from a date variant
    /// let date = NaiveDate::from_ymd_opt(2025, 4, 12).unwrap();
    /// let v1 = Variant::from(date);
    /// assert_eq!(v1.as_naive_date(), Some(date));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_naive_date(), None);
    /// ```
    pub fn as_naive_date(&self) -> Option<NaiveDate> {
        if let Variant::Date(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// Converts this variant to a `DateTime<Utc>` if possible.
    ///
    /// Returns `Some(DateTime<Utc>)` for timestamp variants,
    /// `None` for non-timestamp variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a DateTime<Utc> from a UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap().and_utc();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_datetime_utc(), Some(datetime));
    ///
    /// // or a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap();
    /// let v2 = Variant::from(datetime);
    /// assert_eq!(v2.as_datetime_utc(), Some(datetime.and_utc()));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_datetime_utc(), None);
    /// ```
    pub fn as_datetime_utc(&self) -> Option<DateTime<Utc>> {
        match *self {
            Variant::TimestampMicros(d) => Some(d),
            Variant::TimestampNtzMicros(d) => Some(d.and_utc()),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDateTime` if possible.
    ///
    /// Returns `Some(NaiveDateTime)` for timestamp variants,
    /// `None` for non-timestamp variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDateTime from a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_naive_datetime(), Some(datetime));
    ///
    /// // or a UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap().and_utc();
    /// let v2 = Variant::from(datetime);
    /// assert_eq!(v2.as_naive_datetime(), Some(datetime.naive_utc()));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_naive_datetime(), None);
    /// ```
    pub fn as_naive_datetime(&self) -> Option<NaiveDateTime> {
        match *self {
            Variant::TimestampNtzMicros(d) => Some(d),
            Variant::TimestampMicros(d) => Some(d.naive_utc()),
            _ => None,
        }
    }

    /// Converts this variant to a `&[u8]` if possible.
    ///
    /// Returns `Some(&[u8])` for binary variants,
    /// `None` for non-binary variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a byte slice from a binary variant
    /// let data = b"hello!";
    /// let v1 = Variant::Binary(data);
    /// assert_eq!(v1.as_u8_slice(), Some(data.as_slice()));
    ///
    /// // but not from other variant types
    /// let v2 = Variant::from(123i64);
    /// assert_eq!(v2.as_u8_slice(), None);
    /// ```
    pub fn as_u8_slice(&'v self) -> Option<&'v [u8]> {
        if let Variant::Binary(d) = self {
            Some(d)
        } else {
            None
        }
    }

    /// Converts this variant to a `&str` if possible.
    ///
    /// Returns `Some(&str)` for string variants (both regular and short strings),
    /// `None` for non-string variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a string from string variants
    /// let s = "hello!";
    /// let v1 = Variant::from(s);
    /// assert_eq!(v1.as_string(), Some(s));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from(123i64);
    /// assert_eq!(v2.as_string(), None);
    /// ```
    pub fn as_string(&'v self) -> Option<&'v str> {
        match self {
            Variant::String(s) | Variant::ShortString(ShortString(s)) => Some(s),
            _ => None,
        }
    }

    /// Converts this variant to an `i8` if possible.
    ///
    /// Returns `Some(i8)` for integer variants that fit in `i8` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i8 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int8(), Some(123i8));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(1234i64);
    /// assert_eq!(v2.as_int8(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int8(), None);
    /// ```
    pub fn as_int8(&self) -> Option<i8> {
        match *self {
            Variant::Int8(i) => Some(i),
            Variant::Int16(i) => i.try_into().ok(),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i16` if possible.
    ///
    /// Returns `Some(i16)` for integer variants that fit in `i16` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i16 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int16(), Some(123i16));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(123456i64);
    /// assert_eq!(v2.as_int16(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int16(), None);
    /// ```
    pub fn as_int16(&self) -> Option<i16> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i32` if possible.
    ///
    /// Returns `Some(i32)` for integer variants that fit in `i32` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i32 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int32(), Some(123i32));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(12345678901i64);
    /// assert_eq!(v2.as_int32(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int32(), None);
    /// ```
    pub fn as_int32(&self) -> Option<i32> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) => Some(i),
            Variant::Int64(i) => i.try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i64` if possible.
    ///
    /// Returns `Some(i64)` for integer variants that fit in `i64` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i64
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int64(), Some(123i64));
    ///
    /// // but not a variant that cannot be cast into an integer
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_int64(), None);
    /// ```
    pub fn as_int64(&self) -> Option<i64> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) => Some(i.into()),
            Variant::Int64(i) => Some(i),
            _ => None,
        }
    }

    /// Converts this variant to tuple with a 4-byte unscaled value if possible.
    ///
    /// Returns `Some((i32, u8))` for decimal variants where the unscaled value
    /// fits in `i32` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal4::try_new(1234_i32, 2).unwrap());
    /// assert_eq!(v1.as_decimal_int32(), Some((1234_i32, 2)));
    ///
    /// // and from larger decimal variants if they fit
    /// let v2 = Variant::from(VariantDecimal8::try_new(1234_i64, 2).unwrap());
    /// assert_eq!(v2.as_decimal_int32(), Some((1234_i32, 2)));
    ///
    /// // but not if the value would overflow i32
    /// let v3 = Variant::from(VariantDecimal8::try_new(12345678901i64, 2).unwrap());
    /// assert_eq!(v3.as_decimal_int32(), None);
    ///
    /// // or if the variant is not a decimal
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_decimal_int32(), None);
    /// ```
    pub fn as_decimal_int32(&self) -> Option<(i32, u8)> {
        match *self {
            Variant::Decimal4(decimal4) => Some((decimal4.integer, decimal4.scale)),
            Variant::Decimal8(decimal8) => {
                if let Ok(converted_integer) = decimal8.integer.try_into() {
                    Some((converted_integer, decimal8.scale))
                } else {
                    None
                }
            }
            Variant::Decimal16(decimal16) => {
                if let Ok(converted_integer) = decimal16.integer.try_into() {
                    Some((converted_integer, decimal16.scale))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Converts this variant to tuple with an 8-byte unscaled value if possible.
    ///
    /// Returns `Some((i64, u8))` for decimal variants where the unscaled value
    /// fits in `i64` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal8, VariantDecimal16};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal8::try_new(1234_i64, 2).unwrap());
    /// assert_eq!(v1.as_decimal_int64(), Some((1234_i64, 2)));
    ///
    /// // and from larger decimal variants if they fit
    /// let v2 = Variant::from(VariantDecimal16::try_new(1234_i128, 2).unwrap());
    /// assert_eq!(v2.as_decimal_int64(), Some((1234_i64, 2)));
    ///
    /// // but not if the value would overflow i64
    /// let v3 = Variant::from(VariantDecimal16::try_new(2e19 as i128, 2).unwrap());
    /// assert_eq!(v3.as_decimal_int64(), None);
    ///
    /// // or if the variant is not a decimal
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_decimal_int64(), None);
    /// ```
    pub fn as_decimal_int64(&self) -> Option<(i64, u8)> {
        match *self {
            Variant::Decimal4(decimal) => Some((decimal.integer.into(), decimal.scale)),
            Variant::Decimal8(decimal) => Some((decimal.integer, decimal.scale)),
            Variant::Decimal16(decimal) => {
                if let Ok(converted_integer) = decimal.integer.try_into() {
                    Some((converted_integer, decimal.scale))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Converts this variant to tuple with a 16-byte unscaled value if possible.
    ///
    /// Returns `Some((i128, u8))` for decimal variants where the unscaled value
    /// fits in `i128` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal16};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal16::try_new(1234_i128, 2).unwrap());
    /// assert_eq!(v1.as_decimal_int128(), Some((1234_i128, 2)));
    ///
    /// // but not if the variant is not a decimal
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_decimal_int128(), None);
    /// ```
    pub fn as_decimal_int128(&self) -> Option<(i128, u8)> {
        match *self {
            Variant::Decimal4(decimal) => Some((decimal.integer.into(), decimal.scale)),
            Variant::Decimal8(decimal) => Some((decimal.integer.into(), decimal.scale)),
            Variant::Decimal16(decimal) => Some((decimal.integer, decimal.scale)),
            _ => None,
        }
    }
    /// Converts this variant to an `f32` if possible.
    ///
    /// Returns `Some(f32)` for float and double variants,
    /// `None` for non-floating-point variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract an f32 from a float variant
    /// let v1 = Variant::from(std::f32::consts::PI);
    /// assert_eq!(v1.as_f32(), Some(std::f32::consts::PI));
    ///
    /// // and from a double variant (with loss of precision to nearest f32)
    /// let v2 = Variant::from(std::f64::consts::PI);
    /// assert_eq!(v2.as_f32(), Some(std::f32::consts::PI));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_f32(), None);
    /// ```
    #[allow(clippy::cast_possible_truncation)]
    pub fn as_f32(&self) -> Option<f32> {
        match *self {
            Variant::Float(i) => Some(i),
            Variant::Double(i) => Some(i as f32),
            _ => None,
        }
    }

    /// Converts this variant to an `f64` if possible.
    ///
    /// Returns `Some(f64)` for float and double variants,
    /// `None` for non-floating-point variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract an f64 from a float variant
    /// let v1 = Variant::from(std::f32::consts::PI);
    /// assert_eq!(v1.as_f64(), Some(std::f32::consts::PI as f64));
    ///
    /// // and from a double variant
    /// let v2 = Variant::from(std::f64::consts::PI);
    /// assert_eq!(v2.as_f64(), Some(std::f64::consts::PI));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_f64(), None);
    /// ```
    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Variant::Float(i) => Some(i.into()),
            Variant::Double(i) => Some(i),
            _ => None,
        }
    }

    /// Converts this variant to an `Object` if it is an [`VariantObject`].
    ///
    /// Returns `Some(&VariantObject)` for object variants,
    /// `None` for non-object variants.
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantObject};
    /// # let (metadata, value) = {
    /// # let mut builder = VariantBuilder::new();
    /// #   let mut obj = builder.new_object();
    /// #   obj.append_value("name", "John");
    /// #   obj.finish();
    /// #   builder.finish()
    /// # };
    /// // object that is {"name": "John"}
    ///  let variant = Variant::try_new(&metadata, &value).unwrap();
    /// // use the `as_object` method to access the object
    /// let obj = variant.as_object().expect("variant should be an object");
    /// assert_eq!(obj.field_by_name("name").unwrap(), Some(Variant::from("John")));
    /// ```
    pub fn as_object(&'m self) -> Option<&'m VariantObject<'m, 'v>> {
        if let Variant::Object(obj) = self {
            Some(obj)
        } else {
            None
        }
    }

    /// Converts this variant to a `List` if it is a [`VariantList`].
    ///
    /// Returns `Some(&VariantList)` for list variants,
    /// `None` for non-list variants.
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantList};
    /// # let (metadata, value) = {
    /// # let mut builder = VariantBuilder::new();
    /// #   let mut list = builder.new_list();
    /// #   list.append_value("John");
    /// #   list.append_value("Doe");
    /// #   list.finish();
    /// #   builder.finish()
    /// # };
    /// // list that is ["John", "Doe"]
    /// let variant = Variant::try_new(&metadata, &value).unwrap();
    /// // use the `as_list` method to access the list
    /// let list = variant.as_list().expect("variant should be a list");
    /// assert_eq!(list.len(), 2);
    /// assert_eq!(list.get(0).unwrap(), Variant::from("John"));
    /// assert_eq!(list.get(1).unwrap(), Variant::from("Doe"));
    /// ```
    pub fn as_list(&'m self) -> Option<&'m VariantList<'m, 'v>> {
        if let Variant::List(list) = self {
            Some(list)
        } else {
            None
        }
    }

    /// Return the metadata associated with this variant, if any.
    ///
    /// Returns `Some(&VariantMetadata)` for object and list variants,
    pub fn metadata(&self) -> Option<&'m VariantMetadata> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::List(VariantList { metadata, .. }) => Some(metadata),
            _ => None,
        }
    }
}

impl From<()> for Variant<'_, '_> {
    fn from((): ()) -> Self {
        Variant::Null
    }
}

impl From<i8> for Variant<'_, '_> {
    fn from(value: i8) -> Self {
        Variant::Int8(value)
    }
}

impl From<i16> for Variant<'_, '_> {
    fn from(value: i16) -> Self {
        Variant::Int16(value)
    }
}

impl From<i32> for Variant<'_, '_> {
    fn from(value: i32) -> Self {
        Variant::Int32(value)
    }
}

impl From<i64> for Variant<'_, '_> {
    fn from(value: i64) -> Self {
        Variant::Int64(value)
    }
}

impl From<VariantDecimal4> for Variant<'_, '_> {
    fn from(value: VariantDecimal4) -> Self {
        Variant::Decimal4(value)
    }
}
impl From<bool> for Variant<'_, '_> {
    fn from(value: bool) -> Self {
        match value {
            true => Variant::BooleanTrue,
            false => Variant::BooleanFalse,
        }
    }
}

impl From<VariantDecimal8> for Variant<'_, '_> {
    fn from(value: VariantDecimal8) -> Self {
        Variant::Decimal8(value)
    }
}

impl From<VariantDecimal16> for Variant<'_, '_> {
    fn from(value: VariantDecimal16) -> Self {
        Variant::Decimal16(value)
    }
}

impl From<f32> for Variant<'_, '_> {
    fn from(value: f32) -> Self {
        Variant::Float(value)
    }
}

impl From<f64> for Variant<'_, '_> {
    fn from(value: f64) -> Self {
        Variant::Double(value)
    }
}

impl From<NaiveDate> for Variant<'_, '_> {
    fn from(value: NaiveDate) -> Self {
        Variant::Date(value)
    }
}

impl From<DateTime<Utc>> for Variant<'_, '_> {
    fn from(value: DateTime<Utc>) -> Self {
        Variant::TimestampMicros(value)
    }
}
impl From<NaiveDateTime> for Variant<'_, '_> {
    fn from(value: NaiveDateTime) -> Self {
        Variant::TimestampNtzMicros(value)
    }
}

impl<'v> From<&'v [u8]> for Variant<'_, 'v> {
    fn from(value: &'v [u8]) -> Self {
        Variant::Binary(value)
    }
}

impl<'v> From<&'v str> for Variant<'_, 'v> {
    fn from(value: &'v str) -> Self {
        if value.len() > MAX_SHORT_STRING_BYTES {
            Variant::String(value)
        } else {
            Variant::ShortString(ShortString(value))
        }
    }
}

impl TryFrom<(i32, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i32, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal4(VariantDecimal4::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i64, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i64, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal8(VariantDecimal8::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i128, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i128, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal16(VariantDecimal16::try_new(
            value.0, value.1,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_short_string() {
        let short_string = ShortString::try_new("norm").expect("should fit in short string");
        assert_eq!(short_string.as_str(), "norm");

        let long_string = "a".repeat(MAX_SHORT_STRING_BYTES + 1);
        let res = ShortString::try_new(&long_string);
        assert!(res.is_err());
    }

    #[test]
    fn test_variant_decimal_conversion() {
        let decimal4 = VariantDecimal4::try_new(1234_i32, 2).unwrap();
        let variant = Variant::from(decimal4);
        assert_eq!(variant.as_decimal_int32(), Some((1234_i32, 2)));

        let decimal8 = VariantDecimal8::try_new(12345678901_i64, 2).unwrap();
        let variant = Variant::from(decimal8);
        assert_eq!(variant.as_decimal_int64(), Some((12345678901_i64, 2)));

        let decimal16 = VariantDecimal16::try_new(123456789012345678901234567890_i128, 2).unwrap();
        let variant = Variant::from(decimal16);
        assert_eq!(
            variant.as_decimal_int128(),
            Some((123456789012345678901234567890_i128, 2))
        );
    }

    #[test]
    fn test_invalid_variant_decimal_conversion() {
        let decimal4 = VariantDecimal4::try_new(123456789_i32, 20);
        assert!(decimal4.is_err(), "i32 overflow should fail");
    }
}
