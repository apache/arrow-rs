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
use crate::decoder::{
    self, get_basic_type, get_primitive_type, VariantBasicType, VariantPrimitiveType,
};
use crate::utils::{array_from_slice, first_byte_from_slice, slice_from_slice, string_from_slice};
use arrow_schema::ArrowError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use std::{num::TryFromIntError, ops::Range};

#[derive(Clone, Debug, Copy, PartialEq)]
enum OffsetSizeBytes {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
}

impl OffsetSizeBytes {
    /// Build from the `offset_size_minus_one` bits (see spec).
    fn try_new(offset_size_minus_one: u8) -> Result<Self, ArrowError> {
        use OffsetSizeBytes::*;
        let result = match offset_size_minus_one {
            0 => One,
            1 => Two,
            2 => Three,
            3 => Four,
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "offset_size_minus_one must be 0–3".to_string(),
                ))
            }
        };
        Ok(result)
    }

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
pub struct VariantMetadataHeader {
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
    pub fn try_new(bytes: &[u8]) -> Result<Self, ArrowError> {
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

    pub fn header(&self) -> VariantMetadataHeader {
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VariantObject<'m, 'v> {
    pub metadata: &'m VariantMetadata<'m>,
    pub value_metadata: u8,
    pub value_data: &'v [u8],
}
impl<'m, 'v> VariantObject<'m, 'v> {
    pub fn fields(&self) -> Result<impl Iterator<Item = (&'m str, Variant<'m, 'v>)>, ArrowError> {
        todo!();
        #[allow(unreachable_code)] // Just to infer the return type
        Ok(vec![].into_iter())
    }
    pub fn field(&self, _name: &'m str) -> Result<Variant<'m, 'v>, ArrowError> {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VariantArray<'m, 'v> {
    pub metadata: &'m VariantMetadata<'m>,
    pub value_metadata: u8,
    pub value_data: &'v [u8],
}

impl<'m, 'v> VariantArray<'m, 'v> {
    /// Return the length of this array
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Is the array of zero length
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn values(&self) -> Result<impl Iterator<Item = Variant<'m, 'v>>, ArrowError> {
        todo!();
        #[allow(unreachable_code)] // Just to infer the return type
        Ok(vec![].into_iter())
    }

    pub fn get(&self, index: usize) -> Result<Variant<'m, 'v>, ArrowError> {
        // The 6 first bits to the left are the value_header and the 2 bits
        // to the right are the basic type, so we shift to get only the value_header
        let value_header = self.value_metadata >> 2;
        let is_large = (value_header & 0x04) != 0; // 3rd bit from the right
        let field_offset_size_minus_one = value_header & 0x03; // Last two bits
        let offset_size = OffsetSizeBytes::try_new(field_offset_size_minus_one)?;
        // The size of the num_elements entry in the array value_data is 4 bytes if
        // is_large is true, otherwise 1 byte.
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };
        // Read the num_elements
        // The size of the num_elements entry in the array value_data is 4 bytes if
        // is_large is true, otherwise 1 byte.
        let num_elements = num_elements_size.unpack_usize(self.value_data, 0, 0)?;
        let first_offset_byte = num_elements_size as usize;

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

        // Skip num_elements bytes to read the offsets
        let start_field_offset_from_first_value_byte =
            offset_size.unpack_usize(self.value_data, first_offset_byte, index)?;
        let end_field_offset_from_first_value_byte =
            offset_size.unpack_usize(self.value_data, first_offset_byte, index + 1)?;

        // Read the value bytes from the offsets
        let variant_value_bytes = slice_from_slice(
            self.value_data,
            first_value_byte + start_field_offset_from_first_value_byte
                ..first_value_byte + end_field_offset_from_first_value_byte,
        )?;
        let variant = Variant::try_new(self.metadata, variant_value_bytes)?;
        Ok(variant)
    }
}

// impl<'m, 'v> Index<usize> for VariantArray<'m, 'v> {
//     type Output = Variant<'m, 'v>;
//
// }

/// Variant value. May contain references to metadata and value
#[derive(Clone, Debug, Copy, PartialEq)]
pub enum Variant<'m, 'v> {
    // TODO: Add types for the rest of the primitive types, once API is agreed upon
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Date(NaiveDate),
    TimestampMicros(DateTime<Utc>),
    TimestampNTZMicros(NaiveDateTime),
    Decimal4 { integer: i32, scale: u8 },
    Decimal8 { integer: i64, scale: u8 },
    Decimal16 { integer: i128, scale: u8 },
    Float(f32),
    BooleanTrue,
    BooleanFalse,

    // Note: only need the *value* buffer
    Binary(&'v [u8]),
    String(&'v str),
    ShortString(&'v str),

    // need both metadata & value
    Object(VariantObject<'m, 'v>),
    Array(VariantArray<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    /// Parse the buffers and return the appropriate variant.
    pub fn try_new(metadata: &'m VariantMetadata, value: &'v [u8]) -> Result<Self, ArrowError> {
        let value_metadata = *first_byte_from_slice(value)?;
        let value_data = slice_from_slice(value, 1..)?;
        let new_self = match get_basic_type(value_metadata)? {
            VariantBasicType::Primitive => match get_primitive_type(value_metadata)? {
                VariantPrimitiveType::Null => Variant::Null,
                VariantPrimitiveType::Int8 => Variant::Int8(decoder::decode_int8(value_data)?),
                VariantPrimitiveType::Int16 => Variant::Int16(decoder::decode_int16(value_data)?),
                VariantPrimitiveType::Int32 => Variant::Int32(decoder::decode_int32(value_data)?),
                VariantPrimitiveType::Int64 => Variant::Int64(decoder::decode_int64(value_data)?),
                VariantPrimitiveType::Decimal4 => {
                    let (integer, scale) = decoder::decode_decimal4(value_data)?;
                    Variant::Decimal4 { integer, scale }
                }
                VariantPrimitiveType::Decimal8 => {
                    let (integer, scale) = decoder::decode_decimal8(value_data)?;
                    Variant::Decimal8 { integer, scale }
                }
                VariantPrimitiveType::Decimal16 => {
                    let (integer, scale) = decoder::decode_decimal16(value_data)?;
                    Variant::Decimal16 { integer, scale }
                }
                VariantPrimitiveType::Float => Variant::Float(decoder::decode_float(value_data)?),
                VariantPrimitiveType::BooleanTrue => Variant::BooleanTrue,
                VariantPrimitiveType::BooleanFalse => Variant::BooleanFalse,
                // TODO: Add types for the rest, once API is agreed upon
                VariantPrimitiveType::Date => Variant::Date(decoder::decode_date(value_data)?),
                VariantPrimitiveType::TimestampMicros => {
                    Variant::TimestampMicros(decoder::decode_timestamp_micros(value_data)?)
                }
                VariantPrimitiveType::TimestampNTZMicros => {
                    Variant::TimestampNTZMicros(decoder::decode_timestampntz_micros(value_data)?)
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
            VariantBasicType::Object => Variant::Object(VariantObject {
                metadata,
                value_metadata,
                value_data,
            }),
            VariantBasicType::Array => Variant::Array(VariantArray {
                metadata,
                value_metadata,
                value_data,
            }),
        };
        Ok(new_self)
    }

    pub fn as_null(&self) -> Option<()> {
        matches!(self, Variant::Null).then_some(())
    }

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
            Variant::TimestampNTZMicros(d) => Some(d.and_utc()),
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
            Variant::TimestampNTZMicros(d) => Some(d),
            Variant::TimestampMicros(d) => Some(d.naive_utc()),
            _ => None,
        }
    }

    pub fn as_u8_slice(&'v self) -> Option<&'v [u8]> {
        if let Variant::Binary(d) = self {
            Some(d)
        } else {
            None
        }
    }

    pub fn as_string(&'v self) -> Option<&'v str> {
        match self {
            Variant::String(s) | Variant::ShortString(s) => Some(s),
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

    pub fn as_decimal_int32(&self) -> Option<(i32, u8)> {
        match *self {
            Variant::Decimal4 { integer, scale } => Some((integer, scale)),
            Variant::Decimal8 { integer, scale } => {
                if let Ok(converted_integer) = integer.try_into() {
                    Some((converted_integer, scale))
                } else {
                    None
                }
            }
            Variant::Decimal16 { integer, scale } => {
                if let Ok(converted_integer) = integer.try_into() {
                    Some((converted_integer, scale))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_decimal_int64(&self) -> Option<(i64, u8)> {
        match *self {
            Variant::Decimal4 { integer, scale } => Some((integer.into(), scale)),
            Variant::Decimal8 { integer, scale } => Some((integer, scale)),
            Variant::Decimal16 { integer, scale } => {
                if let Ok(converted_integer) = integer.try_into() {
                    Some((converted_integer, scale))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_decimal_int128(&self) -> Option<(i128, u8)> {
        match *self {
            Variant::Decimal4 { integer, scale } => Some((integer.into(), scale)),
            Variant::Decimal8 { integer, scale } => Some((integer.into(), scale)),
            Variant::Decimal16 { integer, scale } => Some((integer, scale)),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match *self {
            Variant::Float(i) => Some(i),
            // TODO Add Variant::Double
            // TODO Add int variants?
            // TODO Add decimal variants?
            _ => None,
        }
    }

    pub fn metadata(&self) -> Option<&'m VariantMetadata> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::Array(VariantArray { metadata, .. }) => Some(*metadata),
            _ => None,
        }
    }
}

impl<'m, 'v> From<i8> for Variant<'m, 'v> {
    fn from(value: i8) -> Self {
        Variant::Int8(value)
    }
}

impl<'m, 'v> From<i16> for Variant<'m, 'v> {
    fn from(value: i16) -> Self {
        Variant::Int16(value)
    }
}

impl<'m, 'v> From<i32> for Variant<'m, 'v> {
    fn from(value: i32) -> Self {
        Variant::Int32(value)
    }
}

impl<'m, 'v> From<i64> for Variant<'m, 'v> {
    fn from(value: i64) -> Self {
        Variant::Int64(value)
    }
}

impl<'m, 'v> From<(i32, u8)> for Variant<'m, 'v> {
    fn from(value: (i32, u8)) -> Self {
        Variant::Decimal4 {
            integer: value.0,
            scale: value.1,
        }
    }
}

impl<'m, 'v> From<(i64, u8)> for Variant<'m, 'v> {
    fn from(value: (i64, u8)) -> Self {
        Variant::Decimal8 {
            integer: value.0,
            scale: value.1,
        }
    }
}

impl<'m, 'v> From<(i128, u8)> for Variant<'m, 'v> {
    fn from(value: (i128, u8)) -> Self {
        Variant::Decimal16 {
            integer: value.0,
            scale: value.1,
        }
    }
}

impl<'m, 'v> From<f32> for Variant<'m, 'v> {
    fn from(value: f32) -> Self {
        Variant::Float(value)
    }
}

impl<'m, 'v> From<bool> for Variant<'m, 'v> {
    fn from(value: bool) -> Self {
        match value {
            true => Variant::BooleanTrue,
            false => Variant::BooleanFalse,
        }
    }
}

impl<'m, 'v> From<NaiveDate> for Variant<'m, 'v> {
    fn from(value: NaiveDate) -> Self {
        Variant::Date(value)
    }
}

impl<'m, 'v> From<DateTime<Utc>> for Variant<'m, 'v> {
    fn from(value: DateTime<Utc>) -> Self {
        Variant::TimestampMicros(value)
    }
}
impl<'m, 'v> From<NaiveDateTime> for Variant<'m, 'v> {
    fn from(value: NaiveDateTime) -> Self {
        Variant::TimestampNTZMicros(value)
    }
}

impl<'m, 'v> From<&'v [u8]> for Variant<'m, 'v> {
    fn from(value: &'v [u8]) -> Self {
        Variant::Binary(value)
    }
}

impl<'m, 'v> From<&'v str> for Variant<'m, 'v> {
    fn from(value: &'v str) -> Self {
        if value.len() < 64 {
            Variant::ShortString(value)
        } else {
            Variant::String(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset() {
        assert_eq!(OffsetSizeBytes::try_new(0).unwrap(), OffsetSizeBytes::One);
        assert_eq!(OffsetSizeBytes::try_new(1).unwrap(), OffsetSizeBytes::Two);
        assert_eq!(OffsetSizeBytes::try_new(2).unwrap(), OffsetSizeBytes::Three);
        assert_eq!(OffsetSizeBytes::try_new(3).unwrap(), OffsetSizeBytes::Four);

        // everything outside 0-3 must error
        assert!(OffsetSizeBytes::try_new(4).is_err());
        assert!(OffsetSizeBytes::try_new(255).is_err());
    }

    #[test]
    fn unpack_usize_all_widths() {
        // One-byte offsets
        let buf_one = [0x01u8, 0xAB, 0xCD];
        assert_eq!(
            OffsetSizeBytes::One.unpack_usize(&buf_one, 0, 0).unwrap(),
            0x01
        );
        assert_eq!(
            OffsetSizeBytes::One.unpack_usize(&buf_one, 0, 2).unwrap(),
            0xCD
        );

        // Two-byte offsets (little-endian 0x1234, 0x5678)
        let buf_two = [0x34, 0x12, 0x78, 0x56];
        assert_eq!(
            OffsetSizeBytes::Two.unpack_usize(&buf_two, 0, 0).unwrap(),
            0x1234
        );
        assert_eq!(
            OffsetSizeBytes::Two.unpack_usize(&buf_two, 0, 1).unwrap(),
            0x5678
        );

        // Three-byte offsets (0x030201 and 0x0000FF)
        let buf_three = [0x01, 0x02, 0x03, 0xFF, 0x00, 0x00];
        assert_eq!(
            OffsetSizeBytes::Three
                .unpack_usize(&buf_three, 0, 0)
                .unwrap(),
            0x030201
        );
        assert_eq!(
            OffsetSizeBytes::Three
                .unpack_usize(&buf_three, 0, 1)
                .unwrap(),
            0x0000FF
        );

        // Four-byte offsets (0x12345678, 0x90ABCDEF)
        let buf_four = [0x78, 0x56, 0x34, 0x12, 0xEF, 0xCD, 0xAB, 0x90];
        assert_eq!(
            OffsetSizeBytes::Four.unpack_usize(&buf_four, 0, 0).unwrap(),
            0x1234_5678
        );
        assert_eq!(
            OffsetSizeBytes::Four.unpack_usize(&buf_four, 0, 1).unwrap(),
            0x90AB_CDEF
        );
    }

    #[test]
    fn unpack_usize_out_of_bounds() {
        let tiny = [0x00u8]; // deliberately too short
        assert!(OffsetSizeBytes::Two.unpack_usize(&tiny, 0, 0).is_err());
        assert!(OffsetSizeBytes::Three.unpack_usize(&tiny, 0, 0).is_err());
    }

    #[test]
    fn unpack_simple() {
        let buf = [
            0x41, // header
            0x02, 0x00, // dictionary_size = 2
            0x00, 0x00, // offset[0] = 0
            0x05, 0x00, // offset[1] = 5
            0x09, 0x00, // offset[2] = 9
        ];

        let width = OffsetSizeBytes::Two;

        // dictionary_size starts immediately after the header
        let dict_size = width.unpack_usize(&buf, 1, 0).unwrap();
        assert_eq!(dict_size, 2);

        let first = width.unpack_usize(&buf, 1, 1).unwrap();
        assert_eq!(first, 0);

        let second = width.unpack_usize(&buf, 1, 2).unwrap();
        assert_eq!(second, 5);

        let third = width.unpack_usize(&buf, 1, 3).unwrap();
        assert_eq!(third, 9);

        let err = width.unpack_usize(&buf, 1, 4);
        assert!(err.is_err())
    }

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
        assert_eq!(md.get_field_by(0).unwrap(), "cat");
        assert_eq!(md.get_field_by(1).unwrap(), "dog");

        // Offsets
        assert_eq!(md.get_offset_by(0).unwrap(), 0x00);
        assert_eq!(md.get_offset_by(1).unwrap(), 0x03);
        // We only have 2 keys, the final offset should not be accessible using this method.
        let err = md.get_offset_by(2).unwrap_err();

        assert!(
            matches!(err, ArrowError::InvalidArgumentError(ref msg)
                     if msg.contains("Index 2 out of bounds for dictionary of length 2")),
            "unexpected error: {err:?}"
        );
        let fields: Vec<(usize, &str)> = md
            .fields()
            .unwrap()
            .enumerate()
            .map(|(i, r)| (i, r.unwrap()))
            .collect();
        assert_eq!(fields, vec![(0usize, "cat"), (1usize, "dog")]);
    }

    /// Too short buffer test (missing one required offset).
    /// Should error with “metadata shorter than dictionary_size implies”.
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
        assert_eq!(working_md.get_field_by(0).unwrap(), "a");
        assert_eq!(working_md.get_field_by(1).unwrap(), "b");

        let truncated = &bytes[..bytes.len() - 1];

        let err = VariantMetadata::try_new(truncated).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(ref msg)
                     if msg.contains("Last offset")),
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
            matches!(err, ArrowError::InvalidArgumentError(ref msg) if msg.contains("monotonically")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn try_new_truncated_offsets_inline() {
        // Missing final offset
        let bytes = &[0b0000_0001, 0x02, 0x00, 0x01];

        let err = VariantMetadata::try_new(bytes).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(ref msg) if msg.contains("shorter")),
            "unexpected error: {err:?}"
        );
    }
}
