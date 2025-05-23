use crate::decoder::{
    self, get_basic_type, get_primitive_type, VariantBasicType, VariantPrimitiveType,
};
use crate::utils::{array_from_slice, first_byte_from_slice, invalid_utf8_err, slice_from_slice};
use arrow_schema::ArrowError;
use std::{
    num::TryFromIntError,
    ops::{Index, Range},
    str,
};

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
pub(crate) struct VariantMetadataHeader {
    version: u8,
    is_sorted: bool,
    /// Note: This is `offset_size_minus_one` + 1
    offset_size: OffsetSizeBytes,
}

// According to the spec this is currently always = 1, and so we store this const for validation
// purposes and to make that visible.
const CORRECT_VERSION_VALUE: u8 = 1;

impl<'m> VariantMetadataHeader {
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
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let Some(header) = bytes.get(0) else {
            return Err(ArrowError::InvalidArgumentError(
                "Received zero bytes".to_string(),
            ));
        };

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
        let offset_size = header.offset_size as usize; // Cheap to copy

        let dict_bytes_offset = 1usize // 1-byte header
            .checked_add(offset_size) // 1 + offset_size
            .and_then(|p| {
                dict_size
                    .checked_add(1) // dict_size + 1
                    .and_then(|n| n.checked_mul(offset_size))
                    .and_then(|table_size| p.checked_add(table_size))
            })
            .ok_or_else(|| ArrowError::InvalidArgumentError("metadata length overflow".into()))?;
        if bytes.len() < dict_bytes_offset {
            return Err(ArrowError::InvalidArgumentError(
                "Metadata shorter than dictionary_size implies".to_string(),
            ));
        }

        // Check that all offsets are monotonically increasing
        // TODO: add test for validation
        let mut prev = None;
        for (i, offset) in (0..=dict_size)
            .map(|i| header.offset_size.unpack_usize(bytes, 1, i + 1))
            .enumerate()
        {
            let offset = offset?;
            if i == 0 && offset != 0 {
                return Err(ArrowError::InvalidArgumentError(
                    "First offset is non-zero".to_string(),
                ));
            }
            if prev.is_some_and(|prev| prev >= offset) {
                return Err(ArrowError::InvalidArgumentError(
                    "Offsets are not monotonically increasing".to_string(),
                ));
            }
            prev = Some(offset);
        }

        // Check that the final offset equals the length of the
        // dictionary-string section.
        let dict_block_len = bytes.len() - dict_bytes_offset; // actual length of the string block

        if prev != Some(dict_block_len) {
            // `prev` holds the last offset seen still
            return Err(ArrowError::InvalidArgumentError(
                "Last offset does not equal dictionary length".to_string(),
            ));
        }

        Ok(Self {
            bytes,
            header,
            dict_size,
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

    /// Get the offset by key-index
    pub fn get_offset_by(&self, index: usize) -> Result<Range<usize>, ArrowError> {
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

    /// Get the key-name by index
    pub fn get_field_by(&self, index: usize) -> Result<&'m str, ArrowError> {
        let range = self.get_offset_by(index)?;
        self.get_field_by_offset(range)
    }

    /// Gets the field using an offset (Range) - helper method to keep consistent API.
    pub(crate) fn get_field_by_offset(&self, offset: Range<usize>) -> Result<&'m str, ArrowError> {
        let dictionary_key_start_byte = 1 // header
                    + self.header.offset_size as usize // dictionary_size field itself
                    + (self.dict_size + 1) * (self.header.offset_size as usize); // all offset entries
        let dictionary_keys_bytes =
            slice_from_slice(self.bytes, dictionary_key_start_byte..self.bytes.len())?;
        let dictionary_key_bytes =
            slice_from_slice(dictionary_keys_bytes, offset.start..offset.end)?;
        let result = str::from_utf8(dictionary_key_bytes).map_err(|_| invalid_utf8_err())?;
        Ok(result)
    }

    pub fn header(&self) -> VariantMetadataHeader {
        self.header
    }

    /// Get the offsets as an iterator
    // TODO: Write tests
    pub fn offsets(&self) -> impl Iterator<Item = Result<Range<usize>, ArrowError>> + 'm {
        let offset_size = self.header.offset_size; // `Copy`
        let bytes = self.bytes;

        let iterator = (0..self.dict_size).map(move |i| {
            // This wont be out of bounds as long as dict_size and offsets have been validated
            // during construction via `try_new`, as it calls unpack_usize for the
            // indices `1..dict_size+1` already.
            let start = offset_size.unpack_usize(bytes, 1, i + 1);
            let end = offset_size.unpack_usize(bytes, 1, i + 2);

            match (start, end) {
                (Ok(s), Ok(e)) => Ok(s..e),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        });

        iterator
    }

    /// Get all key-names as an Iterator of strings
    pub fn fields(
        &'m self,
    ) -> Result<impl Iterator<Item = Result<&'m str, ArrowError>>, ArrowError> {
        let iterator = self
            .offsets()
            .map(move |range_res| range_res.and_then(|r| self.get_field_by_offset(r)));
        Ok(iterator)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VariantObject<'m, 'v> {
    pub metadata: &'m VariantMetadata<'m>,
    pub value: &'v [u8],
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
    pub value: &'v [u8],
}

impl<'m, 'v> VariantArray<'m, 'v> {
    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn values(&self) -> Result<impl Iterator<Item = Variant<'m, 'v>>, ArrowError> {
        todo!();
        #[allow(unreachable_code)] // Just to infer the return type
        Ok(vec![].into_iter())
    }
}

impl<'m, 'v> Index<usize> for VariantArray<'m, 'v> {
    type Output = Variant<'m, 'v>;

    fn index(&self, _index: usize) -> &Self::Output {
        todo!()
    }
}

/// Variant value. May contain references to metadata and value
#[derive(Clone, Debug, Copy, PartialEq)]
pub enum Variant<'m, 'v> {
    // TODO: Add types for the rest of the primitive types, once API is agreed upon
    Null,
    Int8(i8),

    BooleanTrue,
    BooleanFalse,

    // Note: only need the *value* buffer
    String(&'v str),
    ShortString(&'v str),

    // need both metadata & value
    Object(VariantObject<'m, 'v>),
    Array(VariantArray<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    /// Parse the buffers and return the appropriate variant.
    pub fn try_new(metadata: &'m VariantMetadata, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = *first_byte_from_slice(value)?;
        let new_self = match get_basic_type(header)? {
            VariantBasicType::Primitive => match get_primitive_type(header)? {
                VariantPrimitiveType::Null => Variant::Null,
                VariantPrimitiveType::Int8 => Variant::Int8(decoder::decode_int8(value)?),
                VariantPrimitiveType::BooleanTrue => Variant::BooleanTrue,
                VariantPrimitiveType::BooleanFalse => Variant::BooleanFalse,
                // TODO: Add types for the rest, once API is agreed upon
                VariantPrimitiveType::String => {
                    Variant::String(decoder::decode_long_string(value)?)
                }
            },
            VariantBasicType::ShortString => {
                Variant::ShortString(decoder::decode_short_string(value)?)
            }
            VariantBasicType::Object => Variant::Object(VariantObject { metadata, value }),
            VariantBasicType::Array => Variant::Array(VariantArray { metadata, value }),
        };
        Ok(new_self)
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Variant::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Variant::BooleanTrue => Some(true),
            Variant::BooleanFalse => Some(false),
            _ => None,
        }
    }

    pub fn as_string(&'v self) -> Option<&'v str> {
        match self {
            Variant::String(s) | Variant::ShortString(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int8(&self) -> Option<i8> {
        match self {
            Variant::Int8(i) => Some(*i),
            // TODO: Add branches for type-widening/shortening when implemting rest of primitives for int
            // Variant::Int16(i) => i.try_into().ok(),
            // ...
            _ => None,
        }
    }

    pub fn metadata(&self) -> Option<&'m [u8]> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::Array(VariantArray { metadata, .. }) => Some(metadata.as_bytes()),
            _ => None,
        }
    }
}

impl<'m, 'v> From<i8> for Variant<'m, 'v> {
    fn from(value: i8) -> Self {
        Variant::Int8(value)
    }
}

impl<'m, 'v> From<bool> for Variant<'m, 'v> {
    fn from(value: bool) -> Self {
        if value {
            Variant::BooleanTrue
        } else {
            Variant::BooleanFalse
        }
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
            0x0302_01
        );
        assert_eq!(
            OffsetSizeBytes::Three
                .unpack_usize(&buf_three, 0, 1)
                .unwrap(),
            0x0000_FF
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
}
