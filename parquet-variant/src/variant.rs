use crate::decoder::{
    self, get_basic_type, get_primitive_type, VariantBasicType, VariantPrimitiveType,
};
use crate::utils::{array_from_slice, invalid_utf8_err, non_empty_slice, slice_from_slice};
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
            Three => todo!(), // ugh, endianness
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
        let is_sorted = (header & 0x10) != 0; // Fifth bit
        let offset_size_minus_one = (header >> 6) & 0x03; // Last two bits
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

        // TODO: Refactor, add test for validation
        let valid = (0..=dict_size)
            .map(|i| header.offset_size.unpack_usize(bytes, 1, i + 1))
            .scan(0, |prev, cur| {
                let Ok(cur_offset) = cur else {
                    return Some(false);
                };
                // Skip the first offset, which is always 0
                if *prev == 0 {
                    *prev = cur_offset;
                    return Some(true);
                }

                let valid = cur_offset > *prev;
                *prev = cur_offset;
                Some(valid)
            })
            .all(|valid| valid);

        if !valid {
            return Err(ArrowError::InvalidArgumentError(
                "Offsets are not monotonically increasing".to_string(),
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
    pub fn version(&self) -> usize {
        todo!()
    }

    /// Get the offset by key-index
    pub fn get_offset_by(&self, index: usize) -> Result<Range<usize>, ArrowError> {
        // TODO: Should we memoize the offsets? There could be thousands of them (https://github.com/apache/arrow-rs/pull/7535#discussion_r2101351294)
        if index >= self.dict_size {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index {} out of bounds for dictionary of length {}",
                index, self.dict_size
            )));
        }

        // Skipping the header byte (setting byte_offset = 1) and the dictionary_size (setting offset_index +1)
        // TODO: Validate size before looking up?
        // TODO: Fix location / bytes here, the index is wrong.
        let start = self
            .header
            .offset_size
            .unpack_usize(self.bytes, 1, index + 1)?;
        let end = self
            .header
            .offset_size
            .unpack_usize(self.bytes, 1, index + 2)?;
        Ok(start..end)
    }

    /// Get the key-name by index
    pub fn get_by(&self, index: usize) -> Result<&'m str, ArrowError> {
        match self.get_offset_by(index) {
            Ok(range) => {
                let bytes = slice_from_slice(self.bytes, 1 + range.start..1 + range.end)?;
                let result = str::from_utf8(bytes).map_err(|_| invalid_utf8_err())?;
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    pub fn header(&self) -> VariantMetadataHeader {
        self.header
    }

    // TODO: Fix this + next two
    /// Get the offset_minus_one value from the header
    pub fn offset_size_minus_one(&self) -> Result<u8, ArrowError> {
        Ok(non_empty_slice(self.bytes)?[0] & (0b11 << 6)) // Grab the last 2 bits
    }

    /// Get the offset_size
    pub fn offset_size(&self) -> Result<u8, ArrowError> {
        Ok(self.offset_size_minus_one()? + 1)
    }

    /// Get the offsets as an iterator
    // TODO: Do we want this kind of API?
    // TODO: Test once API is agreed upon
    pub fn offsets(&'m self) -> Result<impl Iterator<Item = (usize, usize)> + 'm, ArrowError> {
        struct OffsetIterators<'m> {
            buffer: &'m [u8],
            dict_len: usize,
            seen: usize,
            offset_size: usize,
        }
        impl<'m> Iterator for OffsetIterators<'m> {
            type Item = (usize, usize); // (start, end) positions of the bytes
                                        // TODO: Check bounds here or ensure they're correct
            fn next(&mut self) -> Option<Self::Item> {
                // +1 to skip the first offset
                if self.seen < self.dict_len {
                    let start = usize::from_le_bytes(
                        self.buffer[(self.seen ) * self.offset_size + 1 // +1 to skip header
                            ..(self.seen ) * self.offset_size + 1]
                            .try_into()
                            .ok()?,
                    );
                    self.seen += 1;
                    let end = usize::from_le_bytes(
                        self.buffer[(self.seen ) * self.offset_size + 1 // +1 to skip header
                            ..(self.seen ) * self.offset_size + 1]
                            .try_into()
                            .ok()?,
                    );

                    Some((start, end))
                } else {
                    None
                }
            }
        }
        let iterator: OffsetIterators = OffsetIterators {
            buffer: self.bytes,
            dict_len: self.dict_size,
            seen: 0,
            offset_size: self.offset_size()? as usize,
        };
        Ok(iterator)
    }

    /// Get all key-names as an Iterator of strings
    // TODO: Result
    pub fn fields(&self) -> impl Iterator<Item = &'m str> {
        // Do the same as for offsets
        todo!();
        vec![].into_iter()
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
        let header = non_empty_slice(value)?[0];
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
