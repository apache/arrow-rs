use std::{borrow::Cow, ops::Index};

use crate::decoder::{self, get_variant_type};
use arrow_schema::ArrowError;
use strum_macros::EnumDiscriminants;

#[derive(Clone, Copy, Debug, PartialEq)]
/// Encodes the Variant Metadata, see the Variant spec file for more information
pub struct VariantMetadata<'m> {
    bytes: &'m [u8],
}

impl<'m> VariantMetadata<'m> {
    /// View the raw bytes (needed by very low-level decoders)
    #[inline]
    pub const fn as_bytes(&self) -> &'m [u8] {
        self.bytes
    }

    /// Whether the dictionary keys are sorted and unique
    pub fn is_sorted(&self) -> bool {
        todo!()
    }

    /// Get the dict length
    pub fn dict_len(&self) -> Result<usize, ArrowError> {
        let dict_len_bytes = &self.bytes[1..self.offset_size()? as usize + 1];
        let dict_len = usize::from_le_bytes(dict_len_bytes.try_into().map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "Unable to convert dictionary_size bytes into usize: {}",
                e,
            ))
        })?);
        Ok(dict_len)
    }
    pub fn version(&self) -> usize {
        todo!()
    }

    /// Get the offset by index
    pub fn get_offset_by(&self, index: usize) -> Result<usize, ArrowError> {
        todo!()
    }

    /// Get the header byte, which has the following form
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
    pub fn header(&self) -> u8 {
        self.bytes[0]
    }

    /// Get the offset_minus_one value from the header
    pub fn offset_size_minus_one(&self) -> Result<u8, ArrowError> {
        if self.bytes.is_empty() {
            Err(ArrowError::InvalidArgumentError(
                "Tried to get offset_size_minus_one from header, but self.bytes buffer is emtpy."
                    .to_string(),
            ))
        } else {
            Ok(self.bytes[0] & (0b11 << 6)) // Grab the last 2 bits
        }
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
            total: usize,
            seen: usize,
            offset_size: usize,
        }
        impl<'m> Iterator for OffsetIterators<'m> {
            type Item = (usize, usize); // (start, end) positions of the bytes

            fn next(&mut self) -> Option<Self::Item> {
                // +1 to skip the first offset
                if self.seen < self.total {
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
            total: self.dict_len()?,
            seen: 0,
            offset_size: self.offset_size()? as usize,
        };
        Ok(iterator)
    }
    /// Get the key-name-bytes by index
    pub fn get_by(&self, index: usize) -> Result<&'m str, ArrowError> {
        todo!()
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
    pub metadata: VariantMetadata<'m>,
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
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
}

// TODO: Let's agree on the API here, also should we expose a way to get the values as a vec of
// variants for those who want it? Would require allocations.
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
// TODO: Add copy if no Cow on String and Shortstring?
#[derive(Clone, Debug, PartialEq, EnumDiscriminants)]
#[strum_discriminants(name(VariantType))]
pub enum Variant<'m, 'v> {
    // TODO: Add 'legs' for the rest of the primitive types, once API is agreed upon
    Null,
    Int8(i8),

    BooleanTrue,
    BooleanFalse,

    // only need the *value* buffer
    // TODO: Do we want Cow<'v, str> over &'v str? It enables From<String> - discuss on PR
    String(Cow<'v, str>),
    ShortString(Cow<'v, str>),

    // need both metadata & value
    Object(VariantObject<'m, 'v>),
    Array(VariantArray<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    /// Parse the buffers and return the appropriate variant.
    pub fn try_new(metadata: &'m [u8], value: &'v [u8]) -> Result<Self, ArrowError> {
        Ok(match get_variant_type(value)? {
            VariantType::Null => Variant::Null,
            VariantType::BooleanTrue => Variant::BooleanTrue,
            VariantType::BooleanFalse => Variant::BooleanFalse,

            VariantType::Int8 => Variant::Int8(decoder::decode_int8(value)?),

            // TODO: Add 'legs' for the rest of the primitive types, once API is agreed upon
            VariantType::String => {
                Variant::String(Cow::Borrowed(decoder::decode_long_string(value)?))
            }

            VariantType::ShortString => {
                Variant::ShortString(Cow::Borrowed(decoder::decode_short_string(value)?))
            }

            VariantType::Object => Variant::Object(VariantObject {
                metadata: VariantMetadata { bytes: metadata },
                value,
            }),
            VariantType::Array => Variant::Array(VariantArray {
                metadata: VariantMetadata { bytes: metadata },
                value,
            }),
        })
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
            _ => None,
        }
    }

    /// Borrow the raw metadata, if this variant has any.
    pub fn metadata(&self) -> Option<&'m [u8]> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::Array(VariantArray { metadata, .. }) => Some(metadata.as_bytes()),
            _ => None,
        }
    }

    /// Borrow the raw value bytes, if present.
    pub fn value(&'v self) -> Option<&'v [u8]> {
        match self {
            // Both arms bind `value` with the same type
            Variant::Object(VariantObject { value, .. })
            | Variant::Array(VariantArray { value, .. }) => Some(*value),

            // Short and long strings borrow from inside the slice
            Variant::String(s) | Variant::ShortString(s) => Some(s.as_bytes()),

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
            Variant::ShortString(Cow::Borrowed(value))
        } else {
            Variant::String(Cow::Borrowed(value))
        }
    }
}

impl<'m, 'v> From<String> for Variant<'m, 'v> {
    fn from(value: String) -> Self {
        if value.len() < 64 {
            Variant::ShortString(Cow::Owned(value))
        } else {
            Variant::String(Cow::Owned(value))
        }
    }
}
