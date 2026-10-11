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

//! Views over encoded variant bytes that read only what they use.
//!
//! Building a view parses one header and nothing else. Each accessor reads and bounds-checks only
//! the bytes it needs, so malformed input produces an error, never a panic. A view does not check
//! UTF-8, sort order, or any offset it does not read. Field names are compared as bytes, which
//! orders them exactly as `str` does.
//!
//! The views are the weakest state of a variant. The [`Variant`], [`VariantMetadata`],
//! [`VariantObject`] and [`VariantList`] types are the stronger states:
//!
//! - [`ValueView::to_variant`], [`MetadataView::into_metadata`], [`ObjectView::into_object`] and
//!   [`ListView::into_list`] check the first and last offsets, which bound the encoding. Those
//!   checks are the ones that unvalidated instances of the public types guarantee.
//! - The `with_full_validation` methods of the public types check everything else.
//!
//! A child value starts at its offset and extends to the end of its parent, because object field
//! values need not be stored in field order. [`ValueView::bytes`] finds the exact encoding of a
//! value when a caller needs it.

use arrow_schema::ArrowError;

pub use crate::decoder::VariantBasicType;

use crate::decoder::{VariantPrimitiveType, get_basic_type};
use crate::path::VariantPathElement;
use crate::utils::{
    array_from_slice, first_byte_from_slice, overflow_error, slice_from_slice,
    slice_from_slice_at_offset, str_from_bytes, try_binary_search,
};
use crate::variant::list::VariantListHeader;
use crate::variant::metadata::VariantMetadataHeader;
use crate::variant::object::VariantObjectHeader;
use crate::variant::{
    EMPTY_VARIANT_METADATA, Variant, VariantList, VariantMetadata, VariantObject,
};

/// The size of the header byte of a metadata dictionary, object or list.
const NUM_HEADER_BYTES: u32 = 1;

#[inline]
fn checked(value: Option<u32>, what: &str) -> Result<u32, ArrowError> {
    value.ok_or_else(|| overflow_error(what))
}

fn index_error(kind: &str, index: usize, len: usize) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "{kind} index {index} is out of bounds for length {len}"
    ))
}

/// A metadata dictionary of which only the header has been read.
#[derive(Debug, Clone, Copy)]
pub struct MetadataView<'m> {
    pub(crate) bytes: &'m [u8],
    pub(crate) header: VariantMetadataHeader,
    pub(crate) dictionary_size: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m> MetadataView<'m> {
    /// Reads the header and the dictionary size of the metadata dictionary in `bytes`.
    #[inline]
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header = VariantMetadataHeader::try_new(first_byte_from_slice(bytes)?)?;
        let dictionary_size =
            header
                .offset_size
                .unpack_u32_at_offset(bytes, NUM_HEADER_BYTES as _, 0)?;

        // There are dictionary_size + 1 offsets, and the string bytes immediately follow them.
        let first_value_byte = checked(
            dictionary_size
                .checked_add(1)
                .and_then(|n| n.checked_mul(header.offset_size()))
                .and_then(|n| n.checked_add(header.first_offset_byte())),
            "offset of variant metadata dictionary",
        )?;

        Ok(Self {
            bytes,
            header,
            dictionary_size,
            first_value_byte,
        })
    }

    /// The number of entries in the dictionary.
    #[inline]
    pub fn len(&self) -> usize {
        self.dictionary_size as _
    }

    /// True if the dictionary has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.dictionary_size == 0
    }

    /// True if the header declares the entries unique and sorted.
    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.header.is_sorted
    }

    /// The bytes of entry `id`, without UTF-8 validation.
    #[inline]
    pub fn entry(&self, id: usize) -> Result<&'m [u8], ArrowError> {
        if id >= self.len() {
            return Err(index_error("Metadata dictionary", id, self.len()));
        }
        self.read_entry(id)
    }

    /// The entry `id` as a string.
    pub fn entry_str(&self, id: usize) -> Result<&'m str, ArrowError> {
        str_from_bytes(self.entry(id)?)
    }

    /// The id of the entry equal to `name`, if any.
    ///
    /// The search is a binary search if the dictionary is sorted and has more than a few entries,
    /// and a linear scan otherwise.
    pub fn find(&self, name: &[u8]) -> Result<Option<u32>, ArrowError> {
        if self.is_sorted() && self.len() > 10 {
            let found = try_binary_search(self.len(), |id| Ok(self.read_entry(id)?.cmp(name)))?;
            return Ok(found.map(|id| id as u32));
        }
        for id in 0..self.len() {
            if self.read_entry(id)? == name {
                return Ok(Some(id as u32));
            }
        }
        Ok(None)
    }

    /// Checks the first and last offsets, and returns the bounded [`VariantMetadata`].
    #[inline]
    pub fn into_metadata(self) -> Result<VariantMetadata<'m>, ArrowError> {
        VariantMetadata::try_from_view(self)
    }

    /// Entry `i` of the offset array, for `i` in `0..=self.len()`.
    #[inline]
    pub(crate) fn offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.offset_size.unpack_u32_at_offset(
            self.bytes,
            self.header.first_offset_byte() as _,
            i,
        )
    }

    // The bytes of entry `id`, for `id` in `0..self.len()`.
    #[inline]
    fn read_entry(&self, id: usize) -> Result<&'m [u8], ArrowError> {
        let range = self.offset(id)? as usize..self.offset(id + 1)? as usize;
        slice_from_slice_at_offset(self.bytes, self.first_value_byte as _, range)
    }
}

/// A variant value of which only the header byte has been read.
#[derive(Debug, Clone, Copy)]
pub struct ValueView<'m, 'v> {
    pub(crate) metadata: MetadataView<'m>,
    pub(crate) value: &'v [u8],
    header: u8,
}

impl<'m, 'v> ValueView<'m, 'v> {
    /// Reads the header byte of the value that starts at the beginning of `value`.
    ///
    /// `value` may extend past the end of the value.
    #[inline]
    pub fn try_new(metadata: MetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = first_byte_from_slice(value)?;
        Ok(Self {
            metadata,
            value,
            header,
        })
    }

    /// The metadata dictionary of this value.
    #[inline]
    pub fn metadata(&self) -> MetadataView<'m> {
        self.metadata
    }

    /// The basic type of this value.
    #[inline]
    pub fn basic_type(&self) -> VariantBasicType {
        get_basic_type(self.header)
    }

    /// This value as an object, or `None` if it is not an object.
    pub fn as_object(&self) -> Result<Option<ObjectView<'m, 'v>>, ArrowError> {
        match self.basic_type() {
            VariantBasicType::Object => ObjectView::try_new(self.metadata, self.value).map(Some),
            _ => Ok(None),
        }
    }

    /// This value as a list, or `None` if it is not a list.
    pub fn as_list(&self) -> Result<Option<ListView<'m, 'v>>, ArrowError> {
        match self.basic_type() {
            VariantBasicType::Array => ListView::try_new(self.metadata, self.value).map(Some),
            _ => Ok(None),
        }
    }

    /// The value of field `name`, or `None` if this value is not an object or has no such field.
    pub fn get_field(&self, name: &[u8]) -> Result<Option<Self>, ArrowError> {
        match self.as_object()? {
            Some(object) => object.get(name),
            None => Ok(None),
        }
    }

    /// Element `index`, or `None` if this value is not a list or has no such element.
    pub fn get_index(&self, index: usize) -> Result<Option<Self>, ArrowError> {
        match self.as_list()? {
            Some(list) => list.get(index),
            None => Ok(None),
        }
    }

    /// Follows `path` from this value, or returns `None` if a step does not exist.
    ///
    /// A [`VariantPathElement::ListElement`] step does not exist, as in [`Variant::get_path`].
    #[inline]
    pub fn get_path(&self, path: &[VariantPathElement<'_>]) -> Result<Option<Self>, ArrowError> {
        let mut current = *self;
        for element in path {
            let next = match element {
                VariantPathElement::Field { name } => current.get_field(name.as_bytes())?,
                VariantPathElement::Index { index } => current.get_index(*index)?,
                VariantPathElement::ListElement => None,
            };
            match next {
                Some(next) => current = next,
                None => return Ok(None),
            }
        }
        Ok(Some(current))
    }

    /// Decodes this value as an unvalidated [`Variant`].
    ///
    /// Only an object or a list reads the metadata dictionary, so only they check its bounds.
    #[inline]
    pub fn to_variant(&self) -> Result<Variant<'m, 'v>, ArrowError> {
        let metadata = match self.basic_type() {
            VariantBasicType::Object | VariantBasicType::Array => self.metadata.into_metadata()?,
            VariantBasicType::Primitive | VariantBasicType::ShortString => EMPTY_VARIANT_METADATA,
        };
        Variant::try_new_with_metadata_and_shallow_validation(metadata, self.value)
    }

    /// The exact encoding of this value, without any bytes that follow it.
    pub fn bytes(&self) -> Result<&'v [u8], ArrowError> {
        let len = match self.basic_type() {
            VariantBasicType::Primitive => primitive_len(self.header, self.value)?,
            VariantBasicType::ShortString => 1 + (self.header >> 2) as usize,
            VariantBasicType::Object => {
                ObjectView::try_new(self.metadata, self.value)?.byte_len()?
            }
            VariantBasicType::Array => ListView::try_new(self.metadata, self.value)?.byte_len()?,
        };
        slice_from_slice(self.value, ..len)
    }
}

/// The encoded size of the primitive value with header byte `header` at the start of `value`.
fn primitive_len(header: u8, value: &[u8]) -> Result<usize, ArrowError> {
    use VariantPrimitiveType::*;
    let payload = match VariantPrimitiveType::try_from(header >> 2)? {
        Null | BooleanTrue | BooleanFalse => 0,
        Int8 => 1,
        Int16 => 2,
        Int32 | Date | Float => 4,
        Decimal4 => 5,
        Int64 | Double | TimestampMicros | TimestampNtzMicros | TimestampNanos
        | TimestampNtzNanos | Time => 8,
        Decimal8 => 9,
        Uuid => 16,
        Decimal16 => 17,
        Binary | String => 4 + u32::from_le_bytes(array_from_slice(value, 1)?) as usize,
    };
    Ok(1 + payload)
}

/// A variant object of which only the header has been read.
#[derive(Debug, Clone, Copy)]
pub struct ObjectView<'m, 'v> {
    pub(crate) metadata: MetadataView<'m>,
    pub(crate) value: &'v [u8],
    pub(crate) header: VariantObjectHeader,
    pub(crate) num_elements: u32,
    pub(crate) first_field_offset_byte: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m, 'v> ObjectView<'m, 'v> {
    /// Reads the header and the field count of the object at the start of `value`.
    #[inline]
    pub fn try_new(metadata: MetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = VariantObjectHeader::try_new(first_byte_from_slice(value)?)?;
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, NUM_HEADER_BYTES as _, 0)?;

        let first_field_offset_byte = checked(
            num_elements
                .checked_mul(header.field_id_size())
                .and_then(|n| n.checked_add(header.field_ids_start_byte())),
            "offset of variant object field offsets",
        )?;
        let first_value_byte = checked(
            num_elements
                .checked_add(1)
                .and_then(|n| n.checked_mul(header.field_offset_size()))
                .and_then(|n| n.checked_add(first_field_offset_byte)),
            "offset of variant object field values",
        )?;

        Ok(Self {
            metadata,
            value,
            header,
            num_elements,
            first_field_offset_byte,
            first_value_byte,
        })
    }

    /// The number of fields.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_elements as _
    }

    /// True if the object has no fields.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_elements == 0
    }

    /// The metadata dictionary id of field `i`.
    pub fn field_id(&self, i: usize) -> Result<u32, ArrowError> {
        self.check_index(i)?;
        self.read_field_id(i)
    }

    /// The name of field `i`, without UTF-8 validation.
    pub fn field_name(&self, i: usize) -> Result<&'m [u8], ArrowError> {
        self.check_index(i)?;
        self.read_field_name(i)
    }

    /// The value of field `i`.
    pub fn field(&self, i: usize) -> Result<ValueView<'m, 'v>, ArrowError> {
        self.check_index(i)?;
        self.read_field(i)
    }

    /// The position of the field named `name`, if any.
    ///
    /// The spec requires fields to be sorted by name, so this is a binary search.
    pub fn find(&self, name: &[u8]) -> Result<Option<usize>, ArrowError> {
        try_binary_search(self.len(), |i| Ok(self.read_field_name(i)?.cmp(name)))
    }

    /// The value of the field named `name`, if any.
    pub fn get(&self, name: &[u8]) -> Result<Option<ValueView<'m, 'v>>, ArrowError> {
        self.find(name)?.map(|i| self.read_field(i)).transpose()
    }

    /// The size of the encoded object.
    ///
    /// The spec makes the last field offset point one byte past the end of the last value.
    pub fn byte_len(&self) -> Result<usize, ArrowError> {
        let end = checked(
            self.read_offset(self.len())?
                .checked_add(self.first_value_byte),
            "variant object size",
        )?;
        Ok(end as _)
    }

    /// Checks the last field offset, and returns the bounded [`VariantObject`] that uses
    /// `metadata`, the promoted form of this view's metadata dictionary.
    #[inline]
    pub fn into_object(
        self,
        metadata: VariantMetadata<'m>,
    ) -> Result<VariantObject<'m, 'v>, ArrowError> {
        VariantObject::try_from_view(self, metadata)
    }

    #[inline]
    fn check_index(&self, i: usize) -> Result<(), ArrowError> {
        match i < self.len() {
            true => Ok(()),
            false => Err(index_error("Variant object field", i, self.len())),
        }
    }

    // The reads below take a field index in `0..self.len()`, which the caller has checked.
    // `read_offset` also takes `self.len()`, for the end of the last value.

    #[inline]
    pub(crate) fn read_field_id(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.field_id_size.unpack_u32_at_offset(
            self.value,
            self.header.field_ids_start_byte() as _,
            i,
        )
    }

    #[inline]
    fn read_field_name(&self, i: usize) -> Result<&'m [u8], ArrowError> {
        self.metadata.entry(self.read_field_id(i)? as _)
    }

    #[inline]
    fn read_offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.field_offset_size.unpack_u32_at_offset(
            self.value,
            self.first_field_offset_byte as _,
            i,
        )
    }

    #[inline]
    pub(crate) fn read_field(&self, i: usize) -> Result<ValueView<'m, 'v>, ArrowError> {
        let start = checked(
            self.read_offset(i)?.checked_add(self.first_value_byte),
            "variant object field start",
        )?;
        ValueView::try_new(
            self.metadata,
            slice_from_slice(self.value, start as usize..)?,
        )
    }
}

/// A variant list of which only the header has been read.
#[derive(Debug, Clone, Copy)]
pub struct ListView<'m, 'v> {
    pub(crate) metadata: MetadataView<'m>,
    pub(crate) value: &'v [u8],
    pub(crate) header: VariantListHeader,
    pub(crate) num_elements: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m, 'v> ListView<'m, 'v> {
    /// Reads the header and the element count of the list at the start of `value`.
    #[inline]
    pub fn try_new(metadata: MetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = VariantListHeader::try_new(first_byte_from_slice(value)?)?;
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, NUM_HEADER_BYTES as _, 0)?;

        // There are num_elements + 1 offsets, and the values immediately follow them.
        let first_value_byte = checked(
            num_elements
                .checked_add(1)
                .and_then(|n| n.checked_mul(header.offset_size()))
                .and_then(|n| n.checked_add(header.first_offset_byte())),
            "offset of variant list values",
        )?;

        Ok(Self {
            metadata,
            value,
            header,
            num_elements,
            first_value_byte,
        })
    }

    /// The number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_elements as _
    }

    /// True if the list has no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_elements == 0
    }

    /// Element `index`, or `None` if `index` is out of bounds.
    pub fn get(&self, index: usize) -> Result<Option<ValueView<'m, 'v>>, ArrowError> {
        if index >= self.len() {
            return Ok(None);
        }
        let start = checked(
            self.read_offset(index)?.checked_add(self.first_value_byte),
            "variant list element start",
        )?;
        ValueView::try_new(
            self.metadata,
            slice_from_slice(self.value, start as usize..)?,
        )
        .map(Some)
    }

    /// The size of the encoded list.
    pub fn byte_len(&self) -> Result<usize, ArrowError> {
        let end = checked(
            self.read_offset(self.len())?
                .checked_add(self.first_value_byte),
            "variant array size",
        )?;
        Ok(end as _)
    }

    /// Checks the first and last offsets, and returns the bounded [`VariantList`] that uses
    /// `metadata`, the promoted form of this view's metadata dictionary.
    #[inline]
    pub fn into_list(
        self,
        metadata: VariantMetadata<'m>,
    ) -> Result<VariantList<'m, 'v>, ArrowError> {
        VariantList::try_from_view(self, metadata)
    }

    /// Entry `i` of the offset array, for `i` in `0..=self.len()`.
    #[inline]
    pub(crate) fn read_offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.offset_size.unpack_u32_at_offset(
            self.value,
            self.header.first_offset_byte() as _,
            i,
        )
    }
}
