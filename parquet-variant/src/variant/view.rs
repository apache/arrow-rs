// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Views over encoded variant bytes that read only what they use. See [`VariantView`].

use arrow_schema::ArrowError;

use crate::decoder::{VariantBasicType, get_basic_type};
use crate::utils::{
    first_byte_from_slice, overflow_error, slice_from_slice, slice_from_slice_at_offset,
    str_from_bytes, try_binary_search_range_by,
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

/// A metadata dictionary of which only the header has been read. See [`VariantView`].
#[derive(Debug, Clone, Copy)]
pub struct VariantMetadataView<'m> {
    pub(crate) bytes: &'m [u8],
    pub(crate) header: VariantMetadataHeader,
    pub(crate) dictionary_size: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m> VariantMetadataView<'m> {
    /// Reads the header and the dictionary size of the metadata dictionary in `bytes`.
    #[inline]
    pub fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header = VariantMetadataHeader::try_new(first_byte_from_slice(bytes)?)?;
        let dictionary_size =
            header
                .offset_size_bytes()
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
        self.header.is_sorted()
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
            let found =
                try_binary_search_range_by(0..self.len(), |id| Ok(self.read_entry(id)?.cmp(name)))?;
            return Ok(found.map(|id| id as u32));
        }
        for id in 0..self.len() {
            if self.read_entry(id)? == name {
                return Ok(Some(id as u32));
            }
        }
        Ok(None)
    }

    /// Attempts to interpret this view as an [unvalidated](VariantMetadata#validation)
    /// [`VariantMetadata`], which checks the first and last offsets.
    #[inline]
    pub fn try_into_metadata(self) -> Result<VariantMetadata<'m>, ArrowError> {
        VariantMetadata::try_from_view(self)
    }

    /// Entry `i` of the offset array, for `i` in `0..=self.len()`.
    #[inline]
    pub(crate) fn offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.offset_size_bytes().unpack_u32_at_offset(
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
///
/// Views such as [`VariantView`], [`VariantMetadataView`], [`VariantObjectView`] and
/// [`VariantListView`] do the minimum parsing work needed, and defer as much validation as possible
/// to when a value is accessed. Each accessor bounds-checks only the bytes that it reads, and returns
/// an error on malformed input instead of panicking. Field names are compared as bytes, which orders
/// them as `str` does, so a search does not check UTF-8. This is more efficient when reading a few
/// values out of a variant, such as a path, without validating the entire object.
///
/// For cases that need to validate the entire variant, use [`Variant`], [`VariantMetadata`],
/// [`VariantObject`] and [`VariantList`], which do much more validation during construction and
/// offer `with_full_validation` methods for additional checks. [`Self::try_to_variant`] and the
/// `try_into_*` methods of the other views convert a view to these types.
///
/// # Example
///
/// ```
/// # use parquet_variant::{Variant, VariantBuilder, VariantMetadataView, VariantView};
/// # use arrow_schema::ArrowError;
/// # fn main() -> Result<(), ArrowError> {
/// let mut builder = VariantBuilder::new();
/// let mut object = builder.new_object();
/// object.insert("name", "Ferris");
/// object.new_list("scores").with_value(7i8).with_value(9i8).finish();
/// object.finish();
/// let (metadata, value) = builder.finish();
///
/// let view = VariantView::try_new(VariantMetadataView::try_new(&metadata)?, &value)?;
/// let scores = view.get_field(b"scores")?.expect("the field exists");
/// let found = scores.get_index(1)?.expect("the element exists");
/// assert_eq!(found.try_to_variant()?, Variant::Int8(9));
/// assert!(view.get_field(b"missing")?.is_none());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct VariantView<'m, 'v> {
    pub(crate) metadata: VariantMetadataView<'m>,
    pub(crate) value: &'v [u8],
    header: u8,
}

impl<'m, 'v> VariantView<'m, 'v> {
    /// Reads the header byte of the value that starts at the beginning of `value`.
    ///
    /// `value` may extend past the end of the value.
    #[inline]
    pub fn try_new(metadata: VariantMetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = first_byte_from_slice(value)?;
        Ok(Self {
            metadata,
            value,
            header,
        })
    }

    /// The metadata dictionary of this value.
    #[inline]
    pub fn metadata(&self) -> VariantMetadataView<'m> {
        self.metadata
    }

    /// The basic type of this value.
    #[inline]
    pub fn basic_type(&self) -> VariantBasicType {
        get_basic_type(self.header)
    }

    /// This value as an object, or `None` if it is not an object.
    pub fn as_object(&self) -> Result<Option<VariantObjectView<'m, 'v>>, ArrowError> {
        match self.basic_type() {
            VariantBasicType::Object => {
                VariantObjectView::try_new(self.metadata, self.value).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// This value as a list, or `None` if it is not a list.
    pub fn as_list(&self) -> Result<Option<VariantListView<'m, 'v>>, ArrowError> {
        match self.basic_type() {
            VariantBasicType::Array => {
                VariantListView::try_new(self.metadata, self.value).map(Some)
            }
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

    /// Decodes this value as an unvalidated [`Variant`].
    ///
    /// Only an object or a list reads the metadata dictionary, so only they check its bounds.
    #[inline]
    pub fn try_to_variant(&self) -> Result<Variant<'m, 'v>, ArrowError> {
        let metadata = match self.basic_type() {
            VariantBasicType::Object | VariantBasicType::Array => {
                self.metadata.try_into_metadata()?
            }
            VariantBasicType::Primitive | VariantBasicType::ShortString => EMPTY_VARIANT_METADATA,
        };
        Variant::try_new_with_metadata_and_shallow_validation(metadata, self.value)
    }
}

/// A variant object of which only the header has been read. See [`VariantView`].
#[derive(Debug, Clone, Copy)]
pub struct VariantObjectView<'m, 'v> {
    pub(crate) metadata: VariantMetadataView<'m>,
    pub(crate) value: &'v [u8],
    pub(crate) header: VariantObjectHeader,
    pub(crate) num_elements: u32,
    pub(crate) first_field_offset_byte: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m, 'v> VariantObjectView<'m, 'v> {
    /// Reads the header and the field count of the object at the start of `value`.
    #[inline]
    pub fn try_new(metadata: VariantMetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = VariantObjectHeader::new(first_byte_from_slice(value)?);
        let num_elements = header.num_elements_size_bytes().unpack_u32_at_offset(
            value,
            NUM_HEADER_BYTES as _,
            0,
        )?;

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
    #[inline]
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
    pub fn field(&self, i: usize) -> Result<VariantView<'m, 'v>, ArrowError> {
        self.check_index(i)?;
        self.read_field(i)
    }

    // The bytes from the start of field `i`'s value to the end of the object, without the
    // throwaway view that `field` would build.
    #[inline]
    pub(crate) fn field_bytes(&self, i: usize) -> Result<&'v [u8], ArrowError> {
        self.check_index(i)?;
        self.read_field_bytes(i)
    }

    /// The position of the field named `name`, if any.
    ///
    /// The spec requires fields to be sorted by name, so this is a binary search.
    pub fn find(&self, name: &[u8]) -> Result<Option<usize>, ArrowError> {
        try_binary_search_range_by(0..self.len(), |i| Ok(self.read_field_name(i)?.cmp(name)))
    }

    /// The value of the field named `name`, if any.
    pub fn get(&self, name: &[u8]) -> Result<Option<VariantView<'m, 'v>>, ArrowError> {
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

    /// Attempts to interpret this view and `metadata` as an [unvalidated](VariantObject#validation)
    /// [`VariantObject`], which checks the last field offset.
    #[inline]
    pub fn try_into_object(
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
        self.header.field_id_size_bytes().unpack_u32_at_offset(
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
        self.header.field_offset_size_bytes().unpack_u32_at_offset(
            self.value,
            self.first_field_offset_byte as _,
            i,
        )
    }

    #[inline]
    pub(crate) fn read_field(&self, i: usize) -> Result<VariantView<'m, 'v>, ArrowError> {
        VariantView::try_new(self.metadata, self.read_field_bytes(i)?)
    }

    #[inline]
    pub(crate) fn read_field_bytes(&self, i: usize) -> Result<&'v [u8], ArrowError> {
        let start = checked(
            self.read_offset(i)?.checked_add(self.first_value_byte),
            "variant object field start",
        )?;
        slice_from_slice(self.value, start as usize..)
    }
}

/// A variant list of which only the header has been read. See [`VariantView`].
#[derive(Debug, Clone, Copy)]
pub struct VariantListView<'m, 'v> {
    pub(crate) metadata: VariantMetadataView<'m>,
    pub(crate) value: &'v [u8],
    pub(crate) header: VariantListHeader,
    pub(crate) num_elements: u32,
    pub(crate) first_value_byte: u32,
}

impl<'m, 'v> VariantListView<'m, 'v> {
    /// Reads the header and the element count of the list at the start of `value`.
    #[inline]
    pub fn try_new(metadata: VariantMetadataView<'m>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = VariantListHeader::try_new(first_byte_from_slice(value)?)?;
        let num_elements = header.num_elements_size_bytes().unpack_u32_at_offset(
            value,
            NUM_HEADER_BYTES as _,
            0,
        )?;

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
    pub fn get(&self, index: usize) -> Result<Option<VariantView<'m, 'v>>, ArrowError> {
        if index >= self.len() {
            return Ok(None);
        }
        let start = checked(
            self.read_offset(index)?.checked_add(self.first_value_byte),
            "variant list element start",
        )?;
        VariantView::try_new(
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

    /// Attempts to interpret this view and `metadata` as an [unvalidated](VariantList#validation)
    /// [`VariantList`], which checks the first and last offsets.
    #[inline]
    pub fn try_into_list(
        self,
        metadata: VariantMetadata<'m>,
    ) -> Result<VariantList<'m, 'v>, ArrowError> {
        VariantList::try_from_view(self, metadata)
    }

    /// Entry `i` of the offset array, for `i` in `0..=self.len()`.
    #[inline]
    pub(crate) fn read_offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.offset_size_bytes().unpack_u32_at_offset(
            self.value,
            self.header.first_offset_byte() as _,
            i,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VariantBuilder;

    const LONG_STRING: &str =
        "a string that is longer than the sixty-three bytes that a short string can hold";

    /// `{"a": 1, "b": "short", "list": [1, "two", {"x": true}], "nested": {"k": LONG_STRING}}`
    fn sample() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("a", 1i8);
        object.insert("b", "short");
        let mut list = object.new_list("list");
        list.append_value(1i8);
        list.append_value("two");
        list.new_object().with_field("x", true).finish();
        list.finish();
        object
            .new_object("nested")
            .with_field("k", LONG_STRING)
            .finish();
        object.finish();
        builder.finish()
    }

    fn view<'a>(metadata: &'a [u8], value: &'a [u8]) -> VariantView<'a, 'a> {
        VariantView::try_new(VariantMetadataView::try_new(metadata).unwrap(), value).unwrap()
    }

    fn metadata_with_names(names: &[&str]) -> Vec<u8> {
        let mut builder = VariantBuilder::new().with_field_names(names.iter().copied());
        builder.append_value(1i8);
        builder.finish().0
    }

    fn assert_metadata_view_matches(bytes: &[u8]) {
        let metadata = VariantMetadata::try_new(bytes).unwrap();
        let view = VariantMetadataView::try_new(bytes).unwrap();
        assert_eq!(view.len(), metadata.len());
        assert_eq!(view.is_empty(), metadata.is_empty());
        assert_eq!(view.is_sorted(), metadata.is_sorted());
        for id in 0..metadata.len() {
            let name = metadata.get(id).unwrap();
            assert_eq!(view.entry(id).unwrap(), name.as_bytes());
            assert_eq!(view.entry_str(id).unwrap(), name);
            assert_eq!(view.find(name.as_bytes()).unwrap(), Some(id as u32));
        }
        assert_eq!(view.find(b"no such name").unwrap(), None);
        assert!(view.entry(view.len()).is_err());
        assert!(view.entry_str(view.len()).is_err());
        // Promotion gives the unvalidated form.
        assert_eq!(
            view.try_into_metadata().unwrap(),
            VariantMetadata::new(bytes)
        );
    }

    #[test]
    fn test_metadata_view_sorted() {
        // More than 10 entries, so `find` uses its binary search.
        let names: Vec<String> = (0..64).map(|i| format!("field_{i:03}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let bytes = metadata_with_names(&names);
        assert!(VariantMetadataView::try_new(&bytes).unwrap().is_sorted());
        assert_metadata_view_matches(&bytes);
    }

    #[test]
    fn test_metadata_view_unsorted() {
        let bytes = metadata_with_names(&["zebra", "apple", "mango", "kiwi"]);
        assert!(!VariantMetadataView::try_new(&bytes).unwrap().is_sorted());
        assert_metadata_view_matches(&bytes);
    }

    #[test]
    fn test_metadata_view_empty() {
        let view = VariantMetadataView::try_new(crate::EMPTY_VARIANT_METADATA_BYTES).unwrap();
        assert!(view.is_empty());
        assert_eq!(view.find(b"a").unwrap(), None);
        assert!(view.entry(0).is_err());
    }

    #[test]
    fn test_metadata_view_entry_is_not_utf8_validated() {
        // Dictionary with one entry, the invalid UTF-8 byte 0xFF.
        let bytes = [0b0000_0001, 1, 0, 1, 0xFF];
        let view = VariantMetadataView::try_new(&bytes).unwrap();
        assert_eq!(view.entry(0).unwrap(), &[0xFF]);
        assert_eq!(view.find(&[0xFF]).unwrap(), Some(0));
        assert!(view.entry_str(0).is_err());
    }

    #[test]
    fn test_object_view_matches_object() {
        let (metadata, value) = sample();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let object = variant.as_object().unwrap();
        let root = view(&metadata, &value);
        let object_view = root.as_object().unwrap().unwrap();

        assert_eq!(object_view.len(), object.len());
        assert!(!object_view.is_empty());
        for i in 0..object.len() {
            let name = object.field_name(i).unwrap();
            assert_eq!(object_view.field_name(i).unwrap(), name.as_bytes());
            let field_id = object_view.field_id(i).unwrap();
            assert_eq!(root.metadata().entry_str(field_id as _).unwrap(), name);
            assert_eq!(
                object_view.field(i).unwrap().try_to_variant().unwrap(),
                object.field(i).unwrap()
            );
            assert_eq!(object_view.find(name.as_bytes()).unwrap(), Some(i));
            let found = object_view.get(name.as_bytes()).unwrap().unwrap();
            assert_eq!(found.try_to_variant().unwrap(), object.get(name).unwrap());
        }

        assert_eq!(object_view.find(b"missing").unwrap(), None);
        assert!(object_view.get(b"missing").unwrap().is_none());
        assert!(object_view.field(object.len()).is_err());
        assert!(object_view.field_id(object.len()).is_err());
        assert!(object_view.field_name(object.len()).is_err());

        assert_eq!(object_view.byte_len().unwrap(), value.len());
        let metadata = root.metadata().try_into_metadata().unwrap();
        assert_eq!(&object_view.try_into_object(metadata).unwrap(), object);
    }

    #[test]
    fn test_list_view_matches_list() {
        let (metadata, value) = sample();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let variant = variant.get_object_field("list").unwrap();
        let list = variant.as_list().unwrap();
        let element = view(&metadata, &value).get_field(b"list").unwrap().unwrap();
        let list_view = element.as_list().unwrap().unwrap();

        assert_eq!(list_view.len(), list.len());
        assert!(!list_view.is_empty());
        for i in 0..list.len() {
            let found = list_view.get(i).unwrap().unwrap();
            assert_eq!(found.try_to_variant().unwrap(), list.get(i).unwrap());
        }
        assert!(list_view.get(list.len()).unwrap().is_none());

        let metadata = element.metadata().try_into_metadata().unwrap();
        assert_eq!(&list_view.try_into_list(metadata).unwrap(), list);
    }

    #[test]
    fn test_value_view_types() {
        let (metadata, value) = sample();
        let root = view(&metadata, &value);
        assert_eq!(root.basic_type(), VariantBasicType::Object);
        assert!(root.as_object().unwrap().is_some());
        assert!(root.as_list().unwrap().is_none());
        assert!(root.get_index(0).unwrap().is_none());

        let list = root.get_field(b"list").unwrap().unwrap();
        assert_eq!(list.basic_type(), VariantBasicType::Array);
        assert!(list.as_list().unwrap().is_some());
        assert!(list.as_object().unwrap().is_none());
        assert!(list.get_field(b"a").unwrap().is_none());

        let primitive = root.get_field(b"a").unwrap().unwrap();
        assert_eq!(primitive.basic_type(), VariantBasicType::Primitive);
        assert!(primitive.as_object().unwrap().is_none());
        assert!(primitive.as_list().unwrap().is_none());

        let short_string = root.get_field(b"b").unwrap().unwrap();
        assert_eq!(short_string.basic_type(), VariantBasicType::ShortString);
    }

    /// Reads everything reachable from `value` through the views, ignoring errors.
    fn walk(value: VariantView<'_, '_>) {
        // Malformed input may declare more children than its bytes can hold.
        const MAX_CHILDREN: usize = 64;

        let _ = value.try_to_variant();
        let metadata = value.metadata();
        for id in 0..metadata.len().min(MAX_CHILDREN) {
            let _ = metadata.entry(id);
            let _ = metadata.entry_str(id);
        }
        let _ = metadata.find(b"k");
        let _ = metadata.try_into_metadata();

        if let Ok(Some(object)) = value.as_object() {
            let _ = object.byte_len();
            let _ = object.find(b"k");
            let _ = object.get(b"k");
            for i in 0..object.len().min(MAX_CHILDREN) {
                let _ = object.field_id(i);
                let _ = object.field_name(i);
                if let Ok(field) = object.field(i) {
                    walk(field);
                }
            }
            if let Ok(metadata) = metadata.try_into_metadata() {
                let _ = object.try_into_object(metadata);
            }
        }
        if let Ok(Some(list)) = value.as_list() {
            let _ = list.byte_len();
            for i in 0..list.len().min(MAX_CHILDREN) {
                if let Ok(Some(element)) = list.get(i) {
                    walk(element);
                }
            }
            if let Ok(metadata) = metadata.try_into_metadata() {
                let _ = list.try_into_list(metadata);
            }
        }
    }

    fn walk_bytes(metadata: &[u8], value: &[u8]) {
        let Ok(metadata) = VariantMetadataView::try_new(metadata) else {
            return;
        };
        if let Ok(value) = VariantView::try_new(metadata, value) {
            walk(value);
        }
    }

    #[test]
    fn test_truncated_input_does_not_panic() {
        let (metadata, value) = sample();
        for len in 0..metadata.len() {
            walk_bytes(&metadata[..len], &value);
        }
        for len in 0..value.len() {
            walk_bytes(&metadata, &value[..len]);
        }
    }

    #[test]
    fn test_corrupt_input_does_not_panic() {
        let (metadata, value) = sample();
        for corrupt in [0x00, 0x7F, 0xFF] {
            for i in 0..metadata.len() {
                let mut metadata = metadata.clone();
                metadata[i] = corrupt;
                walk_bytes(&metadata, &value);
            }
            for i in 0..value.len() {
                let mut value = value.clone();
                value[i] = corrupt;
                walk_bytes(&metadata, &value);
            }
        }
    }
}
