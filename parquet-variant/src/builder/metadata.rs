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

use std::collections::HashMap;

use arrow_schema::ArrowError;
use indexmap::IndexSet;

use crate::{VariantMetadata, int_size};

/// Write little-endian integer to buffer
fn write_offset(buf: &mut Vec<u8>, value: usize, nbytes: u8) {
    let bytes = value.to_le_bytes();
    buf.extend_from_slice(&bytes[..nbytes as usize]);
}

/// A trait for building variant metadata dictionaries, to be used in conjunction with a
/// [`ValueBuilder`]. The trait provides methods for managing field names and their IDs, as well as
/// rolling back a failed builder operation that might have created new field ids.
///
/// [`ValueBuilder`]: crate::builder::ValueBuilder
pub trait MetadataBuilder: std::fmt::Debug {
    /// Attempts to register a field name, returning the corresponding (possibly newly-created)
    /// field id on success. Attempting to register the same field name twice will _generally_
    /// produce the same field id both times, but the variant spec does not actually require it.
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError>;

    /// Retrieves the field name for a given field id, which must be less than
    /// [`Self::num_field_names`]. Panics if the field id is out of bounds.
    fn field_name(&self, field_id: usize) -> &str;

    /// Returns the number of field names stored in this metadata builder. Any number less than this
    /// is a valid field id. The builder can be reverted back to this size later on (discarding any
    /// newer/higher field ids) by calling [`Self::truncate_field_names`].
    fn num_field_names(&self) -> usize;

    /// Reverts the field names to a previous size, discarding any newly out of bounds field ids.
    fn truncate_field_names(&mut self, new_size: usize);

    /// Finishes the current metadata dictionary, returning the new size of the underlying buffer.
    fn finish(&mut self) -> usize;
}

impl MetadataBuilder for WritableMetadataBuilder {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        Ok(self.upsert_field_name(field_name))
    }
    fn field_name(&self, field_id: usize) -> &str {
        self.field_name(field_id)
    }
    fn num_field_names(&self) -> usize {
        self.num_field_names()
    }
    fn truncate_field_names(&mut self, new_size: usize) {
        self.field_names.truncate(new_size)
    }
    fn finish(&mut self) -> usize {
        self.finish()
    }
}

/// A metadata builder that cannot register new field names, and merely returns the field id
/// associated with a known field name. This is useful for variant unshredding operations, where the
/// metadata column is fixed and -- per variant shredding spec -- already contains all field names
/// from the typed_value column. It is also useful when projecting a subset of fields from a variant
/// object value, since the bytes can be copied across directly without re-encoding their field ids.
///
/// NOTE: [`Self::finish`] is a no-op. If the intent is to make a copy of the underlying bytes each
/// time `finish` is called, a different trait impl will be needed.
#[derive(Debug)]
pub struct ReadOnlyMetadataBuilder<'m> {
    metadata: &'m VariantMetadata<'m>,
    // A cache that tracks field names this builder has already seen, because finding the field id
    // for a given field name is expensive -- O(n) for a large and unsorted metadata dictionary.
    known_field_names: HashMap<&'m str, u32>,
}

impl<'m> ReadOnlyMetadataBuilder<'m> {
    /// Creates a new read-only metadata builder from the given metadata dictionary.
    pub fn new(metadata: &'m VariantMetadata<'m>) -> Self {
        Self {
            metadata,
            known_field_names: HashMap::new(),
        }
    }
}

impl MetadataBuilder for ReadOnlyMetadataBuilder<'_> {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        if let Some(field_id) = self.known_field_names.get(field_name) {
            return Ok(*field_id);
        }

        let Some((field_id, field_name)) = self.metadata.get_entry(field_name) else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Field name '{field_name}' not found in metadata dictionary"
            )));
        };

        self.known_field_names.insert(field_name, field_id);
        Ok(field_id)
    }
    fn field_name(&self, field_id: usize) -> &str {
        &self.metadata[field_id]
    }
    fn num_field_names(&self) -> usize {
        self.metadata.len()
    }
    fn truncate_field_names(&mut self, new_size: usize) {
        debug_assert_eq!(self.metadata.len(), new_size);
    }
    fn finish(&mut self) -> usize {
        self.metadata.bytes.len()
    }
}

/// Builder for constructing metadata for [`Variant`] values.
///
/// This is used internally by the [`VariantBuilder`] to construct the metadata
///
/// You can use an existing `Vec<u8>` as the metadata buffer by using the `from` impl.
///
/// [`Variant`]: crate::Variant
/// [`VariantBuilder`]: crate::VariantBuilder
#[derive(Default, Debug)]
pub struct WritableMetadataBuilder {
    pub(crate) field_names: IndexSet<String>,

    pub(crate) is_sorted: bool,

    /// Output buffer. Metadata is written to the end of this buffer
    metadata_buffer: Vec<u8>,
}

impl WritableMetadataBuilder {
    /// Upsert field name to dictionary, return its ID
    pub fn upsert_field_name(&mut self, field_name: &str) -> u32 {
        let (id, new_entry) = self.field_names.insert_full(field_name.to_string());

        if new_entry {
            let n = self.num_field_names();

            // Dictionary sort order tracking:
            // - An empty dictionary is unsorted (ambiguous in spec but required by interop tests)
            // - A single-entry dictionary is trivially sorted
            // - Otherwise, an already-sorted dictionary becomes unsorted if the new entry breaks order
            self.is_sorted =
                n == 1 || self.is_sorted && (self.field_names[n - 2] < self.field_names[n - 1]);
        }

        id as u32
    }

    /// The current length of the underlying metadata buffer
    pub fn offset(&self) -> usize {
        self.metadata_buffer.len()
    }

    /// Returns the number of field names stored in the metadata builder.
    /// Note: this method should be the only place to call `self.field_names.len()`
    ///
    /// # Panics
    ///
    /// If the number of field names exceeds the maximum allowed value for `u32`.
    fn num_field_names(&self) -> usize {
        let n = self.field_names.len();
        assert!(n <= u32::MAX as usize);

        n
    }

    fn field_name(&self, i: usize) -> &str {
        &self.field_names[i]
    }

    fn metadata_size(&self) -> usize {
        self.field_names.iter().map(|k| k.len()).sum()
    }

    /// Finalizes the metadata dictionary and appends its serialized bytes to the underlying buffer,
    /// returning the resulting [`Self::offset`]. The builder state is reset and ready to start
    /// building a new metadata dictionary.
    pub fn finish(&mut self) -> usize {
        let nkeys = self.num_field_names();

        // Calculate metadata size
        let total_dict_size: usize = self.metadata_size();

        let metadata_buffer = &mut self.metadata_buffer;
        let is_sorted = std::mem::take(&mut self.is_sorted);
        let field_names = std::mem::take(&mut self.field_names);

        // Determine appropriate offset size based on the larger of dict size or total string size
        let max_offset = std::cmp::max(total_dict_size, nkeys);
        let offset_size = int_size(max_offset);

        let offset_start = 1 + offset_size as usize;
        let string_start = offset_start + (nkeys + 1) * offset_size as usize;
        let metadata_size = string_start + total_dict_size;

        metadata_buffer.reserve(metadata_size);

        // Write header: version=1, field names are sorted, with calculated offset_size
        metadata_buffer.push(0x01 | (is_sorted as u8) << 4 | ((offset_size - 1) << 6));

        // Write dictionary size
        write_offset(metadata_buffer, nkeys, offset_size);

        // Write offsets
        let mut cur_offset = 0;
        for key in field_names.iter() {
            write_offset(metadata_buffer, cur_offset, offset_size);
            cur_offset += key.len();
        }
        // Write final offset
        write_offset(metadata_buffer, cur_offset, offset_size);

        // Write string data
        for key in field_names {
            metadata_buffer.extend_from_slice(key.as_bytes());
        }

        metadata_buffer.len()
    }

    /// Returns the inner buffer, consuming self without finalizing any in progress metadata.
    pub fn into_inner(self) -> Vec<u8> {
        self.metadata_buffer
    }
}

impl<S: AsRef<str>> FromIterator<S> for WritableMetadataBuilder {
    fn from_iter<T: IntoIterator<Item = S>>(iter: T) -> Self {
        let mut this = Self::default();
        this.extend(iter);

        this
    }
}

impl<S: AsRef<str>> Extend<S> for WritableMetadataBuilder {
    fn extend<T: IntoIterator<Item = S>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        let (min, _) = iter.size_hint();

        self.field_names.reserve(min);

        for field_name in iter {
            self.upsert_field_name(field_name.as_ref());
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        ParentState, ValueBuilder, Variant, VariantBuilder, VariantMetadata,
        builder::{
            metadata::{ReadOnlyMetadataBuilder, WritableMetadataBuilder},
            object::ObjectBuilder,
        },
    };

    #[test]
    fn test_metadata_builder_from_iter() {
        let metadata = WritableMetadataBuilder::from_iter(vec!["apple", "banana", "cherry"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "apple");
        assert_eq!(metadata.field_name(1), "banana");
        assert_eq!(metadata.field_name(2), "cherry");
        assert!(metadata.is_sorted);

        let metadata = WritableMetadataBuilder::from_iter(["zebra", "apple", "banana"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "zebra");
        assert_eq!(metadata.field_name(1), "apple");
        assert_eq!(metadata.field_name(2), "banana");
        assert!(!metadata.is_sorted);

        let metadata = WritableMetadataBuilder::from_iter(Vec::<&str>::new());
        assert_eq!(metadata.num_field_names(), 0);
        assert!(!metadata.is_sorted);
    }

    #[test]
    fn test_metadata_builder_extend() {
        let mut metadata = WritableMetadataBuilder::default();
        assert_eq!(metadata.num_field_names(), 0);
        assert!(!metadata.is_sorted);

        metadata.extend(["apple", "cherry"]);
        assert_eq!(metadata.num_field_names(), 2);
        assert_eq!(metadata.field_name(0), "apple");
        assert_eq!(metadata.field_name(1), "cherry");
        assert!(metadata.is_sorted);

        // extend with more field names that maintain sort order
        metadata.extend(vec!["dinosaur", "monkey"]);
        assert_eq!(metadata.num_field_names(), 4);
        assert_eq!(metadata.field_name(2), "dinosaur");
        assert_eq!(metadata.field_name(3), "monkey");
        assert!(metadata.is_sorted);

        // test extending with duplicate field names
        let initial_count = metadata.num_field_names();
        metadata.extend(["apple", "monkey"]);
        assert_eq!(metadata.num_field_names(), initial_count); // No new fields added
    }

    #[test]
    fn test_metadata_builder_extend_sort_order() {
        let mut metadata = WritableMetadataBuilder::default();

        metadata.extend(["middle"]);
        assert!(metadata.is_sorted);

        metadata.extend(["zebra"]);
        assert!(metadata.is_sorted);

        // add field that breaks sort order
        metadata.extend(["apple"]);
        assert!(!metadata.is_sorted);
    }

    #[test]
    fn test_metadata_builder_from_iter_with_string_types() {
        // &str
        let metadata = WritableMetadataBuilder::from_iter(["a", "b", "c"]);
        assert_eq!(metadata.num_field_names(), 3);

        // string
        let metadata = WritableMetadataBuilder::from_iter(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(metadata.num_field_names(), 3);

        // mixed types (anything that implements AsRef<str>)
        let field_names: Vec<Box<str>> = vec!["a".into(), "b".into(), "c".into()];
        let metadata = WritableMetadataBuilder::from_iter(field_names);
        assert_eq!(metadata.num_field_names(), 3);
    }

    #[test]
    fn test_read_only_metadata_builder() {
        // First create some metadata with a few field names
        let mut default_builder = VariantBuilder::new();
        default_builder.add_field_name("name");
        default_builder.add_field_name("age");
        default_builder.add_field_name("active");
        let (metadata_bytes, _) = default_builder.finish();

        // Use the metadata to build new variant values
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
        let mut value_builder = ValueBuilder::new();

        {
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            let mut obj = ObjectBuilder::new(state, false);

            // These should succeed because the fields exist in the metadata
            obj.insert("name", "Alice");
            obj.insert("age", 30i8);
            obj.insert("active", true);
            obj.finish();
        }

        let value = value_builder.into_inner();

        // Verify the variant was built correctly
        let variant = Variant::try_new(&metadata_bytes, &value).unwrap();
        let obj = variant.as_object().unwrap();
        assert_eq!(obj.get("name"), Some(Variant::from("Alice")));
        assert_eq!(obj.get("age"), Some(Variant::Int8(30)));
        assert_eq!(obj.get("active"), Some(Variant::from(true)));
    }

    #[test]
    fn test_read_only_metadata_builder_fails_on_unknown_field() {
        // Create metadata with only one field
        let mut default_builder = VariantBuilder::new();
        default_builder.add_field_name("known_field");
        let (metadata_bytes, _) = default_builder.finish();

        // Use the metadata to build new variant values
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
        let mut value_builder = ValueBuilder::new();

        {
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            let mut obj = ObjectBuilder::new(state, false);

            // This should succeed
            obj.insert("known_field", "value");

            // This should fail because "unknown_field" is not in the metadata
            let result = obj.try_insert("unknown_field", "value");
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Field name 'unknown_field' not found")
            );
        }
    }
}
