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
use crate::builder::list::ListBuilder;
use crate::builder::metadata::MetadataBuilder;
use crate::decoder::VariantBasicType;
use crate::{
    BASIC_TYPE_BITS, BuilderSpecificState, ParentState, ValueBuilder, Variant, VariantBuilderExt,
    int_size,
};
use arrow_schema::ArrowError;
use indexmap::IndexMap;

fn object_header(large: bool, id_size: u8, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 4))
        | ((id_size - 1) << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Object as u8
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
///
/// [`VariantBuilder`]: crate::VariantBuilder
#[derive(Debug)]
pub struct ObjectBuilder<'a, S: BuilderSpecificState> {
    parent_state: ParentState<'a, S>,
    pub(crate) fields: IndexMap<u32, usize>, // (field_id, offset)
    validate_unique_fields: bool,
}

impl<'a, S: BuilderSpecificState> ObjectBuilder<'a, S> {
    /// Creates a new object builder, nested on top of the given parent state.
    pub fn new(parent_state: ParentState<'a, S>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            fields: IndexMap::new(),
            validate_unique_fields,
        }
    }

    /// Add a field with key and value to the object
    ///
    /// # See Also
    /// - [`ObjectBuilder::try_insert`] for a fallible version.
    /// - [`ObjectBuilder::with_field`] for a builder-style API.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ObjectBuilder::try_insert`]
    pub fn insert<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, key: &str, value: T) {
        let (state, _) = self.parent_state(key).unwrap();
        ValueBuilder::append_variant(state, value.into())
    }

    /// Add a field with key and value to the object
    ///
    /// # See Also
    /// - [`ObjectBuilder::insert`] for an infallible version that panics
    /// - [`ObjectBuilder::try_with_field`] for a builder-style API.
    ///
    /// # Note
    /// Attempting to insert a duplicate field name produces an error if unique field
    /// validation is enabled. Otherwise, the new value overwrites the previous field mapping
    /// without erasing the old value, resulting in a larger variant
    pub fn try_insert<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        key: &str,
        value: T,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state(key)?;
        ValueBuilder::try_append_variant(state, value.into())
    }

    /// Add a field with key and value to the object by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy, and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended, but the value's new field name is handled normally.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ObjectBuilder::try_insert_bytes`]
    pub fn insert_bytes<'m, 'd>(&mut self, key: &str, value: impl Into<Variant<'m, 'd>>) {
        self.try_insert_bytes(key, value).unwrap()
    }

    /// Add a field with key and value to the object by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy, and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended, but the value's new field name is handled normally.
    ///
    /// # Note
    /// When inserting duplicate keys, the new value overwrites the previous mapping,
    /// but the old value remains in the buffer, resulting in a larger variant
    pub fn try_insert_bytes<'m, 'd>(
        &mut self,
        key: &str,
        value: impl Into<Variant<'m, 'd>>,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state(key)?;
        ValueBuilder::append_variant_bytes(state, value.into());
        Ok(())
    }

    /// Builder style API for adding a field with key and value to the object
    ///
    /// Same as [`ObjectBuilder::insert`], but returns `self` for chaining.
    pub fn with_field<'m, 'd, T: Into<Variant<'m, 'd>>>(mut self, key: &str, value: T) -> Self {
        self.insert(key, value);
        self
    }

    /// Builder style API for adding a field with key and value to the object
    ///
    /// Same as [`ObjectBuilder::try_insert`], but returns `self` for chaining.
    pub fn try_with_field<'m, 'd, T: Into<Variant<'m, 'd>>>(
        mut self,
        key: &str,
        value: T,
    ) -> Result<Self, ArrowError> {
        self.try_insert(key, value)?;
        Ok(self)
    }

    /// Enables validation for unique field keys when inserting into this object.
    ///
    /// When this is enabled, calling [`ObjectBuilder::finish`] will return an error
    /// if any duplicate field keys were added using [`ObjectBuilder::insert`].
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    // Returns validate_unique_fields because we can no longer reference self once this method returns.
    fn parent_state<'b>(
        &'b mut self,
        field_name: &str,
    ) -> Result<(ParentState<'b, ObjectState<'b>>, bool), ArrowError> {
        let validate_unique_fields = self.validate_unique_fields;
        let state = ParentState::try_object(
            self.parent_state.value_builder,
            self.parent_state.metadata_builder,
            &mut self.fields,
            self.parent_state.saved_value_builder_offset,
            field_name,
            validate_unique_fields,
        )?;
        Ok((state, validate_unique_fields))
    }

    /// Returns an object builder that can be used to append a new (nested) object to this object.
    ///
    /// Panics if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object<'b>(&'b mut self, key: &'b str) -> ObjectBuilder<'b, ObjectState<'b>> {
        self.try_new_object(key).unwrap()
    }

    /// Returns an object builder that can be used to append a new (nested) object to this object.
    ///
    /// Fails if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn try_new_object<'b>(
        &'b mut self,
        key: &str,
    ) -> Result<ObjectBuilder<'b, ObjectState<'b>>, ArrowError> {
        let (parent_state, validate_unique_fields) = self.parent_state(key)?;
        Ok(ObjectBuilder::new(parent_state, validate_unique_fields))
    }

    /// Returns a list builder that can be used to append a new (nested) list to this object.
    ///
    /// Panics if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list<'b>(&'b mut self, key: &str) -> ListBuilder<'b, ObjectState<'b>> {
        self.try_new_list(key).unwrap()
    }

    /// Returns a list builder that can be used to append a new (nested) list to this object.
    ///
    /// Fails if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn try_new_list<'b>(
        &'b mut self,
        key: &str,
    ) -> Result<ListBuilder<'b, ObjectState<'b>>, ArrowError> {
        let (parent_state, validate_unique_fields) = self.parent_state(key)?;
        Ok(ListBuilder::new(parent_state, validate_unique_fields))
    }

    /// Finalizes this object and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) {
        let metadata_builder = self.parent_state.metadata_builder();

        self.fields.sort_by(|&field_a_id, _, &field_b_id, _| {
            let field_a_name = metadata_builder.field_name(field_a_id as usize);
            let field_b_name = metadata_builder.field_name(field_b_id as usize);
            field_a_name.cmp(field_b_name)
        });

        let max_id = self.fields.iter().map(|(i, _)| *i).max().unwrap_or(0);
        let id_size = int_size(max_id as usize);

        let starting_offset = self.parent_state.saved_value_builder_offset;
        let value_builder = self.parent_state.value_builder();
        let current_offset = value_builder.offset();
        // Current object starts from `object_start_offset`
        let data_size = current_offset - starting_offset;
        let offset_size = int_size(data_size);

        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;

        let header_size = 1 + // header byte
            (if is_large { 4 } else { 1 }) + // num_fields
            (num_fields * id_size as usize) + // field IDs
            ((num_fields + 1) * offset_size as usize); // field offsets + data_size

        // Shift existing data to make room for the header
        value_builder.inner_mut().splice(
            starting_offset..starting_offset,
            std::iter::repeat_n(0u8, header_size),
        );

        // Write header at the original start position
        let mut header_pos = starting_offset;

        // Write header byte
        let header = object_header(is_large, id_size, offset_size);

        header_pos = self
            .parent_state
            .value_builder()
            .append_header_start_from_buf_pos(header_pos, header, is_large, num_fields);

        header_pos = self
            .parent_state
            .value_builder()
            .append_offset_array_start_from_buf_pos(
                header_pos,
                self.fields.keys().copied().map(|id| id as usize),
                None,
                id_size,
            );

        self.parent_state
            .value_builder()
            .append_offset_array_start_from_buf_pos(
                header_pos,
                self.fields.values().copied(),
                Some(data_size),
                offset_size,
            );
        self.parent_state.finish();
    }
}

impl<'a, 'm, 'v, S, K, V> Extend<(K, V)> for ObjectBuilder<'a, S>
where
    S: BuilderSpecificState,
    K: AsRef<str>,
    V: Into<Variant<'m, 'v>>,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter.into_iter() {
            self.insert(key.as_ref(), value);
        }
    }
}

/// Internal state for object building
#[derive(Debug)]
pub struct ObjectState<'a> {
    fields: &'a mut IndexMap<u32, usize>,
    saved_fields_size: usize,
}

// `ObjectBuilder::finish()` eagerly updates the field offsets, which we should rollback on failure.
impl BuilderSpecificState for ObjectState<'_> {
    fn rollback(&mut self) {
        self.fields.truncate(self.saved_fields_size);
    }
}

impl<'a> ParentState<'a, ObjectState<'a>> {
    /// Creates a new instance suitable for an [`ObjectBuilder`]. The value and metadata builder state
    /// is checkpointed and will roll back on drop, unless [`Self::finish`] is called. The new
    /// field's name and offset are also captured eagerly and will also roll back if not finished.
    ///
    /// The call fails if the field name is invalid (e.g. because it duplicates an existing field).
    pub fn try_object(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
        fields: &'a mut IndexMap<u32, usize>,
        saved_parent_value_builder_offset: usize,
        field_name: &str,
        validate_unique_fields: bool,
    ) -> Result<Self, ArrowError> {
        // The saved_parent_buffer_offset is the buffer size as of when the parent builder was
        // constructed. The saved_buffer_offset is the buffer size as of now (when a child builder
        // is created). The variant field_offset entry for this field is their difference.
        let saved_value_builder_offset = value_builder.offset();
        let saved_fields_size = fields.len();
        let saved_metadata_builder_dict_size = metadata_builder.num_field_names();
        let field_id = metadata_builder.try_upsert_field_name(field_name)?;
        let field_start = saved_value_builder_offset - saved_parent_value_builder_offset;
        if fields.insert(field_id, field_start).is_some() && validate_unique_fields {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Duplicate field name: {field_name}"
            )));
        }

        let builder_state = ObjectState {
            fields,
            saved_fields_size,
        };
        Ok(Self {
            saved_metadata_builder_dict_size,
            saved_value_builder_offset,
            value_builder,
            metadata_builder,
            builder_state,
            finished: false,
        })
    }
}

/// A [`VariantBuilderExt`] that inserts a new field into a variant object.
pub struct ObjectFieldBuilder<'o, 'v, 's, S: BuilderSpecificState> {
    key: &'s str,
    builder: &'o mut ObjectBuilder<'v, S>,
}

impl<'o, 'v, 's, S: BuilderSpecificState> ObjectFieldBuilder<'o, 'v, 's, S> {
    pub fn new(key: &'s str, builder: &'o mut ObjectBuilder<'v, S>) -> Self {
        Self { key, builder }
    }
}

impl<S: BuilderSpecificState> VariantBuilderExt for ObjectFieldBuilder<'_, '_, '_, S> {
    type State<'a>
        = ObjectState<'a>
    where
        Self: 'a;

    /// A NULL object field is interpreted as missing, so nothing gets inserted at all.
    fn append_null(&mut self) {}
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.builder.insert(self.key, value);
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError> {
        self.builder.try_new_list(self.key)
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        self.builder.try_new_object(self.key)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ParentState, ValueBuilder, Variant, VariantBuilder, VariantMetadata,
        builder::{metadata::ReadOnlyMetadataBuilder, object::ObjectBuilder},
        decoder::VariantBasicType,
    };

    #[test]
    fn test_object() {
        let mut builder = VariantBuilder::new();

        builder
            .new_object()
            .with_field("name", "John")
            .with_field("age", 42i8)
            .finish();

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_object_field_ordering() {
        let mut builder = VariantBuilder::new();

        builder
            .new_object()
            .with_field("zebra", "stripes")
            .with_field("apple", "red")
            .with_field("banana", "yellow")
            .finish();

        let (_, value) = builder.finish();

        let header = value[0];
        assert_eq!(header & 0x03, VariantBasicType::Object as u8);

        let field_count = value[1] as usize;
        assert_eq!(field_count, 3);

        // Get field IDs from the object header
        let field_ids: Vec<u8> = value[2..5].to_vec();

        // apple(1), banana(2), zebra(0)
        assert_eq!(field_ids, vec![1, 2, 0]);
    }

    #[test]
    fn test_duplicate_fields_in_object() {
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("name", "Ron Artest")
            .with_field("name", "Metta World Peace") // Duplicate field
            .finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let obj = variant.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert_eq!(obj.field(0).unwrap(), Variant::from("Metta World Peace"));

        assert_eq!(
            vec![("name", Variant::from("Metta World Peace"))],
            obj.iter().collect::<Vec<_>>()
        );
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

    // matthew
    #[test]
    fn test_append_object() {
        let (m1, v1) = make_object();
        let variant = Variant::new(&m1, &v1);

        let mut builder = VariantBuilder::new().with_metadata(VariantMetadata::new(&m1));

        builder.append_value(variant.clone());

        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    /// make an object variant with field names in reverse lexicographical order
    fn make_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        let mut obj = builder.new_object();

        obj.insert("b", true);
        obj.insert("a", false);
        obj.finish();
        builder.finish()
    }

    #[test]
    fn test_append_nested_object() {
        let (m1, v1) = make_nested_object();
        let variant = Variant::new(&m1, &v1);

        // because we can guarantee metadata is validated through the builder
        let mut builder = VariantBuilder::new().with_metadata(VariantMetadata::new(&m1));
        builder.append_value(variant.clone());

        let (metadata, value) = builder.finish();
        let result_variant = Variant::new(&metadata, &value);

        assert_eq!(variant, result_variant);
    }

    /// make a nested object variant
    fn make_nested_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        {
            let mut outer_obj = builder.new_object();

            {
                let mut inner_obj = outer_obj.new_object("b");
                inner_obj.insert("a", "inner_value");
                inner_obj.finish();
            }

            outer_obj.finish();
        }

        builder.finish()
    }

    #[test]
    fn test_nested_object() {
        /*
        {
            "c": {
                "b": "a"
            }
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a");
                inner_object_builder.finish();
            }

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 1);
        assert_eq!(outer_object.field_name(0).unwrap(), "c");

        let inner_object_variant = outer_object.field(0).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 1);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));
    }

    #[test]
    fn test_nested_object_with_duplicate_field_names_per_object() {
        /*
        {
            "c": {
                "b": false,
                "c": "a"
            },
            "b": false,
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", false);
                inner_object_builder.insert("c", "a");

                inner_object_builder.finish();
            }

            outer_object_builder.insert("b", false);
            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 2);
        assert_eq!(outer_object.field_name(0).unwrap(), "b");

        let inner_object_variant = outer_object.field(1).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 2);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from(false));
        assert_eq!(inner_object.field_name(1).unwrap(), "c");
        assert_eq!(inner_object.field(1).unwrap(), Variant::from("a"));
    }

    #[test]
    fn test_nested_object_with_heterogeneous_fields() {
        /*
        {
            "a": false,
            "c": {
                "b": "a",
                "c": {
                   "aa": "bb",
                },
                "d": {
                    "cc": "dd"
                }
            },
            "b": true,
            "d": {
               "e": 1,
               "f": [1, true],
               "g": ["tree", false],
            }
        }
        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();

            outer_object_builder.insert("a", false);

            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a");

                {
                    let mut inner_inner_object_builder = inner_object_builder.new_object("c");
                    inner_inner_object_builder.insert("aa", "bb");
                    inner_inner_object_builder.finish();
                }

                {
                    let mut inner_inner_object_builder = inner_object_builder.new_object("d");
                    inner_inner_object_builder.insert("cc", "dd");
                    inner_inner_object_builder.finish();
                }
                inner_object_builder.finish();
            }

            outer_object_builder.insert("b", true);

            {
                let mut inner_object_builder = outer_object_builder.new_object("d");
                inner_object_builder.insert("e", 1);
                {
                    let mut inner_list_builder = inner_object_builder.new_list("f");
                    inner_list_builder.append_value(1);
                    inner_list_builder.append_value(true);

                    inner_list_builder.finish();
                }

                {
                    let mut inner_list_builder = inner_object_builder.new_list("g");
                    inner_list_builder.append_value("tree");
                    inner_list_builder.append_value(false);

                    inner_list_builder.finish();
                }

                inner_object_builder.finish();
            }

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();

        // note, object fields are now sorted lexigraphically by field name
        /*
         {
            "a": false,
            "b": true,
            "c": {
                "b": "a",
                "c": {
                   "aa": "bb",
                },
                "d": {
                    "cc": "dd"
                }
            },
            "d": {
               "e": 1,
               "f": [1, true],
               "g": ["tree", false],
            }
        }
        */

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 4);

        assert_eq!(outer_object.field_name(0).unwrap(), "a");
        assert_eq!(outer_object.field(0).unwrap(), Variant::from(false));

        assert_eq!(outer_object.field_name(2).unwrap(), "c");

        let inner_object_variant = outer_object.field(2).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 3);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));

        let inner_iner_object_variant_c = inner_object.field(1).unwrap();
        let inner_inner_object_c = inner_iner_object_variant_c.as_object().unwrap();
        assert_eq!(inner_inner_object_c.len(), 1);
        assert_eq!(inner_inner_object_c.field_name(0).unwrap(), "aa");
        assert_eq!(inner_inner_object_c.field(0).unwrap(), Variant::from("bb"));

        let inner_iner_object_variant_d = inner_object.field(2).unwrap();
        let inner_inner_object_d = inner_iner_object_variant_d.as_object().unwrap();
        assert_eq!(inner_inner_object_d.len(), 1);
        assert_eq!(inner_inner_object_d.field_name(0).unwrap(), "cc");
        assert_eq!(inner_inner_object_d.field(0).unwrap(), Variant::from("dd"));

        assert_eq!(outer_object.field_name(1).unwrap(), "b");
        assert_eq!(outer_object.field(1).unwrap(), Variant::from(true));

        let out_object_variant_d = outer_object.field(3).unwrap();
        let out_object_d = out_object_variant_d.as_object().unwrap();
        assert_eq!(out_object_d.len(), 3);
        assert_eq!("e", out_object_d.field_name(0).unwrap());
        assert_eq!(Variant::from(1), out_object_d.field(0).unwrap());
        assert_eq!("f", out_object_d.field_name(1).unwrap());

        let first_inner_list_variant_f = out_object_d.field(1).unwrap();
        let first_inner_list_f = first_inner_list_variant_f.as_list().unwrap();
        assert_eq!(2, first_inner_list_f.len());
        assert_eq!(Variant::from(1), first_inner_list_f.get(0).unwrap());
        assert_eq!(Variant::from(true), first_inner_list_f.get(1).unwrap());

        let second_inner_list_variant_g = out_object_d.field(2).unwrap();
        let second_inner_list_g = second_inner_list_variant_g.as_list().unwrap();
        assert_eq!(2, second_inner_list_g.len());
        assert_eq!(Variant::from("tree"), second_inner_list_g.get(0).unwrap());
        assert_eq!(Variant::from(false), second_inner_list_g.get(1).unwrap());
    }

    #[test]
    fn test_object_without_unique_field_validation() {
        let mut builder = VariantBuilder::new();

        // Root object with duplicates
        let mut obj = builder.new_object();
        obj.insert("a", 1);
        obj.insert("a", 2);
        obj.finish();

        // Deeply nested list structure with duplicates
        let mut builder = VariantBuilder::new();
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut nested_obj = inner_list.new_object();
        nested_obj.insert("x", 1);
        nested_obj.insert("x", 2);
        nested_obj.new_list("x").with_value(3).finish();
        nested_obj.new_object("x").with_field("y", 4).finish();
        nested_obj.finish();
        inner_list.finish();
        outer_list.finish();

        // Verify the nested object is built correctly -- the nested object "x" should have "won"
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_element = variant.get_list_element(0).unwrap();
        let inner_element = outer_element.get_list_element(0).unwrap();
        let outer_field = inner_element.get_object_field("x").unwrap();
        let inner_field = outer_field.get_object_field("y").unwrap();
        assert_eq!(inner_field, Variant::from(4));
    }

    #[test]
    fn test_object_with_unique_field_validation() {
        let mut builder = VariantBuilder::new().with_validate_unique_fields(true);

        // Root-level object with duplicates
        let result = builder
            .new_object()
            .with_field("a", 1)
            .with_field("b", 2)
            .try_with_field("a", 3);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: a"
        );

        // Deeply nested list -> list -> object with duplicate
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut object = inner_list.new_object().with_field("x", 1);
        let nested_result = object.try_insert("x", 2);
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );
        let nested_result = object.try_new_list("x");
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );

        let nested_result = object.try_new_object("x");
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );

        drop(object);
        inner_list.finish();
        outer_list.finish();

        // Valid object should succeed
        let mut list = builder.new_list();
        let mut valid_obj = list.new_object();
        valid_obj.insert("m", 1);
        valid_obj.insert("n", 2);

        valid_obj.finish();
        list.finish();
    }
}
