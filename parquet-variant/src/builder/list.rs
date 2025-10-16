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

use crate::{
    BASIC_TYPE_BITS, BuilderSpecificState, ParentState, ValueBuilder, Variant, VariantBuilderExt,
    builder::{metadata::MetadataBuilder, object::ObjectBuilder},
    decoder::VariantBasicType,
    int_size,
};
use arrow_schema::ArrowError;

fn array_header(large: bool, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Array as u8
}

/// Append `value_size` bytes of given `value` into `dest`.
fn append_packed_u32(dest: &mut Vec<u8>, value: u32, value_size: usize) {
    let n = dest.len() + value_size;
    dest.extend(value.to_le_bytes());
    dest.truncate(n);
}

/// A builder for creating [`Variant::List`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
///
/// [`VariantBuilder`]: crate::VariantBuilder
#[derive(Debug)]
pub struct ListBuilder<'a, S: BuilderSpecificState> {
    parent_state: ParentState<'a, S>,
    offsets: Vec<usize>,
    validate_unique_fields: bool,
}

impl<'a, S: BuilderSpecificState> ListBuilder<'a, S> {
    /// Creates a new list builder, nested on top of the given parent state.
    pub fn new(parent_state: ParentState<'a, S>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            offsets: vec![],
            validate_unique_fields,
        }
    }

    /// Enables unique field key validation for objects created within this list.
    ///
    /// Propagates the validation flag to any [`ObjectBuilder`]s created using
    /// [`ListBuilder::new_object`].
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    // Returns validate_unique_fields because we can no longer reference self once this method returns.
    fn parent_state(&mut self) -> (ParentState<'_, ListState<'_>>, bool) {
        let state = ParentState::list(
            self.parent_state.value_builder,
            self.parent_state.metadata_builder,
            &mut self.offsets,
            self.parent_state.saved_value_builder_offset,
        );
        (state, self.validate_unique_fields)
    }

    /// Returns an object builder that can be used to append a new (nested) object to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object(&mut self) -> ObjectBuilder<'_, ListState<'_>> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ObjectBuilder::new(parent_state, validate_unique_fields)
    }

    /// Returns a list builder that can be used to append a new (nested) list to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list(&mut self) -> ListBuilder<'_, ListState<'_>> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ListBuilder::new(parent_state, validate_unique_fields)
    }

    /// Appends a variant to the list.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ListBuilder::try_append_value`].
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        let (state, _) = self.parent_state();
        ValueBuilder::append_variant(state, value.into())
    }

    /// Appends a new primitive value to this list
    pub fn try_append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        value: T,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state();
        ValueBuilder::try_append_variant(state, value.into())
    }

    /// Appends a variant value to this list by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy. For other variant types, this falls back to the standard append
    /// behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended.
    pub fn append_value_bytes<'m, 'd>(&mut self, value: impl Into<Variant<'m, 'd>>) {
        let (state, _) = self.parent_state();
        ValueBuilder::append_variant_bytes(state, value.into())
    }

    /// Builder-style API for appending a value to the list and returning self to enable method chaining.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ListBuilder::try_with_value`].
    pub fn with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(mut self, value: T) -> Self {
        self.append_value(value);
        self
    }

    /// Builder-style API for appending a value to the list and returns self for method chaining.
    ///
    /// This is the fallible version of [`ListBuilder::with_value`].
    pub fn try_with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        mut self,
        value: T,
    ) -> Result<Self, ArrowError> {
        self.try_append_value(value)?;
        Ok(self)
    }

    /// Finalizes this list and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) {
        let starting_offset = self.parent_state.saved_value_builder_offset;
        let value_builder = self.parent_state.value_builder();

        let data_size = value_builder
            .offset()
            .checked_sub(starting_offset)
            .expect("Data size overflowed usize");

        let num_elements = self.offsets.len();
        let is_large = num_elements > u8::MAX as usize;
        let offset_size = int_size(data_size);

        let num_elements_size = if is_large { 4 } else { 1 }; // is_large: 4 bytes, else 1 byte.
        let num_elements = self.offsets.len();
        let header_size = 1 +      // header (i.e., `array_header`)
            num_elements_size +  // num_element_size
            (num_elements + 1) * offset_size as usize; // offsets and data size

        // Calculated header size becomes a hint; being wrong only risks extra allocations.
        // Make sure to reserve enough capacity to handle the extra bytes we'll truncate.
        let mut bytes_to_splice = Vec::with_capacity(header_size + 3);
        // Write header
        let header = array_header(is_large, offset_size);
        bytes_to_splice.push(header);

        append_packed_u32(&mut bytes_to_splice, num_elements as u32, num_elements_size);

        for offset in &self.offsets {
            append_packed_u32(&mut bytes_to_splice, *offset as u32, offset_size as usize);
        }

        append_packed_u32(&mut bytes_to_splice, data_size as u32, offset_size as usize);

        value_builder
            .inner_mut()
            .splice(starting_offset..starting_offset, bytes_to_splice);

        self.parent_state.finish();
    }
}

impl<'a, S: BuilderSpecificState> VariantBuilderExt for ListBuilder<'a, S> {
    type State<'s>
        = ListState<'s>
    where
        Self: 's;

    /// Variant arrays cannot encode NULL values, only `Variant::Null`.
    fn append_null(&mut self) {
        self.append_value(Variant::Null);
    }
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.append_value(value);
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(self.new_list())
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(self.new_object())
    }
}

impl<'a, 'm, 'v, S, V> Extend<V> for ListBuilder<'a, S>
where
    S: BuilderSpecificState,
    V: Into<Variant<'m, 'v>>,
{
    fn extend<T: IntoIterator<Item = V>>(&mut self, iter: T) {
        for v in iter.into_iter() {
            self.append_value(v);
        }
    }
}

/// Internal state for list building
#[derive(Debug)]
pub struct ListState<'a> {
    offsets: &'a mut Vec<usize>,
    saved_offsets_size: usize,
}

// `ListBuilder::finish()` eagerly updates the list offsets, which we should rollback on failure.
impl BuilderSpecificState for ListState<'_> {
    fn rollback(&mut self) {
        self.offsets.truncate(self.saved_offsets_size);
    }
}

impl<'a> ParentState<'a, ListState<'a>> {
    /// Creates a new instance suitable for a [`ListBuilder`]. The value and metadata builder state
    /// is checkpointed and will roll back on drop, unless [`Self::finish`] is called. The new
    /// element's offset is also captured eagerly and will also roll back if not finished.
    pub fn list(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
        offsets: &'a mut Vec<usize>,
        saved_parent_value_builder_offset: usize,
    ) -> Self {
        // The saved_parent_buffer_offset is the buffer size as of when the parent builder was
        // constructed. The saved_buffer_offset is the buffer size as of now (when a child builder
        // is created). The variant field_offset entry for this list element is their difference.
        let saved_value_builder_offset = value_builder.offset();
        let saved_offsets_size = offsets.len();
        offsets.push(saved_value_builder_offset - saved_parent_value_builder_offset);

        let builder_state = ListState {
            offsets,
            saved_offsets_size,
        };
        Self {
            saved_metadata_builder_dict_size: metadata_builder.num_field_names(),
            saved_value_builder_offset,
            metadata_builder,
            value_builder,
            builder_state,
            finished: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ShortString, ValueBuilder, VariantBuilder, VariantMetadata,
        builder::metadata::ReadOnlyMetadataBuilder,
    };

    use super::*;

    #[test]
    fn test_list() {
        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value(1i8)
            .with_value(2i8)
            .with_value("test")
            .finish();

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());

        let variant = Variant::try_new(&metadata, &value).unwrap();

        match variant {
            Variant::List(list) => {
                let val0 = list.get(0).unwrap();
                assert_eq!(val0, Variant::Int8(1));

                let val1 = list.get(1).unwrap();
                assert_eq!(val1, Variant::Int8(2));

                let val2 = list.get(2).unwrap();
                assert_eq!(val2, Variant::ShortString(ShortString("test")));
            }
            _ => panic!("Expected an array variant, got: {variant:?}"),
        }
    }

    #[test]
    fn test_nested_list() {
        let mut builder = VariantBuilder::new();

        let mut outer_list_builder = builder.new_list();

        // create inner list
        outer_list_builder
            .new_list()
            .with_value("a")
            .with_value("b")
            .with_value("c")
            .with_value("d")
            .finish();

        outer_list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_list = variant.as_list().unwrap();

        assert_eq!(outer_list.len(), 1);

        let inner_variant = outer_list.get(0).unwrap();
        let inner_list = inner_variant.as_list().unwrap();

        assert_eq!(
            vec![
                Variant::from("a"),
                Variant::from("b"),
                Variant::from("c"),
                Variant::from("d"),
            ],
            inner_list.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_super_nested_list() {
        /*
        [[[[[1]]]]]
        */

        let mut builder = VariantBuilder::new();
        {
            let mut list_builder1 = builder.new_list();
            {
                let mut list_builder2 = list_builder1.new_list();
                {
                    let mut list_builder3 = list_builder2.new_list();
                    {
                        let mut list_builder4 = list_builder3.new_list();
                        {
                            let mut list_builder5 = list_builder4.new_list();
                            list_builder5.append_value(1);
                            list_builder5.finish();
                        }
                        list_builder4.finish();
                    }
                    list_builder3.finish();
                }
                list_builder2.finish();
            }
            list_builder1.finish();
        }

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list1 = variant.as_list().unwrap();
        assert_eq!(list1.len(), 1);

        let list2_variant = list1.get(0).unwrap();
        let list2 = list2_variant.as_list().unwrap();
        assert_eq!(list2.len(), 1);

        let list3_variant = list2.get(0).unwrap();
        let list3 = list3_variant.as_list().unwrap();
        assert_eq!(list3.len(), 1);

        let list4_variant = list3.get(0).unwrap();
        let list4 = list4_variant.as_list().unwrap();
        assert_eq!(list4.len(), 1);

        let list5_variant = list4.get(0).unwrap();
        let list5 = list5_variant.as_list().unwrap();
        assert_eq!(list5.len(), 1);

        assert_eq!(list5.len(), 1);

        assert_eq!(list5.get(0).unwrap(), Variant::from(1));
    }

    #[test]
    fn test_list_append_bytes_subset() {
        // Create an original list
        let mut builder = VariantBuilder::new();
        {
            let mut list = builder.new_list();
            list.append_value("item1");
            list.append_value(42i32);
            list.append_value(true);
            list.append_value("item4");
            list.append_value(1.234f64);
            list.finish();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_list = original_variant.as_list().unwrap();

        // Create a new list copying subset of elements interleaved with new ones
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(&metadata2);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata2);
        {
            let mut list = ListBuilder::new(state, true);

            // Copy first element using bytes API
            list.append_value_bytes(original_list.get(0).unwrap());

            // Add new element
            list.append_value("new_item");

            // Copy third element using bytes API
            list.append_value_bytes(original_list.get(2).unwrap());

            // Add another new element
            list.append_value(99i32);

            // Copy last element using bytes API
            list.append_value_bytes(original_list.get(4).unwrap());

            list.finish();
        }
        let value2 = builder2.into_inner();
        let result_variant = Variant::try_new(&metadata1, &value2).unwrap();
        let result_list = result_variant.as_list().unwrap();

        // Verify the list contains expected elements
        assert_eq!(result_list.len(), 5);
        assert_eq!(result_list.get(0).unwrap().as_string().unwrap(), "item1");
        assert_eq!(result_list.get(1).unwrap().as_string().unwrap(), "new_item");
        assert!(result_list.get(2).unwrap().as_boolean().unwrap());
        assert_eq!(result_list.get(3).unwrap().as_int32().unwrap(), 99);
        assert_eq!(result_list.get(4).unwrap().as_f64().unwrap(), 1.234);
    }

    #[test]
    fn test_append_list() {
        let (m1, v1) = make_list();
        let variant = Variant::new(&m1, &v1);
        let mut builder = VariantBuilder::new();
        builder.append_value(variant.clone());
        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    /// make a simple List variant
    fn make_list() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value(1234)
            .with_value("a string value")
            .finish();

        builder.finish()
    }

    #[test]
    fn test_append_nested_list() {
        let (m1, v1) = make_nested_list();
        let variant = Variant::new(&m1, &v1);
        let mut builder = VariantBuilder::new();
        builder.append_value(variant.clone());
        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    fn make_nested_list() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        let mut list = builder.new_list();

        //create inner list
        list.new_list()
            .with_value("the dog licked the oil")
            .with_value(4.3)
            .finish();

        list.finish();

        builder.finish()
    }

    #[test]
    fn test_object_list() {
        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        list_builder
            .new_object()
            .with_field("id", 1)
            .with_field("type", "Cauliflower")
            .finish();

        list_builder
            .new_object()
            .with_field("id", 2)
            .with_field("type", "Beets")
            .finish();

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();

        assert_eq!(list.len(), 2);

        let obj1_variant = list.get(0).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();

        assert_eq!(
            vec![
                ("id", Variant::from(1)),
                ("type", Variant::from("Cauliflower")),
            ],
            obj1.iter().collect::<Vec<_>>()
        );

        let obj2_variant = list.get(1).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();

        assert_eq!(
            vec![("id", Variant::from(2)), ("type", Variant::from("Beets")),],
            obj2.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_object_list2() {
        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        list_builder.new_object().with_field("a", 1).finish();

        list_builder.new_object().with_field("b", 2).finish();

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 2);

        let obj1_variant = list.get(0).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();
        assert_eq!(
            vec![("a", Variant::from(1)),],
            obj1.iter().collect::<Vec<_>>()
        );

        let obj2_variant = list.get(1).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();
        assert_eq!(
            vec![("b", Variant::from(2)),],
            obj2.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_hetergenous_list() {
        /*
        [
            1,
            { "a": 1 },
            2,
            { "b": 2},
            3
        ]
        */

        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        list_builder.append_value(1);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("a", 1);
            object_builder.finish();
        }

        list_builder.append_value(2);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2);
            object_builder.finish();
        }

        list_builder.append_value(3);

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 5);
        assert_eq!(list.get(0).unwrap(), Variant::from(1));

        let obj1_variant = list.get(1).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();
        assert_eq!(
            vec![("a", Variant::from(1)),],
            obj1.iter().collect::<Vec<_>>()
        );

        assert_eq!(list.get(2).unwrap(), Variant::from(2));

        let obj2_variant = list.get(3).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();
        assert_eq!(
            vec![("b", Variant::from(2)),],
            obj2.iter().collect::<Vec<_>>()
        );

        assert_eq!(list.get(4).unwrap(), Variant::from(3));
    }

    // This test wants to cover the logic for reuse parent buffer for list builder
    // the builder looks like
    // [ "apple", "false", [{"a": "b", "b": "c"}, {"c":"d", "d":"e"}], [[1, true], ["tree", false]], 1]
    #[test]
    fn test_nested_list_with_heterogeneous_fields_for_buffer_reuse() {
        let mut builder = VariantBuilder::new();

        {
            let mut outer_list_builder = builder.new_list();

            outer_list_builder.append_value("apple");
            outer_list_builder.append_value(false);

            {
                // the list here wants to cover the logic object builder inside list builder
                let mut inner_list_builder = outer_list_builder.new_list();

                {
                    let mut inner_object_builder = inner_list_builder.new_object();
                    inner_object_builder.insert("a", "b");
                    inner_object_builder.insert("b", "c");
                    inner_object_builder.finish();
                }

                {
                    // the seconde object builder here wants to cover the logic for
                    // list builder resue the parent buffer.
                    let mut inner_object_builder = inner_list_builder.new_object();
                    inner_object_builder.insert("c", "d");
                    inner_object_builder.insert("d", "e");
                    inner_object_builder.finish();
                }

                inner_list_builder.finish();
            }

            {
                // the list here wants to cover the logic list builder inside list builder
                let mut inner_list_builder = outer_list_builder.new_list();

                {
                    let mut double_inner_list_builder = inner_list_builder.new_list();
                    double_inner_list_builder.append_value(1);
                    double_inner_list_builder.append_value(true);

                    double_inner_list_builder.finish();
                }

                {
                    let mut double_inner_list_builder = inner_list_builder.new_list();
                    double_inner_list_builder.append_value("tree");
                    double_inner_list_builder.append_value(false);

                    double_inner_list_builder.finish();
                }
                inner_list_builder.finish();
            }

            outer_list_builder.append_value(1);

            outer_list_builder.finish();
        }

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_list = variant.as_list().unwrap();

        assert_eq!(5, outer_list.len());

        // Primitive value
        assert_eq!(Variant::from("apple"), outer_list.get(0).unwrap());
        assert_eq!(Variant::from(false), outer_list.get(1).unwrap());
        assert_eq!(Variant::from(1), outer_list.get(4).unwrap());

        // The first inner list [{"a": "b", "b": "c"}, {"c":"d", "d":"e"}]
        let list1_variant = outer_list.get(2).unwrap();
        let list1 = list1_variant.as_list().unwrap();
        assert_eq!(2, list1.len());

        let list1_obj1_variant = list1.get(0).unwrap();
        let list1_obj1 = list1_obj1_variant.as_object().unwrap();
        assert_eq!("a", list1_obj1.field_name(0).unwrap());
        assert_eq!(Variant::from("b"), list1_obj1.field(0).unwrap());

        assert_eq!("b", list1_obj1.field_name(1).unwrap());
        assert_eq!(Variant::from("c"), list1_obj1.field(1).unwrap());

        // The second inner list [[1, true], ["tree", false]]
        let list2_variant = outer_list.get(3).unwrap();
        let list2 = list2_variant.as_list().unwrap();
        assert_eq!(2, list2.len());

        // The list [1, true]
        let list2_list1_variant = list2.get(0).unwrap();
        let list2_list1 = list2_list1_variant.as_list().unwrap();
        assert_eq!(2, list2_list1.len());
        assert_eq!(Variant::from(1), list2_list1.get(0).unwrap());
        assert_eq!(Variant::from(true), list2_list1.get(1).unwrap());

        // The list ["true", false]
        let list2_list2_variant = list2.get(1).unwrap();
        let list2_list2 = list2_list2_variant.as_list().unwrap();
        assert_eq!(2, list2_list2.len());
        assert_eq!(Variant::from("tree"), list2_list2.get(0).unwrap());
        assert_eq!(Variant::from(false), list2_list2.get(1).unwrap());
    }
}
