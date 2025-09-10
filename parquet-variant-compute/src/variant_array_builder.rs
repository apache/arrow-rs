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

//! [`VariantArrayBuilder`] implementation

use crate::VariantArray;
use arrow::array::{ArrayRef, BinaryViewArray, BinaryViewBuilder, NullBufferBuilder, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Fields};
use parquet_variant::{
    BuilderSpecificState, ListBuilder, MetadataBuilder, ObjectBuilder, Variant, VariantBuilderExt,
    EMPTY_VARIANT_METADATA,
};
use parquet_variant::{
    ParentState, ReadOnlyMetadataBuilder, ValueBuilder, WritableMetadataBuilder,
};
use std::sync::Arc;

/// A builder for [`VariantArray`]
///
/// This builder is used to construct a `VariantArray` and allows APIs for
/// adding metadata
///
/// This builder always creates a `VariantArray` using [`BinaryViewArray`] for both
/// the metadata and value fields.
///
/// # TODO
/// 1. Support shredding: <https://github.com/apache/arrow-rs/issues/7895>
///
/// ## Example:
/// ```
/// # use arrow::array::Array;
/// # use parquet_variant::{Variant, VariantBuilder, VariantBuilderExt};
/// # use parquet_variant_compute::VariantArrayBuilder;
/// // Create a new VariantArrayBuilder with a capacity of 100 rows
/// let mut builder = VariantArrayBuilder::new(100);
/// // append variant values
/// builder.append_variant(Variant::from(42));
/// // append a null row (note not a Variant::Null)
/// builder.append_null();
/// // append an object to the builder using VariantBuilderExt methods directly
/// builder.new_object()
///   .with_field("foo", "bar")
///   .finish();
///
/// // create the final VariantArray
/// let variant_array = builder.build();
/// assert_eq!(variant_array.len(), 3);
/// // // Access the values
/// // row 1 is not null and is an integer
/// assert!(!variant_array.is_null(0));
/// assert_eq!(variant_array.value(0), Variant::from(42i32));
/// // row 1 is null
/// assert!(variant_array.is_null(1));
/// // row 2 is not null and is an object
/// assert!(!variant_array.is_null(2));
/// let value = variant_array.value(2);
/// let obj = value.as_object().expect("expected object");
/// assert_eq!(obj.get("foo"), Some(Variant::from("bar")));
/// ```
#[derive(Debug)]
pub struct VariantArrayBuilder {
    /// Nulls
    nulls: NullBufferBuilder,
    /// builder for all the metadata
    metadata_builder: WritableMetadataBuilder,
    /// ending offset for each serialized metadata dictionary in the buffer
    metadata_offsets: Vec<usize>,
    /// builder for values
    value_builder: ValueBuilder,
    /// ending offset for each serialized variant value in the buffer
    value_offsets: Vec<usize>,
    /// The fields of the final `StructArray`
    ///
    /// TODO: 1) Add extension type metadata
    /// TODO: 2) Add support for shredding
    fields: Fields,
}

impl VariantArrayBuilder {
    pub fn new(row_capacity: usize) -> Self {
        // The subfields are expected to be non-nullable according to the parquet variant spec.
        let metadata_field = Field::new("metadata", DataType::BinaryView, false);
        let value_field = Field::new("value", DataType::BinaryView, false);

        Self {
            nulls: NullBufferBuilder::new(row_capacity),
            metadata_builder: WritableMetadataBuilder::default(),
            metadata_offsets: Vec::with_capacity(row_capacity),
            value_builder: ValueBuilder::new(),
            value_offsets: Vec::with_capacity(row_capacity),
            fields: Fields::from(vec![metadata_field, value_field]),
        }
    }

    /// Build the final builder
    pub fn build(self) -> VariantArray {
        let Self {
            mut nulls,
            metadata_builder,
            metadata_offsets,
            value_builder,
            value_offsets,
            fields,
        } = self;

        let metadata_buffer = metadata_builder.into_inner();
        let metadata_array = binary_view_array_from_buffers(metadata_buffer, metadata_offsets);

        let value_buffer = value_builder.into_inner();
        let value_array = binary_view_array_from_buffers(value_buffer, value_offsets);

        // The build the final struct array
        let inner = StructArray::new(
            fields,
            vec![
                Arc::new(metadata_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
            nulls.finish(),
        );
        // TODO add arrow extension type metadata

        VariantArray::try_new(Arc::new(inner)).expect("valid VariantArray by construction")
    }

    /// Appends a null row to the builder.
    pub fn append_null(&mut self) {
        self.nulls.append_null();
        // The subfields are expected to be non-nullable according to the parquet variant spec.
        self.metadata_offsets.push(self.metadata_builder.offset());
        self.value_offsets.push(self.value_builder.offset());
    }

    /// Append the [`Variant`] to the builder as the next row
    pub fn append_variant(&mut self, variant: Variant) {
        ValueBuilder::append_variant(self.parent_state(), variant);
    }

    /// Creates a builder-specific parent state
    fn parent_state(&mut self) -> ParentState<'_, ArrayBuilderState<'_>> {
        let state = ArrayBuilderState {
            metadata_offsets: &mut self.metadata_offsets,
            value_offsets: &mut self.value_offsets,
            nulls: &mut self.nulls,
        };

        ParentState::new(&mut self.value_builder, &mut self.metadata_builder, state)
    }
}

/// Builder-specific state for array building that manages array-level offsets and nulls. See
/// [`VariantBuilderExt`] for details.
#[derive(Debug)]
pub struct ArrayBuilderState<'a> {
    metadata_offsets: &'a mut Vec<usize>,
    value_offsets: &'a mut Vec<usize>,
    nulls: &'a mut NullBufferBuilder,
}

// All changes are pending until finalized
impl BuilderSpecificState for ArrayBuilderState<'_> {
    fn finish(
        &mut self,
        metadata_builder: &mut dyn MetadataBuilder,
        value_builder: &mut ValueBuilder,
    ) {
        self.metadata_offsets.push(metadata_builder.finish());
        self.value_offsets.push(value_builder.offset());
        self.nulls.append_non_null();
    }
}

impl VariantBuilderExt for VariantArrayBuilder {
    type State<'a>
        = ArrayBuilderState<'a>
    where
        Self: 'a;

    /// Appending NULL to a variant array produces an actual NULL value
    fn append_null(&mut self) {
        self.append_null();
    }

    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.append_variant(value.into());
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(ListBuilder::new(self.parent_state(), false))
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(ObjectBuilder::new(self.parent_state(), false))
    }
}

/// A builder for creating only the value column of a [`VariantArray`]
///
/// This builder is used when you have existing metadata and only need to build
/// the value column. It's useful for scenarios like variant unshredding, data
/// transformation, or filtering where you want to reuse existing metadata.
///
/// The builder produces a [`BinaryViewArray`] that can be combined with existing
/// metadata to create a complete [`VariantArray`].
///
/// # Example:
/// ```
/// # use arrow::array::Array;
/// # use parquet_variant::{Variant, EMPTY_VARIANT_METADATA};
/// # use parquet_variant_compute::VariantValueArrayBuilder;
/// // Create a variant value builder for 10 rows
/// let mut builder = VariantValueArrayBuilder::new(10);
///
/// // Append some values with their corresponding metadata
/// // In practice, you should use the existing metadata you have access to.
/// builder.append_value(Variant::from(42), EMPTY_VARIANT_METADATA).unwrap();
/// builder.append_null();
/// builder.append_value(Variant::from("hello"), EMPTY_VARIANT_METADATA).unwrap();
///
/// // Build the final value array
/// let value_array = builder.build();
/// assert_eq!(value_array.len(), 3);
/// ```
#[derive(Debug)]
#[allow(unused)]
pub struct VariantValueArrayBuilder {
    value_builder: ValueBuilder,
    value_offsets: Vec<usize>,
    nulls: NullBufferBuilder,
}

#[allow(unused)]
impl VariantValueArrayBuilder {
    /// Create a new `VariantValueArrayBuilder` with the specified row capacity
    pub fn new(row_capacity: usize) -> Self {
        Self {
            value_builder: ValueBuilder::new(),
            value_offsets: Vec::with_capacity(row_capacity),
            nulls: NullBufferBuilder::new(row_capacity),
        }
    }

    /// Build the final value array
    ///
    /// Returns a [`BinaryViewArray`] containing the serialized variant values.
    /// This can be combined with existing metadata to create a complete [`VariantArray`].
    pub fn build(mut self) -> Result<BinaryViewArray, ArrowError> {
        let value_buffer = self.value_builder.into_inner();
        let mut array = binary_view_array_from_buffers(value_buffer, self.value_offsets);
        if let Some(nulls) = self.nulls.finish() {
            let (views, buffers, _) = array.into_parts();
            array = BinaryViewArray::try_new(views, buffers, Some(nulls))?;
        }
        Ok(array)
    }

    /// Append a null row to the builder
    ///
    /// WARNING: It is only safe to call this method when building the `value` field of a shredded
    /// variant column (which is nullable). The `value` field of a binary (unshredded) variant
    /// column is non-nullable, and callers should instead invoke [`Self::append_value`] with
    /// `Variant::Null`, passing the appropriate metadata value.
    pub fn append_null(&mut self) {
        self.value_offsets.push(self.value_builder.offset());
        self.nulls.append_null();
    }

    /// Append a variant value with its corresponding metadata
    ///
    /// # Arguments
    /// * `value` - The variant value to append
    /// * `metadata` - The metadata dictionary for this variant (used for field name resolution)
    ///
    /// # Returns
    /// * `Ok(())` if the value was successfully appended
    /// * `Err(ArrowError)` if the variant contains field names not found in the metadata
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, EMPTY_VARIANT_METADATA};
    /// # use parquet_variant_compute::VariantValueArrayBuilder;
    /// let mut builder = VariantValueArrayBuilder::new(10);
    /// builder.append_value(Variant::from(42), EMPTY_VARIANT_METADATA).unwrap();
    /// ```
    pub fn append_value(&mut self, value: Variant<'_, '_>) {
        let metadata = value.metadata().cloned().unwrap_or(EMPTY_VARIANT_METADATA);
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
        ValueBuilder::append_variant_bytes(self.parent_state(&mut metadata_builder), value);
    }

    /// Creates a builder-specific parent state
    pub fn parent_state<'a>(
        &'a mut self,
        metadata_builder: &'a mut dyn MetadataBuilder,
    ) -> ParentState<'a, ValueArrayBuilderState<'a>> {
        let state = ValueArrayBuilderState {
            value_offsets: &mut self.value_offsets,
            nulls: &mut self.nulls,
        };

        ParentState::new(&mut self.value_builder, metadata_builder, state)
    }
}

/// Builder-specific state for array building that manages array-level offsets and nulls. See
/// [`VariantBuilderExt`] for details.
#[derive(Debug)]
pub struct ValueArrayBuilderState<'a> {
    value_offsets: &'a mut Vec<usize>,
    nulls: &'a mut NullBufferBuilder,
}

// All changes are pending until finalized
impl BuilderSpecificState for ValueArrayBuilderState<'_> {
    fn finish(
        &mut self,
        _metadata_builder: &mut dyn MetadataBuilder,
        value_builder: &mut ValueBuilder,
    ) {
        self.value_offsets.push(value_builder.offset());
        self.nulls.append_non_null();
    }
}

fn binary_view_array_from_buffers(buffer: Vec<u8>, offsets: Vec<usize>) -> BinaryViewArray {
    // All offsets are less than or equal to the buffer length, so we can safely cast all offsets
    // inside the loop below, as long as the buffer length fits in u32.
    u32::try_from(buffer.len()).expect("buffer length should fit in u32");

    let mut builder = BinaryViewBuilder::with_capacity(offsets.len());
    let block = builder.append_block(buffer.into());
    // TODO this can be much faster if it creates the views directly during append
    let mut start = 0;
    for end in offsets {
        let end = end as u32; // Safe cast: validated max offset fits in u32 above
        builder
            .try_append_view(block, start, end - start)
            .expect("Failed to append view");
        start = end;
    }
    builder.finish()
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Array;
    use parquet_variant::{Variant, VariantBuilder, VariantMetadata};

    /// Test that both the metadata and value buffers are non nullable
    #[test]
    fn test_variant_array_builder_non_nullable() {
        let mut builder = VariantArrayBuilder::new(10);
        builder.append_null(); // should not panic
        builder.append_variant(Variant::from(42i32));
        let variant_array = builder.build();

        assert_eq!(variant_array.len(), 2);
        assert!(variant_array.is_null(0));
        assert!(!variant_array.is_null(1));
        assert_eq!(variant_array.value(1), Variant::from(42i32));

        // the metadata and value fields of non shredded variants should not be null
        assert!(variant_array.metadata_field().nulls().is_none());
        assert!(variant_array.value_field().unwrap().nulls().is_none());
        let DataType::Struct(fields) = variant_array.data_type() else {
            panic!("Expected VariantArray to have Struct data type");
        };
        for field in fields {
            assert!(
                !field.is_nullable(),
                "Field {} should be non-nullable",
                field.name()
            );
        }
    }

    /// Test using appending variants to the array builder
    #[test]
    fn test_variant_array_builder() {
        let mut builder = VariantArrayBuilder::new(10);
        builder.append_null(); // should not panic
        builder.append_variant(Variant::from(42i32));

        // make an object in the next row
        builder.new_object().with_field("foo", "bar").finish();

        // append a new list
        builder
            .new_list()
            .with_value(Variant::from(1i32))
            .with_value(Variant::from(2i32))
            .finish();
        let variant_array = builder.build();

        assert_eq!(variant_array.len(), 4);
        assert!(variant_array.is_null(0));
        assert!(!variant_array.is_null(1));
        assert_eq!(variant_array.value(1), Variant::from(42i32));
        assert!(!variant_array.is_null(2));
        let variant = variant_array.value(2);
        let variant = variant.as_object().expect("variant to be an object");
        assert_eq!(variant.get("foo").unwrap(), Variant::from("bar"));
        assert!(!variant_array.is_null(3));
        let variant = variant_array.value(3);
        let list = variant.as_list().expect("variant to be a list");
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_variant_value_array_builder_basic() {
        let mut builder = VariantValueArrayBuilder::new(10);

        // Append some values
        builder.append_value(Variant::from(42i32));
        builder.append_null();
        builder.append_value(Variant::from("hello"));

        let value_array = builder.build().unwrap();
        assert_eq!(value_array.len(), 3);
    }

    #[test]
    fn test_variant_value_array_builder_with_objects() {
        // Create metadata with field names
        let mut metadata_builder = WritableMetadataBuilder::default();
        metadata_builder.upsert_field_name("name");
        metadata_builder.upsert_field_name("age");
        metadata_builder.finish();
        let metadata_bytes = metadata_builder.into_inner();
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create a variant with an object using the same metadata
        let mut variant_builder = VariantBuilder::new().with_metadata(metadata);
        variant_builder
            .new_object()
            .with_field("name", "Alice")
            .with_field("age", 30i32)
            .finish();
        let (_, value_bytes) = variant_builder.finish();
        let variant = Variant::try_new(&metadata_bytes, &value_bytes).unwrap();

        // Now use the value array builder
        let mut builder = VariantValueArrayBuilder::new(10);
        builder.append_value(variant);
        builder.append_null();

        let value_array = builder.build().unwrap();
        assert_eq!(value_array.len(), 2);
    }
}
