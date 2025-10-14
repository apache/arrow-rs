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
    VariantMetadata,
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
/// # use parquet_variant::ShortString;
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
/// // bulk insert a list of values
/// // `Option::None` is a null value
/// builder.extend([None, Some(Variant::from("norm"))]);
///
/// // create the final VariantArray
/// let variant_array = builder.build();
/// assert_eq!(variant_array.len(), 5);
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
/// // row 3 is null
/// assert!(variant_array.is_null(3));
/// // row 4 is not null and is a short string
/// assert!(!variant_array.is_null(4));
/// let value = variant_array.value(4);
/// assert_eq!(value, Variant::ShortString(ShortString::try_new("norm").unwrap()));
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

        VariantArray::try_new(&inner).expect("valid VariantArray by construction")
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

impl<'m, 'v> Extend<Option<Variant<'m, 'v>>> for VariantArrayBuilder {
    fn extend<T: IntoIterator<Item = Option<Variant<'m, 'v>>>>(&mut self, iter: T) {
        for v in iter {
            match v {
                Some(v) => self.append_variant(v),
                None => self.append_null(),
            }
        }
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
/// # use parquet_variant::{Variant};
/// # use parquet_variant_compute::VariantValueArrayBuilder;
/// // Create a variant value builder for 10 rows
/// let mut builder = VariantValueArrayBuilder::new(10);
///
/// // Append some values with their corresponding metadata, which the
/// // builder takes advantage of to avoid creating new metadata.
/// builder.append_value(Variant::from(42));
/// builder.append_null();
/// builder.append_value(Variant::from("hello"));
///
/// // Build the final value array
/// let value_array = builder.build().unwrap();
/// assert_eq!(value_array.len(), 3);
/// ```
#[derive(Debug)]
pub struct VariantValueArrayBuilder {
    value_builder: ValueBuilder,
    value_offsets: Vec<usize>,
    nulls: NullBufferBuilder,
}

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
    /// WARNING: It is only valid to call this method when building the `value` field of a shredded
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
    /// # use parquet_variant::Variant;
    /// # use parquet_variant_compute::VariantValueArrayBuilder;
    /// let mut builder = VariantValueArrayBuilder::new(10);
    /// builder.append_value(Variant::from(42));
    /// ```
    pub fn append_value(&mut self, value: Variant<'_, '_>) {
        // NOTE: Have to clone because the builder consumes `value`
        self.builder_ext(&value.metadata().clone())
            .append_value(value);
    }

    /// Creates a builder-specific parent state.
    ///
    /// For example, this can be useful for code that wants to copy a subset of fields from an
    /// object `value` as a new row of `value_array_builder`:
    ///
    /// ```no_run
    /// # use parquet_variant::{ObjectBuilder, ReadOnlyMetadataBuilder, Variant};
    /// # use parquet_variant_compute::VariantValueArrayBuilder;
    /// # let value = Variant::Null;
    /// # let mut value_array_builder = VariantValueArrayBuilder::new(0);
    /// # fn should_keep(field_name: &str) -> bool { todo!() };
    /// let Variant::Object(obj) = value else {
    ///     panic!("Not a variant object");
    /// };
    /// let mut metadata_builder = ReadOnlyMetadataBuilder::new(&obj.metadata);
    /// let state = value_array_builder.parent_state(&mut metadata_builder);
    /// let mut object_builder = ObjectBuilder::new(state, false);
    /// for (field_name, field_value) in obj.iter() {
    ///     if should_keep(field_name) {
    ///         object_builder.insert_bytes(field_name, field_value);
    ///     }
    /// }
    ///  object_builder.finish(); // appends the filtered object
    /// ```
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

    /// Creates a thin [`VariantBuilderExt`] wrapper for this builder, which hides the `metadata`
    /// parameter (similar to the way [`parquet_variant::ObjectFieldBuilder`] hides field names).
    pub fn builder_ext<'a>(
        &'a mut self,
        metadata: &'a VariantMetadata<'a>,
    ) -> VariantValueArrayBuilderExt<'a> {
        VariantValueArrayBuilderExt {
            metadata_builder: ReadOnlyMetadataBuilder::new(metadata),
            value_builder: self,
        }
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

/// A thin [`VariantBuilderExt`] wrapper that hides the short-lived (per-row)
/// [`ReadOnlyMetadataBuilder`] instances that [`VariantValueArrayBuilder`] requires.
pub struct VariantValueArrayBuilderExt<'a> {
    metadata_builder: ReadOnlyMetadataBuilder<'a>,
    value_builder: &'a mut VariantValueArrayBuilder,
}

impl<'a> VariantValueArrayBuilderExt<'a> {
    /// Creates a new instance from a metadata builder and a reference to a variant value builder.
    pub fn new(
        metadata_builder: ReadOnlyMetadataBuilder<'a>,
        value_builder: &'a mut VariantValueArrayBuilder,
    ) -> Self {
        Self {
            metadata_builder,
            value_builder,
        }
    }
}

impl<'a> VariantBuilderExt for VariantValueArrayBuilderExt<'a> {
    type State<'b>
        = ValueArrayBuilderState<'b>
    where
        Self: 'b;

    fn append_null(&mut self) {
        self.value_builder.append_null()
    }

    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        let state = self.value_builder.parent_state(&mut self.metadata_builder);
        ValueBuilder::append_variant_bytes(state, value.into());
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError> {
        let state = self.value_builder.parent_state(&mut self.metadata_builder);
        Ok(ListBuilder::new(state, false))
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        let state = self.value_builder.parent_state(&mut self.metadata_builder);
        Ok(ObjectBuilder::new(state, false))
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
    use parquet_variant::{ShortString, Variant};

    /// Test that both the metadata and value buffers are non nullable
    #[test]
    fn test_variant_array_builder_non_nullable() {
        let mut builder = VariantArrayBuilder::new(10);

        builder.extend([
            None, // should not panic
            Some(Variant::from(42_i32)),
        ]);

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
    fn test_extend_variant_array_builder() {
        let mut b = VariantArrayBuilder::new(3);
        b.extend([None, Some(Variant::Null), Some(Variant::from("norm"))]);

        let variant_array = b.build();

        assert_eq!(variant_array.len(), 3);
        assert!(variant_array.is_null(0));
        assert_eq!(variant_array.value(1), Variant::Null);
        assert_eq!(
            variant_array.value(2),
            Variant::ShortString(ShortString::try_new("norm").unwrap())
        );
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
        // Populate a variant array with objects
        let mut builder = VariantArrayBuilder::new(3);
        builder
            .new_object()
            .with_field("name", "Alice")
            .with_field("age", 30i32)
            .finish();

        builder
            .new_object()
            .with_field("name", "Bob")
            .with_field("age", 42i32)
            .with_field("city", "Wonderland")
            .finish();

        builder
            .new_object()
            .with_field("name", "Charlie")
            .with_field("age", 1i32)
            .finish();

        let array = builder.build();

        // Copy (some of) the objects over to the value array builder
        //
        // NOTE: Because we will reuse the metadata column, we cannot reorder rows. We can only
        // filter or manipulate values within a row.
        let mut value_builder = VariantValueArrayBuilder::new(3);

        // straight copy
        value_builder.append_value(array.value(0));

        // filtering fields takes more work because we need to manually create an object builder
        let value = array.value(1);
        let mut builder = value_builder.builder_ext(value.metadata());
        builder
            .new_object()
            .with_field("name", value.get_object_field("name").unwrap())
            .with_field("age", value.get_object_field("age").unwrap())
            .finish();

        // same bytes, but now nested and duplicated inside a list
        let value = array.value(2);
        let mut builder = value_builder.builder_ext(value.metadata());
        builder
            .new_list()
            .with_value(value.clone())
            .with_value(value.clone())
            .finish();

        let array2 = VariantArray::from_parts(
            array.metadata_field().clone(),
            Some(value_builder.build().unwrap()),
            None,
            None,
        );

        assert_eq!(array2.len(), 3);
        assert_eq!(array.value(0), array2.value(0));

        assert_eq!(
            array.value(1).get_object_field("name"),
            array2.value(1).get_object_field("name")
        );
        assert_eq!(
            array.value(1).get_object_field("age"),
            array2.value(1).get_object_field("age")
        );

        assert_eq!(array.value(2), array2.value(2).get_list_element(0).unwrap());
        assert_eq!(array.value(2), array2.value(2).get_list_element(1).unwrap());
    }
}
