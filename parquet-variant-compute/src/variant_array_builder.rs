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

use crate::{VariantArray, shred_variant};
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
///
/// ## Shredded Example
///
/// Use [`Self::build_shredded`] or [`Self::with_shredding_schema`] with
/// [`ShreddedSchemaBuilder`] to produce a shredded [`VariantArray`] where known
/// fields are extracted into typed columns.
///
/// ```
/// # use arrow::array::Array;
/// # use arrow_schema::DataType;
/// # use parquet_variant::{Variant, VariantBuilderExt};
/// # use parquet_variant_compute::{ShreddedSchemaBuilder, VariantArrayBuilder};
/// let schema = ShreddedSchemaBuilder::default()
///     .with_path("brand", &DataType::Utf8).unwrap()
///     .with_path("price", &DataType::Float64).unwrap()
///     .build();
///
/// let mut builder = VariantArrayBuilder::new(3).with_shredding_schema(schema);
/// builder.new_object().with_field("brand", "Apple").with_field("price", 999.0f64).finish();
/// builder.new_object().with_field("brand", "Samsung").finish();
/// builder.append_null();
///
/// let arr = builder.try_build().unwrap();
/// assert_eq!(arr.len(), 3);
/// assert!(arr.typed_value_field().is_some());
/// assert!(!arr.is_null(0));
/// assert!(!arr.is_null(1));
/// assert!(arr.is_null(2));
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
    /// TODO: Add extension type metadata
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

    /// Build the final [`VariantArray`] (unshredded).
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

    /// Build a shredded [`VariantArray`] using `as_type` as the shredding schema.
    /// Use [`ShreddedSchemaBuilder`] to construct `as_type` for struct schemas.
    /// Returns `Err` if `as_type` is not a valid variant shredding type.
    pub fn build_shredded(self, as_type: &DataType) -> Result<VariantArray, ArrowError> {
        shred_variant(&self.build(), as_type)
    }

    /// Configure this builder to produce a shredded [`VariantArray`].
    ///
    /// The returned builder keeps [`Self::build`] infallible for unshredded
    /// arrays, and exposes [`ShreddedVariantArrayBuilder::try_build`] for the
    /// fallible shredded path.
    pub fn with_shredding_schema(self, as_type: DataType) -> ShreddedVariantArrayBuilder {
        ShreddedVariantArrayBuilder::new(self, as_type)
    }

    /// Appends a null row to the builder.
    pub fn append_null(&mut self) {
        self.nulls.append_null();
        // The subfields are expected to be non-nullable according to the parquet variant spec.
        self.metadata_offsets.push(self.metadata_builder.offset());
        self.value_offsets.push(self.value_builder.offset());
    }

    /// Appends `n` null rows to the builder.
    pub fn append_nulls(&mut self, n: usize) {
        self.nulls.append_n_nulls(n);
        // The subfields are expected to be non-nullable according to the parquet variant spec.
        let metadata_offset = self.metadata_builder.offset();
        let value_offset = self.value_builder.offset();
        self.metadata_offsets
            .extend(std::iter::repeat_n(metadata_offset, n));
        self.value_offsets
            .extend(std::iter::repeat_n(value_offset, n));
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

/// A [`VariantArrayBuilder`] configured to build a shredded [`VariantArray`].
///
/// This type preserves the chain-style configuration API while keeping
/// [`VariantArrayBuilder::build`] infallible for unshredded arrays.
#[derive(Debug)]
pub struct ShreddedVariantArrayBuilder {
    builder: VariantArrayBuilder,
    as_type: DataType,
}

impl ShreddedVariantArrayBuilder {
    /// Create a shredded builder from an existing [`VariantArrayBuilder`] and
    /// shredding schema.
    pub fn new(builder: VariantArrayBuilder, as_type: DataType) -> Self {
        Self { builder, as_type }
    }

    /// Build the final shredded [`VariantArray`].
    ///
    /// Returns `Err` if the configured shredding schema is not a valid variant
    /// shredding type.
    pub fn try_build(self) -> Result<VariantArray, ArrowError> {
        self.builder.build_shredded(&self.as_type)
    }

    /// Appends a null row to the builder.
    pub fn append_null(&mut self) {
        self.builder.append_null();
    }

    /// Appends `n` null rows to the builder.
    pub fn append_nulls(&mut self, n: usize) {
        self.builder.append_nulls(n);
    }

    /// Append the [`Variant`] to the builder as the next row.
    pub fn append_variant(&mut self, variant: Variant) {
        self.builder.append_variant(variant);
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

impl<'m, 'v> Extend<Option<Variant<'m, 'v>>> for ShreddedVariantArrayBuilder {
    fn extend<T: IntoIterator<Item = Option<Variant<'m, 'v>>>>(&mut self, iter: T) {
        self.builder.extend(iter);
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

impl VariantBuilderExt for ShreddedVariantArrayBuilder {
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
        Ok(ListBuilder::new(self.builder.parent_state(), false))
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(ObjectBuilder::new(self.builder.parent_state(), false))
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
    use crate::ShreddedSchemaBuilder;
    use crate::variant_array::ShreddedVariantFieldArray;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
        StringArray, StructArray,
    };
    use parquet_variant::{ShortString, Uuid, Variant};

    fn typed_value<T: 'static>(arr: &VariantArray) -> &T {
        arr.typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
    }

    fn shredded_field(typed_value: &StructArray, name: &str) -> ShreddedVariantFieldArray {
        ShreddedVariantFieldArray::try_new(typed_value.column_by_name(name).unwrap()).unwrap()
    }

    fn field_typed_value<T: 'static>(field: &ShreddedVariantFieldArray) -> &T {
        field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
    }

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
    fn test_variant_array_builder_append_nulls() {
        let mut builder = VariantArrayBuilder::new(6);
        builder.append_variant(Variant::from(1i32));
        builder.append_nulls(0); // should be a no-op
        builder.append_nulls(3);
        builder.append_variant(Variant::from(2i32));

        let variant_array = builder.build();

        assert_eq!(variant_array.len(), 5);
        assert_eq!(variant_array.value(0), Variant::from(1i32));
        assert!(variant_array.is_null(1));
        assert!(variant_array.is_null(2));
        assert!(variant_array.is_null(3));
        assert_eq!(variant_array.value(4), Variant::from(2i32));
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
            Some(Arc::new(value_builder.build().unwrap())),
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

    #[test]
    fn build_shredded_primitive_int64() {
        let mut b = VariantArrayBuilder::new(3);
        b.append_variant(Variant::Int64(42));
        b.append_variant(Variant::Int64(100));
        b.append_null();
        let arr = b.build_shredded(&DataType::Int64).unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr.typed_value_field().is_some());
        assert!(!arr.is_null(0));
        assert!(!arr.is_null(1));
        assert!(arr.is_null(2));
        assert_eq!(arr.value(0), Variant::Int64(42));
        assert_eq!(arr.value(1), Variant::Int64(100));

        let typed_values = typed_value::<Int64Array>(&arr);
        assert_eq!(typed_values.value(0), 42);
        assert_eq!(typed_values.value(1), 100);
        assert!(typed_values.is_null(2));
    }

    #[test]
    fn with_shredding_schema_try_build_primitive_int64() {
        let mut b = VariantArrayBuilder::new(2).with_shredding_schema(DataType::Int64);
        b.append_variant(Variant::Int64(42));
        b.append_null();

        let arr = b.try_build().unwrap();

        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), Variant::Int64(42));
        assert!(arr.is_null(1));
    }

    #[test]
    fn build_shredded_primitive_utf8() {
        let mut b = VariantArrayBuilder::new(2);
        b.append_variant(Variant::from("hello"));
        b.append_null();
        let arr = b.build_shredded(&DataType::Utf8).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(0), Variant::from("hello"));

        let typed_values = typed_value::<StringArray>(&arr);
        assert_eq!(typed_values.value(0), "hello");
        assert!(typed_values.is_null(1));
    }

    #[test]
    fn build_shredded_primitive_float64() {
        let mut b = VariantArrayBuilder::new(2);
        b.append_variant(Variant::Double(3.14));
        b.append_null();
        let arr = b.build_shredded(&DataType::Float64).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), Variant::Double(3.14));

        let typed_values = typed_value::<Float64Array>(&arr);
        assert_eq!(typed_values.value(0), 3.14);
        assert!(typed_values.is_null(1));
    }

    #[test]
    fn build_shredded_primitive_bool() {
        let mut b = VariantArrayBuilder::new(2);
        b.append_variant(Variant::BooleanTrue);
        b.append_variant(Variant::BooleanFalse);
        let arr = b.build_shredded(&DataType::Boolean).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), Variant::BooleanTrue);
        assert_eq!(arr.value(1), Variant::BooleanFalse);

        let typed_values = typed_value::<BooleanArray>(&arr);
        assert!(typed_values.value(0));
        assert!(!typed_values.value(1));
    }

    #[test]
    fn build_shredded_type_mismatch_falls_back_to_value_column() {
        // Row 0: matches Int64 -> typed_value non-null, value null
        // Row 1: string, does not match -> value non-null, typed_value null
        let mut b = VariantArrayBuilder::new(2);
        b.append_variant(Variant::Int64(7));
        b.append_variant(Variant::from("not an int"));
        let arr = b.build_shredded(&DataType::Int64).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(0));
        assert!(!arr.is_null(1));
        assert_eq!(arr.value(0), Variant::Int64(7));
        assert_eq!(arr.value(1), Variant::from("not an int"));

        let value = arr.value_field().unwrap();
        let typed_values = typed_value::<Int64Array>(&arr);
        assert!(value.is_null(0));
        assert!(typed_values.is_valid(0));
        assert!(value.is_valid(1));
        assert!(typed_values.is_null(1));
    }

    #[test]
    fn build_shredded_struct_single_field() {
        let schema = DataType::Struct(vec![Field::new("brand", DataType::Utf8, true)].into());
        let mut b = VariantArrayBuilder::new(3).with_shredding_schema(schema);
        b.new_object().with_field("brand", "Apple").finish();
        b.new_object().with_field("brand", "Samsung").finish();
        b.append_null();
        let arr = b.try_build().unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 3);
        assert!(!arr.is_null(0));
        assert!(!arr.is_null(1));
        assert!(arr.is_null(2));

        let typed_struct = typed_value::<StructArray>(&arr);
        let brand = shredded_field(typed_struct, "brand");
        let brand_typed_values = field_typed_value::<StringArray>(&brand);
        assert_eq!(brand_typed_values.value(0), "Apple");
        assert_eq!(brand_typed_values.value(1), "Samsung");
        assert!(brand_typed_values.is_null(2));
    }

    #[test]
    fn build_shredded_struct_multi_field() {
        let schema = ShreddedSchemaBuilder::default()
            .with_path("name", &DataType::Utf8)
            .unwrap()
            .with_path("age", &DataType::Int32)
            .unwrap()
            .build();
        let mut b = VariantArrayBuilder::new(2);
        b.new_object()
            .with_field("name", "Alice")
            .with_field("age", 30i32)
            .finish();
        b.new_object().with_field("name", "Bob").finish();
        let arr = b.build_shredded(&schema).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);

        let typed_struct = typed_value::<StructArray>(&arr);
        let name = shredded_field(typed_struct, "name");
        let name_typed_values = field_typed_value::<StringArray>(&name);
        assert_eq!(name_typed_values.value(0), "Alice");
        assert_eq!(name_typed_values.value(1), "Bob");

        let age = shredded_field(typed_struct, "age");
        let age_typed_values = field_typed_value::<Int32Array>(&age);
        assert_eq!(age_typed_values.value(0), 30);
        assert!(age_typed_values.is_null(1));
    }

    #[test]
    fn build_shredded_nested_struct() {
        let schema = ShreddedSchemaBuilder::default()
            .with_path("address.city", &DataType::Utf8)
            .unwrap()
            .with_path("address.zip", &DataType::Utf8)
            .unwrap()
            .build();
        let mut b = VariantArrayBuilder::new(2);
        {
            let mut obj = b.new_object();
            obj.new_object("address")
                .with_field("city", "NYC")
                .with_field("zip", "10001")
                .finish();
            obj.finish();
        }
        b.append_null();
        let arr = b.build_shredded(&schema).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));

        let typed_struct = typed_value::<StructArray>(&arr);
        let address = shredded_field(typed_struct, "address");
        let address_typed_value = field_typed_value::<StructArray>(&address);

        let city = shredded_field(address_typed_value, "city");
        let city_typed_values = field_typed_value::<StringArray>(&city);
        assert_eq!(city_typed_values.value(0), "NYC");
        assert!(city_typed_values.is_null(1));

        let zip = shredded_field(address_typed_value, "zip");
        let zip_typed_values = field_typed_value::<StringArray>(&zip);
        assert_eq!(zip_typed_values.value(0), "10001");
        assert!(zip_typed_values.is_null(1));
    }

    #[test]
    fn build_shredded_list_of_int64() {
        use arrow_schema::Field as ArrowField;
        use std::sync::Arc;
        let list_schema = DataType::List(Arc::new(ArrowField::new("item", DataType::Int64, true)));
        let mut b = VariantArrayBuilder::new(2);
        b.new_list()
            .with_value(Variant::Int64(1))
            .with_value(Variant::Int64(2))
            .finish();
        b.append_null();
        let arr = b.build_shredded(&list_schema).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));

        let typed_list = typed_value::<ListArray>(&arr);
        assert_eq!(typed_list.value_offsets(), &[0, 2, 2]);
        assert!(typed_list.is_valid(0));
        assert!(typed_list.is_null(1));

        let elements = ShreddedVariantFieldArray::try_new(typed_list.values().as_ref()).unwrap();
        let element_values = elements.value_field().unwrap();
        let element_typed_values = field_typed_value::<Int64Array>(&elements);
        assert!(element_values.is_null(0));
        assert!(element_values.is_null(1));
        assert_eq!(element_typed_values.value(0), 1);
        assert_eq!(element_typed_values.value(1), 2);
    }

    #[test]
    fn build_shredded_extend_then_shred() {
        let mut b = VariantArrayBuilder::new(4);
        b.extend([
            Some(Variant::Int64(1)),
            None,
            Some(Variant::Int64(3)),
            Some(Variant::from("oops")),
        ]);
        let arr = b.build_shredded(&DataType::Int64).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 4);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert!(!arr.is_null(3));
        assert_eq!(arr.value(0), Variant::Int64(1));
        assert_eq!(arr.value(2), Variant::Int64(3));
        assert_eq!(arr.value(3), Variant::from("oops"));

        let typed_values = typed_value::<Int64Array>(&arr);
        assert_eq!(typed_values.value(0), 1);
        assert!(typed_values.is_null(1));
        assert_eq!(typed_values.value(2), 3);
        assert!(typed_values.is_null(3));
    }

    #[test]
    fn build_shredded_all_nulls() {
        let mut b = VariantArrayBuilder::new(3);
        b.append_null();
        b.append_null();
        b.append_null();
        let arr = b.build_shredded(&DataType::Int64).unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));

        let typed_values = typed_value::<Int64Array>(&arr);
        assert!(typed_values.is_null(0));
        assert!(typed_values.is_null(1));
        assert!(typed_values.is_null(2));
    }

    #[test]
    fn build_shredded_invalid_type_returns_err() {
        let mut b = VariantArrayBuilder::new(1);
        b.append_variant(Variant::Int64(1));
        let result = b.build_shredded(&DataType::FixedSizeBinary(17));
        assert!(result.is_err());
    }

    #[test]
    fn build_shredded_uuid_fixed_size_binary_16() {
        let uuid_bytes: Vec<u8> = (0u8..16).collect();
        let uuid = Uuid::from_slice(&uuid_bytes).unwrap();
        let mut b = VariantArrayBuilder::new(1);
        b.append_variant(Variant::Uuid(uuid));
        let arr = b.build_shredded(&DataType::FixedSizeBinary(16)).unwrap();
        assert!(arr.typed_value_field().is_some());
        assert_eq!(arr.len(), 1);

        assert_eq!(arr.value(0), Variant::Uuid(uuid));

        let typed_values = typed_value::<FixedSizeBinaryArray>(&arr);
        assert_eq!(typed_values.value(0), uuid_bytes.as_slice());
    }
}
