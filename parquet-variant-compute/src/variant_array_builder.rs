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
use parquet_variant::{MetadataBuilder, ValueBuilder, ListBuilder, ParentState, ObjectBuilder, Variant, VariantBuilderExt};
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
/// // append an object to the builder
/// let mut vb = builder.variant_builder();
/// vb.new_object()
///   .with_field("foo", "bar")
///   .finish()
///   .unwrap();
///  vb.finish(); // must call finish to write the variant to the buffers
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
    /// buffer for all the metadata
    metadata_builder: MetadataBuilder,
    /// (offset, len) pairs for locations of metadata in the buffer
    metadata_offsets: Vec<usize>,
    /// buffer for values
    value_builder: ValueBuilder,
    /// (offset, len) pairs for locations of values in the buffer
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
            metadata_builder: Default::default(), // todo allocation capacity
            metadata_offsets: Vec::with_capacity(row_capacity),
            value_builder: Default::default(),
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

        let metadata_array = binary_view_array_from_buffers(metadata_builder.into_inner(), metadata_offsets);

        let value_array = binary_view_array_from_buffers(value_builder.into_inner(), value_offsets);

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
        let mut direct_builder = self.variant_builder();
        direct_builder.append_value(variant);
        direct_builder.finish();
    }

    /// Return a `VariantArrayVariantBuilder` that writes directly to the
    /// buffers of this builder.
    ///
    /// You must call [`VariantArrayVariantBuilder::finish`] to complete the builder
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantBuilderExt};
    /// # use parquet_variant_compute::{VariantArray, VariantArrayBuilder};
    /// let mut array_builder = VariantArrayBuilder::new(10);
    ///
    /// // First row has a string
    /// let mut variant_builder = array_builder.variant_builder();
    /// variant_builder.append_value("Hello, World!");
    /// // must call finish to write the variant to the buffers
    /// variant_builder.finish();
    ///
    /// // Second row is an object
    /// let mut variant_builder = array_builder.variant_builder();
    /// variant_builder
    ///     .new_object()
    ///     .with_field("my_field", 42i64)
    ///     .finish()
    ///     .unwrap();
    /// variant_builder.finish();
    ///
    /// // finalize the array
    /// let variant_array: VariantArray = array_builder.build();
    ///
    /// // verify what we wrote is still there
    /// assert_eq!(variant_array.value(0), Variant::from("Hello, World!"));
    /// assert!(variant_array.value(1).as_object().is_some());
    ///  ```
    pub fn variant_builder(&mut self) -> VariantArrayVariantBuilder<'_> {
        VariantArrayVariantBuilder::new(self)
    }
}

/// A `VariantBuilderExt` that writes directly to the buffers of a `VariantArrayBuilder`.
///
// This struct implements [`VariantBuilderExt`], so in most cases it can be used as a
// [`VariantBuilder`] to perform variant-related operations for [`VariantArrayBuilder`].
///
/// If [`Self::finish`] is not called, any changes will be rolled back
///
/// See [`VariantArrayBuilder::variant_builder`] for an example
pub struct VariantArrayVariantBuilder<'a> {
    /// was finish called?
    finished: bool,
    /// starting offset in the variant_builder's `metadata` buffer
    metadata_offset: usize,
    /// starting offset in the variant_builder's `value` buffer
    value_offset: usize,
    /// Parent array builder that this variant builder writes to. Buffers
    /// have been moved into the variant builder, and must be returned on
    /// drop
    array_builder: &'a mut VariantArrayBuilder,
}

impl VariantBuilderExt for VariantArrayVariantBuilder<'_> {
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        ValueBuilder::try_append_variant_impl(self.parent_state(), value.into()).unwrap()
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_>, ArrowError> {
        Ok(ListBuilder::new(self.parent_state(), false))
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_>, ArrowError> {
        Ok(ObjectBuilder::new(self.parent_state(), false))
    }
}

impl<'a> VariantArrayVariantBuilder<'a> {
    /// Constructs a new VariantArrayVariantBuilder
    ///
    /// Note this is not public as this is a structure that is logically
    /// part of the [`VariantArrayBuilder`] and relies on its internal structure
    fn new(array_builder: &'a mut VariantArrayBuilder) -> Self {
        let metadata_offset = array_builder.metadata_builder.offset();
        let value_offset = array_builder.value_builder.offset();
        VariantArrayVariantBuilder {
            finished: false,
            metadata_offset,
            value_offset,
            array_builder,
        }
    }

    fn parent_state(&mut self) -> ParentState<'_> {
        ParentState::variant(
            &mut self.array_builder.value_builder,
            &mut self.array_builder.metadata_builder,
        )
    }

    /// Called to finish the in progress variant and write it to the underlying
    /// buffers
    ///
    /// Note if you do not call finish, on drop any changes made to the
    /// underlying buffers will be rolled back.
    pub fn finish(mut self) {
        self.finished = true;

        let metadata_offset = self.metadata_offset;
        let value_offset = self.value_offset;

        let metadata_builder = &mut self.array_builder.metadata_builder;
        let value_builder = &mut self.array_builder.value_builder;

        // Sanity Check: if the buffers got smaller, something went wrong (previous data was lost)
        assert!(metadata_offset <= metadata_builder.offset(), "metadata length decreased unexpectedly");
        assert!(value_offset <= value_builder.offset(), "value length decreased unexpectedly");

        metadata_builder.finish();

        // commit the changes by putting the
        // offsets and lengths into the parent array builder.
        self.array_builder
            .metadata_offsets
            .push(metadata_offset);
        self.array_builder
            .value_offsets
            .push(value_offset);
        self.array_builder.nulls.append_non_null();
    }
}

// Make it harder for people to accidentally forget to `finish` the builder.
impl Drop for VariantArrayVariantBuilder<'_> {
    fn drop(&mut self) {}
}

fn binary_view_array_from_buffers(
    buffer: Vec<u8>,
    mut offsets: Vec<usize>,
) -> BinaryViewArray {
    let mut builder = BinaryViewBuilder::with_capacity(offsets.len());
    offsets.push(buffer.len());
    let block = builder.append_block(buffer.into());
    // TODO this can be much faster if it creates the views directly during append
    for i in 1..offsets.len() {
        let start = u32::try_from(offsets[i-1]).expect("offset should fit in u32");
        let end = u32::try_from(offsets[i]).expect("offset should fit in u32");
        let length = end - start;
        builder
            .try_append_view(block, start, length)
            .expect("Failed to append view");
    }
    builder.finish()
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Array;

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

    /// Test using sub builders to append variants
    #[test]
    fn test_variant_array_builder_variant_builder() {
        let mut builder = VariantArrayBuilder::new(10);
        builder.append_null(); // should not panic
        builder.append_variant(Variant::from(42i32));

        // let's make a sub-object in the next row
        let mut sub_builder = builder.variant_builder();
        sub_builder
            .new_object()
            .with_field("foo", "bar")
            .finish()
            .unwrap();
        sub_builder.finish(); // must call finish to write the variant to the buffers

        // append a new list
        let mut sub_builder = builder.variant_builder();
        sub_builder
            .new_list()
            .with_value(Variant::from(1i32))
            .with_value(Variant::from(2i32))
            .finish();
        sub_builder.finish();
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

    /// Test using non-finished sub builders to append variants
    #[test]
    fn test_variant_array_builder_variant_builder_reset() {
        let mut builder = VariantArrayBuilder::new(10);

        // make a sub-object in the first row
        let mut sub_builder = builder.variant_builder();
        sub_builder
            .new_object()
            .with_field("foo", 1i32)
            .finish()
            .unwrap();
        sub_builder.finish(); // must call finish to write the variant to the buffers

        // start appending an object but don't finish
        let mut sub_builder = builder.variant_builder();
        sub_builder
            .new_object()
            .with_field("bar", 2i32)
            .finish()
            .unwrap();
        drop(sub_builder); // drop the sub builder without finishing it

        // make a third sub-object (this should reset the previous unfinished object)
        let mut sub_builder = builder.variant_builder();
        sub_builder
            .new_object()
            .with_field("baz", 3i32)
            .finish()
            .unwrap();
        sub_builder.finish(); // must call finish to write the variant to the buffers

        let variant_array = builder.build();

        // only the two finished objects should be present
        assert_eq!(variant_array.len(), 2);
        assert!(!variant_array.is_null(0));
        let variant = variant_array.value(0);
        let variant = variant.as_object().expect("variant to be an object");
        assert_eq!(variant.get("foo").unwrap(), Variant::from(1i32));

        assert!(!variant_array.is_null(1));
        let variant = variant_array.value(1);
        let variant = variant.as_object().expect("variant to be an object");
        assert_eq!(variant.get("baz").unwrap(), Variant::from(3i32));
    }
}
