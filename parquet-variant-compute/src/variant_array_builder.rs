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
use arrow_schema::{DataType, Field, Fields};
use parquet_variant::{ListBuilder, ObjectBuilder, Variant, VariantBuilder, VariantBuilderExt};
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
/// // append a pre-constructed metadata and value buffers
/// let (metadata, value) = {
///   let mut vb = VariantBuilder::new();
///   let mut obj = vb.new_object();
///   obj.insert("foo", "bar");
///   obj.finish().unwrap();
///   vb.finish()
/// };
/// builder.append_variant_buffers(&metadata, &value);
///
/// // Use `variant_builder` method to write values directly to the output array
/// let mut vb = builder.variant_builder();
/// vb.append_value("Hello, World!");
/// vb.finish(); // Note: call finish to write the variant to the buffers
///
/// // create the final VariantArray
/// let variant_array = builder.build();
/// assert_eq!(variant_array.len(), 4);
/// // // Access the values
/// // row 1 is not null and is an integer
/// assert!(!variant_array.is_null(0));
/// assert_eq!(variant_array.value(0), Variant::from(42i32));
/// // row 1 is null
/// assert!(variant_array.is_null(1));
/// // row 2 is not null and is an object
/// assert!(!variant_array.is_null(2));
/// assert!(variant_array.value(2).as_object().is_some());
/// // row 3 is a string
/// assert!(!variant_array.is_null(3));
/// assert_eq!(variant_array.value(3), Variant::from("Hello, World!"));
/// ```
#[derive(Debug)]
pub struct VariantArrayBuilder {
    /// Nulls
    nulls: NullBufferBuilder,
    /// buffer for all the metadata
    metadata_buffer: Vec<u8>,
    /// (offset, len) pairs for locations of metadata in the buffer
    metadata_locations: Vec<(usize, usize)>,
    /// buffer for values
    value_buffer: Vec<u8>,
    /// (offset, len) pairs for locations of values in the buffer
    value_locations: Vec<(usize, usize)>,
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
            metadata_buffer: Vec::new(), // todo allocation capacity
            metadata_locations: Vec::with_capacity(row_capacity),
            value_buffer: Vec::new(),
            value_locations: Vec::with_capacity(row_capacity),
            fields: Fields::from(vec![metadata_field, value_field]),
        }
    }

    /// Build the final builder
    pub fn build(self) -> VariantArray {
        let Self {
            mut nulls,
            metadata_buffer,
            metadata_locations,
            value_buffer,
            value_locations,
            fields,
        } = self;

        let metadata_array = binary_view_array_from_buffers(metadata_buffer, metadata_locations);

        let value_array = binary_view_array_from_buffers(value_buffer, value_locations);

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
        let metadata_offset = self.metadata_buffer.len();
        let metadata_length = 0;
        self.metadata_locations
            .push((metadata_offset, metadata_length));
        let value_offset = self.value_buffer.len();
        let value_length = 0;
        self.value_locations.push((value_offset, value_length));
    }

    /// Append the [`Variant`] to the builder as the next row
    pub fn append_variant(&mut self, variant: Variant) {
        let mut direct_builder = self.variant_builder();
        direct_builder.variant_builder.append_value(variant);
        direct_builder.finish()
    }

    /// Append a metadata and values buffer to the builder
    pub fn append_variant_buffers(&mut self, metadata: &[u8], value: &[u8]) {
        self.nulls.append_non_null();
        let metadata_length = metadata.len();
        let metadata_offset = self.metadata_buffer.len();
        self.metadata_locations
            .push((metadata_offset, metadata_length));
        self.metadata_buffer.extend_from_slice(metadata);
        let value_length = value.len();
        let value_offset = self.value_buffer.len();
        self.value_locations.push((value_offset, value_length));
        self.value_buffer.extend_from_slice(value);
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
    pub fn variant_builder(&mut self) -> VariantArrayVariantBuilder {
        // append directly into the metadata and value buffers
        let metadata_buffer = std::mem::take(&mut self.metadata_buffer);
        let value_buffer = std::mem::take(&mut self.value_buffer);
        VariantArrayVariantBuilder::new(self, metadata_buffer, value_buffer)
    }
}

/// A `VariantBuilder` that writes directly to the buffers of a `VariantArrayBuilder`.
///
/// Note this struct implements [`VariantBuilderExt`], so it can be used
/// as a drop-in replacement for [`VariantBuilder`] in most cases.
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
    /// Builder for the in progress variant value, temporarily owns the buffers
    /// from `array_builder`
    variant_builder: VariantBuilder,
}

impl<'a> VariantBuilderExt for VariantArrayVariantBuilder<'a> {
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.variant_builder.append_value(value);
    }

    fn new_list(&mut self) -> ListBuilder {
        self.variant_builder.new_list()
    }

    fn new_object(&mut self) -> ObjectBuilder {
        self.variant_builder.new_object()
    }
}

impl<'a> VariantArrayVariantBuilder<'a> {
    /// Constructs a new VariantArrayVariantBuilder
    ///
    /// Note this is not public as this is a structure that is logically
    /// part of the [`VariantArrayBuilder`] and relies on its internal structure
    fn new(
        array_builder: &'a mut VariantArrayBuilder,
        metadata_buffer: Vec<u8>,
        value_buffer: Vec<u8>,
    ) -> Self {
        let metadata_offset = metadata_buffer.len();
        let value_offset = value_buffer.len();
        VariantArrayVariantBuilder {
            finished: false,
            metadata_offset,
            value_offset,
            variant_builder: VariantBuilder::new_with_buffers(metadata_buffer, value_buffer),
            array_builder,
        }
    }

    /// Return a reference to the underlying `VariantBuilder`
    pub fn inner(&self) -> &VariantBuilder {
        &self.variant_builder
    }

    /// Return a mutable reference to the underlying `VariantBuilder`
    pub fn inner_mut(&mut self) -> &mut VariantBuilder {
        &mut self.variant_builder
    }

    /// Called to finish the in progress variant and write it to the underlying
    /// buffers
    ///
    /// Note if you do not call finish, on drop any changes made to the
    /// underlying buffers will be rolled back.
    pub fn finish(mut self) {
        self.finished = true;
        // Note: buffers are returned and replaced in the drop impl
    }
}

impl<'a> Drop for VariantArrayVariantBuilder<'a> {
    /// If the builder was not finished, roll back any changes made to the
    /// underlying buffers (by truncating them)
    fn drop(&mut self) {
        let metadata_offset = self.metadata_offset;
        let value_offset = self.value_offset;

        // get the buffers back from the variant builder
        let (mut metadata_buffer, mut value_buffer) =
            std::mem::take(&mut self.variant_builder).finish();

        // Sanity Check: if the buffers got smaller, something went wrong (previous data was lost)
        let metadata_len = metadata_buffer
            .len()
            .checked_sub(metadata_offset)
            .expect("metadata length decreased unexpectedly");
        let value_len = value_buffer
            .len()
            .checked_sub(value_offset)
            .expect("value length decreased unexpectedly");

        if self.finished {
            // if the object was finished, commit the changes by putting the
            // offsets and lengths into the parent array builder.
            self.array_builder
                .metadata_locations
                .push((metadata_offset, metadata_len));
            self.array_builder
                .value_locations
                .push((value_offset, value_len));
            self.array_builder.nulls.append_non_null();
        } else {
            // if the object was not finished, truncate the buffers to the
            // original offsets to roll back any changes. Note this is fast
            // because truncate doesn't free any memory: it just has to drop
            // elements (and u8 doesn't have a destructor)
            metadata_buffer.truncate(metadata_offset);
            value_buffer.truncate(value_offset);
        }

        // put the buffers back into the array builder
        self.array_builder.metadata_buffer = metadata_buffer;
        self.array_builder.value_buffer = value_buffer;
    }
}

fn binary_view_array_from_buffers(
    buffer: Vec<u8>,
    locations: Vec<(usize, usize)>,
) -> BinaryViewArray {
    let mut builder = BinaryViewBuilder::with_capacity(locations.len());
    let block = builder.append_block(buffer.into());
    // TODO this can be much faster if it creates the views directly during append
    for (offset, length) in locations {
        let offset = offset.try_into().expect("offset should fit in u32");
        let length = length.try_into().expect("length should fit in u32");
        builder
            .try_append_view(block, offset, length)
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
        assert!(variant_array.value_field().nulls().is_none());
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
