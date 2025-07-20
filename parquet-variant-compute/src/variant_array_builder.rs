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
use parquet_variant::{Variant, VariantBuilder};
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
/// # use parquet_variant::{Variant, VariantBuilder};
/// # use parquet_variant_compute::VariantArrayBuilder;
/// // Create a new VariantArrayBuilder with a capacity of 100 rows
/// let mut builder = VariantArrayBuilder::new(100);
/// // append variant values
/// builder.append_variant(Variant::from(42));
/// // append a null row
/// builder.append_null();
/// // append a pre-constructed metadata and value buffers
/// let (metadata, value) = {
///   let mut vb = VariantBuilder::new();
///   vb.new_object()
///     .with_field("foo", "bar")
///     .finish()
///     .unwrap();
///   vb.finish()
/// };
/// builder.append_variant_buffers(&metadata, &value);
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
/// assert!(variant_array.value(2).as_object().is_some());
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

    /// Finish building the VariantArray (alias for build for compatibility)
    pub fn finish(self) -> VariantArray {
        self.build()
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
        // TODO make this more efficient by avoiding the intermediate buffers
        let mut variant_builder = VariantBuilder::new();
        variant_builder.append_value(variant);
        let (metadata, value) = variant_builder.finish();
        self.append_variant_buffers(&metadata, &value);
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

    // TODO: Return a Variant builder that will write to the underlying buffers (TODO)
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
}
