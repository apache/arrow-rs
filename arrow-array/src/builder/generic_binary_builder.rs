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

use crate::builder::null_buffer_builder::NullBufferBuilder;
use crate::builder::{ArrayBuilder, BufferBuilder, UInt8BufferBuilder};
use crate::{ArrayRef, GenericBinaryArray, OffsetSizeTrait};
use arrow_data::ArrayDataBuilder;
use std::any::Any;
use std::sync::Arc;

///  Array builder for [`GenericBinaryArray`]
#[derive(Debug)]
pub struct GenericBinaryBuilder<OffsetSize: OffsetSizeTrait> {
    value_builder: UInt8BufferBuilder,
    offsets_builder: BufferBuilder<OffsetSize>,
    null_buffer_builder: NullBufferBuilder,
}

impl<OffsetSize: OffsetSizeTrait> GenericBinaryBuilder<OffsetSize> {
    /// Creates a new [`GenericBinaryBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(1024, 1024)
    }

    /// Creates a new [`GenericBinaryBuilder`].
    ///
    /// - `item_capacity` is the number of items to pre-allocate.
    ///   The size of the preallocated buffer of offsets is the number of items plus one.
    /// - `data_capacity` is the total number of bytes of string data to pre-allocate
    ///   (for all items, not per item).
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(item_capacity + 1);
        offsets_builder.append(OffsetSize::zero());
        Self {
            value_builder: UInt8BufferBuilder::new(data_capacity),
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(item_capacity),
        }
    }

    /// Appends a byte slice into the builder.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.value_builder.append_slice(value.as_ref());
        self.null_buffer_builder.append(true);
        self.offsets_builder
            .append(OffsetSize::from_usize(self.value_builder.len()).unwrap());
    }

    /// Append a null value into the builder.
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append(false);
        self.offsets_builder
            .append(OffsetSize::from_usize(self.value_builder.len()).unwrap());
    }

    /// Builds the [`GenericBinaryArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericBinaryArray<OffsetSize> {
        let array_type = GenericBinaryArray::<OffsetSize>::DATA_TYPE;
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(self.offsets_builder.finish())
            .add_buffer(self.value_builder.finish())
            .null_bit_buffer(self.null_buffer_builder.finish());

        self.offsets_builder.append(OffsetSize::zero());
        let array_data = unsafe { array_builder.build_unchecked() };
        GenericBinaryArray::<OffsetSize>::from(array_data)
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[u8] {
        self.value_builder.as_slice()
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.offsets_builder.as_slice()
    }
}

impl<OffsetSize: OffsetSizeTrait> Default for GenericBinaryBuilder<OffsetSize> {
    fn default() -> Self {
        Self::new()
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayBuilder for GenericBinaryBuilder<OffsetSize> {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of binary slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Returns whether the number of binary slots is zero
    fn is_empty(&self) -> bool {
        self.null_buffer_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, OffsetSizeTrait};

    fn _test_generic_binary_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");

        let array = builder.finish();

        assert_eq!(4, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(b"hello", array.value(0));
        assert_eq!([] as [u8; 0], array.value(1));
        assert!(array.is_null(2));
        assert_eq!(b"rust", array.value(3));
        assert_eq!(O::from_usize(5).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(4).unwrap(), array.value_length(3));
    }

    #[test]
    fn test_binary_builder() {
        _test_generic_binary_builder::<i32>()
    }

    #[test]
    fn test_large_binary_builder() {
        _test_generic_binary_builder::<i64>()
    }

    fn _test_generic_binary_builder_all_nulls<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();
        builder.append_null();
        builder.append_null();
        builder.append_null();
        assert_eq!(3, builder.len());
        assert!(!builder.is_empty());

        let array = builder.finish();
        assert_eq!(3, array.null_count());
        assert_eq!(3, array.len());
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert!(array.is_null(2));
    }

    #[test]
    fn test_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i32>()
    }

    #[test]
    fn test_large_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i64>()
    }

    fn _test_generic_binary_builder_reset<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");
        builder.finish();

        assert!(builder.is_empty());

        builder.append_value(b"parquet");
        builder.append_null();
        builder.append_value(b"arrow");
        builder.append_value(b"");
        let array = builder.finish();

        assert_eq!(4, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(b"parquet", array.value(0));
        assert!(array.is_null(1));
        assert_eq!(b"arrow", array.value(2));
        assert_eq!(b"", array.value(1));
        assert_eq!(O::zero(), array.value_offsets()[0]);
        assert_eq!(O::from_usize(7).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(5).unwrap(), array.value_length(2));
    }

    #[test]
    fn test_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i32>()
    }

    #[test]
    fn test_large_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i64>()
    }
}
