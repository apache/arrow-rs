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
    array::{
        ArrayBuilder, ArrayDataBuilder, ArrayRef, GenericBinaryArray, OffsetSizeTrait,
        UInt8BufferBuilder,
    },
    datatypes::DataType,
};
use std::any::Any;
use std::sync::Arc;

use super::{BooleanBufferBuilder, BufferBuilder};

///  Array builder for [`GenericBinaryArray`]
#[derive(Debug)]
pub struct GenericBinaryBuilder<OffsetSize: OffsetSizeTrait> {
    value_builder: UInt8BufferBuilder,
    offsets_builder: BufferBuilder<OffsetSize>,
    null_buffer_builder: BooleanBufferBuilder,
}

impl<OffsetSize: OffsetSizeTrait> GenericBinaryBuilder<OffsetSize> {
    /// Creates a new `GenericBinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(1024);
        offsets_builder.append_n_zeroed(1);
        Self {
            value_builder: UInt8BufferBuilder::new(capacity),
            offsets_builder,
            null_buffer_builder: BooleanBufferBuilder::new(1024),
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

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append(false);
        self.offsets_builder
            .append(OffsetSize::from_usize(self.value_builder.len()).unwrap());
    }

    /// Builds the [`GenericBinaryArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericBinaryArray<OffsetSize> {
        let array_type = if OffsetSize::IS_LARGE {
            DataType::LargeBinary
        } else {
            DataType::Binary
        };
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(self.offsets_builder.finish())
            .add_buffer(self.value_builder.finish())
            .null_bit_buffer(Some(self.null_buffer_builder.finish()));
        let array_data = unsafe { array_builder.build_unchecked() };
        GenericBinaryArray::<OffsetSize>::from(array_data)
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

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Returns whether the number of array slots is zero
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

    fn _test_generic_binary_array_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new(20);

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");

        let binary_array = builder.finish();

        assert_eq!(4, binary_array.len());
        assert_eq!(1, binary_array.null_count());
        assert_eq!(b"hello", binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert!(binary_array.is_null(2));
        assert_eq!(b"rust", binary_array.value(3));
        assert_eq!(O::from_usize(5).unwrap(), binary_array.value_offsets()[2]);
        assert_eq!(O::from_usize(4).unwrap(), binary_array.value_length(3));
    }

    #[test]
    fn test_binary_array_builder() {
        _test_generic_binary_array_builder::<i32>()
    }

    #[test]
    fn test_large_binary_array_builder() {
        _test_generic_binary_array_builder::<i64>()
    }
}
