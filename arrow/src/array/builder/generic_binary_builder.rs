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

use crate::array::{
    ArrayBuilder, ArrayRef, GenericBinaryArray, GenericListBuilder, OffsetSizeTrait,
    UInt8Builder,
};
use std::any::Any;
use std::sync::Arc;

///  Array builder for `BinaryArray`
#[derive(Debug)]
pub struct GenericBinaryBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericListBuilder<OffsetSize, UInt8Builder>,
}

impl<OffsetSize: OffsetSizeTrait> GenericBinaryBuilder<OffsetSize> {
    /// Creates a new `GenericBinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: GenericListBuilder::new(values_builder),
        }
    }

    /// Appends a single byte value into the builder's values array.
    ///
    /// Note, when appending individual byte values you must call `append` to delimit each
    /// distinct list value.
    #[inline]
    pub fn append_byte(&mut self, value: u8) {
        self.builder.values().append_value(value);
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.builder.values().append_slice(value.as_ref());
        self.builder.append(true);
    }

    /// Finish the current variable-length list array slot.
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) {
        self.append(false)
    }

    /// Builds the `BinaryArray` and reset this builder.
    pub fn finish(&mut self) -> GenericBinaryArray<OffsetSize> {
        GenericBinaryArray::<OffsetSize>::from(self.builder.finish())
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
        self.builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::builder::{BinaryBuilder, LargeBinaryBuilder};
    use crate::array::Array;

    #[test]
    fn test_binary_array_builder() {
        let mut builder = BinaryBuilder::new(20);

        builder.append_byte(b'h');
        builder.append_byte(b'e');
        builder.append_byte(b'l');
        builder.append_byte(b'l');
        builder.append_byte(b'o');
        builder.append(true);
        builder.append(true);
        builder.append_byte(b'w');
        builder.append_byte(b'o');
        builder.append_byte(b'r');
        builder.append_byte(b'l');
        builder.append_byte(b'd');
        builder.append(true);

        let binary_array = builder.finish();

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_large_binary_array_builder() {
        let mut builder = LargeBinaryBuilder::new(20);

        builder.append_byte(b'h');
        builder.append_byte(b'e');
        builder.append_byte(b'l');
        builder.append_byte(b'l');
        builder.append_byte(b'o');
        builder.append(true);
        builder.append(true);
        builder.append_byte(b'w');
        builder.append_byte(b'o');
        builder.append_byte(b'r');
        builder.append_byte(b'l');
        builder.append_byte(b'd');
        builder.append(true);

        let binary_array = builder.finish();

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(5, binary_array.value_length(2));
    }
}
