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
    ArrayBuilder, ArrayData, ArrayRef, FixedSizeBinaryArray, UInt8BufferBuilder,
};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use std::any::Any;
use std::sync::Arc;

use super::NullBufferBuilder;

#[derive(Debug)]
pub struct FixedSizeBinaryBuilder {
    values_builder: UInt8BufferBuilder,
    null_buffer_builder: NullBufferBuilder,
    value_length: i32,
}

impl FixedSizeBinaryBuilder {
    /// Creates a new [`FixedSizeBinaryBuilder`]
    pub fn new(byte_width: i32) -> Self {
        Self::with_capacity(1024, byte_width)
    }

    /// Creates a new [`FixedSizeBinaryBuilder`], `capacity` is the number of byte slices
    /// that can be appended without reallocating
    pub fn with_capacity(capacity: usize, byte_width: i32) -> Self {
        assert!(
            byte_width >= 0,
            "value length ({}) of the array must >= 0",
            byte_width
        );
        Self {
            values_builder: UInt8BufferBuilder::new(capacity * byte_width as usize),
            null_buffer_builder: NullBufferBuilder::new(capacity),
            value_length: byte_width,
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically update the null buffer to delimit the slice appended in as a
    /// distinct value element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<()> {
        if self.value_length != value.as_ref().len() as i32 {
            Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths".to_string()
            ))
        } else {
            self.values_builder.append_slice(value.as_ref());
            self.null_buffer_builder.append_non_null();
            Ok(())
        }
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) {
        self.values_builder
            .append_slice(&vec![0u8; self.value_length as usize][..]);
        self.null_buffer_builder.append_null();
    }

    /// Builds the [`FixedSizeBinaryArray`] and reset this builder.
    pub fn finish(&mut self) -> FixedSizeBinaryArray {
        let array_length = self.len();
        let array_data_builder =
            ArrayData::builder(DataType::FixedSizeBinary(self.value_length))
                .add_buffer(self.values_builder.finish())
                .null_bit_buffer(self.null_buffer_builder.finish())
                .len(array_length);
        let array_data = unsafe { array_data_builder.build_unchecked() };
        FixedSizeBinaryArray::from(array_data)
    }
}

impl ArrayBuilder for FixedSizeBinaryBuilder {
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

    use crate::array::Array;
    use crate::array::FixedSizeBinaryArray;
    use crate::datatypes::DataType;

    #[test]
    fn test_fixed_size_binary_builder() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 5);

        //  [b"hello", null, "arrow"]
        builder.append_value(b"hello").unwrap();
        builder.append_null();
        builder.append_value(b"arrow").unwrap();
        let array: FixedSizeBinaryArray = builder.finish();

        assert_eq!(&DataType::FixedSizeBinary(5), array.data_type());
        assert_eq!(3, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(10, array.value_offset(2));
        assert_eq!(5, array.value_length());
    }

    #[test]
    fn test_fixed_size_binary_builder_with_zero_value_length() {
        let mut builder = FixedSizeBinaryBuilder::new(0);

        builder.append_value(b"").unwrap();
        builder.append_null();
        builder.append_value(b"").unwrap();
        assert!(!builder.is_empty());

        let array: FixedSizeBinaryArray = builder.finish();
        assert_eq!(&DataType::FixedSizeBinary(0), array.data_type());
        assert_eq!(3, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(0, array.value_offset(2));
        assert_eq!(0, array.value_length());
        assert_eq!(b"", array.value(0));
        assert_eq!(b"", array.value(2));
    }

    #[test]
    #[should_panic(
        expected = "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths"
    )]
    fn test_fixed_size_binary_builder_with_inconsistent_value_length() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 4);
        builder.append_value(b"hello").unwrap();
    }
    #[test]
    fn test_fixed_size_binary_builder_empty() {
        let mut builder = FixedSizeBinaryBuilder::new(5);
        assert!(builder.is_empty());

        let fixed_size_binary_array = builder.finish();
        assert_eq!(
            &DataType::FixedSizeBinary(5),
            fixed_size_binary_array.data_type()
        );
        assert_eq!(0, fixed_size_binary_array.len());
    }

    #[test]
    #[should_panic(expected = "value length (-1) of the array must >= 0")]
    fn test_fixed_size_binary_builder_invalid_value_length() {
        let _ = FixedSizeBinaryBuilder::with_capacity(15, -1);
    }
}
