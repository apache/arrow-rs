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

use crate::array::Array;
use crate::builder::ArrayBuilder;
use crate::{ArrayRef, FixedSizeBinaryArray};
use arrow_buffer::Buffer;
use arrow_buffer::NullBufferBuilder;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::sync::Arc;

/// Builder for [`FixedSizeBinaryArray`]
/// ```
/// # use arrow_array::builder::FixedSizeBinaryBuilder;
/// # use arrow_array::Array;
/// #
/// let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 5);
/// // [b"hello", null, b"arrow"]
/// builder.append_value(b"hello").unwrap();
/// builder.append_null();
/// builder.append_value(b"arrow").unwrap();
///
/// let array = builder.finish();
/// assert_eq!(array.value(0), b"hello");
/// assert!(array.is_null(1));
/// assert_eq!(array.value(2), b"arrow");
/// ```
#[derive(Debug)]
pub struct FixedSizeBinaryBuilder {
    values_builder: Vec<u8>,
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
            "value length ({byte_width}) of the array must >= 0"
        );
        Self {
            values_builder: Vec::with_capacity(capacity * byte_width as usize),
            null_buffer_builder: NullBufferBuilder::new(capacity),
            value_length: byte_width,
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically update the null buffer to delimit the slice appended in as a
    /// distinct value element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<(), ArrowError> {
        if self.value_length != value.as_ref().len() as i32 {
            Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths"
                    .to_string(),
            ))
        } else {
            self.values_builder.extend_from_slice(value.as_ref());
            self.null_buffer_builder.append_non_null();
            Ok(())
        }
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) {
        self.values_builder
            .extend(std::iter::repeat_n(0u8, self.value_length as usize));
        self.null_buffer_builder.append_null();
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.values_builder
            .extend(std::iter::repeat_n(0u8, self.value_length as usize * n));
        self.null_buffer_builder.append_n_nulls(n);
    }

    /// Appends all elements in array into the builder.
    pub fn append_array(&mut self, array: &FixedSizeBinaryArray) -> Result<(), ArrowError> {
        if self.value_length != array.value_length() {
            return Err(ArrowError::InvalidArgumentError(
                "Cannot append FixedSizeBinaryArray with different value length".to_string(),
            ));
        }
        let buffer = array.value_data();
        self.values_builder.extend_from_slice(buffer);
        if let Some(validity) = array.nulls() {
            self.null_buffer_builder.append_buffer(validity);
        } else {
            self.null_buffer_builder.append_n_non_nulls(array.len());
        }
        Ok(())
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[u8] {
        self.values_builder.as_slice()
    }

    /// Builds the [`FixedSizeBinaryArray`] and reset this builder.
    pub fn finish(&mut self) -> FixedSizeBinaryArray {
        let array_length = self.len();
        let array_data_builder = ArrayData::builder(DataType::FixedSizeBinary(self.value_length))
            .add_buffer(std::mem::take(&mut self.values_builder).into())
            .nulls(self.null_buffer_builder.finish())
            .len(array_length);
        let array_data = unsafe { array_data_builder.build_unchecked() };
        FixedSizeBinaryArray::from(array_data)
    }

    /// Builds the [`FixedSizeBinaryArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> FixedSizeBinaryArray {
        let array_length = self.len();
        let values_buffer = Buffer::from_slice_ref(self.values_builder.as_slice());
        let array_data_builder = ArrayData::builder(DataType::FixedSizeBinary(self.value_length))
            .add_buffer(values_buffer)
            .nulls(self.null_buffer_builder.finish_cloned())
            .len(array_length);
        let array_data = unsafe { array_data_builder.build_unchecked() };
        FixedSizeBinaryArray::from(array_data)
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
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

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Array;

    #[test]
    fn test_fixed_size_binary_builder() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 5);

        //  [b"hello", null, "arrow", null, null, "world"]
        builder.append_value(b"hello").unwrap();
        builder.append_null();
        builder.append_value(b"arrow").unwrap();
        builder.append_nulls(2);
        builder.append_value(b"world").unwrap();
        let array: FixedSizeBinaryArray = builder.finish();

        assert_eq!(&DataType::FixedSizeBinary(5), array.data_type());
        assert_eq!(6, array.len());
        assert_eq!(3, array.null_count());
        assert_eq!(10, array.value_offset(2));
        assert_eq!(15, array.value_offset(3));
        assert_eq!(5, array.value_length());
        assert!(array.is_null(3));
        assert!(array.is_null(4));
    }

    #[test]
    fn test_fixed_size_binary_builder_finish_cloned() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 5);

        //  [b"hello", null, "arrow"]
        builder.append_value(b"hello").unwrap();
        builder.append_null();
        builder.append_value(b"arrow").unwrap();
        let mut array: FixedSizeBinaryArray = builder.finish_cloned();

        assert_eq!(&DataType::FixedSizeBinary(5), array.data_type());
        assert_eq!(3, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(10, array.value_offset(2));
        assert_eq!(5, array.value_length());

        //  [b"finis", null, "clone"]
        builder.append_value(b"finis").unwrap();
        builder.append_null();
        builder.append_value(b"clone").unwrap();

        array = builder.finish();

        assert_eq!(&DataType::FixedSizeBinary(5), array.data_type());
        assert_eq!(6, array.len());
        assert_eq!(2, array.null_count());
        assert_eq!(25, array.value_offset(5));
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

    #[test]
    fn test_fixed_size_binary_builder_append_array() {
        let mut other_builder = FixedSizeBinaryBuilder::with_capacity(3, 5);
        other_builder.append_value(b"hello").unwrap();
        other_builder.append_null();
        other_builder.append_value(b"arrow").unwrap();
        let other_array = other_builder.finish();

        let mut builder = FixedSizeBinaryBuilder::with_capacity(6, 5);
        builder.append_array(&other_array).unwrap();
        // Append again to test if breaks when appending multiple times
        builder.append_array(&other_array).unwrap();
        let array = builder.finish();

        assert_eq!(array.value_length(), other_array.value_length());
        assert_eq!(&DataType::FixedSizeBinary(5), array.data_type());
        assert_eq!(6, array.len());
        assert_eq!(2, array.null_count());
        for i in 0..6 {
            assert_eq!(i * 5, array.value_offset(i as usize));
        }

        assert_eq!(b"hello", array.value(0));
        assert!(array.is_null(1));
        assert_eq!(b"arrow", array.value(2));

        assert_eq!(b"hello", array.value(3));
        assert!(array.is_null(4));
        assert_eq!(b"arrow", array.value(5));
    }

    #[test]
    #[should_panic(expected = "Cannot append FixedSizeBinaryArray with different value length")]
    fn test_fixed_size_binary_builder_append_array_invalid_value_length() {
        let mut other_builder = FixedSizeBinaryBuilder::with_capacity(3, 4);
        other_builder.append_value(b"test").unwrap();
        let other_array = other_builder.finish();
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 5);
        builder.append_array(&other_array).unwrap();
    }
}
