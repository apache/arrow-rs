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
use crate::builder::{ArrayBuilder, BooleanBufferBuilder};
use crate::{ArrayRef, BooleanArray};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::sync::Arc;

///  Array builder for fixed-width primitive types
///
/// # Example
///
/// Create a `BooleanArray` from a `BooleanBuilder`
///
/// ```
///
/// # use arrow_array::{Array, BooleanArray, builder::BooleanBuilder};
///
/// let mut b = BooleanBuilder::new();
/// b.append_value(true);
/// b.append_null();
/// b.append_value(false);
/// b.append_value(true);
/// let arr = b.finish();
///
/// assert_eq!(4, arr.len());
/// assert_eq!(1, arr.null_count());
/// assert_eq!(true, arr.value(0));
/// assert!(arr.is_valid(0));
/// assert!(!arr.is_null(0));
/// assert!(!arr.is_valid(1));
/// assert!(arr.is_null(1));
/// assert_eq!(false, arr.value(2));
/// assert!(arr.is_valid(2));
/// assert!(!arr.is_null(2));
/// assert_eq!(true, arr.value(3));
/// assert!(arr.is_valid(3));
/// assert!(!arr.is_null(3));
/// ```
#[derive(Debug)]
pub struct BooleanBuilder {
    values_builder: BooleanBufferBuilder,
    null_buffer_builder: NullBufferBuilder,
}

impl Default for BooleanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanBuilder {
    /// Creates a new boolean builder
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Creates a new boolean builder with space for `capacity` elements without re-allocating
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values_builder: BooleanBufferBuilder::new(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: bool) {
        self.values_builder.append(v);
        self.null_buffer_builder.append_non_null();
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.values_builder.advance(1);
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.null_buffer_builder.append_n_nulls(n);
        self.values_builder.advance(n);
    }

    /// Appends an `Option<T>` into the builder
    #[inline]
    pub fn append_option(&mut self, v: Option<bool>) {
        match v {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Appends a slice of type `T` into the builder
    #[inline]
    pub fn append_slice(&mut self, v: &[bool]) {
        self.values_builder.append_slice(v);
        self.null_buffer_builder.append_n_non_nulls(v.len());
    }

    /// Appends values from a slice of type `T` and a validity boolean slice.
    ///
    /// Returns an error if the slices are of different lengths
    #[inline]
    pub fn append_values(
        &mut self,
        values: &[bool],
        is_valid: &[bool],
    ) -> Result<(), ArrowError> {
        if values.len() != is_valid.len() {
            Err(ArrowError::InvalidArgumentError(
                "Value and validity lengths must be equal".to_string(),
            ))
        } else {
            self.null_buffer_builder.append_slice(is_valid);
            self.values_builder.append_slice(values);
            Ok(())
        }
    }

    /// Builds the [BooleanArray] and reset this builder.
    pub fn finish(&mut self) -> BooleanArray {
        let len = self.len();
        let null_bit_buffer = self.null_buffer_builder.finish();
        let builder = ArrayData::builder(DataType::Boolean)
            .len(len)
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        BooleanArray::from(array_data)
    }

    /// Builds the [BooleanArray] without resetting the builder.
    pub fn finish_cloned(&self) -> BooleanArray {
        let len = self.len();
        let null_bit_buffer = self
            .null_buffer_builder
            .as_slice()
            .map(Buffer::from_slice_ref);
        let value_buffer = Buffer::from_slice_ref(self.values_builder.as_slice());
        let builder = ArrayData::builder(DataType::Boolean)
            .len(len)
            .add_buffer(value_buffer)
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        BooleanArray::from(array_data)
    }
}

impl ArrayBuilder for BooleanBuilder {
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
        self.values_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.values_builder.is_empty()
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
    use arrow_buffer::Buffer;

    #[test]
    fn test_boolean_array_builder() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BooleanArray::builder(10);
        for i in 0..10 {
            if i == 3 || i == 6 || i == 9 {
                builder.append_value(true);
            } else {
                builder.append_value(false);
            }
        }

        let arr = builder.finish();
        assert_eq!(&buf, arr.values());
        assert_eq!(10, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..10 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i == 3 || i == 6 || i == 9, arr.value(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_array_builder_append_slice() {
        let arr1 =
            BooleanArray::from(vec![Some(true), Some(false), None, None, Some(false)]);

        let mut builder = BooleanArray::builder(0);
        builder.append_slice(&[true, false]);
        builder.append_null();
        builder.append_null();
        builder.append_value(false);
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_boolean_array_builder_append_slice_large() {
        let arr1 = BooleanArray::from(vec![true; 513]);

        let mut builder = BooleanArray::builder(512);
        builder.append_slice(&[true; 513]);
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_boolean_array_builder_no_null() {
        let mut builder = BooleanArray::builder(0);
        builder.append_option(Some(true));
        builder.append_value(false);
        builder.append_slice(&[true, false, true]);
        builder
            .append_values(&[false, false, true], &[true, true, true])
            .unwrap();

        let array = builder.finish();
        assert_eq!(0, array.null_count());
        assert!(array.data().null_buffer().is_none());
    }

    #[test]
    fn test_boolean_array_builder_finish_cloned() {
        let mut builder = BooleanArray::builder(16);
        builder.append_option(Some(true));
        builder.append_value(false);
        builder.append_slice(&[true, false, true]);
        let mut array = builder.finish_cloned();
        assert_eq!(3, array.true_count());
        assert_eq!(2, array.false_count());

        builder
            .append_values(&[false, false, true], &[true, true, true])
            .unwrap();

        array = builder.finish();
        assert_eq!(4, array.true_count());
        assert_eq!(4, array.false_count());

        assert_eq!(0, array.null_count());
        assert!(array.data().null_buffer().is_none());
    }
}
