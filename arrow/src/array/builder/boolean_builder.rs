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

use std::any::Any;
use std::sync::Arc;

use crate::array::ArrayBuilder;
use crate::array::ArrayData;
use crate::array::ArrayRef;
use crate::array::BooleanArray;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};

use super::BooleanBufferBuilder;

///  Array builder for fixed-width primitive types
///
/// # Example
///
/// Create a `BooleanArray` from a `BooleanBuilder`
///
/// ```
///     use arrow::array::{Array, BooleanArray, BooleanBuilder};
///
///     let mut b = BooleanBuilder::new(4);
///     b.append_value(true);
///     b.append_null();
///     b.append_value(false);
///     b.append_value(true);
///     let arr = b.finish();
///
///     assert_eq!(4, arr.len());
///     assert_eq!(1, arr.null_count());
///     assert_eq!(true, arr.value(0));
///     assert!(arr.is_valid(0));
///     assert!(!arr.is_null(0));
///     assert!(!arr.is_valid(1));
///     assert!(arr.is_null(1));
///     assert_eq!(false, arr.value(2));
///     assert!(arr.is_valid(2));
///     assert!(!arr.is_null(2));
///     assert_eq!(true, arr.value(3));
///     assert!(arr.is_valid(3));
///     assert!(!arr.is_null(3));
/// ```
#[derive(Debug)]
pub struct BooleanBuilder {
    values_builder: BooleanBufferBuilder,
    /// We only materialize the builder when we add `false`.
    /// This optimization is **very** important for the performance.
    bitmap_builder: Option<BooleanBufferBuilder>,
}

impl BooleanBuilder {
    /// Creates a new primitive array builder
    pub fn new(capacity: usize) -> Self {
        Self {
            values_builder: BooleanBufferBuilder::new(capacity),
            bitmap_builder: None,
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: bool) -> Result<()> {
        self.values_builder.append(v);
        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append(true)
        }
        Ok(())
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.materialize_bitmap_builder();
        self.bitmap_builder.as_mut().unwrap().append(false);
        self.values_builder.advance(1);
        Ok(())
    }

    /// Appends an `Option<T>` into the builder
    #[inline]
    pub fn append_option(&mut self, v: Option<bool>) -> Result<()> {
        match v {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Appends a slice of type `T` into the builder
    #[inline]
    pub fn append_slice(&mut self, v: &[bool]) -> Result<()> {
        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append_n(v.len(), true)
        }
        self.values_builder.append_slice(v);
        Ok(())
    }

    /// Appends values from a slice of type `T` and a validity boolean slice
    #[inline]
    pub fn append_values(&mut self, values: &[bool], is_valid: &[bool]) -> Result<()> {
        if values.len() != is_valid.len() {
            Err(ArrowError::InvalidArgumentError(
                "Value and validity lengths must be equal".to_string(),
            ))
        } else {
            is_valid
                .iter()
                .any(|v| !*v)
                .then(|| self.materialize_bitmap_builder());
            if let Some(b) = self.bitmap_builder.as_mut() {
                b.append_slice(is_valid)
            }
            self.values_builder.append_slice(values);
            Ok(())
        }
    }

    /// Builds the [BooleanArray] and reset this builder.
    pub fn finish(&mut self) -> BooleanArray {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.as_mut().map(|b| b.finish());
        let builder = ArrayData::builder(DataType::Boolean)
            .len(len)
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        BooleanArray::from(array_data)
    }

    fn materialize_bitmap_builder(&mut self) {
        if self.bitmap_builder.is_none() {
            let mut b = BooleanBufferBuilder::new(0);
            b.reserve(self.values_builder.capacity());
            b.append_n(self.values_builder.len(), true);
            self.bitmap_builder = Some(b);
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array::Array, buffer::Buffer};

    #[test]
    fn test_boolean_array_builder() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BooleanArray::builder(10);
        for i in 0..10 {
            if i == 3 || i == 6 || i == 9 {
                builder.append_value(true).unwrap();
            } else {
                builder.append_value(false).unwrap();
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
        builder.append_slice(&[true, false]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(false).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_boolean_array_builder_append_slice_large() {
        let arr1 = BooleanArray::from(vec![true; 513]);

        let mut builder = BooleanArray::builder(512);
        builder.append_slice(&[true; 513]).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_boolean_array_builder_no_null() {
        let mut builder = BooleanArray::builder(0);
        builder.append_option(Some(true)).unwrap();
        builder.append_value(false).unwrap();
        builder.append_slice(&[true, false, true]).unwrap();
        builder
            .append_values(&[false, false, true], &[true, true, true])
            .unwrap();

        let array = builder.finish();
        assert_eq!(0, array.null_count());
        assert!(array.data().null_buffer().is_none());
    }
}
