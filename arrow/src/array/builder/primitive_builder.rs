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

use crate::array::ArrayData;
use crate::array::ArrayRef;
use crate::array::PrimitiveArray;
use crate::datatypes::ArrowPrimitiveType;
use crate::error::{ArrowError, Result};

use super::{ArrayBuilder, BooleanBufferBuilder, BufferBuilder};

///  Array builder for fixed-width primitive types
#[derive(Debug)]
pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    values_builder: BufferBuilder<T::Native>,
    /// We only materialize the builder when we add `false`.
    /// This optimization is **very** important for performance of `StringBuilder`.
    bitmap_builder: Option<BooleanBufferBuilder>,
}

impl<T: ArrowPrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
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

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    /// Creates a new primitive array builder
    pub fn new(capacity: usize) -> Self {
        Self {
            values_builder: BufferBuilder::<T::Native>::new(capacity),
            bitmap_builder: None,
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: T::Native) -> Result<()> {
        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append(true);
        }
        self.values_builder.append(v);
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

    #[inline]
    pub fn append_nulls(&mut self, n: usize) -> Result<()> {
        self.materialize_bitmap_builder();
        self.bitmap_builder.as_mut().unwrap().append_n(n, false);
        self.values_builder.advance(n);
        Ok(())
    }

    /// Appends an `Option<T>` into the builder
    #[inline]
    pub fn append_option(&mut self, v: Option<T::Native>) -> Result<()> {
        match v {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Appends a slice of type `T` into the builder
    #[inline]
    pub fn append_slice(&mut self, v: &[T::Native]) -> Result<()> {
        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append_n(v.len(), true);
        }
        self.values_builder.append_slice(v);
        Ok(())
    }

    /// Appends values from a slice of type `T` and a validity boolean slice
    #[inline]
    pub fn append_values(
        &mut self,
        values: &[T::Native],
        is_valid: &[bool],
    ) -> Result<()> {
        if values.len() != is_valid.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Value and validity lengths must be equal".to_string(),
            ));
        }
        if is_valid.iter().any(|v| !*v) {
            self.materialize_bitmap_builder();
        }
        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append_slice(is_valid);
        }
        self.values_builder.append_slice(values);
        Ok(())
    }

    /// Appends values from a trusted length iterator.
    ///
    /// # Safety
    /// This requires the iterator be a trusted length. This could instead require
    /// the iterator implement `TrustedLen` once that is stabilized.
    #[inline]
    pub unsafe fn append_trusted_len_iter(
        &mut self,
        iter: impl IntoIterator<Item = T::Native>,
    ) -> Result<()> {
        let iter = iter.into_iter();
        let len = iter
            .size_hint()
            .1
            .expect("append_trusted_len_iter requires an upper bound");

        if let Some(b) = self.bitmap_builder.as_mut() {
            b.append_n(len, true);
        }
        self.values_builder.append_trusted_len_iter(iter);
        Ok(())
    }

    /// Builds the `PrimitiveArray` and reset this builder.
    pub fn finish(&mut self) -> PrimitiveArray<T> {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.as_mut().map(|b| b.finish());
        let null_count = len
            - null_bit_buffer
                .as_ref()
                .map(|b| b.count_set_bits())
                .unwrap_or(len);
        let builder = ArrayData::builder(T::DATA_TYPE)
            .len(len)
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer(if null_count > 0 {
                null_bit_buffer
            } else {
                None
            });

        let array_data = unsafe { builder.build_unchecked() };
        PrimitiveArray::<T>::from(array_data)
    }

    fn materialize_bitmap_builder(&mut self) {
        if self.bitmap_builder.is_some() {
            return;
        }
        let mut b = BooleanBufferBuilder::new(0);
        b.reserve(self.values_builder.capacity());
        b.append_n(self.values_builder.len(), true);
        self.bitmap_builder = Some(b);
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[T::Native] {
        self.values_builder.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::array::Date32Array;
    use crate::array::Int32Array;
    use crate::array::Int32Builder;
    use crate::array::TimestampSecondArray;

    #[test]
    fn test_primitive_array_builder_i32() {
        let mut builder = Int32Array::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_i32_append_iter() {
        let mut builder = Int32Array::builder(5);
        unsafe { builder.append_trusted_len_iter(0..5) }.unwrap();
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_i32_append_nulls() {
        let mut builder = Int32Array::builder(5);
        builder.append_nulls(5).unwrap();
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(5, arr.null_count());
        for i in 0..5 {
            assert!(arr.is_null(i));
            assert!(!arr.is_valid(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_date32() {
        let mut builder = Date32Array::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_timestamp_second() {
        let mut builder = TimestampSecondArray::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i64, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_append_option() {
        let arr1 = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_option(Some(0)).unwrap();
        builder.append_option(None).unwrap();
        builder.append_option(Some(2)).unwrap();
        builder.append_option(None).unwrap();
        builder.append_option(Some(4)).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_append_null() {
        let arr1 = Int32Array::from(vec![Some(0), Some(2), None, None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_value(0).unwrap();
        builder.append_value(2).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(4).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_append_slice() {
        let arr1 = Int32Array::from(vec![Some(0), Some(2), None, None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_slice(&[0, 2]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(4).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_finish() {
        let mut builder = Int32Builder::new(5);
        builder.append_slice(&[2, 4, 6, 8]).unwrap();
        let mut arr = builder.finish();
        assert_eq!(4, arr.len());
        assert_eq!(0, builder.len());

        builder.append_slice(&[1, 3, 5, 7, 9]).unwrap();
        arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }
}
