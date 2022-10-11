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
use crate::builder::{ArrayBuilder, BufferBuilder};
use crate::types::*;
use crate::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_data::ArrayData;
use std::any::Any;
use std::sync::Arc;

pub type Int8Builder = PrimitiveBuilder<Int8Type>;
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBuilder = PrimitiveBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBuilder = PrimitiveBuilder<TimestampNanosecondType>;
pub type Date32Builder = PrimitiveBuilder<Date32Type>;
pub type Date64Builder = PrimitiveBuilder<Date64Type>;
pub type Time32SecondBuilder = PrimitiveBuilder<Time32SecondType>;
pub type Time32MillisecondBuilder = PrimitiveBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBuilder = PrimitiveBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBuilder = PrimitiveBuilder<Time64NanosecondType>;
pub type IntervalYearMonthBuilder = PrimitiveBuilder<IntervalYearMonthType>;
pub type IntervalDayTimeBuilder = PrimitiveBuilder<IntervalDayTimeType>;
pub type IntervalMonthDayNanoBuilder = PrimitiveBuilder<IntervalMonthDayNanoType>;
pub type DurationSecondBuilder = PrimitiveBuilder<DurationSecondType>;
pub type DurationMillisecondBuilder = PrimitiveBuilder<DurationMillisecondType>;
pub type DurationMicrosecondBuilder = PrimitiveBuilder<DurationMicrosecondType>;
pub type DurationNanosecondBuilder = PrimitiveBuilder<DurationNanosecondType>;

pub type Decimal128Builder = PrimitiveBuilder<Decimal128Type>;
pub type Decimal256Builder = PrimitiveBuilder<Decimal256Type>;

///  Array builder for fixed-width primitive types
#[derive(Debug)]
pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    values_builder: BufferBuilder<T::Native>,
    null_buffer_builder: NullBufferBuilder,
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

impl<T: ArrowPrimitiveType> Default for PrimitiveBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    /// Creates a new primitive array builder
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Creates a new primitive array builder with capacity no of items
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values_builder: BufferBuilder::<T::Native>::new(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: T::Native) {
        self.null_buffer_builder.append_non_null();
        self.values_builder.append(v);
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.values_builder.advance(1);
    }

    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.null_buffer_builder.append_n_nulls(n);
        self.values_builder.advance(n);
    }

    /// Appends an `Option<T>` into the builder
    #[inline]
    pub fn append_option(&mut self, v: Option<T::Native>) {
        match v {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Appends a slice of type `T` into the builder
    #[inline]
    pub fn append_slice(&mut self, v: &[T::Native]) {
        self.null_buffer_builder.append_n_non_nulls(v.len());
        self.values_builder.append_slice(v);
    }

    /// Appends values from a slice of type `T` and a validity boolean slice
    #[inline]
    pub fn append_values(&mut self, values: &[T::Native], is_valid: &[bool]) {
        assert_eq!(
            values.len(),
            is_valid.len(),
            "Value and validity lengths must be equal"
        );
        self.null_buffer_builder.append_slice(is_valid);
        self.values_builder.append_slice(values);
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
    ) {
        let iter = iter.into_iter();
        let len = iter
            .size_hint()
            .1
            .expect("append_trusted_len_iter requires an upper bound");

        self.null_buffer_builder.append_n_non_nulls(len);
        self.values_builder.append_trusted_len_iter(iter);
    }

    /// Builds the [`PrimitiveArray`] and reset this builder.
    pub fn finish(&mut self) -> PrimitiveArray<T> {
        let len = self.len();
        let null_bit_buffer = self.null_buffer_builder.finish();
        let builder = ArrayData::builder(T::DATA_TYPE)
            .len(len)
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        PrimitiveArray::<T>::from(array_data)
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[T::Native] {
        self.values_builder.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::Buffer;

    use crate::array::Array;
    use crate::array::BooleanArray;
    use crate::array::Date32Array;
    use crate::array::Int32Array;
    use crate::array::TimestampSecondArray;
    use crate::builder::Int32Builder;

    #[test]
    fn test_primitive_array_builder_i32() {
        let mut builder = Int32Array::builder(5);
        for i in 0..5 {
            builder.append_value(i);
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
        unsafe { builder.append_trusted_len_iter(0..5) };
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
        builder.append_nulls(5);
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
            builder.append_value(i);
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
            builder.append_value(i);
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
    fn test_primitive_array_builder_bool() {
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
    fn test_primitive_array_builder_append_option() {
        let arr1 = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_option(Some(0));
        builder.append_option(None);
        builder.append_option(Some(2));
        builder.append_option(None);
        builder.append_option(Some(4));
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
        builder.append_value(0);
        builder.append_value(2);
        builder.append_null();
        builder.append_null();
        builder.append_value(4);
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
        builder.append_slice(&[0, 2]);
        builder.append_null();
        builder.append_null();
        builder.append_value(4);
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
        let mut builder = Int32Builder::new();
        builder.append_slice(&[2, 4, 6, 8]);
        let mut arr = builder.finish();
        assert_eq!(4, arr.len());
        assert_eq!(0, builder.len());

        builder.append_slice(&[1, 3, 5, 7, 9]);
        arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }
}
