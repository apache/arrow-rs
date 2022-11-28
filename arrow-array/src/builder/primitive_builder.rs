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
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::ArrayData;
use std::any::Any;
use std::sync::Arc;

/// A signed 8-bit integer array builder.
pub type Int8Builder = PrimitiveBuilder<Int8Type>;
/// A signed 16-bit integer array builder.
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
/// A signed 32-bit integer array builder.
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
/// A signed 64-bit integer array builder.
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
/// An usigned 8-bit integer array builder.
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
/// An usigned 16-bit integer array builder.
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
/// An usigned 32-bit integer array builder.
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
/// An usigned 64-bit integer array builder.
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
/// A 32-bit floating point array builder.
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
/// A 64-bit floating point array builder.
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

/// A timestamp second array builder.
pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
/// A timestamp millisecond array builder.
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;
/// A timestamp microsecond array builder.
pub type TimestampMicrosecondBuilder = PrimitiveBuilder<TimestampMicrosecondType>;
/// A timestamp nanosecond array builder.
pub type TimestampNanosecondBuilder = PrimitiveBuilder<TimestampNanosecondType>;

/// A 32-bit date array builder.
pub type Date32Builder = PrimitiveBuilder<Date32Type>;
/// A 64-bit date array builder.
pub type Date64Builder = PrimitiveBuilder<Date64Type>;

/// A 32-bit elaspsed time in seconds array builder.
pub type Time32SecondBuilder = PrimitiveBuilder<Time32SecondType>;
/// A 32-bit elaspsed time in milliseconds array builder.
pub type Time32MillisecondBuilder = PrimitiveBuilder<Time32MillisecondType>;
/// A 64-bit elaspsed time in microseconds array builder.
pub type Time64MicrosecondBuilder = PrimitiveBuilder<Time64MicrosecondType>;
/// A 64-bit elaspsed time in nanoseconds array builder.
pub type Time64NanosecondBuilder = PrimitiveBuilder<Time64NanosecondType>;

/// A “calendar” interval in months array builder.
pub type IntervalYearMonthBuilder = PrimitiveBuilder<IntervalYearMonthType>;
/// A “calendar” interval in days and milliseconds array builder.
pub type IntervalDayTimeBuilder = PrimitiveBuilder<IntervalDayTimeType>;
/// A “calendar” interval in months, days, and nanoseconds array builder.
pub type IntervalMonthDayNanoBuilder = PrimitiveBuilder<IntervalMonthDayNanoType>;

/// An elapsed time in seconds array builder.
pub type DurationSecondBuilder = PrimitiveBuilder<DurationSecondType>;
/// An elapsed time in milliseconds array builder.
pub type DurationMillisecondBuilder = PrimitiveBuilder<DurationMillisecondType>;
/// An elapsed time in microseconds array builder.
pub type DurationMicrosecondBuilder = PrimitiveBuilder<DurationMicrosecondType>;
/// An elapsed time in nanoseconds array builder.
pub type DurationNanosecondBuilder = PrimitiveBuilder<DurationNanosecondType>;

/// A decimal 128 array builder
pub type Decimal128Builder = PrimitiveBuilder<Decimal128Type>;
/// A decimal 256 array builder
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

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
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

    /// Creates a new primitive array builder from buffers
    pub fn new_from_buffer(
        values_buffer: MutableBuffer,
        null_buffer: Option<MutableBuffer>,
    ) -> Self {
        let values_builder = BufferBuilder::<T::Native>::new_from_buffer(values_buffer);

        let null_buffer_builder = null_buffer
            .map(|buffer| {
                NullBufferBuilder::new_from_buffer(buffer, values_builder.len())
            })
            .unwrap_or_else(|| NullBufferBuilder::new_with_len(values_builder.len()));

        Self {
            values_builder,
            null_buffer_builder,
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

    /// Appends `n` no. of null's into the builder
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

    /// Builds the [`PrimitiveArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> PrimitiveArray<T> {
        let len = self.len();
        let null_bit_buffer = self
            .null_buffer_builder
            .as_slice()
            .map(Buffer::from_slice_ref);
        let values_buffer = Buffer::from_slice_ref(self.values_builder.as_slice());
        let builder = ArrayData::builder(T::DATA_TYPE)
            .len(len)
            .add_buffer(values_buffer)
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        PrimitiveArray::<T>::from(array_data)
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[T::Native] {
        self.values_builder.as_slice()
    }

    /// Returns the current values buffer as a mutable slice
    pub fn values_slice_mut(&mut self) -> &mut [T::Native] {
        self.values_builder.as_slice_mut()
    }

    /// Returns the current values buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }

    /// Returns the current values buffer as a mutable slice
    pub fn validity_slice_mut(&mut self) -> Option<&mut [u8]> {
        self.null_buffer_builder.as_slice_mut()
    }

    /// Returns the current values buffer and null buffer as a slice
    pub fn slices_mut(&mut self) -> (&mut [T::Native], Option<&mut [u8]>) {
        (
            self.values_builder.as_slice_mut(),
            self.null_buffer_builder.as_slice_mut(),
        )
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

    #[test]
    fn test_primitive_array_builder_finish_cloned() {
        let mut builder = Int32Builder::new();
        builder.append_value(23);
        builder.append_value(45);
        let result = builder.finish_cloned();
        assert_eq!(result, Int32Array::from(vec![23, 45]));
        builder.append_value(56);
        assert_eq!(builder.finish_cloned(), Int32Array::from(vec![23, 45, 56]));

        builder.append_slice(&[2, 4, 6, 8]);
        let mut arr = builder.finish();
        assert_eq!(7, arr.len());
        assert_eq!(arr, Int32Array::from(vec![23, 45, 56, 2, 4, 6, 8]));
        assert_eq!(0, builder.len());

        builder.append_slice(&[1, 3, 5, 7, 9]);
        arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }
}
