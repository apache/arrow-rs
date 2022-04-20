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

//! Defines a [`BufferBuilder`](crate::array::BufferBuilder) capable
//! of creating a [`Buffer`](crate::buffer::Buffer) which can be used
//! as an internal buffer in an [`ArrayData`](crate::array::ArrayData)
//! object.

use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use crate::array::*;
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

///  Converts a `MutableBuffer` to a `BufferBuilder<T>`.
///
/// `slots` is the number of array slots currently represented in the `MutableBuffer`.
pub(crate) fn mutable_buffer_to_builder<T: ArrowNativeType>(
    mutable_buffer: MutableBuffer,
    slots: usize,
) -> BufferBuilder<T> {
    BufferBuilder::<T> {
        buffer: mutable_buffer,
        len: slots,
        _marker: PhantomData,
    }
}

///  Converts a `BufferBuilder<T>` into its underlying `MutableBuffer`.
///
/// `From` is not implemented because associated type bounds are unstable.
pub(crate) fn builder_to_mutable_buffer<T: ArrowNativeType>(
    builder: BufferBuilder<T>,
) -> MutableBuffer {
    builder.buffer
}

/// Builder for creating a [`Buffer`](crate::buffer::Buffer) object.
///
/// A [`Buffer`](crate::buffer::Buffer) is the underlying data
/// structure of Arrow's [`Arrays`](crate::array::Array).
///
/// For all supported types, there are type definitions for the
/// generic version of `BufferBuilder<T>`, e.g. `UInt8BufferBuilder`.
///
/// # Example:
///
/// ```
/// use arrow::array::UInt8BufferBuilder;
///
/// # fn main() -> arrow::error::Result<()> {
/// let mut builder = UInt8BufferBuilder::new(100);
/// builder.append_slice(&[42, 43, 44]);
/// builder.append(45);
/// let buffer = builder.finish();
///
/// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 43, 44, 45]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BufferBuilder<T: ArrowNativeType> {
    buffer: MutableBuffer,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T: ArrowNativeType> BufferBuilder<T> {
    /// Creates a new builder with initial capacity for _at least_ `capacity`
    /// elements of type `T`.
    ///
    /// The capacity can later be manually adjusted with the
    /// [`reserve()`](BufferBuilder::reserve) method.
    /// Also the
    /// [`append()`](BufferBuilder::append),
    /// [`append_slice()`](BufferBuilder::append_slice) and
    /// [`advance()`](BufferBuilder::advance)
    /// methods automatically increase the capacity if needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    ///
    /// assert!(builder.capacity() >= 10);
    /// ```
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let buffer = MutableBuffer::new(capacity * mem::size_of::<T>());

        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Returns the current number of array elements in the internal buffer.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the internal buffer is empty.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the actual capacity (number of elements) of the internal buffer.
    ///
    /// Note: the internal capacity returned by this method might be larger than
    /// what you'd expect after setting the capacity in the `new()` or `reserve()`
    /// functions.
    pub fn capacity(&self) -> usize {
        let byte_capacity = self.buffer.capacity();
        byte_capacity / std::mem::size_of::<T>()
    }

    /// Increases the number of elements in the internal buffer by `n`
    /// and resizes the buffer as needed.
    ///
    /// The values of the newly added elements are 0.
    /// This method is usually used when appending `NULL` values to the buffer
    /// as they still require physical memory space.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.advance(2);
    ///
    /// assert_eq!(builder.len(), 2);
    /// ```
    #[inline]
    pub fn advance(&mut self, i: usize) {
        let new_buffer_len = (self.len + i) * mem::size_of::<T>();
        self.buffer.resize(new_buffer_len, 0);
        self.len += i;
    }

    /// Reserves memory for _at least_ `n` more elements of type `T`.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.reserve(10);
    ///
    /// assert!(builder.capacity() >= 20);
    /// ```
    #[inline]
    pub fn reserve(&mut self, n: usize) {
        self.buffer.reserve(n * mem::size_of::<T>());
    }

    /// Appends a value of type `T` into the builder,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    #[inline]
    pub fn append(&mut self, v: T) {
        self.reserve(1);
        self.buffer.push(v);
        self.len += 1;
    }

    /// Appends a value of type `T` into the builder N times,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_n(10, 42);
    ///
    /// assert_eq!(builder.len(), 10);
    /// ```
    #[inline]
    pub fn append_n(&mut self, n: usize, v: T) {
        self.reserve(n);
        for _ in 0..n {
            self.buffer.push(v);
        }
        self.len += n;
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// assert_eq!(builder.len(), 3);
    /// ```
    #[inline]
    pub fn append_slice(&mut self, slice: &[T]) {
        self.buffer.extend_from_slice(slice);
        self.len += slice.len();
    }

    /// # Safety
    /// This requires the iterator be a trusted length. This could instead require
    /// the iterator implement `TrustedLen` once that is stabilized.
    #[inline]
    pub unsafe fn append_trusted_len_iter(&mut self, iter: impl IntoIterator<Item = T>) {
        let iter = iter.into_iter();
        let len = iter
            .size_hint()
            .1
            .expect("append_trusted_len_iter expects upper bound");
        self.reserve(len);
        for v in iter {
            self.buffer.push(v)
        }
        self.len += len;
    }

    /// Resets this builder and returns an immutable [`Buffer`](crate::buffer::Buffer).
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::UInt8BufferBuilder;
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// let buffer = builder.finish();
    ///
    /// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 44, 46]);
    /// ```
    #[inline]
    pub fn finish(&mut self) -> Buffer {
        let buf = std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
        self.len = 0;
        buf.into()
    }
}

#[derive(Debug)]
pub struct BooleanBufferBuilder {
    buffer: MutableBuffer,
    len: usize,
}

impl BooleanBufferBuilder {
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let byte_capacity = bit_util::ceil(capacity, 8);
        let buffer = MutableBuffer::new(byte_capacity);
        Self { buffer, len: 0 }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn set_bit(&mut self, index: usize, v: bool) {
        if v {
            bit_util::set_bit(self.buffer.as_mut(), index);
        } else {
            bit_util::unset_bit(self.buffer.as_mut(), index);
        }
    }

    #[inline]
    pub fn get_bit(&self, index: usize) -> bool {
        bit_util::get_bit(self.buffer.as_slice(), index)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    #[inline]
    pub fn advance(&mut self, additional: usize) {
        let new_len = self.len + additional;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        if new_len_bytes > self.buffer.len() {
            self.buffer.resize(new_len_bytes, 0);
        }
        self.len = new_len;
    }

    #[inline]
    /// Advance wih every bit set to true, except for the last byte
    pub fn advance_true(&mut self, additional: usize) {
        let new_len = self.len + additional;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        if new_len_bytes > self.buffer.len() {
            self.buffer.resize(new_len_bytes, u8::MAX);
            self.buffer.as_slice_mut()[new_len_bytes - 1] = 0;
        }
        self.len = new_len;
    }

    /// Reserve space to at least `additional` new bits.
    /// Capacity will be `>= self.len() + additional`.
    /// New bytes are uninitialized and reading them is undefined behavior.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let capacity = self.len + additional;
        if capacity > self.capacity() {
            // convert differential to bytes
            let additional = bit_util::ceil(capacity, 8) - self.buffer.len();
            self.buffer.reserve(additional);
        }
    }

    /// Resizes the buffer, either truncating its contents (with no change in capacity), or
    /// growing it (potentially reallocating it) and writing `false` in the newly available bits.
    #[inline]
    pub fn resize(&mut self, len: usize) {
        let len_bytes = bit_util::ceil(len, 8);
        self.buffer.resize(len_bytes, 0);
        self.len = len;
    }

    #[inline]
    pub fn append(&mut self, v: bool) {
        self.advance(1);
        if v {
            unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), self.len - 1) };
        }
    }

    #[inline]
    pub fn append_n(&mut self, additional: usize, v: bool) {
        let start_bits = min(additional, 8 - self.len() % 8);

        if v {
            self.advance_true(additional);
        } else {
            self.advance(additional);
        }

        if additional > 0 && v {
            let offset = self.len() - additional;

            (0..start_bits).for_each(|i| unsafe {
                bit_util::set_bit_raw(self.buffer.as_mut_ptr(), offset + i)
            });
        }
    }

    #[inline]
    pub fn append_slice(&mut self, slice: &[bool]) {
        let additional = slice.len();
        self.advance(additional);

        let offset = self.len() - additional;
        for (i, v) in slice.iter().enumerate() {
            if *v {
                unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), offset + i) }
            }
        }
    }

    /// Append `range` bits from `to_set`
    ///
    /// `to_set` is a slice of bits packed LSB-first into `[u8]`
    ///
    /// # Panics
    ///
    /// Panics if `to_set` does not contain `ceil(range.end / 8)` bytes
    pub fn append_packed_range(&mut self, range: Range<usize>, to_set: &[u8]) {
        let offset_write = self.len;
        let len = range.end - range.start;
        self.advance(len);
        crate::util::bit_mask::set_bits(
            self.buffer.as_slice_mut(),
            to_set,
            offset_write,
            range.start,
            len,
        );
    }

    /// Returns the packed bits
    pub fn as_slice(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    #[inline]
    pub fn finish(&mut self) -> Buffer {
        let buf = std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
        self.len = 0;
        buf.into()
    }
}

impl From<BooleanBufferBuilder> for Buffer {
    #[inline]
    fn from(builder: BooleanBufferBuilder) -> Self {
        builder.buffer.into()
    }
}

/// Trait for dealing with different array builders at runtime
///
/// # Example
///
/// ```
/// # use arrow::{
/// #     array::{ArrayBuilder, ArrayRef, Float64Builder, Int64Builder, StringArray, StringBuilder},
/// #     error::ArrowError,
/// # };
/// # fn main() -> std::result::Result<(), ArrowError> {
/// // Create
/// let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
///     Box::new(Float64Builder::new(1024)),
///     Box::new(Int64Builder::new(1024)),
///     Box::new(StringBuilder::new(1024)),
/// ];
///
/// // Fill
/// data_builders[0]
///     .as_any_mut()
///     .downcast_mut::<Float64Builder>()
///     .unwrap()
///     .append_value(3.14)?;
/// data_builders[1]
///     .as_any_mut()
///     .downcast_mut::<Int64Builder>()
///     .unwrap()
///     .append_value(-1)?;
/// data_builders[2]
///     .as_any_mut()
///     .downcast_mut::<StringBuilder>()
///     .unwrap()
///     .append_value("🍎")?;
///
/// // Finish
/// let array_refs: Vec<ArrayRef> = data_builders
///     .iter_mut()
///     .map(|builder| builder.finish())
///     .collect();
/// assert_eq!(array_refs[0].len(), 1);
/// assert_eq!(array_refs[1].is_null(0), false);
/// assert_eq!(
///     array_refs[2]
///         .as_any()
///         .downcast_ref::<StringArray>()
///         .unwrap()
///         .value(0),
///     "🍎"
/// );
/// # Ok(())
/// # }
/// ```
pub trait ArrayBuilder: Any + Send {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Returns whether number of array slots is zero
    fn is_empty(&self) -> bool;

    /// Builds the array
    fn finish(&mut self) -> ArrayRef;

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any;

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any>;
}

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
    bitmap_builder: BooleanBufferBuilder,
}

impl BooleanBuilder {
    /// Creates a new primitive array builder
    pub fn new(capacity: usize) -> Self {
        Self {
            values_builder: BooleanBufferBuilder::new(capacity),
            bitmap_builder: BooleanBufferBuilder::new(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: bool) -> Result<()> {
        self.bitmap_builder.append(true);
        self.values_builder.append(v);
        Ok(())
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.bitmap_builder.append(false);
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
        self.bitmap_builder.append_n(v.len(), true);
        self.values_builder.append_slice(v);
        Ok(())
    }

    /// Appends values from a slice of type `T` and a validity boolean slice
    #[inline]
    pub fn append_values(&mut self, values: &[bool], is_valid: &[bool]) -> Result<()> {
        if values.len() != is_valid.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Value and validity lengths must be equal".to_string(),
            ));
        }
        self.bitmap_builder.append_slice(is_valid);
        self.values_builder.append_slice(values);
        Ok(())
    }

    /// Builds the [BooleanArray] and reset this builder.
    pub fn finish(&mut self) -> BooleanArray {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - null_bit_buffer.count_set_bits();
        let mut builder = ArrayData::builder(DataType::Boolean)
            .len(len)
            .add_buffer(self.values_builder.finish());
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer);
        }
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
        self.values_builder.len
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
        self.values_builder.len
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
    pub fn append_values(&mut self, values: &[T::Native], is_valid: &[bool]) -> Result<()> {
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
        let mut builder = ArrayData::builder(T::DATA_TYPE)
            .len(len)
            .add_buffer(self.values_builder.finish());
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer.unwrap());
        }
        let array_data = unsafe { builder.build_unchecked() };
        PrimitiveArray::<T>::from(array_data)
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish_dict(&mut self, values: ArrayRef) -> DictionaryArray<T> {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.as_mut().map(|b| b.finish());
        let null_count = len
            - null_bit_buffer
                .as_ref()
                .map(|b| b.count_set_bits())
                .unwrap_or(len);
        let data_type =
            DataType::Dictionary(Box::new(T::DATA_TYPE), Box::new(values.data_type().clone()));
        let mut builder = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(self.values_builder.finish());
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer.unwrap());
        }
        builder = builder.add_child_data(values.data().clone());
        let array_data = unsafe { builder.build_unchecked() };
        DictionaryArray::<T>::from(array_data)
    }

    fn materialize_bitmap_builder(&mut self) {
        if self.bitmap_builder.is_some() {
            return;
        }
        let mut b = BooleanBufferBuilder::new(0);
        b.reserve(self.values_builder.capacity());
        b.append_n(self.values_builder.len, true);
        self.bitmap_builder = Some(b);
    }
}

///  Array builder for `ListArray`
#[derive(Debug)]
pub struct GenericListBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    offsets_builder: BufferBuilder<OffsetSize>,
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: OffsetSize,
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T> {
    /// Creates a new `ListArrayBuilder` from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(capacity + 1);
        let len = OffsetSize::zero();
        offsets_builder.append(len);
        Self {
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> ArrayBuilder
    for GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
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
        self.len.to_usize().unwrap()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == OffsetSize::zero()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.offsets_builder
            .append(OffsetSize::from_usize(self.values_builder.len()).unwrap());
        self.bitmap_builder.append(is_valid);
        self.len += OffsetSize::one();
        Ok(())
    }

    /// Builds the `ListArray` and reset this builder.
    pub fn finish(&mut self) -> GenericListArray<OffsetSize> {
        let len = self.len();
        self.len = OffsetSize::zero();
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.bitmap_builder.finish();
        self.offsets_builder.append(self.len);
        let field = Box::new(Field::new(
            "item",
            values_data.data_type().clone(),
            true, // TODO: find a consistent way of getting this
        ));
        let data_type = if OffsetSize::is_large() {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        let array_data = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data.clone())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data.build_unchecked() };

        GenericListArray::<OffsetSize>::from(array_data)
    }
}

pub type ListBuilder<T> = GenericListBuilder<i32, T>;
pub type LargeListBuilder<T> = GenericListBuilder<i64, T>;

///  Array builder for `ListArray`
#[derive(Debug)]
pub struct FixedSizeListBuilder<T: ArrayBuilder> {
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: usize,
    list_len: i32,
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T> {
    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    pub fn new(values_builder: T, length: i32) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, length, capacity)
    }

    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, length: i32, capacity: usize) -> Self {
        let mut offsets_builder = Int32BufferBuilder::new(capacity + 1);
        offsets_builder.append(0);
        Self {
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len: 0,
            list_len: length,
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilder for FixedSizeListBuilder<T>
where
    T: 'static,
{
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
        self.len
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    pub fn value_length(&self) -> i32 {
        self.list_len
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid);
        self.len += 1;
        Ok(())
    }

    /// Builds the `FixedSizeListBuilder` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeListArray {
        let len = self.len();
        self.len = 0;
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        // check that values_data length is multiple of len if we have data
        if len != 0 {
            assert!(
                values_data.len() / len == self.list_len as usize,
                "Values of FixedSizeList must have equal lengths, values have length {} and list has {}",
                values_data.len() / len,
                self.list_len
            );
        }

        let null_bit_buffer = self.bitmap_builder.finish();
        let array_data = ArrayData::builder(DataType::FixedSizeList(
            Box::new(Field::new("item", values_data.data_type().clone(), true)),
            self.list_len,
        ))
        .len(len)
        .add_child_data(values_data.clone())
        .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data.build_unchecked() };

        FixedSizeListArray::from(array_data)
    }
}

///  Array builder for `BinaryArray`
#[derive(Debug)]
pub struct GenericBinaryBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericListBuilder<OffsetSize, UInt8Builder>,
}

pub type BinaryBuilder = GenericBinaryBuilder<i32>;
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;

#[derive(Debug)]
pub struct GenericStringBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericListBuilder<OffsetSize, UInt8Builder>,
}

pub type StringBuilder = GenericStringBuilder<i32>;
pub type LargeStringBuilder = GenericStringBuilder<i64>;

#[derive(Debug)]
pub struct FixedSizeBinaryBuilder {
    builder: FixedSizeListBuilder<UInt8Builder>,
}

///
/// Array Builder for [`DecimalArray`]
///
/// See [`DecimalArray`] for example.
///
#[derive(Debug)]
pub struct DecimalBuilder {
    builder: FixedSizeListBuilder<UInt8Builder>,
    precision: usize,
    scale: usize,
}

impl<OffsetSize: BinaryOffsetSizeTrait> ArrayBuilder for GenericBinaryBuilder<OffsetSize> {
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

impl<OffsetSize: StringOffsetSizeTrait> ArrayBuilder for GenericStringBuilder<OffsetSize> {
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
        let a = GenericStringBuilder::<OffsetSize>::finish(self);
        Arc::new(a)
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

impl ArrayBuilder for DecimalBuilder {
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

impl<OffsetSize: BinaryOffsetSizeTrait> GenericBinaryBuilder<OffsetSize> {
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
    pub fn append_byte(&mut self, value: u8) -> Result<()> {
        self.builder.values().append_value(value)?;
        Ok(())
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<()> {
        self.builder.values().append_slice(value.as_ref())?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `BinaryArray` and reset this builder.
    pub fn finish(&mut self) -> GenericBinaryArray<OffsetSize> {
        GenericBinaryArray::<OffsetSize>::from(self.builder.finish())
    }
}

impl<OffsetSize: StringOffsetSizeTrait> GenericStringBuilder<OffsetSize> {
    /// Creates a new `StringBuilder`,
    /// `capacity` is the number of bytes of string data to pre-allocate space for in this builder
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: GenericListBuilder::new(values_builder),
        }
    }

    /// Creates a new `StringBuilder`,
    /// `data_capacity` is the number of bytes of string data to pre-allocate space for in this builder
    /// `item_capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(data_capacity);
        Self {
            builder: GenericListBuilder::with_capacity(values_builder, item_capacity),
        }
    }

    /// Appends a string into the builder.
    ///
    /// Automatically calls the `append` method to delimit the string appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<str>) -> Result<()> {
        self.builder
            .values()
            .append_slice(value.as_ref().as_bytes())?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Append an `Option` value to the array.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<str>>) -> Result<()> {
        match value {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Builds the `StringArray` and reset this builder.
    pub fn finish(&mut self) -> GenericStringArray<OffsetSize> {
        GenericStringArray::<OffsetSize>::from(self.builder.finish())
    }
}

impl FixedSizeBinaryBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, byte_width: i32) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<()> {
        if self.builder.value_length() != value.as_ref().len() as i32 {
            return Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths"
                    .to_string(),
            ));
        }
        self.builder.values().append_slice(value.as_ref())?;
        self.builder.append(true)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `FixedSizeBinaryArray` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::from(self.builder.finish())
    }
}

impl DecimalBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, precision: usize, scale: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        let byte_width = 16;
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
            precision,
            scale,
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: i128) -> Result<()> {
        let value = validate_decimal_precision(value, self.precision)?;
        let value_as_bytes =
            Self::from_i128_to_fixed_size_bytes(value, self.builder.value_length() as usize)?;
        if self.builder.value_length() != value_as_bytes.len() as i32 {
            return Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as DecimalBuilder value lengths"
                    .to_string(),
            ));
        }
        self.builder
            .values()
            .append_slice(value_as_bytes.as_slice())?;
        self.builder.append(true)
    }

    fn from_i128_to_fixed_size_bytes(v: i128, size: usize) -> Result<Vec<u8>> {
        if size > 16 {
            return Err(ArrowError::InvalidArgumentError(
                "DecimalBuilder only supports values up to 16 bytes.".to_string(),
            ));
        }
        let res = v.to_le_bytes();
        let start_byte = 16 - size;
        Ok(res[start_byte..16].to_vec())
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `DecimalArray` and reset this builder.
    pub fn finish(&mut self) -> DecimalArray {
        DecimalArray::from_fixed_size_list_array(self.builder.finish(), self.precision, self.scale)
    }
}

/// Array builder for Struct types.
///
/// Note that callers should make sure that methods of all the child field builders are
/// properly called to maintain the consistency of the data structure.
pub struct StructBuilder {
    fields: Vec<Field>,
    field_builders: Vec<Box<dyn ArrayBuilder>>,
    bitmap_builder: BooleanBufferBuilder,
    len: usize,
}

impl fmt::Debug for StructBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StructBuilder")
            .field("fields", &self.fields)
            .field("bitmap_builder", &self.bitmap_builder)
            .field("len", &self.len)
            .finish()
    }
}

impl ArrayBuilder for StructBuilder {
    /// Returns the number of array slots in the builder.
    ///
    /// Note that this always return the first child field builder's length, and it is
    /// the caller's responsibility to maintain the consistency that all the child field
    /// builder should have the equal number of elements.
    fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Builds the array.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Returns a builder with capacity `capacity` that corresponds to the datatype `DataType`
/// This function is useful to construct arrays from an arbitrary vectors with known/expected
/// schema.
pub fn make_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match datatype {
        DataType::Null => unimplemented!(),
        DataType::Boolean => Box::new(BooleanBuilder::new(capacity)),
        DataType::Int8 => Box::new(Int8Builder::new(capacity)),
        DataType::Int16 => Box::new(Int16Builder::new(capacity)),
        DataType::Int32 => Box::new(Int32Builder::new(capacity)),
        DataType::Int64 => Box::new(Int64Builder::new(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::new(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::new(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::new(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::new(capacity)),
        DataType::Float32 => Box::new(Float32Builder::new(capacity)),
        DataType::Float64 => Box::new(Float64Builder::new(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::new(capacity)),
        DataType::FixedSizeBinary(len) => Box::new(FixedSizeBinaryBuilder::new(capacity, *len)),
        DataType::Decimal(precision, scale) => {
            Box::new(DecimalBuilder::new(capacity, *precision, *scale))
        }
        DataType::Utf8 => Box::new(StringBuilder::new(capacity)),
        DataType::Date32 => Box::new(Date32Builder::new(capacity)),
        DataType::Date64 => Box::new(Date64Builder::new(capacity)),
        DataType::Time32(TimeUnit::Second) => Box::new(Time32SecondBuilder::new(capacity)),
        DataType::Time32(TimeUnit::Millisecond) => {
            Box::new(Time32MillisecondBuilder::new(capacity))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Box::new(Time64MicrosecondBuilder::new(capacity))
        }
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new(capacity)),
        DataType::Timestamp(TimeUnit::Second, _) => Box::new(TimestampSecondBuilder::new(capacity)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new(capacity))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new(capacity))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::new(capacity))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(IntervalYearMonthBuilder::new(capacity))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(IntervalDayTimeBuilder::new(capacity))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Box::new(IntervalMonthDayNanoBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Second) => Box::new(DurationSecondBuilder::new(capacity)),
        DataType::Duration(TimeUnit::Millisecond) => {
            Box::new(DurationMillisecondBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Box::new(DurationMicrosecondBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Box::new(DurationNanosecondBuilder::new(capacity))
        }
        DataType::Struct(fields) => Box::new(StructBuilder::from_fields(fields.clone(), capacity)),
        t => panic!("Data type {:?} is not currently supported", t),
    }
}

impl StructBuilder {
    pub fn new(fields: Vec<Field>, field_builders: Vec<Box<dyn ArrayBuilder>>) -> Self {
        Self {
            fields,
            field_builders,
            bitmap_builder: BooleanBufferBuilder::new(0),
            len: 0,
        }
    }

    pub fn from_fields(fields: Vec<Field>, capacity: usize) -> Self {
        let mut builders = Vec::with_capacity(fields.len());
        for field in &fields {
            builders.push(make_builder(field.data_type(), capacity));
        }
        Self::new(fields, builders)
    }

    /// Returns a mutable reference to the child field builder at index `i`.
    /// Result will be `None` if the input type `T` provided doesn't match the actual
    /// field builder's type.
    pub fn field_builder<T: ArrayBuilder>(&mut self, i: usize) -> Option<&mut T> {
        self.field_builders[i].as_any_mut().downcast_mut::<T>()
    }

    /// Returns the number of fields for the struct this builder is building.
    pub fn num_fields(&self) -> usize {
        self.field_builders.len()
    }

    /// Appends an element (either null or non-null) to the struct. The actual elements
    /// should be appended for each child sub-array in a consistent way.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid);
        self.len += 1;
        Ok(())
    }

    /// Appends a null element to the struct.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `StructArray` and reset this builder.
    pub fn finish(&mut self) -> StructArray {
        let mut child_data = Vec::with_capacity(self.field_builders.len());
        for f in &mut self.field_builders {
            let arr = f.finish();
            child_data.push(arr.data().clone());
        }

        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = self.len - null_bit_buffer.count_set_bits();
        let mut builder = ArrayData::builder(DataType::Struct(self.fields.clone()))
            .len(self.len)
            .child_data(child_data);
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer);
        }

        self.len = 0;

        let array_data = unsafe { builder.build_unchecked() };
        StructArray::from(array_data)
    }
}

#[derive(Debug)]
pub struct MapBuilder<K: ArrayBuilder, V: ArrayBuilder> {
    offsets_builder: BufferBuilder<i32>,
    bitmap_builder: BooleanBufferBuilder,
    field_names: MapFieldNames,
    key_builder: K,
    value_builder: V,
    len: i32,
}

#[derive(Debug, Clone)]
pub struct MapFieldNames {
    pub entry: String,
    pub key: String,
    pub value: String,
}

impl Default for MapFieldNames {
    fn default() -> Self {
        Self {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        }
    }
}

#[allow(dead_code)]
impl<K: ArrayBuilder, V: ArrayBuilder> MapBuilder<K, V> {
    pub fn new(field_names: Option<MapFieldNames>, key_builder: K, value_builder: V) -> Self {
        let capacity = key_builder.len();
        Self::with_capacity(field_names, key_builder, value_builder, capacity)
    }

    pub fn with_capacity(
        field_names: Option<MapFieldNames>,
        key_builder: K,
        value_builder: V,
        capacity: usize,
    ) -> Self {
        let mut offsets_builder = BufferBuilder::<i32>::new(capacity + 1);
        let len = 0;
        offsets_builder.append(len);
        Self {
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            field_names: field_names.unwrap_or_default(),
            key_builder,
            value_builder,
            len,
        }
    }

    pub fn keys(&mut self) -> &mut K {
        &mut self.key_builder
    }

    pub fn values(&mut self) -> &mut V {
        &mut self.value_builder
    }

    /// Finish the current map array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        if self.key_builder.len() != self.value_builder.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot append to a map builder when its keys and values have unequal lengths of {} and {}",
                self.key_builder.len(),
                self.value_builder.len()
            )));
        }
        self.offsets_builder.append(self.key_builder.len() as i32);
        self.bitmap_builder.append(is_valid);
        self.len += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> MapArray {
        let len = self.len();
        self.len = 0;

        // Build the keys
        let keys_arr = self
            .key_builder
            .as_any_mut()
            .downcast_mut::<K>()
            .unwrap()
            .finish();
        let values_arr = self
            .value_builder
            .as_any_mut()
            .downcast_mut::<V>()
            .unwrap()
            .finish();

        let keys_field = Field::new(
            self.field_names.key.as_str(),
            keys_arr.data_type().clone(),
            false, // always nullable
        );
        let values_field = Field::new(
            self.field_names.value.as_str(),
            values_arr.data_type().clone(),
            true,
        );

        let struct_array =
            StructArray::from(vec![(keys_field, keys_arr), (values_field, values_arr)]);

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.bitmap_builder.finish();
        self.offsets_builder.append(self.len);
        let map_field = Box::new(Field::new(
            self.field_names.entry.as_str(),
            struct_array.data_type().clone(),
            false, // always non-nullable
        ));
        let array_data = ArrayData::builder(DataType::Map(map_field, false)) // TODO: support sorted keys
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(struct_array.data().clone())
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data.build_unchecked() };

        MapArray::from(array_data)
    }
}

impl<K: ArrayBuilder, V: ArrayBuilder> ArrayBuilder for MapBuilder<K, V> {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// `FieldData` is a helper struct to track the state of the fields in the `UnionBuilder`.
#[derive(Debug)]
struct FieldData {
    /// The type id for this field
    type_id: i8,
    /// The Arrow data type represented in the `values_buffer`, which is untyped
    data_type: DataType,
    /// A buffer containing the values for this field in raw bytes
    values_buffer: Option<MutableBuffer>,
    ///  The number of array slots represented by the buffer
    slots: usize,
    /// A builder for the bitmap if required (for Sparse Unions)
    bitmap_builder: Option<BooleanBufferBuilder>,
}

impl FieldData {
    /// Creates a new `FieldData`.
    fn new(type_id: i8, data_type: DataType, bitmap_builder: Option<BooleanBufferBuilder>) -> Self {
        Self {
            type_id,
            data_type,
            values_buffer: Some(MutableBuffer::new(1)),
            slots: 0,
            bitmap_builder,
        }
    }

    /// Appends a single value to this `FieldData`'s `values_buffer`.
    #[allow(clippy::unnecessary_wraps)]
    fn append_to_values_buffer<T: ArrowPrimitiveType>(&mut self, v: T::Native) -> Result<()> {
        let values_buffer = self
            .values_buffer
            .take()
            .expect("Values buffer was never created");
        let mut builder: BufferBuilder<T::Native> =
            mutable_buffer_to_builder(values_buffer, self.slots);
        builder.append(v);
        let mutable_buffer = builder_to_mutable_buffer(builder);
        self.values_buffer = Some(mutable_buffer);

        self.slots += 1;
        if let Some(b) = &mut self.bitmap_builder {
            b.append(true)
        };
        Ok(())
    }

    /// Appends a null to this `FieldData`.
    #[allow(clippy::unnecessary_wraps)]
    fn append_null<T: ArrowPrimitiveType>(&mut self) -> Result<()> {
        if let Some(b) = &mut self.bitmap_builder {
            let values_buffer = self
                .values_buffer
                .take()
                .expect("Values buffer was never created");
            let mut builder: BufferBuilder<T::Native> =
                mutable_buffer_to_builder(values_buffer, self.slots);
            builder.advance(1);
            let mutable_buffer = builder_to_mutable_buffer(builder);
            self.values_buffer = Some(mutable_buffer);
            self.slots += 1;
            b.append(false);
        };
        Ok(())
    }

    /// Appends a null to this `FieldData` when the type is not known at compile time.
    ///
    /// As the main `append` method of `UnionBuilder` is generic, we need a way to append null
    /// slots to the fields that are not being appended to in the case of sparse unions.  This
    /// method solves this problem by appending dynamically based on `DataType`.
    ///
    /// Note, this method does **not** update the length of the `UnionArray` (this is done by the
    /// main append operation) and assumes that it is called from a method that is generic over `T`
    /// where `T` satisfies the bound `ArrowPrimitiveType`.
    fn append_null_dynamic(&mut self) -> Result<()> {
        match self.data_type {
            DataType::Null => unimplemented!(),
            DataType::Int8 => self.append_null::<Int8Type>()?,
            DataType::Int16 => self.append_null::<Int16Type>()?,
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => self.append_null::<Int32Type>()?,
            DataType::Int64
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Interval(IntervalUnit::DayTime)
            | DataType::Duration(_) => self.append_null::<Int64Type>()?,
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                self.append_null::<IntervalMonthDayNanoType>()?
            }
            DataType::UInt8 => self.append_null::<UInt8Type>()?,
            DataType::UInt16 => self.append_null::<UInt16Type>()?,
            DataType::UInt32 => self.append_null::<UInt32Type>()?,
            DataType::UInt64 => self.append_null::<UInt64Type>()?,
            DataType::Float32 => self.append_null::<Float32Type>()?,
            DataType::Float64 => self.append_null::<Float64Type>()?,
            _ => unreachable!(
                "All cases of types that satisfy the trait bounds over T are covered above."
            ),
        };
        Ok(())
    }
}

/// Builder type for creating a new `UnionArray`.
///
/// Example: **Dense Memory Layout**
///
/// ```
/// use arrow::array::UnionBuilder;
/// use arrow::datatypes::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_dense(3);
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0_i8);
/// assert_eq!(union.type_id(1), 1_i8);
/// assert_eq!(union.type_id(2), 0_i8);
///
/// assert_eq!(union.value_offset(0), 0_i32);
/// assert_eq!(union.value_offset(1), 0_i32);
/// assert_eq!(union.value_offset(2), 1_i32);
/// ```
///
/// Example: **Sparse Memory Layout**
/// ```
/// use arrow::array::UnionBuilder;
/// use arrow::datatypes::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_sparse(3);
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0_i8);
/// assert_eq!(union.type_id(1), 1_i8);
/// assert_eq!(union.type_id(2), 0_i8);
///
/// assert_eq!(union.value_offset(0), 0_i32);
/// assert_eq!(union.value_offset(1), 1_i32);
/// assert_eq!(union.value_offset(2), 2_i32);
/// ```
#[derive(Debug)]
pub struct UnionBuilder {
    /// The current number of slots in the array
    len: usize,
    /// Maps field names to `FieldData` instances which track the builders for that field
    fields: HashMap<String, FieldData>,
    /// Builder to keep track of type ids
    type_id_builder: Int8BufferBuilder,
    /// Builder to keep track of offsets (`None` for sparse unions)
    value_offset_builder: Option<Int32BufferBuilder>,
    /// Optional builder for null slots
    bitmap_builder: Option<BooleanBufferBuilder>,
}

impl UnionBuilder {
    /// Creates a new dense array builder.
    pub fn new_dense(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: Some(Int32BufferBuilder::new(capacity)),
            bitmap_builder: None,
        }
    }

    /// Creates a new sparse array builder.
    pub fn new_sparse(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: None,
            bitmap_builder: None,
        }
    }

    /// Appends a null to this builder.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        if self.bitmap_builder.is_none() {
            let mut builder = BooleanBufferBuilder::new(self.len + 1);
            for _ in 0..self.len {
                builder.append(true);
            }
            self.bitmap_builder = Some(builder)
        }
        self.bitmap_builder
            .as_mut()
            .expect("Cannot be None")
            .append(false);

        self.type_id_builder.append(i8::default());

        match &mut self.value_offset_builder {
            // Handle dense union
            Some(value_offset_builder) => value_offset_builder.append(i32::default()),
            // Handle sparse union
            None => {
                for (_, fd) in self.fields.iter_mut() {
                    fd.append_null_dynamic()?;
                }
            }
        };
        self.len += 1;
        Ok(())
    }

    /// Appends a value to this builder.
    #[inline]
    pub fn append<T: ArrowPrimitiveType>(&mut self, type_name: &str, v: T::Native) -> Result<()> {
        let type_name = type_name.to_string();

        let mut field_data = match self.fields.remove(&type_name) {
            Some(data) => data,
            None => match self.value_offset_builder {
                Some(_) => {
                    // For Dense Union, we don't build bitmap in individual field
                    FieldData::new(self.fields.len() as i8, T::DATA_TYPE, None)
                }
                None => {
                    let mut fd = FieldData::new(
                        self.fields.len() as i8,
                        T::DATA_TYPE,
                        Some(BooleanBufferBuilder::new(1)),
                    );
                    for _ in 0..self.len {
                        fd.append_null::<T>()?;
                    }
                    fd
                }
            },
        };
        self.type_id_builder.append(field_data.type_id);

        match &mut self.value_offset_builder {
            // Dense Union
            Some(offset_builder) => {
                offset_builder.append(field_data.slots as i32);
            }
            // Sparse Union
            None => {
                for (name, fd) in self.fields.iter_mut() {
                    if name != &type_name {
                        fd.append_null_dynamic()?;
                    }
                }
            }
        }
        field_data.append_to_values_buffer::<T>(v)?;
        self.fields.insert(type_name, field_data);

        // Update the bitmap builder if it exists
        if let Some(b) = &mut self.bitmap_builder {
            b.append(true);
        }
        self.len += 1;
        Ok(())
    }

    /// Builds this builder creating a new `UnionArray`.
    pub fn build(mut self) -> Result<UnionArray> {
        let type_id_buffer = self.type_id_builder.finish();
        let value_offsets_buffer = self.value_offset_builder.map(|mut b| b.finish());
        let mut children = Vec::new();
        for (
            name,
            FieldData {
                type_id,
                data_type,
                values_buffer,
                slots,
                bitmap_builder,
            },
        ) in self.fields.into_iter()
        {
            let buffer = values_buffer
                .expect("The `values_buffer` should only ever be None inside the `append` method.")
                .into();
            let arr_data_builder = ArrayDataBuilder::new(data_type.clone())
                .add_buffer(buffer)
                .len(slots);
            //                .build();
            let arr_data_ref = unsafe {
                match bitmap_builder {
                    Some(mut bb) => arr_data_builder
                        .null_bit_buffer(bb.finish())
                        .build_unchecked(),
                    None => arr_data_builder.build_unchecked(),
                }
            };
            let array_ref = make_array(arr_data_ref);
            children.push((type_id, (Field::new(&name, data_type, false), array_ref)))
        }

        children.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .expect("This will never be None as type ids are always i8 values.")
        });
        let children: Vec<_> = children.into_iter().map(|(_, b)| b).collect();
        let bitmap = self.bitmap_builder.map(|mut b| b.finish());

        UnionArray::try_new(type_id_buffer, value_offsets_buffer, children, bitmap)
    }
}

/// Array builder for `DictionaryArray`. For example to map a set of byte indices
/// to f32 values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
///
/// # Example:
///
/// ```
///  use arrow::array::{
///      Array, PrimitiveBuilder, PrimitiveDictionaryBuilder,
///      UInt8Array, UInt32Array,
///    };
///  use arrow::datatypes::{UInt8Type, UInt32Type};
///
///  let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
///  let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
///  let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
///  builder.append(12345678).unwrap();
///  builder.append_null().unwrap();
///  builder.append(22345678).unwrap();
///  let array = builder.finish();
///
///  assert_eq!(
///      array.keys(),
///      &UInt8Array::from(vec![Some(0), None, Some(1)])
///  );
///
///  // Values are polymorphic and so require a downcast.
///  let av = array.values();
///  let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
///  let avs: &[u32] = ava.values();
///
///  assert!(!array.is_null(0));
///  assert!(array.is_null(1));
///  assert!(!array.is_null(2));
///
///  assert_eq!(avs, &[12345678, 22345678]);
/// ```
#[derive(Debug)]
pub struct PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    keys_builder: PrimitiveBuilder<K>,
    values_builder: PrimitiveBuilder<V>,
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(keys_builder: PrimitiveBuilder<K>, values_builder: PrimitiveBuilder<V>) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }
}

impl<K, V> ArrayBuilder for PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Returns the builder as an non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.keys_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.keys_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    #[inline]
    pub fn append(&mut self, value: V::Native) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.to_byte_slice()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value)?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.to_byte_slice().into(), key);
            Ok(key)
        }
    }

    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let value_ref: ArrayRef = Arc::new(self.values_builder.finish());
        self.keys_builder.finish_dict(value_ref)
    }
}

/// Array builder for `DictionaryArray` that stores Strings. For example to map a set of byte indices
/// to String values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
///
/// ```
/// use arrow::{
///   array::{
///     Int8Array, StringArray,
///     PrimitiveBuilder, StringBuilder, StringDictionaryBuilder,
///   },
///   datatypes::Int8Type,
/// };
///
/// // Create a dictionary array indexed by bytes whose values are Strings.
/// // It can thus hold up to 256 distinct string values.
///
/// let key_builder = PrimitiveBuilder::<Int8Type>::new(100);
/// let value_builder = StringBuilder::new(100);
/// let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
///
/// // The builder builds the dictionary value by value
/// builder.append("abc").unwrap();
/// builder.append_null().unwrap();
/// builder.append("def").unwrap();
/// builder.append("def").unwrap();
/// builder.append("abc").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///   array.keys(),
///   &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
/// );
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(ava.value(0), "abc");
/// assert_eq!(ava.value(1), "def");
///
/// ```
#[derive(Debug)]
pub struct StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    keys_builder: PrimitiveBuilder<K>,
    values_builder: StringBuilder,
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Creates a new `StringDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(keys_builder: PrimitiveBuilder<K>, values_builder: StringBuilder) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }

    /// Creates a new `StringDictionaryBuilder` from a keys builder and a dictionary
    /// which is initialized with the given values.
    /// The indices of those dictionary values are used as keys.
    ///
    /// # Example
    ///
    /// ```
    /// use arrow::datatypes::Int16Type;
    /// use arrow::array::{StringArray, StringDictionaryBuilder, PrimitiveBuilder, Int16Array};
    /// use std::convert::TryFrom;
    ///
    /// let dictionary_values = StringArray::from(vec![None, Some("abc"), Some("def")]);
    ///
    /// let mut builder = StringDictionaryBuilder::new_with_dictionary(PrimitiveBuilder::<Int16Type>::new(3), &dictionary_values).unwrap();
    /// builder.append("def").unwrap();
    /// builder.append_null().unwrap();
    /// builder.append("abc").unwrap();
    ///
    /// let dictionary_array = builder.finish();
    ///
    /// let keys = dictionary_array.keys();
    ///
    /// assert_eq!(keys, &Int16Array::from(vec![Some(2), None, Some(1)]));
    /// ```
    pub fn new_with_dictionary(
        keys_builder: PrimitiveBuilder<K>,
        dictionary_values: &StringArray,
    ) -> Result<Self> {
        let dict_len = dictionary_values.len();
        let mut values_builder =
            StringBuilder::with_capacity(dict_len, dictionary_values.value_data().len());
        let mut map: HashMap<Box<[u8]>, K::Native> = HashMap::with_capacity(dict_len);
        for i in 0..dict_len {
            if dictionary_values.is_valid(i) {
                let value = dictionary_values.value(i);
                map.insert(
                    value.as_bytes().into(),
                    K::Native::from_usize(i).ok_or(ArrowError::DictionaryKeyOverflowError)?,
                );
                values_builder.append_value(value)?;
            } else {
                values_builder.append_null()?;
            }
        }
        Ok(Self {
            keys_builder,
            values_builder,
            map,
        })
    }
}

impl<K> ArrayBuilder for StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Returns the builder as an non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.keys_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.keys_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    pub fn append(&mut self, value: impl AsRef<str>) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.as_ref().as_bytes()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value.as_ref())?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.as_ref().as_bytes().into(), key);
            Ok(key)
        }
    }

    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let value_ref: ArrayRef = Arc::new(self.values_builder.finish());
        self.keys_builder.finish_dict(value_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::bitmap::Bitmap;

    #[test]
    fn test_builder_i32_empty() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(0, b.len());
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b = Int32BufferBuilder::new(0);
        b.append(123);
        let a = b.finish();
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b = Int32BufferBuilder::new(5);
        for i in 0..5 {
            b.append(i);
        }
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(20, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        for i in 0..20 {
            b.append(i);
        }
        assert_eq!(32, b.capacity());
        let a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_builder_finish() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(16, b.capacity());
        for i in 0..10 {
            b.append(i);
        }
        let mut a = b.finish();
        assert_eq!(40, a.len());
        assert_eq!(0, b.len());
        assert_eq!(0, b.capacity());

        // Try build another buffer after cleaning up.
        for i in 0..20 {
            b.append(i)
        }
        assert_eq!(32, b.capacity());
        a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b = UInt8BufferBuilder::new(2);
        assert_eq!(64, b.capacity());
        b.reserve(64);
        assert_eq!(64, b.capacity());
        b.reserve(65);
        assert_eq!(128, b.capacity());

        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        b.reserve(16);
        assert_eq!(16, b.capacity());
        b.reserve(17);
        assert_eq!(32, b.capacity());
    }

    #[test]
    fn test_append_slice() {
        let mut b = UInt8BufferBuilder::new(0);
        b.append_slice(b"Hello, ");
        b.append_slice(b"World!");
        let buffer = b.finish();
        assert_eq!(13, buffer.len());

        let mut b = Int32BufferBuilder::new(0);
        b.append_slice(&[32, 54]);
        let buffer = b.finish();
        assert_eq!(8, buffer.len());
    }

    #[test]
    fn test_append_values() -> Result<()> {
        let mut a = Int8Builder::new(0);
        a.append_value(1)?;
        a.append_null()?;
        a.append_value(-2)?;
        assert_eq!(a.len(), 3);

        // append values
        let values = &[1, 2, 3, 4];
        let is_valid = &[true, true, false, true];
        a.append_values(values, is_valid)?;

        assert_eq!(a.len(), 7);
        let array = a.finish();
        assert_eq!(array.value(0), 1);
        assert!(array.is_null(1));
        assert_eq!(array.value(2), -2);
        assert_eq!(array.value(3), 1);
        assert_eq!(array.value(4), 2);
        assert!(array.is_null(5));
        assert_eq!(array.value(6), 4);

        Ok(())
    }

    #[test]
    fn test_boolean_buffer_builder_write_bytes() {
        let mut b = BooleanBufferBuilder::new(4);
        b.append(false);
        b.append(true);
        b.append(false);
        b.append(true);
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());

        // Overallocate capacity
        let mut b = BooleanBufferBuilder::new(8);
        b.append_slice(&[false, true, false, true]);
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());
    }

    #[test]
    fn test_boolean_buffer_builder_unset_first_bit() {
        let mut buffer = BooleanBufferBuilder::new(4);
        buffer.append(true);
        buffer.append(true);
        buffer.append(false);
        buffer.append(true);
        buffer.set_bit(0, false);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.finish().as_slice(), &[0b1010_u8]);
    }

    #[test]
    fn test_boolean_buffer_builder_unset_last_bit() {
        let mut buffer = BooleanBufferBuilder::new(4);
        buffer.append(true);
        buffer.append(true);
        buffer.append(false);
        buffer.append(true);
        buffer.set_bit(3, false);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.finish().as_slice(), &[0b0011_u8]);
    }

    #[test]
    fn test_boolean_buffer_builder_unset_an_inner_bit() {
        let mut buffer = BooleanBufferBuilder::new(5);
        buffer.append(true);
        buffer.append(true);
        buffer.append(false);
        buffer.append(true);
        buffer.set_bit(1, false);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.finish().as_slice(), &[0b1001_u8]);
    }

    #[test]
    fn test_boolean_buffer_builder_unset_several_bits() {
        let mut buffer = BooleanBufferBuilder::new(5);
        buffer.append(true);
        buffer.append(true);
        buffer.append(true);
        buffer.append(false);
        buffer.append(true);
        buffer.set_bit(1, false);
        buffer.set_bit(2, false);
        assert_eq!(buffer.len(), 5);
        assert_eq!(buffer.finish().as_slice(), &[0b10001_u8]);
    }

    #[test]
    fn test_boolean_buffer_builder_unset_several_bits_bigger_than_one_byte() {
        let mut buffer = BooleanBufferBuilder::new(16);
        buffer.append_n(10, true);
        buffer.set_bit(0, false);
        buffer.set_bit(3, false);
        buffer.set_bit(9, false);
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.finish().as_slice(), &[0b11110110_u8, 0b01_u8]);
    }

    #[test]
    fn test_boolean_buffer_builder_flip_several_bits_bigger_than_one_byte() {
        let mut buffer = BooleanBufferBuilder::new(16);
        buffer.append_n(5, true);
        buffer.append_n(5, false);
        buffer.append_n(5, true);
        buffer.set_bit(0, false);
        buffer.set_bit(3, false);
        buffer.set_bit(9, false);
        buffer.set_bit(6, true);
        buffer.set_bit(14, true);
        buffer.set_bit(13, false);
        assert_eq!(buffer.len(), 15);
        assert_eq!(buffer.finish().as_slice(), &[0b01010110_u8, 0b1011100_u8]);
    }

    #[test]
    fn test_bool_buffer_builder_get_first_bit() {
        let mut buffer = BooleanBufferBuilder::new(16);
        buffer.append_n(8, true);
        buffer.append_n(8, false);
        assert!(buffer.get_bit(0));
    }

    #[test]
    fn test_bool_buffer_builder_get_first_bit_not_requires_mutability() {
        let buffer = {
            let mut buffer = BooleanBufferBuilder::new(16);
            buffer.append_n(8, true);
            buffer
        };

        assert!(buffer.get_bit(0));
    }

    #[test]
    fn test_bool_buffer_builder_get_last_bit() {
        let mut buffer = BooleanBufferBuilder::new(16);
        buffer.append_n(8, true);
        buffer.append_n(8, false);
        assert!(!buffer.get_bit(15));
    }

    #[test]
    fn test_bool_buffer_builder_get_an_inner_bit() {
        let mut buffer = BooleanBufferBuilder::new(16);
        buffer.append_n(4, false);
        buffer.append_n(8, true);
        buffer.append_n(4, false);
        assert!(buffer.get_bit(11));
    }

    #[test]
    fn test_bool_buffer_fuzz() {
        use rand::prelude::*;

        let mut buffer = BooleanBufferBuilder::new(12);
        let mut all_bools = vec![];
        let mut rng = rand::thread_rng();

        let src_len = 32;
        let (src, compacted_src) = {
            let src: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() & 1 == 0))
                .take(src_len)
                .collect();

            let mut compacted_src = BooleanBufferBuilder::new(src_len);
            compacted_src.append_slice(&src);
            (src, compacted_src.finish())
        };

        for _ in 0..100 {
            let a = rng.next_u32() as usize % src_len;
            let b = rng.next_u32() as usize % src_len;

            let start = a.min(b);
            let end = a.max(b);

            buffer.append_packed_range(start..end, compacted_src.as_slice());
            all_bools.extend_from_slice(&src[start..end]);
        }

        let mut compacted = BooleanBufferBuilder::new(all_bools.len());
        compacted.append_slice(&all_bools);

        assert_eq!(buffer.finish(), compacted.finish())
    }

    #[test]
    fn test_boolean_array_builder_append_slice() {
        let arr1 = BooleanArray::from(vec![Some(true), Some(false), None, None, Some(false)]);

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
    fn test_boolean_array_builder_resize() {
        let mut builder = BooleanBufferBuilder::new(20);
        builder.append_n(4, true);
        builder.append_n(7, false);
        builder.append_n(2, true);
        builder.resize(20);

        assert_eq!(builder.len, 20);
        assert_eq!(
            builder.buffer.as_slice(),
            &[0b00001111, 0b00011000, 0b00000000]
        );

        builder.resize(5);
        assert_eq!(builder.len, 5);
        assert_eq!(builder.buffer.as_slice(), &[0b00001111]);

        builder.append_n(4, true);
        assert_eq!(builder.len, 9);
        assert_eq!(builder.buffer.as_slice(), &[0b11101111, 0b00000001]);
    }

    #[test]
    fn test_boolean_builder_increases_buffer_len() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BooleanBufferBuilder::new(8);

        for i in 0..16 {
            if i == 3 || i == 6 || i == 9 {
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        let buf2 = builder.finish();

        assert_eq!(buf.len(), buf2.len());
        assert_eq!(buf.as_slice(), buf2.as_slice());
    }

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
    fn test_primitive_array_builder_bool() {
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

    #[test]
    fn test_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]), values);
        assert_eq!(
            Buffer::from_slice_ref(&[0, 3, 6, 8]),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_large_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = LargeListBuilder::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]), values);
        assert_eq!(
            Buffer::from_slice_ref(&[0i64, 3, 6, 8]),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_array_builder_nulls() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(3, list_array.value_offsets()[2]);
        assert_eq!(3, list_array.value_length(2));
    }

    #[test]
    fn test_large_list_array_builder_nulls() {
        let values_builder = Int32Builder::new(10);
        let mut builder = LargeListBuilder::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(3, list_array.value_offsets()[2]);
        assert_eq!(3, list_array.value_length(2));
    }

    #[test]
    fn test_fixed_size_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7, null]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.values().append_null().unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    fn test_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_slice(&[1, 2, 3]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_slice(&[4, 5, 6]).unwrap();
        builder.append(true).unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.values().append_slice(&[7, 8, 9]).unwrap();
        builder.append(true).unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_fixed_size_list_array_builder_empty() {
        let values_builder = Int32Array::builder(5);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        let arr = builder.finish();
        assert_eq!(0, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_fixed_size_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        builder.values().append_slice(&[1, 2, 3]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_slice(&[4, 5, 6]).unwrap();
        builder.append(true).unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.values().append_slice(&[7, 8, 9]).unwrap();
        builder.append(true).unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_list_list_array_builder() {
        let primitive_builder = Int32Builder::new(10);
        let values_builder = ListBuilder::new(primitive_builder);
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder.values().values().append_value(1).unwrap();
        builder.values().values().append_value(2).unwrap();
        builder.values().append(true).unwrap();
        builder.values().values().append_value(3).unwrap();
        builder.values().values().append_value(4).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.values().values().append_value(5).unwrap();
        builder.values().values().append_value(6).unwrap();
        builder.values().values().append_value(7).unwrap();
        builder.values().append(true).unwrap();
        builder.values().append(false).unwrap();
        builder.values().values().append_value(8).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.values().values().append_value(9).unwrap();
        builder.values().values().append_value(10).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        let list_array = builder.finish();

        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 5, 5, 6]),
            list_array.data().buffers()[0].clone()
        );

        assert_eq!(6, list_array.values().data().len());
        assert_eq!(1, list_array.values().data().null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 4, 7, 7, 8, 10]),
            list_array.values().data().buffers()[0].clone()
        );

        assert_eq!(10, list_array.values().data().child_data()[0].len());
        assert_eq!(0, list_array.values().data().child_data()[0].null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            list_array.values().data().child_data()[0].buffers()[0].clone()
        );
    }

    #[test]
    fn test_binary_array_builder() {
        let mut builder = BinaryBuilder::new(20);

        builder.append_byte(b'h').unwrap();
        builder.append_byte(b'e').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_byte(b'w').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append_byte(b'r').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'd').unwrap();
        builder.append(true).unwrap();

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

        builder.append_byte(b'h').unwrap();
        builder.append_byte(b'e').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_byte(b'w').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append_byte(b'r').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'd').unwrap();
        builder.append(true).unwrap();

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
    fn test_string_array_builder() {
        let mut builder = StringBuilder::new(20);

        builder.append_value("hello").unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let string_array = builder.finish();

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offsets()[2]);
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_fixed_size_binary_builder() {
        let mut builder = FixedSizeBinaryBuilder::new(15, 5);

        //  [b"hello", null, "arrow"]
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"arrow").unwrap();
        let fixed_size_binary_array: FixedSizeBinaryArray = builder.finish();

        assert_eq!(
            &DataType::FixedSizeBinary(5),
            fixed_size_binary_array.data_type()
        );
        assert_eq!(3, fixed_size_binary_array.len());
        assert_eq!(1, fixed_size_binary_array.null_count());
        assert_eq!(10, fixed_size_binary_array.value_offset(2));
        assert_eq!(5, fixed_size_binary_array.value_length());
    }

    #[test]
    fn test_decimal_builder() {
        let mut builder = DecimalBuilder::new(30, 23, 6);

        builder.append_value(8_887_000_000).unwrap();
        builder.append_null().unwrap();
        builder.append_value(-8_887_000_000).unwrap();
        let decimal_array: DecimalArray = builder.finish();

        assert_eq!(&DataType::Decimal(23, 6), decimal_array.data_type());
        assert_eq!(3, decimal_array.len());
        assert_eq!(1, decimal_array.null_count());
        assert_eq!(32, decimal_array.value_offset(2));
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    fn test_string_array_builder_finish() {
        let mut builder = StringBuilder::new(10);

        builder.append_value("hello").unwrap();
        builder.append_value("world").unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.append_value("arrow").unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_string_array_builder_append_string() {
        let mut builder = StringBuilder::new(20);

        let var = "hello".to_owned();
        builder.append_value(&var).unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let string_array = builder.finish();

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offsets()[2]);
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_string_array_builder_append_option() {
        let mut builder = StringBuilder::new(20);
        builder.append_option(Some("hello")).unwrap();
        builder.append_option(None::<&str>).unwrap();
        builder.append_option(None::<String>).unwrap();
        builder.append_option(Some("world")).unwrap();

        let string_array = builder.finish();

        assert_eq!(4, string_array.len());
        assert_eq!("hello", string_array.value(0));
        assert!(string_array.is_null(1));
        assert!(string_array.is_null(2));
        assert_eq!("world", string_array.value(3));
    }

    #[test]
    fn test_struct_array_builder() {
        let string_builder = StringBuilder::new(4);
        let int_builder = Int32Builder::new(4);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Utf8, false));
        field_builders.push(Box::new(string_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert_eq!(2, builder.num_fields());

        let string_builder = builder
            .field_builder::<StringBuilder>(0)
            .expect("builder at field 0 should be string builder");
        string_builder.append_value("joe").unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_value("mark").unwrap();

        let int_builder = builder
            .field_builder::<Int32Builder>(1)
            .expect("builder at field 1 should be int builder");
        int_builder.append_value(1).unwrap();
        int_builder.append_value(2).unwrap();
        int_builder.append_null().unwrap();
        int_builder.append_value(4).unwrap();

        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_null().unwrap();
        builder.append(true).unwrap();

        let arr = builder.finish();

        let struct_data = arr.data();
        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(
            &Some(Bitmap::from(Buffer::from(&[11_u8]))),
            struct_data.null_bitmap()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Buffer::from(&[9_u8]))
            .add_buffer(Buffer::from_slice_ref(&[0, 3, 3, 3, 7]))
            .add_buffer(Buffer::from_slice_ref(b"joemark"))
            .build()
            .unwrap();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Buffer::from_slice_ref(&[11_u8]))
            .add_buffer(Buffer::from_slice_ref(&[1, 2, 0, 4]))
            .build()
            .unwrap();

        assert_eq!(expected_string_data, *arr.column(0).data());
        assert_eq!(expected_int_data, *arr.column(1).data());
    }

    #[test]
    fn test_struct_array_builder_finish() {
        let int_builder = Int32Builder::new(10);
        let bool_builder = BooleanBuilder::new(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));
        field_builders.push(Box::new(bool_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[
                false, true, false, true, false, true, false, true, false, true,
            ])
            .unwrap();

        // Append slot values - all are valid.
        for _ in 0..10 {
            assert!(builder.append(true).is_ok())
        }

        assert_eq!(10, builder.len());

        let arr = builder.finish();

        assert_eq!(10, arr.len());
        assert_eq!(0, builder.len());

        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[1, 3, 5, 7, 9])
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[false, true, false, true, false])
            .unwrap();

        // Append slot values - all are valid.
        for _ in 0..5 {
            assert!(builder.append(true).is_ok())
        }

        assert_eq!(5, builder.len());

        let arr = builder.finish();

        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_map_array_builder() {
        let string_builder = StringBuilder::new(4);
        let int_builder = Int32Builder::new(4);

        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        let string_builder = builder.keys();
        string_builder.append_value("joe").unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_value("mark").unwrap();

        let int_builder = builder.values();
        int_builder.append_value(1).unwrap();
        int_builder.append_value(2).unwrap();
        int_builder.append_null().unwrap();
        int_builder.append_value(4).unwrap();

        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.append(true).unwrap();

        let arr = builder.finish();

        let map_data = arr.data();
        assert_eq!(3, map_data.len());
        assert_eq!(1, map_data.null_count());
        assert_eq!(
            &Some(Bitmap::from(Buffer::from(&[5_u8]))),
            map_data.null_bitmap()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Buffer::from(&[9_u8]))
            .add_buffer(Buffer::from_slice_ref(&[0, 3, 3, 3, 7]))
            .add_buffer(Buffer::from_slice_ref(b"joemark"))
            .build()
            .unwrap();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Buffer::from_slice_ref(&[11_u8]))
            .add_buffer(Buffer::from_slice_ref(&[1, 2, 0, 4]))
            .build()
            .unwrap();

        assert_eq!(&expected_string_data, arr.keys().data());
        assert_eq!(&expected_int_data, arr.values().data());
    }

    // TODO: add a test that finishes building, after designing a spec-compliant
    // way of inserting values to the map.
    // A map's values shouldn't be repeated within a slot

    #[test]
    fn test_struct_array_builder_from_schema() {
        let mut fields = vec![
            Field::new("f1", DataType::Float32, false),
            Field::new("f2", DataType::Utf8, false),
        ];
        let sub_fields = vec![
            Field::new("g1", DataType::Int32, false),
            Field::new("g2", DataType::Boolean, false),
        ];
        let struct_type = DataType::Struct(sub_fields);
        fields.push(Field::new("f3", struct_type, false));

        let mut builder = StructBuilder::from_fields(fields, 5);
        assert_eq!(3, builder.num_fields());
        assert!(builder.field_builder::<Float32Builder>(0).is_some());
        assert!(builder.field_builder::<StringBuilder>(1).is_some());
        assert!(builder.field_builder::<StructBuilder>(2).is_some());
    }

    #[test]
    #[should_panic(
        expected = "Data type List(Field { name: \"item\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None }) is not currently supported"
    )]
    fn test_struct_array_builder_from_schema_unsupported_type() {
        let mut fields = vec![Field::new("f1", DataType::Int16, false)];
        let list_type = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
        fields.push(Field::new("f2", list_type, false));

        let _ = StructBuilder::from_fields(fields, 5);
    }

    #[test]
    fn test_struct_array_builder_field_builder_type_mismatch() {
        let int_builder = Int32Builder::new(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert!(builder.field_builder::<BinaryBuilder>(0).is_none());
    }

    #[test]
    fn test_primitive_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &UInt8Array::from(vec![Some(0), None, Some(1)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
        let avs: &[u32] = ava.values();

        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));

        assert_eq!(avs, &[12345678, 22345678]);
    }

    #[test]
    fn test_string_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<Int8Type>::new(5);
        let value_builder = StringBuilder::new(2);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ava.value(0), "abc");
        assert_eq!(ava.value(1), "def");
    }

    #[test]
    fn test_string_dictionary_builder_with_existing_dictionary() {
        let dictionary = StringArray::from(vec![None, Some("def"), Some("abc")]);

        let key_builder = PrimitiveBuilder::<Int8Type>::new(6);
        let mut builder =
            StringDictionaryBuilder::new_with_dictionary(key_builder, &dictionary).unwrap();
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        builder.append("ghi").unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(2), None, Some(1), Some(1), Some(2), Some(3)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert!(!ava.is_valid(0));
        assert_eq!(ava.value(1), "def");
        assert_eq!(ava.value(2), "abc");
        assert_eq!(ava.value(3), "ghi");
    }

    #[test]
    fn test_string_dictionary_builder_with_reserved_null_value() {
        let dictionary: Vec<Option<&str>> = vec![None];
        let dictionary = StringArray::from(dictionary);

        let key_builder = PrimitiveBuilder::<Int16Type>::new(4);
        let mut builder =
            StringDictionaryBuilder::new_with_dictionary(key_builder, &dictionary).unwrap();
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();

        assert!(array.is_null(1));
        assert!(!array.is_valid(1));

        let keys = array.keys();

        assert_eq!(keys.value(0), 1);
        assert!(keys.is_null(1));
        // zero initialization is currently guaranteed by Buffer allocation and resizing
        assert_eq!(keys.value(1), 0);
        assert_eq!(keys.value(2), 2);
        assert_eq!(keys.value(3), 1);
    }

    #[test]
    #[should_panic(expected = "DictionaryKeyOverflowError")]
    fn test_primitive_dictionary_overflow() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(257);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(257);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        // 256 unique keys.
        for i in 0..256 {
            builder.append(i + 1000).unwrap();
        }
        // Special error if the key overflows (256th entry)
        builder.append(1257).unwrap();
    }
}
