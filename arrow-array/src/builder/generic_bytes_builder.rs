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
use crate::builder::{ArrayBuilder, BufferBuilder, UInt8BufferBuilder};
use crate::types::{ByteArrayType, GenericBinaryType, GenericStringType};
use crate::{ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::ArrayDataBuilder;
use std::any::Any;
use std::sync::Arc;

///  Array builder for [`GenericByteArray`]
pub struct GenericByteBuilder<T: ByteArrayType> {
    value_builder: UInt8BufferBuilder,
    offsets_builder: BufferBuilder<T::Offset>,
    null_buffer_builder: NullBufferBuilder,
}

impl<T: ByteArrayType> GenericByteBuilder<T> {
    /// Creates a new [`GenericByteBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(1024, 1024)
    }

    /// Creates a new [`GenericByteBuilder`].
    ///
    /// - `item_capacity` is the number of items to pre-allocate.
    ///   The size of the preallocated buffer of offsets is the number of items plus one.
    /// - `data_capacity` is the total number of bytes of data to pre-allocate
    ///   (for all items, not per item).
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<T::Offset>::new(item_capacity + 1);
        offsets_builder.append(T::Offset::from_usize(0).unwrap());
        Self {
            value_builder: UInt8BufferBuilder::new(data_capacity),
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(item_capacity),
        }
    }

    /// Appends a value into the builder.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<T::Native>) {
        self.value_builder.append_slice(value.as_ref().as_ref());
        self.null_buffer_builder.append(true);
        self.offsets_builder
            .append(T::Offset::from_usize(self.value_builder.len()).unwrap());
    }

    /// Append an `Option` value into the builder.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<T::Native>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Append a null value into the builder.
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append(false);
        self.offsets_builder
            .append(T::Offset::from_usize(self.value_builder.len()).unwrap());
    }

    /// Builds the [`GenericByteArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericByteArray<T> {
        let array_type = T::DATA_TYPE;
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(self.offsets_builder.finish())
            .add_buffer(self.value_builder.finish())
            .null_bit_buffer(self.null_buffer_builder.finish());

        self.offsets_builder
            .append(T::Offset::from_usize(0).unwrap());
        let array_data = unsafe { array_builder.build_unchecked() };
        GenericByteArray::from(array_data)
    }

    /// Builds the [`GenericByteArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> GenericByteArray<T> {
        let array_type = T::DATA_TYPE;
        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let value_buffer = Buffer::from_slice_ref(self.value_builder.as_slice());
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(offset_buffer)
            .add_buffer(value_buffer)
            .null_bit_buffer(
                self.null_buffer_builder
                    .as_slice()
                    .map(Buffer::from_slice_ref),
            );

        let array_data = unsafe { array_builder.build_unchecked() };
        GenericByteArray::from(array_data)
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[u8] {
        self.value_builder.as_slice()
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[T::Offset] {
        self.offsets_builder.as_slice()
    }
}

impl<T: ByteArrayType> std::fmt::Debug for GenericByteBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}Builder", T::Offset::PREFIX, T::PREFIX)?;
        f.debug_struct("")
            .field("value_builder", &self.value_builder)
            .field("offsets_builder", &self.offsets_builder)
            .field("null_buffer_builder", &self.null_buffer_builder)
            .finish()
    }
}

impl<T: ByteArrayType> Default for GenericByteBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ByteArrayType> ArrayBuilder for GenericByteBuilder<T> {
    /// Returns the number of binary slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Returns whether the number of binary slots is zero
    fn is_empty(&self) -> bool {
        self.null_buffer_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

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
}

///  Array builder for [`GenericStringArray`][crate::GenericStringArray]
pub type GenericStringBuilder<O> = GenericByteBuilder<GenericStringType<O>>;

///  Array builder for [`GenericBinaryArray`][crate::GenericBinaryArray]
pub type GenericBinaryBuilder<O> = GenericByteBuilder<GenericBinaryType<O>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, OffsetSizeTrait};
    use crate::GenericStringArray;

    fn _test_generic_binary_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");

        let array = builder.finish();

        assert_eq!(4, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(b"hello", array.value(0));
        assert_eq!([] as [u8; 0], array.value(1));
        assert!(array.is_null(2));
        assert_eq!(b"rust", array.value(3));
        assert_eq!(O::from_usize(5).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(4).unwrap(), array.value_length(3));
    }

    #[test]
    fn test_binary_builder() {
        _test_generic_binary_builder::<i32>()
    }

    #[test]
    fn test_large_binary_builder() {
        _test_generic_binary_builder::<i64>()
    }

    fn _test_generic_binary_builder_all_nulls<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();
        builder.append_null();
        builder.append_null();
        builder.append_null();
        assert_eq!(3, builder.len());
        assert!(!builder.is_empty());

        let array = builder.finish();
        assert_eq!(3, array.null_count());
        assert_eq!(3, array.len());
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert!(array.is_null(2));
    }

    #[test]
    fn test_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i32>()
    }

    #[test]
    fn test_large_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i64>()
    }

    fn _test_generic_binary_builder_reset<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");
        builder.finish();

        assert!(builder.is_empty());

        builder.append_value(b"parquet");
        builder.append_null();
        builder.append_value(b"arrow");
        builder.append_value(b"");
        let array = builder.finish();

        assert_eq!(4, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(b"parquet", array.value(0));
        assert!(array.is_null(1));
        assert_eq!(b"arrow", array.value(2));
        assert_eq!(b"", array.value(1));
        assert_eq!(O::zero(), array.value_offsets()[0]);
        assert_eq!(O::from_usize(7).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(5).unwrap(), array.value_length(2));
    }

    #[test]
    fn test_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i32>()
    }

    #[test]
    fn test_large_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i64>()
    }

    fn _test_generic_string_array_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::new();
        let owned = "arrow".to_owned();

        builder.append_value("hello");
        builder.append_value("");
        builder.append_value(&owned);
        builder.append_null();
        builder.append_option(Some("rust"));
        builder.append_option(None::<&str>);
        builder.append_option(None::<String>);
        assert_eq!(7, builder.len());

        assert_eq!(
            GenericStringArray::<O>::from(vec![
                Some("hello"),
                Some(""),
                Some("arrow"),
                None,
                Some("rust"),
                None,
                None
            ]),
            builder.finish()
        );
    }

    #[test]
    fn test_string_array_builder() {
        _test_generic_string_array_builder::<i32>()
    }

    #[test]
    fn test_large_string_array_builder() {
        _test_generic_string_array_builder::<i64>()
    }

    fn _test_generic_string_array_builder_finish<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::with_capacity(3, 11);

        builder.append_value("hello");
        builder.append_value("rust");
        builder.append_null();

        builder.finish();
        assert!(builder.is_empty());
        assert_eq!(&[O::zero()], builder.offsets_slice());

        builder.append_value("arrow");
        builder.append_value("parquet");
        let arr = builder.finish();
        // array should not have null buffer because there is not `null` value.
        assert_eq!(None, arr.data().null_buffer());
        assert_eq!(GenericStringArray::<O>::from(vec!["arrow", "parquet"]), arr,)
    }

    #[test]
    fn test_string_array_builder_finish() {
        _test_generic_string_array_builder_finish::<i32>()
    }

    #[test]
    fn test_large_string_array_builder_finish() {
        _test_generic_string_array_builder_finish::<i64>()
    }

    fn _test_generic_string_array_builder_finish_cloned<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::with_capacity(3, 11);

        builder.append_value("hello");
        builder.append_value("rust");
        builder.append_null();

        let mut arr = builder.finish_cloned();
        assert!(!builder.is_empty());
        assert_eq!(3, arr.len());

        builder.append_value("arrow");
        builder.append_value("parquet");
        arr = builder.finish();

        assert!(arr.data().null_buffer().is_some());
        assert_eq!(&[O::zero()], builder.offsets_slice());
        assert_eq!(5, arr.len());
    }

    #[test]
    fn test_string_array_builder_finish_cloned() {
        _test_generic_string_array_builder_finish_cloned::<i32>()
    }

    #[test]
    fn test_large_string_array_builder_finish_cloned() {
        _test_generic_string_array_builder_finish_cloned::<i64>()
    }
}
