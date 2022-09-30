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

use crate::builder::{ArrayBuilder, GenericBinaryBuilder};
use crate::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use std::any::Any;
use std::sync::Arc;

///  Array builder for [`GenericStringArray`]
#[derive(Debug)]
pub struct GenericStringBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericBinaryBuilder<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> GenericStringBuilder<OffsetSize> {
    /// Creates a new [`GenericStringBuilder`].
    pub fn new() -> Self {
        Self {
            builder: GenericBinaryBuilder::new(),
        }
    }

    /// Creates a new [`GenericStringBuilder`].
    ///
    /// - `item_capacity` is the number of items to pre-allocate.
    ///   The size of the preallocated buffer of offsets is the number of items plus one.
    /// - `data_capacity` is the total number of bytes of string data to pre-allocate
    ///   (for all items, not per item).
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        Self {
            builder: GenericBinaryBuilder::with_capacity(item_capacity, data_capacity),
        }
    }

    /// Appends a string into the builder.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<str>) {
        self.builder.append_value(value.as_ref().as_bytes());
    }

    /// Append a null value into the builder.
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_null()
    }

    /// Append an `Option` value into the builder.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<str>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Builds the [`GenericStringArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericStringArray<OffsetSize> {
        let t = GenericStringArray::<OffsetSize>::DATA_TYPE;
        let v = self.builder.finish();
        let builder = v.into_data().into_builder().data_type(t);

        // SAFETY:
        // Data must be UTF-8 as only support writing `str`
        // Offsets must be valid as guaranteed by `GenericBinaryBuilder`
        let data = unsafe { builder.build_unchecked() };
        data.into()
    }

    /// Returns the current values buffer as a slice.
    pub fn values_slice(&self) -> &[u8] {
        self.builder.values_slice()
    }

    /// Returns the current offsets buffer as a slice.
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.builder.offsets_slice()
    }
}

impl<OffsetSize: OffsetSizeTrait> Default for GenericStringBuilder<OffsetSize> {
    fn default() -> Self {
        Self::new()
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayBuilder for GenericStringBuilder<OffsetSize> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, OffsetSizeTrait};
    use crate::builder::ArrayBuilder;

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
}
