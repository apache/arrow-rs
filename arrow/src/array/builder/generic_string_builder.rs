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
    ArrayBuilder, ArrayRef,  GenericStringArray, OffsetSizeTrait
};
use std::any::Any;
use std::sync::Arc;

use super::GenericBinaryBuilder;

///  Array builder for [`GenericStringArray`]
#[derive(Debug)]
pub struct GenericStringBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericBinaryBuilder<OffsetSize>
}

impl<OffsetSize: OffsetSizeTrait> GenericStringBuilder<OffsetSize> {
    /// Creates a new [`GenericStringBuilder`],
    /// `capacity` is the number of bytes of string data to pre-allocate space for in this builder
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: GenericBinaryBuilder::new(capacity),
        }
    }

    /// Creates a new [`GenericStringBuilder`],
    /// `data_capacity` is the number of bytes of string data to pre-allocate space for in this builder
    /// `item_capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        Self {
            builder: GenericBinaryBuilder::with_capacity(item_capacity, data_capacity),
        }
    }

    /// Appends a string into the builder.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<str>) {
        self.builder
            .append_value(value.as_ref().as_bytes());
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_null()
    }

    /// Append an `Option` value to the array.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<str>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Builds the [`GenericStringArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericStringArray<OffsetSize> {
        GenericStringArray::<OffsetSize>::from(self.builder.finish())
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[u8] {
        self.builder.values_slice()
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.builder.offsets_slice()
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
    use crate::array::builder::StringBuilder;
    use crate::array::{Array, ArrayBuilder};

    #[test]
    fn test_string_array_builder() {
        let mut builder = StringBuilder::new(20);

        builder.append_value("hello");
        builder.append_value("");
        builder.append_value("world");

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
    fn test_string_array_builder_finish() {
        let mut builder = StringBuilder::new(10);

        builder.append_value("hello");
        builder.append_value("world");

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.append_value("arrow");
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_string_array_builder_append_string() {
        let mut builder = StringBuilder::new(20);

        let var = "hello".to_owned();
        builder.append_value(&var);
        builder.append_value("");
        builder.append_value("world");

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
        builder.append_option(Some("hello"));
        builder.append_option(None::<&str>);
        builder.append_option(None::<String>);
        builder.append_option(Some("world"));

        let string_array = builder.finish();

        assert_eq!(4, string_array.len());
        assert_eq!("hello", string_array.value(0));
        assert!(string_array.is_null(1));
        assert!(string_array.is_null(2));
        assert_eq!("world", string_array.value(3));
    }
}
