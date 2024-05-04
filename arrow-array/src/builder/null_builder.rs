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

use crate::builder::ArrayBuilder;
use crate::{ArrayRef, NullArray};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use std::any::Any;
use std::sync::Arc;

/// Builder for [`NullArray`]
///
/// # Example
///
/// Create a `NullArray` from a `NullBuilder`
///
/// ```
///
/// # use arrow_array::{Array, NullArray, builder::NullBuilder};
///
/// let mut b = NullBuilder::new();
/// b.append_empty_value();
/// b.append_null();
/// b.append_nulls(3);
/// b.append_empty_values(3);
/// let arr = b.finish();
///
/// assert_eq!(8, arr.len());
/// assert_eq!(0, arr.null_count());
/// ```
#[derive(Debug)]
pub struct NullBuilder {
    len: usize,
}

impl Default for NullBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NullBuilder {
    /// Creates a new null builder
    pub fn new() -> Self {
        Self { len: 0 }
    }

    /// Creates a new null builder with space for `capacity` elements without re-allocating
    #[deprecated = "there is no actual notion of capacity in the NullBuilder, so emulating it makes little sense"]
    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    #[deprecated = "there is no actual notion of capacity in the NullBuilder, so emulating it makes little sense"]
    pub fn capacity(&self) -> usize {
        self.len
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.len += 1;
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.len += n;
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_empty_value(&mut self) {
        self.append_null();
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_empty_values(&mut self, n: usize) {
        self.append_nulls(n);
    }

    /// Builds the [NullArray] and reset this builder.
    pub fn finish(&mut self) -> NullArray {
        let len = self.len();
        let builder = ArrayData::new_null(&DataType::Null, len).into_builder();

        let array_data = unsafe { builder.build_unchecked() };
        NullArray::from(array_data)
    }

    /// Builds the [NullArray] without resetting the builder.
    pub fn finish_cloned(&self) -> NullArray {
        let len = self.len();
        let builder = ArrayData::new_null(&DataType::Null, len).into_builder();

        let array_data = unsafe { builder.build_unchecked() };
        NullArray::from(array_data)
    }
}

impl ArrayBuilder for NullBuilder {
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
    fn test_null_array_builder() {
        let mut builder = NullArray::builder(10);
        builder.append_null();
        builder.append_nulls(4);
        builder.append_empty_value();
        builder.append_empty_values(4);

        let arr = builder.finish();
        assert_eq!(10, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert!(arr.is_nullable());
    }

    #[test]
    fn test_null_array_builder_finish_cloned() {
        let mut builder = NullArray::builder(16);
        builder.append_null();
        builder.append_empty_value();
        builder.append_empty_values(3);
        let mut array = builder.finish_cloned();
        assert_eq!(5, array.len());

        builder.append_empty_values(5);
        array = builder.finish();
        assert_eq!(10, array.len());
    }
}
