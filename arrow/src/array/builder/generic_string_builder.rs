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
use crate::array::{ArrayBuilder, ArrayRef, GenericListBuilder, GenericStringArray, OffsetSizeTrait, UInt8Builder};
use crate::error::{Result};

#[derive(Debug)]
pub struct GenericStringBuilder<OffsetSize: OffsetSizeTrait> {
    builder: GenericListBuilder<OffsetSize, UInt8Builder>,
}

impl<OffsetSize: OffsetSizeTrait> GenericStringBuilder<OffsetSize> {
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
