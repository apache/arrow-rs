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
    ArrayBuilder, ArrayRef, FixedSizeBinaryArray, FixedSizeListBuilder, UInt8Builder,
};
use crate::error::{ArrowError, Result};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct FixedSizeBinaryBuilder {
    builder: FixedSizeListBuilder<UInt8Builder>,
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
                "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths".to_string()
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
