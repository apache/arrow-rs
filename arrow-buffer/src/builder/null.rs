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

use crate::{BooleanBufferBuilder, MutableBuffer, NullBuffer};

/// Builder for creating the null bit buffer.
/// This builder only materializes the buffer when we append `false`.
/// If you only append `true`s to the builder, what you get will be
/// `None` when calling [`finish`](#method.finish).
/// This optimization is **very** important for the performance.
#[derive(Debug)]
pub struct NullBufferBuilder {
    bitmap_builder: Option<BooleanBufferBuilder>,
    /// Store the length of the buffer before materializing.
    len: usize,
    capacity: usize,
}

impl NullBufferBuilder {
    /// Creates a new empty builder.
    /// `capacity` is the number of bits in the null buffer.
    pub fn new(capacity: usize) -> Self {
        Self {
            bitmap_builder: None,
            len: 0,
            capacity,
        }
    }

    /// Creates a new builder with given length.
    pub fn new_with_len(len: usize) -> Self {
        Self {
            bitmap_builder: None,
            len,
            capacity: len,
        }
    }

    /// Creates a new builder from a `MutableBuffer`.
    pub fn new_from_buffer(buffer: MutableBuffer, len: usize) -> Self {
        let capacity = buffer.len() * 8;

        assert!(len <= capacity);

        let bitmap_builder = Some(BooleanBufferBuilder::new_from_buffer(buffer, len));
        Self {
            bitmap_builder,
            len,
            capacity,
        }
    }

    /// Appends `n` `true`s into the builder
    /// to indicate that these `n` items are not nulls.
    #[inline]
    pub fn append_n_non_nulls(&mut self, n: usize) {
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append_n(n, true)
        } else {
            self.len += n;
        }
    }

    /// Appends a `true` into the builder
    /// to indicate that this item is not null.
    #[inline]
    pub fn append_non_null(&mut self) {
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append(true)
        } else {
            self.len += 1;
        }
    }

    /// Appends `n` `false`s into the builder
    /// to indicate that these `n` items are nulls.
    #[inline]
    pub fn append_n_nulls(&mut self, n: usize) {
        self.materialize_if_needed();
        self.bitmap_builder.as_mut().unwrap().append_n(n, false);
    }

    /// Appends a `false` into the builder
    /// to indicate that this item is null.
    #[inline]
    pub fn append_null(&mut self) {
        self.materialize_if_needed();
        self.bitmap_builder.as_mut().unwrap().append(false);
    }

    /// Appends a boolean value into the builder.
    #[inline]
    pub fn append(&mut self, not_null: bool) {
        if not_null {
            self.append_non_null()
        } else {
            self.append_null()
        }
    }

    /// Appends a boolean slice into the builder
    /// to indicate the validations of these items.
    pub fn append_slice(&mut self, slice: &[bool]) {
        if slice.iter().any(|v| !v) {
            self.materialize_if_needed()
        }
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append_slice(slice)
        } else {
            self.len += slice.len();
        }
    }

    /// Builds the null buffer and resets the builder.
    /// Returns `None` if the builder only contains `true`s.
    pub fn finish(&mut self) -> Option<NullBuffer> {
        self.len = 0;
        Some(NullBuffer::new(self.bitmap_builder.take()?.finish()))
    }

    /// Builds the [NullBuffer] without resetting the builder.
    pub fn finish_cloned(&self) -> Option<NullBuffer> {
        let buffer = self.bitmap_builder.as_ref()?.finish_cloned();
        Some(NullBuffer::new(buffer))
    }

    /// Returns the inner bitmap builder as slice
    pub fn as_slice(&self) -> Option<&[u8]> {
        Some(self.bitmap_builder.as_ref()?.as_slice())
    }

    fn materialize_if_needed(&mut self) {
        if self.bitmap_builder.is_none() {
            self.materialize()
        }
    }

    #[cold]
    fn materialize(&mut self) {
        if self.bitmap_builder.is_none() {
            let mut b = BooleanBufferBuilder::new(self.len.max(self.capacity));
            b.append_n(self.len, true);
            self.bitmap_builder = Some(b);
        }
    }

    pub fn as_slice_mut(&mut self) -> Option<&mut [u8]> {
        self.bitmap_builder.as_mut().map(|b| b.as_slice_mut())
    }

    /// Return the allocated size of this builder, useful for memory accounting.
    pub fn allocated_size(&self) -> usize {
        self.bitmap_builder
            .as_ref()
            .map(|b| b.capacity())
            .unwrap_or(0)
    }
}

impl NullBufferBuilder {
    pub fn len(&self) -> usize {
        if let Some(b) = &self.bitmap_builder {
            b.len()
        } else {
            self.len
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_buffer_builder() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_null();
        builder.append_non_null();
        builder.append_n_nulls(2);
        builder.append_n_non_nulls(2);
        assert_eq!(6, builder.len());

        let buf = builder.finish().unwrap();
        assert_eq!(&[0b110010_u8], buf.validity());
    }

    #[test]
    fn test_null_buffer_builder_all_nulls() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_null();
        builder.append_n_nulls(2);
        builder.append_slice(&[false, false, false]);
        assert_eq!(6, builder.len());

        let buf = builder.finish().unwrap();
        assert_eq!(&[0b0_u8], buf.validity());
    }

    #[test]
    fn test_null_buffer_builder_no_null() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_non_null();
        builder.append_n_non_nulls(2);
        builder.append_slice(&[true, true, true]);
        assert_eq!(6, builder.len());

        let buf = builder.finish();
        assert!(buf.is_none());
    }

    #[test]
    fn test_null_buffer_builder_reset() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_slice(&[true, false, true]);
        builder.finish();
        assert!(builder.is_empty());

        builder.append_slice(&[true, true, true]);
        assert!(builder.finish().is_none());
        assert!(builder.is_empty());

        builder.append_slice(&[true, true, false, true]);

        let buf = builder.finish().unwrap();
        assert_eq!(&[0b1011_u8], buf.validity());
    }
}
