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

use crate::buffer::Buffer;

use super::BooleanBufferBuilder;

/// We only materialize the builder when we add `false`.
/// This optimization is **very** important for the performance.
#[derive(Debug)]
pub struct NullBufferBuilder {
    bitmap_builder: Option<BooleanBufferBuilder>,
    len: usize,
    capacity: usize,
}

impl NullBufferBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            bitmap_builder: None,
            len: 0,
            capacity,
        }
    }

    pub fn append_n_true(&mut self, n: usize) {
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append_n(n, true)
        }
        self.len += n;
    }

    #[inline]
    pub fn append_true(&mut self) {
        self.append_n_true(1);
    }

    pub fn append_n_false(&mut self, n: usize) {
        self.materialize_if_needed();
        self.bitmap_builder.as_mut().unwrap().append_n(n, false);
        self.len += n;
    }

    #[inline]
    pub fn append_false(&mut self) {
        self.append_n_false(1);
    }

    pub fn append(&mut self, v: bool) {
        if v {
            self.append_true()
        } else {
            self.append_false()
        }
    }

    pub fn append_slice(&mut self, slice: &[bool]) {
        if slice.iter().any(|v| !v) {
            self.materialize_if_needed()
        }
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append_slice(slice)
        }
        self.len += slice.len();
    }

    pub fn finish(&mut self) -> Option<Buffer> {
        let buf = self.bitmap_builder.as_mut().map(|b| b.finish());
        self.bitmap_builder = None;
        self.len = 0;
        buf
    }

    #[inline]
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
}

impl NullBufferBuilder {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_buffer_builder() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_false();
        builder.append_true();
        builder.append_n_false(2);
        builder.append_n_true(2);
        assert_eq!(6, builder.len());

        let buf = builder.finish().unwrap();
        assert_eq!(Buffer::from(&[0b110010_u8]), buf);
    }

    #[test]
    fn test_null_buffer_builder_all_nulls() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_false();
        builder.append_n_false(2);
        builder.append_slice(&[false, false, false]);
        assert_eq!(6, builder.len());

        let buf = builder.finish().unwrap();
        assert_eq!(Buffer::from(&[0b0_u8]), buf);
    }

    #[test]
    fn test_null_buffer_builder_no_null() {
        let mut builder = NullBufferBuilder::new(0);
        builder.append_true();
        builder.append_n_true(2);
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
        assert_eq!(Buffer::from(&[0b1011_u8]), buf);
    }
}
