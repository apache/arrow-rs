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

use arrow_buffer::{bit_util, Buffer, MutableBuffer};
use arrow_data::bit_mask;
use std::ops::Range;

/// A builder for creating a boolean [`Buffer`]
#[derive(Debug)]
pub struct BooleanBufferBuilder {
    buffer: MutableBuffer,
    len: usize,
}

impl BooleanBufferBuilder {
    /// Creates a new `BooleanBufferBuilder`
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let byte_capacity = bit_util::ceil(capacity, 8);
        let buffer = MutableBuffer::new(byte_capacity);
        Self { buffer, len: 0 }
    }

    /// Creates a new `BooleanBufferBuilder` from [`MutableBuffer`] of `len`
    pub fn new_from_buffer(buffer: MutableBuffer, len: usize) -> Self {
        assert!(len <= buffer.len() * 8);
        Self { buffer, len }
    }

    /// Returns the length of the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Sets a bit in the buffer at `index`
    #[inline]
    pub fn set_bit(&mut self, index: usize, v: bool) {
        if v {
            bit_util::set_bit(self.buffer.as_mut(), index);
        } else {
            bit_util::unset_bit(self.buffer.as_mut(), index);
        }
    }

    /// Gets a bit in the buffer at `index`
    #[inline]
    pub fn get_bit(&self, index: usize) -> bool {
        bit_util::get_bit(self.buffer.as_slice(), index)
    }

    /// Returns true if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the capacity of the buffer
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    /// Advances the buffer by `additional` bits
    #[inline]
    pub fn advance(&mut self, additional: usize) {
        let new_len = self.len + additional;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        if new_len_bytes > self.buffer.len() {
            self.buffer.resize(new_len_bytes, 0);
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

    /// Appends a boolean `v` into the buffer
    #[inline]
    pub fn append(&mut self, v: bool) {
        self.advance(1);
        if v {
            unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), self.len - 1) };
        }
    }

    /// Appends n `additional` bits of value `v` into the buffer
    #[inline]
    pub fn append_n(&mut self, additional: usize, v: bool) {
        self.advance(additional);
        if additional > 0 && v {
            let offset = self.len() - additional;
            (0..additional).for_each(|i| unsafe {
                bit_util::set_bit_raw(self.buffer.as_mut_ptr(), offset + i)
            })
        }
    }

    /// Appends a slice of booleans into the buffer
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
        bit_mask::set_bits(
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

    /// Returns the packed bits
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self.buffer.as_slice_mut()
    }

    /// Creates a [`Buffer`]
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_boolean_array_builder_resize() {
        let mut builder = BooleanBufferBuilder::new(20);
        builder.append_n(4, true);
        builder.append_n(7, false);
        builder.append_n(2, true);
        builder.resize(20);

        assert_eq!(builder.len(), 20);
        assert_eq!(builder.as_slice(), &[0b00001111, 0b00011000, 0b00000000]);

        builder.resize(5);
        assert_eq!(builder.len(), 5);
        assert_eq!(builder.as_slice(), &[0b00001111]);

        builder.append_n(4, true);
        assert_eq!(builder.len(), 9);
        assert_eq!(builder.as_slice(), &[0b11101111, 0b00000001]);
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
}
