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

//! Types for iterating over bitmasks in 64-bit chunks

use crate::{bit_util::read_u64, util::bit_util::ceil};
use std::fmt::Debug;

/// Iterates over an arbitrarily aligned byte buffer
///
/// Yields an iterator of aligned u64, along with the leading and trailing
/// u64 necessary to align the buffer to a 8-byte boundary
///
/// This is unlike [`BitChunkIterator`] which only exposes a trailing u64,
/// and consequently has to perform more work for each read
#[derive(Debug, Clone, Copy)]
pub struct UnalignedBitChunk<'a> {
    lead_padding: usize,
    trailing_padding: usize,

    prefix: Option<u64>,
    chunks: &'a [u64],
    suffix: Option<u64>,
}

impl<'a> UnalignedBitChunk<'a> {
    /// Create a from a byte array, and and an offset and length in bits
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        if len == 0 {
            return Self {
                lead_padding: 0,
                trailing_padding: 0,
                prefix: None,
                chunks: &[],
                suffix: None,
            };
        }

        let byte_offset = offset / 8;
        let offset_padding = offset % 8;

        let bytes_len = (len + offset_padding).div_ceil(8);
        let buffer = &buffer[byte_offset..byte_offset + bytes_len];

        let prefix_mask = compute_prefix_mask(offset_padding);

        // If less than 8 bytes, read into prefix
        if buffer.len() <= 8 {
            let (suffix_mask, trailing_padding) = compute_suffix_mask(len, offset_padding);
            let prefix = read_u64(buffer) & suffix_mask & prefix_mask;

            return Self {
                lead_padding: offset_padding,
                trailing_padding,
                prefix: Some(prefix),
                chunks: &[],
                suffix: None,
            };
        }

        // If less than 16 bytes, read into prefix and suffix
        if buffer.len() <= 16 {
            let (suffix_mask, trailing_padding) = compute_suffix_mask(len, offset_padding);
            let prefix = read_u64(&buffer[..8]) & prefix_mask;
            let suffix = read_u64(&buffer[8..]) & suffix_mask;

            return Self {
                lead_padding: offset_padding,
                trailing_padding,
                prefix: Some(prefix),
                chunks: &[],
                suffix: Some(suffix),
            };
        }

        // Read into prefix and suffix as needed
        let (prefix, mut chunks, suffix) = unsafe { buffer.align_to::<u64>() };
        assert!(
            prefix.len() < 8 && suffix.len() < 8,
            "align_to did not return largest possible aligned slice"
        );

        let (alignment_padding, prefix) = match (offset_padding, prefix.is_empty()) {
            (0, true) => (0, None),
            (_, true) => {
                let prefix = chunks[0] & prefix_mask;
                chunks = &chunks[1..];
                (0, Some(prefix))
            }
            (_, false) => {
                let alignment_padding = (8 - prefix.len()) * 8;

                let prefix = (read_u64(prefix) & prefix_mask) << alignment_padding;
                (alignment_padding, Some(prefix))
            }
        };

        let lead_padding = offset_padding + alignment_padding;
        let (suffix_mask, trailing_padding) = compute_suffix_mask(len, lead_padding);

        let suffix = match (trailing_padding, suffix.is_empty()) {
            (0, _) => None,
            (_, true) => {
                let suffix = chunks[chunks.len() - 1] & suffix_mask;
                chunks = &chunks[..chunks.len() - 1];
                Some(suffix)
            }
            (_, false) => Some(read_u64(suffix) & suffix_mask),
        };

        Self {
            lead_padding,
            trailing_padding,
            prefix,
            chunks,
            suffix,
        }
    }

    /// Returns the number of leading padding bits
    pub fn lead_padding(&self) -> usize {
        self.lead_padding
    }

    /// Returns the number of trailing padding bits
    pub fn trailing_padding(&self) -> usize {
        self.trailing_padding
    }

    /// Returns the prefix, if any
    pub fn prefix(&self) -> Option<u64> {
        self.prefix
    }

    /// Returns the suffix, if any
    pub fn suffix(&self) -> Option<u64> {
        self.suffix
    }

    /// Returns reference to the chunks
    pub fn chunks(&self) -> &'a [u64] {
        self.chunks
    }

    /// Returns an iterator over the chunks
    pub fn iter(&self) -> UnalignedBitChunkIterator<'a> {
        UnalignedBitChunkIterator {
            prefix: self.prefix,
            chunks: self.chunks,
            suffix: self.suffix,
        }
    }

    /// Returns a zipped iterator over two [`UnalignedBitChunk`]
    #[inline]
    pub fn zip(&self, other: &UnalignedBitChunk<'a>) -> UnalignedBitChunkZipIterator<'a> {
        UnalignedBitChunkZipIterator {
            left: self.iter(),
            right: other.iter(),
        }
    }

    /// Counts the number of ones
    pub fn count_ones(&self) -> usize {
        let prefix_count = self.prefix.map(|x| x.count_ones() as usize).unwrap_or(0);
        let chunks_count: usize = self.chunks.iter().map(|&x| x.count_ones() as usize).sum();
        let suffix_count = self.suffix.map(|x| x.count_ones() as usize).unwrap_or(0);
        prefix_count + chunks_count + suffix_count
    }
}

/// An iterator over the chunks of an [`UnalignedBitChunk`]
#[derive(Debug, Clone)]
pub struct UnalignedBitChunkIterator<'a> {
    prefix: Option<u64>,
    chunks: &'a [u64],
    suffix: Option<u64>,
}

impl<'a> Iterator for UnalignedBitChunkIterator<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(prefix) = self.prefix.take() {
            return Some(prefix);
        }
        if let Some((&first, rest)) = self.chunks.split_first() {
            self.chunks = rest;
            return Some(first);
        }
        self.suffix.take()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }

    #[inline]
    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        if let Some(prefix) = self.prefix.take() {
            acc = f(acc, prefix);
        }
        for &chunk in self.chunks {
            acc = f(acc, chunk);
        }
        self.chunks = &[];
        if let Some(suffix) = self.suffix.take() {
            acc = f(acc, suffix);
        }
        acc
    }
}

impl<'a> UnalignedBitChunkIterator<'a> {
    /// Returns a zipped iterator over two [`UnalignedBitChunkIterator`]
    #[inline]
    pub fn zip(self, other: UnalignedBitChunkIterator<'a>) -> UnalignedBitChunkZipIterator<'a> {
        UnalignedBitChunkZipIterator {
            left: self,
            right: other,
        }
    }
}

impl ExactSizeIterator for UnalignedBitChunkIterator<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.prefix.is_some() as usize + self.chunks.len() + self.suffix.is_some() as usize
    }
}

impl std::iter::FusedIterator for UnalignedBitChunkIterator<'_> {}

impl<'a> DoubleEndedIterator for UnalignedBitChunkIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(suffix) = self.suffix.take() {
            return Some(suffix);
        }
        if let Some((&last, rest)) = self.chunks.split_last() {
            self.chunks = rest;
            return Some(last);
        }
        self.prefix.take()
    }
}

/// An iterator over zipped [`UnalignedBitChunk`]
#[derive(Debug)]
pub struct UnalignedBitChunkZipIterator<'a> {
    left: UnalignedBitChunkIterator<'a>,
    right: UnalignedBitChunkIterator<'a>,
}

impl<'a> Iterator for UnalignedBitChunkZipIterator<'a> {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        Some((self.left.next()?, self.right.next()?))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.left.size_hint()
    }

    #[inline]
    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;

        // 1. Consume elements until both are at the 'chunks' stage or one is exhausted.
        while self.left.prefix.is_some() || self.right.prefix.is_some() {
            if let (Some(l), Some(r)) = (self.left.next(), self.right.next()) {
                acc = f(acc, (l, r));
            } else {
                return acc;
            }
        }

        // 2. Now both prefix are None. Zip the chunks.
        let chunk_count = self.left.chunks.len().min(self.right.chunks.len());
        if chunk_count > 0 {
            let (l_chunks, l_rest) = self.left.chunks.split_at(chunk_count);
            let (r_chunks, r_rest) = self.right.chunks.split_at(chunk_count);

            for (&l, &r) in l_chunks.iter().zip(r_chunks.iter()) {
                acc = f(acc, (l, r));
            }

            self.left.chunks = l_rest;
            self.right.chunks = r_rest;
        }

        // 3. Consume remaining (suffix)
        while let (Some(l), Some(r)) = (self.left.next(), self.right.next()) {
            acc = f(acc, (l, r));
        }
        acc
    }

    #[inline]
    fn for_each<F>(self, mut f: F)
    where
        F: FnMut(Self::Item),
    {
        self.fold((), |_, item| f(item));
    }
}

impl ExactSizeIterator for UnalignedBitChunkZipIterator<'_> {}

impl std::iter::FusedIterator for UnalignedBitChunkZipIterator<'_> {}

impl DoubleEndedIterator for UnalignedBitChunkZipIterator<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        Some((self.left.next_back()?, self.right.next_back()?))
    }
}

#[inline]
fn compute_prefix_mask(lead_padding: usize) -> u64 {
    !((1 << lead_padding) - 1)
}

#[inline]
fn compute_suffix_mask(len: usize, lead_padding: usize) -> (u64, usize) {
    let trailing_bits = (len + lead_padding) % 64;

    if trailing_bits == 0 {
        return (u64::MAX, 0);
    }

    let trailing_padding = 64 - trailing_bits;
    let suffix_mask = (1 << trailing_bits) - 1;
    (suffix_mask, trailing_padding)
}

/// Iterates over an arbitrarily aligned byte buffer 64 bits at a time
///
/// [`Self::iter`] yields iterator of `u64`, and a remainder. The first byte in the buffer
/// will be the least significant byte in output u64
#[derive(Debug)]
pub struct BitChunks<'a> {
    buffer: &'a [u8],
    /// offset inside a byte, guaranteed to be between 0 and 7 (inclusive)
    bit_offset: usize,
    /// number of complete u64 chunks
    chunk_len: usize,
    /// number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    remainder_len: usize,
}

impl<'a> BitChunks<'a> {
    /// Create a new [`BitChunks`] from a byte array, and an offset and length in bits
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(
            ceil(offset + len, 8) <= buffer.len(),
            "offset + len out of bounds"
        );

        let byte_offset = offset / 8;
        let bit_offset = offset % 8;

        // number of complete u64 chunks
        let chunk_len = len / 64;
        // number of remaining bits
        let remainder_len = len % 64;

        BitChunks::<'a> {
            buffer: &buffer[byte_offset..],
            bit_offset,
            chunk_len,
            remainder_len,
        }
    }
}

/// Iterator over chunks of 64 bits represented as an u64
#[derive(Debug)]
pub struct BitChunkIterator<'a> {
    buffer: &'a [u8],
    bit_offset: usize,
    chunk_len: usize,
    index: usize,
}

impl<'a> BitChunkIterator<'a> {
    /// Returns a zipped iterator over two [`BitChunkIterator`]
    #[inline]
    pub fn zip(self, other: BitChunkIterator<'a>) -> BitChunksZipIterator<'a> {
        BitChunksZipIterator {
            left: self,
            right: other,
        }
    }
}

impl<'a> BitChunks<'a> {
    /// Returns the number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    #[inline]
    pub const fn remainder_len(&self) -> usize {
        self.remainder_len
    }

    /// Returns the number of `u64` chunks
    #[inline]
    pub const fn chunk_len(&self) -> usize {
        self.chunk_len
    }

    /// Returns the bitmask of remaining bits
    #[inline]
    pub fn remainder_bits(&self) -> u64 {
        let bit_len = self.remainder_len;
        if bit_len == 0 {
            0
        } else {
            let bit_offset = self.bit_offset;
            // number of bytes to read
            // might be one more than sizeof(u64) if the offset is in the middle of a byte
            let byte_len = ceil(bit_len + bit_offset, 8);
            // pointer to remainder bytes after all complete chunks
            let base = unsafe {
                self.buffer
                    .as_ptr()
                    .add(self.chunk_len * std::mem::size_of::<u64>())
            };

            let mut bits = unsafe { std::ptr::read(base) } as u64 >> bit_offset;
            for i in 1..byte_len {
                let byte = unsafe { std::ptr::read(base.add(i)) };
                bits |= (byte as u64) << (i * 8 - bit_offset);
            }

            bits & ((1 << bit_len) - 1)
        }
    }

    /// Return the number of `u64` that are needed to represent all bits
    /// (including remainder).
    ///
    /// This is equal to `chunk_len + 1` if there is a remainder,
    /// otherwise it is equal to `chunk_len`.
    #[inline]
    pub fn num_u64s(&self) -> usize {
        if self.remainder_len == 0 {
            self.chunk_len
        } else {
            self.chunk_len + 1
        }
    }

    /// Return the number of *bytes* that are needed to represent all bits
    /// (including remainder).
    #[inline]
    pub fn num_bytes(&self) -> usize {
        ceil(self.chunk_len * 64 + self.remainder_len, 8)
    }

    /// Returns an iterator over chunks of 64 bits represented as an `u64`
    #[inline]
    pub const fn iter(&self) -> BitChunkIterator<'a> {
        BitChunkIterator::<'a> {
            buffer: self.buffer,
            bit_offset: self.bit_offset,
            chunk_len: self.chunk_len,
            index: 0,
        }
    }

    /// Returns an iterator over chunks of 64 bits, with the remaining bits zero padded to 64-bits
    #[inline]
    pub fn iter_padded(&self) -> impl Iterator<Item = u64> + 'a {
        let remainder = (self.remainder_len > 0).then(|| self.remainder_bits());
        self.iter().chain(remainder)
    }

    /// Returns a zipped iterator over two [`BitChunks`] with the remaining bits zero padded to 64-bits
    ///
    /// # Panics
    ///
    /// Panics if the chunk lengths are not equal
    #[inline]
    pub fn zip_padded(&self, other: &BitChunks<'a>) -> impl Iterator<Item = (u64, u64)> + 'a {
        assert_eq!(self.remainder_len, other.remainder_len);
        let remainder =
            (self.remainder_len > 0).then(|| (self.remainder_bits(), other.remainder_bits()));
        self.zip(other).chain(remainder)
    }
    /// Returns a zipped iterator over two [`BitChunks`]
    #[inline]
    pub fn zip(&self, other: &BitChunks<'a>) -> BitChunksZipIterator<'a> {
        assert_eq!(self.chunk_len, other.chunk_len);
        BitChunksZipIterator {
            left: self.iter(),
            right: other.iter(),
        }
    }
}

/// An iterator over zipped chunks of 64 bits
#[derive(Debug)]
pub struct BitChunksZipIterator<'a> {
    left: BitChunkIterator<'a>,
    right: BitChunkIterator<'a>,
}

impl Iterator for BitChunksZipIterator<'_> {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        Some((self.left.next()?, self.right.next()?))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.left.size_hint()
    }

    #[inline]
    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        let chunk_len = self.left.chunk_len;
        let index = self.left.index;
        if index >= chunk_len {
            return acc;
        }

        let left_data = self.left.buffer.as_ptr() as *const u64;
        let right_data = self.right.buffer.as_ptr() as *const u64;

        let l_off = self.left.bit_offset;
        let r_off = self.right.bit_offset;

        match (l_off, r_off) {
            (0, 0) => {
                for i in index..chunk_len {
                    unsafe {
                        let l = std::ptr::read_unaligned(left_data.add(i)).to_le();
                        let r = std::ptr::read_unaligned(right_data.add(i)).to_le();
                        acc = f(acc, (l, r));
                    }
                }
            }
            (0, r_off) => {
                let r_shift_high = 64 - r_off;
                if self.right.buffer.len() >= (chunk_len + 1) * 8 {
                    let mut r_curr =
                        unsafe { std::ptr::read_unaligned(right_data.add(index)).to_le() };
                    for i in index..chunk_len {
                        unsafe {
                            let l = std::ptr::read_unaligned(left_data.add(i)).to_le();
                            let r_next = std::ptr::read_unaligned(right_data.add(i + 1)).to_le();
                            let r = (r_curr >> r_off) | (r_next << r_shift_high);
                            acc = f(acc, (l, r));
                            r_curr = r_next;
                        }
                    }
                } else {
                    for i in index..chunk_len {
                        unsafe {
                            let l = std::ptr::read_unaligned(left_data.add(i)).to_le();
                            let r_low = std::ptr::read_unaligned(right_data.add(i)).to_le();
                            let r_high =
                                std::ptr::read_unaligned(right_data.add(i + 1) as *const u8) as u64;
                            let r = (r_low >> r_off) | (r_high << r_shift_high);
                            acc = f(acc, (l, r));
                        }
                    }
                }
            }
            (l_off, 0) => {
                let l_shift_high = 64 - l_off;
                if self.left.buffer.len() >= (chunk_len + 1) * 8 {
                    let mut l_curr =
                        unsafe { std::ptr::read_unaligned(left_data.add(index)).to_le() };
                    for i in index..chunk_len {
                        unsafe {
                            let l_next = std::ptr::read_unaligned(left_data.add(i + 1)).to_le();
                            let l = (l_curr >> l_off) | (l_next << l_shift_high);
                            let r = std::ptr::read_unaligned(right_data.add(i)).to_le();
                            acc = f(acc, (l, r));
                            l_curr = l_next;
                        }
                    }
                } else {
                    for i in index..chunk_len {
                        unsafe {
                            let l_low = std::ptr::read_unaligned(left_data.add(i)).to_le();
                            let l_high =
                                std::ptr::read_unaligned(left_data.add(i + 1) as *const u8) as u64;
                            let l = (l_low >> l_off) | (l_high << l_shift_high);
                            let r = std::ptr::read_unaligned(right_data.add(i)).to_le();
                            acc = f(acc, (l, r));
                        }
                    }
                }
            }
            (l_off, r_off) => {
                let l_shift_high = 64 - l_off;
                let r_shift_high = 64 - r_off;

                // We can use a faster sliding window if we have padding
                // Arrow buffers usually have 64-byte padding
                let l_padded = self.left.buffer.len() >= (chunk_len + 1) * 8;
                let r_padded = self.right.buffer.len() >= (chunk_len + 1) * 8;

                if l_padded && r_padded {
                    let mut l_curr =
                        unsafe { std::ptr::read_unaligned(left_data.add(index)).to_le() };
                    let mut r_curr =
                        unsafe { std::ptr::read_unaligned(right_data.add(index)).to_le() };

                    for i in index..chunk_len {
                        unsafe {
                            let l_next = std::ptr::read_unaligned(left_data.add(i + 1)).to_le();
                            let r_next = std::ptr::read_unaligned(right_data.add(i + 1)).to_le();

                            let l = (l_curr >> l_off) | (l_next << l_shift_high);
                            let r = (r_curr >> r_off) | (r_next << r_shift_high);

                            acc = f(acc, (l, r));
                            l_curr = l_next;
                            r_curr = r_next;
                        }
                    }
                } else {
                    // Fallback to safe but slower byte reads for high bits
                    for i in index..chunk_len {
                        unsafe {
                            let l_low = std::ptr::read_unaligned(left_data.add(i)).to_le();
                            let l_high =
                                std::ptr::read_unaligned(left_data.add(i + 1) as *const u8) as u64;
                            let l = (l_low >> l_off) | (l_high << l_shift_high);

                            let r_low = std::ptr::read_unaligned(right_data.add(i)).to_le();
                            let r_high =
                                std::ptr::read_unaligned(right_data.add(i + 1) as *const u8) as u64;
                            let r = (r_low >> r_off) | (r_high << r_shift_high);

                            acc = f(acc, (l, r));
                        }
                    }
                }
            }
        }
        acc
    }

    #[inline]
    fn for_each<F>(self, mut f: F)
    where
        F: FnMut(Self::Item),
    {
        self.fold((), |_, item| f(item));
    }
}

impl ExactSizeIterator for BitChunksZipIterator<'_> {}

impl<'a> IntoIterator for BitChunks<'a> {
    type Item = u64;
    type IntoIter = BitChunkIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Iterator for BitChunkIterator<'_> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        let index = self.index;
        if index >= self.chunk_len {
            return None;
        }

        // cast to *const u64 should be fine since we are using read_unaligned below
        #[allow(clippy::cast_ptr_alignment)]
        let raw_data = self.buffer.as_ptr() as *const u64;

        // bit-packed buffers are stored starting with the least-significant byte first
        // so when reading as u64 on a big-endian machine, the bytes need to be swapped
        let current = unsafe { std::ptr::read_unaligned(raw_data.add(index)).to_le() };

        let bit_offset = self.bit_offset;

        let combined = if bit_offset == 0 {
            current
        } else {
            // the constructor ensures that bit_offset is in 0..8
            // that means we need to read at most one additional byte to fill in the high bits
            let next =
                unsafe { std::ptr::read_unaligned(raw_data.add(index + 1) as *const u8) as u64 };

            (current >> bit_offset) | (next << (64 - bit_offset))
        };

        self.index = index + 1;

        Some(combined)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.chunk_len - self.index;
        (len, Some(len))
    }

    #[inline]
    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        let chunk_len = self.chunk_len;
        let index = self.index;
        if index >= chunk_len {
            return acc;
        }

        let data = self.buffer.as_ptr() as *const u64;
        let bit_offset = self.bit_offset;

        if bit_offset == 0 {
            for i in index..chunk_len {
                let v = unsafe { std::ptr::read_unaligned(data.add(i)).to_le() };
                acc = f(acc, v);
            }
        } else {
            let shift_high = 64 - bit_offset;
            // Use sliding window if padded
            if self.buffer.len() >= (chunk_len + 1) * 8 {
                let mut curr = unsafe { std::ptr::read_unaligned(data.add(index)).to_le() };
                for i in index..chunk_len {
                    unsafe {
                        let next = std::ptr::read_unaligned(data.add(i + 1)).to_le();
                        let v = (curr >> bit_offset) | (next << shift_high);
                        acc = f(acc, v);
                        curr = next;
                    }
                }
            } else {
                for i in index..chunk_len {
                    unsafe {
                        let low = std::ptr::read_unaligned(data.add(i)).to_le();
                        let high = std::ptr::read_unaligned(data.add(i + 1) as *const u8) as u64;
                        let v = (low >> bit_offset) | (high << shift_high);
                        acc = f(acc, v);
                    }
                }
            }
        }
        acc
    }

    #[inline]
    fn for_each<F>(self, mut f: F)
    where
        F: FnMut(Self::Item),
    {
        self.fold((), |_, item| f(item));
    }
}

impl ExactSizeIterator for BitChunkIterator<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.chunk_len - self.index
    }
}

#[cfg(test)]
mod tests {
    use rand::distr::uniform::UniformSampler;
    use rand::distr::uniform::UniformUsize;
    use rand::prelude::*;
    use rand::rng;

    use crate::buffer::Buffer;
    use crate::util::bit_chunk_iterator::{BitChunks, UnalignedBitChunk};

    #[test]
    fn test_iter_aligned() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(0, 64);
        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(vec![0x0706050403020100], result);
    }

    #[test]
    fn test_iter_unaligned() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(4, 64);

        assert_eq!(0, bitchunks.remainder_len());
        assert_eq!(0, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }

    #[test]
    fn test_iter_unaligned_remainder_1_byte() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(4, 66);

        assert_eq!(2, bitchunks.remainder_len());
        assert_eq!(0b00000011, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }

    #[test]
    fn test_iter_unaligned_remainder_bits_across_bytes() {
        let input: &[u8] = &[0b00111111, 0b11111100];
        let buffer: Buffer = Buffer::from(input);

        // remainder contains bits from both bytes
        // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
        let bitchunks = buffer.bit_chunks(6, 7);

        assert_eq!(7, bitchunks.remainder_len());
        assert_eq!(0b1110000, bitchunks.remainder_bits());
    }

    #[test]
    fn test_iter_unaligned_remainder_bits_large() {
        let input: &[u8] = &[
            0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111,
            0b00000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(2, 63);

        assert_eq!(63, bitchunks.remainder_len());
        assert_eq!(
            0b100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111,
            bitchunks.remainder_bits()
        );
    }

    #[test]
    fn test_iter_remainder_out_of_bounds() {
        // allocating a full page should trigger a fault when reading out of bounds
        const ALLOC_SIZE: usize = 4 * 1024;
        let input = vec![0xFF_u8; ALLOC_SIZE];

        let buffer: Buffer = Buffer::from_vec(input);

        let bitchunks = buffer.bit_chunks(57, ALLOC_SIZE * 8 - 57);

        assert_eq!(u64::MAX, bitchunks.iter().last().unwrap());
        assert_eq!(0x7F, bitchunks.remainder_bits());
    }

    #[test]
    #[should_panic(expected = "offset + len out of bounds")]
    fn test_out_of_bound_should_panic_length_is_more_than_buffer_length() {
        const ALLOC_SIZE: usize = 4 * 1024;
        let input = vec![0xFF_u8; ALLOC_SIZE];

        let buffer: Buffer = Buffer::from_vec(input);

        // We are reading more than exists in the buffer
        buffer.bit_chunks(0, (ALLOC_SIZE + 1) * 8);
    }

    #[test]
    #[should_panic(expected = "offset + len out of bounds")]
    fn test_out_of_bound_should_panic_length_is_more_than_buffer_length_but_not_when_not_using_ceil()
     {
        const ALLOC_SIZE: usize = 4 * 1024;
        let input = vec![0xFF_u8; ALLOC_SIZE];

        let buffer: Buffer = Buffer::from_vec(input);

        // We are reading more than exists in the buffer
        buffer.bit_chunks(0, (ALLOC_SIZE * 8) + 1);
    }

    #[test]
    #[should_panic(expected = "offset + len out of bounds")]
    fn test_out_of_bound_should_panic_when_offset_is_not_zero_and_length_is_the_entire_buffer_length()
     {
        const ALLOC_SIZE: usize = 4 * 1024;
        let input = vec![0xFF_u8; ALLOC_SIZE];

        let buffer: Buffer = Buffer::from_vec(input);

        // We are reading more than exists in the buffer
        buffer.bit_chunks(8, ALLOC_SIZE * 8);
    }

    #[test]
    #[should_panic(expected = "offset + len out of bounds")]
    fn test_out_of_bound_should_panic_when_offset_is_not_zero_and_length_is_the_entire_buffer_length_with_ceil()
     {
        const ALLOC_SIZE: usize = 4 * 1024;
        let input = vec![0xFF_u8; ALLOC_SIZE];

        let buffer: Buffer = Buffer::from_vec(input);

        // We are reading more than exists in the buffer
        buffer.bit_chunks(1, ALLOC_SIZE * 8);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_unaligned_bit_chunk_iterator() {
        let buffer = Buffer::from(&[0xFF; 5]);
        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 40);

        assert!(unaligned.chunks().is_empty()); // Less than 128 elements
        assert_eq!(unaligned.lead_padding(), 0);
        assert_eq!(unaligned.trailing_padding(), 24);
        // 24x 1 bit then 40x 0 bits
        assert_eq!(
            unaligned.prefix(),
            Some(0b0000000000000000000000001111111111111111111111111111111111111111)
        );
        assert_eq!(unaligned.suffix(), None);

        let buffer = buffer.slice(1);
        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 32);

        assert!(unaligned.chunks().is_empty()); // Less than 128 elements
        assert_eq!(unaligned.lead_padding(), 0);
        assert_eq!(unaligned.trailing_padding(), 32);
        // 32x 1 bit then 32x 0 bits
        assert_eq!(
            unaligned.prefix(),
            Some(0b0000000000000000000000000000000011111111111111111111111111111111)
        );
        assert_eq!(unaligned.suffix(), None);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 5, 27);

        assert!(unaligned.chunks().is_empty()); // Less than 128 elements
        assert_eq!(unaligned.lead_padding(), 5); // 5 % 8 == 5
        assert_eq!(unaligned.trailing_padding(), 32);
        // 5x 0 bit, 27x 1 bit then 32x 0 bits
        assert_eq!(
            unaligned.prefix(),
            Some(0b0000000000000000000000000000000011111111111111111111111111100000)
        );
        assert_eq!(unaligned.suffix(), None);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 12, 20);

        assert!(unaligned.chunks().is_empty()); // Less than 128 elements
        assert_eq!(unaligned.lead_padding(), 4); // 12 % 8 == 4
        assert_eq!(unaligned.trailing_padding(), 40);
        // 4x 0 bit, 20x 1 bit then 40x 0 bits
        assert_eq!(
            unaligned.prefix(),
            Some(0b0000000000000000000000000000000000000000111111111111111111110000)
        );
        assert_eq!(unaligned.suffix(), None);

        let buffer = Buffer::from(&[0xFF; 14]);

        // Verify buffer alignment
        let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
        assert_eq!(prefix.len(), 0);
        assert_eq!(aligned.len(), 1);
        assert_eq!(suffix.len(), 6);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 112);

        assert!(unaligned.chunks().is_empty()); // Less than 128 elements
        assert_eq!(unaligned.lead_padding(), 0); // No offset and buffer aligned on 64-bit boundary
        assert_eq!(unaligned.trailing_padding(), 16);
        assert_eq!(unaligned.prefix(), Some(u64::MAX));
        assert_eq!(unaligned.suffix(), Some((1 << 48) - 1));

        let buffer = Buffer::from(&[0xFF; 16]);

        // Verify buffer alignment
        let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
        assert_eq!(prefix.len(), 0);
        assert_eq!(aligned.len(), 2);
        assert_eq!(suffix.len(), 0);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 128);

        assert_eq!(unaligned.prefix(), Some(u64::MAX));
        assert_eq!(unaligned.suffix(), Some(u64::MAX));
        assert!(unaligned.chunks().is_empty()); // Exactly 128 elements

        let buffer = Buffer::from(&[0xFF; 64]);

        // Verify buffer alignment
        let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
        assert_eq!(prefix.len(), 0);
        assert_eq!(aligned.len(), 8);
        assert_eq!(suffix.len(), 0);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 512);

        // Buffer is completely aligned and larger than 128 elements -> all in chunks array
        assert_eq!(unaligned.suffix(), None);
        assert_eq!(unaligned.prefix(), None);
        assert_eq!(unaligned.chunks(), [u64::MAX; 8].as_slice());
        assert_eq!(unaligned.lead_padding(), 0);
        assert_eq!(unaligned.trailing_padding(), 0);

        let buffer = buffer.slice(1); // Offset buffer 1 byte off 64-bit alignment

        // Verify buffer alignment
        let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
        assert_eq!(prefix.len(), 7);
        assert_eq!(aligned.len(), 7);
        assert_eq!(suffix.len(), 0);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 504);

        // Need a prefix with 1 byte of lead padding to bring the buffer into alignment
        assert_eq!(unaligned.prefix(), Some(u64::MAX - 0xFF));
        assert_eq!(unaligned.suffix(), None);
        assert_eq!(unaligned.chunks(), [u64::MAX; 7].as_slice());
        assert_eq!(unaligned.lead_padding(), 8);
        assert_eq!(unaligned.trailing_padding(), 0);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 300);

        // Out of 64-bit alignment by 8 bits from buffer, and 17 bits from provided offset
        //   => need 8 + 17 = 25 bits of lead padding + 39 bits in prefix
        //
        // This leaves 300 - 17 = 261 bits remaining
        //   => 4x 64-bit aligned 64-bit chunks + 5 remaining bits
        //   => trailing padding of 59 bits
        assert_eq!(unaligned.lead_padding(), 25);
        assert_eq!(unaligned.trailing_padding(), 59);
        assert_eq!(unaligned.prefix(), Some(u64::MAX - (1 << 25) + 1));
        assert_eq!(unaligned.suffix(), Some(0b11111));
        assert_eq!(unaligned.chunks(), [u64::MAX; 4].as_slice());

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 0);

        assert_eq!(unaligned.prefix(), None);
        assert_eq!(unaligned.suffix(), None);
        assert!(unaligned.chunks().is_empty());
        assert_eq!(unaligned.lead_padding(), 0);
        assert_eq!(unaligned.trailing_padding(), 0);

        let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 1);

        assert_eq!(unaligned.prefix(), Some(2));
        assert_eq!(unaligned.suffix(), None);
        assert!(unaligned.chunks().is_empty());
        assert_eq!(unaligned.lead_padding(), 1);
        assert_eq!(unaligned.trailing_padding(), 62);
    }

    #[test]
    fn test_fold() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let bitchunks = BitChunks::new(input, 0, 128);
        let result = bitchunks.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(x);
            acc
        });
        assert_eq!(vec![0x0706050403020100, 0x0f0e0d0c0b0a0908], result);

        let bitchunks = BitChunks::new(input, 4, 64);
        let result = bitchunks.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(x);
            acc
        });
        // 0x080706050403020100
        // offset 4 bits
        let expected = (0x080706050403020100_u128 >> 4) as u64;
        assert_eq!(vec![expected], result);
    }

    #[test]
    fn test_fold_zip() {
        let left: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let right: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let left_chunks = BitChunks::new(left, 0, 64);
        let right_chunks = BitChunks::new(right, 0, 64);

        let result =
            left_chunks
                .iter()
                .zip(right_chunks.iter())
                .fold(Vec::new(), |mut acc, (l, r)| {
                    acc.push(l ^ r);
                    acc
                });
        assert_eq!(vec![0x0706050403020100 ^ 0x0807060504030201], result);

        let _left_chunks = BitChunks::new(left, 4, 32);
        let _right_chunks = BitChunks::new(right, 8, 32);
        // chunk_len is 0 for 32 bits.
        // let's try 64 bits with offset
        let left: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let right: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let left_chunks = BitChunks::new(left, 4, 64);
        let right_chunks = BitChunks::new(right, 8, 64);

        let result =
            left_chunks
                .iter()
                .zip(right_chunks.iter())
                .fold(Vec::new(), |mut acc, (l, r)| {
                    acc.push(l & r);
                    acc
                });

        let l_expected = {
            let mut b = [0u8; 16];
            b[0..9].copy_from_slice(&left[0..9]);
            u128::from_le_bytes(b) >> 4
        } as u64;
        let r_expected = {
            let mut b = [0u8; 16];
            b[0..9].copy_from_slice(&right[0..9]);
            u128::from_le_bytes(b) >> 8
        } as u64;
        assert_eq!(vec![l_expected & r_expected], result);
    }

    #[test]
    fn test_unaligned_fold_zip() {
        let left: &[u8] = &[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let right: &[u8] = &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        ];

        let cases = [
            (0, 0, 128),
            (4, 4, 64),
            (4, 8, 64),
            (0, 8, 128),
            (1, 2, 128),
            (7, 0, 128),
        ];

        for (l_off, r_off, len) in cases {
            let left_bc = UnalignedBitChunk::new(left, l_off, len);
            let right_bc = UnalignedBitChunk::new(right, r_off, len);

            let expected: Vec<_> = left_bc.iter().zip(right_bc.iter()).collect();

            // Test fold
            let actual: Vec<_> = left_bc.zip(&right_bc).fold(Vec::new(), |mut acc, x| {
                acc.push(x);
                acc
            });
            assert_eq!(
                expected, actual,
                "Fold failed for l_off={}, r_off={}, len={}",
                l_off, r_off, len
            );

            // Test for_each
            let mut actual_for_each = Vec::new();
            left_bc.zip(&right_bc).for_each(|x| actual_for_each.push(x));
            assert_eq!(
                expected, actual_for_each,
                "ForEach failed for l_off={}, r_off={}, len={}",
                l_off, r_off, len
            );

            // Test next()
            let actual_next: Vec<_> = left_bc.zip(&right_bc).collect();
            assert_eq!(
                expected, actual_next,
                "Next failed for l_off={}, r_off={}, len={}",
                l_off, r_off, len
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn fuzz_unaligned_bit_chunk_iterator() {
        let mut rng = rng();

        let uusize = UniformUsize::new(usize::MIN, usize::MAX).unwrap();
        for _ in 0..100 {
            let mask_len = rng.random_range(0..1024);
            let bools: Vec<_> = std::iter::from_fn(|| Some(rng.random()))
                .take(mask_len)
                .collect();

            let buffer = Buffer::from_iter(bools.iter().cloned());

            let max_offset = 64.min(mask_len);
            let offset = uusize.sample(&mut rng).checked_rem(max_offset).unwrap_or(0);

            let max_truncate = 128.min(mask_len - offset);
            let truncate = uusize
                .sample(&mut rng)
                .checked_rem(max_truncate)
                .unwrap_or(0);

            let unaligned =
                UnalignedBitChunk::new(buffer.as_slice(), offset, mask_len - offset - truncate);

            let bool_slice = &bools[offset..mask_len - truncate];

            let count = unaligned.count_ones();
            let expected_count = bool_slice.iter().filter(|x| **x).count();

            assert_eq!(count, expected_count);

            let collected: Vec<u64> = unaligned.iter().collect();

            let get_bit = |idx: usize| -> bool {
                let padded_index = idx + unaligned.lead_padding();
                let byte_idx = padded_index / 64;
                let bit_idx = padded_index % 64;
                (collected[byte_idx] & (1 << bit_idx)) != 0
            };

            for (idx, b) in bool_slice.iter().enumerate() {
                assert_eq!(*b, get_bit(idx))
            }
        }
    }
}
