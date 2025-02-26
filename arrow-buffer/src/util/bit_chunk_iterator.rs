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

use crate::util::bit_util::ceil;
use std::fmt::Debug;

/// Iterates over an arbitrarily aligned byte buffer
///
/// Yields an iterator of aligned u64, along with the leading and trailing
/// u64 necessary to align the buffer to a 8-byte boundary
///
/// This is unlike [`BitChunkIterator`] which only exposes a trailing u64,
/// and consequently has to perform more work for each read
#[derive(Debug)]
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

        let bytes_len = (len + offset_padding + 7) / 8;
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
        self.prefix
            .into_iter()
            .chain(self.chunks.iter().cloned())
            .chain(self.suffix)
    }

    /// Counts the number of ones
    pub fn count_ones(&self) -> usize {
        self.iter().map(|x| x.count_ones() as usize).sum()
    }
}

/// Iterator over an [`UnalignedBitChunk`]
pub type UnalignedBitChunkIterator<'a> = std::iter::Chain<
    std::iter::Chain<std::option::IntoIter<u64>, std::iter::Cloned<std::slice::Iter<'a, u64>>>,
    std::option::IntoIter<u64>,
>;

#[inline]
fn read_u64(input: &[u8]) -> u64 {
    let len = input.len().min(8);
    let mut buf = [0_u8; 8];
    buf[..len].copy_from_slice(input);
    u64::from_le_bytes(buf)
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

/// Iterates over an arbitrarily aligned byte buffer
///
/// Yields an iterator of u64, and a remainder. The first byte in the buffer
/// will be the least significant byte in output u64
///
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
        assert!(ceil(offset + len, 8) <= buffer.len() * 8);

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

impl<'a> BitChunks<'a> {
    /// Returns the number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    #[inline]
    pub const fn remainder_len(&self) -> usize {
        self.remainder_len
    }

    /// Returns the number of chunks
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

    /// Returns an iterator over chunks of 64 bits represented as an u64
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
        self.iter().chain(std::iter::once(self.remainder_bits()))
    }
}

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
        (
            self.chunk_len - self.index,
            Some(self.chunk_len - self.index),
        )
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
    use crate::util::bit_chunk_iterator::UnalignedBitChunk;

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
