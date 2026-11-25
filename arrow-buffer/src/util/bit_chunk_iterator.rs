// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Types for iterating over bitmasks in 64-bit chunks

#[cfg(target_arch = "aarch64")]
use crate::ToByteSlice;
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
    /// Create from a byte array, offset and length in bits
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
        // Safety: u64 has no invalid bit patterns so reinterpreting initialized u8 bytes as u64 is sound.
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
    #[inline]
    pub fn iter(&self) -> UnalignedBitChunkIterator<'a> {
        self.prefix
            .into_iter()
            .chain(self.chunks.iter().copied())
            .chain(self.suffix)
    }

    /// Counts the number of ones
    pub fn count_ones(&self) -> usize {
        let edges = self.prefix.into_iter().chain(self.suffix);
        edges.map(|x| x.count_ones() as usize).sum::<usize>() + count_ones_words(self.chunks)
    }
}

#[cfg(not(target_arch = "aarch64"))]
fn count_ones_words(words: &[u64]) -> usize {
    words.iter().map(|x| x.count_ones() as usize).sum()
}

/// Summing four byte popcounts in `u8` lanes before widening lets LLVM emit one
/// widening per 64-byte block. A `u64::count_ones` sum widens every 16 bytes. On x86
/// the `u64` sum measured faster (#11776), so this is only used on aarch64.
#[cfg(target_arch = "aarch64")]
fn count_ones_words(words: &[u64]) -> usize {
    // A block is 8 words (64 bytes), the unit that is widened once.
    let (blocks, tail) = words.as_chunks::<8>();
    let mut total = 0;
    // `lane_counts` counts in u16 lanes so that one widening per block suffices. Each
    // lane gains at most 32 (four bytes of eight bits) per block, so flush the lanes
    // (add them into `total` and reset them) before they can overflow.
    const BLOCKS_PER_FLUSH: usize = (u16::MAX / 32) as usize;
    for flush_group in blocks.chunks(BLOCKS_PER_FLUSH) {
        let mut lane_counts = [0u16; 16];
        for block in flush_group {
            let bytes = block.as_slice().to_byte_slice();
            // Lane `lane` sums byte `lane` of each of the block's four 16-byte vectors.
            for lane in 0..16 {
                let count = bytes[lane].count_ones() as u8
                    + bytes[16 + lane].count_ones() as u8
                    + bytes[32 + lane].count_ones() as u8
                    + bytes[48 + lane].count_ones() as u8;
                lane_counts[lane] += count as u16;
            }
        }
        total += lane_counts.iter().copied().map(usize::from).sum::<usize>();
    }
    // Counting the tail in words avoids widening each byte popcount to u64.
    total + tail.iter().map(|w| w.count_ones() as usize).sum::<usize>()
}

impl<'a> IntoIterator for &UnalignedBitChunk<'a> {
    type Item = u64;
    type IntoIter = UnalignedBitChunkIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over an [`UnalignedBitChunk`]
pub type UnalignedBitChunkIterator<'a> = std::iter::Chain<
    std::iter::Chain<std::option::IntoIter<u64>, std::iter::Copied<std::slice::Iter<'a, u64>>>,
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
        let end = offset.checked_add(len).expect("offset + len out of bounds");
        assert!(ceil(end, 8) <= buffer.len(), "offset + len out of bounds");

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
            // Safety: the buffer contains `chunk_len * 8 + ceil(remainder_len + bit_offset, 8)`
            // bytes, so offsetting by `chunk_len * 8` and reading `byte_len` bytes is in-bounds.
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

    /// Returns the `index`th complete chunk of 64 bits, the value
    /// [`Self::iter`] yields at that position
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.chunk_len()`
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_buffer::Buffer;
    /// let buffer = Buffer::from((0..17_u8).collect::<Vec<_>>());
    /// // Start at bit 8, so chunk 1 holds bytes 9 through 16
    /// let chunks = buffer.bit_chunks(8, 128);
    /// assert_eq!(chunks.chunk_len(), 2);
    /// assert_eq!(chunks.chunk(1), 0x100f0e0d0c0b0a09);
    /// assert_eq!(chunks.iter().nth(1), Some(chunks.chunk(1)));
    /// ```
    #[inline]
    pub fn chunk(&self, index: usize) -> u64 {
        assert!(index < self.chunk_len, "chunk index out of bounds");
        // Safety: `index < chunk_len`, and the constructor checked the buffer
        // covers every complete chunk plus the remainder byte
        unsafe { read_chunk(self.buffer, self.bit_offset, index) }
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

impl<'a> IntoIterator for &BitChunks<'a> {
    type Item = u64;
    type IntoIter = BitChunkIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Reads the `index`th complete 64-bit chunk of `buffer`, whose bits start
/// at `bit_offset` (in `0..8`)
///
/// # Safety
///
/// `index` must be less than the number of complete chunks, so that the
/// buffer holds at least `index * 8 + 8` bytes, plus one more byte when
/// `bit_offset != 0` (the remainder byte the constructor guarantees)
#[inline]
unsafe fn read_chunk(buffer: &[u8], bit_offset: usize, index: usize) -> u64 {
    debug_assert!(bit_offset < 8);
    debug_assert!(buffer.len() >= index * 8 + 8 + usize::from(bit_offset != 0));
    // cast to *const u64 should be fine since we are using read_unaligned below
    #[expect(clippy::cast_ptr_alignment)]
    let raw_data = buffer.as_ptr().cast::<u64>();

    // bit-packed buffers are stored starting with the least-significant byte first
    // so when reading as u64 on a big-endian machine, the bytes need to be swapped
    // Safety: the caller guarantees `raw_data.add(index)` is in bounds;
    // `read_unaligned` handles any pointer alignment.
    let current = unsafe { std::ptr::read_unaligned(raw_data.add(index)).to_le() };

    if bit_offset == 0 {
        current
    } else {
        // the constructor ensures that bit_offset is in 0..8
        // that means we need to read at most one additional byte to fill in the high bits
        // Safety: the caller guarantees the byte at `index + 1` is in bounds
        let next = unsafe { std::ptr::read_unaligned(raw_data.add(index + 1).cast::<u8>()) as u64 };

        (current >> bit_offset) | (next << (64 - bit_offset))
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

        // Safety: `index < self.chunk_len`, and the constructor checked the
        // buffer covers every complete chunk plus the remainder byte
        let combined = unsafe { read_chunk(self.buffer, self.bit_offset, index) };

        self.index = index + 1;

        Some(combined)
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<u64> {
        self.index = self.index.saturating_add(n).min(self.chunk_len);
        self.next()
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
