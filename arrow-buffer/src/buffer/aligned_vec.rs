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

use std::ptr::NonNull;
use std::sync::Arc;

use super::immutable::Buffer;

#[repr(align(64))]
#[derive(Clone, Copy)]
struct Aligned64 {
    _bytes: [u8; 64],
}

/// A growable, 64-byte-aligned byte buffer that converts into a [`Buffer`] without copying.
///
/// Backed by `Vec<Aligned64>`, guaranteeing 64-byte alignment on every allocation and
/// reallocation (matching a full CPU cache line / AVX-512 SIMD loads). Capacity is always
/// a multiple of 64 bytes; only bytes written via [`extend_from_slice`](Self::extend_from_slice)
/// are visible through slice accessors or the resulting [`Buffer`].
///
/// Prefer over [`MutableBuffer`](super::MutableBuffer) for write-heavy paths where alignment matters.
///
/// Call [`Buffer::from`] to consume the vec and produce a zero-copy [`Buffer`]; an [`Arc`]
/// keeps the allocation alive for the buffer's lifetime.
pub struct AlignedVec {
    raw_vector: Vec<Aligned64>,
    filled_len: usize,
}

impl AlignedVec {
    const CHUNK: usize = 64;

    /// Creates a new, empty `AlignedVec` with no initial allocation.
    /// Use [`with_capacity`](Self::with_capacity) when the approximate final size is known.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new `AlignedVec` with at least `capacity` bytes pre-allocated.
    ///
    /// The actual allocation is rounded up to the nearest multiple of 64 bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        let needed_chunks = capacity.div_ceil(Self::CHUNK);
        Self {
            raw_vector: Vec::with_capacity(needed_chunks),
            filled_len: 0,
        }
    }

    /// Returns the number of bytes written.
    #[inline]
    pub fn len(&self) -> usize {
        self.filled_len
    }

    /// Returns `true` if no bytes have been written.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.filled_len == 0
    }

    /// Returns the total number of bytes allocated (always a multiple of 64).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.raw_vector.capacity() * Self::CHUNK
    }

    fn ensure_capacity(&mut self, total_bytes: usize) {
        let needed_chunks = total_bytes.div_ceil(Self::CHUNK);

        if needed_chunks > self.raw_vector.capacity() {
            let current_chunk_cap = self.raw_vector.capacity();
            let new_chunk_cap = needed_chunks.max(current_chunk_cap * 2).max(1);
            let additional = new_chunk_cap - self.raw_vector.capacity();
            self.raw_vector.reserve(additional);
        }

        if needed_chunks > self.raw_vector.len() {
            unsafe {
                self.raw_vector.set_len(needed_chunks);
            }
        }
    }

    // Returns the entire initialized region as bytes (includes unwritten tail).
    // Used internally to write into positions past filled_len.
    fn allocated_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = self.raw_vector.as_mut_ptr().cast::<u8>();
        let len = self.raw_vector.len() * Self::CHUNK;
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }

    /// Returns the written bytes as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        let ptr = self.raw_vector.as_ptr().cast::<u8>();
        unsafe { std::slice::from_raw_parts(ptr, self.filled_len) }
    }

    /// Returns the written bytes as a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.raw_vector.as_mut_ptr().cast::<u8>();
        unsafe { std::slice::from_raw_parts_mut(ptr, self.filled_len) }
    }

    /// Appends `data` to the buffer, reallocating if necessary.
    ///
    /// Empty slices are a no-op.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let offset = self.filled_len;
        let end = offset + data.len();
        // Fast path: enough initialized chunks already exist; skip div_ceil + set_len.
        if end > self.raw_vector.len() * Self::CHUNK {
            self.ensure_capacity(end);
        }
        self.allocated_bytes_mut()[offset..end].copy_from_slice(data);
        self.filled_len = end;
    }

    /// Reduces the written length to `len` bytes. No-op if `len` is not less than the
    /// current length. The allocation is always retained.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len < self.filled_len {
            self.filled_len = len;
        }
    }

    /// Resets the written length to zero without releasing the allocation.
    #[inline]
    pub fn clear(&mut self) {
        self.filled_len = 0;
    }
}

impl Default for AlignedVec {
    fn default() -> Self {
        Self::new()
    }
}

impl From<AlignedVec> for Buffer {
    fn from(mut vec: AlignedVec) -> Self {
        let filled_len = vec.filled_len;
        if filled_len == 0 {
            return Buffer::from(&[] as &[u8]);
        }
        let ptr = NonNull::new(vec.raw_vector.as_mut_ptr().cast::<u8>())
            .expect("Vec<Aligned64> heap pointer is never null when len > 0");
        // Safety: ptr is valid for filled_len bytes; Arc<AlignedVec> keeps the allocation alive.
        unsafe { Buffer::from_custom_allocation(ptr, filled_len, Arc::new(vec)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_aligned(buf: &AlignedVec) -> bool {
        if buf.is_empty() {
            // No bytes written yet; check the raw Vec pointer directly.
            buf.raw_vector.as_ptr().align_offset(64) == 0
        } else {
            buf.as_slice().as_ptr().align_offset(64) == 0
        }
    }

    #[test]
    fn test_empty_on_construction() {
        let buf = AlignedVec::with_capacity(256);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.as_slice(), &[] as &[u8]);
    }

    #[test]
    fn test_extend_accumulates_and_mut_slice_reflects_writes() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"foo");
        assert_eq!(buf.len(), 3);
        buf.extend_from_slice(b"bar");
        assert_eq!(buf.len(), 6);
        buf.extend_from_slice(b"baz");
        assert_eq!(buf.len(), 9);
        assert_eq!(buf.as_slice(), b"foobarbaz");

        buf.as_mut_slice()
            .iter_mut()
            .for_each(|byte| *byte = byte.to_ascii_uppercase());
        assert_eq!(buf.len(), 9);
        assert_eq!(buf.as_slice(), b"FOOBARBAZ");
    }

    #[test]
    fn test_truncate() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"hello world");
        buf.truncate(5);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_slice(), b"hello");
        // truncate to same length is a no-op
        buf.truncate(5);
        assert_eq!(buf.len(), 5);
        // truncate beyond current length is a no-op
        buf.truncate(100);
        assert_eq!(buf.len(), 5);
        // truncate to zero behaves like clear
        buf.truncate(0);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_clear_resets_len_and_reuses_allocation() {
        let mut buf = AlignedVec::with_capacity(128);
        let initial_capacity = buf.capacity();
        assert!(is_aligned(&buf));

        buf.extend_from_slice(&[0u8; 127]);
        assert_eq!(buf.len(), 127);
        assert_eq!(
            buf.capacity(),
            initial_capacity,
            "capacity must not grow within pre-allocated range"
        );

        buf.clear();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_slice(), &[] as &[u8]);
        assert_eq!(
            buf.capacity(),
            initial_capacity,
            "capacity must not change after clear"
        );

        buf.extend_from_slice(b"reused");
        assert_eq!(buf.len(), 6);
        assert_eq!(buf.as_slice(), b"reused");
        assert_eq!(
            buf.capacity(),
            initial_capacity,
            "capacity must not change on re-use within range"
        );

        // Write past the original capacity to force reallocation.
        buf.extend_from_slice(&[1u8; 200]);
        assert!(
            buf.capacity() > initial_capacity,
            "capacity must grow after exceeding pre-allocated range"
        );
        assert!(is_aligned(&buf));
    }

    #[test]
    fn test_into_buffer_alignment_and_data() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"aligned data");
        assert!(is_aligned(&buf));
        let buffer = Buffer::from(buf);
        assert_eq!(buffer.as_slice(), b"aligned data");
        assert_eq!(
            buffer.as_ptr().align_offset(64),
            0,
            "buffer must be 64-byte aligned"
        );
    }

    #[test]
    fn test_into_buffer_empty() {
        let buf = AlignedVec::new();
        let buffer = Buffer::from(buf);
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_alignment_at_capacity_boundaries() {
        for capacity in [0, 1, 63, 64, 65, 127, 128, 129] {
            let mut buf = AlignedVec::with_capacity(capacity);
            buf.extend_from_slice(b"x");
            assert!(
                is_aligned(&buf),
                "with_capacity({capacity}) must produce a 64-byte-aligned allocation"
            );
        }
    }

    #[test]
    fn test_pointer_stable_within_capacity() {
        let mut buf = AlignedVec::with_capacity(256);
        assert!(is_aligned(&buf));

        buf.extend_from_slice(&[0u8; 128]);
        let ptr_after_first_write = buf.as_slice().as_ptr();

        buf.extend_from_slice(&[1u8; 128]);
        assert_eq!(buf.len(), 256);
        assert_eq!(
            buf.as_slice().as_ptr(),
            ptr_after_first_write,
            "no reallocation must occur within pre-allocated capacity"
        );
        assert_eq!(
            &buf.as_slice()[..128],
            &[0u8; 128],
            "first 128 bytes must be intact"
        );
        assert_eq!(
            &buf.as_slice()[128..],
            &[1u8; 128],
            "second 128 bytes must be intact"
        );
        assert!(is_aligned(&buf));

        // One more write past capacity forces a reallocation; new pointer must still be aligned.
        buf.extend_from_slice(&[2u8; 1]);
        assert_eq!(buf.len(), 257);
        assert_eq!(
            buf.as_slice()[256],
            2u8,
            "byte written after realloc must be correct"
        );
        assert!(buf.capacity() > 256);
        assert!(is_aligned(&buf));
    }

    #[test]
    fn test_alignment_survives_multiple_reallocations() {
        let mut buf = AlignedVec::with_capacity(0);
        let mut total_written = 0;

        // Sizes are intentionally not multiples of 64 — alignment must hold
        // regardless of how much data is written, not just when writes happen to align.
        for chunk_size in [1usize, 63, 65, 127, 509, 1021, 4093, 16381, 32771, 65533] {
            buf.extend_from_slice(&vec![0xABu8; chunk_size]);
            total_written += chunk_size;

            assert_eq!(buf.len(), total_written);
            assert!(
                is_aligned(&buf),
                "alignment lost after writing chunk_size={chunk_size}"
            );
        }
    }

    #[test]
    fn test_cross_chunk_boundary() {
        // Write data that spans multiple 64-byte chunks to exercise ensure_capacity growth.
        let mut buf = AlignedVec::with_capacity(0);
        let data: Vec<u8> = (0u8..=255).collect();
        buf.extend_from_slice(&data);
        assert_eq!(buf.len(), 256);
        assert_eq!(buf.as_slice(), data.as_slice());
        assert!(is_aligned(&buf));
        let buffer = Buffer::from(buf);
        assert_eq!(buffer.as_slice(), data.as_slice());
        assert_eq!(buffer.as_ptr().align_offset(64), 0);
    }

    #[test]
    fn test_empty_extend_and_clear_are_nondestructive() {
        let mut buf = AlignedVec::with_capacity(64);
        buf.extend_from_slice(b"hello");
        let cap = buf.capacity();

        buf.extend_from_slice(&[]);
        assert_eq!(buf.len(), 5, "empty extend must not change len");
        assert_eq!(buf.capacity(), cap, "empty extend must not change capacity");
        assert_eq!(buf.as_slice(), b"hello");
        assert!(is_aligned(&buf));

        buf.clear();
        assert_eq!(buf.len(), 0);
        assert_eq!(
            buf.as_mut_slice().len(),
            0,
            "as_mut_slice after clear must be empty"
        );
        assert_eq!(buf.capacity(), cap, "clear must not release allocation");
        assert!(is_aligned(&buf));
    }

    #[test]
    fn test_truncate_edge_cases() {
        // Successive truncates, chunk-boundary truncate, and stale-data overwrite.
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"hello world!");
        buf.truncate(10);
        buf.truncate(5);
        assert_eq!(buf.as_slice(), b"hello");
        buf.truncate(5); // same len — no-op
        buf.truncate(3);
        assert_eq!(buf.as_slice(), b"hel");
        assert!(is_aligned(&buf));

        // Truncate exactly at a 64-byte chunk boundary.
        let mut buf = AlignedVec::with_capacity(128);
        buf.extend_from_slice(&[0xAAu8; 64]);
        buf.extend_from_slice(&[0xBBu8; 64]);
        buf.truncate(64);
        assert_eq!(buf.len(), 64);
        assert_eq!(buf.as_slice(), &[0xAAu8; 64]);
        assert!(is_aligned(&buf));

        // Bytes past the truncation point must be overwritten, not leaked.
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(&[0xFFu8; 20]);
        buf.truncate(10);
        buf.extend_from_slice(&[0x00u8; 10]);
        assert_eq!(buf.len(), 20);
        assert_eq!(&buf.as_slice()[10..], &[0x00u8; 10]);
        assert!(is_aligned(&buf));
    }

    #[test]
    fn test_data_integrity_survives_realloc() {
        let mut buf = AlignedVec::with_capacity(128);
        let initial_capacity = buf.capacity();

        let pattern_a = vec![0xAAu8; 64];
        let pattern_b = vec![0xBBu8; 64];
        buf.extend_from_slice(&pattern_a);
        buf.extend_from_slice(&pattern_b);
        assert_eq!(buf.capacity(), initial_capacity, "no realloc yet");

        buf.extend_from_slice(&[0xCCu8]);
        assert!(buf.capacity() > initial_capacity, "must have reallocated");
        assert!(is_aligned(&buf));

        assert_eq!(
            &buf.as_slice()[..64],
            pattern_a.as_slice(),
            "first 64 bytes must survive realloc"
        );
        assert_eq!(
            &buf.as_slice()[64..128],
            pattern_b.as_slice(),
            "second 64 bytes must survive realloc"
        );
        assert_eq!(buf.as_slice()[128], 0xCC);
        assert_eq!(buf.len(), 129);
    }

    #[test]
    fn test_buffer_conversion_lifetime_and_bounds() {
        // Arc must keep the allocation alive after AlignedVec is dropped.
        let buffer = {
            let mut vec = AlignedVec::new();
            vec.extend_from_slice(b"lifetime test");
            assert!(is_aligned(&vec));
            Buffer::from(vec)
        };
        assert_eq!(buffer.as_slice(), b"lifetime test");
        assert_eq!(buffer.as_ptr().align_offset(64), 0);

        // Buffer must not expose bytes past filled_len even though the Vec holds more.
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(&[0xAAu8; 100]);
        buf.truncate(50);
        let buffer = Buffer::from(buf);
        assert_eq!(
            buffer.len(),
            50,
            "Buffer must not expose bytes past filled_len"
        );
        assert_eq!(buffer.as_slice(), &[0xAAu8; 50]);
        assert_eq!(buffer.as_ptr().align_offset(64), 0);
    }
}
