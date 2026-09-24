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

use std::alloc::Layout;
use std::ptr::NonNull;

use super::immutable::Buffer;
use crate::alloc::Deallocation;
use crate::bytes::Bytes;

/// 64-byte-aligned unit of storage. `Vec<Chunk>` guarantees that every
/// (re)allocation starts on a 64-byte boundary (cache line / AVX-512).
#[repr(align(64))]
#[derive(Clone, Copy)]
struct Chunk([u8; 64]);

const CHUNK: usize = size_of::<Chunk>();

/// A write-only, 64-byte-aligned byte buffer backed by `Vec<Chunk>`.
///
/// Alignment is guaranteed on every allocation and reallocation because
/// `Vec<Chunk>` derives its alignment from `Chunk`'s `#[repr(align(64))]`.
/// Only bytes written via [`extend_from_slice`](Self::extend_from_slice) are
/// visible; the rest of the allocation is uninitialized.
///
/// Converts into a [`Buffer`] zero-copy via [`Buffer::from`].
pub struct AlignedVec {
    raw: Vec<Chunk>,
    len: usize,
}

impl AlignedVec {
    /// Creates an empty `AlignedVec` with no allocation.
    pub fn new() -> Self {
        Self { raw: Vec::new(), len: 0 }
    }

    /// Creates an `AlignedVec` pre-allocated for at least `capacity` bytes.
    ///
    /// Capacity is rounded up to the next multiple of 64.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            raw: Vec::with_capacity(capacity.div_ceil(CHUNK)),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.raw.capacity() * CHUNK
    }

    /// Appends `data`, reallocating if necessary.
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        if new_len > self.raw.capacity() * CHUNK {
            self.grow(new_len);
        }
        // SAFETY: grow (or with_capacity) ensures the allocation covers [0, capacity*CHUNK).
        // raw.len() is kept at 0 so Vec's drop never touches the written bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.raw.as_mut_ptr().cast::<u8>().add(self.len),
                data.len(),
            );
        }
        self.len = new_len;
    }

    #[cold]
    fn grow(&mut self, needed: usize) {
        let current = self.raw.capacity();
        let needed_chunks = needed.div_ceil(CHUNK);
        // Double the current capacity (or use needed_chunks if larger) to amortize reallocations.
        let new_chunks = needed_chunks.max(current * 2).max(1);
        self.raw.reserve(new_chunks - current);
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for self.len bytes.
        unsafe { std::slice::from_raw_parts(self.raw.as_ptr().cast(), self.len) }
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl Default for AlignedVec {
    fn default() -> Self {
        Self::new()
    }
}

impl From<AlignedVec> for Buffer {
    fn from(vec: AlignedVec) -> Self {
        if vec.len == 0 {
            return Buffer::from(&[] as &[u8]);
        }
        let filled_len = vec.len;
        let mut raw = vec.raw;
        let capacity = raw.capacity();
        let ptr = NonNull::new(raw.as_mut_ptr().cast::<u8>())
            .expect("non-null when capacity > 0");
        let layout = Layout::array::<Chunk>(capacity).expect("valid layout");
        std::mem::forget(raw);
        // SAFETY: ptr is the Vec<Chunk> allocation, 64-byte aligned, valid for at least filled_len bytes.
        let bytes = unsafe { Bytes::new(ptr, filled_len, Deallocation::Standard(layout)) };
        Buffer::from(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_aligned(p: *const u8) -> bool {
        p.align_offset(64) == 0
    }

    #[test]
    fn test_write_and_read() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"foo");
        buf.extend_from_slice(b"bar");
        assert_eq!(buf.as_slice(), b"foobar");
        assert_eq!(buf.len(), 6);
    }

    #[test]
    fn test_alignment() {
        for capacity in [0, 1, 63, 64, 65, 128, 129] {
            let mut buf = AlignedVec::with_capacity(capacity);
            buf.extend_from_slice(b"x");
            assert!(is_aligned(buf.as_slice().as_ptr()), "capacity={capacity}");
        }
    }

    #[test]
    fn test_alignment_survives_realloc() {
        let mut buf = AlignedVec::new();
        for chunk in [1usize, 63, 65, 127, 509, 4093] {
            buf.extend_from_slice(&vec![0xABu8; chunk]);
            assert!(is_aligned(buf.as_slice().as_ptr()), "chunk={chunk}");
        }
    }

    #[test]
    fn test_clear_and_truncate() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"hello world");
        buf.truncate(5);
        assert_eq!(buf.as_slice(), b"hello");
        let cap = buf.capacity();
        buf.clear();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), cap);
    }

    #[test]
    fn test_into_buffer() {
        let mut buf = AlignedVec::new();
        buf.extend_from_slice(b"aligned");
        let b = Buffer::from(buf);
        assert_eq!(b.as_slice(), b"aligned");
        assert!(is_aligned(b.as_ptr()));
    }

    #[test]
    fn test_into_buffer_empty() {
        let b = Buffer::from(AlignedVec::new());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn test_pointer_stable_within_capacity() {
        let mut buf = AlignedVec::with_capacity(256);
        buf.extend_from_slice(&[0u8; 128]);
        let ptr = buf.as_slice().as_ptr();
        buf.extend_from_slice(&[1u8; 128]);
        assert_eq!(buf.as_slice().as_ptr(), ptr);
        assert_eq!(buf.len(), 256);
    }

    #[test]
    fn test_data_integrity_after_realloc() {
        let mut buf = AlignedVec::with_capacity(64);
        buf.extend_from_slice(&[0xAAu8; 64]);
        buf.extend_from_slice(&[0xBBu8; 64]);
        assert_eq!(&buf.as_slice()[..64], &[0xAAu8; 64]);
        assert_eq!(&buf.as_slice()[64..], &[0xBBu8; 64]);
        assert!(is_aligned(buf.as_slice().as_ptr()));
    }
}
