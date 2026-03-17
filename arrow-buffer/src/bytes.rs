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

//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].
//! Note that this is a low-level functionality of this crate.

use core::slice;
use std::ptr::NonNull;
use std::{fmt::Debug, fmt::Formatter};

use crate::alloc::Deallocation;
use crate::buffer::dangling_ptr;

#[cfg(feature = "pool")]
use crate::pool::{MemoryPool, MemoryReservation};
#[cfg(feature = "pool")]
use std::sync::Mutex;

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
///
/// Note that this structure is an internal implementation detail of the
/// arrow-rs crate. While it has the same name and similar API as
/// [`bytes::Bytes`] it is not limited to rust's global allocator nor u8
/// alignment. It is possible to create a `Bytes` from `bytes::Bytes` using the
/// `From` implementation.
///
/// In the most common case, this buffer is allocated using [`alloc`](std::alloc::alloc)
/// with an alignment of [`ALIGNMENT`](crate::alloc::ALIGNMENT)
///
/// When the region is allocated by a different allocator, [Deallocation::Custom], this calls the
/// custom deallocator to deallocate the region when it is no longer needed.
///
pub struct Bytes {
    /// The raw pointer to be beginning of the region
    ptr: NonNull<u8>,

    /// The number of bytes visible to this region. This is always smaller than its capacity (when available).
    len: usize,

    /// how to deallocate this region
    deallocation: Deallocation,

    /// Memory reservation for tracking memory usage
    #[cfg(feature = "pool")]
    pub(super) reservation: Mutex<Option<Box<dyn MemoryReservation>>>,
}

impl Bytes {
    /// Takes ownership of an allocated memory region,
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `deallocation` - Type of allocation
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    #[inline]
    pub(crate) unsafe fn new(ptr: NonNull<u8>, len: usize, deallocation: Deallocation) -> Bytes {
        Bytes {
            ptr,
            len,
            deallocation,
            #[cfg(feature = "pool")]
            reservation: Mutex::new(None),
        }
    }

    fn as_slice(&self) -> &[u8] {
        self
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
    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub fn capacity(&self) -> usize {
        match self.deallocation {
            Deallocation::Standard(layout) => layout.size(),
            // we only know the size of the custom allocation
            // its underlying capacity might be larger
            Deallocation::Custom(_, size) => size,
        }
    }

    /// Register this [`Bytes`] with the provided [`MemoryPool`], replacing any prior reservation.
    #[cfg(feature = "pool")]
    pub fn claim(&self, pool: &dyn MemoryPool) {
        *self.reservation.lock().unwrap() = Some(pool.reserve(self.capacity()));
    }

    /// Resize the memory reservation of this buffer
    ///
    /// This is a no-op if this buffer doesn't have a reservation.
    #[cfg(feature = "pool")]
    fn resize_reservation(&self, new_size: usize) {
        let mut guard = self.reservation.lock().unwrap();
        if let Some(mut reservation) = guard.take() {
            // Resize the reservation
            reservation.resize(new_size);

            // Put it back
            *guard = Some(reservation);
        }
    }

    /// Try to reallocate the underlying memory region to a new size (smaller or larger).
    ///
    /// Only works for bytes allocated with the standard allocator.
    /// Returns `Err` if the memory was allocated with a custom allocator,
    /// or the call to `realloc` failed, for whatever reason.
    /// In case of `Err`, the [`Bytes`] will remain as it was (i.e. have the old size).
    pub fn try_realloc(&mut self, new_len: usize) -> Result<(), ()> {
        if let Deallocation::Standard(old_layout) = self.deallocation {
            if old_layout.size() == new_len {
                return Ok(()); // Nothing to do
            }

            if let Ok(new_layout) = std::alloc::Layout::from_size_align(new_len, old_layout.align())
            {
                let old_ptr = self.ptr.as_ptr();

                let new_ptr = match new_layout.size() {
                    0 => {
                        // SAFETY: Verified that old_layout.size != new_len (0)
                        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), old_layout) };
                        Some(dangling_ptr())
                    }
                    // SAFETY: the call to `realloc` is safe if all the following hold (from https://doc.rust-lang.org/stable/std/alloc/trait.GlobalAlloc.html#method.realloc):
                    // * `old_ptr` must be currently allocated via this allocator (guaranteed by the invariant/contract of `Bytes`)
                    // * `old_layout` must be the same layout that was used to allocate that block of memory (same)
                    // * `new_len` must be greater than zero
                    // * `new_len`, when rounded up to the nearest multiple of `layout.align()`, must not overflow `isize` (guaranteed by the success of `Layout::from_size_align`)
                    _ => NonNull::new(unsafe { std::alloc::realloc(old_ptr, old_layout, new_len) }),
                };

                if let Some(ptr) = new_ptr {
                    self.ptr = ptr;
                    self.len = new_len;
                    self.deallocation = Deallocation::Standard(new_layout);

                    #[cfg(feature = "pool")]
                    {
                        // Resize reservation
                        self.resize_reservation(new_len);
                    }

                    return Ok(());
                }
            }
        }

        Err(())
    }

    #[inline]
    pub(crate) fn deallocation(&self) -> &Deallocation {
        &self.deallocation
    }
}

// Deallocation is Send + Sync, repeating the bound here makes that refactoring safe
// The only field that is not automatically Send+Sync then is the NonNull ptr
unsafe impl Send for Bytes where Deallocation: Send {}
unsafe impl Sync for Bytes where Deallocation: Sync {}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            Deallocation::Standard(layout) => match layout.size() {
                0 => {} // Nothing to do
                _ => unsafe { std::alloc::dealloc(self.ptr.as_ptr(), *layout) },
            },
            // The automatic drop implementation will free the memory once the reference count reaches zero
            Deallocation::Custom(_allocation, _size) => (),
        }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Bytes {{ ptr: {:?}, len: {}, data: ", self.ptr, self.len,)?;

        f.debug_list().entries(self.iter()).finish()?;

        write!(f, " }}")
    }
}

impl From<bytes::Bytes> for Bytes {
    fn from(value: bytes::Bytes) -> Self {
        let len = value.len();
        Self {
            len,
            ptr: NonNull::new(value.as_ptr() as _).unwrap(),
            deallocation: Deallocation::Custom(std::sync::Arc::new(value), len),
            #[cfg(feature = "pool")]
            reservation: Mutex::new(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes() {
        let message = b"hello arrow";

        // we can create a Bytes from bytes::Bytes (created from slices)
        let c_bytes: bytes::Bytes = message.as_ref().into();
        let a_bytes: Bytes = c_bytes.into();
        assert_eq!(a_bytes.as_slice(), message);

        // we can create a Bytes from bytes::Bytes (created from Vec)
        let c_bytes: bytes::Bytes = bytes::Bytes::from(message.to_vec());
        let a_bytes: Bytes = c_bytes.into();
        assert_eq!(a_bytes.as_slice(), message);
    }

    #[cfg(feature = "pool")]
    mod pool_tests {
        use super::*;

        use crate::pool::TrackingMemoryPool;

        #[test]
        fn test_bytes_with_pool() {
            // Create a standard allocation
            let buffer = unsafe {
                let layout =
                    std::alloc::Layout::from_size_align(1024, crate::alloc::ALIGNMENT).unwrap();
                let ptr = std::alloc::alloc(layout);
                assert!(!ptr.is_null());

                Bytes::new(
                    NonNull::new(ptr).unwrap(),
                    1024,
                    Deallocation::Standard(layout),
                )
            };

            // Create a memory pool
            let pool = TrackingMemoryPool::default();
            assert_eq!(pool.used(), 0);

            // Reserve memory and assign to buffer. Claim twice.
            buffer.claim(&pool);
            assert_eq!(pool.used(), 1024);
            buffer.claim(&pool);
            assert_eq!(pool.used(), 1024);

            // Memory should be released when buffer is dropped
            drop(buffer);
            assert_eq!(pool.used(), 0);
        }

        #[test]
        fn test_bytes_drop_releases_pool() {
            let pool = TrackingMemoryPool::default();

            {
                // Create a buffer with pool
                let _buffer = unsafe {
                    let layout =
                        std::alloc::Layout::from_size_align(1024, crate::alloc::ALIGNMENT).unwrap();
                    let ptr = std::alloc::alloc(layout);
                    assert!(!ptr.is_null());

                    let bytes = Bytes::new(
                        NonNull::new(ptr).unwrap(),
                        1024,
                        Deallocation::Standard(layout),
                    );

                    bytes.claim(&pool);
                    bytes
                };

                assert_eq!(pool.used(), 1024);
            }

            // Buffer has been dropped, memory should be released
            assert_eq!(pool.used(), 0);
        }
    }
}
