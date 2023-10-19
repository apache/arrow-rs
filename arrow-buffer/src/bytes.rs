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

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
///
/// This structs' API is inspired by the `bytes::Bytes`, but it is not limited to using rust's
/// global allocator nor u8 alignment.
///
/// In the most common case, this buffer is allocated using [`alloc`](std::alloc::alloc)
/// with an alignment of [`ALIGNMENT`](crate::alloc::ALIGNMENT)
///
/// When the region is allocated by a different allocator, [Deallocation::Custom], this calls the
/// custom deallocator to deallocate the region when it is no longer needed.
pub struct Bytes {
    /// The raw pointer to be beginning of the region
    ptr: NonNull<u8>,

    /// The number of bytes visible to this region. This is always smaller than its capacity (when available).
    len: usize,

    /// how to deallocate this region
    deallocation: Deallocation,
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
            // we cannot determine this in general,
            // and thus we state that this is externally-owned memory
            Deallocation::Custom(_) => 0,
        }
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
            Deallocation::Custom(_allocation) => (),
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
        Self {
            len: value.len(),
            ptr: NonNull::new(value.as_ptr() as _).unwrap(),
            deallocation: Deallocation::Custom(std::sync::Arc::new(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes() {
        let bytes = bytes::Bytes::from(vec![1, 2, 3, 4]);
        let arrow_bytes: Bytes = bytes.clone().into();

        assert_eq!(bytes.as_ptr(), arrow_bytes.as_ptr());

        drop(bytes);
        drop(arrow_bytes);

        let _ = Bytes::from(bytes::Bytes::new());
    }
}
