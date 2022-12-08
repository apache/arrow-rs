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

//! Defines memory-related functions, such as allocate/deallocate/reallocate memory
//! regions, cache and allocation alignments.

use std::alloc::{handle_alloc_error, Layout};
use std::fmt::{Debug, Formatter};
use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;

mod alignment;

pub use alignment::ALIGNMENT;

/// Returns an aligned non null pointer similar to [`NonNull::dangling`]
///
/// Note that the pointer value may potentially represent a valid pointer, which means
/// this must not be used as a "not yet initialized" sentinel value.
///
/// Types that lazily allocate must track initialization by some other means.
#[inline]
fn dangling_ptr() -> NonNull<u8> {
    // SAFETY: ALIGNMENT is a non-zero usize which is then casted
    // to a *mut T. Therefore, `ptr` is not null and the conditions for
    // calling new_unchecked() are respected.
    unsafe { NonNull::new_unchecked(ALIGNMENT as *mut u8) }
}

/// Allocates a cache-aligned memory region of `size` bytes with uninitialized values.
/// This is more performant than using [allocate_aligned_zeroed] when all bytes will have
/// an unknown or non-zero value and is semantically similar to `malloc`.
pub fn allocate_aligned(size: usize) -> NonNull<u8> {
    unsafe {
        if size == 0 {
            dangling_ptr()
        } else {
            let layout = Layout::from_size_align_unchecked(size, ALIGNMENT);
            let raw_ptr = std::alloc::alloc(layout);
            NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
        }
    }
}

/// Allocates a cache-aligned memory region of `size` bytes with `0` on all of them.
/// This is more performant than using [allocate_aligned] and setting all bytes to zero
/// and is semantically similar to `calloc`.
pub fn allocate_aligned_zeroed(size: usize) -> NonNull<u8> {
    unsafe {
        if size == 0 {
            dangling_ptr()
        } else {
            let layout = Layout::from_size_align_unchecked(size, ALIGNMENT);
            let raw_ptr = std::alloc::alloc_zeroed(layout);
            NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
        }
    }
}

/// # Safety
///
/// This function is unsafe because undefined behavior can result if the caller does not ensure all
/// of the following:
///
/// * ptr must denote a block of memory currently allocated via this allocator,
///
/// * size must be the same size that was used to allocate that block of memory,
pub unsafe fn free_aligned(ptr: NonNull<u8>, size: usize) {
    if size != 0 {
        std::alloc::dealloc(
            ptr.as_ptr() as *mut u8,
            Layout::from_size_align_unchecked(size, ALIGNMENT),
        );
    }
}

/// # Safety
///
/// This function is unsafe because undefined behavior can result if the caller does not ensure all
/// of the following:
///
/// * ptr must be currently allocated via this allocator,
///
/// * new_size must be greater than zero.
///
/// * new_size, when rounded up to the nearest multiple of [ALIGNMENT], must not overflow (i.e.,
/// the rounded value must be less than usize::MAX).
pub unsafe fn reallocate(
    ptr: NonNull<u8>,
    old_size: usize,
    new_size: usize,
) -> NonNull<u8> {
    if old_size == 0 {
        return allocate_aligned(new_size);
    }

    if new_size == 0 {
        free_aligned(ptr, old_size);
        return dangling_ptr();
    }

    let raw_ptr = std::alloc::realloc(
        ptr.as_ptr() as *mut u8,
        Layout::from_size_align_unchecked(old_size, ALIGNMENT),
        new_size,
    );
    NonNull::new(raw_ptr).unwrap_or_else(|| {
        handle_alloc_error(Layout::from_size_align_unchecked(new_size, ALIGNMENT))
    })
}

/// The owner of an allocation.
/// The trait implementation is responsible for dropping the allocations once no more references exist.
pub trait Allocation: RefUnwindSafe + Send + Sync {}

impl<T: RefUnwindSafe + Send + Sync> Allocation for T {}

/// Mode of deallocating memory regions
pub(crate) enum Deallocation {
    /// An allocation of the given capacity that needs to be deallocated using arrows's cache aligned allocator.
    /// See [allocate_aligned] and [free_aligned].
    Arrow(usize),
    /// An allocation from an external source like the FFI interface or a Rust Vec.
    /// Deallocation will happen
    Custom(Arc<dyn Allocation>),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Deallocation::Arrow(capacity) => {
                write!(f, "Deallocation::Arrow {{ capacity: {} }}", capacity)
            }
            Deallocation::Custom(_) => {
                write!(f, "Deallocation::Custom {{ capacity: unknown }}")
            }
        }
    }
}
