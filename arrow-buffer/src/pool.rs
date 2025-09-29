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

//! This module contains traits for memory pool traits and an implementation
//! for tracking memory usage.
//!
//! The basic traits are [`MemoryPool`] and [`MemoryReservation`]. And default
//! implementation of [`MemoryPool`] is [`TrackingMemoryPool`]. Their relationship
//! is as follows:
//!
//! ```text
//!     (pool tracker)                        (resizable)           
//!  ┌──────────────────┐ fn reserve() ┌─────────────────────────┐
//!  │ trait MemoryPool │─────────────►│ trait MemoryReservation │
//!  └──────────────────┘              └─────────────────────────┘
//! ```

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A memory reservation within a [`MemoryPool`] that is freed on drop
pub trait MemoryReservation: Debug + Send + Sync {
    /// Returns the size of this reservation in bytes.
    fn size(&self) -> usize;

    /// Resize this reservation to a new size in bytes.
    fn resize(&mut self, new_size: usize);
}

/// A pool of memory that can be reserved and released.
///
/// This is used to accurately track memory usage when buffers are shared
/// between multiple arrays or other data structures.
///
/// For example, assume we have two arrays that share underlying buffer.
/// It's hard to tell how much memory is used by them because we can't
/// tell if the buffer is shared or not.
///
/// ```text
///       Array A           Array B    
///    ┌────────────┐    ┌────────────┐
///    │ slices...  │    │ slices...  │
///    │────────────│    │────────────│
///    │ Arc<Bytes> │    │ Arc<Bytes> │ (shared buffer)
///    └─────▲──────┘    └───────▲────┘
///          │                   │     
///          │       Bytes       │     
///          │  ┌─────────────┐  │     
///          │  │   data...   │  │     
///          │  │─────────────│  │     
///          └──│   Memory    │──┘   (tracked with a memory pool)  
///             │ Reservation │        
///             └─────────────┘        
/// ```
///
/// With a memory pool, we can count the memory usage by the shared buffer
/// directly.
pub trait MemoryPool: Debug + Send + Sync {
    /// Reserves memory from the pool. Infallible.
    ///
    /// Returns a reservation of the requested size.
    fn reserve(&self, size: usize) -> Box<dyn MemoryReservation>;

    /// Returns the current available memory in the pool.
    ///
    /// The pool may be overfilled, so this method might return a negative value.
    fn available(&self) -> isize;

    /// Returns the current used memory from the pool.
    fn used(&self) -> usize;

    /// Returns the maximum memory that can be reserved from the pool.
    fn capacity(&self) -> usize;
}

/// A simple [`MemoryPool`] that reports the total memory usage
#[derive(Debug, Default)]
pub struct TrackingMemoryPool(Arc<AtomicUsize>);

impl TrackingMemoryPool {
    /// Returns the total allocated size
    pub fn allocated(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl MemoryPool for TrackingMemoryPool {
    fn reserve(&self, size: usize) -> Box<dyn MemoryReservation> {
        self.0.fetch_add(size, Ordering::Relaxed);
        Box::new(Tracker {
            size,
            shared: Arc::clone(&self.0),
        })
    }

    fn available(&self) -> isize {
        isize::MAX - self.used() as isize
    }

    fn used(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn capacity(&self) -> usize {
        usize::MAX
    }
}

#[derive(Debug)]
struct Tracker {
    size: usize,
    shared: Arc<AtomicUsize>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.shared.fetch_sub(self.size, Ordering::Relaxed);
    }
}

impl MemoryReservation for Tracker {
    fn size(&self) -> usize {
        self.size
    }

    fn resize(&mut self, new: usize) {
        match self.size < new {
            true => self.shared.fetch_add(new - self.size, Ordering::Relaxed),
            false => self.shared.fetch_sub(self.size - new, Ordering::Relaxed),
        };
        self.size = new;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracking_memory_pool() {
        let pool = TrackingMemoryPool::default();

        // Reserve 512 bytes
        let reservation = pool.reserve(512);
        assert_eq!(reservation.size(), 512);
        assert_eq!(pool.used(), 512);
        assert_eq!(pool.available(), isize::MAX - 512);

        // Reserve another 256 bytes
        let reservation2 = pool.reserve(256);
        assert_eq!(reservation2.size(), 256);
        assert_eq!(pool.used(), 768);
        assert_eq!(pool.available(), isize::MAX - 768);

        // Test resize to increase
        let mut reservation_mut = reservation;
        reservation_mut.resize(600);
        assert_eq!(reservation_mut.size(), 600);
        assert_eq!(pool.used(), 856); // 600 + 256

        // Test resize to decrease
        reservation_mut.resize(400);
        assert_eq!(reservation_mut.size(), 400);
        assert_eq!(pool.used(), 656); // 400 + 256

        // Drop the first reservation
        drop(reservation_mut);
        assert_eq!(pool.used(), 256);

        // Drop the second reservation
        drop(reservation2);
        assert_eq!(pool.used(), 0);
    }
}
