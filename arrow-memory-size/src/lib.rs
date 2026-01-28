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

//! Memory size estimation utilities for Apache Arrow
//!
//! This crate provides the [`HeapSize`] trait for calculating heap memory usage
//! of data structures, with implementations for standard library types.
//!
//! For Arrow type implementations, see:
//! - [`arrow-buffer`](https://docs.rs/arrow-buffer) for buffer types
//! - [`arrow-array`](https://docs.rs/arrow-array) for array types
//!
//! # Example
//!
//! ```
//! use arrow_memory_size::HeapSize;
//!
//! let v: Vec<String> = vec!["hello".to_string(), "world".to_string()];
//! let heap_bytes = v.heap_size();
//! let total_bytes = v.total_size();
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/arrow-rs/refs/heads/main/docs/source/_static/images/Arrow-logo_hex_black-txt_transparent-bg.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/arrow-rs/refs/heads/main/docs/source/_static/images/Arrow-logo_hex_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

/// Trait for calculating the heap memory size of a value.
///
/// This trait provides methods for calculating how much heap memory
/// a data structure has allocated. This is useful for memory tracking,
/// cache management, and debugging memory usage.
///
/// # Semantics
///
/// - [`heap_size`](HeapSize::heap_size): Returns only the bytes allocated on the heap
///   by this value, not including the size of the value itself.
/// - [`total_size`](HeapSize::total_size): Returns the total memory footprint including
///   both the stack size of the value and its heap allocations.
///
/// # Example
///
/// ```
/// use arrow_memory_size::HeapSize;
///
/// let s = String::from("hello");
/// assert!(s.heap_size() >= 5); // At least 5 bytes for "hello"
/// assert!(s.total_size() >= s.heap_size() + std::mem::size_of::<String>());
/// ```
pub trait HeapSize {
    /// Return the size of any bytes allocated on the heap by this object,
    /// including heap memory in nested structures.
    ///
    /// Note that the size of the type itself is not included in the result --
    /// instead, that size is added by the caller (e.g. container) or via
    /// [`total_size`](HeapSize::total_size).
    fn heap_size(&self) -> usize;

    /// Return the total size of this object including heap allocations
    /// and the size of the object itself.
    fn total_size(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_size()
    }
}

// =============================================================================
// Standard library implementations
// =============================================================================

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        let item_size = std::mem::size_of::<T>();
        // Account for the Vec's buffer capacity
        (self.capacity() * item_size) +
        // Plus any heap allocations by the contents
        self.iter().map(|t| t.heap_size()).sum::<usize>()
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let capacity = self.capacity();
        if capacity == 0 {
            return 0;
        }

        // HashMap doesn't provide a way to get its heap size, so this is an approximation based on
        // the behavior of hashbrown::HashMap as at version 0.16.0, and may become inaccurate
        // if the implementation changes.
        let key_val_size = std::mem::size_of::<(K, V)>();
        // Overhead for the control tags group, which may be smaller depending on architecture
        let group_size = 16;
        // 1 byte of metadata stored per bucket.
        let metadata_size = 1;

        // Compute the number of buckets for the capacity. Based on hashbrown's capacity_to_buckets
        let buckets = if capacity < 15 {
            let min_cap = match key_val_size {
                0..=1 => 14,
                2..=3 => 7,
                _ => 3,
            };
            let cap = min_cap.max(capacity);
            if cap < 4 {
                4
            } else if cap < 8 {
                8
            } else {
                16
            }
        } else {
            (capacity.saturating_mul(8) / 7).next_power_of_two()
        };

        group_size
            + (buckets * (key_val_size + metadata_size))
            + self.keys().map(|k| k.heap_size()).sum::<usize>()
            + self.values().map(|v| v.heap_size()).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for HashSet<T> {
    fn heap_size(&self) -> usize {
        let capacity = self.capacity();
        if capacity == 0 {
            return 0;
        }

        // HashSet is implemented as HashMap<T, ()>, so we use similar approximation
        let item_size = std::mem::size_of::<(T, ())>();
        let group_size = 16;
        let metadata_size = 1;

        let buckets = if capacity < 15 {
            let min_cap = match item_size {
                0..=1 => 14,
                2..=3 => 7,
                _ => 3,
            };
            let cap = min_cap.max(capacity);
            if cap < 4 {
                4
            } else if cap < 8 {
                8
            } else {
                16
            }
        } else {
            (capacity.saturating_mul(8) / 7).next_power_of_two()
        };

        group_size
            + (buckets * (item_size + metadata_size))
            + self.iter().map(|item| item.heap_size()).sum::<usize>()
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for BTreeMap<K, V> {
    fn heap_size(&self) -> usize {
        if self.is_empty() {
            return 0;
        }

        // BTreeMap stores entries in nodes. This is an approximation.
        // Each node has some overhead for child pointers and length tracking.
        // The B parameter is typically 6 for BTreeMap, meaning nodes can hold 2B-1 = 11 entries.
        let entry_size = std::mem::size_of::<(K, V)>();
        let len = self.len();

        // Approximate: each entry + some per-node overhead
        // Nodes are approximately 2/3 full on average after random insertions
        let node_overhead_per_entry = 16; // Approximate overhead for pointers and metadata

        (len * (entry_size + node_overhead_per_entry))
            + self.keys().map(|k| k.heap_size()).sum::<usize>()
            + self.values().map(|v| v.heap_size()).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for BTreeSet<T> {
    fn heap_size(&self) -> usize {
        if self.is_empty() {
            return 0;
        }

        // BTreeSet is implemented as BTreeMap<T, ()>
        let entry_size = std::mem::size_of::<(T, ())>();
        let len = self.len();
        let node_overhead_per_entry = 16;

        (len * (entry_size + node_overhead_per_entry))
            + self.iter().map(|item| item.heap_size()).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_size(&self) -> usize {
        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * std::mem::size_of::<usize>() + std::mem::size_of::<T>() + self.as_ref().heap_size()
    }
}

impl HeapSize for Arc<dyn HeapSize> {
    fn heap_size(&self) -> usize {
        2 * std::mem::size_of::<usize>()
            + std::mem::size_of_val(self.as_ref())
            + self.as_ref().heap_size()
    }
}

impl<T: HeapSize> HeapSize for Box<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + self.as_ref().heap_size()
    }
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map(|inner| inner.heap_size()).unwrap_or(0)
    }
}

impl HeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

// Primitive types - no heap allocations

impl HeapSize for bool {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u8 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u16 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u64 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u128 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i8 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i16 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i64 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i128 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for isize {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for f32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for f64 {
    fn heap_size(&self) -> usize {
        0
    }
}

// =============================================================================
// Tuple implementations (up to 12 elements)
// =============================================================================

impl HeapSize for () {
    fn heap_size(&self) -> usize {
        0
    }
}

impl<T0: HeapSize> HeapSize for (T0,) {
    fn heap_size(&self) -> usize {
        self.0.heap_size()
    }
}

impl<T0: HeapSize, T1: HeapSize> HeapSize for (T0, T1) {
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size()
    }
}

impl<T0: HeapSize, T1: HeapSize, T2: HeapSize> HeapSize for (T0, T1, T2) {
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size() + self.2.heap_size()
    }
}

impl<T0: HeapSize, T1: HeapSize, T2: HeapSize, T3: HeapSize> HeapSize for (T0, T1, T2, T3) {
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size() + self.2.heap_size() + self.3.heap_size()
    }
}

impl<T0: HeapSize, T1: HeapSize, T2: HeapSize, T3: HeapSize, T4: HeapSize> HeapSize
    for (T0, T1, T2, T3, T4)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
    }
}

impl<T0: HeapSize, T1: HeapSize, T2: HeapSize, T3: HeapSize, T4: HeapSize, T5: HeapSize> HeapSize
    for (T0, T1, T2, T3, T4, T5)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
    T7: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6, T7)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
            + self.7.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
    T7: HeapSize,
    T8: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6, T7, T8)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
            + self.7.heap_size()
            + self.8.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
    T7: HeapSize,
    T8: HeapSize,
    T9: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
            + self.7.heap_size()
            + self.8.heap_size()
            + self.9.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
    T7: HeapSize,
    T8: HeapSize,
    T9: HeapSize,
    T10: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
            + self.7.heap_size()
            + self.8.heap_size()
            + self.9.heap_size()
            + self.10.heap_size()
    }
}

impl<
    T0: HeapSize,
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
    T4: HeapSize,
    T5: HeapSize,
    T6: HeapSize,
    T7: HeapSize,
    T8: HeapSize,
    T9: HeapSize,
    T10: HeapSize,
    T11: HeapSize,
> HeapSize for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
            + self.1.heap_size()
            + self.2.heap_size()
            + self.3.heap_size()
            + self.4.heap_size()
            + self.5.heap_size()
            + self.6.heap_size()
            + self.7.heap_size()
            + self.8.heap_size()
            + self.9.heap_size()
            + self.10.heap_size()
            + self.11.heap_size()
    }
}

// =============================================================================
// Array implementation
// =============================================================================

impl<T: HeapSize, const N: usize> HeapSize for [T; N] {
    fn heap_size(&self) -> usize {
        self.iter().map(|item| item.heap_size()).sum()
    }
}

// =============================================================================
// Synchronization primitives
// =============================================================================

impl<T: HeapSize> HeapSize for Mutex<T> {
    fn heap_size(&self) -> usize {
        // Try to lock; if poisoned or would block, return 0 as best effort
        match self.try_lock() {
            Ok(guard) => guard.heap_size(),
            Err(_) => 0,
        }
    }
}

impl<T: HeapSize> HeapSize for RwLock<T> {
    fn heap_size(&self) -> usize {
        // Try to read lock; if poisoned or would block, return 0 as best effort
        match self.try_read() {
            Ok(guard) => guard.heap_size(),
            Err(_) => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_heap_size() {
        let s = String::from("hello");
        assert!(s.heap_size() >= 5);
    }

    #[test]
    fn test_vec_heap_size() {
        let v: Vec<i32> = vec![1, 2, 3, 4, 5];
        assert!(v.heap_size() >= 5 * std::mem::size_of::<i32>());
    }

    #[test]
    fn test_nested_vec_heap_size() {
        let v: Vec<String> = vec!["hello".to_string(), "world".to_string()];
        let size = v.heap_size();
        // Should include Vec buffer + String heap allocations
        assert!(size >= 10); // "hello" + "world" = 10 chars minimum
    }

    #[test]
    fn test_option_heap_size() {
        let some: Option<String> = Some("hello".to_string());
        let none: Option<String> = None;

        assert!(some.heap_size() >= 5);
        assert_eq!(none.heap_size(), 0);
    }

    #[test]
    fn test_box_heap_size() {
        let b = Box::new("hello".to_string());
        let size = b.heap_size();
        // Should include String struct size + string data
        assert!(size >= std::mem::size_of::<String>() + 5);
    }

    #[test]
    fn test_primitive_heap_size() {
        assert_eq!(42i32.heap_size(), 0);
        assert_eq!(3.14f64.heap_size(), 0);
        assert_eq!(true.heap_size(), 0);
    }

    #[test]
    fn test_total_size() {
        let s = String::from("hello");
        let total = s.total_size();
        assert_eq!(total, std::mem::size_of::<String>() + s.heap_size());
    }

    #[test]
    fn test_tuple_heap_size() {
        let t0: () = ();
        assert_eq!(t0.heap_size(), 0);

        let t1 = ("hello".to_string(),);
        assert!(t1.heap_size() >= 5);

        let t2 = ("hello".to_string(), "world".to_string());
        assert!(t2.heap_size() >= 10);

        let t3 = (1i32, "hello".to_string(), vec![1u8, 2, 3]);
        assert!(t3.heap_size() >= 5 + 3); // string + vec
    }

    #[test]
    fn test_array_heap_size() {
        let arr: [i32; 5] = [1, 2, 3, 4, 5];
        assert_eq!(arr.heap_size(), 0); // primitives have no heap

        let arr: [String; 2] = ["hello".to_string(), "world".to_string()];
        assert!(arr.heap_size() >= 10);

        let arr: [Vec<u8>; 3] = [vec![1, 2], vec![3, 4, 5], vec![6]];
        assert!(arr.heap_size() >= 6); // at least 6 bytes for elements
    }

    #[test]
    fn test_mutex_heap_size() {
        let m = Mutex::new("hello".to_string());
        assert!(m.heap_size() >= 5);

        let m = Mutex::new(vec![1i32, 2, 3]);
        assert!(m.heap_size() >= 3 * std::mem::size_of::<i32>());
    }

    #[test]
    fn test_rwlock_heap_size() {
        let rw = RwLock::new("hello".to_string());
        assert!(rw.heap_size() >= 5);

        let rw = RwLock::new(vec![1i32, 2, 3]);
        assert!(rw.heap_size() >= 3 * std::mem::size_of::<i32>());
    }
}
