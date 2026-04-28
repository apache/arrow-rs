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
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for calculating the size of various containers
pub trait HeapSize {
    /// Return the size of any bytes allocated on the heap by this object,
    /// including heap memory in those structures
    ///
    /// Note that the size of the type itself is not included in the result --
    /// instead, that size is added by the caller (e.g. container).
    fn heap_size(&self) -> usize;
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        let item_size = std::mem::size_of::<T>();
        // account for the contents of the Vec
        (self.capacity() * item_size) +
            // add any heap allocations by contents
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

impl HeapSize for bool {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for u8 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for i32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for i64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for f32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for f64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
