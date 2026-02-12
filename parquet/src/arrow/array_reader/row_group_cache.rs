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

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::collections::{HashMap, VecDeque};

/// Starting row ID for this batch
///
/// The `BatchID` is used to identify batches of rows within a row group.
///
/// The row_index in the id are relative to the rows being read from the
/// underlying column reader (which might already have a RowSelection applied)
///
/// The `BatchID` for any particular row is `row_index / batch_size`. The
/// integer division ensures that rows in the same batch share the same
/// the BatchID which can be calculated quickly from the row index
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct BatchID {
    pub val: usize,
}

/// Cache key that uniquely identifies a batch within a row group
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Column index in the row group
    pub column_idx: usize,
    /// Starting row ID for this batch
    pub batch_id: BatchID,
}

fn get_array_memory_size_for_cache(array: &ArrayRef) -> usize {
    match array.data_type() {
        // TODO: this is temporary workaround. It's very difficult to measure the actual memory usage of one StringViewArray,
        // because the underlying buffer is shared with multiple StringViewArrays.
        DataType::Utf8View => {
            use arrow_array::cast::AsArray;
            let array = array.as_string_view();
            array.len() * 16 + array.total_buffer_bytes_used() + std::mem::size_of_val(array)
        }
        _ => array.get_array_memory_size(),
    }
}

/// Row group cache that stores decoded arrow arrays at batch granularity
///
/// This cache is designed to avoid duplicate decoding when the same column
/// appears in both filter predicates and output projection.
#[derive(Debug)]
struct CacheEntry {
    array: ArrayRef,
    size: usize,
}

#[derive(Debug)]
pub struct RowGroupCache {
    /// Cache storage mapping (column_idx, row_id) -> ArrayRef
    cache: HashMap<CacheKey, CacheEntry>,
    /// Exact-selected cache stream per column (single-filter fast-path).
    ///
    /// Entries are queued in producer order and consumed FIFO by the matching
    /// projection reader, so no additional row filtering is required.
    selected_cache: HashMap<usize, VecDeque<CacheEntry>>,
    /// Cache granularity
    batch_size: usize,
    /// Maximum cache size in bytes
    max_cache_bytes: usize,
    /// Current cache size in bytes
    current_cache_size: usize,
}

impl RowGroupCache {
    /// Creates a new empty row group cache
    pub fn new(batch_size: usize, max_cache_bytes: usize) -> Self {
        Self {
            cache: HashMap::new(),
            selected_cache: HashMap::new(),
            batch_size,
            max_cache_bytes,
            current_cache_size: 0,
        }
    }

    /// Inserts an array into the cache for the given column and starting row ID
    /// Returns true if the array was inserted, false if it would exceed the cache size limit
    pub fn insert(&mut self, column_idx: usize, batch_id: BatchID, array: ArrayRef) -> bool {
        let array_size = get_array_memory_size_for_cache(&array);

        // Check if adding this array would exceed the cache size limit
        if self.current_cache_size + array_size > self.max_cache_bytes {
            return false; // Cache is full, don't insert
        }

        let key = CacheKey {
            column_idx,
            batch_id,
        };

        let existing = self.cache.insert(
            key,
            CacheEntry {
                array,
                size: array_size,
            },
        );
        assert!(existing.is_none());
        self.current_cache_size += array_size;
        true
    }

    /// Retrieves a cached array for the given column and row ID
    /// Returns None if not found in cache
    pub fn get(&self, column_idx: usize, batch_id: BatchID) -> Option<ArrayRef> {
        let key = CacheKey {
            column_idx,
            batch_id,
        };
        self.cache.get(&key).map(|entry| entry.array.clone())
    }

    /// Gets the batch size for this cache
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Removes a cached array for the given column and row ID
    /// Returns true if the entry was found and removed, false otherwise
    pub fn remove(&mut self, column_idx: usize, batch_id: BatchID) -> bool {
        let key = CacheKey {
            column_idx,
            batch_id,
        };
        if let Some(entry) = self.cache.remove(&key) {
            self.current_cache_size = self.current_cache_size.saturating_sub(entry.size);
            true
        } else {
            false
        }
    }

    /// Inserts selected rows for a cached column (ExactSelected mode)
    pub fn insert_selected(&mut self, column_idx: usize, array: ArrayRef) -> bool {
        let array_size = get_array_memory_size_for_cache(&array);
        if self.current_cache_size + array_size > self.max_cache_bytes {
            return false;
        }
        self.selected_cache
            .entry(column_idx)
            .or_default()
            .push_back(CacheEntry {
                array,
                size: array_size,
            });
        self.current_cache_size += array_size;
        true
    }

    /// Pops the next selected batch for a column (ExactSelected mode)
    pub fn pop_selected(&mut self, column_idx: usize) -> Option<ArrayRef> {
        let queue = self.selected_cache.get_mut(&column_idx)?;
        let entry = queue.pop_front()?;
        self.current_cache_size = self.current_cache_size.saturating_sub(entry.size);
        if queue.is_empty() {
            self.selected_cache.remove(&column_idx);
        }
        Some(entry.array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use std::sync::Arc;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = RowGroupCache::new(1000, usize::MAX);

        // Create test array
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        // Test insert and get
        let batch_id = BatchID { val: 0 };
        assert!(cache.insert(0, batch_id, array.clone()));
        let retrieved = cache.get(0, batch_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 5);

        // Test miss
        let miss = cache.get(1, batch_id);
        assert!(miss.is_none());

        // Test different row_id
        let miss = cache.get(0, BatchID { val: 1000 });
        assert!(miss.is_none());
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = RowGroupCache::new(1000, usize::MAX);

        // Create test arrays
        let array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

        // Insert arrays
        assert!(cache.insert(0, BatchID { val: 0 }, array1.clone()));
        assert!(cache.insert(0, BatchID { val: 1000 }, array2.clone()));
        assert!(cache.insert(1, BatchID { val: 0 }, array1.clone()));

        // Verify they're there
        assert!(cache.get(0, BatchID { val: 0 }).is_some());
        assert!(cache.get(0, BatchID { val: 1000 }).is_some());
        assert!(cache.get(1, BatchID { val: 0 }).is_some());

        // Remove one entry
        let removed = cache.remove(0, BatchID { val: 0 });
        assert!(removed);
        assert!(cache.get(0, BatchID { val: 0 }).is_none());

        // Other entries should still be there
        assert!(cache.get(0, BatchID { val: 1000 }).is_some());
        assert!(cache.get(1, BatchID { val: 0 }).is_some());

        // Try to remove non-existent entry
        let not_removed = cache.remove(0, BatchID { val: 0 });
        assert!(!not_removed);

        // Remove remaining entries
        assert!(cache.remove(0, BatchID { val: 1000 }));
        assert!(cache.remove(1, BatchID { val: 0 }));

        // Cache should be empty
        assert!(cache.get(0, BatchID { val: 1000 }).is_none());
        assert!(cache.get(1, BatchID { val: 0 }).is_none());
    }
}
