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
use std::collections::HashMap;

/// A cached batch containing array data and optional definition/repetition levels
#[derive(Debug, Clone)]
pub struct CachedBatch {
    /// The decoded array data
    pub array: ArrayRef,
    /// Definition levels for nullability tracking in nested structures
    pub def_levels: Option<Vec<i16>>,
    /// Repetition levels for list boundary tracking
    pub rep_levels: Option<Vec<i16>>,
}

impl CachedBatch {
    /// Creates a new cached batch with just array data (no levels)
    #[cfg(test)]
    pub fn new(array: ArrayRef) -> Self {
        Self {
            array,
            def_levels: None,
            rep_levels: None,
        }
    }

    /// Creates a new cached batch with array data and levels
    pub fn with_levels(
        array: ArrayRef,
        def_levels: Option<Vec<i16>>,
        rep_levels: Option<Vec<i16>>,
    ) -> Self {
        Self {
            array,
            def_levels,
            rep_levels,
        }
    }

    /// Returns the memory size of this cached batch
    fn memory_size(&self) -> usize {
        let array_size = get_array_memory_size_for_cache(&self.array);
        let def_size = self
            .def_levels
            .as_ref()
            .map(|l| l.capacity() * std::mem::size_of::<i16>())
            .unwrap_or(0);
        let rep_size = self
            .rep_levels
            .as_ref()
            .map(|l| l.capacity() * std::mem::size_of::<i16>())
            .unwrap_or(0);
        array_size + def_size + rep_size
    }
}

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
pub struct RowGroupCache {
    /// Cache storage mapping (column_idx, row_id) -> CachedBatch
    cache: HashMap<CacheKey, CachedBatch>,
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
            batch_size,
            max_cache_bytes,
            current_cache_size: 0,
        }
    }

    /// Inserts a batch into the cache for the given column and starting row ID
    /// Returns true if the batch was inserted, false if it would exceed the cache size limit
    pub fn insert(&mut self, column_idx: usize, batch_id: BatchID, batch: CachedBatch) -> bool {
        let batch_size = batch.memory_size();

        // Check if adding this batch would exceed the cache size limit
        if self.current_cache_size + batch_size > self.max_cache_bytes {
            return false; // Cache is full, don't insert
        }

        let key = CacheKey {
            column_idx,
            batch_id,
        };

        let existing = self.cache.insert(key, batch);
        assert!(existing.is_none());
        self.current_cache_size += batch_size;
        true
    }

    /// Retrieves a cached batch for the given column and row ID
    /// Returns None if not found in cache
    pub fn get(&self, column_idx: usize, batch_id: BatchID) -> Option<CachedBatch> {
        let key = CacheKey {
            column_idx,
            batch_id,
        };
        self.cache.get(&key).cloned()
    }

    /// Gets the batch size for this cache
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Removes a cached batch for the given column and row ID
    /// Returns true if the entry was found and removed, false otherwise
    pub fn remove(&mut self, column_idx: usize, batch_id: BatchID) -> bool {
        let key = CacheKey {
            column_idx,
            batch_id,
        };
        if let Some(batch) = self.cache.remove(&key) {
            self.current_cache_size -= batch.memory_size();
            true
        } else {
            false
        }
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
        let batch = CachedBatch::new(array);

        // Test insert and get
        let batch_id = BatchID { val: 0 };
        assert!(cache.insert(0, batch_id, batch));
        let retrieved = cache.get(0, batch_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().array.len(), 5);

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

        // Insert batches
        assert!(cache.insert(0, BatchID { val: 0 }, CachedBatch::new(array1.clone())));
        assert!(cache.insert(0, BatchID { val: 1000 }, CachedBatch::new(array2.clone())));
        assert!(cache.insert(1, BatchID { val: 0 }, CachedBatch::new(array1.clone())));

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
