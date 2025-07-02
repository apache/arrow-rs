use arrow_array::ArrayRef;
use std::collections::HashMap;

/// Cache key that uniquely identifies a batch within a row group
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Column index in the row group
    pub column_idx: usize,
    /// Starting row ID for this batch
    pub row_id: usize,
}

/// Row group cache that stores decoded arrow arrays at batch granularity
///
/// This cache is designed to avoid duplicate decoding when the same column
/// appears in both filter predicates and output projection.
#[derive(Debug)]
pub struct RowGroupCache {
    /// Cache storage mapping (column_idx, row_id) -> ArrayRef
    cache: HashMap<CacheKey, ArrayRef>,
    /// Cache granularity
    batch_size: usize,
    /// Maximum cache size in bytes (None means unlimited)
    max_cache_size: Option<usize>,
    /// Current cache size in bytes
    current_cache_size: usize,
}

impl Default for RowGroupCache {
    fn default() -> Self {
        Self::new(1000, None)
    }
}

impl RowGroupCache {
    /// Creates a new empty row group cache
    pub fn new(batch_size: usize, max_cache_size: Option<usize>) -> Self {
        Self {
            cache: HashMap::new(),
            batch_size,
            max_cache_size,
            current_cache_size: 0,
        }
    }

    /// Inserts an array into the cache for the given column and starting row ID
    /// Returns true if the array was inserted, false if it would exceed the cache size limit
    pub fn insert(&mut self, column_idx: usize, row_id: usize, array: ArrayRef) -> bool {
        let array_size = array.get_array_memory_size();

        // Check if adding this array would exceed the cache size limit
        if let Some(max_size) = self.max_cache_size {
            if self.current_cache_size + array_size > max_size {
                return false; // Cache is full, don't insert
            }
        }

        let key = CacheKey { column_idx, row_id };

        let existing = self.cache.insert(key, array);
        assert!(existing.is_none());
        self.current_cache_size += array_size;
        true
    }

    /// Retrieves a cached array for the given column and row ID
    /// Returns None if not found in cache
    pub fn get(&self, column_idx: usize, row_id: usize) -> Option<ArrayRef> {
        let key = CacheKey { column_idx, row_id };
        self.cache.get(&key).cloned()
    }

    /// Gets the batch size for this cache
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Gets the maximum cache size in bytes (None means unlimited)
    pub fn max_cache_size(&self) -> Option<usize> {
        self.max_cache_size
    }

    /// Gets the current cache size in bytes
    pub fn current_cache_size(&self) -> usize {
        self.current_cache_size
    }

    /// Returns true if the cache has reached its maximum size
    pub fn is_full(&self) -> bool {
        match self.max_cache_size {
            Some(max_size) => self.current_cache_size >= max_size,
            None => false,
        }
    }

    /// Removes a cached array for the given column and row ID
    /// Returns true if the entry was found and removed, false otherwise
    pub fn remove(&mut self, column_idx: usize, row_id: usize) -> bool {
        let key = CacheKey { column_idx, row_id };
        if let Some(array) = self.cache.remove(&key) {
            self.current_cache_size -= array.get_array_memory_size();
            true
        } else {
            false
        }
    }

    /// Clears all entries from the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.current_cache_size = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use std::sync::Arc;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = RowGroupCache::new(1000, None);

        // Create test array
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        // Test insert and get
        assert!(cache.insert(0, 0, array.clone()));
        let retrieved = cache.get(0, 0);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 5);

        // Test miss
        let miss = cache.get(1, 0);
        assert!(miss.is_none());

        // Test different row_id
        let miss = cache.get(0, 1000);
        assert!(miss.is_none());
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = RowGroupCache::new(1000, None);

        // Create test arrays
        let array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

        // Insert arrays
        assert!(cache.insert(0, 0, array1.clone()));
        assert!(cache.insert(0, 1000, array2.clone()));
        assert!(cache.insert(1, 0, array1.clone()));

        // Verify they're there
        assert!(cache.get(0, 0).is_some());
        assert!(cache.get(0, 1000).is_some());
        assert!(cache.get(1, 0).is_some());

        // Remove one entry
        let removed = cache.remove(0, 0);
        assert!(removed);
        assert!(cache.get(0, 0).is_none());

        // Other entries should still be there
        assert!(cache.get(0, 1000).is_some());
        assert!(cache.get(1, 0).is_some());

        // Try to remove non-existent entry
        let not_removed = cache.remove(0, 0);
        assert!(!not_removed);

        // Remove remaining entries
        assert!(cache.remove(0, 1000));
        assert!(cache.remove(1, 0));

        // Cache should be empty
        assert!(cache.get(0, 1000).is_none());
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn test_cache_with_max_size() {
        // Create a cache with a very small max size
        let mut cache = RowGroupCache::new(1000, Some(100));

        assert_eq!(cache.max_cache_size(), Some(100));
        assert_eq!(cache.current_cache_size(), 0);
        assert!(!cache.is_full());

        // Create a test array
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let array_size = array.get_array_memory_size();

        // If array is larger than max cache size, insertion should fail
        if array_size > 100 {
            assert!(!cache.insert(0, 0, array.clone()));
            assert_eq!(cache.current_cache_size(), 0);
            assert!(cache.get(0, 0).is_none());
        } else {
            // If array fits, insertion should succeed
            assert!(cache.insert(0, 0, array.clone()));
            assert_eq!(cache.current_cache_size(), array_size);
            assert!(cache.get(0, 0).is_some());

            // Try to insert another array that would exceed the limit
            let array2: ArrayRef = Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10]));
            let array2_size = array2.get_array_memory_size();

            if array_size + array2_size > 100 {
                assert!(!cache.insert(0, 1000, array2.clone()));
                assert_eq!(cache.current_cache_size(), array_size);
                assert!(cache.get(0, 1000).is_none());
            }
        }
    }

    #[test]
    fn test_cache_unlimited_size() {
        let mut cache = RowGroupCache::new(1000, None);

        assert_eq!(cache.max_cache_size(), None);
        assert_eq!(cache.current_cache_size(), 0);
        assert!(!cache.is_full());

        // Should be able to insert multiple arrays without size limit
        for i in 0..10 {
            let array: ArrayRef = Arc::new(Int32Array::from(vec![i, i + 1, i + 2]));
            assert!(cache.insert(0, (i * 1000) as usize, array));
        }

        assert_eq!(cache.cache.len(), 10);
        assert!(!cache.is_full());
    }

    #[test]
    fn test_cache_size_tracking() {
        let mut cache = RowGroupCache::new(1000, None);

        let array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));

        let size1 = array1.get_array_memory_size();
        let size2 = array2.get_array_memory_size();

        // Insert first array
        assert!(cache.insert(0, 0, array1.clone()));
        assert_eq!(cache.current_cache_size(), size1);

        // Insert second array
        assert!(cache.insert(1, 0, array2.clone()));
        assert_eq!(cache.current_cache_size(), size1 + size2);

        // Replace first array with second array
        assert!(cache.insert(0, 0, array2.clone()));
        assert_eq!(cache.current_cache_size(), size2 + size2);

        // Remove one entry
        assert!(cache.remove(0, 0));
        assert_eq!(cache.current_cache_size(), size2);

        // Remove remaining entry
        assert!(cache.remove(1, 0));
        assert_eq!(cache.current_cache_size(), 0);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = RowGroupCache::new(1000, Some(1000));

        // Insert some arrays
        for i in 0..5 {
            let array: ArrayRef = Arc::new(Int32Array::from(vec![i, i + 1, i + 2]));
            assert!(cache.insert(0, (i * 1000) as usize, array));
        }

        assert!(cache.current_cache_size() > 0);
        assert_eq!(cache.cache.len(), 5);

        // Clear the cache
        cache.clear();

        assert_eq!(cache.current_cache_size(), 0);
        assert_eq!(cache.cache.len(), 0);
        assert!(!cache.is_full());
    }

    #[test]
    fn test_cache_full_detection() {
        // Create a cache that can hold approximately one small array
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array_size = array.get_array_memory_size();

        let mut cache = RowGroupCache::new(1000, Some(array_size));

        assert!(!cache.is_full());

        // Insert array - should succeed and make cache full
        assert!(cache.insert(0, 0, array.clone()));
        assert!(cache.is_full());

        // Try to insert another array - should fail
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        assert!(!cache.insert(1, 0, array2));

        // Cache should still be full with original array
        assert!(cache.is_full());
        assert!(cache.get(0, 0).is_some());
        assert!(cache.get(1, 0).is_none());
    }
}
