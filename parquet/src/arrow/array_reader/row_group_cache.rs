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
#[derive(Debug, Default)]
pub struct RowGroupCache {
    /// Cache storage mapping (column_idx, row_id) -> ArrayRef
    cache: HashMap<CacheKey, ArrayRef>,
    /// Cache granularity
    batch_size: usize,
}

impl RowGroupCache {
    /// Creates a new empty row group cache
    pub fn new(batch_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            batch_size,
        }
    }

    /// Inserts an array into the cache for the given column and starting row ID
    pub fn insert(&mut self, column_idx: usize, row_id: usize, array: ArrayRef) {
        let key = CacheKey { column_idx, row_id };
        self.cache.insert(key, array);
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

    /// Removes a cached array for the given column and row ID
    /// Returns true if the entry was found and removed, false otherwise
    pub fn remove(&mut self, column_idx: usize, row_id: usize) -> bool {
        let key = CacheKey { column_idx, row_id };
        self.cache.remove(&key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use std::sync::Arc;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = RowGroupCache::new(1000);

        // Create test array
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        // Test insert and get
        cache.insert(0, 0, array.clone());
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
        let mut cache = RowGroupCache::new(1000);

        // Create test arrays
        let array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

        // Insert arrays
        cache.insert(0, 0, array1.clone());
        cache.insert(0, 1000, array2.clone());
        cache.insert(1, 0, array1.clone());

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
}
