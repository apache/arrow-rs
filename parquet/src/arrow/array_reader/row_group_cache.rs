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
    /// Batch size used for cache entries
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
}
