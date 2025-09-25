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

//! Page-level cache for storing decompressed pages to avoid redundant I/O and decompression

use crate::column::page::Page;
use std::sync::{Arc, OnceLock};

/// Cache key that uniquely identifies a page within a file
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageCacheKey {
    /// Unique file identifier (derived from UUID or other unique metadata)
    pub file_id: u64,
    /// Row group index within the file
    pub rg_idx: usize,
    /// Column index within the row group  
    pub column_idx: usize,
    /// Page offset within the file
    pub page_offset: u64,
}

impl PageCacheKey {
    /// Create a new PageCacheKey
    pub fn new(file_id: u64, rg_idx: usize, column_idx: usize, page_offset: u64) -> Self {
        Self {
            file_id,
            rg_idx,
            column_idx,
            page_offset,
        }
    }
}

/// Trait defining the interface for page cache strategies
pub trait PageCacheStrategy: Send + Sync {
    /// Get a page from the cache
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>>;

    /// Put a page into the cache
    fn put(&self, key: PageCacheKey, page: Arc<Page>);
}

/// High-level context struct that holds shared resources for Parquet operations
/// This allows cache sharing across different files, threads, and readers
#[derive(Clone, Default)]
pub struct ParquetContext {
    /// Page cache shared across all readers using this context
    /// None for zero overhead when caching is disabled
    pub page_cache: Option<Arc<dyn PageCacheStrategy>>,
}

/// Shared default ParquetContext instance to ensure cache sharing across all readers
static DEFAULT_PARQUET_CONTEXT: OnceLock<ParquetContext> = OnceLock::new();

impl ParquetContext {
    /// Create a new ParquetContext with no caching
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the global shared default page cache strategy
    ///
    /// This must be called before any readers are created to take effect.
    /// Returns `Ok(())` if successful, `Err(context)` if already initialized.
    pub fn set_cache(page_cache: Option<Arc<dyn PageCacheStrategy>>) -> Result<(), ParquetContext> {
        DEFAULT_PARQUET_CONTEXT.set(Self { page_cache })
    }

    /// Get the shared ParquetContext instance
    pub fn get_cache() -> &'static ParquetContext {
        DEFAULT_PARQUET_CONTEXT.get_or_init(ParquetContext::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::page::Page;
    use std::collections::HashMap;

    /// Mock page cache for testing
    struct MockPageCache {
        storage: HashMap<PageCacheKey, Arc<Page>>,
    }

    impl MockPageCache {
        fn new() -> Self {
            Self {
                storage: HashMap::new(),
            }
        }
    }

    impl PageCacheStrategy for MockPageCache {
        fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
            self.storage.get(key).cloned()
        }

        fn put(&self, _key: PageCacheKey, _page: Arc<Page>) {
            // For testing purposes, we don't actually store anything
        }
    }

    #[test]
    fn test_pluggable_cache_interface() {
        let mock_cache = Arc::new(MockPageCache::new());
        let key = PageCacheKey::new(456, 1, 2, 2000);

        // Test that we can use the cache through the trait interface
        assert!(mock_cache.get(&key).is_none());

        let test_page = Arc::new(Page::DataPage {
            buf: bytes::Bytes::new(),
            num_values: 0,
            encoding: crate::basic::Encoding::PLAIN,
            def_level_encoding: crate::basic::Encoding::PLAIN,
            rep_level_encoding: crate::basic::Encoding::PLAIN,
            statistics: None,
        });

        mock_cache.put(key, test_page);
    }

    #[test]
    fn test_default_context_has_no_cache() {
        let default_context = ParquetContext::default();

        // Default context should have no cache (true zero overhead)
        assert!(default_context.page_cache.is_none());
    }
}
