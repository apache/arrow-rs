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
use moka::sync::Cache;
use std::fmt;
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

/// Configuration options for the page cache
pub enum PageCacheConfig {
    /// Moka-based caching with specified max size
    Moka {
        /// Maximum number of pages to cache
        max_pages: u64,
    },
}

/// Trait defining the interface for page cache strategies
pub trait PageCacheStrategy: Send + Sync {
    /// Get a page from the cache
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>>;

    /// Put a page into the cache
    fn put(&self, key: PageCacheKey, page: Arc<Page>);
}

/// Moka-based page cache implementation (thread-safe, production-ready)
pub struct MokaPageCache {
    cache: Cache<PageCacheKey, Arc<Page>>,
}

impl MokaPageCache {
    /// Create a new MokaPageCache with specified max number of pages
    pub fn new(max_pages: u64) -> Self {
        // MokaPageCache created with capacity: {} pages
        Self {
            cache: Cache::builder().max_capacity(max_pages).build(),
        }
    }
}

impl PageCacheStrategy for MokaPageCache {
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
        self.cache.get(key)
    }

    fn put(&self, key: PageCacheKey, page: Arc<Page>) {
        self.cache.insert(key, page);
    }
}

impl fmt::Debug for MokaPageCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MokaPageCache")
            .field("cached_pages", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .finish()
    }
}

/// Enum to hold different page cache strategies and do static dispatch
#[derive(Debug)]
pub enum PageCacheType {
    /// Moka-based caching (recommended for production)
    Moka(MokaPageCache),
}

/// Dispatch methods to the underlying cache strategy
impl PageCacheType {
    /// Get a page from the cache
    pub fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
        match self {
            PageCacheType::Moka(c) => c.get(key),
        }
    }

    /// Put a page into the cache
    pub fn put(&self, key: PageCacheKey, page: Arc<Page>) {
        match self {
            PageCacheType::Moka(c) => c.put(key, page),
        }
    }
}

/// High-level context struct that holds shared resources for Parquet operations
/// This allows cache sharing across different files, threads, and readers
#[derive(Debug, Clone)]
pub struct ParquetContext {
    /// Page cache shared across all readers using this context
    /// None for zero overhead when caching is disabled
    pub page_cache: Option<Arc<PageCacheType>>,
}

impl Default for ParquetContext {
    fn default() -> Self {
        Self {
            page_cache: Some(Arc::new(PageCacheType::Moka(MokaPageCache::new(100)))),
        }
    }
}

/// Shared default ParquetContext instance to ensure cache sharing across all readers
static DEFAULT_PARQUET_CONTEXT: OnceLock<ParquetContext> = OnceLock::new();

impl ParquetContext {
    /// Create a new ParquetContext with default configuration (100-page Moka cache)
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new ParquetContext with no caching (zero overhead)
    pub fn new_no_cache() -> Self {
        Self { page_cache: None }
    }

    /// Create a new ParquetContext with a specific page cache
    pub fn with_page_cache(page_cache: Option<Arc<PageCacheType>>) -> Self {
        Self { page_cache }
    }

    /// Set the global shared default ParquetContext
    ///
    /// This must be called before any readers are created to take effect.
    /// Returns `Ok(())` if successful, `Err(context)` if already initialized.
    pub fn set_shared_default(context: ParquetContext) -> Result<(), ParquetContext> {
        DEFAULT_PARQUET_CONTEXT.set(context)
    }

    /// Get the shared default ParquetContext instance
    /// This ensures all readers share the same cache when no explicit context is provided
    pub fn shared_default() -> &'static ParquetContext {
        DEFAULT_PARQUET_CONTEXT.get_or_init(|| ParquetContext::default())
    }
}
