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
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

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
    /// No caching
    NoCache,
    /// Two-queue caching with specified max size and probationary ratio
    TwoQueue {
        /// Maximum number of pages to cache
        max_pages: usize,
        /// Ratio of probationary pages
        probationary_ratio: f64,
    },
}

/// Trait defining the interface for page cache strategies
pub trait PageCacheStrategy: Send + Sync {
    /// Get a page from the cache
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>>;

    /// Put a page into the cache
    fn put(&self, key: PageCacheKey, page: Arc<Page>);
}

/// Two-queue page cache implementation
pub struct TwoQueuePageCache {
    /// Main storage mapping cache keys to (page, is_protected) tuples
    map: DashMap<PageCacheKey, (Arc<Page>, bool)>,
    /// Maximum number of entries in the probationary queue
    probationary_max: usize,
    /// Maximum number of entries in the protected queue
    protected_max: usize,

    /// Queue tracking probationary pages in LRU order
    probationary: Mutex<VecDeque<PageCacheKey>>,
    /// Queue tracking protected pages in LRU order
    protected: Mutex<VecDeque<PageCacheKey>>,
}

impl TwoQueuePageCache {
    /// Create a new TwoQueuePageCache with specified max size and probationary ratio
    pub fn new(max_pages: usize, probationary_ratio: f64) -> Self {
        let probationary_max = ((max_pages as f64) * probationary_ratio).ceil() as usize;
        let protected_max = max_pages - probationary_max;
        Self {
            map: DashMap::new(),
            probationary_max,
            protected_max,
            probationary: Mutex::new(VecDeque::new()),
            protected: Mutex::new(VecDeque::new()),
        }
    }

    /// Evict pages to maintain probationary/protected size ratios
    fn evict_if_needed(&self) {
        // Evict from probationary first
        let mut prob = self.probationary.lock().unwrap();
        while prob.len() > self.probationary_max {
            if let Some(oldest) = prob.pop_front() {
                self.map.remove(&oldest);
            }
        }
        drop(prob);

        // Evict from protected if necessary
        let mut prot = self.protected.lock().unwrap();
        while prot.len() > self.protected_max {
            if let Some(oldest) = prot.pop_front() {
                self.map.remove(&oldest);
            }
        }
    }
}

impl PageCacheStrategy for TwoQueuePageCache {
    /// Get a page from the cache
    fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
        if let Some(mut entry) = self.map.get_mut(key) {
            let (page, is_protected) = &mut *entry;
            let page_clone = page.clone();

            if !*is_protected {
                // Promote to protected
                *is_protected = true;

                let mut prob = self.probationary.lock().unwrap();
                prob.retain(|k| k != key);
                drop(prob);

                let mut prot = self.protected.lock().unwrap();
                prot.retain(|k| k != key);
                prot.push_back(key.clone());
                drop(prot);

                // Release the DashMap entry before calling evict_if_needed to avoid deadlock
                drop(entry);
                // Ensure protected does not exceed limit
                self.evict_if_needed();
            } else {
                // Refresh protected LRU
                let mut prot = self.protected.lock().unwrap();
                prot.retain(|k| k != key);
                prot.push_back(key.clone());
            }

            Some(page_clone)
        } else {
            None
        }
    }

    /// Put a page into the cache
    fn put(&self, key: PageCacheKey, page: Arc<Page>) {
        if self.map.contains_key(&key) {
            // Reset to probationary
            self.map.insert(key.clone(), (page, false));
            {
                let mut prob = self.probationary.lock().unwrap();
                prob.retain(|k| k != &key);
                prob.push_back(key.clone());
            }
            // Remove from protected if it was there
            {
                let mut prot = self.protected.lock().unwrap();
                prot.retain(|k| k != &key);
            }
            self.evict_if_needed();
            return;
        }

        // Insert into probationary
        self.map.insert(key.clone(), (page, false));
        {
            let mut prob = self.probationary.lock().unwrap();
            prob.retain(|k| k != &key);
            prob.push_back(key);
        }

        self.evict_if_needed();
    }
}

/// No caching strategy (no-op)
pub struct NoPageCache;

/// No-op implementation of PageCacheStrategy
impl PageCacheStrategy for NoPageCache {
    fn get(&self, _key: &PageCacheKey) -> Option<Arc<Page>> {
        None
    }

    fn put(&self, _key: PageCacheKey, _page: Arc<Page>) {
        // No-op
    }
}

/// Enum to hold different page cache strategies and do static dispatch
pub enum PageCacheType {
    /// No caching
    NoCache(NoPageCache),
    /// Two-queue caching
    TwoQueue(TwoQueuePageCache),
}

/// Dispatch methods to the underlying cache strategy
impl PageCacheType {
    /// Get a page from the cache
    pub fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
        match self {
            PageCacheType::NoCache(c) => c.get(key),
            PageCacheType::TwoQueue(c) => c.get(key),
        }
    }

    /// Put a page into the cache
    pub fn put(&self, key: PageCacheKey, page: Arc<Page>) {
        match self {
            PageCacheType::NoCache(c) => c.put(key, page),
            PageCacheType::TwoQueue(c) => c.put(key, page),
        }
    }
}
