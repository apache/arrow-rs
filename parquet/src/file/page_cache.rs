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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::VecDeque;

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
    pub fn new(file_id: u64, rg_idx: usize, column_idx: usize, page_offset: u64) -> Self {
        Self {
            file_id,
            rg_idx,
            column_idx, 
            page_offset,
        }
    }
}

pub struct PageCache {
    map: DashMap<PageCacheKey, (Arc<Page>, bool)>, // (page, is_protected)
    probationary_max: usize,
    protected_max: usize,

    probationary: Mutex<VecDeque<PageCacheKey>>,
    protected: Mutex<VecDeque<PageCacheKey>>,
}

impl PageCache {
    /// `probationary_ratio` in 0.0..1.0
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

    pub fn get(&self, key: &PageCacheKey) -> Option<Arc<Page>> {
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

    pub fn put(&self, key: PageCacheKey, page: Arc<Page>) {
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

    pub fn size(&self) -> usize {
        self.map.len()
    }

}

/// Global page cache instance
pub static PAGE_CACHE: std::sync::OnceLock<PageCache> = std::sync::OnceLock::new();

/// Initialize the global page cache with default parameters
pub fn init_page_cache_default() -> &'static PageCache {
    PAGE_CACHE.get_or_init(|| {
        PageCache::new(100, 0.25)
    })
}

/// Get a page from the global cache
pub fn get_cached_page(key: &PageCacheKey) -> Option<Arc<Page>> {
    let cache = PAGE_CACHE.get_or_init(|| {
        PageCache::new(100, 0.25)
    });
    cache.get(key)
}

/// Store a page in the global cache
pub fn store_cached_page(key: PageCacheKey, page: Arc<Page>) {
    let cache = PAGE_CACHE.get_or_init(|| {
        PageCache::new(100, 0.25)
    });
    cache.put(key, page)
}