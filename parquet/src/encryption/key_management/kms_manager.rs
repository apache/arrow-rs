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

use crate::encryption::key_management::kms::{KmsClientFactory, KmsClientRef, KmsConnectionConfig};
use crate::errors::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Cache of key encryption keys (KEKs), keyed by their base64 encoded key id
pub(crate) type KekCache = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Manages caching KMS clients and KEK caches
pub(crate) struct KmsManager {
    kms_client_factory: Box<dyn KmsClientFactory>,
    kms_client_cache: ExpiringCache<ClientKey, KmsClientRef>,
    kek_caches: ExpiringCache<KekCacheKey, KekCache>,
}

impl KmsManager {
    pub fn new<T>(kms_client_factory: T) -> Self
    where
        T: KmsClientFactory + 'static,
    {
        Self {
            kms_client_factory: Box::new(kms_client_factory),
            kms_client_cache: ExpiringCache::new(),
            kek_caches: ExpiringCache::new(),
        }
    }

    pub fn get_client(
        &self,
        kms_connection_config: &Arc<KmsConnectionConfig>,
        cache_lifetime: Option<Duration>,
    ) -> Result<KmsClientRef> {
        self.clear_expired_entries(cache_lifetime);
        // Hold a read lock while the KMS is created to prevent a race condition where the token
        // could be updated after we read it but before the KMS client factory reads it.
        let key_access_token = kms_connection_config.read_key_access_token();
        let key = ClientKey::new(
            key_access_token.clone(),
            kms_connection_config.kms_instance_id().to_owned(),
        );
        self.kms_client_cache
            .get_or_create(key, cache_lifetime, || {
                self.kms_client_factory.create_client(kms_connection_config)
            })
    }

    pub fn get_kek_cache(
        &self,
        kms_connection_config: &Arc<KmsConnectionConfig>,
        cache_lifetime: Option<Duration>,
    ) -> KekCache {
        self.clear_expired_entries(cache_lifetime);
        let key = KekCacheKey::new(kms_connection_config.key_access_token().clone());
        self.kek_caches
            .get_or_create(key, cache_lifetime, || {
                Ok(Arc::new(Mutex::new(Default::default())))
            })
            .unwrap()
    }

    fn clear_expired_entries(&self, cleanup_interval: Option<Duration>) {
        if let Some(cleanup_interval) = cleanup_interval {
            self.kms_client_cache.clear_expired(cleanup_interval);
            self.kek_caches.clear_expired(cleanup_interval);
        }
    }
}

struct ExpiringCache<TKey, TValue> {
    cache: Mutex<HashMap<TKey, ExpiringCacheValue<TValue>>>,
    last_cleanup: Mutex<Instant>,
}

#[derive(Debug)]
struct ExpiringCacheValue<TValue> {
    value: TValue,
    expiration_time: Option<Instant>,
}

impl<TValue> ExpiringCacheValue<TValue> {
    pub fn new(value: TValue, cache_duration: Option<Duration>) -> Self {
        Self {
            value,
            expiration_time: cache_duration.map(|d| now() + d),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self.expiration_time {
            None => true,
            Some(expiration_time) => now() < expiration_time,
        }
    }
}

impl<TKey, TValue> ExpiringCache<TKey, TValue>
where
    TKey: Clone + Eq + Hash,
    TValue: Clone,
{
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::default()),
            last_cleanup: Mutex::new(now()),
        }
    }

    pub fn get_or_create<F>(
        &self,
        key: TKey,
        cache_lifetime: Option<Duration>,
        creator: F,
    ) -> Result<TValue>
    where
        F: FnOnce() -> Result<TValue>,
    {
        let mut cache = self.cache.lock().unwrap();
        let entry = cache.entry(key);
        match entry {
            Entry::Occupied(entry) if entry.get().is_valid() => Ok(entry.get().value.clone()),
            entry => {
                let value = creator()?;
                // Can change this to use entry.insert_entry once MSRV >= 1.83.0
                entry
                    .and_modify(|e| *e = ExpiringCacheValue::new(value.clone(), cache_lifetime))
                    .or_insert_with(|| ExpiringCacheValue::new(value.clone(), cache_lifetime));
                Ok(value)
            }
        }
    }

    /// Remove any expired entries from the cache
    pub fn clear_expired(&self, cleanup_interval: Duration) {
        {
            let mut last_cleanup = self.last_cleanup.lock().unwrap();
            if last_cleanup.elapsed() < cleanup_interval {
                return;
            }
            *last_cleanup = now();
        }

        let mut cache = self.cache.lock().unwrap();
        let to_remove: Vec<TKey> = cache
            .iter()
            .filter_map(|(k, v)| if v.is_valid() { None } else { Some(k.clone()) })
            .collect();
        for k in to_remove {
            cache.remove(&k);
        }
    }
}

/// Key used to cache KMS clients
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ClientKey {
    key_access_token: String,
    kms_instance_id: String,
}

impl ClientKey {
    pub fn new(key_access_token: String, kms_instance_id: String) -> Self {
        Self {
            key_access_token,
            kms_instance_id,
        }
    }
}

// Key used to cache KEK caches
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct KekCacheKey {
    key_access_token: String,
}

impl KekCacheKey {
    pub fn new(key_access_token: String) -> Self {
        Self { key_access_token }
    }
}

#[cfg(not(test))]
#[inline(always)]
fn now() -> Instant {
    Instant::now()
}

#[cfg(test)]
fn now() -> Instant {
    mock_time::now()
}

#[cfg(test)]
pub mod mock_time {
    //! Allows controlling the time returned by now() for testing cache behaviour
    use std::sync::{Mutex, MutexGuard, RwLock};
    use std::time::{Duration, Instant};

    static MOCK_NOW: RwLock<Option<Instant>> = RwLock::new(None);

    // Mutex to prevent multiple tests controlling time concurrently
    static CONTROL_MUTEX: Mutex<()> = Mutex::new(());

    pub struct TimeController {
        _control_guard: MutexGuard<'static, ()>,
    }

    impl TimeController {
        /// Advance the time returned by `now` by the specified duration
        pub fn advance(&self, duration: Duration) {
            let mut now_lock = MOCK_NOW.write().unwrap();
            if let Some(now) = &mut *now_lock {
                *now += duration;
            }
        }
    }

    /// Get the current time
    pub fn now() -> Instant {
        let now_lock = MOCK_NOW.read().unwrap();
        now_lock.unwrap_or_else(Instant::now)
    }

    /// Get a [`TimeController`] that can be used to advance the time in a test
    pub fn time_controller() -> TimeController {
        {
            let mut now_guard = MOCK_NOW.write().unwrap();
            *now_guard = Some(Instant::now());
        }
        let control_guard = CONTROL_MUTEX.lock().unwrap();
        TimeController {
            _control_guard: control_guard,
        }
    }
}
