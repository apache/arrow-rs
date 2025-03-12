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

type ClientFactory = Mutex<Box<dyn KmsClientFactory>>;

/// Cache of key encryption keys, keyed by their base64 encoded key id
pub type KekCache = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Manages caching the KMS and allowing interaction with it
pub struct KmsManager {
    kms_client_factory: ClientFactory,
    kms_client_cache: ExpiringCache<ClientKey, KmsClientRef>,
    kek_caches: ExpiringCache<KekCacheKey, KekCache>,
}

impl KmsManager {
    pub fn new<T>(kms_client_factory: T) -> Self
    where
        T: KmsClientFactory + 'static,
    {
        Self {
            kms_client_factory: Mutex::new(Box::new(kms_client_factory)),
            kms_client_cache: ExpiringCache::new(),
            kek_caches: ExpiringCache::new(),
        }
    }

    pub fn get_client(
        &self,
        kms_connection_config: &Arc<KmsConnectionConfig>,
        cache_lifetime: Option<Duration>,
    ) -> Result<KmsClientRef> {
        let key_access_token = kms_connection_config.read_key_access_token();
        let key = ClientKey::new(
            key_access_token.clone(),
            kms_connection_config.kms_instance_id().to_owned(),
        );
        self.kms_client_cache
            .get_or_create_fallible(key, cache_lifetime, || {
                let client_factory = self.kms_client_factory.lock().unwrap();
                client_factory.create_client(kms_connection_config)
            })
    }

    pub fn get_kek_cache(
        &self,
        kms_connection_config: &Arc<KmsConnectionConfig>,
        cache_lifetime: Option<Duration>,
    ) -> KekCache {
        let key = KekCacheKey::new(kms_connection_config.key_access_token().clone());
        self.kek_caches.get_or_create(key, cache_lifetime, || {
            Arc::new(Mutex::new(Default::default()))
        })
    }

    // TODO: Clear expired clients and KEK caches
}

struct ExpiringCache<TKey, TValue> {
    cache: Mutex<HashMap<TKey, ExpiringCacheValue<TValue>>>,
}

#[derive(Debug)]
struct ExpiringCacheValue<TValue> {
    value: TValue,
    creation_time: Instant,
}

impl<TValue> ExpiringCacheValue<TValue> {
    pub fn new(value: TValue) -> Self {
        Self {
            value,
            creation_time: Instant::now(),
        }
    }

    pub fn is_valid(&self, cache_lifetime: Option<Duration>) -> bool {
        match cache_lifetime {
            None => true,
            Some(cache_lifetime) => {
                let age = Instant::now().saturating_duration_since(self.creation_time);
                age < cache_lifetime
            }
        }
    }
}

impl<TKey, TValue> ExpiringCache<TKey, TValue>
where
    TKey: Eq + Hash,
    TValue: Clone,
{
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::default()),
        }
    }

    pub fn get_or_create<F>(
        &self,
        key: TKey,
        cache_lifetime: Option<Duration>,
        creator: F,
    ) -> TValue
    where
        F: FnOnce() -> TValue,
    {
        let mut cache = self.cache.lock().unwrap();
        let entry = cache.entry(key);
        match entry {
            Entry::Occupied(entry) if entry.get().is_valid(cache_lifetime) => {
                entry.get().value.clone()
            }
            entry => {
                let value = creator();
                // TODO: Change to use entry.insert_entry once MSRV >= 1.83.0
                entry
                    .and_modify(|e| *e = ExpiringCacheValue::new(value.clone()))
                    .or_insert_with(|| ExpiringCacheValue::new(value.clone()));
                value
            }
        }
    }

    pub fn get_or_create_fallible<F>(
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
            Entry::Occupied(entry) if entry.get().is_valid(cache_lifetime) => {
                Ok(entry.get().value.clone())
            }
            entry => {
                let value = creator()?;
                // TODO: Change to use entry.insert_entry once MSRV >= 1.83.0
                entry
                    .and_modify(|e| *e = ExpiringCacheValue::new(value.clone()))
                    .or_insert_with(|| ExpiringCacheValue::new(value.clone()));
                Ok(value)
            }
        }
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct KekCacheKey {
    key_access_token: String,
}

impl KekCacheKey {
    pub fn new(key_access_token: String) -> Self {
        Self { key_access_token }
    }
}
