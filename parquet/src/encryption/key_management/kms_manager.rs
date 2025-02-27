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

use crate::encryption::key_management::kms::{
    KmsClient, KmsClientFactory, KmsClientRef, KmsConnectionConfig,
};
use crate::errors::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type ClientFactory = Mutex<Box<dyn KmsClientFactory>>;

/// Cache of key encryption keys, keyed by their base64 encoded key id
pub type KekCache = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Manages caching the KMS and allowing interaction with it
pub struct KmsManager {
    kms_client_factory: ClientFactory,
    kms_client_cache: Mutex<HashMap<ClientKey, ClientEntry>>,
    kek_caches: Mutex<HashMap<KekCacheKey, KekCacheEntry>>,
}

impl KmsManager {
    pub fn new(kms_client_factory: ClientFactory) -> Self {
        Self {
            kms_client_factory,
            kms_client_cache: Mutex::new(HashMap::default()),
            kek_caches: Mutex::new(HashMap::default()),
        }
    }

    pub fn get_client(
        &self,
        kms_connection_config: &KmsConnectionConfig,
        cache_lifetime: Option<Duration>,
    ) -> Result<Arc<dyn KmsClient>> {
        let mut guard = self.kms_client_cache.lock().unwrap();
        let kms_client_cache = &mut *guard;
        let key = ClientKey::new(
            kms_connection_config.key_access_token(),
            kms_connection_config.kms_instance_id().to_owned(),
        );
        let entry = kms_client_cache.entry(key);
        let client = match entry {
            Entry::Occupied(entry) if entry.get().is_valid(cache_lifetime) => {
                entry.get().client.clone()
            }
            entry => {
                let client_factory = self.kms_client_factory.lock().unwrap();
                let client = client_factory.create_client(kms_connection_config)?;
                entry.insert_entry(ClientEntry::new(client.clone()));
                client
            }
        };
        Ok(client)
    }

    pub fn get_kek_cache(
        &self,
        kms_connection_config: &KmsConnectionConfig,
        cache_lifetime: Option<Duration>,
    ) -> KekCache {
        let mut guard = self.kek_caches.lock().unwrap();
        let kek_caches = &mut *guard;
        let key = KekCacheKey::new(kms_connection_config.key_access_token());
        let entry = kek_caches.entry(key);
        match entry {
            Entry::Occupied(entry) if entry.get().is_valid(cache_lifetime) => {
                entry.get().kek_cache.clone()
            }
            entry => {
                let kek_cache = Arc::new(Mutex::new(Default::default()));
                entry.insert_entry(KekCacheEntry::new(kek_cache.clone()));
                kek_cache
            }
        }
    }

    // TODO: Clear expired clients and KEK caches
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

struct ClientEntry {
    client: KmsClientRef,
    creation_time: Instant,
}

impl ClientEntry {
    pub fn new(client: KmsClientRef) -> Self {
        Self {
            client,
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct KekCacheKey {
    key_access_token: String,
}

impl KekCacheKey {
    pub fn new(key_access_token: String) -> Self {
        Self { key_access_token }
    }
}

struct KekCacheEntry {
    kek_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    creation_time: Instant,
}

impl KekCacheEntry {
    pub fn new(kek_cache: KekCache) -> Self {
        Self {
            kek_cache,
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
