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

/// Manages caching the KMS and allowing interaction with it
pub struct KmsManager {
    kms_client_factory: ClientFactory,
    kms_client_cache: Mutex<HashMap<ClientKey, ClientEntry>>,
}

impl KmsManager {
    pub fn new(kms_client_factory: ClientFactory) -> Self {
        Self {
            kms_client_factory,
            kms_client_cache: Mutex::new(HashMap::default()),
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

    // TODO: Clear expired clients
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
