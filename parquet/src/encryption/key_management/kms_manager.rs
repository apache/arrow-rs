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

use crate::encryption::key_management::kms::{KmsClient, KmsClientRef, KmsConnectionConfig};
use crate::errors::Result;
use std::sync::{Arc, Mutex};

pub type ClientFactory =
    Mutex<Box<dyn FnMut(&KmsConnectionConfig) -> Result<KmsClientRef> + Send + Sync>>;

/// Manages caching the KMS and allowing interaction with it
pub struct KmsManager {
    kms_client_factory: ClientFactory,
    kms_client: Mutex<Option<KmsClientRef>>,
}

impl KmsManager {
    pub fn new(kms_client_factory: ClientFactory) -> Self {
        Self {
            kms_client_factory,
            kms_client: Mutex::new(None),
        }
    }

    pub fn get_client(
        &self,
        kms_connection_config: &KmsConnectionConfig,
    ) -> Result<Arc<dyn KmsClient>> {
        let mut guard = self.kms_client.lock().unwrap();
        let kms_client = &mut *guard;
        let client = match kms_client {
            None => {
                let mut client_factory = self.kms_client_factory.lock().unwrap();
                let client = client_factory(kms_connection_config)?;
                *kms_client = Some(client.clone());
                client
            }
            Some(client) => client.clone(),
        };
        Ok(client)
    }
}
