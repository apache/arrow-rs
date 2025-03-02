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

use crate::encryption::key_management::crypto_factory::EncryptionConfiguration;
use crate::encryption::key_management::key_encryption::encrypt_encryption_key;
use crate::encryption::key_management::key_material::KeyMaterialBuilder;
use crate::encryption::key_management::kms::KmsConnectionConfig;
use crate::encryption::key_management::kms_manager::KmsManager;
use crate::errors::Result;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Creates key material for data encryption keys
pub struct KeyWrapper<'a> {
    kms_manager: Arc<KmsManager>,
    kms_connection_config: Arc<KmsConnectionConfig>,
    encryption_configuration: &'a EncryptionConfiguration,
    master_key_to_kek: HashMap<String, KeyEncryptionKey>,
}

impl<'a> KeyWrapper<'a> {
    pub fn new(
        kms_manager: Arc<KmsManager>,
        kms_connection_config: Arc<KmsConnectionConfig>,
        encryption_configuration: &'a EncryptionConfiguration,
    ) -> Self {
        Self {
            kms_manager,
            kms_connection_config,
            encryption_configuration,
            master_key_to_kek: Default::default(),
        }
    }

    pub fn get_key_metadata(
        &mut self,
        key: &[u8],
        master_key_id: &str,
        is_footer_key: bool,
    ) -> Result<Vec<u8>> {
        let key_material_builder = if is_footer_key {
            let kms_config = &self.kms_connection_config;
            KeyMaterialBuilder::for_footer_key(
                kms_config.kms_instance_id().to_owned(),
                kms_config.kms_instance_url().to_owned(),
            )
        } else {
            KeyMaterialBuilder::for_column_key()
        };

        let key_material = if self.encryption_configuration.double_wrapping() {
            let kek = match self.master_key_to_kek.entry(master_key_id.to_owned()) {
                Entry::Occupied(kek) => kek.into_mut(),
                Entry::Vacant(entry) => entry.insert(generate_key_encryption_key(
                    master_key_id,
                    &self.kms_manager,
                    &self.kms_connection_config,
                    self.encryption_configuration.cache_lifetime(),
                )?),
            };

            let wrapped_dek = encrypt_encryption_key(key, &kek.key_id, &kek.key)?;

            key_material_builder
                .with_double_wrapped_key(
                    master_key_id.to_owned(),
                    kek.encoded_key_id.to_owned(),
                    kek.wrapped_key.to_owned(),
                    wrapped_dek,
                )
                .build()?
        } else {
            let kms_client = self.kms_manager.get_client(
                &self.kms_connection_config,
                self.encryption_configuration.cache_lifetime(),
            )?;
            let wrapped = kms_client.wrap_key(&key, master_key_id)?;
            key_material_builder
                .with_single_wrapped_key(master_key_id.to_owned(), wrapped)
                .build()?
        };

        let serialized_material = key_material.serialize()?;

        Ok(serialized_material.into_bytes())
    }
}

fn generate_key_encryption_key(
    master_key_id: &str,
    kms_manager: &Arc<KmsManager>,
    kms_connection_config: &Arc<KmsConnectionConfig>,
    cache_lifetime: Option<Duration>,
) -> Result<KeyEncryptionKey> {
    let rng = SystemRandom::new();

    let mut key = vec![0u8; 16];
    rng.fill(&mut key)?;

    // Key ids should be globally unique to allow caching decrypted keys during reading
    let mut key_id = vec![0u8; 16];
    rng.fill(&mut key_id)?;

    let encoded_key_id = BASE64_STANDARD.encode(&key_id);

    let kms_client = kms_manager.get_client(kms_connection_config, cache_lifetime)?;
    let wrapped_key = kms_client.wrap_key(&key, master_key_id)?;

    Ok(KeyEncryptionKey {
        key_id,
        encoded_key_id,
        key,
        wrapped_key,
    })
}

struct KeyEncryptionKey {
    key_id: Vec<u8>,
    encoded_key_id: String,
    key: Vec<u8>,
    wrapped_key: String,
}
