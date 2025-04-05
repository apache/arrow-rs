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

use crate::encryption::decrypt::KeyRetriever;
use crate::encryption::key_management::crypto_factory::DecryptionConfiguration;
use crate::encryption::key_management::key_encryption;
use crate::encryption::key_management::key_material::KeyMaterial;
use crate::encryption::key_management::kms::KmsConnectionConfig;
use crate::encryption::key_management::kms_manager::{KekCache, KmsManager};
use crate::errors::{ParquetError, Result};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};

/// Unwraps (decrypts) key encryption keys and data encryption keys using a KMS
pub(crate) struct KeyUnwrapper {
    kms_manager: Arc<KmsManager>,
    kms_connection_config: RwLock<Arc<KmsConnectionConfig>>,
    decryption_configuration: DecryptionConfiguration,
    kek_cache: KekCache,
}

impl KeyUnwrapper {
    pub fn new(
        kms_manager: Arc<KmsManager>,
        kms_connection_config: Arc<KmsConnectionConfig>,
        decryption_configuration: DecryptionConfiguration,
    ) -> Self {
        let kek_cache = kms_manager.get_kek_cache(
            &kms_connection_config,
            decryption_configuration.cache_lifetime(),
        );
        let kms_connection_config = RwLock::new(kms_connection_config);
        KeyUnwrapper {
            kms_manager,
            kms_connection_config,
            decryption_configuration,
            kek_cache,
        }
    }

    fn unwrap_single_wrapped_key(&self, wrapped_dek: &str, master_key_id: &str) -> Result<Vec<u8>> {
        let kms_connection_config = self.kms_connection_config.read().unwrap();
        let client = self.kms_manager.get_client(
            &kms_connection_config,
            self.decryption_configuration.cache_lifetime(),
        )?;
        client.unwrap_key(wrapped_dek, master_key_id)
    }

    fn unwrap_double_wrapped_key(
        &self,
        wrapped_dek: &str,
        master_key_id: &str,
        kek_id: &str,
        wrapped_kek: &str,
    ) -> Result<Vec<u8>> {
        let mut kek_cache = self.kek_cache.lock().unwrap();
        let kek = match kek_cache.entry(kek_id.to_owned()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let kms_connection_config = self.kms_connection_config.read().unwrap();
                let client = self.kms_manager.get_client(
                    &kms_connection_config,
                    self.decryption_configuration.cache_lifetime(),
                )?;
                let kek = client.unwrap_key(wrapped_kek, master_key_id)?;
                entry.insert(kek)
            }
        };
        let decoded_kek_id = BASE64_STANDARD
            .decode(kek_id)
            .map_err(|e| general_err!("Could not base64 decode key encryption key id: {}", e))?;
        key_encryption::decrypt_encryption_key(wrapped_dek, &decoded_kek_id, kek)
    }

    fn update_kms_config_from_footer_metadata(
        &self,
        kms_instance_id: &str,
        kms_instance_url: &str,
    ) -> Result<()> {
        let mut kms_connection_config = self.kms_connection_config.write().unwrap();

        if !kms_connection_config.kms_instance_id().is_empty()
            && !kms_connection_config.kms_instance_url().is_empty()
        {
            return Ok(());
        }

        let mut_config = Arc::make_mut(&mut kms_connection_config);
        if mut_config.kms_instance_id().is_empty() {
            if kms_instance_id.is_empty() {
                return Err(general_err!(
                    "KMS instance ID not set in connection configuration or footer key metadata"
                ));
            }
            mut_config.set_kms_instance_id(kms_instance_id.to_owned());
        }

        if mut_config.kms_instance_url().is_empty() {
            if kms_instance_url.is_empty() {
                return Err(general_err!(
                    "KMS instance URL not set in connection configuration or footer key metadata"
                ));
            }
            mut_config.set_kms_instance_url(kms_instance_url.to_owned());
        }

        Ok(())
    }
}

impl KeyRetriever for KeyUnwrapper {
    fn retrieve_key(&self, key_metadata: &[u8]) -> Result<Vec<u8>> {
        let key_material = std::str::from_utf8(key_metadata)?;
        let key_material = KeyMaterial::deserialize(key_material)?;
        if !key_material.internal_storage {
            return Err(nyi_err!(
                "Decryption using external key material is not yet implemented"
            ));
        }

        // If unwrapping a footer key, optionally set the KMS instance ID and URL
        if let (Some(instance_id), Some(instance_url)) = (
            &key_material.kms_instance_id,
            &key_material.kms_instance_url,
        ) {
            self.update_kms_config_from_footer_metadata(instance_id, instance_url)?;
        }

        if key_material.double_wrapping {
            if let (Some(kek_id), Some(wrapped_kek)) =
                (key_material.key_encryption_key_id, key_material.wrapped_kek)
            {
                self.unwrap_double_wrapped_key(
                    &key_material.wrapped_dek,
                    &key_material.master_key_id,
                    &kek_id,
                    &wrapped_kek,
                )
            } else {
                Err(general_err!(
                    "Key uses double wrapping but key encryption key is not set"
                ))
            }
        } else {
            self.unwrap_single_wrapped_key(&key_material.wrapped_dek, &key_material.master_key_id)
        }
    }
}
