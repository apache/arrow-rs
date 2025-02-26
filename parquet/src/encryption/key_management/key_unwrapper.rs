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
use crate::encryption::key_management::key_material::deserialize_key_material;
use crate::encryption::key_management::kms::KmsConnectionConfig;
use crate::encryption::key_management::kms_manager::KmsManager;
use crate::errors;
use crate::errors::ParquetError;
use std::sync::{Arc, RwLock};

pub struct KeyUnwrapper {
    kms_manager: Arc<KmsManager>,
    kms_connection_config: Arc<RwLock<KmsConnectionConfig>>,
    decryption_configuration: DecryptionConfiguration,
}

impl KeyUnwrapper {
    pub fn new(
        kms_manager: Arc<KmsManager>,
        kms_connection_config: Arc<RwLock<KmsConnectionConfig>>,
        decryption_configuration: DecryptionConfiguration,
    ) -> Self {
        KeyUnwrapper {
            kms_manager,
            kms_connection_config,
            decryption_configuration,
        }
    }
}

impl KeyRetriever for KeyUnwrapper {
    fn retrieve_key(&self, key_metadata: &[u8]) -> errors::Result<Vec<u8>> {
        let key_material = std::str::from_utf8(key_metadata)?;
        let key_material = deserialize_key_material(key_material)?;
        if key_material.double_wrapping {
            return Err(ParquetError::NYI(
                "Double wrapping is not yet implemented".to_owned(),
            ));
        };
        let kms_connection_config = self.kms_connection_config.read().unwrap();
        let client = self.kms_manager.get_client(&kms_connection_config)?;
        client.unwrap_key(&key_material.wrapped_dek, &key_material.master_key_id)
    }
}
