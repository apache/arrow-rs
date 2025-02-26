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

use crate::errors::{ParquetError, Result};
use serde::{Deserialize, Serialize};

/// Serializable key material that describes a wrapped encryption key
/// and includes metadata required to unwrap it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyMaterial {
    /// The type of the key material.
    /// Currently only one type is supported: "PKMT1"
    #[serde(rename = "keyMaterialType")]
    pub key_material_type: String,

    /// Whether key material is stored inline in this JSON data or in an external file
    #[serde(rename = "internalStorage")]
    pub internal_storage: bool,

    /// If internal storage is false, a reference to the external key material file
    #[serde(rename = "keyReference")]
    pub key_reference: Option<String>,

    /// Whether the material belongs to a file footer key
    #[serde(rename = "isFooterKey")]
    pub is_footer_key: bool,

    /// The KMS instance ID. Only written for footer key material
    #[serde(rename = "kmsInstanceID")]
    pub kms_instance_id: Option<String>,

    /// The KMS instance URL. Only written for footer key material
    #[serde(rename = "kmsInstanceURL")]
    pub kms_instance_url: Option<String>,

    /// An identifier for the master key used to generate the key material
    #[serde(rename = "masterKeyID")]
    pub master_key_id: String,

    /// The wrapped data encryption key
    #[serde(rename = "wrappedDEK")]
    pub wrapped_dek: String,

    /// Whether double wrapping is used, where data encryption keys are wrapped
    /// with a key encryption key, which in turn is wrapped with the master key.
    /// If false (single wrapping), data encryption keys are wrapped directly with the master key.
    #[serde(rename = "doubleWrapping")]
    pub double_wrapping: bool,

    /// The identifier of the key encryption key used to wrap the data encryption key.
    /// Only written in double wrapping mode.
    #[serde(rename = "keyEncryptionKeyID")]
    pub key_encryption_key_id: Option<String>,

    /// The wrapped key encryption key. Only written in double wrapping mode.
    #[serde(rename = "wrappedKEK")]
    pub wrapped_kek: Option<String>,
}

pub struct KeyMaterialBuilder {
    is_footer_key: bool,
    kms_instance_id: Option<String>,
    kms_instance_url: Option<String>,
    master_key_id: Option<String>,
    wrapped_dek: Option<String>,
    double_wrapping: bool,
    key_encryption_key_id: Option<String>,
    wrapped_kek: Option<String>,
}

impl KeyMaterialBuilder {
    pub fn for_footer_key(kms_instance_id: String, kms_instance_url: String) -> Self {
        Self {
            is_footer_key: true,
            kms_instance_id: Some(kms_instance_id),
            kms_instance_url: Some(kms_instance_url),
            master_key_id: None,
            wrapped_dek: None,
            double_wrapping: false,
            key_encryption_key_id: None,
            wrapped_kek: None,
        }
    }

    pub fn for_column_key() -> Self {
        Self {
            is_footer_key: false,
            kms_instance_id: None,
            kms_instance_url: None,
            master_key_id: None,
            wrapped_dek: None,
            double_wrapping: false,
            key_encryption_key_id: None,
            wrapped_kek: None,
        }
    }

    pub fn with_single_wrapped_key(mut self, master_key_id: String, wrapped_dek: String) -> Self {
        self.double_wrapping = false;
        self.master_key_id = Some(master_key_id);
        self.wrapped_dek = Some(wrapped_dek);
        self
    }

    pub fn with_double_wrapped_key(
        mut self,
        master_key_id: String,
        key_encryption_key_id: String,
        wrapped_kek: String,
        wrapped_dek: String,
    ) -> Self {
        self.double_wrapping = true;
        self.master_key_id = Some(master_key_id);
        self.key_encryption_key_id = Some(key_encryption_key_id);
        self.wrapped_kek = Some(wrapped_kek);
        self.wrapped_dek = Some(wrapped_dek);
        self
    }

    pub fn build(self) -> Result<KeyMaterial> {
        if let (Some(master_key_id), Some(wrapped_dek)) = (self.master_key_id, self.wrapped_dek) {
            Ok(KeyMaterial {
                key_material_type: "PKMT1".to_string(),
                internal_storage: true,
                key_reference: None,
                is_footer_key: self.is_footer_key,
                kms_instance_id: self.kms_instance_id,
                kms_instance_url: self.kms_instance_url,
                master_key_id,
                wrapped_dek,
                double_wrapping: self.double_wrapping,
                key_encryption_key_id: self.key_encryption_key_id,
                wrapped_kek: self.wrapped_kek,
            })
        } else {
            Err(general_err!(
                "Wrapped key not set when building key material"
            ))
        }
    }
}

impl KeyMaterial {
    pub fn deserialize(key_material: &str) -> Result<Self> {
        let material: KeyMaterial = serde_json::from_str(key_material)
            .map_err(|e| general_err!("Error deserializing JSON encryption key material: {}", e))?;
        if material.key_material_type != "PKMT1" {
            return Err(general_err!(
                "Unsupported key material type: {}",
                material.key_material_type
            ));
        }
        Ok(material)
    }

    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| general_err!("Error serializing key material to JSON: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_key_material_round_trip() {
        let key_material =
            KeyMaterialBuilder::for_footer_key("DEFAULT".to_owned(), "DEFAULT".to_owned())
                .with_double_wrapped_key(
                    "kf".to_owned(),
                    "kek1".to_owned(),
                    "AAAA".to_owned(),
                    "BBBB".to_owned(),
                )
                .build()
                .unwrap();

        let serialized = key_material.serialize().unwrap();
        let deserialized = KeyMaterial::deserialize(&serialized).unwrap();

        assert_eq!(key_material, deserialized);
    }

    #[test]
    fn test_column_key_material_round_trip() {
        let key_material = KeyMaterialBuilder::for_column_key()
            .with_double_wrapped_key(
                "kc1".to_owned(),
                "kek1".to_owned(),
                "AAAA".to_owned(),
                "BBBB".to_owned(),
            )
            .build()
            .unwrap();

        let serialized = key_material.serialize().unwrap();
        let deserialized = KeyMaterial::deserialize(&serialized).unwrap();

        assert_eq!(key_material, deserialized);
    }

    #[test]
    fn test_single_wrapping_key_material_round_trip() {
        let key_material = KeyMaterialBuilder::for_column_key()
            .with_single_wrapped_key("kc1".to_owned(), "CCCC".to_owned())
            .build()
            .unwrap();

        let serialized = key_material.serialize().unwrap();
        let deserialized = KeyMaterial::deserialize(&serialized).unwrap();

        assert_eq!(key_material, deserialized);
    }
}
