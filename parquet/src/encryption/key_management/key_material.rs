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
#[derive(Serialize, Deserialize)]
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

pub fn deserialize_key_material(key_material: &str) -> Result<KeyMaterial> {
    let material: KeyMaterial = serde_json::from_str(key_material).map_err(|e| {
        ParquetError::General(format!(
            "Error deserializing JSON encryption key material: {e}"
        ))
    })?;
    if material.key_material_type != "PKMT1" {
        return Err(ParquetError::General(format!(
            "Unsupported key material type: {}",
            material.key_material_type
        )));
    }
    Ok(material)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_footer_key_material() {
        let key_material = r#"{
            "keyMaterialType": "PKMT1",
            "internalStorage": true,
            "isFooterKey": true,
            "kmsInstanceID": "DEFAULT",
            "kmsInstanceURL": "DEFAULT",
            "masterKeyID": "kf",
            "wrappedDEK": "AAAA",
            "doubleWrapping": true,
            "keyEncryptionKeyID": "kek_01",
            "wrappedKEK": "BBBB"
        }"#;

        let key_material = deserialize_key_material(key_material).unwrap();
        assert_eq!(key_material.key_material_type, "PKMT1");
        assert!(key_material.internal_storage);
        assert!(key_material.is_footer_key);
        assert!(key_material.double_wrapping);
        assert_eq!(key_material.kms_instance_id.as_deref(), Some("DEFAULT"));
        assert_eq!(key_material.kms_instance_url.as_deref(), Some("DEFAULT"));
        assert_eq!(key_material.master_key_id, "kf");
        assert_eq!(key_material.wrapped_dek, "AAAA");
        assert_eq!(
            key_material.key_encryption_key_id.as_deref(),
            Some("kek_01")
        );
        assert_eq!(key_material.wrapped_kek.as_deref(), Some("BBBB"));
    }

    #[test]
    fn test_deserialize_column_key_material() {
        let key_material = r#"{
            "keyMaterialType": "PKMT1",
            "internalStorage": true,
            "isFooterKey": false,
            "masterKeyID": "kc1",
            "wrappedDEK": "AAAA",
            "doubleWrapping": true,
            "keyEncryptionKeyID": "kek_01",
            "wrappedKEK": "BBBB"
        }"#;

        let key_material = deserialize_key_material(key_material).unwrap();
        assert_eq!(key_material.key_material_type, "PKMT1");
        assert!(key_material.internal_storage);
        assert!(!key_material.is_footer_key);
        assert!(key_material.double_wrapping);
        assert!(key_material.kms_instance_id.is_none());
        assert!(key_material.kms_instance_url.is_none());
        assert_eq!(key_material.master_key_id, "kc1");
        assert_eq!(key_material.wrapped_dek, "AAAA");
        assert_eq!(
            key_material.key_encryption_key_id.as_deref(),
            Some("kek_01")
        );
        assert_eq!(key_material.wrapped_kek.as_deref(), Some("BBBB"));
    }

    #[test]
    fn test_deserialize_single_wrapping_key_material() {
        let key_material = r#"{
            "keyMaterialType": "PKMT1",
            "internalStorage": true,
            "isFooterKey": false,
            "masterKeyID": "kc1",
            "wrappedDEK": "AAAA",
            "doubleWrapping": false
        }"#;

        let key_material = deserialize_key_material(key_material).unwrap();
        assert_eq!(key_material.key_material_type, "PKMT1");
        assert!(key_material.internal_storage);
        assert!(!key_material.is_footer_key);
        assert!(!key_material.double_wrapping);
        assert!(key_material.kms_instance_id.is_none());
        assert!(key_material.kms_instance_url.is_none());
        assert_eq!(key_material.master_key_id, "kc1");
        assert_eq!(key_material.wrapped_dek, "AAAA");
        assert!(key_material.key_encryption_key_id.is_none());
        assert!(key_material.wrapped_kek.is_none());
    }
}
