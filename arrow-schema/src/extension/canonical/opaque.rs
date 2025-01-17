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

//! Opaque
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#opaque>

use serde::{Deserialize, Serialize};

use crate::{extension::ExtensionType, ArrowError, DataType};

/// The extension type for `Opaque`.
///
/// Extension name: `arrow.opaque`.
///
/// Opaque represents a type that an Arrow-based system received from an
/// external (often non-Arrow) system, but that it cannot interpret. In this
/// case, it can pass on Opaque to its clients to at least show that a field
/// exists and preserve metadata about the type from the other system.
///
/// The storage type of this extension is any type. If there is no underlying
/// data, the storage type should be Null.
#[derive(Debug, Clone, PartialEq)]
pub struct Opaque(OpaqueMetadata);

impl Opaque {
    /// Returns a new `Opaque` extension type.
    pub fn new(type_name: impl Into<String>, vendor_name: impl Into<String>) -> Self {
        Self(OpaqueMetadata::new(type_name, vendor_name))
    }

    /// Returns the name of the unknown type in the external system.
    pub fn type_name(&self) -> &str {
        self.0.type_name()
    }

    /// Returns the name of the external system.
    pub fn vendor_name(&self) -> &str {
        self.0.vendor_name()
    }
}

impl From<OpaqueMetadata> for Opaque {
    fn from(value: OpaqueMetadata) -> Self {
        Self(value)
    }
}

/// Extension type metadata for [`Opaque`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct OpaqueMetadata {
    /// Name of the unknown type in the external system.
    type_name: String,

    /// Name of the external system.
    vendor_name: String,
}

impl OpaqueMetadata {
    /// Returns a new `OpaqueMetadata`.
    pub fn new(type_name: impl Into<String>, vendor_name: impl Into<String>) -> Self {
        OpaqueMetadata {
            type_name: type_name.into(),
            vendor_name: vendor_name.into(),
        }
    }

    /// Returns the name of the unknown type in the external system.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the name of the external system.
    pub fn vendor_name(&self) -> &str {
        &self.vendor_name
    }
}

impl ExtensionType for Opaque {
    const NAME: &str = "arrow.opaque";

    type Metadata = OpaqueMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(serde_json::to_string(self.metadata()).expect("metadata serialization"))
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        metadata.map_or_else(
            || {
                Err(ArrowError::InvalidArgumentError(
                    "Opaque extension types requires metadata".to_owned(),
                ))
            },
            |value| {
                serde_json::from_str(value).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "Opaque metadata deserialization failed: {e}"
                    ))
                })
            },
        )
    }

    fn supports_data_type(&self, _data_type: &DataType) -> Result<(), ArrowError> {
        // Any type
        Ok(())
    }

    fn try_new(_data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Ok(Self::from(metadata))
    }
}
