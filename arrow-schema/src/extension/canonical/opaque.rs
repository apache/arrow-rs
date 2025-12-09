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

use serde_core::ser::SerializeStruct;
use serde_core::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
};

use crate::{ArrowError, DataType, extension::ExtensionType};

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
#[derive(Debug, Clone, PartialEq)]
pub struct OpaqueMetadata {
    /// Name of the unknown type in the external system.
    type_name: String,

    /// Name of the external system.
    vendor_name: String,
}

impl Serialize for OpaqueMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OpaqueMetadata", 2)?;
        state.serialize_field("type_name", &self.type_name)?;
        state.serialize_field("vendor_name", &self.vendor_name)?;
        state.end()
    }
}

#[derive(Debug)]
enum MetadataField {
    TypeName,
    VendorName,
}

struct MetadataFieldVisitor;

impl<'de> Visitor<'de> for MetadataFieldVisitor {
    type Value = MetadataField;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("`type_name` or `vendor_name`")
    }

    fn visit_str<E>(self, value: &str) -> Result<MetadataField, E>
    where
        E: serde_core::de::Error,
    {
        match value {
            "type_name" => Ok(MetadataField::TypeName),
            "vendor_name" => Ok(MetadataField::VendorName),
            _ => Err(serde_core::de::Error::unknown_field(
                value,
                &["type_name", "vendor_name"],
            )),
        }
    }
}

impl<'de> Deserialize<'de> for MetadataField {
    fn deserialize<D>(deserializer: D) -> Result<MetadataField, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(MetadataFieldVisitor)
    }
}

struct OpaqueMetadataVisitor;

impl<'de> Visitor<'de> for OpaqueMetadataVisitor {
    type Value = OpaqueMetadata;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct OpaqueMetadata")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<OpaqueMetadata, V::Error>
    where
        V: serde_core::de::SeqAccess<'de>,
    {
        let type_name = seq
            .next_element()?
            .ok_or_else(|| serde_core::de::Error::invalid_length(0, &self))?;
        let vendor_name = seq
            .next_element()?
            .ok_or_else(|| serde_core::de::Error::invalid_length(1, &self))?;
        Ok(OpaqueMetadata {
            type_name,
            vendor_name,
        })
    }

    fn visit_map<V>(self, mut map: V) -> Result<OpaqueMetadata, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut type_name = None;
        let mut vendor_name = None;

        while let Some(key) = map.next_key()? {
            match key {
                MetadataField::TypeName => {
                    if type_name.is_some() {
                        return Err(serde_core::de::Error::duplicate_field("type_name"));
                    }
                    type_name = Some(map.next_value()?);
                }
                MetadataField::VendorName => {
                    if vendor_name.is_some() {
                        return Err(serde_core::de::Error::duplicate_field("vendor_name"));
                    }
                    vendor_name = Some(map.next_value()?);
                }
            }
        }

        let type_name =
            type_name.ok_or_else(|| serde_core::de::Error::missing_field("type_name"))?;
        let vendor_name =
            vendor_name.ok_or_else(|| serde_core::de::Error::missing_field("vendor_name"))?;

        Ok(OpaqueMetadata {
            type_name,
            vendor_name,
        })
    }
}

impl<'de> Deserialize<'de> for OpaqueMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "OpaqueMetadata",
            &["type_name", "vendor_name"],
            OpaqueMetadataVisitor,
        )
    }
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
    const NAME: &'static str = "arrow.opaque";

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

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        Field,
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    };

    use super::*;

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let opaque = Opaque::new("name", "vendor");
        let mut field = Field::new("", DataType::Null, false);
        field.try_with_extension_type(opaque.clone())?;
        assert_eq!(field.try_extension_type::<Opaque>()?, opaque);
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::Opaque(opaque)
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field = Field::new("", DataType::Null, false).with_metadata(
            [(
                EXTENSION_TYPE_METADATA_KEY.to_owned(),
                r#"{ "type_name": "type", "vendor_name": "vendor" }"#.to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        field.extension_type::<Opaque>();
    }

    #[test]
    #[should_panic(expected = "Opaque extension types requires metadata")]
    fn missing_metadata() {
        let field = Field::new("", DataType::Null, false).with_metadata(
            [(EXTENSION_TYPE_NAME_KEY.to_owned(), Opaque::NAME.to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<Opaque>();
    }

    #[test]
    #[should_panic(
        expected = "Opaque metadata deserialization failed: missing field `vendor_name`"
    )]
    fn invalid_metadata() {
        let field = Field::new("", DataType::Null, false).with_metadata(
            [
                (EXTENSION_TYPE_NAME_KEY.to_owned(), Opaque::NAME.to_owned()),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    r#"{ "type_name": "no-vendor" }"#.to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<Opaque>();
    }
}
