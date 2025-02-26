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

//! Variant
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variant>

use base64::engine::Engine as _;
use base64::engine::general_purpose::STANDARD;
use crate::{extension::ExtensionType, ArrowError, DataType};

/// The extension type for `Variant`.
///
/// Extension name: `arrow.variant`.
///
/// The storage type of this extension is **Binary or LargeBinary**.
/// A Variant is a flexible structure that can store **Primitives, Arrays, or Objects**.
/// It is stored as **two binary values**: `metadata` and `value`.
///
/// The **metadata field is required** and must be a valid Variant metadata string.
/// The **value field is required** and contains the serialized Variant data.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variant>
#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    metadata: Vec<u8>, // Required binary metadata
    value: Vec<u8>, // Required binary value
}

impl Variant {
    /// Creates a new `Variant` with metadata and value.
    pub fn new(metadata: Vec<u8>, value: Vec<u8>) -> Self {
        Self { metadata, value }
    }

    /// Creates a Variant representing an empty structure.
    pub fn empty() -> Result<Self, ArrowError> {
        Err(ArrowError::InvalidArgumentError(
            "Variant cannot be empty because metadata and value are required".to_owned(),
        ))
    }

    /// Returns the metadata as a byte array.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// Returns the value as an byte array.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Sets the value of the Variant.
    pub fn set_value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }
}

impl ExtensionType for Variant {
    const NAME: &'static str = "arrow.variant";

    type Metadata = Vec<u8>; // Metadata is directly Vec<u8>

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(STANDARD.encode(&self.metadata)) // Encode metadata as STANDARD string
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        match metadata {
            Some(meta) => STANDARD.decode(meta)
                .map_err(|_| ArrowError::InvalidArgumentError("Invalid Variant metadata".to_owned())),
            None => Ok(Vec::new()), // Default to empty metadata if None
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Variant data type mismatch, expected Binary or LargeBinary, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let variant = Self { metadata, value: vec![0]};
        variant.supports_data_type(data_type)?;
        Ok(variant)
    }
    
    
}

#[cfg(test)]
mod tests {
    use crate::{
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
        Field,
    };

    use super::*;

    #[test]
    fn variant_metadata_encoding_decoding() {
        let metadata = b"variant_metadata".to_vec();
        let encoded = STANDARD.encode(&metadata);
        let decoded = Variant::deserialize_metadata(Some(&encoded)).unwrap();
        assert_eq!(metadata, decoded);
    }

    #[test]
    fn variant_metadata_invalid_decoding() {
        let result = Variant::deserialize_metadata(Some("invalid_base64"));
        assert!(result.is_err());
    }

    #[test]
    fn variant_metadata_none_decoding() {
        let decoded = Variant::deserialize_metadata(None).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn variant_supports_valid_data_types() {
        let variant = Variant::new(vec![1, 2, 3], vec![4, 5, 6]);
        assert!(variant.supports_data_type(&DataType::Binary).is_ok());
        assert!(variant.supports_data_type(&DataType::LargeBinary).is_ok());

        let variant = Variant::try_new(&DataType::Binary, vec![1, 2, 3]).unwrap().set_value(vec![4, 5, 6]);
        assert!(variant.supports_data_type(&DataType::Binary).is_ok());

        let variant = Variant::try_new(&DataType::LargeBinary, vec![1, 2, 3]).unwrap().set_value(vec![4, 5, 6]);
        assert!(variant.supports_data_type(&DataType::LargeBinary).is_ok());

        let result = Variant::try_new(&DataType::Utf8, vec![1, 2, 3]);
        assert!(result.is_err());
        if let Err(ArrowError::InvalidArgumentError(msg)) = result {
            assert!(msg.contains("Variant data type mismatch"));
        }
    }

    #[test]
    #[should_panic(expected = "Variant data type mismatch")]
    fn variant_rejects_invalid_data_type() {
        let variant = Variant::new(vec![1, 2, 3], vec![4, 5, 6]);
        variant.supports_data_type(&DataType::Utf8).unwrap();
    }

    #[test]
    fn variant_creation() {
        let metadata = vec![10, 20, 30];
        let value = vec![40, 50, 60];
        let variant = Variant::new(metadata.clone(), value.clone());
        assert_eq!(variant.value(), &value);
    }

    #[test]
    fn variant_empty() {
        let variant = Variant::empty();
        assert!(variant.is_err());
    }

    #[test]
    fn variant_field_extension() {
        let mut field = Field::new("", DataType::Binary, false);
        let variant = Variant::new(vec![1, 2, 3], vec![4, 5, 6]);
        field.try_with_extension_type(variant).unwrap();
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_NAME_KEY),
            Some(&"arrow.variant".to_owned())
        );
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn variant_missing_name() {
        let field = Field::new("", DataType::Binary, false).with_metadata(
            [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "{}".to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<Variant>();
    }

}
