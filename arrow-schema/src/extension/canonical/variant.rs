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
/// The storage type of this extension is **Struct containing two binary fields**:
/// - metadata: Binary field containing the variant metadata
/// - value: Binary field containing the serialized variant data
/// 
/// A Variant is a flexible structure that can store **Primitives, Arrays, or Objects**.
///
/// Both metadata and value fields are required.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variant>
#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    metadata: Vec<u8>, // Required binary metadata
    value: Vec<u8>,    // Required binary value
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

    type Metadata = Vec<u8>;

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
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err(ArrowError::InvalidArgumentError(
                        "Variant struct must have exactly two fields".to_owned(),
                    ));
                }
                
                let metadata_field = fields.iter()
                    .find(|f| f.name() == "metadata")
                    .ok_or_else(|| ArrowError::InvalidArgumentError(
                        "Variant struct must have a field named 'metadata'".to_owned(),
                    ))?;

                let value_field = fields.iter()
                    .find(|f| f.name() == "value")
                    .ok_or_else(|| ArrowError::InvalidArgumentError(
                        "Variant struct must have a field named 'value'".to_owned(),
                    ))?;

                match (metadata_field.data_type(), value_field.data_type()) {
                    (DataType::Binary, DataType::Binary) |
                    (DataType::LargeBinary, DataType::LargeBinary) => Ok(()),
                    _ => Err(ArrowError::InvalidArgumentError(
                        "Variant struct fields must both be Binary or LargeBinary".to_owned(),
                    )),
                }
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Variant data type mismatch, expected Struct, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let variant = Self { metadata, value: vec![0] };
        variant.supports_data_type(data_type)?;
        Ok(variant)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        extension::{EXTENSION_TYPE_NAME_KEY},
        Field, DataType,
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
        // Test with actual binary data
        let metadata = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let value = vec![0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F];
        let variant = Variant::new(metadata.clone(), value.clone());
        
        // Test with Binary fields
        let struct_type = DataType::Struct(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false)
        ].into());
        assert!(variant.supports_data_type(&struct_type).is_ok());

        // Test with LargeBinary fields
        let struct_type = DataType::Struct(vec![
            Field::new("metadata", DataType::LargeBinary, false),
            Field::new("value", DataType::LargeBinary, false)
        ].into());
        assert!(variant.supports_data_type(&struct_type).is_ok());

        // Test with invalid type
        let result = Variant::try_new(&DataType::Utf8, metadata);
        assert!(result.is_err());
        if let Err(ArrowError::InvalidArgumentError(msg)) = result {
            assert!(msg.contains("Variant data type mismatch"));
        }
    }

    #[test]
    fn variant_creation_and_access() {
        // Test with actual binary data
        let metadata = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let value = vec![0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F];
        let variant = Variant::new(metadata.clone(), value.clone());
        assert_eq!(variant.metadata(), &metadata);
        assert_eq!(variant.value(), &value);
    }

    #[test]
    fn variant_field_extension() {
        let struct_type = DataType::Struct(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false)
        ].into());
        
        // Test with actual binary data
        let metadata = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let value = vec![0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F];
        let variant = Variant::new(metadata.clone(), value.clone());
        
        let mut field = Field::new("", struct_type, false);
        field.try_with_extension_type(variant.clone()).unwrap();
        
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_NAME_KEY),
            Some(&"arrow.variant".to_owned())
        );
        
        #[cfg(feature = "canonical_extension_types")]
        {
            let recovered = field.try_canonical_extension_type().unwrap();
            if let CanonicalExtensionType::Variant(recovered_variant) = recovered {
                assert_eq!(recovered_variant.metadata(), variant.metadata());
            } else {
                panic!("Expected Variant type");
            }
        }
    }
}
