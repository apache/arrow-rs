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
//! Implements Arrow ExtensionType for Variant type.

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

    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::default())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        if metadata.is_some_and(str::is_empty) {
            Ok("")
        } else {
            Err(ArrowError::InvalidArgumentError(
                "Variant extension type expects an empty string as metadata".to_owned(),
            ))
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

                let metadata_field =
                    fields
                        .iter()
                        .find(|f| f.name() == "metadata")
                        .ok_or_else(|| {
                            ArrowError::InvalidArgumentError(
                                "Variant struct must have a field named 'metadata'".to_owned(),
                            )
                        })?;

                let value_field = fields.iter().find(|f| f.name() == "value").ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "Variant struct must have a field named 'value'".to_owned(),
                    )
                })?;

                match (metadata_field.data_type(), value_field.data_type()) {
                    (DataType::Binary, DataType::Binary)
                    | (DataType::LargeBinary, DataType::LargeBinary) => {
                        if metadata_field.is_nullable() || value_field.is_nullable() {
                            return Err(ArrowError::InvalidArgumentError(
                                "Variant struct fields must not be nullable".to_owned(),
                            ));
                        }
                        Ok(())
                    }
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

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        // First validate the data type
        let variant = Variant::new(Vec::new(), Vec::new());
        variant.supports_data_type(data_type)?;
        Ok(variant)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
        DataType, Field,
    };

    use super::*;

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let struct_type = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, false),
            ]
            .into(),
        );

        let mut field = Field::new("", struct_type, false);
        let variant = Variant::new(Vec::new(), Vec::new());

        field.try_with_extension_type(variant.clone())?;
        field.try_extension_type::<Variant>()?;

        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::Variant(variant)
        );

        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let struct_type = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, false),
            ]
            .into(),
        );

        let field = Field::new("", struct_type, false).with_metadata(
            [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "".to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<Variant>();
    }

    #[test]
    #[should_panic(expected = "Variant data type mismatch")]
    fn invalid_type() {
        Field::new("", DataType::Int8, false).with_extension_type(Variant::new(vec![], vec![]));
    }

    #[test]
    #[should_panic(expected = "Variant extension type expects an empty string as metadata")]
    fn invalid_metadata() {
        let struct_type = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, false),
            ]
            .into(),
        );

        let field = Field::new("", struct_type, false).with_metadata(
            [
                (EXTENSION_TYPE_NAME_KEY.to_owned(), Variant::NAME.to_owned()),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    "non-empty".to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<Variant>();
    }

    #[test]
    fn variant_supports_valid_data_types() {
        // Test valid struct types
        let valid_types = [
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::Binary, false),
                ]
                .into(),
            ),
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::LargeBinary, false),
                    Field::new("value", DataType::LargeBinary, false),
                ]
                .into(),
            ),
        ];

        for data_type in valid_types {
            let variant = Variant::new(vec![1], vec![2]);
            assert!(variant.supports_data_type(&data_type).is_ok());
        }

        // Test invalid types
        let invalid_types = [
            DataType::Utf8,
            DataType::Struct(vec![Field::new("single", DataType::Binary, false)].into()),
            DataType::Struct(
                vec![
                    Field::new("wrong1", DataType::Binary, false),
                    Field::new("wrong2", DataType::Binary, false),
                ]
                .into(),
            ),
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, true), // nullable
                    Field::new("value", DataType::Binary, false),
                ]
                .into(),
            ),
        ];

        for data_type in invalid_types {
            let variant = Variant::new(vec![1], vec![2]);
            assert!(variant.supports_data_type(&data_type).is_err());
        }
    }
}
