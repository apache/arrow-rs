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

//! Big decimal canonical extension type.

use crate::{ArrowError, DataType, extension::ExtensionType};
use serde_json::{Map, Number, Value};

/// The only supported layout version for [`BigDecimal`].
pub const BIG_DECIMAL_LAYOUT_VERSION: i8 = 1;

/// Informational schema-level metadata for [`BigDecimal`].
///
/// Optional values are part of the extension type identity. They describe the
/// source column and do not replace the precision and scale encoded per value.
/// They are not used to accept or reject row values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BigDecimalMetadata {
    max_precision: Option<i32>,
    max_scale: Option<i16>,
    supports_infinity: Option<bool>,
    supports_nan: Option<bool>,
}

impl BigDecimalMetadata {
    /// Creates metadata after validating its constraints.
    pub fn try_new(
        max_precision: Option<i32>,
        max_scale: Option<i16>,
        supports_infinity: Option<bool>,
        supports_nan: Option<bool>,
    ) -> Result<Self, ArrowError> {
        if max_precision.is_some_and(|v| v <= 0) {
            return Err(ArrowError::InvalidArgumentError(
                "BigDecimal max_precision must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            max_precision,
            max_scale,
            supports_infinity,
            supports_nan,
        })
    }

    /// Returns the packed value layout version.
    pub fn layout_version(&self) -> i8 {
        BIG_DECIMAL_LAYOUT_VERSION
    }

    /// Returns the declared maximum significant decimal digits.
    pub fn max_precision(&self) -> Option<i32> {
        self.max_precision
    }

    /// Returns the declared maximum scale.
    pub fn max_scale(&self) -> Option<i16> {
        self.max_scale
    }

    /// Returns whether the source explicitly supports infinities.
    pub fn supports_infinity(&self) -> Option<bool> {
        self.supports_infinity
    }

    /// Returns whether the source explicitly supports NaNs.
    pub fn supports_nan(&self) -> Option<bool> {
        self.supports_nan
    }
}

/// The canonical `arrow.big_decimal` extension type.
///
/// The storage type is [`DataType::BinaryView`]. Each non-null value contains
/// a class byte, a little-endian signed 16-bit scale, and, for finite values,
/// a minimal unsigned little-endian magnitude.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BigDecimal(BigDecimalMetadata);

impl BigDecimal {
    /// Creates an extension type with the provided metadata.
    pub fn new(metadata: BigDecimalMetadata) -> Self {
        Self(metadata)
    }
}

impl ExtensionType for BigDecimal {
    const NAME: &'static str = "arrow.big_decimal";

    type Metadata = BigDecimalMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        let metadata = self.metadata();
        if metadata == &BigDecimalMetadata::default() {
            return Some(String::new());
        }

        let mut object = Map::new();
        if let Some(value) = metadata.max_precision {
            object.insert(
                "max_precision".to_owned(),
                Value::Number(Number::from(value)),
            );
        }
        if let Some(value) = metadata.max_scale {
            object.insert("max_scale".to_owned(), Value::Number(Number::from(value)));
        }
        if let Some(value) = metadata.supports_infinity {
            object.insert("supports_infinity".to_owned(), Value::Bool(value));
        }
        if let Some(value) = metadata.supports_nan {
            object.insert("supports_nan".to_owned(), Value::Bool(value));
        }
        Some(Value::Object(object).to_string())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        let Some(metadata) = metadata else {
            return Ok(BigDecimalMetadata::default());
        };
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Ok(BigDecimalMetadata::default());
        }

        let value: Value = serde_json::from_str(metadata).map_err(|error| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid BigDecimal extension metadata: {error}"
            ))
        })?;
        let object = value.as_object().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "BigDecimal extension metadata must be a JSON object".to_owned(),
            )
        })?;

        for key in object.keys() {
            match key.as_str() {
                "layout_version" | "max_precision" | "max_scale" | "supports_infinity"
                | "supports_nan" => {}
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Unknown BigDecimal extension metadata field: {key}"
                    )));
                }
            }
        }

        if let Some(version) = optional_i64(object, "layout_version")?
            && version != i64::from(BIG_DECIMAL_LAYOUT_VERSION)
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported BigDecimal layout_version: {version}"
            )));
        }

        let max_precision = optional_i64(object, "max_precision")?
            .map(|value| {
                i32::try_from(value).map_err(|_| {
                    ArrowError::InvalidArgumentError(
                        "BigDecimal max_precision is outside the int32 range".to_owned(),
                    )
                })
            })
            .transpose()?;
        let max_scale = optional_i64(object, "max_scale")?
            .map(|value| {
                i16::try_from(value).map_err(|_| {
                    ArrowError::InvalidArgumentError(
                        "BigDecimal max_scale is outside the int16 range".to_owned(),
                    )
                })
            })
            .transpose()?;
        let supports_infinity = optional_bool(object, "supports_infinity")?;
        let supports_nan = optional_bool(object, "supports_nan")?;

        BigDecimalMetadata::try_new(max_precision, max_scale, supports_infinity, supports_nan)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::BinaryView => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "BigDecimal data type mismatch, expected BinaryView, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let value = Self(metadata);
        value.supports_data_type(data_type)?;
        Ok(value)
    }

    fn validate(data_type: &DataType, metadata: Self::Metadata) -> Result<(), ArrowError> {
        Self(metadata).supports_data_type(data_type)
    }
}

fn optional_i64(object: &Map<String, Value>, name: &str) -> Result<Option<i64>, ArrowError> {
    object
        .get(name)
        .map(|value| {
            value.as_i64().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("BigDecimal {name} must be an integer"))
            })
        })
        .transpose()
}

fn optional_bool(object: &Map<String, Value>, name: &str) -> Result<Option<bool>, ArrowError> {
    object
        .get(name)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("BigDecimal {name} must be a boolean"))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Field, extension::CanonicalExtensionType};

    #[test]
    fn metadata_round_trip_and_identity() {
        let metadata =
            BigDecimalMetadata::try_new(Some(38), Some(10), Some(true), Some(false)).unwrap();
        let extension = BigDecimal::new(metadata.clone());
        let serialized = extension.serialize_metadata().unwrap();
        assert_eq!(
            serialized,
            r#"{"max_precision":38,"max_scale":10,"supports_infinity":true,"supports_nan":false}"#
        );
        assert_eq!(
            BigDecimal::deserialize_metadata(Some(&serialized)).unwrap(),
            metadata
        );

        let mut field = Field::new("value", DataType::BinaryView, true);
        field.try_with_extension_type(extension.clone()).unwrap();
        assert_eq!(field.try_extension_type::<BigDecimal>().unwrap(), extension);
        assert_eq!(
            CanonicalExtensionType::try_from(&field).unwrap(),
            CanonicalExtensionType::BigDecimal(extension)
        );
    }

    #[test]
    fn default_metadata_forms_are_equivalent() {
        assert_eq!(
            BigDecimal::deserialize_metadata(None).unwrap(),
            BigDecimalMetadata::default()
        );
        assert_eq!(
            BigDecimal::deserialize_metadata(Some("")).unwrap(),
            BigDecimalMetadata::default()
        );
        assert_eq!(
            BigDecimal::deserialize_metadata(Some("{}")).unwrap(),
            BigDecimalMetadata::default()
        );
        assert_eq!(
            BigDecimal::deserialize_metadata(Some(r#"{"layout_version":1}"#)).unwrap(),
            BigDecimalMetadata::default()
        );
    }

    #[test]
    fn rejects_invalid_metadata_or_storage() {
        for metadata in [
            r#"{"layout_version":2}"#,
            r#"{"max_precision":0}"#,
            r#"{"max_scale":32768}"#,
            r#"{"supports_nan":1}"#,
            r#"{"unknown":true}"#,
        ] {
            assert!(BigDecimal::deserialize_metadata(Some(metadata)).is_err());
        }
        assert!(
            BigDecimal::default()
                .supports_data_type(&DataType::Binary)
                .is_err()
        );
    }
}
