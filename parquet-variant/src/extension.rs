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

//! [`VariantType`] Arrow extension type

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};

/// Arrow Variant [`ExtensionType`].
///
/// Represents the canonical Arrow Extension Type for storing variants.
/// See [`VariantArray`] in the `parquet-variant-compute` crate for more examples
/// of using this extension type.
///
/// [`VariantArray`]: https://docs.rs/parquet-variant-compute/latest/parquet_variant_compute/struct.VariantArray.html
pub struct VariantType;

impl ExtensionType for VariantType {
    const NAME: &'static str = "arrow.parquet.variant";

    // Variants extension metadata is an empty string
    // <https://github.com/apache/arrow/blob/d803afcc43f5d132506318fd9e162d33b2c3d4cd/docs/source/format/CanonicalExtensions.rst?plain=1#L473>
    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::new())
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Ok("")
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        if matches!(data_type, DataType::Struct(_)) {
            Ok(())
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "VariantType only supports StructArray, got {data_type}"
            )))
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type)?;
        Ok(Self)
    }

    fn validate(data_type: &DataType, _metadata: Self::Metadata) -> Result<(), ArrowError> {
        Self.supports_data_type(data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Fields};

    #[test]
    fn variant_type_requires_struct() {
        let field = Field::new("v", DataType::Struct(Fields::empty()), true)
            .with_extension_type(VariantType);
        assert!(field.has_valid_extension_type::<VariantType>());

        let err = Field::new("v", DataType::Binary, true)
            .try_with_extension_type(VariantType)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: VariantType only supports StructArray, got Binary"
        );
    }
}
