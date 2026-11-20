// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`VariantType`] Arrow extension type

use crate::extension::ExtensionType;
use crate::{ArrowError, DataType};

/// Arrow Variant [`ExtensionType`].
///
/// Represents the canonical Arrow Extension Type for storing variants.
/// See [`VariantArray`] in the `parquet-variant-compute` crate for more examples
/// of using this extension type.
///
/// # Storage
///
/// The storage type must be a struct with the following fields, in any order:
///
/// 1. `metadata`: binary, large_binary, or binary_view, optionally dictionary
///    or run-end encoded
///
/// 2. `value`: binary, large_binary, or binary_view, required unless
///    `typed_value` is present
///
/// 3. `typed_value`: optional shredded values
///
/// The `typed_value` types and the encoded bytes are not checked here, see
/// [`VariantArray`].
///
/// [`VariantArray`]: https://docs.rs/parquet-variant-compute/latest/parquet_variant_compute/struct.VariantArray.html
#[derive(Debug, Clone, PartialEq)]
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
        // `DataType::is_binary` also accepts FixedSizeBinary
        let is_binary = |data_type: &DataType| {
            matches!(
                data_type,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
        };
        let err = |message: String| Err(ArrowError::InvalidArgumentError(message));

        let DataType::Struct(fields) = data_type else {
            return err(format!(
                "VariantType only supports StructArray, got {data_type}"
            ));
        };

        let Some((_, metadata)) = fields.find("metadata") else {
            return err(
                "Invalid VariantType: StructArray must contain a 'metadata' field".to_string(),
            );
        };
        let metadata_ok = match metadata.data_type() {
            DataType::Dictionary(_, values) => is_binary(values),
            DataType::RunEndEncoded(_, values) => is_binary(values.data_type()),
            other => is_binary(other),
        };
        if !metadata_ok {
            return err(format!(
                "VariantType 'metadata' field must be Binary, LargeBinary, BinaryView, or a Dictionary or RunEndEncoded array of one of those types, got {}",
                metadata.data_type()
            ));
        }

        match fields.find("value") {
            Some((_, value)) if !is_binary(value.data_type()) => err(format!(
                "VariantType 'value' field must be Binary, LargeBinary, or BinaryView, got {}",
                value.data_type()
            )),
            None if fields.find("typed_value").is_none() => err(
                "Invalid VariantType: StructArray must contain a 'value' or 'typed_value' field"
                    .to_string(),
            ),
            _ => Ok(()),
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
    use crate::extension::CanonicalExtensionType;
    use crate::{Field, Fields};
    use std::sync::Arc;

    fn storage(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    fn metadata(data_type: DataType) -> Field {
        Field::new("metadata", data_type, false)
    }

    fn value(data_type: DataType) -> Field {
        Field::new("value", data_type, true)
    }

    fn typed_value(data_type: DataType) -> Field {
        Field::new("typed_value", data_type, true)
    }

    #[test]
    fn variant_type_canonical_extension() {
        let data_type = storage(vec![
            metadata(DataType::BinaryView),
            value(DataType::BinaryView),
        ]);
        let field = Field::new("v", data_type, true).with_extension_type(VariantType);
        assert!(field.has_valid_extension_type::<VariantType>());
        assert_eq!(
            field.try_canonical_extension_type().unwrap(),
            CanonicalExtensionType::Variant(VariantType),
        );
    }

    #[test]
    fn variant_type_accepts_valid_storage() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary));
        let run_end_encoded = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::LargeBinary, true)),
        );
        let valid = [
            storage(vec![metadata(DataType::Binary), value(DataType::Binary)]),
            storage(vec![
                value(DataType::LargeBinary),
                metadata(DataType::LargeBinary),
            ]),
            storage(vec![metadata(dictionary), value(DataType::BinaryView)]),
            storage(vec![metadata(run_end_encoded), value(DataType::BinaryView)]),
            storage(vec![
                metadata(DataType::BinaryView),
                value(DataType::BinaryView),
                typed_value(DataType::Int64),
            ]),
            // Allowed by the Arrow spec, though the Parquet shredding spec requires `value`
            storage(vec![
                metadata(DataType::BinaryView),
                typed_value(DataType::Utf8),
            ]),
        ];
        for data_type in valid {
            VariantType::validate(&data_type, "").unwrap();
        }
    }

    #[test]
    fn variant_type_rejects_invalid_storage() {
        let err = Field::new("v", DataType::Binary, true)
            .try_with_extension_type(VariantType)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: VariantType only supports StructArray, got Binary"
        );

        let invalid = [
            (
                storage(vec![Field::new("unrelated", DataType::Int32, false)]),
                "Invalid VariantType: StructArray must contain a 'metadata' field",
            ),
            (
                storage(vec![
                    metadata(DataType::FixedSizeBinary(16)),
                    value(DataType::BinaryView),
                ]),
                "VariantType 'metadata' field must be Binary, LargeBinary, BinaryView, or a Dictionary or RunEndEncoded array of one of those types, got FixedSizeBinary(16)",
            ),
            (
                storage(vec![
                    metadata(DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::Utf8),
                    )),
                    value(DataType::BinaryView),
                ]),
                "VariantType 'metadata' field must be Binary, LargeBinary, BinaryView, or a Dictionary or RunEndEncoded array of one of those types, got Dictionary(Int8, Utf8)",
            ),
            (
                storage(vec![metadata(DataType::BinaryView), value(DataType::Utf8)]),
                "VariantType 'value' field must be Binary, LargeBinary, or BinaryView, got Utf8",
            ),
            (
                storage(vec![metadata(DataType::BinaryView)]),
                "Invalid VariantType: StructArray must contain a 'value' or 'typed_value' field",
            ),
        ];
        for (data_type, expected) in invalid {
            let err = VariantType::validate(&data_type, "").unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("Invalid argument error: {expected}")
            );
        }
    }
}
