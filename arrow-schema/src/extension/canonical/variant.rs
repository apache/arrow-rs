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
/// The storage type must be a struct with a binary `metadata` field and at
/// least one of a binary `value` field or a `typed_value` field. Nullability
/// is not checked, matching `VariantArray`, so files written with a nullable
/// unshredded `value` still read. Validating `typed_value` types and the
/// encoded bytes is left to `VariantArray`.
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
        let ok = match data_type {
            DataType::Struct(fields) => {
                let metadata_ok =
                    fields
                        .find("metadata")
                        .is_some_and(|(_, field)| match field.data_type() {
                            DataType::Dictionary(_, values) => is_binary(values),
                            DataType::RunEndEncoded(_, values) => is_binary(values.data_type()),
                            other => is_binary(other),
                        });
                let value_ok = match fields.find("value") {
                    Some((_, field)) => is_binary(field.data_type()),
                    None => fields.find("typed_value").is_some(),
                };
                metadata_ok && value_ok
            }
            _ => false,
        };

        ok.then_some(()).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "VariantType data type mismatch, expected Struct with binary \"metadata\" and binary \"value\" and/or \"typed_value\" fields, found {data_type}"
            ))
        })
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
            "Invalid argument error: VariantType data type mismatch, expected Struct with binary \"metadata\" and binary \"value\" and/or \"typed_value\" fields, found Binary"
        );

        let invalid = [
            storage(vec![Field::new("unrelated", DataType::Int32, false)]),
            storage(vec![metadata(DataType::Utf8), value(DataType::BinaryView)]),
            storage(vec![
                metadata(DataType::FixedSizeBinary(16)),
                value(DataType::BinaryView),
            ]),
            storage(vec![
                metadata(DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                )),
                value(DataType::BinaryView),
            ]),
            storage(vec![metadata(DataType::BinaryView), value(DataType::Utf8)]),
            storage(vec![metadata(DataType::BinaryView)]),
        ];
        for data_type in invalid {
            assert!(
                VariantType::validate(&data_type, "").is_err(),
                "{data_type}"
            );
        }
    }
}
