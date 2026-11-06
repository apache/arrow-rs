// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Canonical extension types.
//!
//! The Arrow columnar format allows defining extension types so as to extend
//! standard Arrow data types with custom semantics. Often these semantics will
//! be specific to a system or application. However, it is beneficial to share
//! the definitions of well-known extension types so as to improve
//! interoperability between different systems integrating Arrow columnar data.
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#format-canonical-extensions>

mod bool8;
pub use bool8::Bool8;
mod fixed_closedness_range;
pub use fixed_closedness_range::{FixedClosednessRange, FixedClosednessRangeMetadata, RangeClosed};
mod fixed_shape_tensor;
pub use fixed_shape_tensor::{FixedShapeTensor, FixedShapeTensorMetadata};
mod json;
pub use json::{Json, JsonMetadata};
mod opaque;
pub use opaque::{Opaque, OpaqueMetadata};
mod timestamp_with_offset;
pub use timestamp_with_offset::TimestampWithOffset;
mod uuid;
pub use uuid::Uuid;
mod variable_closedness_range;
pub use variable_closedness_range::VariableClosednessRange;
mod variable_shape_tensor;
pub use variable_shape_tensor::{VariableShapeTensor, VariableShapeTensorMetadata};
mod variant;
pub use variant::VariantType;

use crate::{ArrowError, Field};

use super::ExtensionType;

/// Canonical extension types.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#format-canonical-extensions>
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalExtensionType {
    /// The extension type for `FixedShapeTensor`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor>
    FixedShapeTensor(FixedShapeTensor),

    /// The extension type for `VariableShapeTensor`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor>
    VariableShapeTensor(VariableShapeTensor),

    /// The extension type for 'JSON'.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#json>
    Json(Json),

    /// The extension type for `UUID`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid>
    Uuid(Uuid),

    /// The extension type for `Opaque`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#opaque>
    Opaque(Opaque),

    /// The extension type for `FixedClosednessRange`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-closedness-range>
    FixedClosednessRange(FixedClosednessRange),

    /// The extension type for `VariableClosednessRange`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-closedness-range>
    VariableClosednessRange(VariableClosednessRange),

    /// The extension type for `Bool8`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#bit-boolean>
    Bool8(Bool8),

    /// The extension type for `TimestampWithOffset`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#timestamp-with-offset>
    TimestampWithOffset(TimestampWithOffset),

    /// The extension type for Parquet Variant.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant>
    Variant(VariantType),
}

impl TryFrom<&Field> for CanonicalExtensionType {
    type Error = ArrowError;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        // Canonical extension type names start with `arrow.`
        match value.extension_type_name() {
            // An extension type name with an `arrow.` prefix
            Some(name) if name.starts_with("arrow.") => match name {
                FixedShapeTensor::NAME => value
                    .try_extension_type::<FixedShapeTensor>()
                    .map(Into::into),
                VariableShapeTensor::NAME => value
                    .try_extension_type::<VariableShapeTensor>()
                    .map(Into::into),
                Json::NAME => value.try_extension_type::<Json>().map(Into::into),
                Uuid::NAME => value.try_extension_type::<Uuid>().map(Into::into),
                Opaque::NAME => value.try_extension_type::<Opaque>().map(Into::into),
                FixedClosednessRange::NAME => value
                    .try_extension_type::<FixedClosednessRange>()
                    .map(Into::into),
                VariableClosednessRange::NAME => value
                    .try_extension_type::<VariableClosednessRange>()
                    .map(Into::into),
                Bool8::NAME => value.try_extension_type::<Bool8>().map(Into::into),
                TimestampWithOffset::NAME => value
                    .try_extension_type::<TimestampWithOffset>()
                    .map(Into::into),
                VariantType::NAME => value.try_extension_type::<VariantType>().map(Into::into),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported canonical extension type: {name}"
                ))),
            },
            // Name missing the expected prefix
            Some(name) => Err(ArrowError::InvalidArgumentError(format!(
                "Field extension type name mismatch, expected a name with an `arrow.` prefix, found {name}"
            ))),
            // Name missing
            None => Err(ArrowError::InvalidArgumentError(
                "Field extension type name missing".to_owned(),
            )),
        }
    }
}

impl From<FixedShapeTensor> for CanonicalExtensionType {
    fn from(value: FixedShapeTensor) -> Self {
        CanonicalExtensionType::FixedShapeTensor(value)
    }
}

impl From<VariableShapeTensor> for CanonicalExtensionType {
    fn from(value: VariableShapeTensor) -> Self {
        CanonicalExtensionType::VariableShapeTensor(value)
    }
}

impl From<Json> for CanonicalExtensionType {
    fn from(value: Json) -> Self {
        CanonicalExtensionType::Json(value)
    }
}

impl From<Uuid> for CanonicalExtensionType {
    fn from(value: Uuid) -> Self {
        CanonicalExtensionType::Uuid(value)
    }
}

impl From<Opaque> for CanonicalExtensionType {
    fn from(value: Opaque) -> Self {
        CanonicalExtensionType::Opaque(value)
    }
}

impl From<FixedClosednessRange> for CanonicalExtensionType {
    fn from(value: FixedClosednessRange) -> Self {
        CanonicalExtensionType::FixedClosednessRange(value)
    }
}

impl From<VariableClosednessRange> for CanonicalExtensionType {
    fn from(value: VariableClosednessRange) -> Self {
        CanonicalExtensionType::VariableClosednessRange(value)
    }
}

impl From<Bool8> for CanonicalExtensionType {
    fn from(value: Bool8) -> Self {
        CanonicalExtensionType::Bool8(value)
    }
}

impl From<TimestampWithOffset> for CanonicalExtensionType {
    fn from(value: TimestampWithOffset) -> Self {
        CanonicalExtensionType::TimestampWithOffset(value)
    }
}

impl From<VariantType> for CanonicalExtensionType {
    fn from(value: VariantType) -> Self {
        CanonicalExtensionType::Variant(value)
    }
}
