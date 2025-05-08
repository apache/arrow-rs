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
mod fixed_shape_tensor;
pub use fixed_shape_tensor::{FixedShapeTensor, FixedShapeTensorMetadata};
mod json;
pub use json::{Json, JsonMetadata};
mod opaque;
pub use opaque::{Opaque, OpaqueMetadata};
mod uuid;
pub use uuid::Uuid;
mod variable_shape_tensor;
pub use variable_shape_tensor::{VariableShapeTensor, VariableShapeTensorMetadata};

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

    /// The extension type for `Bool8`.
    ///
    /// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#bit-boolean>
    Bool8(Bool8),
}

impl TryFrom<&Field> for CanonicalExtensionType {
    type Error = ArrowError;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        // Canonical extension type names start with `arrow.`
        match value.extension_type_name() {
            // An extension type name with an `arrow.` prefix
            Some(name) if name.starts_with("arrow.") => match name {
                FixedShapeTensor::NAME => value.try_extension_type::<FixedShapeTensor>().map(Into::into),
                VariableShapeTensor::NAME => value.try_extension_type::<VariableShapeTensor>().map(Into::into),
                Json::NAME => value.try_extension_type::<Json>().map(Into::into),
                Uuid::NAME => value.try_extension_type::<Uuid>().map(Into::into),
                Opaque::NAME => value.try_extension_type::<Opaque>().map(Into::into),
                Bool8::NAME => value.try_extension_type::<Bool8>().map(Into::into),
                _ => Err(ArrowError::InvalidArgumentError(format!("Unsupported canonical extension type: {name}"))),
            },
            // Name missing the expected prefix
            Some(name) => Err(ArrowError::InvalidArgumentError(format!(
                "Field extension type name mismatch, expected a name with an `arrow.` prefix, found {name}"
            ))),
            // Name missing
            None => Err(ArrowError::InvalidArgumentError("Field extension type name missing".to_owned())),
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

impl From<Bool8> for CanonicalExtensionType {
    fn from(value: Bool8) -> Self {
        CanonicalExtensionType::Bool8(value)
    }
}
