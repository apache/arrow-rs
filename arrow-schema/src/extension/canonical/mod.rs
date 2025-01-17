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
