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

//! Extension types.
//!
//! <div class="warning">This module is experimental. There might be breaking changes between minor releases.</div>

#[cfg(feature = "canonical_extension_types")]
mod canonical;
#[cfg(feature = "canonical_extension_types")]
pub use canonical::*;

use crate::{ArrowError, DataType};

/// The metadata key for the string name identifying an [`ExtensionType`].
pub const EXTENSION_TYPE_NAME_KEY: &str = "ARROW:extension:name";

/// The metadata key for a serialized representation of the [`ExtensionType`]
/// necessary to reconstruct the custom type.
pub const EXTENSION_TYPE_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Extension types.
///
/// User-defined “extension” types can be defined setting certain key value
/// pairs in the [`Field`] metadata structure. These extension keys are:
/// - [`EXTENSION_TYPE_NAME_KEY`]
/// - [`EXTENSION_TYPE_METADATA_KEY`]
///
/// Canonical extension types support in this crate requires the
/// `canonical_extension_types` feature.
///
/// Extension types may or may not use the [`EXTENSION_TYPE_METADATA_KEY`]
/// field.
///
/// # Example
///
/// The example below demonstrates how to implement this trait for a `Uuid`
/// type. Note this is not the canonical extension type for `Uuid`, which does
/// not include information about the `Uuid` version.
///
/// ```
/// # use arrow_schema::ArrowError;
/// # fn main() -> Result<(), ArrowError> {
/// use arrow_schema::{DataType, extension::ExtensionType, Field};
/// use std::{fmt, str::FromStr};
///
/// /// The different Uuid versions.
/// #[derive(Clone, Copy, Debug, PartialEq)]
/// enum UuidVersion {
///     V1,
///     V2,
///     V3,
///     V4,
///     V5,
///     V6,
///     V7,
///     V8,
/// }
///
/// // We'll use `Display` to serialize.
/// impl fmt::Display for UuidVersion {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(
///             f,
///             "{}",
///             match self {
///                 Self::V1 => "V1",
///                 Self::V2 => "V2",
///                 Self::V3 => "V3",
///                 Self::V4 => "V4",
///                 Self::V5 => "V5",
///                 Self::V6 => "V6",
///                 Self::V7 => "V7",
///                 Self::V8 => "V8",
///             }
///         )
///     }
/// }
///
/// // And `FromStr` to deserialize.
/// impl FromStr for UuidVersion {
///     type Err = ArrowError;
///
///     fn from_str(s: &str) -> Result<Self, Self::Err> {
///         match s {
///             "V1" => Ok(Self::V1),
///             "V2" => Ok(Self::V2),
///             "V3" => Ok(Self::V3),
///             "V4" => Ok(Self::V4),
///             "V5" => Ok(Self::V5),
///             "V6" => Ok(Self::V6),
///             "V7" => Ok(Self::V7),
///             "V8" => Ok(Self::V8),
///             _ => Err(ArrowError::ParseError("Invalid UuidVersion".to_owned())),
///         }
///     }
/// }
///
/// /// This is the extension type, not the container for Uuid values. It
/// /// stores the Uuid version (this is the metadata of this extension type).
/// #[derive(Clone, Copy, Debug, PartialEq)]
/// struct Uuid(UuidVersion);
///
/// impl ExtensionType for Uuid {
///     // We use a namespace as suggested by the specification.
///     const NAME: &'static str = "myorg.example.uuid";
///
///     // The metadata type is the Uuid version.
///     type Metadata = UuidVersion;
///
///     // We just return a reference to the Uuid version.
///     fn metadata(&self) -> &Self::Metadata {
///         &self.0
///     }
///
///     // We use the `Display` implementation to serialize the Uuid
///     // version.
///     fn serialize_metadata(&self) -> Option<String> {
///         Some(self.0.to_string())
///     }
///
///     // We use the `FromStr` implementation to deserialize the Uuid
///     // version.
///     fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
///         metadata.map_or_else(
///             || {
///                 Err(ArrowError::InvalidArgumentError(
///                     "Uuid extension type metadata missing".to_owned(),
///                 ))
///             },
///             str::parse,
///         )
///     }
///
///     // The only supported data type is `FixedSizeBinary(16)`.
///     fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
///         match data_type {
///             DataType::FixedSizeBinary(16) => Ok(()),
///             data_type => Err(ArrowError::InvalidArgumentError(format!(
///                 "Uuid data type mismatch, expected FixedSizeBinary(16), found {data_type}"
///             ))),
///         }
///     }
///
///     // We should always check if the data type is supported before
///     // constructing the extension type.
///     fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
///         let uuid = Self(metadata);
///         uuid.supports_data_type(data_type)?;
///         Ok(uuid)
///     }
/// }
///
/// // We can now construct the extension type.
/// let uuid_v1 = Uuid(UuidVersion::V1);
///
/// // And add it to a field.
/// let mut field =
///     Field::new("", DataType::FixedSizeBinary(16), false).with_extension_type(uuid_v1);
///
/// // And extract it from this field.
/// assert_eq!(field.try_extension_type::<Uuid>()?, uuid_v1);
///
/// // When we try to add this to a field with an unsupported data type we
/// // get an error.
/// let result = Field::new("", DataType::Null, false).try_with_extension_type(uuid_v1);
/// assert!(result.is_err());
/// # Ok(()) }
/// ```
///
/// <https://arrow.apache.org/docs/format/Columnar.html#extension-types>
///
/// [`Field`]: crate::Field
pub trait ExtensionType: Sized {
    /// The name identifying this extension type.
    ///
    /// This is the string value that is used for the
    /// [`EXTENSION_TYPE_NAME_KEY`] in the [`Field::metadata`] of a [`Field`]
    /// to identify this extension type.
    ///
    /// We recommend that you use a “namespace”-style prefix for extension
    /// type names to minimize the possibility of conflicts with multiple Arrow
    /// readers and writers in the same application. For example, use
    /// `myorg.name_of_type` instead of simply `name_of_type`.
    ///
    /// Extension names beginning with `arrow.` are reserved for canonical
    /// extension types, they should not be used for third-party extension
    /// types.
    ///
    /// Extension names are case-sensitive.
    ///
    /// [`Field`]: crate::Field
    /// [`Field::metadata`]: crate::Field::metadata
    const NAME: &'static str;

    /// The metadata type of this extension type.
    ///
    /// Implementations can use strongly or loosly typed data structures here
    /// depending on the complexity of the metadata.
    ///
    /// Implementations can also use `Self` here if the extension type can be
    /// constructed directly from its metadata.
    ///
    /// If an extension type defines no metadata it should use `()` to indicate
    /// this.
    type Metadata;

    /// Returns a reference to the metadata of this extension type, or `&()` if
    /// if this extension type defines no metadata (`Self::Metadata=()`).
    fn metadata(&self) -> &Self::Metadata;

    /// Returns the serialized representation of the metadata of this extension
    /// type, or `None` if this extension type defines no metadata
    /// (`Self::Metadata=()`).
    ///
    /// This is string value that is used for the
    /// [`EXTENSION_TYPE_METADATA_KEY`] in the [`Field::metadata`] of a
    /// [`Field`].
    ///
    /// [`Field`]: crate::Field
    /// [`Field::metadata`]: crate::Field::metadata
    fn serialize_metadata(&self) -> Option<String>;

    /// Deserialize the metadata of this extension type from the serialized
    /// representation of the metadata. An extension type that defines no
    /// metadata should expect `None` for the serialized metadata and return
    /// `Ok(())`.
    ///
    /// This function should return an error when
    /// - expected metadata is missing (for extensions types with non-optional
    ///   metadata)
    /// - unexpected metadata is set (for extension types without metadata)
    /// - deserialization of metadata fails
    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError>;

    /// Returns `OK())` iff the given data type is supported by this extension
    /// type.
    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError>;

    /// Construct this extension type for a field with the given data type and
    /// metadata.
    ///
    /// This should return an error if the given data type is not supported by
    /// this extension type.
    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError>;
}
