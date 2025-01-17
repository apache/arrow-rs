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

//! UUID
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid>

use crate::{extension::ExtensionType, ArrowError, DataType};

/// The extension type for `UUID`.
///
/// Extension name: `arrow.uuid`.
///
/// The storage type of the extension is `FixedSizeBinary` with a length of
/// 16 bytes.
///
/// Note:
/// A specific UUID version is not required or guaranteed. This extension
/// represents UUIDs as `FixedSizeBinary(16)` with big-endian notation and
/// does not interpret the bytes in any way.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid>
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Uuid;

impl ExtensionType for Uuid {
    const NAME: &'static str = "arrow.uuid";

    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        metadata.map_or_else(
            || Ok(()),
            |_| {
                Err(ArrowError::InvalidArgumentError(
                    "Uuid extension type expects no metadata".to_owned(),
                ))
            },
        )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::FixedSizeBinary(16) => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Uuid data type mismatch, expected FixedSizeBinary(16), found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type).map(|_| Self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        extension::{CanonicalExtensionType, EXTENSION_TYPE_METADATA_KEY},
        Field,
    };

    use super::*;

    #[test]
    fn uuid() -> Result<(), ArrowError> {
        let mut field = Field::new("", DataType::FixedSizeBinary(16), false);
        field.try_with_extension_type(Uuid)?;
        assert!(field.try_extension_type::<Uuid>().is_ok());
        assert_eq!(
            field.try_canonical_extension_type().unwrap(),
            CanonicalExtensionType::Uuid(Uuid)
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "expected FixedSizeBinary(16), found FixedSizeBinary(8)")]
    fn uuid_bad_type() {
        Field::new("", DataType::FixedSizeBinary(8), false).with_extension_type(Uuid);
    }

    #[test]
    fn uuid_with_metadata() {
        // Add metadata that's not expected for uuid.
        let field = Field::new("", DataType::FixedSizeBinary(16), false)
            .with_extension_type(Uuid)
            .with_metadata(
                [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "".to_owned())]
                    .into_iter()
                    .collect(),
            );
        // This returns an error now because `Uuid` expects no metadata.
        assert!(field.try_extension_type::<Uuid>().is_err());
    }
}
