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

//! JSON
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#json>

use serde::{Deserialize, Serialize};

use crate::{extension::ExtensionType, ArrowError, DataType};

/// The extension type for `JSON`.
///
/// Extension name: `arrow.json`.
///
/// The storage type of this extension is `String` or `LargeString` or
/// `StringView`. Only UTF-8 encoded JSON as specified in [rfc8259](https://datatracker.ietf.org/doc/html/rfc8259)
/// is supported.
///
/// This type does not have any parameters.
///
/// Metadata is either an empty string or a JSON string with an empty
/// object. In the future, additional fields may be added, but they are not
/// required to interpret the array.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#json>
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Json(JsonMetadata);

/// Empty object
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// Extension type metadata for [`Json`].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct JsonMetadata(Option<Empty>);

impl ExtensionType for Json {
    const NAME: &'static str = "arrow.json";

    type Metadata = JsonMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(
            self.metadata()
                .0
                .as_ref()
                .map(serde_json::to_string)
                .map(Result::unwrap)
                .unwrap_or_else(|| "".to_owned()),
        )
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        const ERR: &str = "Json extension type metadata is either an empty string or a JSON string with an empty object";
        metadata
            .map_or_else(
                || Err(ArrowError::InvalidArgumentError(ERR.to_owned())),
                |metadata| {
                    match metadata {
                        // Empty string
                        "" => Ok(None),
                        value => serde_json::from_str::<Empty>(value)
                            .map(Option::Some)
                            .map_err(|_| ArrowError::InvalidArgumentError(ERR.to_owned())),
                    }
                },
            )
            .map(JsonMetadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Json data type mismatch, expected one of Utf8, LargeUtf8, Utf8View, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let json = Self(metadata);
        json.supports_data_type(data_type)?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
        Field,
    };

    use super::*;

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let mut field = Field::new("", DataType::Utf8, false);
        field.try_with_extension_type(Json::default())?;
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_METADATA_KEY),
            Some(&"".to_owned())
        );
        assert_eq!(
            field.try_extension_type::<Json>()?,
            Json(JsonMetadata(None))
        );

        let mut field = Field::new("", DataType::LargeUtf8, false);
        field.try_with_extension_type(Json(JsonMetadata(Some(Empty {}))))?;
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_METADATA_KEY),
            Some(&"{}".to_owned())
        );
        assert_eq!(
            field.try_extension_type::<Json>()?,
            Json(JsonMetadata(Some(Empty {})))
        );

        let mut field = Field::new("", DataType::Utf8View, false);
        field.try_with_extension_type(Json::default())?;
        field.try_extension_type::<Json>()?;
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::Json(Json::default())
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field = Field::new("", DataType::Int8, false).with_metadata(
            [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "{}".to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<Json>();
    }

    #[test]
    #[should_panic(expected = "expected one of Utf8, LargeUtf8, Utf8View, found Null")]
    fn invalid_type() {
        Field::new("", DataType::Null, false).with_extension_type(Json::default());
    }

    #[test]
    #[should_panic(
        expected = "Json extension type metadata is either an empty string or a JSON string with an empty object"
    )]
    fn invalid_metadata() {
        let field = Field::new("", DataType::Utf8, false).with_metadata(
            [
                (EXTENSION_TYPE_NAME_KEY.to_owned(), Json::NAME.to_owned()),
                (EXTENSION_TYPE_METADATA_KEY.to_owned(), "1234".to_owned()),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<Json>();
    }

    #[test]
    #[should_panic(
        expected = "Json extension type metadata is either an empty string or a JSON string with an empty object"
    )]
    fn missing_metadata() {
        let field = Field::new("", DataType::LargeUtf8, false).with_metadata(
            [(EXTENSION_TYPE_NAME_KEY.to_owned(), Json::NAME.to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<Json>();
    }
}
