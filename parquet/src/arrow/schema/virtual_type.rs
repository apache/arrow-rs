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

//! RowNumber
//!

use arrow_schema::{ArrowError, DataType, Field, extension::ExtensionType};

/// Prefix for virtual column extension type names.
macro_rules! VIRTUAL_PREFIX {
    () => {
        "parquet.virtual."
    };
}

/// The extension type for row numbers.
///
/// Extension name: `parquet.virtual.row_number`.
///
/// This virtual column has storage type `Int64` and uses empty string metadata.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RowNumber;

impl ExtensionType for RowNumber {
    const NAME: &'static str = concat!(VIRTUAL_PREFIX!(), "row_number");
    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::default())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        if metadata.is_some_and(str::is_empty) {
            Ok("")
        } else {
            Err(ArrowError::InvalidArgumentError(
                "Virtual column extension type expects an empty string as metadata".to_owned(),
            ))
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Int64 => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Virtual column data type mismatch, expected Int64, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type).map(|_| Self)
    }
}

/// Returns `true` if the field is a virtual column.
///
/// Virtual columns have extension type names starting with `parquet.virtual.`.
pub fn is_virtual_column(field: &Field) -> bool {
    field
        .extension_type_name()
        .is_some_and(|name| name.starts_with(VIRTUAL_PREFIX!()))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{
        ArrowError, DataType, Field,
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    };

    use super::*;

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let mut field = Field::new("", DataType::Int64, false);
        field.try_with_extension_type(RowNumber)?;
        field.try_extension_type::<RowNumber>()?;

        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field = Field::new("", DataType::Int64, false).with_metadata(
            [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "".to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<RowNumber>();
    }

    #[test]
    #[should_panic(expected = "expected Int64, found Int32")]
    fn invalid_type() {
        Field::new("", DataType::Int32, false).with_extension_type(RowNumber);
    }

    #[test]
    #[should_panic(expected = "Virtual column extension type expects an empty string as metadata")]
    fn missing_metadata() {
        let field = Field::new("", DataType::Int64, false).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                RowNumber::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        field.extension_type::<RowNumber>();
    }

    #[test]
    #[should_panic(expected = "Virtual column extension type expects an empty string as metadata")]
    fn invalid_metadata() {
        let field = Field::new("", DataType::Int64, false).with_metadata(
            [
                (
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    RowNumber::NAME.to_owned(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    "non-empty".to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<RowNumber>();
    }
}
