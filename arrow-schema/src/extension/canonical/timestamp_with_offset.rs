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

//! Timestamp with an offset in minutes
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#timestamp-with-offset>

use crate::{ArrowError, DataType, extension::ExtensionType};

/// The extension type for `TimestampWithOffset`.
///
/// Extension name: `arrow.timestamp_with_offset`.
///
/// The storage type of the extension is `Struct` where:
/// - `timestamp`: a `Timestamp(time_unit, Some("UTC"))`, where `time_unit` is any `TimeUnit`
/// - `offset_minutes`: a signed 16-bit integer (`Int16`) representing the offset in minutes
///   from the UTC timezone
///
/// This type has a `time_unit` type parameter which is a `TimeUnit`.
///
/// Metadata is either empty or an empty string.
///
/// This type represents a timestamp column that stores potentially different timezone offsets
/// per value. The timestamp is stored in UTC alongside the original timezone offset in minutes.
///
/// The main motivation behind this type is to have a way to represent tuples of the ANSI SQL
/// `TIMESTAMP WITH TIME ZONE`. Different database engines have different names for this.
///
/// It's worth noting that the data source needs to resolve timezone strings such as `UTC` or
/// `Americas/Los_Angeles` into an offset in minutes in order to construct a `TimestampWithOffset`.
/// This makes `TimestampWithOffset` type "lossy" in the sense that any original "unresolved"
/// timezone string gets lost in this conversion. It's a tradeoff for optimizing the row
/// representation and simplifying the client code, which does not need to know how to convert
/// from timezone string to its corresponding offset in minutes.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#timestamp-with-offset>
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct TimestampWithOffset;

const TIMESTAMP_FIELD_NAME: &str = "timestamp";
const OFFSET_FIELD_NAME: &str = "offset_minutes";

impl ExtensionType for TimestampWithOffset {
    const NAME: &'static str = "arrow.timestamp_with_offset";

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
            |v| {
                if !v.is_empty() {
                    Err(ArrowError::InvalidArgumentError(
                        "TimestampWithOffset extension type expects no metadata".to_owned(),
                    ))
                } else {
                    Ok(())
                }
            },
        )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let ok = match data_type {
            DataType::Struct(fields) => match fields.len() {
                2 => {
                    let maybe_timestamp = fields.first().unwrap();
                    let maybe_offset = fields.get(1).unwrap();

                    let timestamp_type_ok = matches!(maybe_timestamp.data_type(), DataType::Timestamp(_, tz) if {
                        match tz {
                            Some(tz) => {
                                tz.as_ref() == "UTC"
                            },
                            None => false
                        }
                    });

                    maybe_timestamp.name() == TIMESTAMP_FIELD_NAME
                        && timestamp_type_ok
                        && !maybe_timestamp.is_nullable()
                        && maybe_offset.name() == OFFSET_FIELD_NAME
                        && matches!(maybe_offset.data_type(), DataType::Int16)
                        && !maybe_offset.is_nullable()
                }
                _ => false,
            },
            _ => false,
        };

        match ok {
            true => Ok(()),
            false => Err(ArrowError::InvalidArgumentError(format!(
                "TimestampWithOffset data type mismatch, expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type).map(|_| Self)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        Field, Fields, TimeUnit,
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    };

    use super::*;

    fn make_valid_field(time_unit: TimeUnit) -> Field {
        Field::new(
            "",
            DataType::Struct(Fields::from_iter([
                Field::new(
                    TIMESTAMP_FIELD_NAME,
                    DataType::Timestamp(time_unit, Some("UTC".into())),
                    false,
                ),
                Field::new(OFFSET_FIELD_NAME, DataType::Int16, false),
            ])),
            false,
        )
    }

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let time_units = [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];

        for time_unit in time_units {
            let mut field = make_valid_field(time_unit);
            field.try_with_extension_type(TimestampWithOffset)?;
            field.try_extension_type::<TimestampWithOffset>()?;
            #[cfg(feature = "canonical_extension_types")]
            assert_eq!(
                field.try_canonical_extension_type()?,
                CanonicalExtensionType::TimestampWithOffset(TimestampWithOffset)
            );
        }

        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field = make_valid_field(TimeUnit::Second)
            .with_metadata([(EXTENSION_TYPE_METADATA_KEY.to_owned(), "".to_owned())].into());
        field.extension_type::<TimestampWithOffset>();
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Boolean"
    )]
    fn invalid_type_top_level() {
        Field::new("", DataType::Boolean, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_struct_field_count() {
        let data_type =
            DataType::Struct(Fields::from_iter([Field::new("", DataType::Int16, false)]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_wrong_timestamp_type() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(TIMESTAMP_FIELD_NAME, DataType::Int16, false),
            Field::new(OFFSET_FIELD_NAME, DataType::Int16, false),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_wrong_offset_type() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(
                TIMESTAMP_FIELD_NAME,
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                false,
            ),
            Field::new(OFFSET_FIELD_NAME, DataType::UInt64, false),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_nullable_timestamp() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(
                TIMESTAMP_FIELD_NAME,
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            Field::new(OFFSET_FIELD_NAME, DataType::Int16, false),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_nullable_offset() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(
                TIMESTAMP_FIELD_NAME,
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                false,
            ),
            Field::new(OFFSET_FIELD_NAME, DataType::Int16, true),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_no_timezone() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(
                TIMESTAMP_FIELD_NAME,
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new(OFFSET_FIELD_NAME, DataType::Int16, false),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    #[should_panic(
        expected = "expected Struct(\"timestamp\": Timestamp(_, Some(\"UTC\")), \"offset_minutes\": Int16), found Struct"
    )]
    fn invalid_type_wrong_timezone() {
        let data_type = DataType::Struct(Fields::from_iter([
            Field::new(
                TIMESTAMP_FIELD_NAME,
                DataType::Timestamp(TimeUnit::Second, Some("Americas/Sao_Paulo".into())),
                false,
            ),
            Field::new(OFFSET_FIELD_NAME, DataType::Int16, false),
        ]));
        Field::new("", data_type, false).with_extension_type(TimestampWithOffset);
    }

    #[test]
    fn no_metadata() {
        let field = make_valid_field(TimeUnit::Second).with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                TimestampWithOffset::NAME.to_owned(),
            )]
            .into(),
        );
        field.extension_type::<TimestampWithOffset>();
    }

    #[test]
    fn empty_metadata() {
        let field = make_valid_field(TimeUnit::Second).with_metadata(
            [
                (
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    TimestampWithOffset::NAME.to_owned(),
                ),
                (EXTENSION_TYPE_METADATA_KEY.to_owned(), String::new()),
            ]
            .into(),
        );
        field.extension_type::<TimestampWithOffset>();
    }
}
