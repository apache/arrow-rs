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

//! Variable closedness range
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-closedness-range>

use crate::{ArrowError, DataType, Fields, extension::ExtensionType};

/// The extension type for a bounded set (mathematical interval) whose bound
/// inclusivity is recorded per value rather than as a single type-level
/// parameter.
///
/// Extension name: `arrow.variable_closedness_range`.
///
/// A range is a bounded set (mathematical interval) defined by a lower and
/// an upper bound over an orderable value type T. T may be any orderable
/// Arrow type, for example an integer, floating-point, decimal, date, time,
/// timestamp, duration, string or binary type. This specification defines
/// only the storage layout, not the order of any type.
///
/// The storage type is a `Struct` with exactly four fields, in order:
/// - `lower` (type T): the lower bound.
/// - `upper` (type T): the upper bound.
/// - `lower_inc` (non-nullable `Boolean`): whether the lower bound is
///   inclusive for that value.
/// - `upper_inc` (non-nullable `Boolean`): whether the upper bound is
///   inclusive for that value.
///
/// `lower` and `upper` must share the same data type T. Each of them may
/// independently be nullable or non-nullable: a nullable bound can hold
/// null to represent an unbounded (infinite) endpoint on that side, while a
/// non-nullable bound is always finite. A null bound is always treated as
/// exclusive, regardless of its `lower_inc` / `upper_inc` flag: only a null
/// bound means the range is unbounded on that side. `lower_inc` and
/// `upper_inc` are always non-nullable.
///
/// This type has no type-level parameters: inclusivity is carried per value
/// in the `lower_inc` and `upper_inc` fields rather than fixed by the type.
/// The extension metadata therefore serializes to the empty JSON object
/// `{}`.
///
/// Ranges that canonicalize to a single closedness shared by all values use
/// the companion `arrow.fixed_closedness_range` extension type instead.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-closedness-range>
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct VariableClosednessRange;

/// Validates that `data_type` is an acceptable storage type for
/// `arrow.variable_closedness_range`.
///
/// Checks that the data type is a struct with exactly four fields named
/// "lower", "upper", "lower_inc" and "upper_inc", in that order. "lower" and
/// "upper" must share the same data type; each may be nullable or
/// non-nullable independently. "lower_inc" and "upper_inc" must be
/// non-nullable `Boolean`.
fn validate_storage(data_type: &DataType) -> Result<(), ArrowError> {
    let fields: &Fields = match data_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "VariableClosednessRange data type mismatch, expected Struct, found {other}"
            )));
        }
    };

    if fields.len() != 4 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, expected Struct with 4 fields, found {} field(s)",
            fields.len()
        )));
    }

    let lower = &fields[0];
    let upper = &fields[1];
    let lower_inc = &fields[2];
    let upper_inc = &fields[3];

    if lower.name() != "lower" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, expected first field named \"lower\", found \"{}\"",
            lower.name()
        )));
    }

    if upper.name() != "upper" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, expected second field named \"upper\", found \"{}\"",
            upper.name()
        )));
    }

    if lower_inc.name() != "lower_inc" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, expected third field named \"lower_inc\", found \"{}\"",
            lower_inc.name()
        )));
    }

    if upper_inc.name() != "upper_inc" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, expected fourth field named \"upper_inc\", found \"{}\"",
            upper_inc.name()
        )));
    }

    if lower.data_type() != upper.data_type() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, \"lower\" and \"upper\" fields must have the same data type, found \"{}\" and \"{}\"",
            lower.data_type(),
            upper.data_type()
        )));
    }

    if lower_inc.data_type() != &DataType::Boolean || upper_inc.data_type() != &DataType::Boolean {
        return Err(ArrowError::InvalidArgumentError(format!(
            "VariableClosednessRange data type mismatch, \"lower_inc\" and \"upper_inc\" fields must be Boolean, found \"{}\" and \"{}\"",
            lower_inc.data_type(),
            upper_inc.data_type()
        )));
    }

    if lower_inc.is_nullable() || upper_inc.is_nullable() {
        return Err(ArrowError::InvalidArgumentError(
            "VariableClosednessRange data type mismatch, \"lower_inc\" and \"upper_inc\" fields must be non-nullable".to_owned(),
        ));
    }

    Ok(())
}

/// Validates that `metadata`, if present, is either empty or a valid JSON
/// object.
///
/// This type has no type-level parameters, so the metadata carries no
/// information. A missing metadata key and an empty string are both
/// accepted, as is any JSON object (unknown keys are ignored for
/// forward-compatibility). Anything else, such as malformed JSON or a JSON
/// value that is not an object, is rejected.
fn validate_metadata(metadata: Option<&str>) -> Result<(), ArrowError> {
    match metadata {
        None => Ok(()),
        Some("") => Ok(()),
        Some(value) => {
            let parsed: serde_json::Value = serde_json::from_str(value).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "VariableClosednessRange metadata deserialization failed: {e}"
                ))
            })?;
            if parsed.is_object() {
                Ok(())
            } else {
                Err(ArrowError::InvalidArgumentError(format!(
                    "VariableClosednessRange metadata must be a JSON object, found \"{value}\""
                )))
            }
        }
    }
}

impl ExtensionType for VariableClosednessRange {
    const NAME: &'static str = "arrow.variable_closedness_range";

    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        // There are no type-level parameters, but the metadata is still
        // emitted explicitly as the empty JSON object for forward-compat.
        Some("{}".to_owned())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        validate_metadata(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        validate_storage(data_type)
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type).map(|()| Self)
    }

    fn validate(data_type: &DataType, _metadata: Self::Metadata) -> Result<(), ArrowError> {
        Self.supports_data_type(data_type)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        Field,
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    };

    use super::*;

    fn make_range_struct(value_type: DataType) -> DataType {
        DataType::Struct(
            [
                Field::new("lower", value_type.clone(), true),
                Field::new("upper", value_type, true),
                Field::new("lower_inc", DataType::Boolean, false),
                Field::new("upper_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        )
    }

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let storage = make_range_struct(DataType::Int32);
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(VariableClosednessRange)?;
        field.try_extension_type::<VariableClosednessRange>()?;
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::VariableClosednessRange(VariableClosednessRange)
        );
        Ok(())
    }

    #[test]
    fn valid_string_value_type() -> Result<(), ArrowError> {
        // T may be any orderable Arrow type, including a string type.
        let storage = make_range_struct(DataType::Utf8);
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(VariableClosednessRange)?;
        field.try_extension_type::<VariableClosednessRange>()?;
        Ok(())
    }

    #[test]
    fn metadata_serializes_to_empty_object() -> Result<(), ArrowError> {
        let storage = make_range_struct(DataType::Int32);
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(VariableClosednessRange)?;
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_METADATA_KEY),
            Some(&"{}".to_owned())
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Extension type name missing")]
    fn missing_name() {
        let storage = make_range_struct(DataType::Int32);
        let field =
            Field::new("", storage, false).with_metadata([(EXTENSION_TYPE_METADATA_KEY, "{}")]);
        field.extension_type::<VariableClosednessRange>();
    }

    #[test]
    fn missing_metadata_is_accepted() -> Result<(), ArrowError> {
        // Unlike FixedClosednessRange, this type has no type-level
        // parameters, so a missing metadata key is equivalent to `{}`.
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false)
            .with_metadata([(EXTENSION_TYPE_NAME_KEY, VariableClosednessRange::NAME)]);
        field.try_extension_type::<VariableClosednessRange>()?;
        Ok(())
    }

    #[test]
    fn empty_and_object_and_unknown_keys_metadata_are_accepted() -> Result<(), ArrowError> {
        let storage = make_range_struct(DataType::Int32);
        for serialized in ["", "{}", r#"{"extra":42}"#] {
            let field = Field::new("", storage.clone(), false).with_metadata([
                (EXTENSION_TYPE_NAME_KEY, VariableClosednessRange::NAME),
                (EXTENSION_TYPE_METADATA_KEY, serialized),
            ]);
            field.try_extension_type::<VariableClosednessRange>()?;
        }
        Ok(())
    }

    #[test]
    #[should_panic(expected = "VariableClosednessRange metadata deserialization failed")]
    fn invalid_metadata_malformed_json() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false).with_metadata([
            (EXTENSION_TYPE_NAME_KEY, VariableClosednessRange::NAME),
            (EXTENSION_TYPE_METADATA_KEY, "{"),
        ]);
        field.extension_type::<VariableClosednessRange>();
    }

    #[test]
    #[should_panic(expected = "VariableClosednessRange metadata must be a JSON object")]
    fn invalid_metadata_not_an_object() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false).with_metadata([
            (EXTENSION_TYPE_NAME_KEY, VariableClosednessRange::NAME),
            (EXTENSION_TYPE_METADATA_KEY, "[]"),
        ]);
        field.extension_type::<VariableClosednessRange>();
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, expected Struct, found Int32"
    )]
    fn invalid_storage_non_struct() {
        Field::new("", DataType::Int32, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, expected Struct with 4 fields"
    )]
    fn invalid_storage_wrong_field_count() {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, expected first field named \"lower\""
    )]
    fn invalid_storage_wrong_field_names() {
        let storage = DataType::Struct(
            [
                Field::new("start", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
                Field::new("lower_inc", DataType::Boolean, false),
                Field::new("upper_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, expected third field named \"lower_inc\""
    )]
    fn invalid_storage_wrong_inc_field_order() {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
                Field::new("upper_inc", DataType::Boolean, false),
                Field::new("lower_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, \"lower\" and \"upper\" fields must have the same data type"
    )]
    fn invalid_storage_mismatched_bound_types() {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int64, true),
                Field::new("lower_inc", DataType::Boolean, false),
                Field::new("upper_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, \"lower_inc\" and \"upper_inc\" fields must be Boolean"
    )]
    fn invalid_storage_non_boolean_flags() {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
                Field::new("lower_inc", DataType::Int8, false),
                Field::new("upper_inc", DataType::Int8, false),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    #[should_panic(
        expected = "VariableClosednessRange data type mismatch, \"lower_inc\" and \"upper_inc\" fields must be non-nullable"
    )]
    fn invalid_storage_nullable_flags() {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
                Field::new("lower_inc", DataType::Boolean, true),
                Field::new("upper_inc", DataType::Boolean, true),
            ]
            .into_iter()
            .collect(),
        );
        Field::new("", storage, false).with_extension_type(VariableClosednessRange);
    }

    #[test]
    fn accepts_non_nullable_bounds() -> Result<(), ArrowError> {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, false),
                Field::new("upper", DataType::Int32, false),
                Field::new("lower_inc", DataType::Boolean, false),
                Field::new("upper_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        );
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(VariableClosednessRange)?;
        field.try_extension_type::<VariableClosednessRange>()?;
        Ok(())
    }

    #[test]
    fn accepts_asymmetric_bound_nullability() -> Result<(), ArrowError> {
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, false),
                Field::new("lower_inc", DataType::Boolean, false),
                Field::new("upper_inc", DataType::Boolean, false),
            ]
            .into_iter()
            .collect(),
        );
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(VariableClosednessRange)?;
        field.try_extension_type::<VariableClosednessRange>()?;
        Ok(())
    }
}
