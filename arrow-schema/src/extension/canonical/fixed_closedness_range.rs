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

//! Fixed closedness range
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-closedness-range>

use serde_core::de::{IgnoredAny, MapAccess, Visitor};
use serde_core::ser::SerializeStruct;
use serde_core::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ArrowError, DataType, Fields, extension::ExtensionType};

/// Which bound(s) of a [`FixedClosednessRange`] interval are inclusive.
///
/// Uses the same vocabulary as pandas: "left", "right", "both", "neither".
/// A null (unbounded) bound is always treated as exclusive, regardless of
/// this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeClosed {
    /// The left (lower) endpoint is included; the right (upper) is excluded.
    Left,
    /// The left (lower) endpoint is excluded; the right (upper) is included.
    Right,
    /// Both endpoints are included (closed interval).
    Both,
    /// Neither endpoint is included (open interval).
    Neither,
}

impl Serialize for RangeClosed {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            RangeClosed::Left => "left",
            RangeClosed::Right => "right",
            RangeClosed::Both => "both",
            RangeClosed::Neither => "neither",
        })
    }
}

struct RangeClosedVisitor;

impl Visitor<'_> for RangeClosedVisitor {
    type Value = RangeClosed;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("one of \"left\", \"right\", \"both\", \"neither\"")
    }

    fn visit_str<E>(self, value: &str) -> Result<RangeClosed, E>
    where
        E: serde_core::de::Error,
    {
        match value {
            "left" => Ok(RangeClosed::Left),
            "right" => Ok(RangeClosed::Right),
            "both" => Ok(RangeClosed::Both),
            "neither" => Ok(RangeClosed::Neither),
            _ => Err(serde_core::de::Error::unknown_variant(
                value,
                &["left", "right", "both", "neither"],
            )),
        }
    }
}

impl<'de> Deserialize<'de> for RangeClosed {
    fn deserialize<D>(deserializer: D) -> Result<RangeClosed, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(RangeClosedVisitor)
    }
}

/// Extension type metadata for [`FixedClosednessRange`].
#[derive(Debug, Clone, PartialEq)]
pub struct FixedClosednessRangeMetadata {
    /// Whether the interval endpoints are included or excluded.
    closed: RangeClosed,
}

impl FixedClosednessRangeMetadata {
    /// Returns a new `FixedClosednessRangeMetadata`.
    pub fn new(closed: RangeClosed) -> Self {
        FixedClosednessRangeMetadata { closed }
    }

    /// Returns whether the interval endpoints are included or excluded.
    pub fn closed(&self) -> RangeClosed {
        self.closed
    }
}

impl Serialize for FixedClosednessRangeMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FixedClosednessRangeMetadata", 1)?;
        state.serialize_field("closed", &self.closed)?;
        state.end()
    }
}

#[derive(Debug)]
enum MetadataField {
    Closed,
    /// Any key other than `closed`. Unknown keys are ignored to allow
    /// forward-compatible extensions, as required by the specification.
    Other,
}

struct MetadataFieldVisitor;

impl Visitor<'_> for MetadataFieldVisitor {
    type Value = MetadataField;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a metadata field name")
    }

    fn visit_str<E>(self, value: &str) -> Result<MetadataField, E>
    where
        E: serde_core::de::Error,
    {
        match value {
            "closed" => Ok(MetadataField::Closed),
            _ => Ok(MetadataField::Other),
        }
    }
}

impl<'de> Deserialize<'de> for MetadataField {
    fn deserialize<D>(deserializer: D) -> Result<MetadataField, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(MetadataFieldVisitor)
    }
}

struct FixedClosednessRangeMetadataVisitor;

impl<'de> Visitor<'de> for FixedClosednessRangeMetadataVisitor {
    type Value = FixedClosednessRangeMetadata;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct FixedClosednessRangeMetadata")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<FixedClosednessRangeMetadata, V::Error>
    where
        V: serde_core::de::SeqAccess<'de>,
    {
        let closed = seq
            .next_element()?
            .ok_or_else(|| serde_core::de::Error::invalid_length(0, &self))?;
        Ok(FixedClosednessRangeMetadata { closed })
    }

    fn visit_map<V>(self, mut map: V) -> Result<FixedClosednessRangeMetadata, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut closed = None;

        while let Some(key) = map.next_key()? {
            match key {
                MetadataField::Closed => {
                    if closed.is_some() {
                        return Err(serde_core::de::Error::duplicate_field("closed"));
                    }
                    closed = Some(map.next_value()?);
                }
                MetadataField::Other => {
                    // Unknown keys are ignored for forward compatibility.
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        let closed = closed.ok_or_else(|| serde_core::de::Error::missing_field("closed"))?;
        Ok(FixedClosednessRangeMetadata { closed })
    }
}

impl<'de> Deserialize<'de> for FixedClosednessRangeMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "FixedClosednessRangeMetadata",
            &["closed"],
            FixedClosednessRangeMetadataVisitor,
        )
    }
}

/// The extension type for a bounded set (mathematical interval) whose
/// closedness is a type-level parameter shared by all values.
///
/// Extension name: `arrow.fixed_closedness_range`.
///
/// A range is a bounded set (mathematical interval) defined by a lower and
/// an upper bound over an orderable value type T. T may be any orderable
/// Arrow type, for example an integer, floating-point, decimal, date, time,
/// timestamp, duration, string or binary type. This specification defines
/// only the storage layout, not the order of any type.
///
/// The storage type is a `Struct` with exactly two fields, in order:
/// - `lower` (type T): the lower bound.
/// - `upper` (type T): the upper bound.
///
/// Both fields must share the same data type T. Each bound may
/// independently be nullable or non-nullable: a nullable bound can hold
/// null to represent an unbounded (infinite) endpoint on that side, while a
/// non-nullable bound is always finite. A null bound is always treated as
/// exclusive, regardless of the `closed` parameter: only a null bound means
/// the range is unbounded on that side.
///
/// The `closed` parameter specifies which non-null bound(s) are inclusive;
/// see [`RangeClosed`]. It is required and is not defaulted on the wire: an
/// empty metadata string, or a JSON object without a `closed` key, is
/// invalid.
///
/// Ranges whose closedness differs per value use the companion
/// `arrow.variable_closedness_range` extension type instead.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-closedness-range>
#[derive(Debug, Clone, PartialEq)]
pub struct FixedClosednessRange(FixedClosednessRangeMetadata);

impl FixedClosednessRange {
    /// Returns a new `FixedClosednessRange` extension type.
    pub fn new(closed: RangeClosed) -> Self {
        Self(FixedClosednessRangeMetadata::new(closed))
    }

    /// Returns whether the interval endpoints are included or excluded.
    pub fn closed(&self) -> RangeClosed {
        self.0.closed()
    }
}

impl From<FixedClosednessRangeMetadata> for FixedClosednessRange {
    fn from(value: FixedClosednessRangeMetadata) -> Self {
        Self(value)
    }
}

/// Validates that `data_type` is an acceptable storage type for
/// `arrow.fixed_closedness_range`.
///
/// Checks that the data type is a struct with exactly two fields named
/// "lower" and "upper", both sharing the same data type. Each bound may be
/// nullable or non-nullable independently; nullability is not required.
fn validate_storage(data_type: &DataType) -> Result<(), ArrowError> {
    let fields: &Fields = match data_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "FixedClosednessRange data type mismatch, expected Struct, found {other}"
            )));
        }
    };

    if fields.len() != 2 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "FixedClosednessRange data type mismatch, expected Struct with 2 fields, found {} field(s)",
            fields.len()
        )));
    }

    let lower = &fields[0];
    let upper = &fields[1];

    if lower.name() != "lower" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "FixedClosednessRange data type mismatch, expected first field named \"lower\", found \"{}\"",
            lower.name()
        )));
    }

    if upper.name() != "upper" {
        return Err(ArrowError::InvalidArgumentError(format!(
            "FixedClosednessRange data type mismatch, expected second field named \"upper\", found \"{}\"",
            upper.name()
        )));
    }

    if lower.data_type() != upper.data_type() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "FixedClosednessRange data type mismatch, \"lower\" and \"upper\" fields must have the same data type, found \"{}\" and \"{}\"",
            lower.data_type(),
            upper.data_type()
        )));
    }

    Ok(())
}

impl ExtensionType for FixedClosednessRange {
    const NAME: &'static str = "arrow.fixed_closedness_range";

    type Metadata = FixedClosednessRangeMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(serde_json::to_string(self.metadata()).expect("metadata serialization"))
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        metadata.map_or_else(
            || {
                Err(ArrowError::InvalidArgumentError(
                    "FixedClosednessRange extension type requires metadata".to_owned(),
                ))
            },
            |value| {
                serde_json::from_str(value).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "FixedClosednessRange metadata deserialization failed: {e}"
                    ))
                })
            },
        )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        validate_storage(data_type)
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        validate_storage(data_type)?;
        Ok(Self::from(metadata))
    }

    fn validate(data_type: &DataType, _metadata: Self::Metadata) -> Result<(), ArrowError> {
        validate_storage(data_type)
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
            ]
            .into_iter()
            .collect(),
        )
    }

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = make_range_struct(DataType::Int32);
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(range.clone())?;
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::FixedClosednessRange(range)
        );
        Ok(())
    }

    #[test]
    fn valid_string_value_type() -> Result<(), ArrowError> {
        // T may be any orderable Arrow type, including a string type.
        let range = FixedClosednessRange::new(RangeClosed::Left);
        let storage = make_range_struct(DataType::Utf8);
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(range.clone())?;
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        Ok(())
    }

    #[test]
    fn roundtrip_all_closed_values() -> Result<(), ArrowError> {
        let storage = make_range_struct(DataType::Int32);
        for closed in [
            RangeClosed::Left,
            RangeClosed::Right,
            RangeClosed::Both,
            RangeClosed::Neither,
        ] {
            let range = FixedClosednessRange::new(closed);
            let mut field = Field::new("", storage.clone(), false);
            field.try_with_extension_type(range.clone())?;
            let recovered = field.try_extension_type::<FixedClosednessRange>()?;
            assert_eq!(recovered.closed(), closed);
        }
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Extension type name missing")]
    fn missing_name() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false)
            .with_metadata([(EXTENSION_TYPE_METADATA_KEY, r#"{"closed":"both"}"#)]);
        field.extension_type::<FixedClosednessRange>();
    }

    #[test]
    #[should_panic(expected = "FixedClosednessRange extension type requires metadata")]
    fn missing_metadata() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false)
            .with_metadata([(EXTENSION_TYPE_NAME_KEY, FixedClosednessRange::NAME)]);
        field.extension_type::<FixedClosednessRange>();
    }

    #[test]
    #[should_panic(expected = "FixedClosednessRange metadata deserialization failed")]
    fn invalid_metadata_bad_closed_string() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false).with_metadata([
            (EXTENSION_TYPE_NAME_KEY, FixedClosednessRange::NAME),
            (EXTENSION_TYPE_METADATA_KEY, r#"{"closed":"invalid"}"#),
        ]);
        field.extension_type::<FixedClosednessRange>();
    }

    #[test]
    #[should_panic(expected = "FixedClosednessRange metadata deserialization failed")]
    fn invalid_metadata_missing_closed_key() {
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false).with_metadata([
            (EXTENSION_TYPE_NAME_KEY, FixedClosednessRange::NAME),
            (EXTENSION_TYPE_METADATA_KEY, r"{}"),
        ]);
        field.extension_type::<FixedClosednessRange>();
    }

    #[test]
    fn unknown_metadata_keys_are_ignored() -> Result<(), ArrowError> {
        // Additional keys in the JSON object should be ignored to allow
        // forward-compatible extensions.
        let range = FixedClosednessRange::new(RangeClosed::Right);
        let storage = make_range_struct(DataType::Int32);
        let field = Field::new("", storage, false).with_metadata([
            (EXTENSION_TYPE_NAME_KEY, FixedClosednessRange::NAME),
            (
                EXTENSION_TYPE_METADATA_KEY,
                r#"{"closed":"right","extra":42}"#,
            ),
        ]);
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "FixedClosednessRange data type mismatch, expected Struct, found Int32"
    )]
    fn invalid_storage_non_struct() {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let field = Field::new("", DataType::Int32, false);
        field.with_extension_type(range);
    }

    #[test]
    #[should_panic(
        expected = "FixedClosednessRange data type mismatch, expected Struct with 2 fields"
    )]
    fn invalid_storage_wrong_field_count() {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage =
            DataType::Struct(std::iter::once(Field::new("lower", DataType::Int32, true)).collect());
        let field = Field::new("", storage, false);
        field.with_extension_type(range);
    }

    #[test]
    #[should_panic(
        expected = "FixedClosednessRange data type mismatch, expected first field named \"lower\""
    )]
    fn invalid_storage_wrong_field_names() {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = DataType::Struct(
            [
                Field::new("start", DataType::Int32, true),
                Field::new("end", DataType::Int32, true),
            ]
            .into_iter()
            .collect(),
        );
        let field = Field::new("", storage, false);
        field.with_extension_type(range);
    }

    #[test]
    fn accepts_non_nullable_lower() -> Result<(), ArrowError> {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, false),
                Field::new("upper", DataType::Int32, true),
            ]
            .into_iter()
            .collect(),
        );
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(range.clone())?;
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        Ok(())
    }

    #[test]
    fn accepts_non_nullable_upper() -> Result<(), ArrowError> {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, false),
            ]
            .into_iter()
            .collect(),
        );
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(range.clone())?;
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        Ok(())
    }

    #[test]
    fn accepts_both_non_nullable() -> Result<(), ArrowError> {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, false),
                Field::new("upper", DataType::Int32, false),
            ]
            .into_iter()
            .collect(),
        );
        let mut field = Field::new("", storage, false);
        field.try_with_extension_type(range.clone())?;
        assert_eq!(field.try_extension_type::<FixedClosednessRange>()?, range);
        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "FixedClosednessRange data type mismatch, \"lower\" and \"upper\" fields must have the same data type"
    )]
    fn invalid_storage_mismatched_types() {
        let range = FixedClosednessRange::new(RangeClosed::Both);
        let storage = DataType::Struct(
            [
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int64, true),
            ]
            .into_iter()
            .collect(),
        );
        let field = Field::new("", storage, false);
        field.with_extension_type(range);
    }
}
