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

use crate::error::ArrowError;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::datatype::DataType;

/// Describes a single column in a [`Schema`](super::Schema).
///
/// A [`Schema`](super::Schema) is an ordered collection of
/// [`Field`] objects.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    metadata: Option<BTreeMap<String, String>>,
}

// Auto-derive `PartialEq` traits will pull `dict_id` and `dict_is_ordered`
// into comparison. However, these properties are only used in IPC context
// for matching dictionary encoded data. They are not necessary to be same
// to consider schema equality. For example, in C++ `Field` implementation,
// it doesn't contain these dictionary properties too.
impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.metadata == other.metadata
    }
}

impl Eq for Field {}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Field {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name
            .cmp(other.name())
            .then(self.data_type.cmp(other.data_type()))
            .then(self.nullable.cmp(&other.nullable))
            .then(self.metadata.cmp(&other.metadata))
    }
}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);
        self.metadata.hash(state);
    }
}

impl Field {
    /// Creates a new field
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        }
    }

    /// Creates a new field that has additional dictionary information
    pub fn new_dict(
        name: &str,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: None,
        }
    }

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    #[inline]
    pub fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>) {
        // To make serde happy, convert Some(empty_map) to None.
        self.metadata = None;
        if let Some(v) = metadata {
            if !v.is_empty() {
                self.metadata = Some(v);
            }
        }
    }

    /// Sets the metadata of this `Field` to be `metadata` and returns self
    pub fn with_metadata(mut self, metadata: Option<BTreeMap<String, String>>) -> Self {
        self.set_metadata(metadata);
        self
    }

    /// Returns the immutable reference to the `Field`'s optional custom metadata.
    #[inline]
    pub const fn metadata(&self) -> Option<&BTreeMap<String, String>> {
        self.metadata.as_ref()
    }

    /// Returns an immutable reference to the `Field`'s name.
    #[inline]
    pub const fn name(&self) -> &String {
        &self.name
    }

    /// Set the name of the [`Field`] and returns self.
    ///
    /// ```
    /// # use arrow_schema::*;
    /// let field = Field::new("c1", DataType::Int64, false)
    ///    .with_name("c2");
    ///
    /// assert_eq!(field.name(), "c2");
    /// ```
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Returns an immutable reference to the [`Field`]'s  [`DataType`].
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Set [`DataType`] of the [`Field`] and returns self.
    ///
    /// ```
    /// # use arrow_schema::*;
    /// let field = Field::new("c1", DataType::Int64, false)
    ///    .with_data_type(DataType::Utf8);
    ///
    /// assert_eq!(field.data_type(), &DataType::Utf8);
    /// ```
    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    /// Indicates whether this [`Field`] supports null values.
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Set `nullable` of the [`Field`] and returns self.
    ///
    /// ```
    /// # use arrow_schema::*;
    /// let field = Field::new("c1", DataType::Int64, false)
    ///    .with_nullable(true);
    ///
    /// assert_eq!(field.is_nullable(), true);
    /// ```
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Returns a (flattened) [`Vec`] containing all child [`Field`]s
    /// within `self` contained within this field (including `self`)
    pub(crate) fn fields(&self) -> Vec<&Field> {
        let mut collected_fields = vec![self];
        collected_fields.append(&mut Field::_fields(&self.data_type));

        collected_fields
    }

    fn _fields(dt: &DataType) -> Vec<&Field> {
        match dt {
            DataType::Struct(fields) | DataType::Union(fields, _, _) => {
                fields.iter().flat_map(|f| f.fields()).collect()
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => field.fields(),
            DataType::Dictionary(_, value_field) => Field::_fields(value_field.as_ref()),
            _ => vec![],
        }
    }

    /// Returns a vector containing all (potentially nested) `Field` instances selected by the
    /// dictionary ID they use
    #[inline]
    pub(crate) fn fields_with_dict_id(&self, id: i64) -> Vec<&Field> {
        self.fields()
            .into_iter()
            .filter(|&field| {
                matches!(field.data_type(), DataType::Dictionary(_, _))
                    && field.dict_id == id
            })
            .collect()
    }

    /// Returns the dictionary ID, if this is a dictionary type.
    #[inline]
    pub const fn dict_id(&self) -> Option<i64> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_id),
            _ => None,
        }
    }

    /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type.
    #[inline]
    pub const fn dict_is_ordered(&self) -> Option<bool> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_is_ordered),
            _ => None,
        }
    }

    /// Merge this field into self if it is compatible.
    ///
    /// Struct fields are merged recursively.
    ///
    /// NOTE: `self` may be updated to a partial / unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// # use arrow_schema::*;
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    pub fn try_merge(&mut self, from: &Field) -> Result<(), ArrowError> {
        if from.dict_id != self.dict_id {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_id".to_string(),
            ));
        }
        if from.dict_is_ordered != self.dict_is_ordered {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_is_ordered"
                    .to_string(),
            ));
        }
        // merge metadata
        match (self.metadata(), from.metadata()) {
            (Some(self_metadata), Some(from_metadata)) => {
                let mut merged = self_metadata.clone();
                for (key, from_value) in from_metadata {
                    if let Some(self_value) = self_metadata.get(key) {
                        if self_value != from_value {
                            return Err(ArrowError::SchemaError(format!(
                                "Fail to merge field due to conflicting metadata data value for key {}", key),
                            ));
                        }
                    } else {
                        merged.insert(key.clone(), from_value.clone());
                    }
                }
                self.set_metadata(Some(merged));
            }
            (None, Some(from_metadata)) => {
                self.set_metadata(Some(from_metadata.clone()));
            }
            _ => {}
        }
        match &mut self.data_type {
            DataType::Struct(nested_fields) => match &from.data_type {
                DataType::Struct(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        match nested_fields
                            .iter_mut()
                            .find(|self_field| self_field.name == from_field.name)
                        {
                            Some(self_field) => self_field.try_merge(from_field)?,
                            None => nested_fields.push(from_field.clone()),
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Union(nested_fields, type_ids, _) => match &from.data_type {
                DataType::Union(from_nested_fields, from_type_ids, _) => {
                    for (idx, from_field) in from_nested_fields.iter().enumerate() {
                        let mut is_new_field = true;
                        let field_type_id = from_type_ids.get(idx).unwrap();

                        for (self_idx, self_field) in nested_fields.iter_mut().enumerate()
                        {
                            if from_field == self_field {
                                let self_type_id = type_ids.get(self_idx).unwrap();

                                // If the nested fields in two unions are the same, they must have same
                                // type id.
                                if self_type_id != field_type_id {
                                    return Err(ArrowError::SchemaError(
                                        "Fail to merge schema Field due to conflicting type ids in union datatype"
                                            .to_string(),
                                    ));
                                }

                                is_new_field = false;
                                break;
                            }
                        }

                        if is_new_field {
                            nested_fields.push(from_field.clone());
                            type_ids.push(*field_type_id);
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::List(_)
            | DataType::Map(_, _)
            | DataType::Dictionary(_, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                if self.data_type != from.data_type {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            }
        }
        self.nullable |= from.nullable;

        Ok(())
    }

    /// Check to see if `self` is a superset of `other` field. Superset is defined as:
    ///
    /// * if nullability doesn't match, self needs to be nullable
    /// * self.metadata is a superset of other.metadata
    /// * all other fields are equal
    pub fn contains(&self, other: &Field) -> bool {
        self.name == other.name
        && self.data_type == other.data_type
        && self.dict_id == other.dict_id
        && self.dict_is_ordered == other.dict_is_ordered
        // self need to be nullable or both of them are not nullable
        && (self.nullable || !other.nullable)
        // make sure self.metadata is a superset of other.metadata
        && match (&self.metadata, &other.metadata) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(self_meta), Some(other_meta)) => {
                other_meta.iter().all(|(k, v)| {
                    match self_meta.get(k) {
                        Some(s) => s == v,
                        None => false
                    }
                })
            }
        }
    }
}

// TODO: improve display with crate https://crates.io/crates/derive_more ?
impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_fields_with_dict_id() {
        let dict1 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            10,
            false,
        );
        let dict2 = Field::new_dict(
            "dict2",
            DataType::Dictionary(DataType::Int32.into(), DataType::Int8.into()),
            false,
            20,
            false,
        );

        let field = Field::new(
            "struct<dict1, list[struct<dict2, list[struct<dict1]>]>",
            DataType::Struct(vec![
                dict1.clone(),
                Field::new(
                    "list[struct<dict1, list[struct<dict2>]>]",
                    DataType::List(Box::new(Field::new(
                        "struct<dict1, list[struct<dict2>]>",
                        DataType::Struct(vec![
                            dict1.clone(),
                            Field::new(
                                "list[struct<dict2>]",
                                DataType::List(Box::new(Field::new(
                                    "struct<dict2>",
                                    DataType::Struct(vec![dict2.clone()]),
                                    false,
                                ))),
                                false,
                            ),
                        ]),
                        false,
                    ))),
                    false,
                ),
            ]),
            false,
        );

        for field in field.fields_with_dict_id(10) {
            assert_eq!(dict1, *field);
        }
        for field in field.fields_with_dict_id(20) {
            assert_eq!(dict2, *field);
        }
    }

    fn get_field_hash(field: &Field) -> u64 {
        let mut s = DefaultHasher::new();
        field.hash(&mut s);
        s.finish()
    }

    #[test]
    fn test_field_comparison_case() {
        // dictionary-encoding properties not used for field comparison
        let dict1 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            10,
            false,
        );
        let dict2 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            20,
            false,
        );

        assert_eq!(dict1, dict2);
        assert_eq!(get_field_hash(&dict1), get_field_hash(&dict2));

        let dict1 = Field::new_dict(
            "dict0",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            10,
            false,
        );

        assert_ne!(dict1, dict2);
        assert_ne!(get_field_hash(&dict1), get_field_hash(&dict2));
    }

    #[test]
    fn test_contains_reflexivity() {
        let mut field = Field::new("field1", DataType::Float16, false);
        field.set_metadata(Some(BTreeMap::from([
            (String::from("k0"), String::from("v0")),
            (String::from("k1"), String::from("v1")),
        ])));
        assert!(field.contains(&field))
    }

    #[test]
    fn test_contains_transitivity() {
        let child_field = Field::new("child1", DataType::Float16, false);

        let mut field1 = Field::new("field1", DataType::Struct(vec![child_field]), false);
        field1.set_metadata(Some(BTreeMap::from([(
            String::from("k1"),
            String::from("v1"),
        )])));

        let mut field2 = Field::new("field1", DataType::Struct(vec![]), true);
        field2.set_metadata(Some(BTreeMap::from([(
            String::from("k2"),
            String::from("v2"),
        )])));
        field2.try_merge(&field1).unwrap();

        let mut field3 = Field::new("field1", DataType::Struct(vec![]), false);
        field3.set_metadata(Some(BTreeMap::from([(
            String::from("k3"),
            String::from("v3"),
        )])));
        field3.try_merge(&field2).unwrap();

        assert!(field2.contains(&field1));
        assert!(field3.contains(&field2));
        assert!(field3.contains(&field1));

        assert!(!field1.contains(&field2));
        assert!(!field1.contains(&field3));
        assert!(!field2.contains(&field3));
    }

    #[test]
    fn test_contains_nullable() {
        let field1 = Field::new("field1", DataType::Boolean, true);
        let field2 = Field::new("field1", DataType::Boolean, false);
        assert!(field1.contains(&field2));
        assert!(!field2.contains(&field1));
    }

    #[test]
    fn test_contains_must_have_same_fields() {
        let child_field1 = Field::new("child1", DataType::Float16, false);
        let child_field2 = Field::new("child2", DataType::Float16, false);

        let field1 =
            Field::new("field1", DataType::Struct(vec![child_field1.clone()]), true);
        let field2 = Field::new(
            "field1",
            DataType::Struct(vec![child_field1, child_field2]),
            true,
        );

        assert!(!field1.contains(&field2));
        assert!(!field2.contains(&field1));
    }
}
