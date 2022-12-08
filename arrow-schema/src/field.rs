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
use std::collections::HashMap;
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
    metadata: HashMap<String, String>,
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
            .then_with(|| self.data_type.cmp(other.data_type()))
            .then_with(|| self.nullable.cmp(&other.nullable))
            .then_with(|| {
                // ensure deterministic key order
                let mut keys: Vec<&String> =
                    self.metadata.keys().chain(other.metadata.keys()).collect();
                keys.sort();
                for k in keys {
                    match (self.metadata.get(k), other.metadata.get(k)) {
                        (None, None) => {}
                        (Some(_), None) => {
                            return Ordering::Less;
                        }
                        (None, Some(_)) => {
                            return Ordering::Greater;
                        }
                        (Some(v1), Some(v2)) => match v1.cmp(v2) {
                            Ordering::Equal => {}
                            other => {
                                return other;
                            }
                        },
                    }
                }

                Ordering::Equal
            })
    }
}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}

impl Field {
    /// Creates a new field
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new field that has additional dictionary information
    pub fn new_dict(
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: HashMap::default(),
        }
    }

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    #[inline]
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = HashMap::default();
        if !metadata.is_empty() {
            self.metadata = metadata;
        }
    }

    /// Sets the metadata of this `Field` to be `metadata` and returns self
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.set_metadata(metadata);
        self
    }

    /// Returns the immutable reference to the `Field`'s optional custom metadata.
    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
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
            return Err(ArrowError::SchemaError(format!(
                "Fail to merge schema field '{}' because from dict_id = {} does not match {}",
                self.name, from.dict_id, self.dict_id
            )));
        }
        if from.dict_is_ordered != self.dict_is_ordered {
            return Err(ArrowError::SchemaError(format!(
                "Fail to merge schema field '{}' because from dict_is_ordered = {} does not match {}",
                self.name, from.dict_is_ordered, self.dict_is_ordered
            )));
        }
        // merge metadata
        match (self.metadata().is_empty(), from.metadata().is_empty()) {
            (false, false) => {
                let mut merged = self.metadata().clone();
                for (key, from_value) in from.metadata() {
                    if let Some(self_value) = self.metadata.get(key) {
                        if self_value != from_value {
                            return Err(ArrowError::SchemaError(format!(
                                "Fail to merge field '{}' due to conflicting metadata data value for key {}.
                                    From value = {} does not match {}", self.name, key, from_value, self_value),
                            ));
                        }
                    } else {
                        merged.insert(key.clone(), from_value.clone());
                    }
                }
                self.set_metadata(merged);
            }
            (true, false) => {
                self.set_metadata(from.metadata().clone());
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
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Struct",
                            self.name, from.data_type)
                ))}
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
                                        format!("Fail to merge schema field '{}' because the self_type_id = {} does not equal field_type_id = {}",
                                            self.name, self_type_id, field_type_id)
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
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Union",
                            self.name, from.data_type)
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
                        format!("Fail to merge schema field '{}' because the from data_type = {} does not equal {}",
                            self.name, from.data_type, self.data_type)
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
        && match (&self.metadata.is_empty(), &other.metadata.is_empty()) {
            (_, true) => true,
            (true, false) => false,
            (false, false) => {
                other.metadata().iter().all(|(k, v)| {
                    match self.metadata().get(k) {
                        Some(s) => s == v,
                        None => false
                    }
                })
            }
        }
    }

    /// Return size of this instance in bytes.
    ///
    /// Includes the size of `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.data_type)
            + self.data_type.size()
            + self.name.capacity()
            + (std::mem::size_of::<(String, String)>() * self.metadata.capacity())
            + self
                .metadata
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>()
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
    fn test_new_with_string() {
        // Fields should allow owned Strings to support reuse
        let s = String::from("c1");
        Field::new(s, DataType::Int64, false);
    }

    #[test]
    fn test_new_dict_with_string() {
        // Fields should allow owned Strings to support reuse
        let s = String::from("c1");
        Field::new_dict(s, DataType::Int64, false, 4, false);
    }

    #[test]
    fn test_merge_incompatible_types() {
        let mut field = Field::new("c1", DataType::Int64, false);
        let result = field
            .try_merge(&Field::new("c1", DataType::Float32, true))
            .expect_err("should fail")
            .to_string();
        assert_eq!("Schema error: Fail to merge schema field 'c1' because the from data_type = Float32 does not equal Int64", result);
    }

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
    fn test_field_comparison_metadata() {
        let f1 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
            (String::from("k1"), String::from("v1")),
            (String::from("k2"), String::from("v2")),
        ]));
        let f2 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
            (String::from("k1"), String::from("v1")),
            (String::from("k3"), String::from("v3")),
        ]));
        let f3 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
            (String::from("k1"), String::from("v1")),
            (String::from("k3"), String::from("v4")),
        ]));

        assert!(f1.cmp(&f2).is_lt());
        assert!(f2.cmp(&f3).is_lt());
        assert!(f1.cmp(&f3).is_lt());
    }

    #[test]
    fn test_contains_reflexivity() {
        let mut field = Field::new("field1", DataType::Float16, false);
        field.set_metadata(HashMap::from([
            (String::from("k0"), String::from("v0")),
            (String::from("k1"), String::from("v1")),
        ]));
        assert!(field.contains(&field))
    }

    #[test]
    fn test_contains_transitivity() {
        let child_field = Field::new("child1", DataType::Float16, false);

        let mut field1 = Field::new("field1", DataType::Struct(vec![child_field]), false);
        field1.set_metadata(HashMap::from([(String::from("k1"), String::from("v1"))]));

        let mut field2 = Field::new("field1", DataType::Struct(vec![]), true);
        field2.set_metadata(HashMap::from([(String::from("k2"), String::from("v2"))]));
        field2.try_merge(&field1).unwrap();

        let mut field3 = Field::new("field1", DataType::Struct(vec![]), false);
        field3.set_metadata(HashMap::from([(String::from("k3"), String::from("v3"))]));
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

    #[cfg(feature = "serde")]
    fn assert_binary_serde_round_trip(field: Field) {
        let serialized = bincode::serialize(&field).unwrap();
        let deserialized: Field = bincode::deserialize(&serialized).unwrap();
        assert_eq!(field, deserialized)
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_field_without_metadata_serde() {
        let field = Field::new("name", DataType::Boolean, true);
        assert_binary_serde_round_trip(field)
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_field_with_empty_metadata_serde() {
        let field =
            Field::new("name", DataType::Boolean, false).with_metadata(HashMap::new());

        assert_binary_serde_round_trip(field)
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_field_with_nonempty_metadata_serde() {
        let mut metadata = HashMap::new();
        metadata.insert("hi".to_owned(), "".to_owned());
        let field = Field::new("name", DataType::Boolean, false).with_metadata(metadata);

        assert_binary_serde_round_trip(field)
    }
}
