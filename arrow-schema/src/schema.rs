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

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

use crate::error::ArrowError;
use crate::field::Field;

/// A reference-counted reference to a [`Schema`].
pub type SchemaRef = std::sync::Arc<Schema>;

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Schema {
    pub fields: Vec<Field>,
    /// A map of key-value pairs containing additional meta data.
    pub metadata: HashMap<String, String>,
}

impl Schema {
    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: HashMap::new(),
        }
    }

    /// Creates a new [`Schema`] from a sequence of [`Field`] values.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_schema::*;
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let schema = Schema::new(vec![field_a, field_b]);
    /// ```
    pub fn new(fields: Vec<Field>) -> Self {
        Self::new_with_metadata(fields, HashMap::new())
    }

    /// Creates a new [`Schema`] from a sequence of [`Field`] values
    /// and adds additional metadata in form of key value pairs.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_schema::*;
    /// # use std::collections::HashMap;
    ///
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let mut metadata: HashMap<String, String> = HashMap::new();
    /// metadata.insert("row_count".to_string(), "100".to_string());
    ///
    /// let schema = Schema::new_with_metadata(vec![field_a, field_b], metadata);
    /// ```
    #[inline]
    pub const fn new_with_metadata(
        fields: Vec<Field>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self { fields, metadata }
    }

    /// Sets the metadata of this `Schema` to be `metadata` and returns self
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Returns a new schema with only the specified columns in the new schema
    /// This carries metadata from the parent schema over as well
    pub fn project(&self, indices: &[usize]) -> Result<Schema, ArrowError> {
        let new_fields = indices
            .iter()
            .map(|i| {
                self.fields.get(*i).cloned().ok_or_else(|| {
                    ArrowError::SchemaError(format!(
                        "project index {} out of bounds, max field {}",
                        i,
                        self.fields().len()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new_with_metadata(new_fields, self.metadata.clone()))
    }

    /// Merge schema into self if it is compatible. Struct fields will be merged recursively.
    ///
    /// Example:
    ///
    /// ```
    /// # use arrow_schema::*;
    ///
    /// let merged = Schema::try_merge(vec![
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, false),
    ///         Field::new("c2", DataType::Utf8, false),
    ///     ]),
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///     merged,
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// );
    /// ```
    pub fn try_merge(
        schemas: impl IntoIterator<Item = Self>,
    ) -> Result<Self, ArrowError> {
        schemas
            .into_iter()
            .try_fold(Self::empty(), |mut merged, schema| {
                let Schema { metadata, fields } = schema;
                for (key, value) in metadata.into_iter() {
                    // merge metadata
                    if let Some(old_val) = merged.metadata.get(&key) {
                        if old_val != &value {
                            return Err(ArrowError::SchemaError(format!(
                                "Fail to merge schema due to conflicting metadata. \
                                         Key '{}' has different values '{}' and '{}'",
                                key, old_val, value
                            )));
                        }
                    }
                    merged.metadata.insert(key, value);
                }
                // merge fields
                for field in fields.into_iter() {
                    let merged_field =
                        merged.fields.iter_mut().find(|f| f.name() == field.name());
                    match merged_field {
                        Some(merged_field) => merged_field.try_merge(&field)?,
                        // found a new field, add to field list
                        None => merged.fields.push(field),
                    }
                }
                Ok(merged)
            })
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    /// Returns a vector with references to all fields (including nested fields)
    #[inline]
    pub fn all_fields(&self) -> Vec<&Field> {
        self.fields.iter().flat_map(|f| f.fields()).collect()
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&Field, ArrowError> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns a vector of immutable references to all [`Field`] instances selected by
    /// the dictionary ID they use.
    pub fn fields_with_dict_id(&self, dict_id: i64) -> Vec<&Field> {
        self.fields
            .iter()
            .flat_map(|f| f.fields_with_dict_id(dict_id))
            .collect()
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize, ArrowError> {
        (0..self.fields.len())
            .find(|idx| self.fields[*idx].name() == name)
            .ok_or_else(|| {
                let valid_fields: Vec<String> =
                    self.fields.iter().map(|f| f.name().clone()).collect();
                ArrowError::SchemaError(format!(
                    "Unable to get field named \"{}\". Valid fields: {:?}",
                    name, valid_fields
                ))
            })
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparison rules:
    ///
    /// * `self` and `other` should contain the same number of fields
    /// * for every field `f` in `other`, the field in `self` with corresponding index should be a
    /// superset of `f`.
    /// * self.metadata is a superset of other.metadata
    ///
    /// In other words, any record conforms to `other` should also conform to `self`.
    pub fn contains(&self, other: &Schema) -> bool {
        self.fields.len() == other.fields.len()
        && self.fields.iter().zip(other.fields.iter()).all(|(f1, f2)| f1.contains(f2))
        // make sure self.metadata is a superset of other.metadata
        && other.metadata.iter().all(|(k, v1)| match self.metadata.get(k) {
            Some(v2) => v1 == v2,
            _ => false,
        })
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            &self
                .fields
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

// need to implement `Hash` manually because `HashMap` implement Eq but no `Hash`
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fields.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::DataType;
    use crate::{TimeUnit, UnionMode};

    #[test]
    #[cfg(feature = "serde")]
    fn test_ser_de_metadata() {
        // ser/de with empty metadata
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ]);

        let json = serde_json::to_string(&schema).unwrap();
        let de_schema = serde_json::from_str(&json).unwrap();

        assert_eq!(schema, de_schema);

        // ser/de with non-empty metadata
        let schema = schema
            .with_metadata([("key".to_owned(), "val".to_owned())].into_iter().collect());
        let json = serde_json::to_string(&schema).unwrap();
        let de_schema = serde_json::from_str(&json).unwrap();

        assert_eq!(schema, de_schema);
    }

    #[test]
    fn test_projection() {
        let mut metadata = HashMap::new();
        metadata.insert("meta".to_string(), "data".to_string());

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ])
        .with_metadata(metadata);

        let projected: Schema = schema.project(&[0, 2]).unwrap();

        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.fields()[0].name(), "name");
        assert_eq!(projected.fields()[1].name(), "priority");
        assert_eq!(projected.metadata.get("meta").unwrap(), "data")
    }

    #[test]
    fn test_oob_projection() {
        let mut metadata = HashMap::new();
        metadata.insert("meta".to_string(), "data".to_string());

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ])
        .with_metadata(metadata);

        let projected = schema.project(&[0, 3]);

        assert!(projected.is_err());
        if let Err(e) = projected {
            assert_eq!(
                e.to_string(),
                "Schema error: project index 3 out of bounds, max field 3".to_string()
            )
        }
    }

    #[test]
    fn test_schema_contains() {
        let mut metadata1 = HashMap::new();
        metadata1.insert("meta".to_string(), "data".to_string());

        let schema1 = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ])
        .with_metadata(metadata1.clone());

        let mut metadata2 = HashMap::new();
        metadata2.insert("meta".to_string(), "data".to_string());
        metadata2.insert("meta2".to_string(), "data".to_string());
        let schema2 = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ])
        .with_metadata(metadata2);

        // reflexivity
        assert!(schema1.contains(&schema1));
        assert!(schema2.contains(&schema2));

        assert!(!schema1.contains(&schema2));
        assert!(schema2.contains(&schema1));
    }

    #[test]
    fn schema_equality() {
        let schema1 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);

        assert_eq!(schema1, schema2);

        let schema3 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float32, true),
        ]);
        let schema4 = Schema::new(vec![
            Field::new("C1", DataType::Utf8, false),
            Field::new("C2", DataType::Float64, true),
        ]);

        assert_ne!(schema1, schema3);
        assert_ne!(schema1, schema4);
        assert_ne!(schema2, schema3);
        assert_ne!(schema2, schema4);
        assert_ne!(schema3, schema4);

        let f = Field::new("c1", DataType::Utf8, false).with_metadata(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        );
        let schema5 = Schema::new(vec![
            f,
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        assert_ne!(schema1, schema5);
    }

    #[test]
    fn create_schema_string() {
        let schema = person_schema();
        assert_eq!(schema.to_string(),
                   "Field { name: \"first_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {\"k\": \"v\"} }, \
        Field { name: \"last_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"address\", data_type: Struct([\
            Field { name: \"street\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
            Field { name: \"zip\", data_type: UInt16, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }\
        ]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"interests\", data_type: Dictionary(Int32, Utf8), nullable: true, dict_id: 123, dict_is_ordered: true, metadata: {} }")
    }

    #[test]
    fn schema_field_accessors() {
        let schema = person_schema();

        // test schema accessors
        assert_eq!(schema.fields().len(), 4);

        // test field accessors
        let first_name = &schema.fields()[0];
        assert_eq!(first_name.name(), "first_name");
        assert_eq!(first_name.data_type(), &DataType::Utf8);
        assert!(!first_name.is_nullable());
        assert_eq!(first_name.dict_id(), None);
        assert_eq!(first_name.dict_is_ordered(), None);

        let metadata = first_name.metadata();
        assert!(!metadata.is_empty());
        let md = &metadata;
        assert_eq!(md.len(), 1);
        let key = md.get("k");
        assert!(key.is_some());
        assert_eq!(key.unwrap(), "v");

        let interests = &schema.fields()[3];
        assert_eq!(interests.name(), "interests");
        assert_eq!(
            interests.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(interests.dict_id(), Some(123));
        assert_eq!(interests.dict_is_ordered(), Some(true));
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_index_of() {
        let schema = person_schema();
        assert_eq!(schema.index_of("first_name").unwrap(), 0);
        assert_eq!(schema.index_of("last_name").unwrap(), 1);
        schema.index_of("nickname").unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_field_with_name() {
        let schema = person_schema();
        assert_eq!(
            schema.field_with_name("first_name").unwrap().name(),
            "first_name"
        );
        assert_eq!(
            schema.field_with_name("last_name").unwrap().name(),
            "last_name"
        );
        schema.field_with_name("nickname").unwrap();
    }

    #[test]
    fn schema_field_with_dict_id() {
        let schema = person_schema();

        let fields_dict_123: Vec<_> = schema
            .fields_with_dict_id(123)
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(fields_dict_123, vec!["interests"]);

        assert!(schema.fields_with_dict_id(456).is_empty());
    }

    fn person_schema() -> Schema {
        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: HashMap<String, String> = kv_array.iter().cloned().collect();
        let first_name =
            Field::new("first_name", DataType::Utf8, false).with_metadata(field_metadata);

        Schema::new(vec![
            first_name,
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
            Field::new_dict(
                "interests",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                123,
                true,
            ),
        ])
    }

    #[test]
    fn test_try_merge_field_with_metadata() {
        // 1. Different values for the same key should cause error.
        let metadata1: HashMap<String, String> = [("foo".to_string(), "bar".to_string())]
            .iter()
            .cloned()
            .collect();
        let f1 = Field::new("first_name", DataType::Utf8, false).with_metadata(metadata1);

        let metadata2: HashMap<String, String> = [("foo".to_string(), "baz".to_string())]
            .iter()
            .cloned()
            .collect();
        let f2 = Field::new("first_name", DataType::Utf8, false).with_metadata(metadata2);

        assert!(
            Schema::try_merge(vec![Schema::new(vec![f1]), Schema::new(vec![f2])])
                .is_err()
        );

        // 2. None + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let metadata2: HashMap<String, String> =
            [("missing".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect();
        let f2 = Field::new("first_name", DataType::Utf8, false).with_metadata(metadata2);

        assert!(f1.try_merge(&f2).is_ok());
        assert!(!f1.metadata().is_empty());
        assert_eq!(f1.metadata(), f2.metadata());

        // 3. Some + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false).with_metadata(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        );
        let f2 = Field::new("first_name", DataType::Utf8, false).with_metadata(
            [("foo2".to_string(), "bar2".to_string())]
                .iter()
                .cloned()
                .collect(),
        );

        assert!(f1.try_merge(&f2).is_ok());
        assert!(!f1.metadata().is_empty());
        assert_eq!(
            f1.metadata().clone(),
            [
                ("foo".to_string(), "bar".to_string()),
                ("foo2".to_string(), "bar2".to_string())
            ]
            .iter()
            .cloned()
            .collect()
        );

        // 4. Some + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false).with_metadata(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        );
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(!f1.metadata().is_empty());
        assert_eq!(
            f1.metadata().clone(),
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect()
        );

        // 5. None + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata().is_empty());
    }

    #[test]
    fn test_schema_merge() {
        let merged = Schema::try_merge(vec![
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new(
                    "address",
                    DataType::Struct(vec![Field::new("zip", DataType::UInt16, false)]),
                    false,
                ),
            ]),
            Schema::new_with_metadata(
                vec![
                    // nullable merge
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            // add new nested field
                            Field::new("street", DataType::Utf8, false),
                            // nullable merge on nested field
                            Field::new("zip", DataType::UInt16, true),
                        ]),
                        false,
                    ),
                    // new field
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>(),
            ),
        ])
        .unwrap();

        assert_eq!(
            merged,
            Schema::new_with_metadata(
                vec![
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            Field::new("zip", DataType::UInt16, true),
                            Field::new("street", DataType::Utf8, false),
                        ]),
                        false,
                    ),
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            )
        );

        // support merge union fields
        assert_eq!(
            Schema::try_merge(vec![
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(
                        vec![
                            Field::new("c11", DataType::Utf8, true),
                            Field::new("c12", DataType::Utf8, true),
                        ],
                        vec![0, 1],
                        UnionMode::Dense
                    ),
                    false
                ),]),
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(
                        vec![
                            Field::new("c12", DataType::Utf8, true),
                            Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                        ],
                        vec![1, 2],
                        UnionMode::Dense
                    ),
                    false
                ),])
            ])
            .unwrap(),
            Schema::new(vec![Field::new(
                "c1",
                DataType::Union(
                    vec![
                        Field::new("c11", DataType::Utf8, true),
                        Field::new("c12", DataType::Utf8, true),
                        Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                    ],
                    vec![0, 1, 2],
                    UnionMode::Dense
                ),
                false
            ),]),
        );

        // incompatible field should throw error
        assert!(Schema::try_merge(vec![
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
            ]),
            Schema::new(vec![Field::new("last_name", DataType::Int64, false),])
        ])
        .is_err());

        // incompatible metadata should throw error
        let res = Schema::try_merge(vec![
            Schema::new_with_metadata(
                vec![Field::new("first_name", DataType::Utf8, false)],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>(),
            ),
            Schema::new_with_metadata(
                vec![Field::new("last_name", DataType::Utf8, false)],
                [("foo".to_string(), "baz".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>(),
            ),
        ])
        .unwrap_err();

        let expected = "Fail to merge schema due to conflicting metadata. Key 'foo' has different values 'bar' and 'baz'";
        assert!(
            res.to_string().contains(expected),
            "Could not find expected string '{}' in '{}'",
            expected,
            res
        );
    }
}
