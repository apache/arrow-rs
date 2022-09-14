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

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Schema {
    pub fields: Vec<Field>,
    /// A map of key-value pairs containing additional meta data.
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "HashMap::is_empty", default)
    )]
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

    /// Generate a JSON representation of the `Schema`.
    #[cfg(feature = "json")]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "fields": self.fields.iter().map(|field| field.to_json()).collect::<Vec<serde_json::Value>>(),
            "metadata": serde_json::to_value(&self.metadata).unwrap()
        })
    }

    /// Parse a `Schema` definition from a JSON representation.
    #[cfg(feature = "json")]
    pub fn from(json: &serde_json::Value) -> Result<Self, ArrowError> {
        use serde_json::Value;
        match *json {
            Value::Object(ref schema) => {
                let fields = if let Some(Value::Array(fields)) = schema.get("fields") {
                    fields.iter().map(Field::from).collect::<Result<_, _>>()?
                } else {
                    return Err(ArrowError::ParseError(
                        "Schema fields should be an array".to_string(),
                    ));
                };

                let metadata = if let Some(value) = schema.get("metadata") {
                    Self::from_metadata(value)?
                } else {
                    HashMap::default()
                };

                Ok(Self { fields, metadata })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for schema".to_string(),
            )),
        }
    }

    /// Parse a `metadata` definition from a JSON representation.
    /// The JSON can either be an Object or an Array of Objects.
    #[cfg(feature = "json")]
    fn from_metadata(
        json: &serde_json::Value,
    ) -> Result<HashMap<String, String>, ArrowError> {
        use serde_json::Value;
        match json {
            Value::Array(_) => {
                let mut hashmap = HashMap::new();
                let values: Vec<MetadataKeyValue> = serde_json::from_value(json.clone())
                    .map_err(|_| {
                        ArrowError::ParseError(
                            "Unable to parse object into key-value pair".to_string(),
                        )
                    })?;
                for meta in values {
                    hashmap.insert(meta.key.clone(), meta.value);
                }
                Ok(hashmap)
            }
            Value::Object(md) => md
                .iter()
                .map(|(k, v)| {
                    if let Value::String(v) = v {
                        Ok((k.to_string(), v.to_string()))
                    } else {
                        Err(ArrowError::ParseError(
                            "metadata `value` field must be a string".to_string(),
                        ))
                    }
                })
                .collect::<Result<_, _>>(),
            _ => Err(ArrowError::ParseError(
                "`metadata` field must be an object".to_string(),
            )),
        }
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

#[cfg(feature = "json")]
#[derive(serde::Deserialize)]
struct MetadataKeyValue {
    key: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::DataType;

    #[test]
    #[cfg(feature = "json")]
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
                "Schema error: project index 3 out of bounds, max field 3"
                    .to_string()
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
}
