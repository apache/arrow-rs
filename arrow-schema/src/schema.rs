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
use std::sync::Arc;

use crate::error::ArrowError;
use crate::field::Field;
use crate::{DataType, FieldRef, Fields};

/// A builder to facilitate building a [`Schema`] from iteratively from [`FieldRef`]
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    fields: Vec<FieldRef>,
    metadata: HashMap<String, String>,
}

impl SchemaBuilder {
    /// Creates a new empty [`SchemaBuilder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new empty [`SchemaBuilder`] with space for `capacity` fields
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
            metadata: Default::default(),
        }
    }

    /// Appends a [`FieldRef`] to this [`SchemaBuilder`] without checking for collision
    pub fn push(&mut self, field: impl Into<FieldRef>) {
        self.fields.push(field.into())
    }

    /// Removes and returns the [`FieldRef`] as index `idx`
    ///
    /// # Panics
    ///
    /// Panics if index out of bounds
    pub fn remove(&mut self, idx: usize) -> FieldRef {
        self.fields.remove(idx)
    }

    /// Returns an immutable reference to the [`FieldRef`] at index `idx`
    ///
    /// # Panics
    ///
    /// Panics if index out of bounds
    pub fn field(&mut self, idx: usize) -> &FieldRef {
        &mut self.fields[idx]
    }

    /// Returns a mutable reference to the [`FieldRef`] at index `idx`
    ///
    /// # Panics
    ///
    /// Panics if index out of bounds
    pub fn field_mut(&mut self, idx: usize) -> &mut FieldRef {
        &mut self.fields[idx]
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    pub fn metadata(&mut self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Returns a mutable reference to the Map of custom metadata key-value pairs.
    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    /// Reverse the fileds
    pub fn reverse(&mut self) {
        self.fields.reverse();
    }

    /// Appends a [`FieldRef`] to this [`SchemaBuilder`] checking for collision
    ///
    /// If an existing field exists with the same name, calls [`Field::try_merge`]
    pub fn try_merge(&mut self, field: &FieldRef) -> Result<(), ArrowError> {
        // This could potentially be sped up with a HashMap or similar
        let existing = self.fields.iter_mut().find(|f| f.name() == field.name());
        match existing {
            Some(e) if Arc::ptr_eq(e, field) => {} // Nothing to do
            Some(e) => match Arc::get_mut(e) {
                Some(e) => e.try_merge(field.as_ref())?,
                None => {
                    let mut t = e.as_ref().clone();
                    t.try_merge(field)?;
                    *e = Arc::new(t)
                }
            },
            None => self.fields.push(field.clone()),
        }
        Ok(())
    }

    /// Consume this [`SchemaBuilder`] yielding the final [`Schema`]
    pub fn finish(self) -> Schema {
        Schema {
            fields: self.fields.into(),
            metadata: self.metadata,
        }
    }
}

impl From<&Fields> for SchemaBuilder {
    fn from(value: &Fields) -> Self {
        Self {
            fields: value.to_vec(),
            metadata: Default::default(),
        }
    }
}

impl From<Fields> for SchemaBuilder {
    fn from(value: Fields) -> Self {
        Self {
            fields: value.to_vec(),
            metadata: Default::default(),
        }
    }
}

impl From<&Schema> for SchemaBuilder {
    fn from(value: &Schema) -> Self {
        Self::from(value.clone())
    }
}

impl From<Schema> for SchemaBuilder {
    fn from(value: Schema) -> Self {
        Self {
            fields: value.fields.to_vec(),
            metadata: value.metadata,
        }
    }
}

impl Extend<FieldRef> for SchemaBuilder {
    fn extend<T: IntoIterator<Item = FieldRef>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.fields.reserve(iter.size_hint().0);
        for f in iter {
            self.push(f)
        }
    }
}

impl Extend<Field> for SchemaBuilder {
    fn extend<T: IntoIterator<Item = Field>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.fields.reserve(iter.size_hint().0);
        for f in iter {
            self.push(f)
        }
    }
}

/// A reference-counted reference to a [`Schema`].
pub type SchemaRef = Arc<Schema>;

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Schema {
    /// A sequence of fields that describe the schema.
    pub fields: Fields,
    /// A map of key-value pairs containing additional meta data.
    pub metadata: HashMap<String, String>,
}

impl Schema {
    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            fields: Default::default(),
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
    pub fn new(fields: impl Into<Fields>) -> Self {
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
    pub fn new_with_metadata(fields: impl Into<Fields>, metadata: HashMap<String, String>) -> Self {
        Self {
            fields: fields.into(),
            metadata,
        }
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
    pub fn try_merge(schemas: impl IntoIterator<Item = Self>) -> Result<Self, ArrowError> {
        let mut out_meta = HashMap::new();
        let mut out_fields = SchemaBuilder::new();
        for schema in schemas {
            let Schema { metadata, fields } = schema;

            // merge metadata
            for (key, value) in metadata.into_iter() {
                if let Some(old_val) = out_meta.get(&key) {
                    if old_val != &value {
                        return Err(ArrowError::SchemaError(format!(
                            "Fail to merge schema due to conflicting metadata. \
                                         Key '{key}' has different values '{old_val}' and '{value}'"
                        )));
                    }
                }
                out_meta.insert(key, value);
            }

            // merge fields
            fields.iter().try_for_each(|x| out_fields.try_merge(x))?
        }

        Ok(out_fields.finish().with_metadata(out_meta))
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Returns a vector with references to all fields (including nested fields)
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_schema::{DataType, Field, Fields, Schema};
    ///
    /// let f1 = Arc::new(Field::new("a", DataType::Boolean, false));
    ///
    /// let f2_inner = Arc::new(Field::new("b_inner", DataType::Int8, false));
    /// let f2 = Arc::new(Field::new("b", DataType::List(f2_inner.clone()), false));
    ///
    /// let f3_inner1 = Arc::new(Field::new("c_inner1", DataType::Int8, false));
    /// let f3_inner2 = Arc::new(Field::new("c_inner2", DataType::Int8, false));
    /// let f3 = Arc::new(Field::new(
    ///     "c",
    ///     DataType::Struct(vec![f3_inner1.clone(), f3_inner2.clone()].into()),
    ///     false
    /// ));
    ///
    /// let mut schema = Schema::new(vec![
    ///   f1.clone(), f2.clone(), f3.clone()
    /// ]);
    /// assert_eq!(
    ///     schema.flattened_fields(),
    ///     vec![
    ///         f1.as_ref(),
    ///         f2.as_ref(),
    ///         f2_inner.as_ref(),
    ///         f3.as_ref(),
    ///         f3_inner1.as_ref(),
    ///         f3_inner2.as_ref()
    ///    ]
    /// );
    /// ```
    #[inline]
    pub fn flattened_fields(&self) -> Vec<&Field> {
        self.fields.iter().flat_map(|f| f.fields()).collect()
    }

    /// Returns a vector with references to all fields (including nested fields)
    #[deprecated(since = "52.2.0", note = "Use `flattened_fields` instead")]
    #[inline]
    pub fn all_fields(&self) -> Vec<&Field> {
        self.flattened_fields()
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected using an
    /// offset within the internal `fields` vector.
    ///
    /// # Panics
    ///
    /// Panics if index out of bounds
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&Field, ArrowError> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns a vector of immutable references to all [`Field`] instances selected by
    /// the dictionary ID they use.
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With it, all functions related to it."
    )]
    pub fn fields_with_dict_id(&self, dict_id: i64) -> Vec<&Field> {
        #[allow(deprecated)]
        self.fields
            .iter()
            .flat_map(|f| f.fields_with_dict_id(dict_id))
            .collect()
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize, ArrowError> {
        let (idx, _) = self.fields().find(name).ok_or_else(|| {
            let valid_fields: Vec<_> = self.fields.iter().map(|f| f.name()).collect();
            ArrowError::SchemaError(format!(
                "Unable to get field named \"{name}\". Valid fields: {valid_fields:?}"
            ))
        })?;
        Ok(idx)
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Normalize a [`Schema`] into a flat table.
    ///
    /// Nested [`Field`]s will generate names separated by `separator`, up to a depth of `max_level`
    /// (unlimited if `None`).
    ///
    /// e.g. given a [`Schema`]:
    ///
    /// ```text
    ///     "foo": StructArray<"bar": Utf8>
    /// ```
    ///
    /// A separator of `"."` would generate a batch with the schema:
    ///
    /// ```text
    ///     "foo.bar": Utf8
    /// ```
    ///
    /// Note that giving a depth of `Some(0)` to `max_level` is the same as passing in `None`;
    /// it will be treated as unlimited.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_schema::{DataType, Field, Fields, Schema};
    /// let schema = Schema::new(vec![
    ///     Field::new(
    ///         "a",
    ///         DataType::Struct(Fields::from(vec![
    ///             Arc::new(Field::new("animals", DataType::Utf8, true)),
    ///             Arc::new(Field::new("n_legs", DataType::Int64, true)),
    ///         ])),
    ///         false,
    ///     ),
    /// ])
    /// .normalize(".", None)
    /// .expect("valid normalization");
    /// let expected = Schema::new(vec![
    ///     Field::new("a.animals", DataType::Utf8, true),
    ///     Field::new("a.n_legs", DataType::Int64, true),
    /// ]);
    /// assert_eq!(schema, expected);
    /// ```
    pub fn normalize(&self, separator: &str, max_level: Option<usize>) -> Result<Self, ArrowError> {
        let max_level = match max_level.unwrap_or(usize::MAX) {
            0 => usize::MAX,
            val => val,
        };
        let mut stack: Vec<(usize, Vec<&str>, &FieldRef)> = self
            .fields()
            .iter()
            .rev()
            .map(|f| {
                let name_vec: Vec<&str> = vec![f.name()];
                (0, name_vec, f)
            })
            .collect();
        let mut fields: Vec<FieldRef> = Vec::new();

        while let Some((depth, name, field_ref)) = stack.pop() {
            match field_ref.data_type() {
                DataType::Struct(ff) if depth < max_level => {
                    // Need to zip these in reverse to maintain original order
                    for fff in ff.into_iter().rev() {
                        let mut name = name.clone();
                        name.push(separator);
                        name.push(fff.name());
                        stack.push((depth + 1, name, fff))
                    }
                }
                _ => {
                    let updated_field = Field::new(
                        name.concat(),
                        field_ref.data_type().clone(),
                        field_ref.is_nullable(),
                    );
                    fields.push(Arc::new(updated_field));
                }
            }
        }
        Ok(Schema::new(fields))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        let (idx, field) = self.fields.find(name)?;
        Some((idx, field.as_ref()))
    }

    /// Check to see if `self` is a superset of `other` schema.
    ///
    /// In particular returns true if `self.metadata` is a superset of `other.metadata`
    /// and [`Fields::contains`] for `self.fields` and `other.fields`
    ///
    /// In other words, any record that conforms to `other` should also conform to `self`.
    pub fn contains(&self, other: &Schema) -> bool {
        // make sure self.metadata is a superset of other.metadata
        self.fields.contains(&other.fields)
            && other
                .metadata
                .iter()
                .all(|(k, v1)| self.metadata.get(k).map(|v2| v1 == v2).unwrap_or_default())
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
#[allow(clippy::derived_hash_with_manual_eq)]
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
    use crate::datatype::DataType;
    use crate::{TimeUnit, UnionMode};

    use super::*;

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
        let schema =
            schema.with_metadata([("key".to_owned(), "val".to_owned())].into_iter().collect());
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
        #[allow(deprecated)]
        let dict_id = first_name.dict_id();
        assert_eq!(dict_id, None);
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
        #[allow(deprecated)]
        let dict_id = interests.dict_id();
        assert_eq!(dict_id, Some(123));
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
    fn normalize_simple() {
        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("animals", DataType::Utf8, true)),
                    Arc::new(Field::new("n_legs", DataType::Int64, true)),
                    Arc::new(Field::new("year", DataType::Int64, true)),
                ])),
                false,
            ),
            Field::new("month", DataType::Int64, true),
        ])
        .normalize(".", Some(0))
        .expect("valid normalization");

        let expected = Schema::new(vec![
            Field::new("a.animals", DataType::Utf8, true),
            Field::new("a.n_legs", DataType::Int64, true),
            Field::new("a.year", DataType::Int64, true),
            Field::new("month", DataType::Int64, true),
        ]);

        assert_eq!(schema, expected);

        // Check that 0, None have the same result
        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("animals", DataType::Utf8, true)),
                    Arc::new(Field::new("n_legs", DataType::Int64, true)),
                    Arc::new(Field::new("year", DataType::Int64, true)),
                ])),
                false,
            ),
            Field::new("month", DataType::Int64, true),
        ])
        .normalize(".", None)
        .expect("valid normalization");

        assert_eq!(schema, expected);
    }

    #[test]
    fn normalize_nested() {
        let a = Arc::new(Field::new("a", DataType::Utf8, true));
        let b = Arc::new(Field::new("b", DataType::Int64, false));
        let c = Arc::new(Field::new("c", DataType::Int64, true));

        let d = Arc::new(Field::new("d", DataType::Utf8, true));
        let e = Arc::new(Field::new("e", DataType::Int64, false));
        let f = Arc::new(Field::new("f", DataType::Int64, true));

        let one = Arc::new(Field::new(
            "1",
            DataType::Struct(Fields::from(vec![a.clone(), b.clone(), c.clone()])),
            false,
        ));
        let two = Arc::new(Field::new(
            "2",
            DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
            true,
        ));

        let exclamation = Arc::new(Field::new(
            "!",
            DataType::Struct(Fields::from(vec![one, two])),
            false,
        ));

        let normalize_all = Schema::new(vec![exclamation.clone()])
            .normalize(".", Some(0))
            .expect("valid normalization");

        let expected = Schema::new(vec![
            Field::new("!.1.a", DataType::Utf8, true),
            Field::new("!.1.b", DataType::Int64, false),
            Field::new("!.1.c", DataType::Int64, true),
            Field::new("!.2.d", DataType::Utf8, true),
            Field::new("!.2.e", DataType::Int64, false),
            Field::new("!.2.f", DataType::Int64, true),
        ]);

        assert_eq!(normalize_all, expected);

        let normalize_depth_one = Schema::new(vec![exclamation])
            .normalize(".", Some(1))
            .expect("valid normalization");

        let expected = Schema::new(vec![
            Field::new("!.1", DataType::Struct(Fields::from(vec![a, b, c])), false),
            Field::new("!.2", DataType::Struct(Fields::from(vec![d, e, f])), true),
        ]);

        assert_eq!(normalize_depth_one, expected);
    }

    #[test]
    fn normalize_list() {
        // Only the Struct type field should be unwrapped
        let a = Arc::new(Field::new("a", DataType::Utf8, true));
        let b = Arc::new(Field::new("b", DataType::Int64, false));
        let c = Arc::new(Field::new("c", DataType::Int64, true));
        let d = Arc::new(Field::new("d", DataType::Utf8, true));
        let e = Arc::new(Field::new("e", DataType::Int64, false));
        let f = Arc::new(Field::new("f", DataType::Int64, true));

        let one = Arc::new(Field::new(
            "1",
            DataType::Struct(Fields::from(vec![a.clone(), b.clone(), c.clone()])),
            true,
        ));

        let two = Arc::new(Field::new(
            "2",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                true,
            ))),
            false,
        ));

        let exclamation = Arc::new(Field::new(
            "!",
            DataType::Struct(Fields::from(vec![one.clone(), two.clone()])),
            false,
        ));

        let normalize_all = Schema::new(vec![exclamation.clone()])
            .normalize(".", None)
            .expect("valid normalization");

        // List shouldn't be affected
        let expected = Schema::new(vec![
            Field::new("!.1.a", DataType::Utf8, true),
            Field::new("!.1.b", DataType::Int64, false),
            Field::new("!.1.c", DataType::Int64, true),
            Field::new(
                "!.2",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                    true,
                ))),
                false,
            ),
        ]);

        assert_eq!(normalize_all, expected);
        assert_eq!(normalize_all.fields().len(), 4);

        // FixedSizeList
        let two = Arc::new(Field::new(
            "2",
            DataType::FixedSizeList(
                Arc::new(Field::new_fixed_size_list(
                    "3",
                    Arc::new(Field::new_list_field(
                        DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                        true,
                    )),
                    1,
                    true,
                )),
                1,
            ),
            false,
        ));

        let exclamation = Arc::new(Field::new(
            "!",
            DataType::Struct(Fields::from(vec![one.clone(), two])),
            false,
        ));

        let normalize_all = Schema::new(vec![exclamation.clone()])
            .normalize(".", None)
            .expect("valid normalization");

        // FixedSizeList shouldn't be affected
        let expected = Schema::new(vec![
            Field::new("!.1.a", DataType::Utf8, true),
            Field::new("!.1.b", DataType::Int64, false),
            Field::new("!.1.c", DataType::Int64, true),
            Field::new(
                "!.2",
                DataType::FixedSizeList(
                    Arc::new(Field::new_fixed_size_list(
                        "3",
                        Arc::new(Field::new_list_field(
                            DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                            true,
                        )),
                        1,
                        true,
                    )),
                    1,
                ),
                false,
            ),
        ]);

        assert_eq!(normalize_all, expected);
        assert_eq!(normalize_all.fields().len(), 4);

        // LargeList
        let two = Arc::new(Field::new(
            "2",
            DataType::FixedSizeList(
                Arc::new(Field::new_large_list(
                    "3",
                    Arc::new(Field::new_list_field(
                        DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                        true,
                    )),
                    true,
                )),
                1,
            ),
            false,
        ));

        let exclamation = Arc::new(Field::new(
            "!",
            DataType::Struct(Fields::from(vec![one.clone(), two])),
            false,
        ));

        let normalize_all = Schema::new(vec![exclamation.clone()])
            .normalize(".", None)
            .expect("valid normalization");

        // LargeList shouldn't be affected
        let expected = Schema::new(vec![
            Field::new("!.1.a", DataType::Utf8, true),
            Field::new("!.1.b", DataType::Int64, false),
            Field::new("!.1.c", DataType::Int64, true),
            Field::new(
                "!.2",
                DataType::FixedSizeList(
                    Arc::new(Field::new_large_list(
                        "3",
                        Arc::new(Field::new_list_field(
                            DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                            true,
                        )),
                        true,
                    )),
                    1,
                ),
                false,
            ),
        ]);

        assert_eq!(normalize_all, expected);
        assert_eq!(normalize_all.fields().len(), 4);
    }

    #[test]
    fn normalize_deep_nested() {
        // No unwrapping expected
        let a = Arc::new(Field::new("a", DataType::Utf8, true));
        let b = Arc::new(Field::new("b", DataType::Int64, false));
        let c = Arc::new(Field::new("c", DataType::Int64, true));
        let d = Arc::new(Field::new("d", DataType::Utf8, true));
        let e = Arc::new(Field::new("e", DataType::Int64, false));
        let f = Arc::new(Field::new("f", DataType::Int64, true));

        let one = Arc::new(Field::new(
            "1",
            DataType::Struct(Fields::from(vec![a.clone(), b.clone(), c.clone()])),
            true,
        ));

        let two = Arc::new(Field::new(
            "2",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![d.clone(), e.clone(), f.clone()])),
                true,
            ))),
            false,
        ));

        let l10 = Arc::new(Field::new(
            "l10",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![one, two])),
                true,
            ))),
            false,
        ));

        let l9 = Arc::new(Field::new(
            "l9",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l10])),
                true,
            ))),
            false,
        ));

        let l8 = Arc::new(Field::new(
            "l8",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l9])),
                true,
            ))),
            false,
        ));
        let l7 = Arc::new(Field::new(
            "l7",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l8])),
                true,
            ))),
            false,
        ));
        let l6 = Arc::new(Field::new(
            "l6",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l7])),
                true,
            ))),
            false,
        ));
        let l5 = Arc::new(Field::new(
            "l5",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l6])),
                true,
            ))),
            false,
        ));
        let l4 = Arc::new(Field::new(
            "l4",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l5])),
                true,
            ))),
            false,
        ));
        let l3 = Arc::new(Field::new(
            "l3",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l4])),
                true,
            ))),
            false,
        ));
        let l2 = Arc::new(Field::new(
            "l2",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l3])),
                true,
            ))),
            false,
        ));
        let l1 = Arc::new(Field::new(
            "l1",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![l2])),
                true,
            ))),
            false,
        ));

        let normalize_all = Schema::new(vec![l1])
            .normalize(".", None)
            .expect("valid normalization");

        assert_eq!(normalize_all.fields().len(), 1);
    }

    #[test]
    fn normalize_dictionary() {
        let a = Arc::new(Field::new("a", DataType::Utf8, true));
        let b = Arc::new(Field::new("b", DataType::Int64, false));

        let one = Arc::new(Field::new(
            "1",
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(Fields::from(vec![a.clone(), b.clone()]))),
            ),
            false,
        ));

        let normalize_all = Schema::new(vec![one.clone()])
            .normalize(".", None)
            .expect("valid normalization");

        let expected = Schema::new(vec![Field::new(
            "1",
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(Fields::from(vec![a.clone(), b.clone()]))),
            ),
            false,
        )]);

        assert_eq!(normalize_all, expected);
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

        #[allow(deprecated)]
        let fields_dict_123: Vec<_> = schema
            .fields_with_dict_id(123)
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(fields_dict_123, vec!["interests"]);

        #[allow(deprecated)]
        let is_empty = schema.fields_with_dict_id(456).is_empty();
        assert!(is_empty);
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
                DataType::Struct(Fields::from(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ])),
                false,
            ),
            #[allow(deprecated)]
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

        assert!(Schema::try_merge(vec![Schema::new(vec![f1]), Schema::new(vec![f2])]).is_err());

        // 2. None + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let metadata2: HashMap<String, String> = [("missing".to_string(), "value".to_string())]
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
                    DataType::Struct(vec![Field::new("zip", DataType::UInt16, false)].into()),
                    false,
                ),
            ]),
            Schema::new_with_metadata(
                vec![
                    // nullable merge
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(Fields::from(vec![
                            // add new nested field
                            Field::new("street", DataType::Utf8, false),
                            // nullable merge on nested field
                            Field::new("zip", DataType::UInt16, true),
                        ])),
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
                        DataType::Struct(Fields::from(vec![
                            Field::new("zip", DataType::UInt16, true),
                            Field::new("street", DataType::Utf8, false),
                        ])),
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
                Schema::new(vec![Field::new_union(
                    "c1",
                    vec![0, 1],
                    vec![
                        Field::new("c11", DataType::Utf8, true),
                        Field::new("c12", DataType::Utf8, true),
                    ],
                    UnionMode::Dense
                ),]),
                Schema::new(vec![Field::new_union(
                    "c1",
                    vec![1, 2],
                    vec![
                        Field::new("c12", DataType::Utf8, true),
                        Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                    ],
                    UnionMode::Dense
                ),])
            ])
            .unwrap(),
            Schema::new(vec![Field::new_union(
                "c1",
                vec![0, 1, 2],
                vec![
                    Field::new("c11", DataType::Utf8, true),
                    Field::new("c12", DataType::Utf8, true),
                    Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                ],
                UnionMode::Dense
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
            "Could not find expected string '{expected}' in '{res}'"
        );
    }

    #[test]
    fn test_schema_builder_change_field() {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("a", DataType::Int32, false));
        builder.push(Field::new("b", DataType::Utf8, false));
        *builder.field_mut(1) = Arc::new(Field::new("c", DataType::Int32, false));
        assert_eq!(
            builder.fields,
            vec![
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Field::new("c", DataType::Int32, false))
            ]
        );
    }

    #[test]
    fn test_schema_builder_reverse() {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("a", DataType::Int32, false));
        builder.push(Field::new("b", DataType::Utf8, true));
        builder.reverse();
        assert_eq!(
            builder.fields,
            vec![
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(Field::new("a", DataType::Int32, false))
            ]
        );
    }

    #[test]
    fn test_schema_builder_metadata() {
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert("key".to_string(), "value".to_string());

        let fields = vec![Field::new("test", DataType::Int8, true)];
        let mut builder: SchemaBuilder = Schema::new(fields).with_metadata(metadata).into();
        builder.metadata_mut().insert("k".into(), "v".into());
        let out = builder.finish();
        assert_eq!(out.metadata.len(), 2);
        assert_eq!(out.metadata["k"], "v");
        assert_eq!(out.metadata["key"], "value");
    }
}
