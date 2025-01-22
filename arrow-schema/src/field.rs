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
use std::sync::Arc;

use crate::datatype::DataType;
#[cfg(feature = "canonical_extension_types")]
use crate::extension::CanonicalExtensionType;
use crate::schema::SchemaBuilder;
use crate::{
    extension::{ExtensionType, EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
    Fields, UnionFields, UnionMode,
};

/// A reference counted [`Field`]
pub type FieldRef = Arc<Field>;

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
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With it, all fields related to it."
    )]
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
    /// Default list member field name
    pub const LIST_FIELD_DEFAULT_NAME: &'static str = "item";

    /// Creates a new field with the given name, type, and nullability
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        #[allow(deprecated)]
        Field {
            name: name.into(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new `Field` suitable for [`DataType::List`] and
    /// [`DataType::LargeList`]
    ///
    /// While not required, this method follows the convention of naming the
    /// `Field` `"item"`.
    ///
    /// # Example
    /// ```
    /// # use arrow_schema::{Field, DataType};
    /// assert_eq!(
    ///   Field::new("item", DataType::Int32, true),
    ///   Field::new_list_field(DataType::Int32, true)
    /// );
    /// ```
    pub fn new_list_field(data_type: DataType, nullable: bool) -> Self {
        Self::new(Self::LIST_FIELD_DEFAULT_NAME, data_type, nullable)
    }

    /// Creates a new field that has additional dictionary information
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With the dict_id field disappearing this function signature will change by removing the dict_id parameter."
    )]
    pub fn new_dict(
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        #[allow(deprecated)]
        Field {
            name: name.into(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: HashMap::default(),
        }
    }

    /// Create a new [`Field`] with [`DataType::Dictionary`]
    ///
    /// Use [`Self::new_dict`] for more advanced dictionary options
    ///
    /// # Panics
    ///
    /// Panics if [`!key.is_dictionary_key_type`][DataType::is_dictionary_key_type]
    pub fn new_dictionary(
        name: impl Into<String>,
        key: DataType,
        value: DataType,
        nullable: bool,
    ) -> Self {
        assert!(
            key.is_dictionary_key_type(),
            "{key} is not a valid dictionary key"
        );
        let data_type = DataType::Dictionary(Box::new(key), Box::new(value));
        Self::new(name, data_type, nullable)
    }

    /// Create a new [`Field`] with [`DataType::Struct`]
    ///
    /// - `name`: the name of the [`DataType::Struct`] field
    /// - `fields`: the description of each struct element
    /// - `nullable`: if the [`DataType::Struct`] array is nullable
    pub fn new_struct(name: impl Into<String>, fields: impl Into<Fields>, nullable: bool) -> Self {
        Self::new(name, DataType::Struct(fields.into()), nullable)
    }

    /// Create a new [`Field`] with [`DataType::List`]
    ///
    /// - `name`: the name of the [`DataType::List`] field
    /// - `value`: the description of each list element
    /// - `nullable`: if the [`DataType::List`] array is nullable
    pub fn new_list(name: impl Into<String>, value: impl Into<FieldRef>, nullable: bool) -> Self {
        Self::new(name, DataType::List(value.into()), nullable)
    }

    /// Create a new [`Field`] with [`DataType::LargeList`]
    ///
    /// - `name`: the name of the [`DataType::LargeList`] field
    /// - `value`: the description of each list element
    /// - `nullable`: if the [`DataType::LargeList`] array is nullable
    pub fn new_large_list(
        name: impl Into<String>,
        value: impl Into<FieldRef>,
        nullable: bool,
    ) -> Self {
        Self::new(name, DataType::LargeList(value.into()), nullable)
    }

    /// Create a new [`Field`] with [`DataType::FixedSizeList`]
    ///
    /// - `name`: the name of the [`DataType::FixedSizeList`] field
    /// - `value`: the description of each list element
    /// - `size`: the size of the fixed size list
    /// - `nullable`: if the [`DataType::FixedSizeList`] array is nullable
    pub fn new_fixed_size_list(
        name: impl Into<String>,
        value: impl Into<FieldRef>,
        size: i32,
        nullable: bool,
    ) -> Self {
        Self::new(name, DataType::FixedSizeList(value.into(), size), nullable)
    }

    /// Create a new [`Field`] with [`DataType::Map`]
    ///
    /// - `name`: the name of the [`DataType::Map`] field
    /// - `entries`: the name of the inner [`DataType::Struct`] field
    /// - `keys`: the map keys
    /// - `values`: the map values
    /// - `sorted`: if the [`DataType::Map`] array is sorted
    /// - `nullable`: if the [`DataType::Map`] array is nullable
    pub fn new_map(
        name: impl Into<String>,
        entries: impl Into<String>,
        keys: impl Into<FieldRef>,
        values: impl Into<FieldRef>,
        sorted: bool,
        nullable: bool,
    ) -> Self {
        let data_type = DataType::Map(
            Arc::new(Field::new(
                entries.into(),
                DataType::Struct(Fields::from([keys.into(), values.into()])),
                false, // The inner map field is always non-nullable (#1697),
            )),
            sorted,
        );
        Self::new(name, data_type, nullable)
    }

    /// Create a new [`Field`] with [`DataType::Union`]
    ///
    /// - `name`: the name of the [`DataType::Union`] field
    /// - `type_ids`: the union type ids
    /// - `fields`: the union fields
    /// - `mode`: the union mode
    pub fn new_union<S, F, T>(name: S, type_ids: T, fields: F, mode: UnionMode) -> Self
    where
        S: Into<String>,
        F: IntoIterator,
        F::Item: Into<FieldRef>,
        T: IntoIterator<Item = i8>,
    {
        Self::new(
            name,
            DataType::Union(UnionFields::new(type_ids, fields), mode),
            false, // Unions cannot be nullable
        )
    }

    /// Sets the `Field`'s optional custom metadata.
    #[inline]
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
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

    /// Returns the extension type name of this [`Field`], if set.
    ///
    /// This returns the value of [`EXTENSION_TYPE_NAME_KEY`], if set in
    /// [`Field::metadata`]. If the key is missing, there is no extension type
    /// name and this returns `None`.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_schema::{DataType, extension::EXTENSION_TYPE_NAME_KEY, Field};
    ///
    /// let field = Field::new("", DataType::Null, false);
    /// assert_eq!(field.extension_type_name(), None);
    ///
    /// let field = Field::new("", DataType::Null, false).with_metadata(
    ///    [(EXTENSION_TYPE_NAME_KEY.to_owned(), "example".to_owned())]
    ///        .into_iter()
    ///        .collect(),
    /// );
    /// assert_eq!(field.extension_type_name(), Some("example"));
    /// ```
    pub fn extension_type_name(&self) -> Option<&str> {
        self.metadata()
            .get(EXTENSION_TYPE_NAME_KEY)
            .map(String::as_ref)
    }

    /// Returns the extension type metadata of this [`Field`], if set.
    ///
    /// This returns the value of [`EXTENSION_TYPE_METADATA_KEY`], if set in
    /// [`Field::metadata`]. If the key is missing, there is no extension type
    /// metadata and this returns `None`.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_schema::{DataType, extension::EXTENSION_TYPE_METADATA_KEY, Field};
    ///
    /// let field = Field::new("", DataType::Null, false);
    /// assert_eq!(field.extension_type_metadata(), None);
    ///
    /// let field = Field::new("", DataType::Null, false).with_metadata(
    ///    [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "example".to_owned())]
    ///        .into_iter()
    ///        .collect(),
    /// );
    /// assert_eq!(field.extension_type_metadata(), Some("example"));
    /// ```
    pub fn extension_type_metadata(&self) -> Option<&str> {
        self.metadata()
            .get(EXTENSION_TYPE_METADATA_KEY)
            .map(String::as_ref)
    }

    /// Returns an instance of the given [`ExtensionType`] of this [`Field`],
    /// if set in the [`Field::metadata`].
    ///
    /// # Error
    ///
    /// Returns an error if
    /// - this field does not have the name of this extension type
    ///   ([`ExtensionType::NAME`]) in the [`Field::metadata`] (mismatch or
    ///   missing)
    /// - the deserialization of the metadata
    ///   ([`ExtensionType::deserialize_metadata`]) fails
    /// - the construction of the extension type ([`ExtensionType::try_new`])
    ///   fail (for example when the [`Field::data_type`] is not supported by
    ///   the extension type ([`ExtensionType::supports_data_type`]))
    pub fn try_extension_type<E: ExtensionType>(&self) -> Result<E, ArrowError> {
        // Check the extension name in the metadata
        match self.extension_type_name() {
            // It should match the name of the given extension type
            Some(name) if name == E::NAME => {
                // Deserialize the metadata and try to construct the extension
                // type
                E::deserialize_metadata(self.extension_type_metadata())
                    .and_then(|metadata| E::try_new(self.data_type(), metadata))
            }
            // Name mismatch
            Some(name) => Err(ArrowError::InvalidArgumentError(format!(
                "Field extension type name mismatch, expected {}, found {name}",
                E::NAME
            ))),
            // Name missing
            None => Err(ArrowError::InvalidArgumentError(
                "Field extension type name missing".to_owned(),
            )),
        }
    }

    /// Returns an instance of the given [`ExtensionType`] of this [`Field`],
    /// panics if this [`Field`] does not have this extension type.
    ///
    /// # Panic
    ///
    /// This calls [`Field::try_extension_type`] and panics when it returns an
    /// error.
    pub fn extension_type<E: ExtensionType>(&self) -> E {
        self.try_extension_type::<E>()
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Updates the metadata of this [`Field`] with the [`ExtensionType::NAME`]
    /// and [`ExtensionType::metadata`] of the given [`ExtensionType`], if the
    /// given extension type supports the [`Field::data_type`] of this field
    /// ([`ExtensionType::supports_data_type`]).
    ///
    /// If the given extension type defines no metadata, a previously set
    /// value of [`EXTENSION_TYPE_METADATA_KEY`] is cleared.
    ///
    /// # Error
    ///
    /// This functions returns an error if the data type of this field does not
    /// match any of the supported storage types of the given extension type.
    pub fn try_with_extension_type<E: ExtensionType>(
        &mut self,
        extension_type: E,
    ) -> Result<(), ArrowError> {
        // Make sure the data type of this field is supported
        extension_type.supports_data_type(&self.data_type)?;

        self.metadata
            .insert(EXTENSION_TYPE_NAME_KEY.to_owned(), E::NAME.to_owned());
        match extension_type.serialize_metadata() {
            Some(metadata) => self
                .metadata
                .insert(EXTENSION_TYPE_METADATA_KEY.to_owned(), metadata),
            // If this extension type has no metadata, we make sure to
            // clear previously set metadata.
            None => self.metadata.remove(EXTENSION_TYPE_METADATA_KEY),
        };

        Ok(())
    }

    /// Updates the metadata of this [`Field`] with the [`ExtensionType::NAME`]
    /// and [`ExtensionType::metadata`] of the given [`ExtensionType`].
    ///
    /// # Panics
    ///
    /// This calls [`Field::try_with_extension_type`] and panics when it
    /// returns an error.
    pub fn with_extension_type<E: ExtensionType>(mut self, extension_type: E) -> Self {
        self.try_with_extension_type(extension_type)
            .unwrap_or_else(|e| panic!("{e}"));
        self
    }

    /// Returns the [`CanonicalExtensionType`] of this [`Field`], if set.
    ///
    /// # Error
    ///
    /// Returns an error if
    /// - this field does have a canonical extension type (mismatch or missing)
    /// - the canonical extension is not supported
    /// - the construction of the extension type fails
    #[cfg(feature = "canonical_extension_types")]
    pub fn try_canonical_extension_type(&self) -> Result<CanonicalExtensionType, ArrowError> {
        CanonicalExtensionType::try_from(self)
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
            DataType::Struct(fields) => fields.iter().flat_map(|f| f.fields()).collect(),
            DataType::Union(fields, _) => fields.iter().flat_map(|(_, f)| f.fields()).collect(),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => field.fields(),
            DataType::Dictionary(_, value_field) => Field::_fields(value_field.as_ref()),
            DataType::RunEndEncoded(_, field) => field.fields(),
            _ => vec![],
        }
    }

    /// Returns a vector containing all (potentially nested) `Field` instances selected by the
    /// dictionary ID they use
    #[inline]
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With it, all fields related to it."
    )]
    pub(crate) fn fields_with_dict_id(&self, id: i64) -> Vec<&Field> {
        self.fields()
            .into_iter()
            .filter(|&field| {
                #[allow(deprecated)]
                let matching_dict_id = field.dict_id == id;
                matches!(field.data_type(), DataType::Dictionary(_, _)) && matching_dict_id
            })
            .collect()
    }

    /// Returns the dictionary ID, if this is a dictionary type.
    #[inline]
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With it, all fields related to it."
    )]
    pub const fn dict_id(&self) -> Option<i64> {
        match self.data_type {
            #[allow(deprecated)]
            DataType::Dictionary(_, _) => Some(self.dict_id),
            _ => None,
        }
    }

    /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type.
    ///
    /// # Example
    /// ```
    /// # use arrow_schema::{DataType, Field};
    /// // non dictionaries do not have a dict is ordered flat
    /// let field = Field::new("c1", DataType::Int64, false);
    /// assert_eq!(field.dict_is_ordered(), None);
    /// // by default dictionary is not ordered
    /// let field = Field::new("c1", DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)), false);
    /// assert_eq!(field.dict_is_ordered(), Some(false));
    /// let field = field.with_dict_is_ordered(true);
    /// assert_eq!(field.dict_is_ordered(), Some(true));
    /// ```
    #[inline]
    pub const fn dict_is_ordered(&self) -> Option<bool> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_is_ordered),
            _ => None,
        }
    }

    /// Set the is ordered field for this `Field`, if it is a dictionary.
    ///
    /// Does nothing if this is not a dictionary type.
    ///
    /// See [`Field::dict_is_ordered`] for more information.
    pub fn with_dict_is_ordered(mut self, dict_is_ordered: bool) -> Self {
        if matches!(self.data_type, DataType::Dictionary(_, _)) {
            self.dict_is_ordered = dict_is_ordered;
        };
        self
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
        #[allow(deprecated)]
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
                    let mut builder = SchemaBuilder::new();
                    nested_fields.iter().chain(from_nested_fields).try_for_each(|f| builder.try_merge(f))?;
                    *nested_fields = builder.finish().fields;
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Struct",
                            self.name, from.data_type)
                ))}
            },
            DataType::Union(nested_fields, _) => match &from.data_type {
                DataType::Union(from_nested_fields, _) => {
                    nested_fields.try_merge(from_nested_fields)?
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Union",
                            self.name, from.data_type)
                    ));
                }
            },
            DataType::List(field) => match &from.data_type {
                DataType::List(from_field) => {
                    let mut f = (**field).clone();
                    f.try_merge(from_field)?;
                    (*field) = Arc::new(f);
                },
                _ => {
                    return Err(ArrowError::SchemaError(
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::List",
                            self.name, from.data_type)
                ))}
            },
            DataType::LargeList(field) => match &from.data_type {
                DataType::LargeList(from_field) => {
                    let mut f = (**field).clone();
                    f.try_merge(from_field)?;
                    (*field) = Arc::new(f);
                },
                _ => {
                    return Err(ArrowError::SchemaError(
                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::LargeList",
                            self.name, from.data_type)
                ))}
            },
            DataType::Null => {
                self.nullable = true;
                self.data_type = from.data_type.clone();
            }
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
            | DataType::BinaryView
            | DataType::Interval(_)
            | DataType::LargeListView(_)
            | DataType::ListView(_)
            | DataType::Map(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                if from.data_type == DataType::Null {
                    self.nullable = true;
                } else if self.data_type != from.data_type {
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
        #[allow(deprecated)]
        let matching_dict_id = self.dict_id == other.dict_id;
        self.name == other.name
        && self.data_type.contains(&other.data_type)
        && matching_dict_id
        && self.dict_is_ordered == other.dict_is_ordered
        // self need to be nullable or both of them are not nullable
        && (self.nullable || !other.nullable)
        // make sure self.metadata is a superset of other.metadata
        && other.metadata.iter().all(|(k, v1)| {
            self.metadata.get(k).map(|v2| v1 == v2).unwrap_or_default()
        })
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
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[test]
    fn test_new_with_string() {
        // Fields should allow owned Strings to support reuse
        let s = "c1";
        Field::new(s, DataType::Int64, false);
    }

    #[test]
    fn test_new_dict_with_string() {
        // Fields should allow owned Strings to support reuse
        let s = "c1";
        #[allow(deprecated)]
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
    fn test_merge_with_null() {
        let mut field1 = Field::new("c1", DataType::Null, true);
        field1
            .try_merge(&Field::new("c1", DataType::Float32, false))
            .expect("should widen type to nullable float");
        assert_eq!(Field::new("c1", DataType::Float32, true), field1);

        let mut field2 = Field::new("c2", DataType::Utf8, false);
        field2
            .try_merge(&Field::new("c2", DataType::Null, true))
            .expect("should widen type to nullable utf8");
        assert_eq!(Field::new("c2", DataType::Utf8, true), field2);
    }

    #[test]
    fn test_merge_with_nested_null() {
        let mut struct1 = Field::new(
            "s1",
            DataType::Struct(Fields::from(vec![Field::new(
                "inner",
                DataType::Float32,
                false,
            )])),
            false,
        );

        let struct2 = Field::new(
            "s2",
            DataType::Struct(Fields::from(vec![Field::new(
                "inner",
                DataType::Null,
                false,
            )])),
            true,
        );

        struct1
            .try_merge(&struct2)
            .expect("should widen inner field's type to nullable float");
        assert_eq!(
            Field::new(
                "s1",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::Float32,
                    true,
                )])),
                true,
            ),
            struct1
        );

        let mut list1 = Field::new(
            "l1",
            DataType::List(Field::new("inner", DataType::Float32, false).into()),
            false,
        );

        let list2 = Field::new(
            "l2",
            DataType::List(Field::new("inner", DataType::Null, false).into()),
            true,
        );

        list1
            .try_merge(&list2)
            .expect("should widen inner field's type to nullable float");
        assert_eq!(
            Field::new(
                "l1",
                DataType::List(Field::new("inner", DataType::Float32, true).into()),
                true,
            ),
            list1
        );

        let mut large_list1 = Field::new(
            "ll1",
            DataType::LargeList(Field::new("inner", DataType::Float32, false).into()),
            false,
        );

        let large_list2 = Field::new(
            "ll2",
            DataType::LargeList(Field::new("inner", DataType::Null, false).into()),
            true,
        );

        large_list1
            .try_merge(&large_list2)
            .expect("should widen inner field's type to nullable float");
        assert_eq!(
            Field::new(
                "ll1",
                DataType::LargeList(Field::new("inner", DataType::Float32, true).into()),
                true,
            ),
            large_list1
        );
    }

    #[test]
    fn test_fields_with_dict_id() {
        #[allow(deprecated)]
        let dict1 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            10,
            false,
        );
        #[allow(deprecated)]
        let dict2 = Field::new_dict(
            "dict2",
            DataType::Dictionary(DataType::Int32.into(), DataType::Int8.into()),
            false,
            20,
            false,
        );

        let field = Field::new(
            "struct<dict1, list[struct<dict2, list[struct<dict1]>]>",
            DataType::Struct(Fields::from(vec![
                dict1.clone(),
                Field::new(
                    "list[struct<dict1, list[struct<dict2>]>]",
                    DataType::List(Arc::new(Field::new(
                        "struct<dict1, list[struct<dict2>]>",
                        DataType::Struct(Fields::from(vec![
                            dict1.clone(),
                            Field::new(
                                "list[struct<dict2>]",
                                DataType::List(Arc::new(Field::new(
                                    "struct<dict2>",
                                    DataType::Struct(vec![dict2.clone()].into()),
                                    false,
                                ))),
                                false,
                            ),
                        ])),
                        false,
                    ))),
                    false,
                ),
            ])),
            false,
        );

        #[allow(deprecated)]
        for field in field.fields_with_dict_id(10) {
            assert_eq!(dict1, *field);
        }
        #[allow(deprecated)]
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
        #[allow(deprecated)]
        let dict1 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            10,
            false,
        );
        #[allow(deprecated)]
        let dict2 = Field::new_dict(
            "dict1",
            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
            false,
            20,
            false,
        );

        assert_eq!(dict1, dict2);
        assert_eq!(get_field_hash(&dict1), get_field_hash(&dict2));

        #[allow(deprecated)]
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

        let mut field1 = Field::new(
            "field1",
            DataType::Struct(Fields::from(vec![child_field])),
            false,
        );
        field1.set_metadata(HashMap::from([(String::from("k1"), String::from("v1"))]));

        let mut field2 = Field::new("field1", DataType::Struct(Fields::default()), true);
        field2.set_metadata(HashMap::from([(String::from("k2"), String::from("v2"))]));
        field2.try_merge(&field1).unwrap();

        let mut field3 = Field::new("field1", DataType::Struct(Fields::default()), false);
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

        let field1 = Field::new(
            "field1",
            DataType::Struct(vec![child_field1.clone()].into()),
            true,
        );
        let field2 = Field::new(
            "field1",
            DataType::Struct(vec![child_field1, child_field2].into()),
            true,
        );

        assert!(!field1.contains(&field2));
        assert!(!field2.contains(&field1));

        // UnionFields with different type ID
        let field1 = Field::new(
            "field1",
            DataType::Union(
                UnionFields::new(
                    vec![1, 2],
                    vec![
                        Field::new("field1", DataType::UInt8, true),
                        Field::new("field3", DataType::Utf8, false),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        );
        let field2 = Field::new(
            "field1",
            DataType::Union(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("field1", DataType::UInt8, false),
                        Field::new("field3", DataType::Utf8, false),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        );
        assert!(!field1.contains(&field2));

        // UnionFields with same type ID
        let field1 = Field::new(
            "field1",
            DataType::Union(
                UnionFields::new(
                    vec![1, 2],
                    vec![
                        Field::new("field1", DataType::UInt8, true),
                        Field::new("field3", DataType::Utf8, false),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        );
        let field2 = Field::new(
            "field1",
            DataType::Union(
                UnionFields::new(
                    vec![1, 2],
                    vec![
                        Field::new("field1", DataType::UInt8, false),
                        Field::new("field3", DataType::Utf8, false),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        );
        assert!(field1.contains(&field2));
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
        let field = Field::new("name", DataType::Boolean, false).with_metadata(HashMap::new());

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
