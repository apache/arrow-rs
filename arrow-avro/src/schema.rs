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

use arrow_schema::ArrowError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use strum_macros::AsRefStr;

/// The metadata key used for storing the JSON encoded [`Schema`]
pub const SCHEMA_METADATA_KEY: &str = "avro.schema";

/// The Avro single‑object encoding “magic” bytes (`0xC3 0x01`)
pub const SINGLE_OBJECT_MAGIC: [u8; 2] = [0xC3, 0x01];

/// Compare two Avro schemas for equality (identical schemas).
/// Returns true if the schemas have the same parsing canonical form (i.e., logically identical).
pub fn compare_schemas(writer: &Schema, reader: &Schema) -> Result<bool, ArrowError> {
    let canon_writer = generate_canonical_form(writer)?;
    let canon_reader = generate_canonical_form(reader)?;
    Ok(canon_writer == canon_reader)
}

/// Either a [`PrimitiveType`] or a reference to a previously defined named type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#names>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
/// A type name in an Avro schema
///
/// This represents the different ways a type can be referenced in an Avro schema.
pub enum TypeName<'a> {
    /// A primitive type like null, boolean, int, etc.
    Primitive(PrimitiveType),
    /// A reference to another named type
    Ref(&'a str),
}

/// A primitive type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#primitive-types>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "lowercase")]
pub enum PrimitiveType {
    /// null: no value
    Null,
    /// boolean: a binary value
    Boolean,
    /// int: 32-bit signed integer
    Int,
    /// long: 64-bit signed integer
    Long,
    /// float: single precision (32-bit) IEEE 754 floating-point number
    Float,
    /// double: double precision (64-bit) IEEE 754 floating-point number
    Double,
    /// bytes: sequence of 8-bit unsigned bytes
    Bytes,
    /// string: Unicode character sequence
    String,
}

/// Additional attributes within a [`Schema`]
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-declaration>
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes<'a> {
    /// A logical type name
    ///
    /// <https://avro.apache.org/docs/1.11.1/specification/#logical-types>
    #[serde(default)]
    pub logical_type: Option<&'a str>,

    /// Additional JSON attributes
    #[serde(flatten)]
    pub additional: HashMap<&'a str, serde_json::Value>,
}

impl Attributes<'_> {
    /// Returns the field metadata for this [`Attributes`]
    pub(crate) fn field_metadata(&self) -> HashMap<String, String> {
        self.additional
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

/// A type definition that is not a variant of [`ComplexType`]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Type<'a> {
    /// The type of this Avro data structure
    #[serde(borrow)]
    pub r#type: TypeName<'a>,
    /// Additional attributes associated with this type
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// An Avro schema
///
/// This represents the different shapes of Avro schemas as defined in the specification.
/// See <https://avro.apache.org/docs/1.11.1/specification/#schemas> for more details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Schema<'a> {
    /// A direct type name (primitive or reference)
    #[serde(borrow)]
    TypeName(TypeName<'a>),
    /// A union of multiple schemas (e.g., ["null", "string"])
    #[serde(borrow)]
    Union(Vec<Schema<'a>>),
    /// A complex type such as record, array, map, etc.
    #[serde(borrow)]
    Complex(ComplexType<'a>),
    /// A type with attributes
    #[serde(borrow)]
    Type(Type<'a>),
}

/// A complex type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#complex-types>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ComplexType<'a> {
    /// Record type: a sequence of fields with names and types
    #[serde(borrow)]
    Record(Record<'a>),
    /// Enum type: a set of named values
    #[serde(borrow)]
    Enum(Enum<'a>),
    /// Array type: a sequence of values of the same type
    #[serde(borrow)]
    Array(Array<'a>),
    /// Map type: a mapping from strings to values of the same type
    #[serde(borrow)]
    Map(Map<'a>),
    /// Fixed type: a fixed-size byte array
    #[serde(borrow)]
    Fixed(Fixed<'a>),
}

/// A record
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-record>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record<'a> {
    /// Name of the record
    #[serde(borrow)]
    pub name: &'a str,
    /// Optional namespace for the record, provides a way to organize names
    #[serde(borrow, default)]
    pub namespace: Option<&'a str>,
    /// Optional documentation string for the record
    #[serde(borrow, default)]
    pub doc: Option<&'a str>,
    /// Alternative names for this record
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    /// The fields contained in this record
    #[serde(borrow)]
    pub fields: Vec<Field<'a>>,
    /// Additional attributes for this record
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A field within a [`Record`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field<'a> {
    /// Name of the field within the record
    #[serde(borrow)]
    pub name: &'a str,
    /// Optional documentation for this field
    #[serde(borrow, default)]
    pub doc: Option<&'a str>,
    /// The field's type definition
    #[serde(borrow)]
    pub r#type: Schema<'a>,
    /// Optional default value for this field
    #[serde(borrow, default)]
    pub default: Option<&'a str>,
}

/// An enumeration
///
/// <https://avro.apache.org/docs/1.11.1/specification/#enums>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Enum<'a> {
    /// Name of the enum
    #[serde(borrow)]
    pub name: &'a str,
    /// Optional namespace for the enum, provides organizational structure
    #[serde(borrow, default)]
    pub namespace: Option<&'a str>,
    /// Optional documentation string describing the enum
    #[serde(borrow, default)]
    pub doc: Option<&'a str>,
    /// Alternative names for this enum
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    /// The symbols (values) that this enum can have
    #[serde(borrow)]
    pub symbols: Vec<&'a str>,
    /// Optional default value for this enum
    #[serde(borrow, default)]
    pub default: Option<&'a str>,
    /// Additional attributes for this enum
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// An array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#arrays>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Array<'a> {
    /// The schema for items in this array
    #[serde(borrow)]
    pub items: Box<Schema<'a>>,
    /// Additional attributes for this array
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A map
///
/// <https://avro.apache.org/docs/1.11.1/specification/#maps>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Map<'a> {
    /// The schema for values in this map
    #[serde(borrow)]
    pub values: Box<Schema<'a>>,
    /// Additional attributes for this map
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// A fixed length binary array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#fixed>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fixed<'a> {
    /// Name of the fixed type
    #[serde(borrow)]
    pub name: &'a str,
    /// Optional namespace for the fixed type
    #[serde(borrow, default)]
    pub namespace: Option<&'a str>,
    /// Alternative names for this fixed type
    #[serde(borrow, default)]
    pub aliases: Vec<&'a str>,
    /// The number of bytes in this fixed type
    pub size: usize,
    /// Additional attributes for this fixed type
    #[serde(flatten)]
    pub attributes: Attributes<'a>,
}

/// Supported fingerprint algorithms for Avro schema identification.
/// Currently only `Rabin` is supported, `SHA256` and `MD5` support will come in a future update
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FingerprintAlgorithm {
    /// 64‑bit CRC‑64‑AVRO Rabin fingerprint.
    Rabin,
}

/// A schema fingerprint in one of the supported formats.
///
/// This is used as the key inside `SchemaStore` `HashMap`. Each `SchemaStore`
/// instance always stores only one variant, matching its configured
/// `FingerprintAlgorithm`, but the enum makes the API uniform.
/// Currently only `Rabin` is supported
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-fingerprints>
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fingerprint {
    /// A 64-bit Rabin fingerprint.
    Rabin(u64),
}

/// Allow easy extraction of the algorithm used to create a fingerprint.
impl From<&Fingerprint> for FingerprintAlgorithm {
    fn from(fp: &Fingerprint) -> Self {
        match fp {
            Fingerprint::Rabin(_) => FingerprintAlgorithm::Rabin,
        }
    }
}

/// Generates a fingerprint for the given `Schema` using the specified `FingerprintAlgorithm`.
pub(crate) fn generate_fingerprint(
    schema: &Schema,
    hash_type: FingerprintAlgorithm,
) -> Result<Fingerprint, ArrowError> {
    let canonical = generate_canonical_form(schema).map_err(|e| {
        ArrowError::ComputeError(format!("Failed to generate canonical form for schema: {e}"))
    })?;
    match hash_type {
        FingerprintAlgorithm::Rabin => {
            Ok(Fingerprint::Rabin(compute_fingerprint_rabin(&canonical)))
        }
    }
}

/// Generates the 64-bit Rabin fingerprint for the given `Schema`.
///
/// The fingerprint is computed from the canonical form of the schema.
/// This is also known as `CRC-64-AVRO`.
///
/// # Returns
/// A `Fingerprint::Rabin` variant containing the 64-bit fingerprint.
pub fn generate_fingerprint_rabin(schema: &Schema) -> Result<Fingerprint, ArrowError> {
    generate_fingerprint(schema, FingerprintAlgorithm::Rabin)
}

/// Generates the Parsed Canonical Form for the given [`Schema`].
///
/// The canonical form is a standardized JSON representation of the schema,
/// primarily used for generating a schema fingerprint for equality checking.
///
/// This form strips attributes that do not affect the schema's identity,
/// such as `doc` fields, `aliases`, and any properties not defined in the
/// Avro specification.
///
/// <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
pub fn generate_canonical_form(schema: &Schema) -> Result<String, ArrowError> {
    build_canonical(schema, None)
}

/// An in-memory cache of Avro schemas, indexed by their fingerprint.
///
/// `SchemaStore` provides a mechanism to store and retrieve Avro schemas efficiently.
/// Each schema is associated with a unique [`Fingerprint`], which is generated based
/// on the schema's canonical form and a specific hashing algorithm.
///
/// A `SchemaStore` instance is configured to use a single [`FingerprintAlgorithm`] such as Rabin,
/// MD5 (not yet supported), or SHA256 (not yet supported) for all its operations.
/// This ensures consistency when generating fingerprints and looking up schemas.
/// All schemas registered will have their fingerprint computed with this algorithm, and
/// lookups must use a matching fingerprint.
///
/// The lifetime parameter `'a` corresponds to the lifetime of the string slices
/// contained within the stored [`Schema`] objects. This means the `SchemaStore`
/// cannot outlive the data referenced by the schemas it contains.
///
/// # Examples
///
/// ```no_run
/// // Create a new store with the default Rabin fingerprinting.
/// use arrow_avro::schema::{PrimitiveType, Schema, SchemaStore, TypeName};
///
/// let mut store = SchemaStore::new();
/// let schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));
/// // Register the schema to get its fingerprint.
/// let fingerprint = store.register(schema.clone()).unwrap();
/// // Use the fingerprint to look up the schema.
/// let retrieved_schema = store.lookup(&fingerprint).cloned();
/// assert_eq!(retrieved_schema, Some(schema));
/// ```
#[derive(Debug, Clone)]
pub struct SchemaStore<'a> {
    /// The hashing algorithm used for generating fingerprints.
    fingerprint_algorithm: FingerprintAlgorithm,
    /// A map from a schema's fingerprint to the schema itself.
    schemas: HashMap<Fingerprint, Schema<'a>>,
}

impl<'a> TryFrom<&'a [Schema<'a>]> for SchemaStore<'a> {
    type Error = ArrowError;

    /// Creates a `SchemaStore` from a slice of schemas.
    /// Each schema in the slice is registered with the new store.
    fn try_from(schemas: &'a [Schema<'a>]) -> Result<Self, Self::Error> {
        let mut store = SchemaStore::new();
        for schema in schemas {
            store.register(schema.clone())?;
        }
        Ok(store)
    }
}

impl<'a> Default for SchemaStore<'a> {
    fn default() -> Self {
        Self {
            fingerprint_algorithm: FingerprintAlgorithm::Rabin,
            schemas: HashMap::new(),
        }
    }
}

impl<'a> SchemaStore<'a> {
    /// Creates an empty `SchemaStore` using the default fingerprinting algorithm (64-bit Rabin).
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a schema with the store and returns its fingerprint.
    ///
    /// A fingerprint is calculated for the given schema using the store's configured
    /// hash type. If a schema with the same fingerprint does not already exist in the
    /// store, the new schema is inserted. If the fingerprint already exists, the
    /// existing schema is not overwritten.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to register.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Fingerprint` of the schema if successful,
    /// or an `ArrowError` on failure.
    pub fn register(&mut self, schema: Schema<'a>) -> Result<Fingerprint, ArrowError> {
        let fp = generate_fingerprint(&schema, self.fingerprint_algorithm)?;
        match self.schemas.entry(fp) {
            Entry::Occupied(entry) => {
                if entry.get() != &schema {
                    return Err(ArrowError::ComputeError(format!(
                        "Schema fingerprint collision detected for fingerprint {fp:?}"
                    )));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(schema);
            }
        }
        Ok(fp)
    }

    /// Looks up a schema by its `Fingerprint`.
    ///
    /// # Arguments
    ///
    /// * `fp` - A reference to the `Fingerprint` of the schema to look up.
    ///
    /// # Returns
    ///
    /// An `Option` containing a clone of the `Schema` if found, otherwise `None`.
    pub fn lookup(&self, fp: &Fingerprint) -> Option<&Schema<'a>> {
        self.schemas.get(fp)
    }

    /// Returns the `FingerprintAlgorithm` used by the `SchemaStore` for fingerprinting.
    pub(crate) fn fingerprint_algorithm(&self) -> FingerprintAlgorithm {
        self.fingerprint_algorithm
    }
}

fn quote(s: &str) -> Result<String, ArrowError> {
    serde_json::to_string(s)
        .map_err(|e| ArrowError::ComputeError(format!("Failed to quote string: {e}")))
}

// Avro names are defined by a `name` and an optional `namespace`.
// The full name is composed of the namespace and the name, separated by a dot.
//
// Avro specification defines two ways to specify a full name:
// 1. The `name` attribute contains the full name (e.g., "a.b.c.d").
//    In this case, the `namespace` attribute is ignored.
// 2. The `name` attribute contains the simple name (e.g., "d") and the
//    `namespace` attribute contains the namespace (e.g., "a.b.c").
//
// Each part of the name must match the regex `^[A-Za-z_][A-Za-z0-9_]*$`.
// Complex paths with quotes or backticks like `a."hi".b` are not supported.
//
// This function constructs the full name and extracts the namespace,
// handling both ways of specifying the name. It prioritizes a namespace
// defined within the `name` attribute itself, then the explicit `namespace_attr`,
// and finally the `enclosing_ns`.
fn make_full_name(
    name: &str,
    namespace_attr: Option<&str>,
    enclosing_ns: Option<&str>,
) -> Result<(String, Option<String>), ArrowError> {
    // `name` already contains a dot then treat as full-name, ignore namespace.
    if let Some((ns, _)) = name.rsplit_once('.') {
        return Ok((name.to_string(), Some(ns.to_string())));
    }
    Ok(match namespace_attr.or(enclosing_ns) {
        Some(ns) => (format!("{ns}.{name}"), Some(ns.to_string())),
        None => (name.to_string(), None),
    })
}

fn build_canonical(schema: &Schema, enclosing_ns: Option<&str>) -> Result<String, ArrowError> {
    Ok(match schema {
        Schema::TypeName(tn) | Schema::Type(Type { r#type: tn, .. }) => match tn {
            TypeName::Primitive(pt) => quote(pt.as_ref())?,
            TypeName::Ref(name) => {
                let (full_name, _) = make_full_name(name, None, enclosing_ns)?;
                quote(&full_name)?
            }
        },
        Schema::Union(branches) => format!(
            "[{}]",
            branches
                .iter()
                .map(|b| build_canonical(b, enclosing_ns))
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        ),
        Schema::Complex(ct) => match ct {
            ComplexType::Record(r) => {
                let (full_name, child_ns) = make_full_name(r.name, r.namespace, enclosing_ns)?;
                let fields = r
                    .fields
                    .iter()
                    .map(|f| {
                        let field_type =
                            build_canonical(&f.r#type, child_ns.as_deref().or(enclosing_ns))?;
                        Ok(format!(
                            "{{\"name\":{},\"type\":{}}}",
                            quote(f.name)?,
                            field_type
                        ))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?
                    .join(",");
                format!(
                    r#"{{"name":{},"type":"record","fields":[{fields}]}}"#,
                    quote(&full_name)?,
                )
            }
            ComplexType::Enum(e) => {
                let (full_name, _) = make_full_name(e.name, e.namespace, enclosing_ns)?;
                let symbols = e
                    .symbols
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",");
                format!(
                    "{{\"name\":{},\"type\":\"enum\",\"symbols\":[{}]}}",
                    quote(&full_name)?,
                    symbols
                )
            }
            ComplexType::Array(arr) => format!(
                "{{\"type\":\"array\",\"items\":{}}}",
                build_canonical(&arr.items, enclosing_ns)?
            ),
            ComplexType::Map(map) => format!(
                "{{\"type\":\"map\",\"values\":{}}}",
                build_canonical(&map.values, enclosing_ns)?
            ),
            ComplexType::Fixed(f) => {
                let (full_name, _) = make_full_name(f.name, f.namespace, enclosing_ns)?;
                format!(
                    "{{\"name\":{},\"type\":\"fixed\",\"size\":{}}}",
                    quote(&full_name)?,
                    f.size
                )
            }
        },
    })
}

/// 64‑bit Rabin fingerprint as described in the Avro spec.
const EMPTY: u64 = 0xc15d_213a_a4d7_a795;

/// Build one entry of the polynomial‑division table.
///
/// We cannot yet write `for _ in 0..8` here: `for` loops rely on
/// `Iterator::next`, which is not `const` on stable Rust.  Until the
/// `const_for` feature (tracking issue #87575) is stabilized, a `while`
/// loop is the only option in a `const fn`
const fn one_entry(i: usize) -> u64 {
    let mut fp = i as u64;
    let mut j = 0;
    while j < 8 {
        fp = (fp >> 1) ^ (EMPTY & (0u64.wrapping_sub(fp & 1)));
        j += 1;
    }
    fp
}

/// Build the full 256‑entry table at compile time.
///
/// We cannot yet write `for _ in 0..256` here: `for` loops rely on
/// `Iterator::next`, which is not `const` on stable Rust.  Until the
/// `const_for` feature (tracking issue #87575) is stabilized, a `while`
/// loop is the only option in a `const fn`
const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = one_entry(i);
        i += 1;
    }
    table
}

/// The pre‑computed table.
static FINGERPRINT_TABLE: [u64; 256] = build_table();

/// Computes the 64-bit Rabin fingerprint for a given canonical schema string.
/// This implementation is based on the Avro specification for schema fingerprinting.
pub(crate) fn compute_fingerprint_rabin(canonical_form: &str) -> u64 {
    let mut fp = EMPTY;
    for &byte in canonical_form.as_bytes() {
        let idx = ((fp as u8) ^ byte) as usize;
        fp = (fp >> 8) ^ FINGERPRINT_TABLE[idx];
    }
    fp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{AvroDataType, AvroField};
    use arrow_schema::{DataType, Fields, TimeUnit};
    use serde_json::json;

    fn int_schema() -> Schema<'static> {
        Schema::TypeName(TypeName::Primitive(PrimitiveType::Int))
    }

    fn record_schema() -> Schema<'static> {
        Schema::Complex(ComplexType::Record(Record {
            name: "record1",
            namespace: Some("test.namespace"),
            doc: Some("A test record"),
            aliases: vec![],
            fields: vec![
                Field {
                    name: "field1",
                    doc: Some("An integer field"),
                    r#type: int_schema(),
                    default: None,
                },
                Field {
                    name: "field2",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: None,
                },
            ],
            attributes: Attributes::default(),
        }))
    }

    #[test]
    fn test_deserialize() {
        let t: Schema = serde_json::from_str("\"string\"").unwrap();
        assert_eq!(
            t,
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String))
        );

        let t: Schema = serde_json::from_str("[\"int\", \"null\"]").unwrap();
        assert_eq!(
            t,
            Schema::Union(vec![
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
            ])
        );

        let t: Type = serde_json::from_str(
            r#"{
                   "type":"long",
                   "logicalType":"timestamp-micros"
                }"#,
        )
        .unwrap();

        let timestamp = Type {
            r#type: TypeName::Primitive(PrimitiveType::Long),
            attributes: Attributes {
                logical_type: Some("timestamp-micros"),
                additional: Default::default(),
            },
        };

        assert_eq!(t, timestamp);

        let t: ComplexType = serde_json::from_str(
            r#"{
                   "type":"fixed",
                   "name":"fixed",
                   "namespace":"topLevelRecord.value",
                   "size":11,
                   "logicalType":"decimal",
                   "precision":25,
                   "scale":2
                }"#,
        )
        .unwrap();

        let decimal = ComplexType::Fixed(Fixed {
            name: "fixed",
            namespace: Some("topLevelRecord.value"),
            aliases: vec![],
            size: 11,
            attributes: Attributes {
                logical_type: Some("decimal"),
                additional: vec![("precision", json!(25)), ("scale", json!(2))]
                    .into_iter()
                    .collect(),
            },
        });

        assert_eq!(t, decimal);

        let schema: Schema = serde_json::from_str(
            r#"{
               "type":"record",
               "name":"topLevelRecord",
               "fields":[
                  {
                     "name":"value",
                     "type":[
                        {
                           "type":"fixed",
                           "name":"fixed",
                           "namespace":"topLevelRecord.value",
                           "size":11,
                           "logicalType":"decimal",
                           "precision":25,
                           "scale":2
                        },
                        "null"
                     ]
                  }
               ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "topLevelRecord",
                namespace: None,
                doc: None,
                aliases: vec![],
                fields: vec![Field {
                    name: "value",
                    doc: None,
                    r#type: Schema::Union(vec![
                        Schema::Complex(decimal),
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                    ]),
                    default: None,
                },],
                attributes: Default::default(),
            }))
        );

        let schema: Schema = serde_json::from_str(
            r#"{
                  "type": "record",
                  "name": "LongList",
                  "aliases": ["LinkedLongs"],
                  "fields" : [
                    {"name": "value", "type": "long"},
                    {"name": "next", "type": ["null", "LongList"]}
                  ]
                }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "LongList",
                namespace: None,
                doc: None,
                aliases: vec!["LinkedLongs"],
                fields: vec![
                    Field {
                        name: "value",
                        doc: None,
                        r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                        default: None,
                    },
                    Field {
                        name: "next",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Ref("LongList")),
                        ]),
                        default: None,
                    }
                ],
                attributes: Attributes::default(),
            }))
        );

        // Recursive schema are not supported
        let err = AvroField::try_from(&schema).unwrap_err().to_string();
        assert_eq!(err, "Parser error: Failed to resolve .LongList");

        let schema: Schema = serde_json::from_str(
            r#"{
               "type":"record",
               "name":"topLevelRecord",
               "fields":[
                  {
                     "name":"id",
                     "type":[
                        "int",
                        "null"
                     ]
                  },
                  {
                     "name":"timestamp_col",
                     "type":[
                        {
                           "type":"long",
                           "logicalType":"timestamp-micros"
                        },
                        "null"
                     ]
                  }
               ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "topLevelRecord",
                namespace: None,
                doc: None,
                aliases: vec![],
                fields: vec![
                    Field {
                        name: "id",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        ]),
                        default: None,
                    },
                    Field {
                        name: "timestamp_col",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::Type(timestamp),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        ]),
                        default: None,
                    }
                ],
                attributes: Default::default(),
            }))
        );
        let codec = AvroField::try_from(&schema).unwrap();
        assert_eq!(
            codec.field(),
            arrow_schema::Field::new(
                "topLevelRecord",
                DataType::Struct(Fields::from(vec![
                    arrow_schema::Field::new("id", DataType::Int32, true),
                    arrow_schema::Field::new(
                        "timestamp_col",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                        true
                    ),
                ])),
                false
            )
        );

        let schema: Schema = serde_json::from_str(
            r#"{
                  "type": "record",
                  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
                  "fields": [
                    {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
                  ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::Complex(ComplexType::Record(Record {
                name: "HandshakeRequest",
                namespace: Some("org.apache.avro.ipc"),
                doc: None,
                aliases: vec![],
                fields: vec![
                    Field {
                        name: "clientHash",
                        doc: None,
                        r#type: Schema::Complex(ComplexType::Fixed(Fixed {
                            name: "MD5",
                            namespace: None,
                            aliases: vec![],
                            size: 16,
                            attributes: Default::default(),
                        })),
                        default: None,
                    },
                    Field {
                        name: "clientProtocol",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                        ]),
                        default: None,
                    },
                    Field {
                        name: "serverHash",
                        doc: None,
                        r#type: Schema::TypeName(TypeName::Ref("MD5")),
                        default: None,
                    },
                    Field {
                        name: "meta",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::Complex(ComplexType::Map(Map {
                                values: Box::new(Schema::TypeName(TypeName::Primitive(
                                    PrimitiveType::Bytes
                                ))),
                                attributes: Default::default(),
                            })),
                        ]),
                        default: None,
                    }
                ],
                attributes: Default::default(),
            }))
        );
    }

    #[test]
    fn test_new_schema_store() {
        let store = SchemaStore::new();
        assert!(store.schemas.is_empty());
    }

    #[test]
    fn test_try_from_schemas_rabin() {
        let schemas = vec![int_schema(), record_schema()];
        let store = SchemaStore::try_from(schemas.as_slice()).unwrap();
        let record_fp = Fingerprint::Rabin(compute_fingerprint_rabin("\"int\""));
        assert_eq!(store.lookup(&record_fp).cloned(), Some(int_schema()));
        let canonical = generate_canonical_form(&record_schema()).unwrap();
        let rec_fp = Fingerprint::Rabin(compute_fingerprint_rabin(&canonical));
        assert_eq!(store.lookup(&rec_fp).cloned(), Some(record_schema()));
    }

    #[test]
    fn test_try_from_with_duplicates() {
        let schemas = vec![int_schema(), record_schema(), int_schema()];
        let store = SchemaStore::try_from(schemas.as_slice()).unwrap();
        assert_eq!(store.schemas.len(), 2);
        let int_canonical = r#""int""#;
        let int_fp = compute_fingerprint_rabin(int_canonical);
        assert_eq!(
            store.lookup(&Fingerprint::Rabin(int_fp)).cloned(),
            Some(int_schema())
        );
    }

    #[test]
    fn test_register_and_lookup_rabin() {
        let mut store = SchemaStore::new();
        let schema = int_schema();
        let fp_enum = store.register(schema.clone()).unwrap();
        let fp_val = match fp_enum {
            Fingerprint::Rabin(v) => v,
            _ => panic!("expected Rabin fingerprint"),
        };
        assert_eq!(
            store.lookup(&Fingerprint::Rabin(fp_val)).cloned(),
            Some(schema.clone())
        );
        assert!(store
            .lookup(&Fingerprint::Rabin(fp_val.wrapping_add(1)))
            .is_none());
    }

    #[test]
    fn test_register_duplicate_schema() {
        let mut store = SchemaStore::new();
        let schema1 = int_schema();
        let schema2 = int_schema();
        let fingerprint1 = store.register(schema1).unwrap();
        let fingerprint2 = store.register(schema2).unwrap();
        assert_eq!(fingerprint1, fingerprint2);
        assert_eq!(store.schemas.len(), 1);
    }

    #[test]
    fn test_canonical_form_generation_primitive() {
        let schema = int_schema();
        let canonical_form = generate_canonical_form(&schema).unwrap();
        assert_eq!(canonical_form, r#""int""#);
    }

    #[test]
    fn test_canonical_form_generation_record() {
        let schema = record_schema();
        let expected_canonical_form = r#"{"name":"test.namespace.record1","type":"record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"string"}]}"#;
        let canonical_form = generate_canonical_form(&schema).unwrap();
        assert_eq!(canonical_form, expected_canonical_form);
    }

    #[test]
    fn test_fingerprint_calculation() {
        let canonical_form = r#"{"fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}],"name":"test","type":"record"}"#;
        let expected_fingerprint = 10505236152925314060;
        let fingerprint = compute_fingerprint_rabin(canonical_form);
        assert_eq!(fingerprint, expected_fingerprint);
    }

    #[test]
    fn test_register_and_lookup_complex_schema() {
        let mut store = SchemaStore::new();
        let schema = record_schema();
        let canonical_form = r#"{"name":"test.namespace.record1","type":"record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"string"}]}"#;
        let expected_fingerprint =
            Fingerprint::Rabin(super::compute_fingerprint_rabin(canonical_form));
        let fingerprint = store.register(schema.clone()).unwrap();
        assert_eq!(fingerprint, expected_fingerprint);
        let looked_up = store.lookup(&fingerprint).cloned();
        assert_eq!(looked_up, Some(schema));
    }

    #[test]
    fn test_canonical_form_strips_attributes() {
        let schema_with_attrs = Schema::Complex(ComplexType::Record(Record {
            name: "record_with_attrs",
            namespace: None,
            doc: Some("This doc should be stripped"),
            aliases: vec!["alias1", "alias2"],
            fields: vec![Field {
                name: "f1",
                doc: Some("field doc"),
                r#type: Schema::Type(Type {
                    r#type: TypeName::Primitive(PrimitiveType::Bytes),
                    attributes: Attributes {
                        logical_type: Some("decimal"),
                        additional: HashMap::from([("precision", json!(4))]),
                    },
                }),
                default: None,
            }],
            attributes: Attributes {
                logical_type: None,
                additional: HashMap::from([("custom_attr", json!("value"))]),
            },
        }));
        let expected_canonical_form = r#"{"name":"record_with_attrs","type":"record","fields":[{"name":"f1","type":"bytes"}]}"#;
        let canonical_form = generate_canonical_form(&schema_with_attrs).unwrap();
        assert_eq!(canonical_form, expected_canonical_form);
    }
}
