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

use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, IntervalUnit, Schema as ArrowSchema, TimeUnit,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap, Value};
use std::cmp::PartialEq;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use strum_macros::AsRefStr;

/// The metadata key used for storing the JSON encoded [`Schema`]
pub const SCHEMA_METADATA_KEY: &str = "avro.schema";

/// The Avro single‑object encoding “magic” bytes (`0xC3 0x01`)
pub const SINGLE_OBJECT_MAGIC: [u8; 2] = [0xC3, 0x01];

/// Metadata key used to represent Avro enum symbols in an Arrow schema.
pub const AVRO_ENUM_SYMBOLS_METADATA_KEY: &str = "avro.enum.symbols";

/// Metadata key used to store the default value of a field in an Avro schema.
pub const AVRO_FIELD_DEFAULT_METADATA_KEY: &str = "avro.field.default";

/// Metadata key used to store the name of a type in an Avro schema.
pub const AVRO_NAME_METADATA_KEY: &str = "avro.name";

/// Metadata key used to store the name of a type in an Avro schema.
pub const AVRO_NAMESPACE_METADATA_KEY: &str = "avro.namespace";

/// Metadata key used to store the documentation for a type in an Avro schema.
pub const AVRO_DOC_METADATA_KEY: &str = "avro.doc";

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
    #[serde(default)]
    pub default: Option<Value>,
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

/// A wrapper for an Avro schema in its JSON string representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvroSchema {
    /// The Avro schema as a JSON string.
    pub json_string: String,
}

impl TryFrom<&ArrowSchema> for AvroSchema {
    type Error = ArrowError;

    fn try_from(schema: &ArrowSchema) -> Result<Self, Self::Error> {
        // Fast‑path: schema already contains Avro JSON
        if let Some(json) = schema.metadata.get(SCHEMA_METADATA_KEY) {
            return Ok(AvroSchema::new(json.clone()));
        }
        let mut name_gen = NameGenerator::default();
        let fields_json = schema
            .fields()
            .iter()
            .map(|f| arrow_field_to_avro(f, &mut name_gen))
            .collect::<Result<Vec<_>, _>>()?;
        // Assemble top‑level record
        let record_name = schema
            .metadata
            .get(AVRO_NAME_METADATA_KEY)
            .map_or("topLevelRecord", |s| s.as_str());
        let mut record = JsonMap::with_capacity(schema.metadata.len() + 4);
        record.insert("type".into(), Value::String("record".into()));
        record.insert(
            "name".into(),
            Value::String(sanitise_avro_name(record_name)),
        );
        if let Some(ns) = schema.metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
            record.insert("namespace".into(), Value::String(ns.clone()));
        }
        if let Some(doc) = schema.metadata.get(AVRO_DOC_METADATA_KEY) {
            record.insert("doc".into(), Value::String(doc.clone()));
        }
        record.insert("fields".into(), Value::Array(fields_json));
        let schema_prefix = format!("{SCHEMA_METADATA_KEY}.");
        for (meta_key, meta_val) in &schema.metadata {
            // Skip keys already handled or internal
            if meta_key.starts_with("avro.")
                || meta_key.starts_with(schema_prefix.as_str())
                || is_internal_arrow_key(meta_key)
            {
                continue;
            }
            let json_val =
                serde_json::from_str(meta_val).unwrap_or_else(|_| Value::String(meta_val.clone()));
            record.insert(meta_key.clone(), json_val);
        }
        let json_string = serde_json::to_string(&Value::Object(record))
            .map_err(|e| ArrowError::SchemaError(format!("Serialising Avro JSON failed: {e}")))?;
        Ok(AvroSchema::new(json_string))
    }
}

impl AvroSchema {
    /// Creates a new `AvroSchema` from a JSON string.
    pub fn new(json_string: String) -> Self {
        Self { json_string }
    }

    /// Deserializes and returns the `AvroSchema`.
    ///
    /// The returned schema borrows from `self`.
    pub fn schema(&self) -> Result<Schema<'_>, ArrowError> {
        serde_json::from_str(self.json_string.as_str())
            .map_err(|e| ArrowError::ParseError(format!("Invalid Avro schema JSON: {e}")))
    }

    /// Returns the Rabin fingerprint of the schema.
    pub fn fingerprint(&self) -> Result<Fingerprint, ArrowError> {
        generate_fingerprint_rabin(&self.schema()?)
    }
}

/// Supported fingerprint algorithms for Avro schema identification.
/// Currently only `Rabin` is supported, `SHA256` and `MD5` support will come in a future update
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum FingerprintAlgorithm {
    /// 64‑bit CRC‑64‑AVRO Rabin fingerprint.
    #[default]
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
/// # Examples
///
/// ```no_run
/// // Create a new store with the default Rabin fingerprinting.
/// use arrow_avro::schema::{AvroSchema, SchemaStore};
///
/// let mut store = SchemaStore::new();
/// let schema = AvroSchema::new("\"string\"".to_string());
/// // Register the schema to get its fingerprint.
/// let fingerprint = store.register(schema.clone()).unwrap();
/// // Use the fingerprint to look up the schema.
/// let retrieved_schema = store.lookup(&fingerprint).cloned();
/// assert_eq!(retrieved_schema, Some(schema));
/// ```
#[derive(Debug, Clone, Default)]
pub struct SchemaStore {
    /// The hashing algorithm used for generating fingerprints.
    fingerprint_algorithm: FingerprintAlgorithm,
    /// A map from a schema's fingerprint to the schema itself.
    schemas: HashMap<Fingerprint, AvroSchema>,
}

impl TryFrom<&[AvroSchema]> for SchemaStore {
    type Error = ArrowError;

    /// Creates a `SchemaStore` from a slice of schemas.
    /// Each schema in the slice is registered with the new store.
    fn try_from(schemas: &[AvroSchema]) -> Result<Self, Self::Error> {
        let mut store = SchemaStore::new();
        for schema in schemas {
            store.register(schema.clone())?;
        }
        Ok(store)
    }
}

impl SchemaStore {
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
    /// * `schema` - The `AvroSchema` to register.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Fingerprint` of the schema if successful,
    /// or an `ArrowError` on failure.
    pub fn register(&mut self, schema: AvroSchema) -> Result<Fingerprint, ArrowError> {
        let fingerprint = generate_fingerprint(&schema.schema()?, self.fingerprint_algorithm)?;
        match self.schemas.entry(fingerprint) {
            Entry::Occupied(entry) => {
                if entry.get() != &schema {
                    return Err(ArrowError::ComputeError(format!(
                        "Schema fingerprint collision detected for fingerprint {fingerprint:?}"
                    )));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(schema);
            }
        }
        Ok(fingerprint)
    }

    /// Looks up a schema by its `Fingerprint`.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - A reference to the `Fingerprint` of the schema to look up.
    ///
    /// # Returns
    ///
    /// An `Option` containing a clone of the `AvroSchema` if found, otherwise `None`.
    pub fn lookup(&self, fingerprint: &Fingerprint) -> Option<&AvroSchema> {
        self.schemas.get(fingerprint)
    }

    /// Returns a `Vec` containing **all unique [`Fingerprint`]s** currently
    /// held by this [`SchemaStore`].
    ///
    /// The order of the returned fingerprints is unspecified and should not be
    /// relied upon.
    pub fn fingerprints(&self) -> Vec<Fingerprint> {
        self.schemas.keys().copied().collect()
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
) -> (String, Option<String>) {
    // `name` already contains a dot then treat as full-name, ignore namespace.
    if let Some((ns, _)) = name.rsplit_once('.') {
        return (name.to_string(), Some(ns.to_string()));
    }
    match namespace_attr.or(enclosing_ns) {
        Some(ns) => (format!("{ns}.{name}"), Some(ns.to_string())),
        None => (name.to_string(), None),
    }
}

fn build_canonical(schema: &Schema, enclosing_ns: Option<&str>) -> Result<String, ArrowError> {
    Ok(match schema {
        Schema::TypeName(tn) | Schema::Type(Type { r#type: tn, .. }) => match tn {
            TypeName::Primitive(pt) => quote(pt.as_ref())?,
            TypeName::Ref(name) => {
                let (full_name, _) = make_full_name(name, None, enclosing_ns);
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
                let (full_name, child_ns) = make_full_name(r.name, r.namespace, enclosing_ns);
                let fields = r
                    .fields
                    .iter()
                    .map(|f| {
                        let field_type =
                            build_canonical(&f.r#type, child_ns.as_deref().or(enclosing_ns))?;
                        Ok(format!(
                            r#"{{"name":{},"type":{}}}"#,
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
                let (full_name, _) = make_full_name(e.name, e.namespace, enclosing_ns);
                let symbols = e
                    .symbols
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",");
                format!(
                    r#"{{"name":{},"type":"enum","symbols":[{symbols}]}}"#,
                    quote(&full_name)?
                )
            }
            ComplexType::Array(arr) => format!(
                r#"{{"type":"array","items":{}}}"#,
                build_canonical(&arr.items, enclosing_ns)?
            ),
            ComplexType::Map(map) => format!(
                r#"{{"type":"map","values":{}}}"#,
                build_canonical(&map.values, enclosing_ns)?
            ),
            ComplexType::Fixed(f) => {
                let (full_name, _) = make_full_name(f.name, f.namespace, enclosing_ns);
                format!(
                    r#"{{"name":{},"type":"fixed","size":{}}}"#,
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

#[inline]
fn is_internal_arrow_key(key: &str) -> bool {
    key.starts_with("ARROW:") || key == SCHEMA_METADATA_KEY
}

// Sanitize an arbitrary string so it is a valid Avro field or type name
fn sanitise_avro_name(base_name: &str) -> String {
    if base_name.is_empty() {
        return "_".to_owned();
    }
    let mut out: String = base_name
        .chars()
        .map(|char| {
            if char.is_ascii_alphanumeric() || char == '_' {
                char
            } else {
                '_'
            }
        })
        .collect();
    if out.as_bytes()[0].is_ascii_digit() {
        out.insert(0, '_');
    }
    out
}

#[derive(Default)]
struct NameGenerator {
    used: HashSet<String>,
    counters: HashMap<String, usize>,
}

impl NameGenerator {
    fn make_unique(&mut self, field_name: &str) -> String {
        let field_name = sanitise_avro_name(field_name);
        if self.used.insert(field_name.clone()) {
            self.counters.insert(field_name.clone(), 1);
            return field_name;
        }
        let counter = self.counters.entry(field_name.clone()).or_insert(1);
        loop {
            let candidate = format!("{field_name}_{}", *counter);
            if self.used.insert(candidate.clone()) {
                return candidate;
            }
            *counter += 1;
        }
    }
}

fn merge_extras(schema: Value, mut extras: JsonMap<String, Value>) -> Value {
    if extras.is_empty() {
        return schema;
    }
    match schema {
        Value::Object(mut map) => {
            map.extend(extras);
            Value::Object(map)
        }
        Value::Array(mut union) => {
            if let Some(non_null) = union.iter_mut().find(|val| val.as_str() != Some("null")) {
                let original = std::mem::take(non_null);
                *non_null = merge_extras(original, extras);
            }
            Value::Array(union)
        }
        primitive => {
            let mut map = JsonMap::with_capacity(extras.len() + 1);
            map.insert("type".into(), primitive);
            map.extend(extras);
            Value::Object(map)
        }
    }
}

// Convert an Arrow `DataType` into an Avro schema `Value`.
fn datatype_to_avro(
    dt: &DataType,
    field_name: &str,
    metadata: &HashMap<String, String>,
    name_gen: &mut NameGenerator,
) -> Result<(Value, JsonMap<String, Value>), ArrowError> {
    let mut extras = JsonMap::new();
    let val = match dt {
        DataType::Null => Value::String("null".into()),
        DataType::Boolean => Value::String("boolean".into()),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 | DataType::Int32 => {
            Value::String("int".into())
        }
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => Value::String("long".into()),
        DataType::Float16 | DataType::Float32 => Value::String("float".into()),
        DataType::Float64 => Value::String("double".into()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Value::String("string".into()),
        DataType::Binary | DataType::LargeBinary => Value::String("bytes".into()),
        DataType::FixedSizeBinary(len) => {
            let is_uuid = metadata
                .get("logicalType")
                .is_some_and(|value| value == "uuid")
                || (*len == 16
                    && metadata
                        .get("ARROW:extension:name")
                        .is_some_and(|value| value == "uuid"));
            if is_uuid {
                json!({ "type": "string", "logicalType": "uuid" })
            } else {
                json!({
                    "type": "fixed",
                    "name": name_gen.make_unique(field_name),
                    "size": len
                })
            }
        }
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            // Prefer fixed if original size info present
            let mut meta = JsonMap::from_iter([
                ("logicalType".into(), json!("decimal")),
                ("precision".into(), json!(*precision)),
                ("scale".into(), json!(*scale)),
            ]);
            if let Some(size) = metadata
                .get("size")
                .and_then(|val| val.parse::<usize>().ok())
            {
                meta.insert("type".into(), json!("fixed"));
                meta.insert("size".into(), json!(size));
                meta.insert("name".into(), json!(name_gen.make_unique(field_name)));
            } else {
                meta.insert("type".into(), json!("bytes"));
            }
            Value::Object(meta)
        }
        DataType::Date32 => json!({ "type": "int", "logicalType": "date" }),
        DataType::Date64 => json!({ "type": "long", "logicalType": "local-timestamp-millis" }),
        DataType::Time32(unit) => match unit {
            TimeUnit::Millisecond => json!({ "type": "int", "logicalType": "time-millis" }),
            TimeUnit::Second => {
                extras.insert("arrowTimeUnit".into(), Value::String("second".into()));
                Value::String("int".into())
            }
            _ => Value::String("int".into()),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => json!({ "type": "long", "logicalType": "time-micros" }),
            TimeUnit::Nanosecond => {
                extras.insert("arrowTimeUnit".into(), Value::String("nanosecond".into()));
                Value::String("long".into())
            }
            _ => Value::String("long".into()),
        },
        DataType::Timestamp(unit, tz) => {
            let logical_type = match (unit, tz.is_some()) {
                (TimeUnit::Millisecond, true) => "timestamp-millis",
                (TimeUnit::Millisecond, false) => "local-timestamp-millis",
                (TimeUnit::Microsecond, true) => "timestamp-micros",
                (TimeUnit::Microsecond, false) => "local-timestamp-micros",
                (TimeUnit::Second, _) => {
                    extras.insert("arrowTimeUnit".into(), Value::String("second".into()));
                    return Ok((Value::String("long".into()), extras));
                }
                (TimeUnit::Nanosecond, _) => {
                    extras.insert("arrowTimeUnit".into(), Value::String("nanosecond".into()));
                    return Ok((Value::String("long".into()), extras));
                }
            };
            json!({ "type": "long", "logicalType": logical_type })
        }
        DataType::Duration(unit) => {
            extras.insert(
                "arrowDurationUnit".into(),
                Value::String(format!("{unit:?}").to_lowercase()),
            );
            Value::String("long".into())
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => json!({
            "type": "fixed",
            "name": name_gen.make_unique(&format!("{field_name}_duration")),
            "size": 12,
            "logicalType": "duration"
        }),
        DataType::Interval(IntervalUnit::YearMonth) => {
            extras.insert(
                "arrowIntervalUnit".into(),
                Value::String("yearmonth".into()),
            );
            Value::String("long".into())
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            extras.insert("arrowIntervalUnit".into(), Value::String("daytime".into()));
            Value::String("long".into())
        }
        DataType::List(child) | DataType::LargeList(child) => {
            if matches!(dt, DataType::LargeList(_)) {
                extras.insert("arrowLargeList".into(), Value::Bool(true));
            }
            let (items, ie) =
                datatype_to_avro(child.data_type(), child.name(), child.metadata(), name_gen)?;
            json!({
                "type": "array",
                "items": merge_extras(items, ie)
            })
        }
        DataType::FixedSizeList(child, len) => {
            extras.insert("arrowFixedSize".into(), json!(len));
            let (items, ie) =
                datatype_to_avro(child.data_type(), child.name(), child.metadata(), name_gen)?;
            json!({
                "type": "array",
                "items": merge_extras(items, ie)
            })
        }
        DataType::Map(entries, _) => {
            let value_field = match entries.data_type() {
                DataType::Struct(fs) => &fs[1],
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Map 'entries' field must be Struct(key,value)".into(),
                    ));
                }
            };
            let (val_schema, value_entry) = datatype_to_avro(
                value_field.data_type(),
                value_field.name(),
                value_field.metadata(),
                name_gen,
            )?;
            json!({
                "type": "map",
                "values": merge_extras(val_schema, value_entry)
            })
        }
        DataType::Struct(fields) => {
            let avro_fields = fields
                .iter()
                .map(|field| arrow_field_to_avro(field, name_gen))
                .collect::<Result<Vec<_>, _>>()?;
            json!({
                "type": "record",
                "name": name_gen.make_unique(field_name),
                "fields": avro_fields
            })
        }
        DataType::Dictionary(_, value) => {
            if let Some(j) = metadata.get(AVRO_ENUM_SYMBOLS_METADATA_KEY) {
                let symbols: Vec<&str> =
                    serde_json::from_str(j).map_err(|e| ArrowError::ParseError(e.to_string()))?;
                json!({
                    "type": "enum",
                    "name": name_gen.make_unique(field_name),
                    "symbols": symbols
                })
            } else {
                let (inner, ie) = datatype_to_avro(value.as_ref(), field_name, metadata, name_gen)?;
                merge_extras(inner, ie)
            }
        }
        DataType::RunEndEncoded(_, values) => {
            let (inner, ie) = datatype_to_avro(
                values.data_type(),
                values.name(),
                values.metadata(),
                name_gen,
            )?;
            merge_extras(inner, ie)
        }
        DataType::Union(_, _) => {
            return Err(ArrowError::NotYetImplemented(
                "Arrow Union to Avro Union not yet supported".into(),
            ));
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Arrow type {other:?} has no Avro representation"
            )));
        }
    };
    Ok((val, extras))
}

fn arrow_field_to_avro(
    field: &ArrowField,
    name_gen: &mut NameGenerator,
) -> Result<Value, ArrowError> {
    // Sanitize field name to ensure Avro validity but store the original in metadata
    let avro_name = sanitise_avro_name(field.name());
    let (schema, extras) =
        datatype_to_avro(field.data_type(), &avro_name, field.metadata(), name_gen)?;
    // If nullable, wrap `[ "null", <type> ]`, NOTE: second order nullability to be added in a follow-up
    let mut schema = if field.is_nullable() {
        Value::Array(vec![
            Value::String("null".into()),
            merge_extras(schema, extras),
        ])
    } else {
        merge_extras(schema, extras)
    };
    // Build the field map
    let mut map = JsonMap::with_capacity(field.metadata().len() + 3);
    map.insert("name".into(), Value::String(avro_name));
    map.insert("type".into(), schema);
    // Transfer selected metadata
    for (meta_key, meta_val) in field.metadata() {
        if is_internal_arrow_key(meta_key) {
            continue;
        }
        match meta_key.as_str() {
            AVRO_DOC_METADATA_KEY => {
                map.insert("doc".into(), Value::String(meta_val.clone()));
            }
            AVRO_FIELD_DEFAULT_METADATA_KEY => {
                let default_value = serde_json::from_str(meta_val)
                    .unwrap_or_else(|_| Value::String(meta_val.clone()));
                map.insert("default".into(), default_value);
            }
            _ => {
                let json_val = serde_json::from_str(meta_val)
                    .unwrap_or_else(|_| Value::String(meta_val.clone()));
                map.insert(meta_key.clone(), json_val);
            }
        }
    }
    Ok(Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{AvroDataType, AvroField};
    use arrow_schema::{DataType, Fields, SchemaBuilder, TimeUnit};
    use serde_json::json;
    use std::sync::Arc;

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

    fn single_field_schema(field: ArrowField) -> arrow_schema::Schema {
        let mut sb = SchemaBuilder::new();
        sb.push(field);
        sb.finish()
    }

    fn assert_json_contains(avro_json: &str, needle: &str) {
        assert!(
            avro_json.contains(needle),
            "JSON did not contain `{needle}` : {avro_json}"
        )
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
        let int_avro_schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let record_avro_schema = AvroSchema::new(serde_json::to_string(&record_schema()).unwrap());
        let schemas = vec![int_avro_schema.clone(), record_avro_schema.clone()];
        let store = SchemaStore::try_from(schemas.as_slice()).unwrap();
        let int_fp = int_avro_schema.fingerprint().unwrap();
        assert_eq!(store.lookup(&int_fp).cloned(), Some(int_avro_schema));
        let rec_fp = record_avro_schema.fingerprint().unwrap();
        assert_eq!(store.lookup(&rec_fp).cloned(), Some(record_avro_schema));
    }

    #[test]
    fn test_try_from_with_duplicates() {
        let int_avro_schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let record_avro_schema = AvroSchema::new(serde_json::to_string(&record_schema()).unwrap());
        let schemas = vec![
            int_avro_schema.clone(),
            record_avro_schema,
            int_avro_schema.clone(),
        ];
        let store = SchemaStore::try_from(schemas.as_slice()).unwrap();
        assert_eq!(store.schemas.len(), 2);
        let int_fp = int_avro_schema.fingerprint().unwrap();
        assert_eq!(store.lookup(&int_fp).cloned(), Some(int_avro_schema));
    }

    #[test]
    fn test_register_and_lookup_rabin() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let fp_enum = store.register(schema.clone()).unwrap();
        let Fingerprint::Rabin(fp_val) = fp_enum;
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
        let schema1 = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let schema2 = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
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
        let schema = AvroSchema::new(serde_json::to_string(&record_schema()).unwrap());
        let canonical_form = r#"{"name":"test.namespace.record1","type":"record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"string"}]}"#;
        let expected_fingerprint =
            Fingerprint::Rabin(super::compute_fingerprint_rabin(canonical_form));
        let fingerprint = store.register(schema.clone()).unwrap();
        assert_eq!(fingerprint, expected_fingerprint);
        let looked_up = store.lookup(&fingerprint).cloned();
        assert_eq!(looked_up, Some(schema));
    }

    #[test]
    fn test_fingerprints_returns_all_keys() {
        let mut store = SchemaStore::new();
        let fp_int = store
            .register(AvroSchema::new(
                serde_json::to_string(&int_schema()).unwrap(),
            ))
            .unwrap();
        let fp_record = store
            .register(AvroSchema::new(
                serde_json::to_string(&record_schema()).unwrap(),
            ))
            .unwrap();
        let fps = store.fingerprints();
        assert_eq!(fps.len(), 2);
        assert!(fps.contains(&fp_int));
        assert!(fps.contains(&fp_record));
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

    #[test]
    fn test_primitive_mappings() {
        let cases = vec![
            (DataType::Boolean, "\"boolean\""),
            (DataType::Int8, "\"int\""),
            (DataType::Int16, "\"int\""),
            (DataType::Int32, "\"int\""),
            (DataType::Int64, "\"long\""),
            (DataType::UInt8, "\"int\""),
            (DataType::UInt16, "\"int\""),
            (DataType::UInt32, "\"long\""),
            (DataType::UInt64, "\"long\""),
            (DataType::Float16, "\"float\""),
            (DataType::Float32, "\"float\""),
            (DataType::Float64, "\"double\""),
            (DataType::Utf8, "\"string\""),
            (DataType::Binary, "\"bytes\""),
        ];
        for (dt, avro_token) in cases {
            let field = ArrowField::new("col", dt.clone(), false);
            let arrow_schema = single_field_schema(field);
            let avro = AvroSchema::try_from(&arrow_schema).unwrap();
            assert_json_contains(&avro.json_string, avro_token);
        }
    }

    #[test]
    fn test_temporal_mappings() {
        let cases = vec![
            (DataType::Date32, "\"logicalType\":\"date\""),
            (
                DataType::Time32(TimeUnit::Millisecond),
                "\"logicalType\":\"time-millis\"",
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                "\"logicalType\":\"time-micros\"",
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                "\"logicalType\":\"local-timestamp-millis\"",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                "\"logicalType\":\"timestamp-micros\"",
            ),
        ];
        for (dt, needle) in cases {
            let field = ArrowField::new("ts", dt.clone(), true);
            let arrow_schema = single_field_schema(field);
            let avro = AvroSchema::try_from(&arrow_schema).unwrap();
            assert_json_contains(&avro.json_string, needle);
        }
    }

    #[test]
    fn test_decimal_and_uuid() {
        let decimal_field = ArrowField::new("amount", DataType::Decimal128(25, 2), false);
        let dec_schema = single_field_schema(decimal_field);
        let avro_dec = AvroSchema::try_from(&dec_schema).unwrap();
        assert_json_contains(&avro_dec.json_string, "\"logicalType\":\"decimal\"");
        assert_json_contains(&avro_dec.json_string, "\"precision\":25");
        assert_json_contains(&avro_dec.json_string, "\"scale\":2");
        let mut md = HashMap::new();
        md.insert("logicalType".into(), "uuid".into());
        let uuid_field =
            ArrowField::new("id", DataType::FixedSizeBinary(16), false).with_metadata(md);
        let uuid_schema = single_field_schema(uuid_field);
        let avro_uuid = AvroSchema::try_from(&uuid_schema).unwrap();
        assert_json_contains(&avro_uuid.json_string, "\"logicalType\":\"uuid\"");
    }

    #[test]
    fn test_interval_duration() {
        let interval_field = ArrowField::new(
            "span",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        );
        let s = single_field_schema(interval_field);
        let avro = AvroSchema::try_from(&s).unwrap();
        assert_json_contains(&avro.json_string, "\"logicalType\":\"duration\"");
        assert_json_contains(&avro.json_string, "\"size\":12");
        let dur_field = ArrowField::new("latency", DataType::Duration(TimeUnit::Nanosecond), false);
        let s2 = single_field_schema(dur_field);
        let avro2 = AvroSchema::try_from(&s2).unwrap();
        assert_json_contains(&avro2.json_string, "\"arrowDurationUnit\"");
    }

    #[test]
    fn test_complex_types() {
        let list_dt = DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true)));
        let list_schema = single_field_schema(ArrowField::new("numbers", list_dt, false));
        let avro_list = AvroSchema::try_from(&list_schema).unwrap();
        assert_json_contains(&avro_list.json_string, "\"type\":\"array\"");
        assert_json_contains(&avro_list.json_string, "\"items\"");
        let value_field = ArrowField::new("value", DataType::Boolean, true);
        let entries_struct = ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("key", DataType::Utf8, false),
                value_field.clone(),
            ])),
            false,
        );
        let map_dt = DataType::Map(Arc::new(entries_struct), false);
        let map_schema = single_field_schema(ArrowField::new("props", map_dt, false));
        let avro_map = AvroSchema::try_from(&map_schema).unwrap();
        assert_json_contains(&avro_map.json_string, "\"type\":\"map\"");
        assert_json_contains(&avro_map.json_string, "\"values\"");
        let struct_dt = DataType::Struct(Fields::from(vec![
            ArrowField::new("f1", DataType::Int64, false),
            ArrowField::new("f2", DataType::Utf8, true),
        ]));
        let struct_schema = single_field_schema(ArrowField::new("person", struct_dt, true));
        let avro_struct = AvroSchema::try_from(&struct_schema).unwrap();
        assert_json_contains(&avro_struct.json_string, "\"type\":\"record\"");
        assert_json_contains(&avro_struct.json_string, "\"null\"");
    }

    #[test]
    fn test_enum_dictionary() {
        let mut md = HashMap::new();
        md.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.into(),
            "[\"OPEN\",\"CLOSED\"]".into(),
        );
        let enum_dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let field = ArrowField::new("status", enum_dt, false).with_metadata(md);
        let schema = single_field_schema(field);
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(&avro.json_string, "\"type\":\"enum\"");
        assert_json_contains(&avro.json_string, "\"symbols\":[\"OPEN\",\"CLOSED\"]");
    }

    #[test]
    fn test_run_end_encoded() {
        let ree_dt = DataType::RunEndEncoded(
            Arc::new(ArrowField::new("run_ends", DataType::Int32, false)),
            Arc::new(ArrowField::new("values", DataType::Utf8, false)),
        );
        let s = single_field_schema(ArrowField::new("text", ree_dt, false));
        let avro = AvroSchema::try_from(&s).unwrap();
        assert_json_contains(&avro.json_string, "\"string\"");
    }

    #[test]
    fn test_dense_union_error() {
        use arrow_schema::UnionFields;
        let uf: UnionFields = vec![(0i8, Arc::new(ArrowField::new("a", DataType::Int32, false)))]
            .into_iter()
            .collect();
        let union_dt = DataType::Union(uf, arrow_schema::UnionMode::Dense);
        let s = single_field_schema(ArrowField::new("u", union_dt, false));
        let err = AvroSchema::try_from(&s).unwrap_err();
        assert!(err
            .to_string()
            .contains("Arrow Union to Avro Union not yet supported"));
    }

    #[test]
    fn round_trip_primitive() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("f1", DataType::Int32, false)]);
        let avro_schema = AvroSchema::try_from(&arrow_schema).unwrap();
        let decoded = avro_schema.schema().unwrap();
        assert!(matches!(decoded, Schema::Complex(_)));
    }

    #[test]
    fn test_name_generator_sanitization_and_uniqueness() {
        let f1 = ArrowField::new("weird-name", DataType::FixedSizeBinary(8), false);
        let f2 = ArrowField::new("weird name", DataType::FixedSizeBinary(8), false);
        let f3 = ArrowField::new("123bad", DataType::FixedSizeBinary(8), false);
        let arrow_schema = ArrowSchema::new(vec![f1, f2, f3]);
        let avro = AvroSchema::try_from(&arrow_schema).unwrap();
        assert_json_contains(&avro.json_string, "\"name\":\"weird_name\"");
        assert_json_contains(&avro.json_string, "\"name\":\"weird_name_1\"");
        assert_json_contains(&avro.json_string, "\"name\":\"_123bad\"");
    }

    #[test]
    fn test_date64_logical_type_mapping() {
        let field = ArrowField::new("d", DataType::Date64, true);
        let schema = single_field_schema(field);
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(
            &avro.json_string,
            "\"logicalType\":\"local-timestamp-millis\"",
        );
    }

    #[test]
    fn test_duration_list_extras_propagated() {
        let child = ArrowField::new("lat", DataType::Duration(TimeUnit::Microsecond), false);
        let list_dt = DataType::List(Arc::new(child));
        let arrow_schema = single_field_schema(ArrowField::new("durations", list_dt, false));
        let avro = AvroSchema::try_from(&arrow_schema).unwrap();
        assert_json_contains(&avro.json_string, "\"arrowDurationUnit\":\"microsecond\"");
    }

    #[test]
    fn test_interval_yearmonth_extra() {
        let field = ArrowField::new("iv", DataType::Interval(IntervalUnit::YearMonth), false);
        let schema = single_field_schema(field);
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(&avro.json_string, "\"arrowIntervalUnit\":\"yearmonth\"");
    }

    #[test]
    fn test_interval_daytime_extra() {
        let field = ArrowField::new("iv_dt", DataType::Interval(IntervalUnit::DayTime), false);
        let schema = single_field_schema(field);
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(&avro.json_string, "\"arrowIntervalUnit\":\"daytime\"");
    }

    #[test]
    fn test_fixed_size_list_extra() {
        let child = ArrowField::new("item", DataType::Int32, false);
        let dt = DataType::FixedSizeList(Arc::new(child), 3);
        let schema = single_field_schema(ArrowField::new("triples", dt, false));
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(&avro.json_string, "\"arrowFixedSize\":3");
    }

    #[test]
    fn test_map_duration_value_extra() {
        let val_field = ArrowField::new("value", DataType::Duration(TimeUnit::Second), true);
        let entries_struct = ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("key", DataType::Utf8, false),
                val_field,
            ])),
            false,
        );
        let map_dt = DataType::Map(Arc::new(entries_struct), false);
        let schema = single_field_schema(ArrowField::new("metrics", map_dt, false));
        let avro = AvroSchema::try_from(&schema).unwrap();
        assert_json_contains(&avro.json_string, "\"arrowDurationUnit\":\"second\"");
    }

    #[test]
    fn test_schema_with_non_string_defaults_decodes_successfully() {
        let schema_json = r#"{
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int", "default": 0},
                {"name": "b", "type": {"type": "array", "items": "long"}, "default": [1, 2, 3]},
                {"name": "c", "type": {"type": "map", "values": "double"}, "default": {"x": 1.5, "y": 2.5}},
                {"name": "inner", "type": {"type": "record", "name": "Inner", "fields": [
                    {"name": "flag", "type": "boolean", "default": true},
                    {"name": "name", "type": "string", "default": "hi"}
                ]}, "default": {"flag": false, "name": "d"}},
                {"name": "u", "type": ["int", "null"], "default": 42}
            ]
        }"#;

        let schema: Schema = serde_json::from_str(schema_json).expect("schema should parse");
        match &schema {
            Schema::Complex(ComplexType::Record(_)) => {}
            other => panic!("expected record schema, got: {:?}", other),
        }
        // Avro to Arrow conversion
        let field = crate::codec::AvroField::try_from(&schema)
            .expect("Avro->Arrow conversion should succeed");
        let arrow_field = field.field();

        // Build expected Arrow field
        let expected_list_item = ArrowField::new(
            arrow_schema::Field::LIST_FIELD_DEFAULT_NAME,
            DataType::Int64,
            false,
        );
        let expected_b = ArrowField::new("b", DataType::List(Arc::new(expected_list_item)), false);

        let expected_map_value = ArrowField::new("value", DataType::Float64, false);
        let expected_entries = ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("key", DataType::Utf8, false),
                expected_map_value,
            ])),
            false,
        );
        let expected_c =
            ArrowField::new("c", DataType::Map(Arc::new(expected_entries), false), false);

        let expected_inner = ArrowField::new(
            "inner",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("flag", DataType::Boolean, false),
                ArrowField::new("name", DataType::Utf8, false),
            ])),
            false,
        );

        let expected = ArrowField::new(
            "R",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("a", DataType::Int32, false),
                expected_b,
                expected_c,
                expected_inner,
                ArrowField::new("u", DataType::Int32, true),
            ])),
            false,
        );

        assert_eq!(arrow_field, expected);
    }
}
