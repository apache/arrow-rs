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

//! Avro Schema representations for Arrow.

#[cfg(feature = "canonical_extension_types")]
use arrow_schema::extension::ExtensionType;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, IntervalUnit, Schema as ArrowSchema, TimeUnit,
    UnionMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
#[cfg(feature = "sha256")]
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::cmp::PartialEq;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use strum_macros::AsRefStr;

/// The Avro single‑object encoding “magic” bytes (`0xC3 0x01`)
pub const SINGLE_OBJECT_MAGIC: [u8; 2] = [0xC3, 0x01];

/// The Confluent "magic" byte (`0x00`)
pub const CONFLUENT_MAGIC: [u8; 1] = [0x00];

/// The maximum possible length of a prefix.
/// SHA256 (32) + single-object magic (2)
pub const MAX_PREFIX_LEN: usize = 34;

/// The metadata key used for storing the JSON encoded `Schema`
pub const SCHEMA_METADATA_KEY: &str = "avro.schema";

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

/// Default name for the root record in an Avro schema.
pub const AVRO_ROOT_RECORD_DEFAULT_NAME: &str = "topLevelRecord";

/// Avro types are not nullable, with nullability instead encoded as a union
/// where one of the variants is the null type.
///
/// To accommodate this, we specially case two-variant unions where one of the
/// variants is the null type, and use this to derive arrow's notion of nullability
#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub(crate) enum Nullability {
    /// The nulls are encoded as the first union variant
    #[default]
    NullFirst,
    /// The nulls are encoded as the second union variant
    NullSecond,
}

/// Either a [`PrimitiveType`] or a reference to a previously defined named type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#names>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
/// A type name in an Avro schema
///
/// This represents the different ways a type can be referenced in an Avro schema.
pub(crate) enum TypeName<'a> {
    /// A primitive type like null, boolean, int, etc.
    Primitive(PrimitiveType),
    /// A reference to another named type
    Ref(&'a str),
}

/// A primitive type
///
/// <https://avro.apache.org/docs/1.11.1/specification/#primitive-types>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "lowercase")]
pub(crate) enum PrimitiveType {
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

/// Additional attributes within a `Schema`
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-declaration>
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Attributes<'a> {
    /// A logical type name
    ///
    /// <https://avro.apache.org/docs/1.11.1/specification/#logical-types>
    #[serde(default)]
    pub(crate) logical_type: Option<&'a str>,

    /// Additional JSON attributes
    #[serde(flatten)]
    pub(crate) additional: HashMap<&'a str, Value>,
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
pub(crate) struct Type<'a> {
    /// The type of this Avro data structure
    #[serde(borrow)]
    pub(crate) r#type: TypeName<'a>,
    /// Additional attributes associated with this type
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

/// An Avro schema
///
/// This represents the different shapes of Avro schemas as defined in the specification.
/// See <https://avro.apache.org/docs/1.11.1/specification/#schemas> for more details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Schema<'a> {
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
pub(crate) enum ComplexType<'a> {
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
pub(crate) struct Record<'a> {
    /// Name of the record
    #[serde(borrow)]
    pub(crate) name: &'a str,
    /// Optional namespace for the record, provides a way to organize names
    #[serde(borrow, default)]
    pub(crate) namespace: Option<&'a str>,
    /// Optional documentation string for the record
    #[serde(borrow, default)]
    pub(crate) doc: Option<Cow<'a, str>>,
    /// Alternative names for this record
    #[serde(borrow, default)]
    pub(crate) aliases: Vec<&'a str>,
    /// The fields contained in this record
    #[serde(borrow)]
    pub(crate) fields: Vec<Field<'a>>,
    /// Additional attributes for this record
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

fn deserialize_default<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// A field within a [`Record`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Field<'a> {
    /// Name of the field within the record
    #[serde(borrow)]
    pub(crate) name: &'a str,
    /// Optional documentation for this field
    #[serde(borrow, default)]
    pub(crate) doc: Option<Cow<'a, str>>,
    /// The field's type definition
    #[serde(borrow)]
    pub(crate) r#type: Schema<'a>,
    /// Optional default value for this field
    #[serde(deserialize_with = "deserialize_default", default)]
    pub(crate) default: Option<Value>,
    /// Alternative names (aliases) for this field (Avro spec: field-level aliases).
    /// Borrowed from input JSON where possible.
    #[serde(borrow, default)]
    pub(crate) aliases: Vec<&'a str>,
}

/// An enumeration
///
/// <https://avro.apache.org/docs/1.11.1/specification/#enums>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Enum<'a> {
    /// Name of the enum
    #[serde(borrow)]
    pub(crate) name: &'a str,
    /// Optional namespace for the enum, provides organizational structure
    #[serde(borrow, default)]
    pub(crate) namespace: Option<&'a str>,
    /// Optional documentation string describing the enum
    #[serde(borrow, default)]
    pub(crate) doc: Option<Cow<'a, str>>,
    /// Alternative names for this enum
    #[serde(borrow, default)]
    pub(crate) aliases: Vec<&'a str>,
    /// The symbols (values) that this enum can have
    #[serde(borrow)]
    pub(crate) symbols: Vec<&'a str>,
    /// Optional default value for this enum
    #[serde(borrow, default)]
    pub(crate) default: Option<&'a str>,
    /// Additional attributes for this enum
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

/// An array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#arrays>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Array<'a> {
    /// The schema for items in this array
    #[serde(borrow)]
    pub(crate) items: Box<Schema<'a>>,
    /// Additional attributes for this array
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

/// A map
///
/// <https://avro.apache.org/docs/1.11.1/specification/#maps>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Map<'a> {
    /// The schema for values in this map
    #[serde(borrow)]
    pub(crate) values: Box<Schema<'a>>,
    /// Additional attributes for this map
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

/// A fixed length binary array
///
/// <https://avro.apache.org/docs/1.11.1/specification/#fixed>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Fixed<'a> {
    /// Name of the fixed type
    #[serde(borrow)]
    pub(crate) name: &'a str,
    /// Optional namespace for the fixed type
    #[serde(borrow, default)]
    pub(crate) namespace: Option<&'a str>,
    /// Alternative names for this fixed type
    #[serde(borrow, default)]
    pub(crate) aliases: Vec<&'a str>,
    /// The number of bytes in this fixed type
    pub(crate) size: usize,
    /// Additional attributes for this fixed type
    #[serde(flatten)]
    pub(crate) attributes: Attributes<'a>,
}

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub(crate) struct AvroSchemaOptions {
    pub(crate) null_order: Option<Nullability>,
    pub(crate) strip_metadata: bool,
}

/// A wrapper for an Avro schema in its JSON string representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvroSchema {
    /// The Avro schema as a JSON string.
    pub json_string: String,
}

impl TryFrom<&ArrowSchema> for AvroSchema {
    type Error = ArrowError;

    /// Converts an `ArrowSchema` to `AvroSchema`, delegating to
    /// `AvroSchema::from_arrow_with_options` with `None` so that the
    /// union null ordering is decided by `Nullability::default()`.
    fn try_from(schema: &ArrowSchema) -> Result<Self, Self::Error> {
        AvroSchema::from_arrow_with_options(schema, None)
    }
}

impl AvroSchema {
    /// Creates a new `AvroSchema` from a JSON string.
    pub fn new(json_string: String) -> Self {
        Self { json_string }
    }

    pub(crate) fn schema(&self) -> Result<Schema<'_>, ArrowError> {
        serde_json::from_str(self.json_string.as_str())
            .map_err(|e| ArrowError::ParseError(format!("Invalid Avro schema JSON: {e}")))
    }

    /// Returns the fingerprint of the schema, computed using the specified [`FingerprintAlgorithm`].
    ///
    /// The fingerprint is computed over the schema's Parsed Canonical Form
    /// as defined by the Avro specification. Depending on `hash_type`, this
    /// will return one of the supported [`Fingerprint`] variants:
    /// - [`Fingerprint::Rabin`] for [`FingerprintAlgorithm::Rabin`]
    /// - `Fingerprint::MD5` for `FingerprintAlgorithm::MD5`
    /// - `Fingerprint::SHA256` for `FingerprintAlgorithm::SHA256`
    ///
    /// Note: [`FingerprintAlgorithm::Id`] or [`FingerprintAlgorithm::Id64`] cannot be used to generate a fingerprint
    /// and will result in an error. If you intend to use a Schema Registry ID-based
    /// wire format, either use [`SchemaStore::set`] or load the [`Fingerprint::Id`] directly via [`Fingerprint::load_fingerprint_id`] or for
    /// [`Fingerprint::Id64`] via [`Fingerprint::load_fingerprint_id64`].
    ///
    /// See also: <https://avro.apache.org/docs/1.11.1/specification/#schema-fingerprints>
    ///
    /// # Errors
    /// Returns an error if deserializing the schema fails, if generating the
    /// canonical form of the schema fails, or if `hash_type` is [`FingerprintAlgorithm::Id`].
    ///
    /// # Examples
    /// ```
    /// use arrow_avro::schema::{AvroSchema, FingerprintAlgorithm};
    ///
    /// let avro = AvroSchema::new("\"string\"".to_string());
    /// let fp = avro.fingerprint(FingerprintAlgorithm::Rabin).unwrap();
    /// ```
    pub fn fingerprint(&self, hash_type: FingerprintAlgorithm) -> Result<Fingerprint, ArrowError> {
        Self::generate_fingerprint(&self.schema()?, hash_type)
    }

    pub(crate) fn generate_fingerprint(
        schema: &Schema,
        hash_type: FingerprintAlgorithm,
    ) -> Result<Fingerprint, ArrowError> {
        let canonical = Self::generate_canonical_form(schema).map_err(|e| {
            ArrowError::ComputeError(format!("Failed to generate canonical form for schema: {e}"))
        })?;
        match hash_type {
            FingerprintAlgorithm::Rabin => {
                Ok(Fingerprint::Rabin(compute_fingerprint_rabin(&canonical)))
            }
            FingerprintAlgorithm::Id | FingerprintAlgorithm::Id64 => Err(ArrowError::SchemaError(
                "FingerprintAlgorithm of Id or Id64 cannot be used to generate a fingerprint; \
                if using Fingerprint::Id, pass the registry ID in instead using the set method."
                    .to_string(),
            )),
            #[cfg(feature = "md5")]
            FingerprintAlgorithm::MD5 => Ok(Fingerprint::MD5(compute_fingerprint_md5(&canonical))),
            #[cfg(feature = "sha256")]
            FingerprintAlgorithm::SHA256 => {
                Ok(Fingerprint::SHA256(compute_fingerprint_sha256(&canonical)))
            }
        }
    }

    /// Generates the Parsed Canonical Form for the given `Schema`.
    ///
    /// The canonical form is a standardized JSON representation of the schema,
    /// primarily used for generating a schema fingerprint for equality checking.
    ///
    /// This form strips attributes that do not affect the schema's identity,
    /// such as `doc` fields, `aliases`, and any properties not defined in the
    /// Avro specification.
    ///
    /// <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
    pub(crate) fn generate_canonical_form(schema: &Schema) -> Result<String, ArrowError> {
        build_canonical(schema, None)
    }

    /// Build Avro JSON from an Arrow [`ArrowSchema`], applying the given null‑union order and optionally stripping internal Arrow metadata.
    ///
    /// If the input Arrow schema already contains Avro JSON in
    /// [`SCHEMA_METADATA_KEY`], that JSON is returned verbatim to preserve
    /// the exact header encoding alignment; otherwise, a new JSON is generated
    /// honoring `null_union_order` at **all nullable sites**.
    pub(crate) fn from_arrow_with_options(
        schema: &ArrowSchema,
        options: Option<AvroSchemaOptions>,
    ) -> Result<AvroSchema, ArrowError> {
        let opts = options.unwrap_or_default();
        let order = opts.null_order.unwrap_or_default();
        let strip = opts.strip_metadata;
        if !strip {
            if let Some(json) = schema.metadata.get(SCHEMA_METADATA_KEY) {
                return Ok(AvroSchema::new(json.clone()));
            }
        }
        let mut name_gen = NameGenerator::default();
        let fields_json = schema
            .fields()
            .iter()
            .map(|f| arrow_field_to_avro(f, &mut name_gen, order, strip))
            .collect::<Result<Vec<_>, _>>()?;
        let record_name = schema
            .metadata
            .get(AVRO_NAME_METADATA_KEY)
            .map_or(AVRO_ROOT_RECORD_DEFAULT_NAME, |s| s.as_str());
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
        extend_with_passthrough_metadata(&mut record, &schema.metadata);
        let json_string = serde_json::to_string(&Value::Object(record))
            .map_err(|e| ArrowError::SchemaError(format!("Serializing Avro JSON failed: {e}")))?;
        Ok(AvroSchema::new(json_string))
    }
}

/// A stack-allocated, fixed-size buffer for the prefix.
#[derive(Debug, Copy, Clone)]
pub(crate) struct Prefix {
    buf: [u8; MAX_PREFIX_LEN],
    len: u8,
}

impl Prefix {
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

/// Defines the strategy for generating the per-record prefix for an Avro binary stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FingerprintStrategy {
    /// Use the 64-bit Rabin fingerprint (default for single-object encoding).
    #[default]
    Rabin,
    /// Use a Confluent Schema Registry 32-bit ID.
    Id(u32),
    /// Use an Apicurio Schema Registry 64-bit ID.
    Id64(u64),
    #[cfg(feature = "md5")]
    /// Use the 128-bit MD5 fingerprint.
    MD5,
    #[cfg(feature = "sha256")]
    /// Use the 256-bit SHA-256 fingerprint.
    SHA256,
}

impl From<Fingerprint> for FingerprintStrategy {
    fn from(f: Fingerprint) -> Self {
        Self::from(&f)
    }
}

impl From<FingerprintAlgorithm> for FingerprintStrategy {
    fn from(f: FingerprintAlgorithm) -> Self {
        match f {
            FingerprintAlgorithm::Rabin => FingerprintStrategy::Rabin,
            FingerprintAlgorithm::Id => FingerprintStrategy::Id(0),
            FingerprintAlgorithm::Id64 => FingerprintStrategy::Id64(0),
            #[cfg(feature = "md5")]
            FingerprintAlgorithm::MD5 => FingerprintStrategy::MD5,
            #[cfg(feature = "sha256")]
            FingerprintAlgorithm::SHA256 => FingerprintStrategy::SHA256,
        }
    }
}

impl From<&Fingerprint> for FingerprintStrategy {
    fn from(f: &Fingerprint) -> Self {
        match f {
            Fingerprint::Rabin(_) => FingerprintStrategy::Rabin,
            Fingerprint::Id(_) => FingerprintStrategy::Id(0),
            Fingerprint::Id64(_) => FingerprintStrategy::Id64(0),
            #[cfg(feature = "md5")]
            Fingerprint::MD5(_) => FingerprintStrategy::MD5,
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(_) => FingerprintStrategy::SHA256,
        }
    }
}

/// Supported fingerprint algorithms for Avro schema identification.
/// For use with Confluent Schema Registry IDs, set to None.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum FingerprintAlgorithm {
    /// 64‑bit CRC‑64‑AVRO Rabin fingerprint.
    #[default]
    Rabin,
    /// Represents a 32 bit fingerprint not based on a hash algorithm, (e.g., a 32-bit Schema Registry ID.)
    Id,
    /// Represents a 64 bit fingerprint not based on a hash algorithm, (e.g., a 64-bit Schema Registry ID.)
    Id64,
    #[cfg(feature = "md5")]
    /// 128-bit MD5 message digest.
    MD5,
    #[cfg(feature = "sha256")]
    /// 256-bit SHA-256 digest.
    SHA256,
}

/// Allow easy extraction of the algorithm used to create a fingerprint.
impl From<&Fingerprint> for FingerprintAlgorithm {
    fn from(fp: &Fingerprint) -> Self {
        match fp {
            Fingerprint::Rabin(_) => FingerprintAlgorithm::Rabin,
            Fingerprint::Id(_) => FingerprintAlgorithm::Id,
            Fingerprint::Id64(_) => FingerprintAlgorithm::Id64,
            #[cfg(feature = "md5")]
            Fingerprint::MD5(_) => FingerprintAlgorithm::MD5,
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(_) => FingerprintAlgorithm::SHA256,
        }
    }
}

impl From<FingerprintStrategy> for FingerprintAlgorithm {
    fn from(s: FingerprintStrategy) -> Self {
        Self::from(&s)
    }
}

impl From<&FingerprintStrategy> for FingerprintAlgorithm {
    fn from(s: &FingerprintStrategy) -> Self {
        match s {
            FingerprintStrategy::Rabin => FingerprintAlgorithm::Rabin,
            FingerprintStrategy::Id(_) => FingerprintAlgorithm::Id,
            FingerprintStrategy::Id64(_) => FingerprintAlgorithm::Id64,
            #[cfg(feature = "md5")]
            FingerprintStrategy::MD5 => FingerprintAlgorithm::MD5,
            #[cfg(feature = "sha256")]
            FingerprintStrategy::SHA256 => FingerprintAlgorithm::SHA256,
        }
    }
}

/// A schema fingerprint in one of the supported formats.
///
/// This is used as the key inside `SchemaStore` `HashMap`. Each `SchemaStore`
/// instance always stores only one variant, matching its configured
/// `FingerprintAlgorithm`, but the enum makes the API uniform.
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-fingerprints>
/// <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fingerprint {
    /// A 64-bit Rabin fingerprint.
    Rabin(u64),
    /// A 32-bit Schema Registry ID.
    Id(u32),
    /// A 64-bit Schema Registry ID.
    Id64(u64),
    #[cfg(feature = "md5")]
    /// A 128-bit MD5 fingerprint.
    MD5([u8; 16]),
    #[cfg(feature = "sha256")]
    /// A 256-bit SHA-256 fingerprint.
    SHA256([u8; 32]),
}

impl From<FingerprintStrategy> for Fingerprint {
    fn from(s: FingerprintStrategy) -> Self {
        Self::from(&s)
    }
}

impl From<&FingerprintStrategy> for Fingerprint {
    fn from(s: &FingerprintStrategy) -> Self {
        match s {
            FingerprintStrategy::Rabin => Fingerprint::Rabin(0),
            FingerprintStrategy::Id(id) => Fingerprint::Id(*id),
            FingerprintStrategy::Id64(id) => Fingerprint::Id64(*id),
            #[cfg(feature = "md5")]
            FingerprintStrategy::MD5 => Fingerprint::MD5([0; 16]),
            #[cfg(feature = "sha256")]
            FingerprintStrategy::SHA256 => Fingerprint::SHA256([0; 32]),
        }
    }
}

impl From<FingerprintAlgorithm> for Fingerprint {
    fn from(s: FingerprintAlgorithm) -> Self {
        match s {
            FingerprintAlgorithm::Rabin => Fingerprint::Rabin(0),
            FingerprintAlgorithm::Id => Fingerprint::Id(0),
            FingerprintAlgorithm::Id64 => Fingerprint::Id64(0),
            #[cfg(feature = "md5")]
            FingerprintAlgorithm::MD5 => Fingerprint::MD5([0; 16]),
            #[cfg(feature = "sha256")]
            FingerprintAlgorithm::SHA256 => Fingerprint::SHA256([0; 32]),
        }
    }
}

impl Fingerprint {
    /// Loads the 32-bit Schema Registry fingerprint (Confluent Schema Registry ID).
    ///
    /// The provided `id` is in big-endian wire order; this converts it to host order
    /// and returns `Fingerprint::Id`.
    ///
    /// # Returns
    /// A `Fingerprint::Id` variant containing the 32-bit fingerprint.
    pub fn load_fingerprint_id(id: u32) -> Self {
        Fingerprint::Id(u32::from_be(id))
    }

    /// Loads the 64-bit Schema Registry fingerprint (Apicurio Schema Registry ID).
    ///
    /// The provided `id` is in big-endian wire order; this converts it to host order
    /// and returns `Fingerprint::Id64`.
    ///
    /// # Returns
    /// A `Fingerprint::Id64` variant containing the 64-bit fingerprint.
    pub fn load_fingerprint_id64(id: u64) -> Self {
        Fingerprint::Id64(u64::from_be(id))
    }

    /// Constructs a serialized prefix represented as a `Vec<u8>` based on the variant of the enum.
    ///
    /// This method serializes data in different formats depending on the variant of `self`:
    /// - **`Id(id)`**: Uses the Confluent wire format, which includes a predefined magic header (`CONFLUENT_MAGIC`)
    ///   followed by the big-endian byte representation of the `id`.
    /// - **`Id64(id)`**: Uses the Apicurio wire format, which includes a predefined magic header (`CONFLUENT_MAGIC`)
    ///   followed by the big-endian 8-byte representation of the `id`.
    /// - **`Rabin(val)`**: Uses the Avro single-object specification format. This includes a different magic header
    ///   (`SINGLE_OBJECT_MAGIC`) followed by the little-endian byte representation of the `val`.
    /// - **`MD5(bytes)`** (optional, `md5` feature enabled): A non-standard extension that adds the
    ///   `SINGLE_OBJECT_MAGIC` header followed by the provided `bytes`.
    /// - **`SHA256(bytes)`** (optional, `sha256` feature enabled): Similar to the `MD5` variant, this is
    ///   a non-standard extension that attaches the `SINGLE_OBJECT_MAGIC` header followed by the given `bytes`.
    ///
    /// # Returns
    ///
    /// A `Prefix` containing the serialized prefix data.
    ///
    /// # Features
    ///
    /// - You can optionally enable the `md5` feature to include the `MD5` variant.
    /// - You can optionally enable the `sha256` feature to include the `SHA256` variant.
    ///
    pub(crate) fn make_prefix(&self) -> Prefix {
        let mut buf = [0u8; MAX_PREFIX_LEN];
        let len = match self {
            Self::Id(val) => write_prefix(&mut buf, &CONFLUENT_MAGIC, &val.to_be_bytes()),
            Self::Id64(val) => write_prefix(&mut buf, &CONFLUENT_MAGIC, &val.to_be_bytes()),
            Self::Rabin(val) => write_prefix(&mut buf, &SINGLE_OBJECT_MAGIC, &val.to_le_bytes()),
            #[cfg(feature = "md5")]
            Self::MD5(val) => write_prefix(&mut buf, &SINGLE_OBJECT_MAGIC, val),
            #[cfg(feature = "sha256")]
            Self::SHA256(val) => write_prefix(&mut buf, &SINGLE_OBJECT_MAGIC, val),
        };
        Prefix { buf, len }
    }
}

fn write_prefix<const MAGIC_LEN: usize, const PAYLOAD_LEN: usize>(
    buf: &mut [u8; MAX_PREFIX_LEN],
    magic: &[u8; MAGIC_LEN],
    payload: &[u8; PAYLOAD_LEN],
) -> u8 {
    debug_assert!(MAGIC_LEN + PAYLOAD_LEN <= MAX_PREFIX_LEN);
    let total = MAGIC_LEN + PAYLOAD_LEN;
    let prefix_slice = &mut buf[..total];
    prefix_slice[..MAGIC_LEN].copy_from_slice(magic);
    prefix_slice[MAGIC_LEN..total].copy_from_slice(payload);
    total as u8
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

impl TryFrom<HashMap<Fingerprint, AvroSchema>> for SchemaStore {
    type Error = ArrowError;

    /// Creates a `SchemaStore` from a HashMap of schemas.
    /// Each schema in the HashMap is registered with the new store.
    fn try_from(schemas: HashMap<Fingerprint, AvroSchema>) -> Result<Self, Self::Error> {
        Ok(Self {
            schemas,
            ..Self::default()
        })
    }
}

impl SchemaStore {
    /// Creates an empty `SchemaStore` using the default fingerprinting algorithm (64-bit Rabin).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `SchemaStore` using the default fingerprinting algorithm (64-bit Rabin).
    pub fn new_with_type(fingerprint_algorithm: FingerprintAlgorithm) -> Self {
        Self {
            fingerprint_algorithm,
            ..Self::default()
        }
    }

    /// Registers a schema with the store and the provided fingerprint.
    /// Note: Confluent wire format implementations should leverage this method.
    ///
    /// A schema is set in the store, using the provided fingerprint. If a schema
    /// with the same fingerprint does not already exist in the store, the new schema
    /// is inserted. If the fingerprint already exists, the existing schema is not overwritten.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - A reference to the `Fingerprint` of the schema to register.
    /// * `schema` - The `AvroSchema` to register.
    ///
    /// # Returns
    ///
    /// A `Result` returning the provided `Fingerprint` of the schema if successful,
    /// or an `ArrowError` on failure.
    pub fn set(
        &mut self,
        fingerprint: Fingerprint,
        schema: AvroSchema,
    ) -> Result<Fingerprint, ArrowError> {
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

    /// Registers a schema with the store and returns its fingerprint.
    ///
    /// A fingerprint is calculated for the given schema using the store's configured
    /// hash type. If a schema with the same fingerprint does not already exist in the
    /// store, the new schema is inserted. If the fingerprint already exists, the
    /// existing schema is not overwritten. If FingerprintAlgorithm is set to Id or Id64, this
    /// method will return an error. Confluent wire format implementations should leverage the
    /// set method instead.
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
        if self.fingerprint_algorithm == FingerprintAlgorithm::Id
            || self.fingerprint_algorithm == FingerprintAlgorithm::Id64
        {
            return Err(ArrowError::SchemaError(
                "Invalid FingerprintAlgorithm; unable to generate fingerprint. \
            Use the set method directly instead, providing a valid fingerprint"
                    .to_string(),
            ));
        }
        let fingerprint =
            AvroSchema::generate_fingerprint(&schema.schema()?, self.fingerprint_algorithm)?;
        self.set(fingerprint, schema)?;
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
pub(crate) fn make_full_name(
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
                        // PCF [STRIP] per Avro spec: keep only attributes relevant to parsing
                        // ("name" and "type" for fields) and **strip others** such as doc,
                        // default, order, and **aliases**. This preserves canonicalization. See:
                        // https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas
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

#[cfg(feature = "md5")]
/// Compute the **128‑bit MD5** fingerprint of the canonical form.
///
/// Returns a 16‑byte array (`[u8; 16]`) containing the full MD5 digest,
/// exactly as required by the Avro specification.
#[inline]
pub(crate) fn compute_fingerprint_md5(canonical_form: &str) -> [u8; 16] {
    let digest = md5::compute(canonical_form.as_bytes());
    digest.0
}

#[cfg(feature = "sha256")]
/// Compute the **256‑bit SHA‑256** fingerprint of the canonical form.
///
/// Returns a 32‑byte array (`[u8; 32]`) containing the full SHA‑256 digest.
#[inline]
pub(crate) fn compute_fingerprint_sha256(canonical_form: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(canonical_form.as_bytes());
    let digest = hasher.finalize();
    digest.into()
}

#[inline]
fn is_internal_arrow_key(key: &str) -> bool {
    key.starts_with("ARROW:") || key == SCHEMA_METADATA_KEY
}

/// Copies Arrow schema metadata entries to the provided JSON map,
/// skipping keys that are Avro-reserved, internal Arrow keys, or
/// nested under the `avro.schema.` namespace. Values that parse as
/// JSON are inserted as JSON; otherwise the raw string is preserved.
fn extend_with_passthrough_metadata(
    target: &mut JsonMap<String, Value>,
    metadata: &HashMap<String, String>,
) {
    for (meta_key, meta_val) in metadata {
        if meta_key.starts_with("avro.") || is_internal_arrow_key(meta_key) {
            continue;
        }
        let json_val =
            serde_json::from_str(meta_val).unwrap_or_else(|_| Value::String(meta_val.clone()));
        target.insert(meta_key.clone(), json_val);
    }
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

fn merge_extras(schema: Value, extras: JsonMap<String, Value>) -> Value {
    if extras.is_empty() {
        return schema;
    }
    match schema {
        Value::Object(mut map) => {
            map.extend(extras);
            Value::Object(map)
        }
        Value::Array(mut union) => {
            // For unions, we cannot attach attributes to the array itself (per Avro spec).
            // As a fallback for extension metadata, attach extras to the first non-null branch object.
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

#[inline]
fn is_avro_json_null(v: &Value) -> bool {
    matches!(v, Value::String(s) if s == "null")
}

fn wrap_nullable(inner: Value, null_order: Nullability) -> Value {
    let null = Value::String("null".into());
    match inner {
        Value::Array(mut union) => {
            // If this site is already a union and already contains "null",
            // preserve the branch order exactly. Reordering "null" breaks
            // the correspondence between Arrow union child order (type_ids)
            // and the Avro branch index written on the wire.
            if union.iter().any(is_avro_json_null) {
                return Value::Array(union);
            }
            // Otherwise, inject "null" without reordering existing branches.
            match null_order {
                Nullability::NullFirst => union.insert(0, null),
                Nullability::NullSecond => union.push(null),
            }
            Value::Array(union)
        }
        other => match null_order {
            Nullability::NullFirst => Value::Array(vec![null, other]),
            Nullability::NullSecond => Value::Array(vec![other, null]),
        },
    }
}

fn min_fixed_bytes_for_precision(p: usize) -> usize {
    // From the spec: max precision for n=1..=32 bytes:
    // [2,4,6,9,11,14,16,18,21,23,26,28,31,33,35,38,40,43,45,47,50,52,55,57,59,62,64,67,69,71,74,76]
    const MAX_P: [usize; 32] = [
        2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38, 40, 43, 45, 47, 50, 52, 55, 57,
        59, 62, 64, 67, 69, 71, 74, 76,
    ];
    for (i, &max_p) in MAX_P.iter().enumerate() {
        if p <= max_p {
            return i + 1;
        }
    }
    32 // saturate at Decimal256
}

fn union_branch_signature(branch: &Value) -> Result<String, ArrowError> {
    match branch {
        Value::String(t) => Ok(format!("P:{t}")),
        Value::Object(map) => {
            let t = map.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
                ArrowError::SchemaError("Union branch object missing string 'type'".into())
            })?;
            match t {
                "record" | "enum" | "fixed" => {
                    let name = map.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                        ArrowError::SchemaError(format!(
                            "Union branch '{t}' missing required 'name'"
                        ))
                    })?;
                    Ok(format!("N:{t}:{name}"))
                }
                "array" | "map" => Ok(format!("C:{t}")),
                other => Ok(format!("P:{other}")),
            }
        }
        Value::Array(_) => Err(ArrowError::SchemaError(
            "Avro union may not immediately contain another union".into(),
        )),
        _ => Err(ArrowError::SchemaError(
            "Invalid JSON for Avro union branch".into(),
        )),
    }
}

fn datatype_to_avro(
    dt: &DataType,
    field_name: &str,
    metadata: &HashMap<String, String>,
    name_gen: &mut NameGenerator,
    null_order: Nullability,
    strip: bool,
) -> Result<(Value, JsonMap<String, Value>), ArrowError> {
    let mut extras = JsonMap::new();
    let mut handle_decimal = |precision: &u8, scale: &i8| -> Result<Value, ArrowError> {
        if *scale < 0 {
            return Err(ArrowError::SchemaError(format!(
                "Invalid Avro decimal for field '{field_name}': scale ({scale}) must be >= 0"
            )));
        }
        if (*scale as usize) > (*precision as usize) {
            return Err(ArrowError::SchemaError(format!(
                "Invalid Avro decimal for field '{field_name}': scale ({scale}) \
                 must be <= precision ({precision})"
            )));
        }
        let mut meta = JsonMap::from_iter([
            ("logicalType".into(), json!("decimal")),
            ("precision".into(), json!(*precision)),
            ("scale".into(), json!(*scale)),
        ]);
        let mut fixed_size = metadata.get("size").and_then(|v| v.parse::<usize>().ok());
        let carries_name = metadata.contains_key(AVRO_NAME_METADATA_KEY)
            || metadata.contains_key(AVRO_NAMESPACE_METADATA_KEY);
        if fixed_size.is_none() && carries_name {
            fixed_size = Some(min_fixed_bytes_for_precision(*precision as usize));
        }
        if let Some(size) = fixed_size {
            meta.insert("type".into(), json!("fixed"));
            meta.insert("size".into(), json!(size));
            let chosen_name = metadata
                .get(AVRO_NAME_METADATA_KEY)
                .map(|s| sanitise_avro_name(s))
                .unwrap_or_else(|| name_gen.make_unique(field_name));
            meta.insert("name".into(), json!(chosen_name));
            if let Some(ns) = metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
                meta.insert("namespace".into(), json!(ns));
            }
        } else {
            // default to bytes-backed decimal
            meta.insert("type".into(), json!("bytes"));
        }
        Ok(Value::Object(meta))
    };
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
        DataType::BinaryView => {
            if !strip {
                extras.insert("arrowBinaryView".into(), Value::Bool(true));
            }
            Value::String("bytes".into())
        }
        DataType::FixedSizeBinary(len) => {
            let md_is_uuid = metadata
                .get("logicalType")
                .map(|s| s.trim_matches('"') == "uuid")
                .unwrap_or(false);
            #[cfg(feature = "canonical_extension_types")]
            let ext_is_uuid = metadata
                .get(arrow_schema::extension::EXTENSION_TYPE_NAME_KEY)
                .map(|v| v == arrow_schema::extension::Uuid::NAME || v == "uuid")
                .unwrap_or(false);
            #[cfg(not(feature = "canonical_extension_types"))]
            let ext_is_uuid = false;
            let is_uuid = (*len == 16) && (md_is_uuid || ext_is_uuid);
            if is_uuid {
                json!({ "type": "string", "logicalType": "uuid" })
            } else {
                let chosen_name = metadata
                    .get(AVRO_NAME_METADATA_KEY)
                    .map(|s| sanitise_avro_name(s))
                    .unwrap_or_else(|| name_gen.make_unique(field_name));
                let mut obj = JsonMap::from_iter([
                    ("type".into(), json!("fixed")),
                    ("name".into(), json!(chosen_name)),
                    ("size".into(), json!(len)),
                ]);
                if let Some(ns) = metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
                    obj.insert("namespace".into(), json!(ns));
                }
                Value::Object(obj)
            }
        }
        #[cfg(feature = "small_decimals")]
        DataType::Decimal32(precision, scale) | DataType::Decimal64(precision, scale) => {
            handle_decimal(precision, scale)?
        }
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            handle_decimal(precision, scale)?
        }
        DataType::Date32 => json!({ "type": "int", "logicalType": "date" }),
        DataType::Date64 => json!({ "type": "long", "logicalType": "local-timestamp-millis" }),
        DataType::Time32(unit) => match unit {
            TimeUnit::Millisecond => json!({ "type": "int", "logicalType": "time-millis" }),
            TimeUnit::Second => {
                if !strip {
                    extras.insert("arrowTimeUnit".into(), Value::String("second".into()));
                }
                Value::String("int".into())
            }
            _ => Value::String("int".into()),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => json!({ "type": "long", "logicalType": "time-micros" }),
            TimeUnit::Nanosecond => {
                if !strip {
                    extras.insert("arrowTimeUnit".into(), Value::String("nanosecond".into()));
                }
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
                (TimeUnit::Nanosecond, true) => "timestamp-nanos",
                (TimeUnit::Nanosecond, false) => "local-timestamp-nanos",
                (TimeUnit::Second, _) => {
                    if !strip {
                        extras.insert("arrowTimeUnit".into(), Value::String("second".into()));
                    }
                    return Ok((Value::String("long".into()), extras));
                }
            };
            if !strip && matches!(unit, TimeUnit::Nanosecond) {
                extras.insert("arrowTimeUnit".into(), Value::String("nanosecond".into()));
            }
            json!({ "type": "long", "logicalType": logical_type })
        }
        #[cfg(not(feature = "avro_custom_types"))]
        DataType::Duration(_unit) => Value::String("long".into()),
        #[cfg(feature = "avro_custom_types")]
        DataType::Duration(unit) => {
            // When the feature is enabled, create an Avro schema object
            // with the correct `logicalType` annotation.
            let logical_type = match unit {
                TimeUnit::Second => "arrow.duration-seconds",
                TimeUnit::Millisecond => "arrow.duration-millis",
                TimeUnit::Microsecond => "arrow.duration-micros",
                TimeUnit::Nanosecond => "arrow.duration-nanos",
            };
            json!({ "type": "long", "logicalType": logical_type })
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            // Avro duration logical type: fixed(12) with months/days/millis per spec.
            let chosen_name = metadata
                .get(AVRO_NAME_METADATA_KEY)
                .map(|s| sanitise_avro_name(s))
                .unwrap_or_else(|| name_gen.make_unique(field_name));
            let mut obj = JsonMap::from_iter([
                ("type".into(), json!("fixed")),
                ("name".into(), json!(chosen_name)),
                ("size".into(), json!(12)),
                ("logicalType".into(), json!("duration")),
            ]);
            if let Some(ns) = metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
                obj.insert("namespace".into(), json!(ns));
            }
            json!(obj)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            if !strip {
                extras.insert(
                    "arrowIntervalUnit".into(),
                    Value::String("yearmonth".into()),
                );
            }
            Value::String("long".into())
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            if !strip {
                extras.insert("arrowIntervalUnit".into(), Value::String("daytime".into()));
            }
            Value::String("long".into())
        }
        DataType::List(child) | DataType::LargeList(child) => {
            if matches!(dt, DataType::LargeList(_)) && !strip {
                extras.insert("arrowLargeList".into(), Value::Bool(true));
            }
            let items_schema = process_datatype(
                child.data_type(),
                child.name(),
                child.metadata(),
                name_gen,
                null_order,
                child.is_nullable(),
                strip,
            )?;
            json!({
                "type": "array",
                "items": items_schema
            })
        }
        DataType::ListView(child) | DataType::LargeListView(child) => {
            if matches!(dt, DataType::LargeListView(_)) && !strip {
                extras.insert("arrowLargeList".into(), Value::Bool(true));
            }
            if !strip {
                extras.insert("arrowListView".into(), Value::Bool(true));
            }
            let items_schema = process_datatype(
                child.data_type(),
                child.name(),
                child.metadata(),
                name_gen,
                null_order,
                child.is_nullable(),
                strip,
            )?;
            json!({
                "type": "array",
                "items": items_schema
            })
        }
        DataType::FixedSizeList(child, len) => {
            if !strip {
                extras.insert("arrowFixedSize".into(), json!(len));
            }
            let items_schema = process_datatype(
                child.data_type(),
                child.name(),
                child.metadata(),
                name_gen,
                null_order,
                child.is_nullable(),
                strip,
            )?;
            json!({
                "type": "array",
                "items": items_schema
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
            let values_schema = process_datatype(
                value_field.data_type(),
                value_field.name(),
                value_field.metadata(),
                name_gen,
                null_order,
                value_field.is_nullable(),
                strip,
            )?;
            json!({
                "type": "map",
                "values": values_schema
            })
        }
        DataType::Struct(fields) => {
            let avro_fields = fields
                .iter()
                .map(|field| arrow_field_to_avro(field, name_gen, null_order, strip))
                .collect::<Result<Vec<_>, _>>()?;
            // Prefer avro.name/avro.namespace when provided on the struct field metadata
            let chosen_name = metadata
                .get(AVRO_NAME_METADATA_KEY)
                .map(|s| sanitise_avro_name(s))
                .unwrap_or_else(|| name_gen.make_unique(field_name));
            let mut obj = JsonMap::from_iter([
                ("type".into(), json!("record")),
                ("name".into(), json!(chosen_name)),
                ("fields".into(), Value::Array(avro_fields)),
            ]);
            if let Some(ns) = metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
                obj.insert("namespace".into(), json!(ns));
            }
            Value::Object(obj)
        }
        DataType::Dictionary(_, value) => {
            if let Some(j) = metadata.get(AVRO_ENUM_SYMBOLS_METADATA_KEY) {
                let symbols: Vec<&str> =
                    serde_json::from_str(j).map_err(|e| ArrowError::ParseError(e.to_string()))?;
                // Prefer avro.name/namespace when provided for enums
                let chosen_name = metadata
                    .get(AVRO_NAME_METADATA_KEY)
                    .map(|s| sanitise_avro_name(s))
                    .unwrap_or_else(|| name_gen.make_unique(field_name));
                let mut obj = JsonMap::from_iter([
                    ("type".into(), json!("enum")),
                    ("name".into(), json!(chosen_name)),
                    ("symbols".into(), json!(symbols)),
                ]);
                if let Some(ns) = metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
                    obj.insert("namespace".into(), json!(ns));
                }
                Value::Object(obj)
            } else {
                process_datatype(
                    value.as_ref(),
                    field_name,
                    metadata,
                    name_gen,
                    null_order,
                    false,
                    strip,
                )?
            }
        }
        #[cfg(feature = "avro_custom_types")]
        DataType::RunEndEncoded(run_ends, values) => {
            let bits = match run_ends.data_type() {
                DataType::Int16 => 16,
                DataType::Int32 => 32,
                DataType::Int64 => 64,
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "RunEndEncoded requires Int16/Int32/Int64 for run_ends, found: {other:?}"
                    )));
                }
            };
            // Build the value site schema, preserving its own nullability
            let (value_schema, value_extras) = datatype_to_avro(
                values.data_type(),
                values.name(),
                values.metadata(),
                name_gen,
                null_order,
                strip,
            )?;
            let mut merged = merge_extras(value_schema, value_extras);
            if values.is_nullable() {
                merged = wrap_nullable(merged, null_order);
            }
            let mut extras = JsonMap::new();
            extras.insert("logicalType".into(), json!("arrow.run-end-encoded"));
            extras.insert("arrow.runEndIndexBits".into(), json!(bits));
            return Ok((merged, extras));
        }
        #[cfg(not(feature = "avro_custom_types"))]
        DataType::RunEndEncoded(_run_ends, values) => {
            let (value_schema, _extras) = datatype_to_avro(
                values.data_type(),
                values.name(),
                values.metadata(),
                name_gen,
                null_order,
                strip,
            )?;
            return Ok((value_schema, JsonMap::new()));
        }
        DataType::Union(fields, mode) => {
            let mut branches: Vec<Value> = Vec::with_capacity(fields.len());
            let mut type_ids: Vec<i32> = Vec::with_capacity(fields.len());
            for (type_id, field_ref) in fields.iter() {
                // NOTE: `process_datatype` would wrap nullability; force is_nullable=false here.
                let (branch_schema, _branch_extras) = datatype_to_avro(
                    field_ref.data_type(),
                    field_ref.name(),
                    field_ref.metadata(),
                    name_gen,
                    null_order,
                    strip,
                )?;
                // Avro unions cannot immediately contain another union
                if matches!(branch_schema, Value::Array(_)) {
                    return Err(ArrowError::SchemaError(
                        "Avro union may not immediately contain another union".into(),
                    ));
                }
                branches.push(branch_schema);
                type_ids.push(type_id as i32);
            }
            let mut seen: HashSet<String> = HashSet::with_capacity(branches.len());
            for b in &branches {
                let sig = union_branch_signature(b)?;
                if !seen.insert(sig) {
                    return Err(ArrowError::SchemaError(
                        "Avro union contains duplicate branch types (disallowed by spec)".into(),
                    ));
                }
            }
            if !strip {
                extras.insert(
                    "arrowUnionMode".into(),
                    Value::String(
                        match mode {
                            UnionMode::Sparse => "sparse",
                            UnionMode::Dense => "dense",
                        }
                        .to_string(),
                    ),
                );
                extras.insert(
                    "arrowUnionTypeIds".into(),
                    Value::Array(type_ids.into_iter().map(|id| json!(id)).collect()),
                );
            }
            Value::Array(branches)
        }
        #[cfg(not(feature = "small_decimals"))]
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Arrow type {other:?} has no Avro representation"
            )));
        }
    };
    Ok((val, extras))
}

fn process_datatype(
    dt: &DataType,
    field_name: &str,
    metadata: &HashMap<String, String>,
    name_gen: &mut NameGenerator,
    null_order: Nullability,
    is_nullable: bool,
    strip: bool,
) -> Result<Value, ArrowError> {
    let (schema, extras) = datatype_to_avro(dt, field_name, metadata, name_gen, null_order, strip)?;
    let mut merged = merge_extras(schema, extras);
    if is_nullable {
        merged = wrap_nullable(merged, null_order)
    }
    Ok(merged)
}

fn arrow_field_to_avro(
    field: &ArrowField,
    name_gen: &mut NameGenerator,
    null_order: Nullability,
    strip: bool,
) -> Result<Value, ArrowError> {
    let avro_name = sanitise_avro_name(field.name());
    let schema_value = process_datatype(
        field.data_type(),
        &avro_name,
        field.metadata(),
        name_gen,
        null_order,
        field.is_nullable(),
        strip,
    )?;
    // Build the field map
    let mut map = JsonMap::with_capacity(field.metadata().len() + 3);
    map.insert("name".into(), Value::String(avro_name));
    map.insert("type".into(), schema_value);
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
    use crate::codec::{AvroField, AvroFieldBuilder};
    use arrow_schema::{DataType, Fields, SchemaBuilder, TimeUnit, UnionFields};
    use serde_json::json;
    use std::sync::Arc;

    fn int_schema() -> Schema<'static> {
        Schema::TypeName(TypeName::Primitive(PrimitiveType::Int))
    }

    fn record_schema() -> Schema<'static> {
        Schema::Complex(ComplexType::Record(Record {
            name: "record1",
            namespace: Some("test.namespace"),
            doc: Some(Cow::from("A test record")),
            aliases: vec![],
            fields: vec![
                Field {
                    name: "field1",
                    doc: Some(Cow::from("An integer field")),
                    r#type: int_schema(),
                    default: None,
                    aliases: vec![],
                },
                Field {
                    name: "field2",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: None,
                    aliases: vec![],
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
                    aliases: vec![],
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
                        aliases: vec![],
                    },
                    Field {
                        name: "next",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Ref("LongList")),
                        ]),
                        default: None,
                        aliases: vec![],
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
                        aliases: vec![],
                    },
                    Field {
                        name: "timestamp_col",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::Type(timestamp),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        ]),
                        default: None,
                        aliases: vec![],
                    }
                ],
                attributes: Default::default(),
            }))
        );
        let codec = AvroField::try_from(&schema).unwrap();
        let expected_arrow_field = arrow_schema::Field::new(
            "topLevelRecord",
            DataType::Struct(Fields::from(vec![
                arrow_schema::Field::new("id", DataType::Int32, true),
                arrow_schema::Field::new(
                    "timestamp_col",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
            ])),
            false,
        )
        .with_metadata(std::collections::HashMap::from([(
            AVRO_NAME_METADATA_KEY.to_string(),
            "topLevelRecord".to_string(),
        )]));

        assert_eq!(codec.field(), expected_arrow_field);

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
                        aliases: vec![],
                    },
                    Field {
                        name: "clientProtocol",
                        doc: None,
                        r#type: Schema::Union(vec![
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                        ]),
                        default: None,
                        aliases: vec![],
                    },
                    Field {
                        name: "serverHash",
                        doc: None,
                        r#type: Schema::TypeName(TypeName::Ref("MD5")),
                        default: None,
                        aliases: vec![],
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
                        aliases: vec![],
                    }
                ],
                attributes: Default::default(),
            }))
        );
    }

    #[test]
    fn test_canonical_form_generation_comprehensive_record() {
        // NOTE: This schema is identical to the one used in test_deserialize_comprehensive.
        let json_str = r#"{
          "type": "record",
          "name": "E2eComprehensive",
          "namespace": "org.apache.arrow.avrotests.v1",
          "doc": "Comprehensive Avro writer schema to exercise arrow-avro Reader/Decoder paths.",
          "fields": [
            {"name": "id", "type": "long", "doc": "Primary row id", "aliases": ["identifier"]},
            {"name": "flag", "type": "boolean", "default": true, "doc": "A sample boolean with default true"},
            {"name": "ratio_f32", "type": "float", "default": 0.0, "doc": "Float32 example"},
            {"name": "ratio_f64", "type": "double", "default": 0.0, "doc": "Float64 example"},
            {"name": "count_i32", "type": "int", "default": 0, "doc": "Int32 example"},
            {"name": "count_i64", "type": "long", "default": 0, "doc": "Int64 example"},
            {"name": "opt_i32_nullfirst", "type": ["null", "int"], "default": null, "doc": "Nullable int (null-first)"},
            {"name": "opt_str_nullsecond", "type": ["string", "null"], "default": "", "aliases": ["old_opt_str"], "doc": "Nullable string (null-second). Default is empty string."},
            {"name": "tri_union_prim", "type": ["int", "string", "boolean"], "default": 0, "doc": "Union[int, string, boolean] with default on first branch (int=0)."},
            {"name": "str_utf8", "type": "string", "default": "default", "doc": "Plain Utf8 string (Reader may use Utf8View)."},
            {"name": "raw_bytes", "type": "bytes", "default": "", "doc": "Raw bytes field"},
            {"name": "fx16_plain", "type": {"type": "fixed", "name": "Fx16", "namespace": "org.apache.arrow.avrotests.v1.types", "aliases": ["Fixed16Old"], "size": 16}, "doc": "Plain fixed(16)"},
            {"name": "dec_bytes_s10_2", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}, "doc": "Decimal encoded on bytes, precision 10, scale 2"},
            {"name": "dec_fix_s20_4", "type": {"type": "fixed", "name": "DecFix20", "namespace": "org.apache.arrow.avrotests.v1.types", "size": 20, "logicalType": "decimal", "precision": 20, "scale": 4}, "doc": "Decimal encoded on fixed(20), precision 20, scale 4"},
            {"name": "uuid_str", "type": {"type": "string", "logicalType": "uuid"}, "doc": "UUID logical type on string"},
            {"name": "d_date", "type": {"type": "int", "logicalType": "date"}, "doc": "Date32: days since 1970-01-01"},
            {"name": "t_millis", "type": {"type": "int", "logicalType": "time-millis"}, "doc": "Time32-millis"},
            {"name": "t_micros", "type": {"type": "long", "logicalType": "time-micros"}, "doc": "Time64-micros"},
            {"name": "ts_millis_utc", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "Timestamp ms (UTC)"},
            {"name": "ts_micros_utc", "type": {"type": "long", "logicalType": "timestamp-micros"}, "doc": "Timestamp µs (UTC)"},
            {"name": "ts_millis_local", "type": {"type": "long", "logicalType": "local-timestamp-millis"}, "doc": "Local timestamp ms"},
            {"name": "ts_micros_local", "type": {"type": "long", "logicalType": "local-timestamp-micros"}, "doc": "Local timestamp µs"},
            {"name": "interval_mdn", "type": {"type": "fixed", "name": "Dur12", "namespace": "org.apache.arrow.avrotests.v1.types", "size": 12, "logicalType": "duration"}, "doc": "Duration: fixed(12) little-endian (months, days, millis)"},
            {"name": "status", "type": {"type": "enum", "name": "Status", "namespace": "org.apache.arrow.avrotests.v1.types", "symbols": ["UNKNOWN", "NEW", "PROCESSING", "DONE"], "aliases": ["State"], "doc": "Processing status enum with default"}, "default": "UNKNOWN", "doc": "Enum field using default when resolving"},
            {"name": "arr_union", "type": {"type": "array", "items": ["long", "string", "null"]}, "default": [], "doc": "Array whose items are a union[long,string,null]"},
            {"name": "map_union", "type": {"type": "map", "values": ["null", "double", "string"]}, "default": {}, "doc": "Map whose values are a union[null,double,string]"},
            {"name": "address", "type": {"type": "record", "name": "Address", "namespace": "org.apache.arrow.avrotests.v1.types", "doc": "Postal address with defaults and field alias", "fields": [
                {"name": "street", "type": "string", "default": "", "aliases": ["street_name"], "doc": "Street (field alias = street_name)"},
                {"name": "zip", "type": "int", "default": 0, "doc": "ZIP/postal code"},
                {"name": "country", "type": "string", "default": "US", "doc": "Country code"}
            ]}, "doc": "Embedded Address record"},
            {"name": "maybe_auth", "type": {"type": "record", "name": "MaybeAuth", "namespace": "org.apache.arrow.avrotests.v1.types", "doc": "Optional auth token model", "fields": [
                {"name": "user", "type": "string", "doc": "Username"},
                {"name": "token", "type": ["null", "bytes"], "default": null, "doc": "Nullable auth token"}
            ]}},
            {"name": "union_enum_record_array_map", "type": [
                {"type": "enum", "name": "Color", "namespace": "org.apache.arrow.avrotests.v1.types", "symbols": ["RED", "GREEN", "BLUE"], "doc": "Color enum"},
                {"type": "record", "name": "RecA", "namespace": "org.apache.arrow.avrotests.v1.types", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]},
                {"type": "record", "name": "RecB", "namespace": "org.apache.arrow.avrotests.v1.types", "fields": [{"name": "x", "type": "long"}, {"name": "y", "type": "bytes"}]},
                {"type": "array", "items": "long"},
                {"type": "map", "values": "string"}
            ], "doc": "Union of enum, two records, array, and map"},
            {"name": "union_date_or_fixed4", "type": [
                {"type": "int", "logicalType": "date"},
                {"type": "fixed", "name": "Fx4", "size": 4}
            ], "doc": "Union of date(int) or fixed(4)"},
            {"name": "union_interval_or_string", "type": [
                {"type": "fixed", "name": "Dur12U", "size": 12, "logicalType": "duration"},
                "string"
            ], "doc": "Union of duration(fixed12) or string"},
            {"name": "union_uuid_or_fixed10", "type": [
                {"type": "string", "logicalType": "uuid"},
                {"type": "fixed", "name": "Fx10", "size": 10}
            ], "doc": "Union of UUID string or fixed(10)"},
            {"name": "array_records_with_union", "type": {"type": "array", "items": {
                "type": "record", "name": "KV", "namespace": "org.apache.arrow.avrotests.v1.types",
                "fields": [
                    {"name": "key", "type": "string"},
                    {"name": "val", "type": ["null", "int", "long"], "default": null}
                ]
            }}, "doc": "Array<record{key, val: union[null,int,long]}>", "default": []},
            {"name": "union_map_or_array_int", "type": [
                {"type": "map", "values": "int"},
                {"type": "array", "items": "int"}
            ], "doc": "Union[map<string,int>, array<int>]"},
            {"name": "renamed_with_default", "type": "int", "default": 42, "aliases": ["old_count"], "doc": "Field with alias and default"},
            {"name": "person", "type": {"type": "record", "name": "PersonV2", "namespace": "com.example.v2", "aliases": ["com.example.Person"], "doc": "Person record with alias pointing to previous namespace/name", "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int", "default": 0}
            ]}, "doc": "Record using type alias for schema evolution tests"}
          ]
        }"#;
        let avro = AvroSchema::new(json_str.to_string());
        let parsed = avro.schema().expect("schema should deserialize");
        let expected_canonical_form = r#"{"name":"org.apache.arrow.avrotests.v1.E2eComprehensive","type":"record","fields":[{"name":"id","type":"long"},{"name":"flag","type":"boolean"},{"name":"ratio_f32","type":"float"},{"name":"ratio_f64","type":"double"},{"name":"count_i32","type":"int"},{"name":"count_i64","type":"long"},{"name":"opt_i32_nullfirst","type":["null","int"]},{"name":"opt_str_nullsecond","type":["string","null"]},{"name":"tri_union_prim","type":["int","string","boolean"]},{"name":"str_utf8","type":"string"},{"name":"raw_bytes","type":"bytes"},{"name":"fx16_plain","type":{"name":"org.apache.arrow.avrotests.v1.types.Fx16","type":"fixed","size":16}},{"name":"dec_bytes_s10_2","type":"bytes"},{"name":"dec_fix_s20_4","type":{"name":"org.apache.arrow.avrotests.v1.types.DecFix20","type":"fixed","size":20}},{"name":"uuid_str","type":"string"},{"name":"d_date","type":"int"},{"name":"t_millis","type":"int"},{"name":"t_micros","type":"long"},{"name":"ts_millis_utc","type":"long"},{"name":"ts_micros_utc","type":"long"},{"name":"ts_millis_local","type":"long"},{"name":"ts_micros_local","type":"long"},{"name":"interval_mdn","type":{"name":"org.apache.arrow.avrotests.v1.types.Dur12","type":"fixed","size":12}},{"name":"status","type":{"name":"org.apache.arrow.avrotests.v1.types.Status","type":"enum","symbols":["UNKNOWN","NEW","PROCESSING","DONE"]}},{"name":"arr_union","type":{"type":"array","items":["long","string","null"]}},{"name":"map_union","type":{"type":"map","values":["null","double","string"]}},{"name":"address","type":{"name":"org.apache.arrow.avrotests.v1.types.Address","type":"record","fields":[{"name":"street","type":"string"},{"name":"zip","type":"int"},{"name":"country","type":"string"}]}},{"name":"maybe_auth","type":{"name":"org.apache.arrow.avrotests.v1.types.MaybeAuth","type":"record","fields":[{"name":"user","type":"string"},{"name":"token","type":["null","bytes"]}]}},{"name":"union_enum_record_array_map","type":[{"name":"org.apache.arrow.avrotests.v1.types.Color","type":"enum","symbols":["RED","GREEN","BLUE"]},{"name":"org.apache.arrow.avrotests.v1.types.RecA","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]},{"name":"org.apache.arrow.avrotests.v1.types.RecB","type":"record","fields":[{"name":"x","type":"long"},{"name":"y","type":"bytes"}]},{"type":"array","items":"long"},{"type":"map","values":"string"}]},{"name":"union_date_or_fixed4","type":["int",{"name":"org.apache.arrow.avrotests.v1.Fx4","type":"fixed","size":4}]},{"name":"union_interval_or_string","type":[{"name":"org.apache.arrow.avrotests.v1.Dur12U","type":"fixed","size":12},"string"]},{"name":"union_uuid_or_fixed10","type":["string",{"name":"org.apache.arrow.avrotests.v1.Fx10","type":"fixed","size":10}]},{"name":"array_records_with_union","type":{"type":"array","items":{"name":"org.apache.arrow.avrotests.v1.types.KV","type":"record","fields":[{"name":"key","type":"string"},{"name":"val","type":["null","int","long"]}]}}},{"name":"union_map_or_array_int","type":[{"type":"map","values":"int"},{"type":"array","items":"int"}]},{"name":"renamed_with_default","type":"int"},{"name":"person","type":{"name":"com.example.v2.PersonV2","type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}}]}"#;
        let canonical_form =
            AvroSchema::generate_canonical_form(&parsed).expect("canonical form should be built");
        assert_eq!(
            canonical_form, expected_canonical_form,
            "Canonical form must match Avro spec PCF exactly"
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
        let mut schemas: HashMap<Fingerprint, AvroSchema> = HashMap::new();
        schemas.insert(
            int_avro_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .unwrap(),
            int_avro_schema.clone(),
        );
        schemas.insert(
            record_avro_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .unwrap(),
            record_avro_schema.clone(),
        );
        let store = SchemaStore::try_from(schemas).unwrap();
        let int_fp = int_avro_schema
            .fingerprint(FingerprintAlgorithm::Rabin)
            .unwrap();
        assert_eq!(store.lookup(&int_fp).cloned(), Some(int_avro_schema));
        let rec_fp = record_avro_schema
            .fingerprint(FingerprintAlgorithm::Rabin)
            .unwrap();
        assert_eq!(store.lookup(&rec_fp).cloned(), Some(record_avro_schema));
    }

    #[test]
    fn test_try_from_with_duplicates() {
        let int_avro_schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let record_avro_schema = AvroSchema::new(serde_json::to_string(&record_schema()).unwrap());
        let mut schemas: HashMap<Fingerprint, AvroSchema> = HashMap::new();
        schemas.insert(
            int_avro_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .unwrap(),
            int_avro_schema.clone(),
        );
        schemas.insert(
            record_avro_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .unwrap(),
            record_avro_schema.clone(),
        );
        // Insert duplicate of int schema
        schemas.insert(
            int_avro_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .unwrap(),
            int_avro_schema.clone(),
        );
        let store = SchemaStore::try_from(schemas).unwrap();
        assert_eq!(store.schemas.len(), 2);
        let int_fp = int_avro_schema
            .fingerprint(FingerprintAlgorithm::Rabin)
            .unwrap();
        assert_eq!(store.lookup(&int_fp).cloned(), Some(int_avro_schema));
    }

    #[test]
    fn test_register_and_lookup_rabin() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let fp_enum = store.register(schema.clone()).unwrap();
        match fp_enum {
            Fingerprint::Rabin(fp_val) => {
                assert_eq!(
                    store.lookup(&Fingerprint::Rabin(fp_val)).cloned(),
                    Some(schema.clone())
                );
                assert!(
                    store
                        .lookup(&Fingerprint::Rabin(fp_val.wrapping_add(1)))
                        .is_none()
                );
            }
            Fingerprint::Id(_id) => {
                unreachable!("This test should only generate Rabin fingerprints")
            }
            Fingerprint::Id64(_id) => {
                unreachable!("This test should only generate Rabin fingerprints")
            }
            #[cfg(feature = "md5")]
            Fingerprint::MD5(_id) => {
                unreachable!("This test should only generate Rabin fingerprints")
            }
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(_id) => {
                unreachable!("This test should only generate Rabin fingerprints")
            }
        }
    }

    #[test]
    fn test_set_and_lookup_id() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let id = 42u32;
        let fp = Fingerprint::Id(id);
        let out_fp = store.set(fp, schema.clone()).unwrap();
        assert_eq!(out_fp, fp);
        assert_eq!(store.lookup(&fp).cloned(), Some(schema.clone()));
        assert!(store.lookup(&Fingerprint::Id(id.wrapping_add(1))).is_none());
    }

    #[test]
    fn test_set_and_lookup_id64() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let id64: u64 = 0xDEAD_BEEF_DEAD_BEEF;
        let fp = Fingerprint::Id64(id64);
        let out_fp = store.set(fp, schema.clone()).unwrap();
        assert_eq!(out_fp, fp, "set should return the same Id64 fingerprint");
        assert_eq!(
            store.lookup(&fp).cloned(),
            Some(schema.clone()),
            "lookup should find the schema by Id64"
        );
        assert!(
            store
                .lookup(&Fingerprint::Id64(id64.wrapping_add(1)))
                .is_none(),
            "lookup with a different Id64 must return None"
        );
    }

    #[test]
    fn test_fingerprint_id64_conversions() {
        let algo_from_fp = FingerprintAlgorithm::from(&Fingerprint::Id64(123));
        assert_eq!(algo_from_fp, FingerprintAlgorithm::Id64);
        let fp_from_algo = Fingerprint::from(FingerprintAlgorithm::Id64);
        assert!(matches!(fp_from_algo, Fingerprint::Id64(0)));
        let strategy_from_fp = FingerprintStrategy::from(Fingerprint::Id64(5));
        assert!(matches!(strategy_from_fp, FingerprintStrategy::Id64(0)));
        let algo_from_strategy = FingerprintAlgorithm::from(strategy_from_fp);
        assert_eq!(algo_from_strategy, FingerprintAlgorithm::Id64);
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
    fn test_set_and_lookup_with_provided_fingerprint() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let fp = schema.fingerprint(FingerprintAlgorithm::Rabin).unwrap();
        let out_fp = store.set(fp, schema.clone()).unwrap();
        assert_eq!(out_fp, fp);
        assert_eq!(store.lookup(&fp).cloned(), Some(schema));
    }

    #[test]
    fn test_set_duplicate_same_schema_ok() {
        let mut store = SchemaStore::new();
        let schema = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let fp = schema.fingerprint(FingerprintAlgorithm::Rabin).unwrap();
        let _ = store.set(fp, schema.clone()).unwrap();
        let _ = store.set(fp, schema.clone()).unwrap();
        assert_eq!(store.schemas.len(), 1);
    }

    #[test]
    fn test_set_duplicate_different_schema_collision_error() {
        let mut store = SchemaStore::new();
        let schema1 = AvroSchema::new(serde_json::to_string(&int_schema()).unwrap());
        let schema2 = AvroSchema::new(serde_json::to_string(&record_schema()).unwrap());
        // Use the same Fingerprint::Id to simulate a collision across different schemas
        let fp = Fingerprint::Id(123);
        let _ = store.set(fp, schema1).unwrap();
        let err = store.set(fp, schema2).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Schema fingerprint collision"));
    }

    #[test]
    fn test_canonical_form_generation_primitive() {
        let schema = int_schema();
        let canonical_form = AvroSchema::generate_canonical_form(&schema).unwrap();
        assert_eq!(canonical_form, r#""int""#);
    }

    #[test]
    fn test_canonical_form_generation_record() {
        let schema = record_schema();
        let expected_canonical_form = r#"{"name":"test.namespace.record1","type":"record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"string"}]}"#;
        let canonical_form = AvroSchema::generate_canonical_form(&schema).unwrap();
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
        let expected_fingerprint = Fingerprint::Rabin(compute_fingerprint_rabin(canonical_form));
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
            doc: Some(Cow::from("This doc should be stripped")),
            aliases: vec!["alias1", "alias2"],
            fields: vec![Field {
                name: "f1",
                doc: Some(Cow::from("field doc")),
                r#type: Schema::Type(Type {
                    r#type: TypeName::Primitive(PrimitiveType::Bytes),
                    attributes: Attributes {
                        logical_type: None,
                        additional: HashMap::from([("precision", json!(4))]),
                    },
                }),
                default: None,
                aliases: vec![],
            }],
            attributes: Attributes {
                logical_type: None,
                additional: HashMap::from([("custom_attr", json!("value"))]),
            },
        }));
        let expected_canonical_form = r#"{"name":"record_with_attrs","type":"record","fields":[{"name":"f1","type":"bytes"}]}"#;
        let canonical_form = AvroSchema::generate_canonical_form(&schema_with_attrs).unwrap();
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

    #[cfg(feature = "avro_custom_types")]
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
        assert_json_contains(
            &avro2.json_string,
            "\"logicalType\":\"arrow.duration-nanos\"",
        );
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
    fn test_dense_union() {
        let uf: UnionFields = vec![
            (2i8, Arc::new(ArrowField::new("a", DataType::Int32, false))),
            (7i8, Arc::new(ArrowField::new("b", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect();
        let union_dt = DataType::Union(uf, UnionMode::Dense);
        let s = single_field_schema(ArrowField::new("u", union_dt, false));
        let avro =
            AvroSchema::try_from(&s).expect("Arrow Union -> Avro union conversion should succeed");
        let v: serde_json::Value = serde_json::from_str(&avro.json_string).unwrap();
        let fields = v
            .get("fields")
            .and_then(|x| x.as_array())
            .expect("fields array");
        let u_field = fields
            .iter()
            .find(|f| f.get("name").and_then(|n| n.as_str()) == Some("u"))
            .expect("field 'u'");
        let union = u_field.get("type").expect("u.type");
        let arr = union.as_array().expect("u.type must be Avro union array");
        assert_eq!(arr.len(), 2, "expected two union branches");
        let first = &arr[0];
        let obj = first
            .as_object()
            .expect("first branch should be an object with metadata");
        assert_eq!(obj.get("type").and_then(|t| t.as_str()), Some("int"));
        assert_eq!(
            obj.get("arrowUnionMode").and_then(|m| m.as_str()),
            Some("dense")
        );
        let type_ids: Vec<i64> = obj
            .get("arrowUnionTypeIds")
            .and_then(|a| a.as_array())
            .expect("arrowUnionTypeIds array")
            .iter()
            .map(|n| n.as_i64().expect("i64"))
            .collect();
        assert_eq!(type_ids, vec![2, 7], "type id ordering should be preserved");
        assert_eq!(arr[1], Value::String("string".into()));
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

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_duration_list_extras_propagated() {
        let child = ArrowField::new("lat", DataType::Duration(TimeUnit::Microsecond), false);
        let list_dt = DataType::List(Arc::new(child));
        let arrow_schema = single_field_schema(ArrowField::new("durations", list_dt, false));
        let avro = AvroSchema::try_from(&arrow_schema).unwrap();
        assert_json_contains(
            &avro.json_string,
            "\"logicalType\":\"arrow.duration-micros\"",
        );
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

    #[cfg(feature = "avro_custom_types")]
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
        assert_json_contains(
            &avro.json_string,
            "\"logicalType\":\"arrow.duration-seconds\"",
        );
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
        let mut inner_md = std::collections::HashMap::new();
        inner_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "Inner".to_string());
        let expected_inner = ArrowField::new(
            "inner",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("flag", DataType::Boolean, false),
                ArrowField::new("name", DataType::Utf8, false),
            ])),
            false,
        )
        .with_metadata(inner_md);
        let mut root_md = std::collections::HashMap::new();
        root_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "R".to_string());
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
        )
        .with_metadata(root_md);
        assert_eq!(arrow_field, expected);
    }

    #[test]
    fn default_order_is_consistent() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("s", DataType::Utf8, true)]);
        let a = AvroSchema::try_from(&arrow_schema).unwrap().json_string;
        let b = AvroSchema::from_arrow_with_options(&arrow_schema, None);
        assert_eq!(a, b.unwrap().json_string);
    }

    #[test]
    fn test_union_branch_missing_name_errors() {
        for t in ["record", "enum", "fixed"] {
            let branch = json!({ "type": t });
            let err = union_branch_signature(&branch).unwrap_err().to_string();
            assert!(
                err.contains(&format!("Union branch '{t}' missing required 'name'")),
                "expected missing-name error for {t}, got: {err}"
            );
        }
    }

    #[test]
    fn test_union_branch_named_type_signature_includes_name() {
        let rec = json!({ "type": "record", "name": "Foo" });
        assert_eq!(union_branch_signature(&rec).unwrap(), "N:record:Foo");
        let en = json!({ "type": "enum", "name": "Color", "symbols": ["R", "G", "B"] });
        assert_eq!(union_branch_signature(&en).unwrap(), "N:enum:Color");
        let fx = json!({ "type": "fixed", "name": "Bytes16", "size": 16 });
        assert_eq!(union_branch_signature(&fx).unwrap(), "N:fixed:Bytes16");
    }

    #[test]
    fn test_record_field_alias_resolution_without_default() {
        let writer_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"old","type":"int"}]
        }"#;
        let reader_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"new","aliases":["old"],"type":"int"}]
        }"#;
        let writer: Schema = serde_json::from_str(writer_json).unwrap();
        let reader: Schema = serde_json::from_str(reader_json).unwrap();
        let resolved = AvroFieldBuilder::new(&writer)
            .with_reader_schema(&reader)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build()
            .unwrap();
        let expected = ArrowField::new(
            "R",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "new",
                DataType::Int32,
                false,
            )])),
            false,
        );
        assert_eq!(resolved.field(), expected);
    }

    #[test]
    fn test_record_field_alias_ambiguous_in_strict_mode_errors() {
        let writer_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[
            {"name":"a","type":"int","aliases":["old"]},
            {"name":"b","type":"int","aliases":["old"]}
          ]
        }"#;
        let reader_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"target","type":"int","aliases":["old"]}]
        }"#;
        let writer: Schema = serde_json::from_str(writer_json).unwrap();
        let reader: Schema = serde_json::from_str(reader_json).unwrap();
        let err = AvroFieldBuilder::new(&writer)
            .with_reader_schema(&reader)
            .with_utf8view(false)
            .with_strict_mode(true)
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Ambiguous alias 'old'"),
            "expected ambiguous-alias error, got: {err}"
        );
    }

    #[test]
    fn test_pragmatic_writer_field_alias_mapping_non_strict() {
        let writer_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"before","type":"int","aliases":["now"]}]
        }"#;
        let reader_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"now","type":"int"}]
        }"#;
        let writer: Schema = serde_json::from_str(writer_json).unwrap();
        let reader: Schema = serde_json::from_str(reader_json).unwrap();
        let resolved = AvroFieldBuilder::new(&writer)
            .with_reader_schema(&reader)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build()
            .unwrap();
        let expected = ArrowField::new(
            "R",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "now",
                DataType::Int32,
                false,
            )])),
            false,
        );
        assert_eq!(resolved.field(), expected);
    }

    #[test]
    fn test_missing_reader_field_null_first_no_default_is_ok() {
        let writer_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"a","type":"int"}]
        }"#;
        let reader_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[
            {"name":"a","type":"int"},
            {"name":"b","type":["null","int"]}
          ]
        }"#;
        let writer: Schema = serde_json::from_str(writer_json).unwrap();
        let reader: Schema = serde_json::from_str(reader_json).unwrap();
        let resolved = AvroFieldBuilder::new(&writer)
            .with_reader_schema(&reader)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build()
            .unwrap();
        let expected = ArrowField::new(
            "R",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("a", DataType::Int32, false),
                ArrowField::new("b", DataType::Int32, true).with_metadata(HashMap::from([(
                    AVRO_FIELD_DEFAULT_METADATA_KEY.to_string(),
                    "null".to_string(),
                )])),
            ])),
            false,
        );
        assert_eq!(resolved.field(), expected);
    }

    #[test]
    fn test_missing_reader_field_null_second_without_default_errors() {
        let writer_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[{"name":"a","type":"int"}]
        }"#;
        let reader_json = r#"{
          "type":"record",
          "name":"R",
          "fields":[
            {"name":"a","type":"int"},
            {"name":"b","type":["int","null"]}
          ]
        }"#;
        let writer: Schema = serde_json::from_str(writer_json).unwrap();
        let reader: Schema = serde_json::from_str(reader_json).unwrap();
        let err = AvroFieldBuilder::new(&writer)
            .with_reader_schema(&reader)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must have a default value"),
            "expected missing-default error, got: {err}"
        );
    }

    #[test]
    fn test_from_arrow_with_options_respects_schema_metadata_when_not_stripping() {
        let field = ArrowField::new("x", DataType::Int32, true);
        let injected_json =
            r#"{"type":"record","name":"Injected","fields":[{"name":"ignored","type":"int"}]}"#
                .to_string();
        let mut md = HashMap::new();
        md.insert(SCHEMA_METADATA_KEY.to_string(), injected_json.clone());
        md.insert("custom".to_string(), "123".to_string());
        let arrow_schema = ArrowSchema::new_with_metadata(vec![field], md);
        let opts = AvroSchemaOptions {
            null_order: Some(Nullability::NullSecond),
            strip_metadata: false,
        };
        let out = AvroSchema::from_arrow_with_options(&arrow_schema, Some(opts)).unwrap();
        assert_eq!(
            out.json_string, injected_json,
            "When strip_metadata=false and avro.schema is present, return the embedded JSON verbatim"
        );
        let v: Value = serde_json::from_str(&out.json_string).unwrap();
        assert_eq!(v.get("type").and_then(|t| t.as_str()), Some("record"));
        assert_eq!(v.get("name").and_then(|n| n.as_str()), Some("Injected"));
    }

    #[test]
    fn test_from_arrow_with_options_ignores_schema_metadata_when_stripping_and_keeps_passthrough() {
        let field = ArrowField::new("x", DataType::Int32, true);
        let injected_json =
            r#"{"type":"record","name":"Injected","fields":[{"name":"ignored","type":"int"}]}"#
                .to_string();
        let mut md = HashMap::new();
        md.insert(SCHEMA_METADATA_KEY.to_string(), injected_json);
        md.insert("custom_meta".to_string(), "7".to_string());
        let arrow_schema = ArrowSchema::new_with_metadata(vec![field], md);
        let opts = AvroSchemaOptions {
            null_order: Some(Nullability::NullFirst),
            strip_metadata: true,
        };
        let out = AvroSchema::from_arrow_with_options(&arrow_schema, Some(opts)).unwrap();
        assert_json_contains(&out.json_string, "\"type\":\"record\"");
        assert_json_contains(&out.json_string, "\"name\":\"topLevelRecord\"");
        assert_json_contains(&out.json_string, "\"custom_meta\":7");
    }

    #[test]
    fn test_from_arrow_with_options_null_first_for_nullable_primitive() {
        let field = ArrowField::new("s", DataType::Utf8, true);
        let arrow_schema = single_field_schema(field);
        let opts = AvroSchemaOptions {
            null_order: Some(Nullability::NullFirst),
            strip_metadata: true,
        };
        let out = AvroSchema::from_arrow_with_options(&arrow_schema, Some(opts)).unwrap();
        let v: Value = serde_json::from_str(&out.json_string).unwrap();
        let arr = v["fields"][0]["type"]
            .as_array()
            .expect("nullable primitive should be Avro union array");
        assert_eq!(arr[0], Value::String("null".into()));
        assert_eq!(arr[1], Value::String("string".into()));
    }

    #[test]
    fn test_from_arrow_with_options_null_second_for_nullable_primitive() {
        let field = ArrowField::new("s", DataType::Utf8, true);
        let arrow_schema = single_field_schema(field);
        let opts = AvroSchemaOptions {
            null_order: Some(Nullability::NullSecond),
            strip_metadata: true,
        };
        let out = AvroSchema::from_arrow_with_options(&arrow_schema, Some(opts)).unwrap();
        let v: Value = serde_json::from_str(&out.json_string).unwrap();
        let arr = v["fields"][0]["type"]
            .as_array()
            .expect("nullable primitive should be Avro union array");
        assert_eq!(arr[0], Value::String("string".into()));
        assert_eq!(arr[1], Value::String("null".into()));
    }

    #[test]
    fn test_from_arrow_with_options_union_extras_respected_by_strip_metadata() {
        let uf: UnionFields = vec![
            (2i8, Arc::new(ArrowField::new("a", DataType::Int32, false))),
            (7i8, Arc::new(ArrowField::new("b", DataType::Utf8, false))),
        ]
        .into_iter()
        .collect();
        let union_dt = DataType::Union(uf, UnionMode::Dense);
        let arrow_schema = single_field_schema(ArrowField::new("u", union_dt, true));
        let with_extras = AvroSchema::from_arrow_with_options(
            &arrow_schema,
            Some(AvroSchemaOptions {
                null_order: Some(Nullability::NullFirst),
                strip_metadata: false,
            }),
        )
        .unwrap();
        let v_with: Value = serde_json::from_str(&with_extras.json_string).unwrap();
        let union_arr = v_with["fields"][0]["type"].as_array().expect("union array");
        let first_obj = union_arr
            .iter()
            .find(|b| b.is_object())
            .expect("expected an object branch with extras");
        let obj = first_obj.as_object().unwrap();
        assert_eq!(obj.get("type").and_then(|t| t.as_str()), Some("int"));
        assert_eq!(
            obj.get("arrowUnionMode").and_then(|m| m.as_str()),
            Some("dense")
        );
        let type_ids: Vec<i64> = obj["arrowUnionTypeIds"]
            .as_array()
            .expect("arrowUnionTypeIds array")
            .iter()
            .map(|n| n.as_i64().expect("i64"))
            .collect();
        assert_eq!(type_ids, vec![2, 7]);
        let stripped = AvroSchema::from_arrow_with_options(
            &arrow_schema,
            Some(AvroSchemaOptions {
                null_order: Some(Nullability::NullFirst),
                strip_metadata: true,
            }),
        )
        .unwrap();
        let v_stripped: Value = serde_json::from_str(&stripped.json_string).unwrap();
        let union_arr2 = v_stripped["fields"][0]["type"]
            .as_array()
            .expect("union array");
        assert!(
            !union_arr2.iter().any(|b| b
                .as_object()
                .is_some_and(|m| m.contains_key("arrowUnionMode"))),
            "extras must be removed when strip_metadata=true"
        );
        assert_eq!(union_arr2[0], Value::String("null".into()));
        assert_eq!(union_arr2[1], Value::String("int".into()));
        assert_eq!(union_arr2[2], Value::String("string".into()));
    }
}
