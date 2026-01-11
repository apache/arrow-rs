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

//! Codec for Mapping Avro and Arrow types.

use crate::schema::{
    AVRO_ENUM_SYMBOLS_METADATA_KEY, AVRO_FIELD_DEFAULT_METADATA_KEY, AVRO_NAME_METADATA_KEY,
    AVRO_NAMESPACE_METADATA_KEY, Array, Attributes, ComplexType, Enum, Fixed, Map, Nullability,
    PrimitiveType, Record, Schema, Type, TypeName, make_full_name,
};
use arrow_schema::{
    ArrowError, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, Field, Fields,
    IntervalUnit, TimeUnit, UnionFields, UnionMode,
};
#[cfg(feature = "small_decimals")]
use arrow_schema::{DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION};
use indexmap::IndexMap;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;
use strum_macros::AsRefStr;

/// Contains information about how to resolve differences between a writer's and a reader's schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ResolutionInfo {
    /// Indicates that the writer's type should be promoted to the reader's type.
    Promotion(Promotion),
    /// Indicates that a default value should be used for a field.
    DefaultValue(AvroLiteral),
    /// Provides mapping information for resolving enums.
    EnumMapping(EnumMapping),
    /// Provides resolution information for record fields.
    Record(ResolvedRecord),
    /// Provides mapping and shape info for resolving unions.
    Union(ResolvedUnion),
}

/// Represents a literal Avro value.
///
/// This is used to represent default values in an Avro schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AvroLiteral {
    /// Represents a null value.
    Null,
    /// Represents a boolean value.
    Boolean(bool),
    /// Represents an integer value.
    Int(i32),
    /// Represents a long value.
    Long(i64),
    /// Represents a float value.
    Float(f32),
    /// Represents a double value.
    Double(f64),
    /// Represents a bytes value.
    Bytes(Vec<u8>),
    /// Represents a string value.
    String(String),
    /// Represents an enum symbol.
    Enum(String),
    /// Represents a JSON array default for an Avro array, containing element literals.
    Array(Vec<AvroLiteral>),
    /// Represents a JSON object default for an Avro map/struct, mapping string keys to value literals.
    Map(IndexMap<String, AvroLiteral>),
}

/// Contains the necessary information to resolve a writer's record against a reader's record schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedRecord {
    /// Maps a writer's field index to the corresponding reader's field index.
    /// `None` if the writer's field is not present in the reader's schema.
    pub(crate) writer_to_reader: Arc<[Option<usize>]>,
    /// A list of indices in the reader's schema for fields that have a default value.
    pub(crate) default_fields: Arc<[usize]>,
    /// For fields present in the writer's schema but not the reader's, this stores their data type.
    /// This is needed to correctly skip over these fields during deserialization.
    pub(crate) skip_fields: Arc<[Option<AvroDataType>]>,
}

/// Defines the type of promotion to be applied during schema resolution.
///
/// Schema resolution may require promoting a writer's data type to a reader's data type.
/// For example, an `int` can be promoted to a `long`, `float`, or `double`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Promotion {
    /// Direct read with no data type promotion.
    Direct,
    /// Promotes an `int` to a `long`.
    IntToLong,
    /// Promotes an `int` to a `float`.
    IntToFloat,
    /// Promotes an `int` to a `double`.
    IntToDouble,
    /// Promotes a `long` to a `float`.
    LongToFloat,
    /// Promotes a `long` to a `double`.
    LongToDouble,
    /// Promotes a `float` to a `double`.
    FloatToDouble,
    /// Promotes a `string` to `bytes`.
    StringToBytes,
    /// Promotes `bytes` to a `string`.
    BytesToString,
}

impl Display for Promotion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct => write!(formatter, "Direct"),
            Self::IntToLong => write!(formatter, "Int->Long"),
            Self::IntToFloat => write!(formatter, "Int->Float"),
            Self::IntToDouble => write!(formatter, "Int->Double"),
            Self::LongToFloat => write!(formatter, "Long->Float"),
            Self::LongToDouble => write!(formatter, "Long->Double"),
            Self::FloatToDouble => write!(formatter, "Float->Double"),
            Self::StringToBytes => write!(formatter, "String->Bytes"),
            Self::BytesToString => write!(formatter, "Bytes->String"),
        }
    }
}

/// Information required to resolve a writer union against a reader union (or single type).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedUnion {
    /// For each writer branch index, the reader branch index and how to read it.
    /// `None` means the writer branch doesn't resolve against the reader.
    pub(crate) writer_to_reader: Arc<[Option<(usize, Promotion)>]>,
    /// Whether the writer schema at this site is a union
    pub(crate) writer_is_union: bool,
    /// Whether the reader schema at this site is a union
    pub(crate) reader_is_union: bool,
}

/// Holds the mapping information for resolving Avro enums.
///
/// When resolving schemas, the writer's enum symbols must be mapped to the reader's symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumMapping {
    /// A mapping from the writer's symbol index to the reader's symbol index.
    pub(crate) mapping: Arc<[i32]>,
    /// The index to use for a writer's symbol that is not present in the reader's enum
    /// and a default value is specified in the reader's schema.
    pub(crate) default_index: i32,
}

#[cfg(feature = "canonical_extension_types")]
fn with_extension_type(codec: &Codec, field: Field) -> Field {
    match codec {
        Codec::Uuid => field.with_extension_type(arrow_schema::extension::Uuid),
        _ => field,
    }
}

/// An Avro datatype mapped to the arrow data model
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AvroDataType {
    nullability: Option<Nullability>,
    metadata: HashMap<String, String>,
    codec: Codec,
    pub(crate) resolution: Option<ResolutionInfo>,
}

impl AvroDataType {
    /// Create a new [`AvroDataType`] with the given parts.
    pub(crate) fn new(
        codec: Codec,
        metadata: HashMap<String, String>,
        nullability: Option<Nullability>,
    ) -> Self {
        AvroDataType {
            codec,
            metadata,
            nullability,
            resolution: None,
        }
    }

    #[inline]
    fn new_with_resolution(
        codec: Codec,
        metadata: HashMap<String, String>,
        nullability: Option<Nullability>,
        resolution: Option<ResolutionInfo>,
    ) -> Self {
        Self {
            codec,
            metadata,
            nullability,
            resolution,
        }
    }

    /// Returns an arrow [`Field`] with the given name
    pub(crate) fn field_with_name(&self, name: &str) -> Field {
        let mut nullable = self.nullability.is_some();
        if !nullable {
            if let Codec::Union(children, _, _) = self.codec() {
                // If any encoded branch is `null`, mark field as nullable
                if children.iter().any(|c| matches!(c.codec(), Codec::Null)) {
                    nullable = true;
                }
            }
        }
        let data_type = self.codec.data_type();
        let field = Field::new(name, data_type, nullable).with_metadata(self.metadata.clone());
        #[cfg(feature = "canonical_extension_types")]
        return with_extension_type(&self.codec, field);
        #[cfg(not(feature = "canonical_extension_types"))]
        field
    }

    /// Returns a reference to the codec used by this data type
    ///
    /// The codec determines how Avro data is encoded and mapped to Arrow data types.
    /// This is useful when we need to inspect or use the specific encoding of a field.
    pub(crate) fn codec(&self) -> &Codec {
        &self.codec
    }

    /// Returns the nullability status of this data type
    ///
    /// In Avro, nullability is represented through unions with null types.
    /// The returned value indicates how nulls are encoded in the Avro format:
    /// - `Some(Nullability::NullFirst)` - Nulls are encoded as the first union variant
    /// - `Some(Nullability::NullSecond)` - Nulls are encoded as the second union variant
    /// - `None` - The type is not nullable
    pub(crate) fn nullability(&self) -> Option<Nullability> {
        self.nullability
    }

    #[inline]
    fn parse_default_literal(&self, default_json: &Value) -> Result<AvroLiteral, ArrowError> {
        fn expect_string<'v>(
            default_json: &'v Value,
            data_type: &str,
        ) -> Result<&'v str, ArrowError> {
            match default_json {
                Value::String(s) => Ok(s.as_str()),
                _ => Err(ArrowError::SchemaError(format!(
                    "Default value must be a JSON string for {data_type}"
                ))),
            }
        }

        fn parse_bytes_default(
            default_json: &Value,
            expected_len: Option<usize>,
        ) -> Result<Vec<u8>, ArrowError> {
            let s = expect_string(default_json, "bytes/fixed logical types")?;
            let mut out = Vec::with_capacity(s.len());
            for ch in s.chars() {
                let cp = ch as u32;
                if cp > 0xFF {
                    return Err(ArrowError::SchemaError(format!(
                        "Invalid codepoint U+{cp:04X} in bytes/fixed default; must be ≤ 0xFF"
                    )));
                }
                out.push(cp as u8);
            }
            if let Some(len) = expected_len {
                if out.len() != len {
                    return Err(ArrowError::SchemaError(format!(
                        "Default length {} does not match expected fixed size {len}",
                        out.len(),
                    )));
                }
            }
            Ok(out)
        }

        fn parse_json_i64(default_json: &Value, data_type: &str) -> Result<i64, ArrowError> {
            match default_json {
                Value::Number(n) => n.as_i64().ok_or_else(|| {
                    ArrowError::SchemaError(format!("Default {data_type} must be an integer"))
                }),
                _ => Err(ArrowError::SchemaError(format!(
                    "Default {data_type} must be a JSON integer"
                ))),
            }
        }

        fn parse_json_f64(default_json: &Value, data_type: &str) -> Result<f64, ArrowError> {
            match default_json {
                Value::Number(n) => n.as_f64().ok_or_else(|| {
                    ArrowError::SchemaError(format!("Default {data_type} must be a number"))
                }),
                _ => Err(ArrowError::SchemaError(format!(
                    "Default {data_type} must be a JSON number"
                ))),
            }
        }

        // Handle JSON nulls per-spec: allowed only for `null` type or unions with null FIRST
        if default_json.is_null() {
            return match self.codec() {
                Codec::Null => Ok(AvroLiteral::Null),
                Codec::Union(encodings, _, _) if !encodings.is_empty()
                    && matches!(encodings[0].codec(), Codec::Null) =>
                    {
                        Ok(AvroLiteral::Null)
                    }
                _ if self.nullability() == Some(Nullability::NullFirst) => Ok(AvroLiteral::Null),
                _ => Err(ArrowError::SchemaError(
                    "JSON null default is only valid for `null` type or for a union whose first branch is `null`"
                        .to_string(),
                )),
            };
        }
        let lit = match self.codec() {
            Codec::Null => {
                return Err(ArrowError::SchemaError(
                    "Default for `null` type must be JSON null".to_string(),
                ));
            }
            Codec::Boolean => match default_json {
                Value::Bool(b) => AvroLiteral::Boolean(*b),
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Boolean default must be a JSON boolean".to_string(),
                    ));
                }
            },
            Codec::Int32 | Codec::Date32 | Codec::TimeMillis => {
                let i = parse_json_i64(default_json, "int")?;
                if i < i32::MIN as i64 || i > i32::MAX as i64 {
                    return Err(ArrowError::SchemaError(format!(
                        "Default int {i} out of i32 range"
                    )));
                }
                AvroLiteral::Int(i as i32)
            }
            Codec::Int64
            | Codec::TimeMicros
            | Codec::TimestampMillis(_)
            | Codec::TimestampMicros(_)
            | Codec::TimestampNanos(_) => AvroLiteral::Long(parse_json_i64(default_json, "long")?),
            #[cfg(feature = "avro_custom_types")]
            Codec::DurationNanos
            | Codec::DurationMicros
            | Codec::DurationMillis
            | Codec::DurationSeconds => AvroLiteral::Long(parse_json_i64(default_json, "long")?),
            Codec::Float32 => {
                let f = parse_json_f64(default_json, "float")?;
                if !f.is_finite() || f < f32::MIN as f64 || f > f32::MAX as f64 {
                    return Err(ArrowError::SchemaError(format!(
                        "Default float {f} out of f32 range or not finite"
                    )));
                }
                AvroLiteral::Float(f as f32)
            }
            Codec::Float64 => AvroLiteral::Double(parse_json_f64(default_json, "double")?),
            Codec::Utf8 | Codec::Utf8View | Codec::Uuid => {
                AvroLiteral::String(expect_string(default_json, "string/uuid")?.to_string())
            }
            Codec::Binary => AvroLiteral::Bytes(parse_bytes_default(default_json, None)?),
            Codec::Fixed(sz) => {
                AvroLiteral::Bytes(parse_bytes_default(default_json, Some(*sz as usize))?)
            }
            Codec::Decimal(_, _, fixed_size) => {
                AvroLiteral::Bytes(parse_bytes_default(default_json, *fixed_size)?)
            }
            Codec::Enum(symbols) => {
                let s = expect_string(default_json, "enum")?;
                if symbols.iter().any(|sym| sym == s) {
                    AvroLiteral::Enum(s.to_string())
                } else {
                    return Err(ArrowError::SchemaError(format!(
                        "Default enum symbol {s:?} not found in reader enum symbols"
                    )));
                }
            }
            Codec::Interval => AvroLiteral::Bytes(parse_bytes_default(default_json, Some(12))?),
            Codec::List(item_dt) => match default_json {
                Value::Array(items) => AvroLiteral::Array(
                    items
                        .iter()
                        .map(|v| item_dt.parse_default_literal(v))
                        .collect::<Result<_, _>>()?,
                ),
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Default value must be a JSON array for Avro array type".to_string(),
                    ));
                }
            },
            Codec::Map(val_dt) => match default_json {
                Value::Object(map) => {
                    let mut out = IndexMap::with_capacity(map.len());
                    for (k, v) in map {
                        out.insert(k.clone(), val_dt.parse_default_literal(v)?);
                    }
                    AvroLiteral::Map(out)
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Default value must be a JSON object for Avro map type".to_string(),
                    ));
                }
            },
            Codec::Struct(fields) => match default_json {
                Value::Object(obj) => {
                    let mut out: IndexMap<String, AvroLiteral> =
                        IndexMap::with_capacity(fields.len());
                    for f in fields.as_ref() {
                        let name = f.name().to_string();
                        if let Some(sub) = obj.get(&name) {
                            out.insert(name, f.data_type().parse_default_literal(sub)?);
                        } else {
                            // Cache metadata lookup once
                            let stored_default =
                                f.data_type().metadata.get(AVRO_FIELD_DEFAULT_METADATA_KEY);
                            if stored_default.is_none()
                                && f.data_type().nullability() == Some(Nullability::default())
                            {
                                out.insert(name, AvroLiteral::Null);
                            } else if let Some(default_json) = stored_default {
                                let v: Value =
                                    serde_json::from_str(default_json).map_err(|e| {
                                        ArrowError::SchemaError(format!(
                                            "Failed to parse stored subfield default JSON for '{}': {e}",
                                            f.name(),
                                        ))
                                    })?;
                                out.insert(name, f.data_type().parse_default_literal(&v)?);
                            } else {
                                return Err(ArrowError::SchemaError(format!(
                                    "Record default missing required subfield '{}' with non-nullable type {:?}",
                                    f.name(),
                                    f.data_type().codec()
                                )));
                            }
                        }
                    }
                    AvroLiteral::Map(out)
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Default value for record/struct must be a JSON object".to_string(),
                    ));
                }
            },
            Codec::Union(encodings, _, _) => {
                let Some(default_encoding) = encodings.first() else {
                    return Err(ArrowError::SchemaError(
                        "Union with no branches cannot have a default".to_string(),
                    ));
                };
                default_encoding.parse_default_literal(default_json)?
            }
            #[cfg(feature = "avro_custom_types")]
            Codec::RunEndEncoded(values, _) => values.parse_default_literal(default_json)?,
        };
        Ok(lit)
    }

    fn store_default(&mut self, default_json: &Value) -> Result<(), ArrowError> {
        let json_text = serde_json::to_string(default_json).map_err(|e| {
            ArrowError::ParseError(format!("Failed to serialize default to JSON: {e}"))
        })?;
        self.metadata
            .insert(AVRO_FIELD_DEFAULT_METADATA_KEY.to_string(), json_text);
        Ok(())
    }

    fn parse_and_store_default(&mut self, default_json: &Value) -> Result<AvroLiteral, ArrowError> {
        let lit = self.parse_default_literal(default_json)?;
        self.store_default(default_json)?;
        Ok(lit)
    }
}

/// A named [`AvroDataType`]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AvroField {
    name: String,
    data_type: AvroDataType,
}

impl AvroField {
    /// Returns the arrow [`Field`]
    pub(crate) fn field(&self) -> Field {
        self.data_type.field_with_name(&self.name)
    }

    /// Returns the [`AvroDataType`]
    pub(crate) fn data_type(&self) -> &AvroDataType {
        &self.data_type
    }

    /// Returns a new [`AvroField`] with Utf8View support enabled
    ///
    /// This will convert any Utf8 codecs to Utf8View codecs. This method is used to
    /// enable potential performance optimizations in string-heavy workloads by using
    /// Arrow's StringViewArray data structure.
    ///
    /// Returns a new `AvroField` with the same structure, but with string types
    /// converted to use `Utf8View` instead of `Utf8`.
    pub(crate) fn with_utf8view(&self) -> Self {
        let mut field = self.clone();
        if let Codec::Utf8 = field.data_type.codec {
            field.data_type.codec = Codec::Utf8View;
        }
        field
    }

    /// Returns the name of this Avro field
    ///
    /// This is the field name as defined in the Avro schema.
    /// It's used to identify fields within a record structure.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

impl<'a> TryFrom<&Schema<'a>> for AvroField {
    type Error = ArrowError;

    fn try_from(schema: &Schema<'a>) -> Result<Self, Self::Error> {
        match schema {
            Schema::Complex(ComplexType::Record(r)) => {
                let mut resolver = Maker::new(false, false);
                let data_type = resolver.make_data_type(schema, None, None)?;
                Ok(AvroField {
                    data_type,
                    name: r.name.to_string(),
                })
            }
            _ => Err(ArrowError::ParseError(format!(
                "Expected record got {schema:?}"
            ))),
        }
    }
}

/// Builder for an [`AvroField`]
#[derive(Debug)]
pub(crate) struct AvroFieldBuilder<'a> {
    writer_schema: &'a Schema<'a>,
    reader_schema: Option<&'a Schema<'a>>,
    use_utf8view: bool,
    strict_mode: bool,
}

impl<'a> AvroFieldBuilder<'a> {
    /// Creates a new [`AvroFieldBuilder`] for a given writer schema.
    pub(crate) fn new(writer_schema: &'a Schema<'a>) -> Self {
        Self {
            writer_schema,
            reader_schema: None,
            use_utf8view: false,
            strict_mode: false,
        }
    }

    /// Sets the reader schema for schema resolution.
    ///
    /// If a reader schema is provided, the builder will produce a resolved `AvroField`
    /// that can handle differences between the writer's and reader's schemas.
    #[inline]
    pub(crate) fn with_reader_schema(mut self, reader_schema: &'a Schema<'a>) -> Self {
        self.reader_schema = Some(reader_schema);
        self
    }

    /// Enable or disable Utf8View support
    pub(crate) fn with_utf8view(mut self, use_utf8view: bool) -> Self {
        self.use_utf8view = use_utf8view;
        self
    }

    /// Enable or disable strict mode.
    pub(crate) fn with_strict_mode(mut self, strict_mode: bool) -> Self {
        self.strict_mode = strict_mode;
        self
    }

    /// Build an [`AvroField`] from the builder
    pub(crate) fn build(self) -> Result<AvroField, ArrowError> {
        match self.writer_schema {
            Schema::Complex(ComplexType::Record(r)) => {
                let mut resolver = Maker::new(self.use_utf8view, self.strict_mode);
                let data_type =
                    resolver.make_data_type(self.writer_schema, self.reader_schema, None)?;
                Ok(AvroField {
                    name: r.name.to_string(),
                    data_type,
                })
            }
            _ => Err(ArrowError::ParseError(format!(
                "Expected a Record schema to build an AvroField, but got {:?}",
                self.writer_schema
            ))),
        }
    }
}

/// An Avro encoding
///
/// <https://avro.apache.org/docs/1.11.1/specification/#encodings>
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Codec {
    /// Represents Avro null type, maps to Arrow's Null data type
    Null,
    /// Represents Avro boolean type, maps to Arrow's Boolean data type
    Boolean,
    /// Represents Avro int type, maps to Arrow's Int32 data type
    Int32,
    /// Represents Avro long type, maps to Arrow's Int64 data type
    Int64,
    /// Represents Avro float type, maps to Arrow's Float32 data type
    Float32,
    /// Represents Avro double type, maps to Arrow's Float64 data type
    Float64,
    /// Represents Avro bytes type, maps to Arrow's Binary data type
    Binary,
    /// String data represented as UTF-8 encoded bytes, corresponding to Arrow's StringArray
    Utf8,
    /// String data represented as UTF-8 encoded bytes with an optimized view representation,
    /// corresponding to Arrow's StringViewArray which provides better performance for string operations
    ///
    /// The Utf8View option can be enabled via `ReadOptions::use_utf8view`.
    Utf8View,
    /// Represents Avro date logical type, maps to Arrow's Date32 data type
    Date32,
    /// Represents Avro time-millis logical type, maps to Arrow's Time32(TimeUnit::Millisecond) data type
    TimeMillis,
    /// Represents Avro time-micros logical type, maps to Arrow's Time64(TimeUnit::Microsecond) data type
    TimeMicros,
    /// Represents Avro timestamp-millis or local-timestamp-millis logical type
    ///
    /// Maps to Arrow's Timestamp(TimeUnit::Millisecond) data type
    /// The boolean parameter indicates whether the timestamp has a UTC timezone (true) or is local time (false)
    TimestampMillis(bool),
    /// Represents Avro timestamp-micros or local-timestamp-micros logical type
    ///
    /// Maps to Arrow's Timestamp(TimeUnit::Microsecond) data type
    /// The boolean parameter indicates whether the timestamp has a UTC timezone (true) or is local time (false)
    TimestampMicros(bool),
    /// Represents Avro timestamp-nanos or local-timestamp-nanos logical type
    ///
    /// Maps to Arrow's Timestamp(TimeUnit::Nanosecond) data type
    /// The boolean parameter indicates whether the timestamp has a UTC timezone (true) or is local time (false)
    TimestampNanos(bool),
    /// Represents Avro fixed type, maps to Arrow's FixedSizeBinary data type
    /// The i32 parameter indicates the fixed binary size
    Fixed(i32),
    /// Represents Avro decimal type, maps to Arrow's Decimal32, Decimal64, Decimal128, or Decimal256 data types
    ///
    /// The fields are `(precision, scale, fixed_size)`.
    /// - `precision` (`usize`): Total number of digits.
    /// - `scale` (`Option<usize>`): Number of fractional digits.
    /// - `fixed_size` (`Option<usize>`): Size in bytes if backed by a `fixed` type, otherwise `None`.
    Decimal(usize, Option<usize>, Option<usize>),
    /// Represents Avro Uuid type, a FixedSizeBinary with a length of 16.
    Uuid,
    /// Represents an Avro enum, maps to Arrow's Dictionary(Int32, Utf8) type.
    ///
    /// The enclosed value contains the enum's symbols.
    Enum(Arc<[String]>),
    /// Represents Avro array type, maps to Arrow's List data type
    List(Arc<AvroDataType>),
    /// Represents Avro record type, maps to Arrow's Struct data type
    Struct(Arc<[AvroField]>),
    /// Represents Avro map type, maps to Arrow's Map data type
    Map(Arc<AvroDataType>),
    /// Represents Avro duration logical type, maps to Arrow's Interval(IntervalUnit::MonthDayNano) data type
    Interval,
    /// Represents Avro union type, maps to Arrow's Union data type
    Union(Arc<[AvroDataType]>, UnionFields, UnionMode),
    /// Represents Avro custom logical type to map to Arrow Duration(TimeUnit::Nanosecond)
    #[cfg(feature = "avro_custom_types")]
    DurationNanos,
    /// Represents Avro custom logical type to map to Arrow Duration(TimeUnit::Microsecond)
    #[cfg(feature = "avro_custom_types")]
    DurationMicros,
    /// Represents Avro custom logical type to map to Arrow Duration(TimeUnit::Millisecond)
    #[cfg(feature = "avro_custom_types")]
    DurationMillis,
    /// Represents Avro custom logical type to map to Arrow Duration(TimeUnit::Second)
    #[cfg(feature = "avro_custom_types")]
    DurationSeconds,
    #[cfg(feature = "avro_custom_types")]
    RunEndEncoded(Arc<AvroDataType>, u8),
}

impl Codec {
    fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Binary => DataType::Binary,
            Self::Utf8 => DataType::Utf8,
            Self::Utf8View => DataType::Utf8View,
            Self::Date32 => DataType::Date32,
            Self::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
            Self::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
            Self::TimestampMillis(is_utc) => {
                DataType::Timestamp(TimeUnit::Millisecond, is_utc.then(|| "+00:00".into()))
            }
            Self::TimestampMicros(is_utc) => {
                DataType::Timestamp(TimeUnit::Microsecond, is_utc.then(|| "+00:00".into()))
            }
            Self::TimestampNanos(is_utc) => {
                DataType::Timestamp(TimeUnit::Nanosecond, is_utc.then(|| "+00:00".into()))
            }
            Self::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Fixed(size) => DataType::FixedSizeBinary(*size),
            Self::Decimal(precision, scale, _size) => {
                let p = *precision as u8;
                let s = scale.unwrap_or(0) as i8;
                #[cfg(feature = "small_decimals")]
                {
                    if *precision <= DECIMAL32_MAX_PRECISION as usize {
                        DataType::Decimal32(p, s)
                    } else if *precision <= DECIMAL64_MAX_PRECISION as usize {
                        DataType::Decimal64(p, s)
                    } else if *precision <= DECIMAL128_MAX_PRECISION as usize {
                        DataType::Decimal128(p, s)
                    } else {
                        DataType::Decimal256(p, s)
                    }
                }
                #[cfg(not(feature = "small_decimals"))]
                {
                    if *precision <= DECIMAL128_MAX_PRECISION as usize {
                        DataType::Decimal128(p, s)
                    } else {
                        DataType::Decimal256(p, s)
                    }
                }
            }
            Self::Uuid => DataType::FixedSizeBinary(16),
            Self::Enum(_) => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            Self::List(f) => {
                DataType::List(Arc::new(f.field_with_name(Field::LIST_FIELD_DEFAULT_NAME)))
            }
            Self::Struct(f) => DataType::Struct(f.iter().map(|x| x.field()).collect()),
            Self::Map(value_type) => {
                let val_field = value_type.field_with_name("value");
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            val_field,
                        ])),
                        false,
                    )),
                    false,
                )
            }
            Self::Union(_, fields, mode) => DataType::Union(fields.clone(), *mode),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationNanos => DataType::Duration(TimeUnit::Nanosecond),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationMicros => DataType::Duration(TimeUnit::Microsecond),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationMillis => DataType::Duration(TimeUnit::Millisecond),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSeconds => DataType::Duration(TimeUnit::Second),
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(values, bits) => {
                let run_ends_dt = match *bits {
                    16 => DataType::Int16,
                    32 => DataType::Int32,
                    64 => DataType::Int64,
                    _ => unreachable!(),
                };
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", run_ends_dt, false)),
                    Arc::new(Field::new("values", values.codec().data_type(), true)),
                )
            }
        }
    }

    /// Converts a string codec to use Utf8View if requested
    ///
    /// The conversion only happens if both:
    /// 1. `use_utf8view` is true
    /// 2. The codec is currently `Utf8`
    pub(crate) fn with_utf8view(self, use_utf8view: bool) -> Self {
        if use_utf8view && matches!(self, Self::Utf8) {
            Self::Utf8View
        } else {
            self
        }
    }

    #[inline]
    fn union_field_name(&self) -> String {
        UnionFieldKind::from(self).as_ref().to_owned()
    }
}

impl From<PrimitiveType> for Codec {
    fn from(value: PrimitiveType) -> Self {
        match value {
            PrimitiveType::Null => Self::Null,
            PrimitiveType::Boolean => Self::Boolean,
            PrimitiveType::Int => Self::Int32,
            PrimitiveType::Long => Self::Int64,
            PrimitiveType::Float => Self::Float32,
            PrimitiveType::Double => Self::Float64,
            PrimitiveType::Bytes => Self::Binary,
            PrimitiveType::String => Self::Utf8,
        }
    }
}

/// Compute the exact maximum base‑10 precision that fits in `n` bytes for Avro
/// `fixed` decimals stored as two's‑complement unscaled integers (big‑endian).
///
/// Per Avro spec (Decimal logical type), for a fixed length `n`:
/// max precision = ⌊log₁₀(2^(8n − 1) − 1)⌋.
///
/// This function returns `None` if `n` is 0 or greater than 32 (Arrow supports
/// Decimal256, which is 32 bytes and has max precision 76).
const fn max_precision_for_fixed_bytes(n: usize) -> Option<usize> {
    // Precomputed exact table for n = 1..=32
    // 1:2, 2:4, 3:6, 4:9, 5:11, 6:14, 7:16, 8:18, 9:21, 10:23, 11:26, 12:28,
    // 13:31, 14:33, 15:35, 16:38, 17:40, 18:43, 19:45, 20:47, 21:50, 22:52,
    // 23:55, 24:57, 25:59, 26:62, 27:64, 28:67, 29:69, 30:71, 31:74, 32:76
    const MAX_P: [usize; 32] = [
        2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38, 40, 43, 45, 47, 50, 52, 55, 57,
        59, 62, 64, 67, 69, 71, 74, 76,
    ];
    match n {
        1..=32 => Some(MAX_P[n - 1]),
        _ => None,
    }
}

fn parse_decimal_attributes(
    attributes: &Attributes,
    fallback_size: Option<usize>,
    precision_required: bool,
) -> Result<(usize, usize, Option<usize>), ArrowError> {
    let precision = attributes
        .additional
        .get("precision")
        .and_then(|v| v.as_u64())
        .or(if precision_required { None } else { Some(10) })
        .ok_or_else(|| ArrowError::ParseError("Decimal requires precision".to_string()))?
        as usize;
    let scale = attributes
        .additional
        .get("scale")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let size = attributes
        .additional
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|s| s as usize)
        .or(fallback_size);
    if precision == 0 {
        return Err(ArrowError::ParseError(
            "Decimal requires precision > 0".to_string(),
        ));
    }
    if scale > precision {
        return Err(ArrowError::ParseError(format!(
            "Decimal has invalid scale > precision: scale={scale}, precision={precision}"
        )));
    }
    if precision > DECIMAL256_MAX_PRECISION as usize {
        return Err(ArrowError::ParseError(format!(
            "Decimal precision {precision} exceeds maximum supported by Arrow ({})",
            DECIMAL256_MAX_PRECISION
        )));
    }
    if let Some(sz) = size {
        let max_p = max_precision_for_fixed_bytes(sz).ok_or_else(|| {
            ArrowError::ParseError(format!(
                "Invalid fixed size for decimal: {sz}, must be between 1 and 32 bytes"
            ))
        })?;
        if precision > max_p {
            return Err(ArrowError::ParseError(format!(
                "Decimal precision {precision} exceeds capacity of fixed size {sz} bytes (max {max_p})"
            )));
        }
    }
    Ok((precision, scale, size))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum UnionFieldKind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillisUtc,
    TimestampMillisLocal,
    TimestampMicrosUtc,
    TimestampMicrosLocal,
    TimestampNanosUtc,
    TimestampNanosLocal,
    Duration,
    Fixed,
    Decimal,
    Enum,
    Array,
    Record,
    Map,
    Uuid,
    Union,
}

impl From<&Codec> for UnionFieldKind {
    fn from(c: &Codec) -> Self {
        match c {
            Codec::Null => Self::Null,
            Codec::Boolean => Self::Boolean,
            Codec::Int32 => Self::Int,
            Codec::Int64 => Self::Long,
            Codec::Float32 => Self::Float,
            Codec::Float64 => Self::Double,
            Codec::Binary => Self::Bytes,
            Codec::Utf8 | Codec::Utf8View => Self::String,
            Codec::Date32 => Self::Date,
            Codec::TimeMillis => Self::TimeMillis,
            Codec::TimeMicros => Self::TimeMicros,
            Codec::TimestampMillis(true) => Self::TimestampMillisUtc,
            Codec::TimestampMillis(false) => Self::TimestampMillisLocal,
            Codec::TimestampMicros(true) => Self::TimestampMicrosUtc,
            Codec::TimestampMicros(false) => Self::TimestampMicrosLocal,
            Codec::TimestampNanos(true) => Self::TimestampNanosUtc,
            Codec::TimestampNanos(false) => Self::TimestampNanosLocal,
            Codec::Interval => Self::Duration,
            Codec::Fixed(_) => Self::Fixed,
            Codec::Decimal(..) => Self::Decimal,
            Codec::Enum(_) => Self::Enum,
            Codec::List(_) => Self::Array,
            Codec::Struct(_) => Self::Record,
            Codec::Map(_) => Self::Map,
            Codec::Uuid => Self::Uuid,
            Codec::Union(..) => Self::Union,
            #[cfg(feature = "avro_custom_types")]
            Codec::RunEndEncoded(values, _) => UnionFieldKind::from(values.codec()),
            #[cfg(feature = "avro_custom_types")]
            Codec::DurationNanos
            | Codec::DurationMicros
            | Codec::DurationMillis
            | Codec::DurationSeconds => Self::Duration,
        }
    }
}

fn union_branch_name(dt: &AvroDataType) -> String {
    if let Some(name) = dt.metadata.get(AVRO_NAME_METADATA_KEY) {
        if name.contains(".") {
            // Full name
            return name.to_string();
        }
        if let Some(ns) = dt.metadata.get(AVRO_NAMESPACE_METADATA_KEY) {
            return format!("{ns}.{name}");
        }
        return name.to_string();
    }
    dt.codec.union_field_name()
}

fn build_union_fields(encodings: &[AvroDataType]) -> Result<UnionFields, ArrowError> {
    let arrow_fields: Vec<Field> = encodings
        .iter()
        .map(|encoding| encoding.field_with_name(&union_branch_name(encoding)))
        .collect();
    let type_ids: Vec<i8> = (0..arrow_fields.len()).map(|i| i as i8).collect();
    UnionFields::try_new(type_ids, arrow_fields)
}

/// Resolves Avro type names to [`AvroDataType`]
///
/// See <https://avro.apache.org/docs/1.11.1/specification/#names>
#[derive(Debug, Default)]
struct Resolver<'a> {
    map: HashMap<(&'a str, &'a str), AvroDataType>,
}

impl<'a> Resolver<'a> {
    fn register(&mut self, name: &'a str, namespace: Option<&'a str>, schema: AvroDataType) {
        self.map.insert((namespace.unwrap_or(""), name), schema);
    }

    fn resolve(&self, name: &str, namespace: Option<&'a str>) -> Result<AvroDataType, ArrowError> {
        let (namespace, name) = name
            .rsplit_once('.')
            .unwrap_or_else(|| (namespace.unwrap_or(""), name));
        self.map
            .get(&(namespace, name))
            .ok_or_else(|| ArrowError::ParseError(format!("Failed to resolve {namespace}.{name}")))
            .cloned()
    }
}

fn full_name_set(name: &str, ns: Option<&str>, aliases: &[&str]) -> HashSet<String> {
    let mut out = HashSet::with_capacity(1 + aliases.len());
    let (full, _) = make_full_name(name, ns, None);
    out.insert(full);
    for a in aliases {
        let (fa, _) = make_full_name(a, None, ns);
        out.insert(fa);
    }
    out
}

fn names_match(
    writer_name: &str,
    writer_namespace: Option<&str>,
    writer_aliases: &[&str],
    reader_name: &str,
    reader_namespace: Option<&str>,
    reader_aliases: &[&str],
) -> bool {
    let writer_set = full_name_set(writer_name, writer_namespace, writer_aliases);
    let reader_set = full_name_set(reader_name, reader_namespace, reader_aliases);
    // If the canonical full names match, or any alias matches cross-wise.
    !writer_set.is_disjoint(&reader_set)
}

fn ensure_names_match(
    data_type: &str,
    writer_name: &str,
    writer_namespace: Option<&str>,
    writer_aliases: &[&str],
    reader_name: &str,
    reader_namespace: Option<&str>,
    reader_aliases: &[&str],
) -> Result<(), ArrowError> {
    if names_match(
        writer_name,
        writer_namespace,
        writer_aliases,
        reader_name,
        reader_namespace,
        reader_aliases,
    ) {
        Ok(())
    } else {
        Err(ArrowError::ParseError(format!(
            "{data_type} name mismatch writer={writer_name}, reader={reader_name}"
        )))
    }
}

fn primitive_of(schema: &Schema) -> Option<PrimitiveType> {
    match schema {
        Schema::TypeName(TypeName::Primitive(primitive)) => Some(*primitive),
        Schema::Type(Type {
            r#type: TypeName::Primitive(primitive),
            ..
        }) => Some(*primitive),
        _ => None,
    }
}

fn nullable_union_variants<'x, 'y>(
    variant: &'y [Schema<'x>],
) -> Option<(Nullability, &'y Schema<'x>)> {
    if variant.len() != 2 {
        return None;
    }
    let is_null = |schema: &Schema<'x>| {
        matches!(
            schema,
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null))
        )
    };
    match (is_null(&variant[0]), is_null(&variant[1])) {
        (true, false) => Some((Nullability::NullFirst, &variant[1])),
        (false, true) => Some((Nullability::NullSecond, &variant[0])),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum UnionBranchKey {
    Named(String),
    Primitive(PrimitiveType),
    Array,
    Map,
}

fn branch_key_of<'a>(s: &Schema<'a>, enclosing_ns: Option<&'a str>) -> Option<UnionBranchKey> {
    let (name, namespace) = match s {
        Schema::TypeName(TypeName::Primitive(p))
        | Schema::Type(Type {
            r#type: TypeName::Primitive(p),
            ..
        }) => return Some(UnionBranchKey::Primitive(*p)),
        Schema::TypeName(TypeName::Ref(name))
        | Schema::Type(Type {
            r#type: TypeName::Ref(name),
            ..
        }) => (name, None),
        Schema::Complex(ComplexType::Array(_)) => return Some(UnionBranchKey::Array),
        Schema::Complex(ComplexType::Map(_)) => return Some(UnionBranchKey::Map),
        Schema::Complex(ComplexType::Record(r)) => (&r.name, r.namespace),
        Schema::Complex(ComplexType::Enum(e)) => (&e.name, e.namespace),
        Schema::Complex(ComplexType::Fixed(f)) => (&f.name, f.namespace),
        Schema::Union(_) => return None,
    };
    let (full, _) = make_full_name(name, namespace, enclosing_ns);
    Some(UnionBranchKey::Named(full))
}

fn union_first_duplicate<'a>(
    branches: &'a [Schema<'a>],
    enclosing_ns: Option<&'a str>,
) -> Option<String> {
    let mut seen = HashSet::with_capacity(branches.len());
    for schema in branches {
        if let Some(key) = branch_key_of(schema, enclosing_ns) {
            if !seen.insert(key.clone()) {
                let msg = match key {
                    UnionBranchKey::Named(full) => format!("named type {full}"),
                    UnionBranchKey::Primitive(p) => format!("primitive {}", p.as_ref()),
                    UnionBranchKey::Array => "array".to_string(),
                    UnionBranchKey::Map => "map".to_string(),
                };
                return Some(msg);
            }
        }
    }
    None
}

/// Resolves Avro type names to [`AvroDataType`]
///
/// See <https://avro.apache.org/docs/1.11.1/specification/#names>
struct Maker<'a> {
    resolver: Resolver<'a>,
    use_utf8view: bool,
    strict_mode: bool,
}

impl<'a> Maker<'a> {
    fn new(use_utf8view: bool, strict_mode: bool) -> Self {
        Self {
            resolver: Default::default(),
            use_utf8view,
            strict_mode,
        }
    }

    #[cfg(feature = "avro_custom_types")]
    #[inline]
    fn propagate_nullability_into_ree(dt: &mut AvroDataType, nb: Nullability) {
        if let Codec::RunEndEncoded(values, bits) = dt.codec.clone() {
            let mut inner = (*values).clone();
            inner.nullability = Some(nb);
            dt.codec = Codec::RunEndEncoded(Arc::new(inner), bits);
        }
    }

    fn make_data_type<'s>(
        &mut self,
        writer_schema: &'s Schema<'a>,
        reader_schema: Option<&'s Schema<'a>>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        match reader_schema {
            Some(reader_schema) => self.resolve_type(writer_schema, reader_schema, namespace),
            None => self.parse_type(writer_schema, namespace),
        }
    }

    /// Parses a [`AvroDataType`] from the provided `Schema` and the given `name` and `namespace`
    ///
    /// `name`: is the name used to refer to `schema` in its parent
    /// `namespace`: an optional qualifier used as part of a type hierarchy
    /// If the data type is a string, convert to use Utf8View if requested
    ///
    /// This function is used during the schema conversion process to determine whether
    /// string data should be represented as StringArray (default) or StringViewArray.
    ///
    /// `use_utf8view`: if true, use Utf8View instead of Utf8 for string types
    ///
    /// See [`Resolver`] for more information
    fn parse_type<'s>(
        &mut self,
        schema: &'s Schema<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        match schema {
            Schema::TypeName(TypeName::Primitive(p)) => Ok(AvroDataType::new(
                Codec::from(*p).with_utf8view(self.use_utf8view),
                Default::default(),
                None,
            )),
            Schema::TypeName(TypeName::Ref(name)) => self.resolver.resolve(name, namespace),
            Schema::Union(f) => {
                let null = f
                    .iter()
                    .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)));
                match (f.len() == 2, null) {
                    (true, Some(0)) => {
                        let mut field = self.parse_type(&f[1], namespace)?;
                        field.nullability = Some(Nullability::NullFirst);
                        #[cfg(feature = "avro_custom_types")]
                        Self::propagate_nullability_into_ree(&mut field, Nullability::NullFirst);
                        return Ok(field);
                    }
                    (true, Some(1)) => {
                        if self.strict_mode {
                            return Err(ArrowError::SchemaError(
                                "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
                                    .to_string(),
                            ));
                        }
                        let mut field = self.parse_type(&f[0], namespace)?;
                        field.nullability = Some(Nullability::NullSecond);
                        #[cfg(feature = "avro_custom_types")]
                        Self::propagate_nullability_into_ree(&mut field, Nullability::NullSecond);
                        return Ok(field);
                    }
                    _ => {}
                }
                // Validate: unions may not immediately contain unions
                if f.iter().any(|s| matches!(s, Schema::Union(_))) {
                    return Err(ArrowError::SchemaError(
                        "Avro unions may not immediately contain other unions".to_string(),
                    ));
                }
                // Validate: duplicates (named by full name; non-named by kind)
                if let Some(dup) = union_first_duplicate(f, namespace) {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro union contains duplicate branch type: {dup}"
                    )));
                }
                // Parse all branches
                let children: Vec<AvroDataType> = f
                    .iter()
                    .map(|s| self.parse_type(s, namespace))
                    .collect::<Result<_, _>>()?;
                // Build Arrow layout once here
                let union_fields = build_union_fields(&children)?;
                Ok(AvroDataType::new(
                    Codec::Union(Arc::from(children), union_fields, UnionMode::Dense),
                    Default::default(),
                    None,
                ))
            }
            Schema::Complex(c) => match c {
                ComplexType::Record(r) => {
                    let namespace = r.namespace.or(namespace);
                    let mut metadata = r.attributes.field_metadata();
                    let fields = r
                        .fields
                        .iter()
                        .map(|field| {
                            Ok(AvroField {
                                name: field.name.to_string(),
                                data_type: self.parse_type(&field.r#type, namespace)?,
                            })
                        })
                        .collect::<Result<_, ArrowError>>()?;
                    metadata.insert(AVRO_NAME_METADATA_KEY.to_string(), r.name.to_string());
                    if let Some(ns) = namespace {
                        metadata.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), ns.to_string());
                    }
                    let field = AvroDataType {
                        nullability: None,
                        codec: Codec::Struct(fields),
                        metadata,
                        resolution: None,
                    };
                    self.resolver.register(r.name, namespace, field.clone());
                    Ok(field)
                }
                ComplexType::Array(a) => {
                    let field = self.parse_type(a.items.as_ref(), namespace)?;
                    Ok(AvroDataType {
                        nullability: None,
                        metadata: a.attributes.field_metadata(),
                        codec: Codec::List(Arc::new(field)),
                        resolution: None,
                    })
                }
                ComplexType::Fixed(f) => {
                    let size = f.size.try_into().map_err(|e| {
                        ArrowError::ParseError(format!("Overflow converting size to i32: {e}"))
                    })?;
                    let namespace = f.namespace.or(namespace);
                    let mut metadata = f.attributes.field_metadata();
                    metadata.insert(AVRO_NAME_METADATA_KEY.to_string(), f.name.to_string());
                    if let Some(ns) = namespace {
                        metadata.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), ns.to_string());
                    }
                    let field = match f.attributes.logical_type {
                        Some("decimal") => {
                            let (precision, scale, _) =
                                parse_decimal_attributes(&f.attributes, Some(size as usize), true)?;
                            AvroDataType {
                                nullability: None,
                                metadata,
                                codec: Codec::Decimal(precision, Some(scale), Some(size as usize)),
                                resolution: None,
                            }
                        }
                        Some("duration") => {
                            if size != 12 {
                                return Err(ArrowError::ParseError(format!(
                                    "Invalid fixed size for Duration: {size}, must be 12"
                                )));
                            };
                            AvroDataType {
                                nullability: None,
                                metadata,
                                codec: Codec::Interval,
                                resolution: None,
                            }
                        }
                        _ => AvroDataType {
                            nullability: None,
                            metadata,
                            codec: Codec::Fixed(size),
                            resolution: None,
                        },
                    };
                    self.resolver.register(f.name, namespace, field.clone());
                    Ok(field)
                }
                ComplexType::Enum(e) => {
                    let namespace = e.namespace.or(namespace);
                    let symbols = e
                        .symbols
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Arc<[String]>>();
                    let mut metadata = e.attributes.field_metadata();
                    let symbols_json = serde_json::to_string(&e.symbols).map_err(|e| {
                        ArrowError::ParseError(format!("Failed to serialize enum symbols: {e}"))
                    })?;
                    metadata.insert(AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(), symbols_json);
                    metadata.insert(AVRO_NAME_METADATA_KEY.to_string(), e.name.to_string());
                    if let Some(ns) = namespace {
                        metadata.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), ns.to_string());
                    }
                    let field = AvroDataType {
                        nullability: None,
                        metadata,
                        codec: Codec::Enum(symbols),
                        resolution: None,
                    };
                    self.resolver.register(e.name, namespace, field.clone());
                    Ok(field)
                }
                ComplexType::Map(m) => {
                    let val = self.parse_type(&m.values, namespace)?;
                    Ok(AvroDataType {
                        nullability: None,
                        metadata: m.attributes.field_metadata(),
                        codec: Codec::Map(Arc::new(val)),
                        resolution: None,
                    })
                }
            },
            Schema::Type(t) => {
                let mut field = self.parse_type(&Schema::TypeName(t.r#type.clone()), namespace)?;
                // https://avro.apache.org/docs/1.11.1/specification/#logical-types
                match (t.attributes.logical_type, &mut field.codec) {
                    (Some("decimal"), c @ Codec::Binary) => {
                        let (prec, sc, _) = parse_decimal_attributes(&t.attributes, None, false)?;
                        *c = Codec::Decimal(prec, Some(sc), None);
                    }
                    (Some("date"), c @ Codec::Int32) => *c = Codec::Date32,
                    (Some("time-millis"), c @ Codec::Int32) => *c = Codec::TimeMillis,
                    (Some("time-micros"), c @ Codec::Int64) => *c = Codec::TimeMicros,
                    (Some("timestamp-millis"), c @ Codec::Int64) => {
                        *c = Codec::TimestampMillis(true)
                    }
                    (Some("timestamp-micros"), c @ Codec::Int64) => {
                        *c = Codec::TimestampMicros(true)
                    }
                    (Some("local-timestamp-millis"), c @ Codec::Int64) => {
                        *c = Codec::TimestampMillis(false)
                    }
                    (Some("local-timestamp-micros"), c @ Codec::Int64) => {
                        *c = Codec::TimestampMicros(false)
                    }
                    (Some("timestamp-nanos"), c @ Codec::Int64) => *c = Codec::TimestampNanos(true),
                    (Some("local-timestamp-nanos"), c @ Codec::Int64) => {
                        *c = Codec::TimestampNanos(false)
                    }
                    (Some("uuid"), c @ Codec::Utf8) => {
                        // Map Avro string+logicalType=uuid into the UUID Codec,
                        // and preserve the logicalType in Arrow field metadata
                        // so writers can round-trip it correctly.
                        *c = Codec::Uuid;
                        field.metadata.insert("logicalType".into(), "uuid".into());
                    }
                    #[cfg(feature = "avro_custom_types")]
                    (Some("arrow.duration-nanos"), c @ Codec::Int64) => *c = Codec::DurationNanos,
                    #[cfg(feature = "avro_custom_types")]
                    (Some("arrow.duration-micros"), c @ Codec::Int64) => *c = Codec::DurationMicros,
                    #[cfg(feature = "avro_custom_types")]
                    (Some("arrow.duration-millis"), c @ Codec::Int64) => *c = Codec::DurationMillis,
                    #[cfg(feature = "avro_custom_types")]
                    (Some("arrow.duration-seconds"), c @ Codec::Int64) => {
                        *c = Codec::DurationSeconds
                    }
                    #[cfg(feature = "avro_custom_types")]
                    (Some("arrow.run-end-encoded"), _) => {
                        let bits_u8: u8 = t
                            .attributes
                            .additional
                            .get("arrow.runEndIndexBits")
                            .and_then(|v| v.as_u64())
                            .and_then(|n| u8::try_from(n).ok())
                            .ok_or_else(|| ArrowError::ParseError(
                                "arrow.run-end-encoded requires 'arrow.runEndIndexBits' (one of 16, 32, or 64)"
                                    .to_string(),
                            ))?;
                        if bits_u8 != 16 && bits_u8 != 32 && bits_u8 != 64 {
                            return Err(ArrowError::ParseError(format!(
                                "Invalid 'arrow.runEndIndexBits' value {bits_u8}; must be 16, 32, or 64"
                            )));
                        }
                        // Wrap the parsed underlying site as REE
                        let values_site = field.clone();
                        field.codec = Codec::RunEndEncoded(Arc::new(values_site), bits_u8);
                    }
                    (Some(logical), _) => {
                        // Insert unrecognized logical type into metadata map
                        field.metadata.insert("logicalType".into(), logical.into());
                    }
                    (None, _) => {}
                }
                if matches!(field.codec, Codec::Int64) {
                    if let Some(unit) = t
                        .attributes
                        .additional
                        .get("arrowTimeUnit")
                        .and_then(|v| v.as_str())
                    {
                        if unit == "nanosecond" {
                            field.codec = Codec::TimestampNanos(false);
                        }
                    }
                }
                if !t.attributes.additional.is_empty() {
                    for (k, v) in &t.attributes.additional {
                        field.metadata.insert(k.to_string(), v.to_string());
                    }
                }
                Ok(field)
            }
        }
    }

    fn resolve_type<'s>(
        &mut self,
        writer_schema: &'s Schema<'a>,
        reader_schema: &'s Schema<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        if let (Some(write_primitive), Some(read_primitive)) =
            (primitive_of(writer_schema), primitive_of(reader_schema))
        {
            return self.resolve_primitives(write_primitive, read_primitive, reader_schema);
        }
        match (writer_schema, reader_schema) {
            (Schema::Union(writer_variants), Schema::Union(reader_variants)) => {
                let writer_variants = writer_variants.as_slice();
                let reader_variants = reader_variants.as_slice();
                match (
                    nullable_union_variants(writer_variants),
                    nullable_union_variants(reader_variants),
                ) {
                    (Some((w_nb, w_nonnull)), Some((_r_nb, r_nonnull))) => {
                        let mut dt = self.make_data_type(w_nonnull, Some(r_nonnull), namespace)?;
                        dt.nullability = Some(w_nb);
                        #[cfg(feature = "avro_custom_types")]
                        Self::propagate_nullability_into_ree(&mut dt, w_nb);
                        Ok(dt)
                    }
                    _ => self.resolve_unions(writer_variants, reader_variants, namespace),
                }
            }
            (Schema::Union(writer_variants), reader_non_union) => {
                let writer_to_reader: Vec<Option<(usize, Promotion)>> = writer_variants
                    .iter()
                    .map(|writer| {
                        self.resolve_type(writer, reader_non_union, namespace)
                            .ok()
                            .map(|tmp| (0usize, Self::coercion_from(&tmp)))
                    })
                    .collect();
                let mut dt = self.parse_type(reader_non_union, namespace)?;
                dt.resolution = Some(ResolutionInfo::Union(ResolvedUnion {
                    writer_to_reader: Arc::from(writer_to_reader),
                    writer_is_union: true,
                    reader_is_union: false,
                }));
                Ok(dt)
            }
            (writer_non_union, Schema::Union(reader_variants)) => {
                let promo = self.find_best_promotion(
                    writer_non_union,
                    reader_variants.as_slice(),
                    namespace,
                );
                let Some((reader_index, promotion)) = promo else {
                    return Err(ArrowError::SchemaError(
                        "Writer schema does not match any reader union branch".to_string(),
                    ));
                };
                let mut dt = self.parse_type(reader_schema, namespace)?;
                dt.resolution = Some(ResolutionInfo::Union(ResolvedUnion {
                    writer_to_reader: Arc::from(vec![Some((reader_index, promotion))]),
                    writer_is_union: false,
                    reader_is_union: true,
                }));
                Ok(dt)
            }
            (
                Schema::Complex(ComplexType::Array(writer_array)),
                Schema::Complex(ComplexType::Array(reader_array)),
            ) => self.resolve_array(writer_array, reader_array, namespace),
            (
                Schema::Complex(ComplexType::Map(writer_map)),
                Schema::Complex(ComplexType::Map(reader_map)),
            ) => self.resolve_map(writer_map, reader_map, namespace),
            (
                Schema::Complex(ComplexType::Fixed(writer_fixed)),
                Schema::Complex(ComplexType::Fixed(reader_fixed)),
            ) => self.resolve_fixed(writer_fixed, reader_fixed, reader_schema, namespace),
            (
                Schema::Complex(ComplexType::Record(writer_record)),
                Schema::Complex(ComplexType::Record(reader_record)),
            ) => self.resolve_records(writer_record, reader_record, namespace),
            (
                Schema::Complex(ComplexType::Enum(writer_enum)),
                Schema::Complex(ComplexType::Enum(reader_enum)),
            ) => self.resolve_enums(writer_enum, reader_enum, reader_schema, namespace),
            (Schema::TypeName(TypeName::Ref(_)), _) => self.parse_type(reader_schema, namespace),
            (_, Schema::TypeName(TypeName::Ref(_))) => self.parse_type(reader_schema, namespace),
            _ => Err(ArrowError::NotYetImplemented(
                "Other resolutions not yet implemented".to_string(),
            )),
        }
    }

    #[inline]
    fn coercion_from(dt: &AvroDataType) -> Promotion {
        match dt.resolution.as_ref() {
            Some(ResolutionInfo::Promotion(promotion)) => *promotion,
            _ => Promotion::Direct,
        }
    }

    fn find_best_promotion(
        &mut self,
        writer: &Schema<'a>,
        reader_variants: &[Schema<'a>],
        namespace: Option<&'a str>,
    ) -> Option<(usize, Promotion)> {
        let mut first_promotion: Option<(usize, Promotion)> = None;
        for (reader_index, reader) in reader_variants.iter().enumerate() {
            if let Ok(tmp) = self.resolve_type(writer, reader, namespace) {
                let promotion = Self::coercion_from(&tmp);
                if promotion == Promotion::Direct {
                    // An exact match is best, return immediately.
                    return Some((reader_index, promotion));
                } else if first_promotion.is_none() {
                    // Store the first valid promotion but keep searching for a direct match.
                    first_promotion = Some((reader_index, promotion));
                }
            }
        }
        first_promotion
    }

    fn resolve_unions<'s>(
        &mut self,
        writer_variants: &'s [Schema<'a>],
        reader_variants: &'s [Schema<'a>],
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        let reader_encodings: Vec<AvroDataType> = reader_variants
            .iter()
            .map(|reader_schema| self.parse_type(reader_schema, namespace))
            .collect::<Result<_, _>>()?;
        let mut writer_to_reader: Vec<Option<(usize, Promotion)>> =
            Vec::with_capacity(writer_variants.len());
        for writer in writer_variants {
            writer_to_reader.push(self.find_best_promotion(writer, reader_variants, namespace));
        }
        let union_fields = build_union_fields(&reader_encodings)?;
        let mut dt = AvroDataType::new(
            Codec::Union(reader_encodings.into(), union_fields, UnionMode::Dense),
            Default::default(),
            None,
        );
        dt.resolution = Some(ResolutionInfo::Union(ResolvedUnion {
            writer_to_reader: Arc::from(writer_to_reader),
            writer_is_union: true,
            reader_is_union: true,
        }));
        Ok(dt)
    }

    fn resolve_array(
        &mut self,
        writer_array: &Array<'a>,
        reader_array: &Array<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        Ok(AvroDataType {
            nullability: None,
            metadata: reader_array.attributes.field_metadata(),
            codec: Codec::List(Arc::new(self.make_data_type(
                writer_array.items.as_ref(),
                Some(reader_array.items.as_ref()),
                namespace,
            )?)),
            resolution: None,
        })
    }

    fn resolve_map(
        &mut self,
        writer_map: &Map<'a>,
        reader_map: &Map<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        Ok(AvroDataType {
            nullability: None,
            metadata: reader_map.attributes.field_metadata(),
            codec: Codec::Map(Arc::new(self.make_data_type(
                &writer_map.values,
                Some(&reader_map.values),
                namespace,
            )?)),
            resolution: None,
        })
    }

    fn resolve_fixed<'s>(
        &mut self,
        writer_fixed: &Fixed<'a>,
        reader_fixed: &Fixed<'a>,
        reader_schema: &'s Schema<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        ensure_names_match(
            "Fixed",
            writer_fixed.name,
            writer_fixed.namespace,
            &writer_fixed.aliases,
            reader_fixed.name,
            reader_fixed.namespace,
            &reader_fixed.aliases,
        )?;
        if writer_fixed.size != reader_fixed.size {
            return Err(ArrowError::SchemaError(format!(
                "Fixed size mismatch for {}: writer={}, reader={}",
                reader_fixed.name, writer_fixed.size, reader_fixed.size
            )));
        }
        self.parse_type(reader_schema, namespace)
    }

    fn resolve_primitives(
        &mut self,
        write_primitive: PrimitiveType,
        read_primitive: PrimitiveType,
        reader_schema: &Schema<'a>,
    ) -> Result<AvroDataType, ArrowError> {
        if write_primitive == read_primitive {
            return self.parse_type(reader_schema, None);
        }
        let promotion = match (write_primitive, read_primitive) {
            (PrimitiveType::Int, PrimitiveType::Long) => Promotion::IntToLong,
            (PrimitiveType::Int, PrimitiveType::Float) => Promotion::IntToFloat,
            (PrimitiveType::Int, PrimitiveType::Double) => Promotion::IntToDouble,
            (PrimitiveType::Long, PrimitiveType::Float) => Promotion::LongToFloat,
            (PrimitiveType::Long, PrimitiveType::Double) => Promotion::LongToDouble,
            (PrimitiveType::Float, PrimitiveType::Double) => Promotion::FloatToDouble,
            (PrimitiveType::String, PrimitiveType::Bytes) => Promotion::StringToBytes,
            (PrimitiveType::Bytes, PrimitiveType::String) => Promotion::BytesToString,
            _ => {
                return Err(ArrowError::ParseError(format!(
                    "Illegal promotion {write_primitive:?} to {read_primitive:?}"
                )));
            }
        };
        let mut datatype = self.parse_type(reader_schema, None)?;
        datatype.resolution = Some(ResolutionInfo::Promotion(promotion));
        Ok(datatype)
    }

    // Resolve writer vs. reader enum schemas according to Avro 1.11.1.
    //
    // # How enums resolve (writer to reader)
    // Per “Schema Resolution”:
    // * The two schemas must refer to the same (unqualified) enum name (or match
    //   via alias rewriting).
    // * If the writer’s symbol is not present in the reader’s enum and the reader
    //   enum has a `default`, that `default` symbol must be used; otherwise,
    //   error.
    //   https://avro.apache.org/docs/1.11.1/specification/#schema-resolution
    // * Avro “Aliases” are applied from the reader side to rewrite the writer’s
    //   names during resolution. For robustness across ecosystems, we also accept
    //   symmetry here (see note below).
    //   https://avro.apache.org/docs/1.11.1/specification/#aliases
    //
    // # Rationale for this code path
    // 1. Do the work once at schema‑resolution time. Avro serializes an enum as a
    //    writer‑side position. Mapping positions on the hot decoder path is expensive
    //    if done with string lookups. This method builds a `writer_index to reader_index`
    //    vector once, so decoding just does an O(1) table lookup.
    // 2. Adopt the reader’s symbol set and order. We return an Arrow
    //    `Dictionary(Int32, Utf8)` whose dictionary values are the reader enum
    //    symbols. This makes downstream semantics match the reader schema, including
    //    Avro’s sort order rule that orders enums by symbol position in the schema.
    //    https://avro.apache.org/docs/1.11.1/specification/#sort-order
    // 3. Honor Avro’s `default` for enums. Avro 1.9+ allows a type‑level default
    //    on the enum. When the writer emits a symbol unknown to the reader, we map it
    //    to the reader’s validated `default` symbol if present; otherwise we signal an
    //    error at decoding time.
    //    https://avro.apache.org/docs/1.11.1/specification/#enums
    //
    // # Implementation notes
    // * We first check that enum names match or are*alias‑equivalent. The Avro
    //   spec describes alias rewriting using reader aliases; this implementation
    //   additionally treats writer aliases as acceptable for name matching to be
    //   resilient with schemas produced by different tooling.
    // * We build `EnumMapping`:
    //   - `mapping[i]` = reader index of the writer symbol at writer index `i`.
    //   - If the writer symbol is absent and the reader has a default, we store the
    //     reader index of that default.
    //   - Otherwise we store `-1` as a sentinel meaning unresolvable; the decoder
    //     must treat encountering such a value as an error, per the spec.
    // * We persist the reader symbol list in field metadata under
    //   `AVRO_ENUM_SYMBOLS_METADATA_KEY`, so consumers can inspect the dictionary
    //   without needing the original Avro schema.
    // * The Arrow representation is `Dictionary(Int32, Utf8)`, which aligns with
    //   Avro’s integer index encoding for enums.
    //
    // # Examples
    // * Writer `["A","B","C"]`, Reader `["A","B"]`, Reader default `"A"`
    //     `mapping = [0, 1, 0]`, `default_index = 0`.
    // * Writer `["A","B"]`, Reader `["B","A"]` (no default)
    //     `mapping = [1, 0]`, `default_index = -1`.
    // * Writer `["A","B","C"]`, Reader `["A","B"]` (no default)
    //     `mapping = [0, 1, -1]` (decode must error on `"C"`).
    fn resolve_enums(
        &mut self,
        writer_enum: &Enum<'a>,
        reader_enum: &Enum<'a>,
        reader_schema: &Schema<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        ensure_names_match(
            "Enum",
            writer_enum.name,
            writer_enum.namespace,
            &writer_enum.aliases,
            reader_enum.name,
            reader_enum.namespace,
            &reader_enum.aliases,
        )?;
        if writer_enum.symbols == reader_enum.symbols {
            return self.parse_type(reader_schema, namespace);
        }
        let reader_index: HashMap<&str, i32> = reader_enum
            .symbols
            .iter()
            .enumerate()
            .map(|(index, &symbol)| (symbol, index as i32))
            .collect();
        let default_index: i32 = match reader_enum.default {
            Some(symbol) => *reader_index.get(symbol).ok_or_else(|| {
                ArrowError::SchemaError(format!(
                    "Reader enum '{}' default symbol '{symbol}' not found in symbols list",
                    reader_enum.name,
                ))
            })?,
            None => -1,
        };
        let mapping: Vec<i32> = writer_enum
            .symbols
            .iter()
            .map(|&write_symbol| {
                reader_index
                    .get(write_symbol)
                    .copied()
                    .unwrap_or(default_index)
            })
            .collect();
        if self.strict_mode && mapping.iter().any(|&m| m < 0) {
            return Err(ArrowError::SchemaError(format!(
                "Reader enum '{}' does not cover all writer symbols and no default is provided",
                reader_enum.name
            )));
        }
        let mut dt = self.parse_type(reader_schema, namespace)?;
        dt.resolution = Some(ResolutionInfo::EnumMapping(EnumMapping {
            mapping: Arc::from(mapping),
            default_index,
        }));
        let reader_ns = reader_enum.namespace.or(namespace);
        self.resolver
            .register(reader_enum.name, reader_ns, dt.clone());
        Ok(dt)
    }

    #[inline]
    fn build_writer_lookup(
        writer_record: &Record<'a>,
    ) -> (HashMap<&'a str, usize>, HashSet<&'a str>) {
        let mut map: HashMap<&str, usize> = HashMap::with_capacity(writer_record.fields.len() * 2);
        for (idx, wf) in writer_record.fields.iter().enumerate() {
            // Avro field names are unique; last-in wins are acceptable and match previous behavior.
            map.insert(wf.name, idx);
        }
        // Track ambiguous writer aliases (alias used by multiple writer fields)
        let mut ambiguous: HashSet<&str> = HashSet::new();
        for (idx, wf) in writer_record.fields.iter().enumerate() {
            for &alias in &wf.aliases {
                match map.entry(alias) {
                    Entry::Occupied(e) if *e.get() != idx => {
                        ambiguous.insert(alias);
                    }
                    Entry::Vacant(e) => {
                        e.insert(idx);
                    }
                    _ => {}
                }
            }
        }
        (map, ambiguous)
    }

    fn resolve_records(
        &mut self,
        writer_record: &Record<'a>,
        reader_record: &Record<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        ensure_names_match(
            "Record",
            writer_record.name,
            writer_record.namespace,
            &writer_record.aliases,
            reader_record.name,
            reader_record.namespace,
            &reader_record.aliases,
        )?;
        let writer_ns = writer_record.namespace.or(namespace);
        let reader_ns = reader_record.namespace.or(namespace);
        let reader_md = reader_record.attributes.field_metadata();
        // Build writer lookup and ambiguous alias set.
        let (writer_lookup, ambiguous_writer_aliases) = Self::build_writer_lookup(writer_record);
        let mut writer_to_reader: Vec<Option<usize>> = vec![None; writer_record.fields.len()];
        let mut reader_fields: Vec<AvroField> = Vec::with_capacity(reader_record.fields.len());
        // Capture default field indices during the main loop (one pass).
        let mut default_fields: Vec<usize> = Vec::new();
        for (reader_idx, r_field) in reader_record.fields.iter().enumerate() {
            // Direct name match, then reader aliases (a writer alias map is pre-populated).
            let mut match_idx = writer_lookup.get(r_field.name).copied();
            let mut matched_via_alias: Option<&str> = None;
            if match_idx.is_none() {
                for &alias in &r_field.aliases {
                    if let Some(i) = writer_lookup.get(alias).copied() {
                        if self.strict_mode && ambiguous_writer_aliases.contains(alias) {
                            return Err(ArrowError::SchemaError(format!(
                                "Ambiguous alias '{alias}' on reader field '{}' matches multiple writer fields",
                                r_field.name
                            )));
                        }
                        match_idx = Some(i);
                        matched_via_alias = Some(alias);
                        break;
                    }
                }
            }
            if let Some(wi) = match_idx {
                if writer_to_reader[wi].is_none() {
                    let w_schema = &writer_record.fields[wi].r#type;
                    let dt = self.make_data_type(w_schema, Some(&r_field.r#type), reader_ns)?;
                    writer_to_reader[wi] = Some(reader_idx);
                    reader_fields.push(AvroField {
                        name: r_field.name.to_owned(),
                        data_type: dt,
                    });
                    continue;
                } else if self.strict_mode {
                    // Writer field already mapped and strict_mode => error
                    let existing_reader = writer_to_reader[wi].unwrap();
                    let via = matched_via_alias
                        .map(|a| format!("alias '{a}'"))
                        .unwrap_or_else(|| "name match".to_string());
                    return Err(ArrowError::SchemaError(format!(
                        "Multiple reader fields map to the same writer field '{}' via {via} (existing reader index {existing_reader}, new reader index {reader_idx})",
                        writer_record.fields[wi].name
                    )));
                }
                // Non-strict and already mapped -> fall through to defaulting logic
            }
            // No match (or conflicted in non-strict mode): attach default per Avro spec.
            let mut dt = self.parse_type(&r_field.r#type, reader_ns)?;
            if let Some(default_json) = r_field.default.as_ref() {
                dt.resolution = Some(ResolutionInfo::DefaultValue(
                    dt.parse_and_store_default(default_json)?,
                ));
                default_fields.push(reader_idx);
            } else if dt.nullability() == Some(Nullability::NullFirst) {
                // The only valid implicit default for a union is the first branch (null-first case).
                dt.resolution = Some(ResolutionInfo::DefaultValue(
                    dt.parse_and_store_default(&Value::Null)?,
                ));
                default_fields.push(reader_idx);
            } else {
                return Err(ArrowError::SchemaError(format!(
                    "Reader field '{}' not present in writer schema must have a default value",
                    r_field.name
                )));
            }
            reader_fields.push(AvroField {
                name: r_field.name.to_owned(),
                data_type: dt,
            });
        }
        // Build skip_fields in writer order; pre-size and push.
        let mut skip_fields: Vec<Option<AvroDataType>> =
            Vec::with_capacity(writer_record.fields.len());
        for (writer_index, writer_field) in writer_record.fields.iter().enumerate() {
            if writer_to_reader[writer_index].is_some() {
                skip_fields.push(None);
            } else {
                skip_fields.push(Some(self.parse_type(&writer_field.r#type, writer_ns)?));
            }
        }
        let resolved = AvroDataType::new_with_resolution(
            Codec::Struct(Arc::from(reader_fields)),
            reader_md,
            None,
            Some(ResolutionInfo::Record(ResolvedRecord {
                writer_to_reader: Arc::from(writer_to_reader),
                default_fields: Arc::from(default_fields),
                skip_fields: Arc::from(skip_fields),
            })),
        );
        // Register a resolved record by reader name+namespace for potential named type refs.
        self.resolver
            .register(reader_record.name, reader_ns, resolved.clone());
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        AVRO_ROOT_RECORD_DEFAULT_NAME, Array, Attributes, ComplexType, Field as AvroFieldSchema,
        Fixed, PrimitiveType, Record, Schema, Type, TypeName,
    };
    use indexmap::IndexMap;
    use serde_json::{self, Value};

    fn create_schema_with_logical_type(
        primitive_type: PrimitiveType,
        logical_type: &'static str,
    ) -> Schema<'static> {
        let attributes = Attributes {
            logical_type: Some(logical_type),
            additional: Default::default(),
        };

        Schema::Type(Type {
            r#type: TypeName::Primitive(primitive_type),
            attributes,
        })
    }

    fn resolve_promotion(writer: PrimitiveType, reader: PrimitiveType) -> AvroDataType {
        let writer_schema = Schema::TypeName(TypeName::Primitive(writer));
        let reader_schema = Schema::TypeName(TypeName::Primitive(reader));
        let mut maker = Maker::new(false, false);
        maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("promotion should resolve")
    }

    fn mk_primitive(pt: PrimitiveType) -> Schema<'static> {
        Schema::TypeName(TypeName::Primitive(pt))
    }
    fn mk_union(branches: Vec<Schema<'_>>) -> Schema<'_> {
        Schema::Union(branches)
    }

    #[test]
    fn test_date_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Int, "date");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::Date32));
    }

    #[test]
    fn test_time_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Int, "time-millis");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimeMillis));
    }

    #[test]
    fn test_time_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "time-micros");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimeMicros));
    }

    #[test]
    fn test_timestamp_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "timestamp-millis");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMillis(true)));
    }

    #[test]
    fn test_timestamp_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "timestamp-micros");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMicros(true)));
    }

    #[test]
    fn test_local_timestamp_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "local-timestamp-millis");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMillis(false)));
    }

    #[test]
    fn test_local_timestamp_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "local-timestamp-micros");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMicros(false)));
    }

    #[test]
    fn test_uuid_type() {
        let mut codec = Codec::Fixed(16);
        if let c @ Codec::Fixed(16) = &mut codec {
            *c = Codec::Uuid;
        }
        assert!(matches!(codec, Codec::Uuid));
    }

    #[test]
    fn test_duration_logical_type() {
        let mut codec = Codec::Fixed(12);

        if let c @ Codec::Fixed(12) = &mut codec {
            *c = Codec::Interval;
        }

        assert!(matches!(codec, Codec::Interval));
    }

    #[test]
    fn test_decimal_logical_type_not_implemented() {
        let codec = Codec::Fixed(16);

        let process_decimal = || -> Result<(), ArrowError> {
            if let Codec::Fixed(_) = codec {
                return Err(ArrowError::NotYetImplemented(
                    "Decimals are not currently supported".to_string(),
                ));
            }
            Ok(())
        };

        let result = process_decimal();

        assert!(result.is_err());
        if let Err(ArrowError::NotYetImplemented(msg)) = result {
            assert!(msg.contains("Decimals are not currently supported"));
        } else {
            panic!("Expected NotYetImplemented error");
        }
    }
    #[test]
    fn test_unknown_logical_type_added_to_metadata() {
        let schema = create_schema_with_logical_type(PrimitiveType::Int, "custom-type");

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert_eq!(
            result.metadata.get("logicalType"),
            Some(&"custom-type".to_string())
        );
    }

    #[test]
    fn test_string_with_utf8view_enabled() {
        let schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));

        let mut maker = Maker::new(true, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::Utf8View));
    }

    #[test]
    fn test_string_without_utf8view_enabled() {
        let schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));

        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        assert!(matches!(result.codec, Codec::Utf8));
    }

    #[test]
    fn test_record_with_string_and_utf8view_enabled() {
        let field_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));

        let avro_field = crate::schema::Field {
            name: "string_field",
            r#type: field_schema,
            default: None,
            doc: None,
            aliases: vec![],
        };

        let record = Record {
            name: "test_record",
            namespace: None,
            aliases: vec![],
            doc: None,
            fields: vec![avro_field],
            attributes: Attributes::default(),
        };

        let schema = Schema::Complex(ComplexType::Record(record));

        let mut maker = Maker::new(true, false);
        let result = maker.make_data_type(&schema, None, None).unwrap();

        if let Codec::Struct(fields) = &result.codec {
            let first_field_codec = &fields[0].data_type().codec;
            assert!(matches!(first_field_codec, Codec::Utf8View));
        } else {
            panic!("Expected Struct codec");
        }
    }

    #[test]
    fn test_union_with_strict_mode() {
        let schema = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
        ]);

        let mut maker = Maker::new(false, true);
        let result = maker.make_data_type(&schema, None, None);

        assert!(result.is_err());
        match result {
            Err(ArrowError::SchemaError(msg)) => {
                assert!(msg.contains(
                    "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
                ));
            }
            _ => panic!("Expected SchemaError"),
        }
    }

    #[test]
    fn test_resolve_int_to_float_promotion() {
        let result = resolve_promotion(PrimitiveType::Int, PrimitiveType::Float);
        assert!(matches!(result.codec, Codec::Float32));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToFloat))
        );
    }

    #[test]
    fn test_resolve_int_to_double_promotion() {
        let result = resolve_promotion(PrimitiveType::Int, PrimitiveType::Double);
        assert!(matches!(result.codec, Codec::Float64));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToDouble))
        );
    }

    #[test]
    fn test_resolve_long_to_float_promotion() {
        let result = resolve_promotion(PrimitiveType::Long, PrimitiveType::Float);
        assert!(matches!(result.codec, Codec::Float32));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::LongToFloat))
        );
    }

    #[test]
    fn test_resolve_long_to_double_promotion() {
        let result = resolve_promotion(PrimitiveType::Long, PrimitiveType::Double);
        assert!(matches!(result.codec, Codec::Float64));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::LongToDouble))
        );
    }

    #[test]
    fn test_resolve_float_to_double_promotion() {
        let result = resolve_promotion(PrimitiveType::Float, PrimitiveType::Double);
        assert!(matches!(result.codec, Codec::Float64));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::FloatToDouble))
        );
    }

    #[test]
    fn test_resolve_string_to_bytes_promotion() {
        let result = resolve_promotion(PrimitiveType::String, PrimitiveType::Bytes);
        assert!(matches!(result.codec, Codec::Binary));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::StringToBytes))
        );
    }

    #[test]
    fn test_resolve_bytes_to_string_promotion() {
        let result = resolve_promotion(PrimitiveType::Bytes, PrimitiveType::String);
        assert!(matches!(result.codec, Codec::Utf8));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::BytesToString))
        );
    }

    #[test]
    fn test_resolve_illegal_promotion_double_to_float_errors() {
        let writer_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::Double));
        let reader_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::Float));
        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&writer_schema, Some(&reader_schema), None);
        assert!(result.is_err());
        match result {
            Err(ArrowError::ParseError(msg)) => {
                assert!(msg.contains("Illegal promotion"));
            }
            _ => panic!("Expected ParseError for illegal promotion Double -> Float"),
        }
    }

    #[test]
    fn test_promotion_within_nullable_union_keeps_writer_null_ordering() {
        let writer = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
        ]);
        let reader = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Double)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
        ]);
        let mut maker = Maker::new(false, false);
        let result = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        assert!(matches!(result.codec, Codec::Float64));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToDouble))
        );
        assert_eq!(result.nullability, Some(Nullability::NullFirst));
    }

    #[test]
    fn test_resolve_writer_union_to_reader_non_union_partial_coverage() {
        let writer = mk_union(vec![
            mk_primitive(PrimitiveType::String),
            mk_primitive(PrimitiveType::Long),
        ]);
        let reader = mk_primitive(PrimitiveType::Bytes);
        let mut maker = Maker::new(false, false);
        let dt = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        assert!(matches!(dt.codec(), Codec::Binary));
        let resolved = match dt.resolution {
            Some(ResolutionInfo::Union(u)) => u,
            other => panic!("expected union resolution info, got {other:?}"),
        };
        assert!(resolved.writer_is_union && !resolved.reader_is_union);
        assert_eq!(
            resolved.writer_to_reader.as_ref(),
            &[Some((0, Promotion::StringToBytes)), None]
        );
    }

    #[test]
    fn test_resolve_writer_non_union_to_reader_union_prefers_direct_over_promotion() {
        let writer = mk_primitive(PrimitiveType::Long);
        let reader = mk_union(vec![
            mk_primitive(PrimitiveType::Long),
            mk_primitive(PrimitiveType::Double),
        ]);
        let mut maker = Maker::new(false, false);
        let dt = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        let resolved = match dt.resolution {
            Some(ResolutionInfo::Union(u)) => u,
            other => panic!("expected union resolution info, got {other:?}"),
        };
        assert!(!resolved.writer_is_union && resolved.reader_is_union);
        assert_eq!(
            resolved.writer_to_reader.as_ref(),
            &[Some((0, Promotion::Direct))]
        );
    }

    #[test]
    fn test_resolve_writer_non_union_to_reader_union_uses_promotion_when_needed() {
        let writer = mk_primitive(PrimitiveType::Int);
        let reader = mk_union(vec![
            mk_primitive(PrimitiveType::Null),
            mk_primitive(PrimitiveType::Long),
            mk_primitive(PrimitiveType::String),
        ]);
        let mut maker = Maker::new(false, false);
        let dt = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        let resolved = match dt.resolution {
            Some(ResolutionInfo::Union(u)) => u,
            other => panic!("expected union resolution info, got {other:?}"),
        };
        assert_eq!(
            resolved.writer_to_reader.as_ref(),
            &[Some((1, Promotion::IntToLong))]
        );
    }

    #[test]
    fn test_resolve_both_nullable_unions_direct_match() {
        let writer = mk_union(vec![
            mk_primitive(PrimitiveType::Null),
            mk_primitive(PrimitiveType::String),
        ]);
        let reader = mk_union(vec![
            mk_primitive(PrimitiveType::String),
            mk_primitive(PrimitiveType::Null),
        ]);
        let mut maker = Maker::new(false, false);
        let dt = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        assert!(matches!(dt.codec(), Codec::Utf8));
        assert_eq!(dt.nullability, Some(Nullability::NullFirst));
        assert!(dt.resolution.is_none());
    }

    #[test]
    fn test_resolve_both_nullable_unions_with_promotion() {
        let writer = mk_union(vec![
            mk_primitive(PrimitiveType::Null),
            mk_primitive(PrimitiveType::Int),
        ]);
        let reader = mk_union(vec![
            mk_primitive(PrimitiveType::Double),
            mk_primitive(PrimitiveType::Null),
        ]);
        let mut maker = Maker::new(false, false);
        let dt = maker.make_data_type(&writer, Some(&reader), None).unwrap();
        assert!(matches!(dt.codec(), Codec::Float64));
        assert_eq!(dt.nullability, Some(Nullability::NullFirst));
        assert_eq!(
            dt.resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToDouble))
        );
    }

    #[test]
    fn test_resolve_type_promotion() {
        let writer_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::Int));
        let reader_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::Long));
        let mut maker = Maker::new(false, false);
        let result = maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .unwrap();
        assert!(matches!(result.codec, Codec::Int64));
        assert_eq!(
            result.resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToLong))
        );
    }

    #[test]
    fn test_nested_record_type_reuse_without_namespace() {
        let schema_str = r#"
        {
          "type": "record",
          "name": "Record",
          "fields": [
            {
              "name": "nested",
              "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                  { "name": "nested_int", "type": "int" }
                ]
              }
            },
            { "name": "nestedRecord", "type": "Nested" },
            { "name": "nestedArray", "type": { "type": "array", "items": "Nested" } },
            { "name": "nestedMap", "type": { "type": "map", "values": "Nested" } }
          ]
        }
        "#;

        let schema: Schema = serde_json::from_str(schema_str).unwrap();

        let mut maker = Maker::new(false, false);
        let avro_data_type = maker.make_data_type(&schema, None, None).unwrap();

        if let Codec::Struct(fields) = avro_data_type.codec() {
            assert_eq!(fields.len(), 4);

            // nested
            assert_eq!(fields[0].name(), "nested");
            let nested_data_type = fields[0].data_type();
            if let Codec::Struct(nested_fields) = nested_data_type.codec() {
                assert_eq!(nested_fields.len(), 1);
                assert_eq!(nested_fields[0].name(), "nested_int");
                assert!(matches!(nested_fields[0].data_type().codec(), Codec::Int32));
            } else {
                panic!(
                    "'nested' field is not a struct but {:?}",
                    nested_data_type.codec()
                );
            }

            // nestedRecord
            assert_eq!(fields[1].name(), "nestedRecord");
            let nested_record_data_type = fields[1].data_type();
            assert_eq!(
                nested_record_data_type.codec().data_type(),
                nested_data_type.codec().data_type()
            );

            // nestedArray
            assert_eq!(fields[2].name(), "nestedArray");
            if let Codec::List(item_type) = fields[2].data_type().codec() {
                assert_eq!(
                    item_type.codec().data_type(),
                    nested_data_type.codec().data_type()
                );
            } else {
                panic!("'nestedArray' field is not a list");
            }

            // nestedMap
            assert_eq!(fields[3].name(), "nestedMap");
            if let Codec::Map(value_type) = fields[3].data_type().codec() {
                assert_eq!(
                    value_type.codec().data_type(),
                    nested_data_type.codec().data_type()
                );
            } else {
                panic!("'nestedMap' field is not a map");
            }
        } else {
            panic!("Top-level schema is not a struct");
        }
    }

    #[test]
    fn test_nested_enum_type_reuse_with_namespace() {
        let schema_str = r#"
        {
          "type": "record",
          "name": "Record",
          "namespace": "record_ns",
          "fields": [
            {
              "name": "status",
              "type": {
                "type": "enum",
                "name": "Status",
                "namespace": "enum_ns",
                "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
              }
            },
            { "name": "backupStatus", "type": "enum_ns.Status" },
            { "name": "statusHistory", "type": { "type": "array", "items": "enum_ns.Status" } },
            { "name": "statusMap", "type": { "type": "map", "values": "enum_ns.Status" } }
          ]
        }
        "#;

        let schema: Schema = serde_json::from_str(schema_str).unwrap();

        let mut maker = Maker::new(false, false);
        let avro_data_type = maker.make_data_type(&schema, None, None).unwrap();

        if let Codec::Struct(fields) = avro_data_type.codec() {
            assert_eq!(fields.len(), 4);

            // status
            assert_eq!(fields[0].name(), "status");
            let status_data_type = fields[0].data_type();
            if let Codec::Enum(symbols) = status_data_type.codec() {
                assert_eq!(symbols.as_ref(), &["ACTIVE", "INACTIVE", "PENDING"]);
            } else {
                panic!(
                    "'status' field is not an enum but {:?}",
                    status_data_type.codec()
                );
            }

            // backupStatus
            assert_eq!(fields[1].name(), "backupStatus");
            let backup_status_data_type = fields[1].data_type();
            assert_eq!(
                backup_status_data_type.codec().data_type(),
                status_data_type.codec().data_type()
            );

            // statusHistory
            assert_eq!(fields[2].name(), "statusHistory");
            if let Codec::List(item_type) = fields[2].data_type().codec() {
                assert_eq!(
                    item_type.codec().data_type(),
                    status_data_type.codec().data_type()
                );
            } else {
                panic!("'statusHistory' field is not a list");
            }

            // statusMap
            assert_eq!(fields[3].name(), "statusMap");
            if let Codec::Map(value_type) = fields[3].data_type().codec() {
                assert_eq!(
                    value_type.codec().data_type(),
                    status_data_type.codec().data_type()
                );
            } else {
                panic!("'statusMap' field is not a map");
            }
        } else {
            panic!("Top-level schema is not a struct");
        }
    }

    #[test]
    fn test_resolve_from_writer_and_reader_defaults_root_name_for_non_record_reader() {
        let writer_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));
        let reader_schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));
        let mut maker = Maker::new(false, false);
        let data_type = maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("resolution should succeed");
        let field = AvroField {
            name: AVRO_ROOT_RECORD_DEFAULT_NAME.to_string(),
            data_type,
        };
        assert_eq!(field.name(), AVRO_ROOT_RECORD_DEFAULT_NAME);
        assert!(matches!(field.data_type().codec(), Codec::Utf8));
    }

    fn json_string(s: &str) -> Value {
        Value::String(s.to_string())
    }

    fn assert_default_stored(dt: &AvroDataType, default_json: &Value) {
        let stored = dt
            .metadata
            .get(AVRO_FIELD_DEFAULT_METADATA_KEY)
            .cloned()
            .unwrap_or_default();
        let expected = serde_json::to_string(default_json).unwrap();
        assert_eq!(stored, expected, "stored default metadata should match");
    }

    #[test]
    fn test_validate_and_store_default_null_and_nullability_rules() {
        let mut dt_null = AvroDataType::new(Codec::Null, HashMap::new(), None);
        let lit = dt_null.parse_and_store_default(&Value::Null).unwrap();
        assert_eq!(lit, AvroLiteral::Null);
        assert_default_stored(&dt_null, &Value::Null);
        let mut dt_int = AvroDataType::new(Codec::Int32, HashMap::new(), None);
        let err = dt_int.parse_and_store_default(&Value::Null).unwrap_err();
        assert!(
            err.to_string()
                .contains("JSON null default is only valid for `null` type"),
            "unexpected error: {err}"
        );
        let mut dt_int_nf =
            AvroDataType::new(Codec::Int32, HashMap::new(), Some(Nullability::NullFirst));
        let lit2 = dt_int_nf.parse_and_store_default(&Value::Null).unwrap();
        assert_eq!(lit2, AvroLiteral::Null);
        assert_default_stored(&dt_int_nf, &Value::Null);
        let mut dt_int_ns =
            AvroDataType::new(Codec::Int32, HashMap::new(), Some(Nullability::NullSecond));
        let err2 = dt_int_ns.parse_and_store_default(&Value::Null).unwrap_err();
        assert!(
            err2.to_string()
                .contains("JSON null default is only valid for `null` type"),
            "unexpected error: {err2}"
        );
    }

    #[test]
    fn test_validate_and_store_default_primitives_and_temporal() {
        let mut dt_bool = AvroDataType::new(Codec::Boolean, HashMap::new(), None);
        let lit = dt_bool.parse_and_store_default(&Value::Bool(true)).unwrap();
        assert_eq!(lit, AvroLiteral::Boolean(true));
        assert_default_stored(&dt_bool, &Value::Bool(true));
        let mut dt_i32 = AvroDataType::new(Codec::Int32, HashMap::new(), None);
        let lit = dt_i32
            .parse_and_store_default(&serde_json::json!(123))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Int(123));
        assert_default_stored(&dt_i32, &serde_json::json!(123));
        let err = dt_i32
            .parse_and_store_default(&serde_json::json!(i64::from(i32::MAX) + 1))
            .unwrap_err();
        assert!(format!("{err}").contains("out of i32 range"));
        let mut dt_i64 = AvroDataType::new(Codec::Int64, HashMap::new(), None);
        let lit = dt_i64
            .parse_and_store_default(&serde_json::json!(1234567890))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Long(1234567890));
        assert_default_stored(&dt_i64, &serde_json::json!(1234567890));
        let mut dt_f32 = AvroDataType::new(Codec::Float32, HashMap::new(), None);
        let lit = dt_f32
            .parse_and_store_default(&serde_json::json!(1.25))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Float(1.25));
        assert_default_stored(&dt_f32, &serde_json::json!(1.25));
        let err = dt_f32
            .parse_and_store_default(&serde_json::json!(1e39))
            .unwrap_err();
        assert!(format!("{err}").contains("out of f32 range"));
        let mut dt_f64 = AvroDataType::new(Codec::Float64, HashMap::new(), None);
        let lit = dt_f64
            .parse_and_store_default(&serde_json::json!(std::f64::consts::PI))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Double(std::f64::consts::PI));
        assert_default_stored(&dt_f64, &serde_json::json!(std::f64::consts::PI));
        let mut dt_str = AvroDataType::new(Codec::Utf8, HashMap::new(), None);
        let l = dt_str
            .parse_and_store_default(&json_string("hello"))
            .unwrap();
        assert_eq!(l, AvroLiteral::String("hello".into()));
        assert_default_stored(&dt_str, &json_string("hello"));
        let mut dt_strv = AvroDataType::new(Codec::Utf8View, HashMap::new(), None);
        let l = dt_strv
            .parse_and_store_default(&json_string("view"))
            .unwrap();
        assert_eq!(l, AvroLiteral::String("view".into()));
        assert_default_stored(&dt_strv, &json_string("view"));
        let mut dt_uuid = AvroDataType::new(Codec::Uuid, HashMap::new(), None);
        let l = dt_uuid
            .parse_and_store_default(&json_string("00000000-0000-0000-0000-000000000000"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::String("00000000-0000-0000-0000-000000000000".into())
        );
        let mut dt_bin = AvroDataType::new(Codec::Binary, HashMap::new(), None);
        let l = dt_bin.parse_and_store_default(&json_string("ABC")).unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![65, 66, 67]));
        let err = dt_bin
            .parse_and_store_default(&json_string("€")) // U+20AC
            .unwrap_err();
        assert!(format!("{err}").contains("Invalid codepoint"));
        let mut dt_date = AvroDataType::new(Codec::Date32, HashMap::new(), None);
        let ld = dt_date
            .parse_and_store_default(&serde_json::json!(1))
            .unwrap();
        assert_eq!(ld, AvroLiteral::Int(1));
        let mut dt_tmill = AvroDataType::new(Codec::TimeMillis, HashMap::new(), None);
        let lt = dt_tmill
            .parse_and_store_default(&serde_json::json!(86_400_000))
            .unwrap();
        assert_eq!(lt, AvroLiteral::Int(86_400_000));
        let mut dt_tmicros = AvroDataType::new(Codec::TimeMicros, HashMap::new(), None);
        let ltm = dt_tmicros
            .parse_and_store_default(&serde_json::json!(1_000_000))
            .unwrap();
        assert_eq!(ltm, AvroLiteral::Long(1_000_000));
        let mut dt_ts_milli = AvroDataType::new(Codec::TimestampMillis(true), HashMap::new(), None);
        let l1 = dt_ts_milli
            .parse_and_store_default(&serde_json::json!(123))
            .unwrap();
        assert_eq!(l1, AvroLiteral::Long(123));
        let mut dt_ts_micro =
            AvroDataType::new(Codec::TimestampMicros(false), HashMap::new(), None);
        let l2 = dt_ts_micro
            .parse_and_store_default(&serde_json::json!(456))
            .unwrap();
        assert_eq!(l2, AvroLiteral::Long(456));
    }

    #[test]
    fn test_validate_and_store_default_fixed_decimal_interval() {
        let mut dt_fixed = AvroDataType::new(Codec::Fixed(4), HashMap::new(), None);
        let l = dt_fixed
            .parse_and_store_default(&json_string("WXYZ"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![87, 88, 89, 90]));
        let err = dt_fixed
            .parse_and_store_default(&json_string("TOO LONG"))
            .unwrap_err();
        assert!(err.to_string().contains("Default length"));
        let mut dt_dec_fixed =
            AvroDataType::new(Codec::Decimal(10, Some(2), Some(3)), HashMap::new(), None);
        let l = dt_dec_fixed
            .parse_and_store_default(&json_string("abc"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![97, 98, 99]));
        let err = dt_dec_fixed
            .parse_and_store_default(&json_string("toolong"))
            .unwrap_err();
        assert!(err.to_string().contains("Default length"));
        let mut dt_dec_bytes =
            AvroDataType::new(Codec::Decimal(10, Some(2), None), HashMap::new(), None);
        let l = dt_dec_bytes
            .parse_and_store_default(&json_string("freeform"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::Bytes("freeform".bytes().collect::<Vec<_>>())
        );
        let mut dt_interval = AvroDataType::new(Codec::Interval, HashMap::new(), None);
        let l = dt_interval
            .parse_and_store_default(&json_string("ABCDEFGHIJKL"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::Bytes("ABCDEFGHIJKL".bytes().collect::<Vec<_>>())
        );
        let err = dt_interval
            .parse_and_store_default(&json_string("short"))
            .unwrap_err();
        assert!(err.to_string().contains("Default length"));
    }

    #[test]
    fn test_validate_and_store_default_enum_list_map_struct() {
        let symbols: Arc<[String]> = ["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()]
            .into_iter()
            .collect();
        let mut dt_enum = AvroDataType::new(Codec::Enum(symbols), HashMap::new(), None);
        let l = dt_enum
            .parse_and_store_default(&json_string("GREEN"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Enum("GREEN".into()));
        let err = dt_enum
            .parse_and_store_default(&json_string("YELLOW"))
            .unwrap_err();
        assert!(err.to_string().contains("Default enum symbol"));
        let item = AvroDataType::new(Codec::Int64, HashMap::new(), None);
        let mut dt_list = AvroDataType::new(Codec::List(Arc::new(item)), HashMap::new(), None);
        let val = serde_json::json!([1, 2, 3]);
        let l = dt_list.parse_and_store_default(&val).unwrap();
        assert_eq!(
            l,
            AvroLiteral::Array(vec![
                AvroLiteral::Long(1),
                AvroLiteral::Long(2),
                AvroLiteral::Long(3)
            ])
        );
        let err = dt_list
            .parse_and_store_default(&serde_json::json!({"not":"array"}))
            .unwrap_err();
        assert!(err.to_string().contains("JSON array"));
        let val_dt = AvroDataType::new(Codec::Float64, HashMap::new(), None);
        let mut dt_map = AvroDataType::new(Codec::Map(Arc::new(val_dt)), HashMap::new(), None);
        let mv = serde_json::json!({"x": 1.5, "y": 2.5});
        let l = dt_map.parse_and_store_default(&mv).unwrap();
        let mut expected = IndexMap::new();
        expected.insert("x".into(), AvroLiteral::Double(1.5));
        expected.insert("y".into(), AvroLiteral::Double(2.5));
        assert_eq!(l, AvroLiteral::Map(expected));
        // Not object -> error
        let err = dt_map
            .parse_and_store_default(&serde_json::json!(123))
            .unwrap_err();
        assert!(err.to_string().contains("JSON object"));
        let mut field_a = AvroField {
            name: "a".into(),
            data_type: AvroDataType::new(Codec::Int32, HashMap::new(), None),
        };
        let field_b = AvroField {
            name: "b".into(),
            data_type: AvroDataType::new(
                Codec::Int64,
                HashMap::new(),
                Some(Nullability::NullFirst),
            ),
        };
        let mut c_md = HashMap::new();
        c_md.insert(AVRO_FIELD_DEFAULT_METADATA_KEY.into(), "\"xyz\"".into());
        let field_c = AvroField {
            name: "c".into(),
            data_type: AvroDataType::new(Codec::Utf8, c_md, None),
        };
        field_a.data_type.metadata.insert("doc".into(), "na".into());
        let struct_fields: Arc<[AvroField]> = Arc::from(vec![field_a, field_b, field_c]);
        let mut dt_struct = AvroDataType::new(Codec::Struct(struct_fields), HashMap::new(), None);
        let default_obj = serde_json::json!({"a": 7});
        let l = dt_struct.parse_and_store_default(&default_obj).unwrap();
        let mut expected = IndexMap::new();
        expected.insert("a".into(), AvroLiteral::Int(7));
        expected.insert("b".into(), AvroLiteral::Null);
        expected.insert("c".into(), AvroLiteral::String("xyz".into()));
        assert_eq!(l, AvroLiteral::Map(expected));
        assert_default_stored(&dt_struct, &default_obj);
        let req_field = AvroField {
            name: "req".into(),
            data_type: AvroDataType::new(Codec::Boolean, HashMap::new(), None),
        };
        let mut dt_bad = AvroDataType::new(
            Codec::Struct(Arc::from(vec![req_field])),
            HashMap::new(),
            None,
        );
        let err = dt_bad
            .parse_and_store_default(&serde_json::json!({}))
            .unwrap_err();
        assert!(
            err.to_string().contains("missing required subfield 'req'"),
            "unexpected error: {err}"
        );
        let err = dt_struct
            .parse_and_store_default(&serde_json::json!(10))
            .unwrap_err();
        err.to_string().contains("must be a JSON object");
    }

    #[test]
    fn test_resolve_array_promotion_and_reader_metadata() {
        let mut w_add: HashMap<&str, Value> = HashMap::new();
        w_add.insert("who", json_string("writer"));
        let mut r_add: HashMap<&str, Value> = HashMap::new();
        r_add.insert("who", json_string("reader"));
        let writer_schema = Schema::Complex(ComplexType::Array(Array {
            items: Box::new(Schema::TypeName(TypeName::Primitive(PrimitiveType::Int))),
            attributes: Attributes {
                logical_type: None,
                additional: w_add,
            },
        }));
        let reader_schema = Schema::Complex(ComplexType::Array(Array {
            items: Box::new(Schema::TypeName(TypeName::Primitive(PrimitiveType::Long))),
            attributes: Attributes {
                logical_type: None,
                additional: r_add,
            },
        }));
        let mut maker = Maker::new(false, false);
        let dt = maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .unwrap();
        assert_eq!(dt.metadata.get("who"), Some(&"\"reader\"".to_string()));
        if let Codec::List(inner) = dt.codec() {
            assert!(matches!(inner.codec(), Codec::Int64));
            assert_eq!(
                inner.resolution,
                Some(ResolutionInfo::Promotion(Promotion::IntToLong))
            );
        } else {
            panic!("expected list codec");
        }
    }

    #[test]
    fn test_resolve_fixed_success_name_and_size_match_and_alias() {
        let writer_schema = Schema::Complex(ComplexType::Fixed(Fixed {
            name: "MD5",
            namespace: None,
            aliases: vec!["Hash16"],
            size: 16,
            attributes: Attributes::default(),
        }));
        let reader_schema = Schema::Complex(ComplexType::Fixed(Fixed {
            name: "Hash16",
            namespace: None,
            aliases: vec![],
            size: 16,
            attributes: Attributes::default(),
        }));
        let mut maker = Maker::new(false, false);
        let dt = maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .unwrap();
        assert!(matches!(dt.codec(), Codec::Fixed(16)));
    }

    #[test]
    fn test_resolve_records_mapping_default_fields_and_skip_fields() {
        let writer = Schema::Complex(ComplexType::Record(Record {
            name: "R",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![
                crate::schema::Field {
                    name: "a",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                    default: None,
                    aliases: vec![],
                },
                crate::schema::Field {
                    name: "skipme",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: None,
                    aliases: vec![],
                },
                crate::schema::Field {
                    name: "b",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                    default: None,
                    aliases: vec![],
                },
            ],
            attributes: Attributes::default(),
        }));
        let reader = Schema::Complex(ComplexType::Record(Record {
            name: "R",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![
                crate::schema::Field {
                    name: "b",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                    default: None,
                    aliases: vec![],
                },
                crate::schema::Field {
                    name: "a",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                    default: None,
                    aliases: vec![],
                },
                crate::schema::Field {
                    name: "name",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: Some(json_string("anon")),
                    aliases: vec![],
                },
                crate::schema::Field {
                    name: "opt",
                    doc: None,
                    r#type: Schema::Union(vec![
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                    ]),
                    default: None, // should default to null because NullFirst
                    aliases: vec![],
                },
            ],
            attributes: Attributes::default(),
        }));
        let mut maker = Maker::new(false, false);
        let dt = maker
            .make_data_type(&writer, Some(&reader), None)
            .expect("record resolution");
        let fields = match dt.codec() {
            Codec::Struct(f) => f,
            other => panic!("expected struct, got {other:?}"),
        };
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].name(), "b");
        assert_eq!(fields[1].name(), "a");
        assert_eq!(fields[2].name(), "name");
        assert_eq!(fields[3].name(), "opt");
        assert!(matches!(
            fields[1].data_type().resolution,
            Some(ResolutionInfo::Promotion(Promotion::IntToLong))
        ));
        let rec = match dt.resolution {
            Some(ResolutionInfo::Record(ref r)) => r.clone(),
            other => panic!("expected record resolution, got {other:?}"),
        };
        assert_eq!(rec.writer_to_reader.as_ref(), &[Some(1), None, Some(0)]);
        assert_eq!(rec.default_fields.as_ref(), &[2usize, 3usize]);
        assert!(rec.skip_fields[0].is_none());
        assert!(rec.skip_fields[2].is_none());
        let skip1 = rec.skip_fields[1].as_ref().expect("skip field present");
        assert!(matches!(skip1.codec(), Codec::Utf8));
        let name_md = &fields[2].data_type().metadata;
        assert_eq!(
            name_md.get(AVRO_FIELD_DEFAULT_METADATA_KEY),
            Some(&"\"anon\"".to_string())
        );
        let opt_md = &fields[3].data_type().metadata;
        assert_eq!(
            opt_md.get(AVRO_FIELD_DEFAULT_METADATA_KEY),
            Some(&"null".to_string())
        );
    }

    #[test]
    fn test_named_type_alias_resolution_record_cross_namespace() {
        let writer_record = Record {
            name: "PersonV2",
            namespace: Some("com.example.v2"),
            doc: None,
            aliases: vec!["com.example.Person"],
            fields: vec![
                AvroFieldSchema {
                    name: "name",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: None,
                    aliases: vec![],
                },
                AvroFieldSchema {
                    name: "age",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                    default: None,
                    aliases: vec![],
                },
            ],
            attributes: Attributes::default(),
        };
        let reader_record = Record {
            name: "Person",
            namespace: Some("com.example"),
            doc: None,
            aliases: vec![],
            fields: writer_record.fields.clone(),
            attributes: Attributes::default(),
        };
        let writer_schema = Schema::Complex(ComplexType::Record(writer_record));
        let reader_schema = Schema::Complex(ComplexType::Record(reader_record));
        let mut maker = Maker::new(false, false);
        let result = maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("record alias resolution should succeed");
        match result.codec {
            Codec::Struct(ref fields) => assert_eq!(fields.len(), 2),
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn test_named_type_alias_resolution_enum_cross_namespace() {
        let writer_enum = Enum {
            name: "ColorV2",
            namespace: Some("org.example.v2"),
            doc: None,
            aliases: vec!["org.example.Color"],
            symbols: vec!["RED", "GREEN", "BLUE"],
            default: None,
            attributes: Attributes::default(),
        };
        let reader_enum = Enum {
            name: "Color",
            namespace: Some("org.example"),
            doc: None,
            aliases: vec![],
            symbols: vec!["RED", "GREEN", "BLUE"],
            default: None,
            attributes: Attributes::default(),
        };
        let writer_schema = Schema::Complex(ComplexType::Enum(writer_enum));
        let reader_schema = Schema::Complex(ComplexType::Enum(reader_enum));
        let mut maker = Maker::new(false, false);
        maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("enum alias resolution should succeed");
    }

    #[test]
    fn test_named_type_alias_resolution_fixed_cross_namespace() {
        let writer_fixed = Fixed {
            name: "Fx10V2",
            namespace: Some("ns.v2"),
            aliases: vec!["ns.Fx10"],
            size: 10,
            attributes: Attributes::default(),
        };
        let reader_fixed = Fixed {
            name: "Fx10",
            namespace: Some("ns"),
            aliases: vec![],
            size: 10,
            attributes: Attributes::default(),
        };
        let writer_schema = Schema::Complex(ComplexType::Fixed(writer_fixed));
        let reader_schema = Schema::Complex(ComplexType::Fixed(reader_fixed));
        let mut maker = Maker::new(false, false);
        maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("fixed alias resolution should succeed");
    }
}
