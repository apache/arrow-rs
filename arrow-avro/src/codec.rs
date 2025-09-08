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

use crate::schema::{
    Array, Attributes, AvroSchema, ComplexType, Enum, Fixed, Map, Nullability, PrimitiveType,
    Record, Schema, Type, TypeName, AVRO_ENUM_SYMBOLS_METADATA_KEY,
    AVRO_FIELD_DEFAULT_METADATA_KEY,
};
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE,
};
use indexmap::IndexMap;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

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
    /// Represents an unsupported literal type.
    Unsupported,
}

/// Contains the necessary information to resolve a writer's record against a reader's record schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedRecord {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Promotion {
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

/// Holds the mapping information for resolving Avro enums.
///
/// When resolving schemas, the writer's enum symbols must be mapped to the reader's symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumMapping {
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
pub struct AvroDataType {
    nullability: Option<Nullability>,
    metadata: HashMap<String, String>,
    codec: Codec,
    pub(crate) resolution: Option<ResolutionInfo>,
}

impl AvroDataType {
    /// Create a new [`AvroDataType`] with the given parts.
    pub fn new(
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
    pub fn field_with_name(&self, name: &str) -> Field {
        let nullable = self.nullability.is_some();
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
    pub fn codec(&self) -> &Codec {
        &self.codec
    }

    /// Returns the nullability status of this data type
    ///
    /// In Avro, nullability is represented through unions with null types.
    /// The returned value indicates how nulls are encoded in the Avro format:
    /// - `Some(Nullability::NullFirst)` - Nulls are encoded as the first union variant
    /// - `Some(Nullability::NullSecond)` - Nulls are encoded as the second union variant
    /// - `None` - The type is not nullable
    pub fn nullability(&self) -> Option<Nullability> {
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
                ))
            }
            Codec::Boolean => match default_json {
                Value::Bool(b) => AvroLiteral::Boolean(*b),
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Boolean default must be a JSON boolean".into(),
                    ))
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
            | Codec::TimestampMicros(_) => AvroLiteral::Long(parse_json_i64(default_json, "long")?),
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
                        "Default value must be a JSON array for Avro array type".into(),
                    ))
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
                        "Default value must be a JSON object for Avro map type".into(),
                    ))
                }
            },
            Codec::Struct(fields) => match default_json {
                Value::Object(obj) => {
                    let mut out: IndexMap<String, AvroLiteral> =
                        IndexMap::with_capacity(fields.len());
                    for f in fields.as_ref() {
                        let name = f.name().to_string();
                        if let Some(sub) = obj.get(&name) {
                            // Explicit value provided in the record default object
                            let lit = f.data_type().parse_default_literal(sub)?;
                            out.insert(name, lit);
                        } else if let Some(default_json) =
                            f.data_type().metadata.get(AVRO_FIELD_DEFAULT_METADATA_KEY)
                        {
                            // Use the subfield's own stored default (validate and parse)
                            let v: Value = serde_json::from_str(default_json).map_err(|e| {
                                ArrowError::SchemaError(format!(
                                    "Failed to parse stored subfield default JSON for '{}': {e}",
                                    f.name(),
                                ))
                            })?;
                            let lit = f.data_type().parse_default_literal(&v)?;
                            out.insert(name, lit);
                        } else if f.data_type().nullability() == Some(Nullability::NullFirst) {
                            // Only a NullFirst union may implicitly supply null for the missing subfield
                            out.insert(name, AvroLiteral::Null);
                        } else {
                            return Err(ArrowError::SchemaError(format!(
                                "Record default missing required subfield '{}' with non-nullable type {:?}",
                                f.name(),
                                f.data_type().codec()
                            )));
                        }
                    }
                    AvroLiteral::Map(out)
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Default value for record/struct must be a JSON object".into(),
                    ))
                }
            },
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

    fn validate_and_store_default(
        &mut self,
        default_json: &Value,
    ) -> Result<AvroLiteral, ArrowError> {
        let lit = self.parse_default_literal(default_json)?;
        self.store_default(default_json)?;
        Ok(lit)
    }
}

/// A named [`AvroDataType`]
#[derive(Debug, Clone, PartialEq)]
pub struct AvroField {
    name: String,
    data_type: AvroDataType,
}

impl AvroField {
    /// Returns the arrow [`Field`]
    pub fn field(&self) -> Field {
        self.data_type.field_with_name(&self.name)
    }

    /// Returns the [`AvroDataType`]
    pub fn data_type(&self) -> &AvroDataType {
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
    pub fn with_utf8view(&self) -> Self {
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
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Performs schema resolution between a writer and reader schema.
    ///
    /// This is the primary entry point for handling schema evolution. It produces an
    /// `AvroField` that contains all the necessary information to read data written
    /// with the `writer` schema as if it were written with the `reader` schema.
    pub(crate) fn resolve_from_writer_and_reader<'a>(
        writer_schema: &'a Schema<'a>,
        reader_schema: &'a Schema<'a>,
        use_utf8view: bool,
        strict_mode: bool,
    ) -> Result<Self, ArrowError> {
        let top_name = match reader_schema {
            Schema::Complex(ComplexType::Record(r)) => r.name.to_string(),
            _ => "root".to_string(),
        };
        let mut resolver = Maker::new(use_utf8view, strict_mode);
        let data_type = resolver.make_data_type(writer_schema, Some(reader_schema), None)?;
        Ok(Self {
            name: top_name,
            data_type,
        })
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
pub struct AvroFieldBuilder<'a> {
    writer_schema: &'a Schema<'a>,
    reader_schema: Option<&'a Schema<'a>>,
    use_utf8view: bool,
    strict_mode: bool,
}

impl<'a> AvroFieldBuilder<'a> {
    /// Creates a new [`AvroFieldBuilder`] for a given writer schema.
    pub fn new(writer_schema: &'a Schema<'a>) -> Self {
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
    pub fn with_reader_schema(mut self, reader_schema: &'a Schema<'a>) -> Self {
        self.reader_schema = Some(reader_schema);
        self
    }

    /// Enable or disable Utf8View support
    pub fn with_utf8view(mut self, use_utf8view: bool) -> Self {
        self.use_utf8view = use_utf8view;
        self
    }

    /// Enable or disable strict mode.
    pub fn with_strict_mode(mut self, strict_mode: bool) -> Self {
        self.strict_mode = strict_mode;
        self
    }

    /// Build an [`AvroField`] from the builder
    pub fn build(self) -> Result<AvroField, ArrowError> {
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
pub enum Codec {
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
    /// Represents Avro fixed type, maps to Arrow's FixedSizeBinary data type
    /// The i32 parameter indicates the fixed binary size
    Fixed(i32),
    /// Represents Avro decimal type, maps to Arrow's Decimal128 or Decimal256 data types
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
            Self::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Fixed(size) => DataType::FixedSizeBinary(*size),
            Self::Decimal(precision, scale, size) => {
                let p = *precision as u8;
                let s = scale.unwrap_or(0) as i8;
                let too_large_for_128 = match *size {
                    Some(sz) => sz > 16,
                    None => {
                        (p as usize) > DECIMAL128_MAX_PRECISION as usize
                            || (s as usize) > DECIMAL128_MAX_SCALE as usize
                    }
                };
                if too_large_for_128 {
                    DataType::Decimal256(p, s)
                } else {
                    DataType::Decimal128(p, s)
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
                let val_dt = value_type.codec.data_type();
                let val_field = Field::new("value", val_dt, value_type.nullability.is_some())
                    .with_metadata(value_type.metadata.clone());
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
        }
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
    Ok((precision, scale, size))
}

impl Codec {
    /// Converts a string codec to use Utf8View if requested
    ///
    /// The conversion only happens if both:
    /// 1. `use_utf8view` is true
    /// 2. The codec is currently `Utf8`
    ///
    /// # Example
    /// ```
    /// # use arrow_avro::codec::Codec;
    /// let utf8_codec1 = Codec::Utf8;
    /// let utf8_codec2 = Codec::Utf8;
    ///
    /// // Convert to Utf8View
    /// let view_codec = utf8_codec1.with_utf8view(true);
    /// assert!(matches!(view_codec, Codec::Utf8View));
    ///
    /// // Don't convert if use_utf8view is false
    /// let unchanged_codec = utf8_codec2.with_utf8view(false);
    /// assert!(matches!(unchanged_codec, Codec::Utf8));
    /// ```
    pub fn with_utf8view(self, use_utf8view: bool) -> Self {
        if use_utf8view && matches!(self, Self::Utf8) {
            Self::Utf8View
        } else {
            self
        }
    }
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

fn names_match(
    writer_name: &str,
    writer_aliases: &[&str],
    reader_name: &str,
    reader_aliases: &[&str],
) -> bool {
    writer_name == reader_name
        || reader_aliases.contains(&writer_name)
        || writer_aliases.contains(&reader_name)
}

fn ensure_names_match(
    data_type: &str,
    writer_name: &str,
    writer_aliases: &[&str],
    reader_name: &str,
    reader_aliases: &[&str],
) -> Result<(), ArrowError> {
    if names_match(writer_name, writer_aliases, reader_name, reader_aliases) {
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

    /// Parses a [`AvroDataType`] from the provided [`Schema`] and the given `name` and `namespace`
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
                // Special case the common case of nullable primitives
                let null = f
                    .iter()
                    .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)));
                match (f.len() == 2, null) {
                    (true, Some(0)) => {
                        let mut field = self.parse_type(&f[1], namespace)?;
                        field.nullability = Some(Nullability::NullFirst);
                        Ok(field)
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
                        Ok(field)
                    }
                    _ => Err(ArrowError::NotYetImplemented(format!(
                        "Union of {f:?} not currently supported"
                    ))),
                }
            }
            Schema::Complex(c) => match c {
                ComplexType::Record(r) => {
                    let namespace = r.namespace.or(namespace);
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
                    let field = AvroDataType {
                        nullability: None,
                        codec: Codec::Struct(fields),
                        metadata: r.attributes.field_metadata(),
                        resolution: None,
                    };
                    self.resolver.register(r.name, namespace, field.clone());
                    Ok(field)
                }
                ComplexType::Array(a) => {
                    let mut field = self.parse_type(a.items.as_ref(), namespace)?;
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
                    let md = f.attributes.field_metadata();
                    let field = match f.attributes.logical_type {
                        Some("decimal") => {
                            let (precision, scale, _) =
                                parse_decimal_attributes(&f.attributes, Some(size as usize), true)?;
                            AvroDataType {
                                nullability: None,
                                metadata: md,
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
                                metadata: md,
                                codec: Codec::Interval,
                                resolution: None,
                            }
                        }
                        _ => AvroDataType {
                            nullability: None,
                            metadata: md,
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
                    (Some("uuid"), c @ Codec::Utf8) => *c = Codec::Uuid,
                    (Some(logical), _) => {
                        // Insert unrecognized logical type into metadata map
                        field.metadata.insert("logicalType".into(), logical.into());
                    }
                    (None, _) => {}
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
            (Schema::Union(writer_variants), Schema::Union(reader_variants)) => self
                .resolve_nullable_union(
                    writer_variants.as_slice(),
                    reader_variants.as_slice(),
                    namespace,
                ),
            (Schema::TypeName(TypeName::Ref(_)), _) => self.parse_type(reader_schema, namespace),
            (_, Schema::TypeName(TypeName::Ref(_))) => self.parse_type(reader_schema, namespace),
            _ => Err(ArrowError::NotYetImplemented(
                "Other resolutions not yet implemented".to_string(),
            )),
        }
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
            &writer_fixed.aliases,
            reader_fixed.name,
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
                )))
            }
        };
        let mut datatype = self.parse_type(reader_schema, None)?;
        datatype.resolution = Some(ResolutionInfo::Promotion(promotion));
        Ok(datatype)
    }

    fn resolve_nullable_union<'s>(
        &mut self,
        writer_variants: &'s [Schema<'a>],
        reader_variants: &'s [Schema<'a>],
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        match (
            nullable_union_variants(writer_variants),
            nullable_union_variants(reader_variants),
        ) {
            (Some((_, write_nonnull)), Some((read_nb, read_nonnull))) => {
                let mut dt = self.make_data_type(write_nonnull, Some(read_nonnull), namespace)?;
                // Adopt reader union null ordering
                dt.nullability = Some(read_nb);
                Ok(dt)
            }
            _ => Err(ArrowError::NotYetImplemented(
                "Union resolution requires both writer and reader to be 2-branch nullable unions"
                    .to_string(),
            )),
        }
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
            &writer_enum.aliases,
            reader_enum.name,
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

    fn resolve_records(
        &mut self,
        writer_record: &Record<'a>,
        reader_record: &Record<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        ensure_names_match(
            "Record",
            writer_record.name,
            &writer_record.aliases,
            reader_record.name,
            &reader_record.aliases,
        )?;
        let writer_ns = writer_record.namespace.or(namespace);
        let reader_ns = reader_record.namespace.or(namespace);
        let reader_md = reader_record.attributes.field_metadata();
        let writer_index_map: HashMap<&str, usize> = writer_record
            .fields
            .iter()
            .enumerate()
            .map(|(idx, wf)| (wf.name, idx))
            .collect();
        let mut writer_to_reader: Vec<Option<usize>> = vec![None; writer_record.fields.len()];
        let reader_fields: Vec<AvroField> = reader_record
            .fields
            .iter()
            .enumerate()
            .map(|(reader_idx, r_field)| -> Result<AvroField, ArrowError> {
                if let Some(&writer_idx) = writer_index_map.get(r_field.name) {
                    let w_schema = &writer_record.fields[writer_idx].r#type;
                    let dt = self.make_data_type(w_schema, Some(&r_field.r#type), reader_ns)?;
                    writer_to_reader[writer_idx] = Some(reader_idx);
                    Ok(AvroField {
                        name: r_field.name.to_string(),
                        data_type: dt,
                    })
                } else {
                    let mut dt = self.parse_type(&r_field.r#type, reader_ns)?;
                    match r_field.default.as_ref() {
                        Some(default_json) => {
                            dt.resolution = Some(ResolutionInfo::DefaultValue(
                                dt.validate_and_store_default(default_json)?,
                            ));
                        }
                        None => {
                            if dt.nullability() == Some(Nullability::NullFirst) {
                                dt.resolution = Some(ResolutionInfo::DefaultValue(
                                    dt.validate_and_store_default(&Value::Null)?,
                                ));
                            } else {
                                return Err(ArrowError::SchemaError(format!(
                                    "Reader field '{}' not present in writer schema must have a default value",
                                    r_field.name
                                )));
                            }
                        }
                    }
                    Ok(AvroField {
                        name: r_field.name.to_string(),
                        data_type: dt,
                    })
                }
            })
            .collect::<Result<_, _>>()?;
        let default_fields: Vec<usize> = reader_fields
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                matches!(
                    field.data_type().resolution,
                    Some(ResolutionInfo::DefaultValue(_))
                )
                .then_some(index)
            })
            .collect();
        let skip_fields: Vec<Option<AvroDataType>> = writer_record
            .fields
            .iter()
            .enumerate()
            .map(|(writer_index, writer_field)| {
                if writer_to_reader[writer_index].is_some() {
                    Ok(None)
                } else {
                    self.parse_type(&writer_field.r#type, writer_ns).map(Some)
                }
            })
            .collect::<Result<_, ArrowError>>()?;
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
        // Register a resolved record by reader name+namespace for potential named type refs
        self.resolver
            .register(reader_record.name, reader_ns, resolved.clone());
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Attributes, Fixed, PrimitiveType, Schema, Type, TypeName};
    use serde_json;

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

    fn create_fixed_schema(size: usize, logical_type: &'static str) -> Schema<'static> {
        let attributes = Attributes {
            logical_type: Some(logical_type),
            additional: Default::default(),
        };

        Schema::Complex(ComplexType::Fixed(Fixed {
            name: "fixed_type",
            namespace: None,
            aliases: Vec::new(),
            size,
            attributes,
        }))
    }

    fn resolve_promotion(writer: PrimitiveType, reader: PrimitiveType) -> AvroDataType {
        let writer_schema = Schema::TypeName(TypeName::Primitive(writer));
        let reader_schema = Schema::TypeName(TypeName::Primitive(reader));
        let mut maker = Maker::new(false, false);
        maker
            .make_data_type(&writer_schema, Some(&reader_schema), None)
            .expect("promotion should resolve")
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
        let mut codec = Codec::Fixed(16);

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
    fn test_promotion_within_nullable_union_keeps_reader_null_ordering() {
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
        assert_eq!(result.nullability, Some(Nullability::NullSecond));
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
        let lit = dt_null.validate_and_store_default(&Value::Null).unwrap();
        assert_eq!(lit, AvroLiteral::Null);
        assert_default_stored(&dt_null, &Value::Null);
        let mut dt_int = AvroDataType::new(Codec::Int32, HashMap::new(), None);
        let err = dt_int.validate_and_store_default(&Value::Null).unwrap_err();
        assert!(
            err.to_string()
                .contains("JSON null default is only valid for `null` type"),
            "unexpected error: {err}"
        );
        let mut dt_int_nf =
            AvroDataType::new(Codec::Int32, HashMap::new(), Some(Nullability::NullFirst));
        let lit2 = dt_int_nf.validate_and_store_default(&Value::Null).unwrap();
        assert_eq!(lit2, AvroLiteral::Null);
        assert_default_stored(&dt_int_nf, &Value::Null);
        let mut dt_int_ns =
            AvroDataType::new(Codec::Int32, HashMap::new(), Some(Nullability::NullSecond));
        let err2 = dt_int_ns
            .validate_and_store_default(&Value::Null)
            .unwrap_err();
        assert!(
            err2.to_string()
                .contains("JSON null default is only valid for `null` type"),
            "unexpected error: {err2}"
        );
    }

    #[test]
    fn test_validate_and_store_default_primitives_and_temporal() {
        let mut dt_bool = AvroDataType::new(Codec::Boolean, HashMap::new(), None);
        let lit = dt_bool
            .validate_and_store_default(&Value::Bool(true))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Boolean(true));
        assert_default_stored(&dt_bool, &Value::Bool(true));
        let mut dt_i32 = AvroDataType::new(Codec::Int32, HashMap::new(), None);
        let lit = dt_i32
            .validate_and_store_default(&serde_json::json!(123))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Int(123));
        assert_default_stored(&dt_i32, &serde_json::json!(123));
        let err = dt_i32
            .validate_and_store_default(&serde_json::json!(i64::from(i32::MAX) + 1))
            .unwrap_err();
        assert!(format!("{err}").contains("out of i32 range"));
        let mut dt_i64 = AvroDataType::new(Codec::Int64, HashMap::new(), None);
        let lit = dt_i64
            .validate_and_store_default(&serde_json::json!(1234567890))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Long(1234567890));
        assert_default_stored(&dt_i64, &serde_json::json!(1234567890));
        let mut dt_f32 = AvroDataType::new(Codec::Float32, HashMap::new(), None);
        let lit = dt_f32
            .validate_and_store_default(&serde_json::json!(1.25))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Float(1.25));
        assert_default_stored(&dt_f32, &serde_json::json!(1.25));
        let err = dt_f32
            .validate_and_store_default(&serde_json::json!(1e39))
            .unwrap_err();
        assert!(format!("{err}").contains("out of f32 range"));
        let mut dt_f64 = AvroDataType::new(Codec::Float64, HashMap::new(), None);
        let lit = dt_f64
            .validate_and_store_default(&serde_json::json!(std::f64::consts::PI))
            .unwrap();
        assert_eq!(lit, AvroLiteral::Double(std::f64::consts::PI));
        assert_default_stored(&dt_f64, &serde_json::json!(std::f64::consts::PI));
        let mut dt_str = AvroDataType::new(Codec::Utf8, HashMap::new(), None);
        let l = dt_str
            .validate_and_store_default(&json_string("hello"))
            .unwrap();
        assert_eq!(l, AvroLiteral::String("hello".into()));
        assert_default_stored(&dt_str, &json_string("hello"));
        let mut dt_strv = AvroDataType::new(Codec::Utf8View, HashMap::new(), None);
        let l = dt_strv
            .validate_and_store_default(&json_string("view"))
            .unwrap();
        assert_eq!(l, AvroLiteral::String("view".into()));
        assert_default_stored(&dt_strv, &json_string("view"));
        let mut dt_uuid = AvroDataType::new(Codec::Uuid, HashMap::new(), None);
        let l = dt_uuid
            .validate_and_store_default(&json_string("00000000-0000-0000-0000-000000000000"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::String("00000000-0000-0000-0000-000000000000".into())
        );
        let mut dt_bin = AvroDataType::new(Codec::Binary, HashMap::new(), None);
        let l = dt_bin
            .validate_and_store_default(&json_string("ABC"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![65, 66, 67]));
        let err = dt_bin
            .validate_and_store_default(&json_string("€")) // U+20AC
            .unwrap_err();
        assert!(format!("{err}").contains("Invalid codepoint"));
        let mut dt_date = AvroDataType::new(Codec::Date32, HashMap::new(), None);
        let ld = dt_date
            .validate_and_store_default(&serde_json::json!(1))
            .unwrap();
        assert_eq!(ld, AvroLiteral::Int(1));
        let mut dt_tmill = AvroDataType::new(Codec::TimeMillis, HashMap::new(), None);
        let lt = dt_tmill
            .validate_and_store_default(&serde_json::json!(86_400_000))
            .unwrap();
        assert_eq!(lt, AvroLiteral::Int(86_400_000));
        let mut dt_tmicros = AvroDataType::new(Codec::TimeMicros, HashMap::new(), None);
        let ltm = dt_tmicros
            .validate_and_store_default(&serde_json::json!(1_000_000))
            .unwrap();
        assert_eq!(ltm, AvroLiteral::Long(1_000_000));
        let mut dt_ts_milli = AvroDataType::new(Codec::TimestampMillis(true), HashMap::new(), None);
        let l1 = dt_ts_milli
            .validate_and_store_default(&serde_json::json!(123))
            .unwrap();
        assert_eq!(l1, AvroLiteral::Long(123));
        let mut dt_ts_micro =
            AvroDataType::new(Codec::TimestampMicros(false), HashMap::new(), None);
        let l2 = dt_ts_micro
            .validate_and_store_default(&serde_json::json!(456))
            .unwrap();
        assert_eq!(l2, AvroLiteral::Long(456));
    }

    #[test]
    fn test_validate_and_store_default_fixed_decimal_interval() {
        let mut dt_fixed = AvroDataType::new(Codec::Fixed(4), HashMap::new(), None);
        let l = dt_fixed
            .validate_and_store_default(&json_string("WXYZ"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![87, 88, 89, 90]));
        let err = dt_fixed
            .validate_and_store_default(&json_string("TOO LONG"))
            .unwrap_err();
        assert!(format!("{err}").contains("Default length"));
        let mut dt_dec_fixed =
            AvroDataType::new(Codec::Decimal(10, Some(2), Some(3)), HashMap::new(), None);
        let l = dt_dec_fixed
            .validate_and_store_default(&json_string("abc"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Bytes(vec![97, 98, 99]));
        let err = dt_dec_fixed
            .validate_and_store_default(&json_string("toolong"))
            .unwrap_err();
        assert!(format!("{err}").contains("Default length"));
        let mut dt_dec_bytes =
            AvroDataType::new(Codec::Decimal(10, Some(2), None), HashMap::new(), None);
        let l = dt_dec_bytes
            .validate_and_store_default(&json_string("freeform"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::Bytes("freeform".bytes().collect::<Vec<_>>())
        );
        let mut dt_interval = AvroDataType::new(Codec::Interval, HashMap::new(), None);
        let l = dt_interval
            .validate_and_store_default(&json_string("ABCDEFGHIJKL"))
            .unwrap();
        assert_eq!(
            l,
            AvroLiteral::Bytes("ABCDEFGHIJKL".bytes().collect::<Vec<_>>())
        );
        let err = dt_interval
            .validate_and_store_default(&json_string("short"))
            .unwrap_err();
        assert!(format!("{err}").contains("Default length"));
    }

    #[test]
    fn test_validate_and_store_default_enum_list_map_struct() {
        let symbols: Arc<[String]> = ["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()]
            .into_iter()
            .collect();
        let mut dt_enum = AvroDataType::new(Codec::Enum(symbols), HashMap::new(), None);
        let l = dt_enum
            .validate_and_store_default(&json_string("GREEN"))
            .unwrap();
        assert_eq!(l, AvroLiteral::Enum("GREEN".into()));
        let err = dt_enum
            .validate_and_store_default(&json_string("YELLOW"))
            .unwrap_err();
        assert!(format!("{err}").contains("Default enum symbol"));
        let item = AvroDataType::new(Codec::Int64, HashMap::new(), None);
        let mut dt_list = AvroDataType::new(Codec::List(Arc::new(item)), HashMap::new(), None);
        let val = serde_json::json!([1, 2, 3]);
        let l = dt_list.validate_and_store_default(&val).unwrap();
        assert_eq!(
            l,
            AvroLiteral::Array(vec![
                AvroLiteral::Long(1),
                AvroLiteral::Long(2),
                AvroLiteral::Long(3)
            ])
        );
        let err = dt_list
            .validate_and_store_default(&serde_json::json!({"not":"array"}))
            .unwrap_err();
        assert!(format!("{err}").contains("JSON array"));
        let val_dt = AvroDataType::new(Codec::Float64, HashMap::new(), None);
        let mut dt_map = AvroDataType::new(Codec::Map(Arc::new(val_dt)), HashMap::new(), None);
        let mv = serde_json::json!({"x": 1.5, "y": 2.5});
        let l = dt_map.validate_and_store_default(&mv).unwrap();
        let mut expected = IndexMap::new();
        expected.insert("x".into(), AvroLiteral::Double(1.5));
        expected.insert("y".into(), AvroLiteral::Double(2.5));
        assert_eq!(l, AvroLiteral::Map(expected));
        // Not object -> error
        let err = dt_map
            .validate_and_store_default(&serde_json::json!(123))
            .unwrap_err();
        assert!(format!("{err}").contains("JSON object"));
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
        let l = dt_struct.validate_and_store_default(&default_obj).unwrap();
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
            .validate_and_store_default(&serde_json::json!({}))
            .unwrap_err();
        assert!(
            format!("{err}").contains("missing required subfield 'req'"),
            "unexpected error: {err}"
        );
        let err = dt_struct
            .validate_and_store_default(&serde_json::json!(10))
            .unwrap_err();
        assert!(format!("{err}").contains("must be a JSON object"));
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
                },
                crate::schema::Field {
                    name: "skipme",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: None,
                },
                crate::schema::Field {
                    name: "b",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                    default: None,
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
                },
                crate::schema::Field {
                    name: "a",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
                    default: None,
                },
                crate::schema::Field {
                    name: "name",
                    doc: None,
                    r#type: Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
                    default: Some(json_string("anon")),
                },
                crate::schema::Field {
                    name: "opt",
                    doc: None,
                    r#type: Schema::Union(vec![
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                        Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
                    ]),
                    default: None, // should default to null because NullFirst
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
}
