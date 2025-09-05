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
    Attributes, AvroSchema, ComplexType, PrimitiveType, Record, Schema, Type, TypeName,
    AVRO_ENUM_SYMBOLS_METADATA_KEY,
};
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE,
};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

/// Avro types are not nullable, with nullability instead encoded as a union
/// where one of the variants is the null type.
///
/// To accommodate this we special case two-variant unions where one of the
/// variants is the null type, and use this to derive arrow's notion of nullability
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Nullability {
    /// The nulls are encoded as the first union variant
    NullFirst,
    /// The nulls are encoded as the second union variant
    NullSecond,
}

/// Contains information about how to resolve differences between a writer's and a reader's schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ResolutionInfo {
    /// Indicates that the writer's type should be promoted to the reader's type.
    Promotion(Promotion),
    /// Indicates that a default value should be used for a field. (Implemented in a Follow-up PR)
    DefaultValue(AvroLiteral),
    /// Provides mapping information for resolving enums. (Implemented in a Follow-up PR)
    EnumMapping(EnumMapping),
    /// Provides resolution information for record fields. (Implemented in a Follow-up PR)
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
        match (writer_schema, reader_schema) {
            (
                Schema::TypeName(TypeName::Primitive(writer_primitive)),
                Schema::TypeName(TypeName::Primitive(reader_primitive)),
            ) => self.resolve_primitives(*writer_primitive, *reader_primitive, reader_schema),
            (
                Schema::Type(Type {
                    r#type: TypeName::Primitive(writer_primitive),
                    ..
                }),
                Schema::Type(Type {
                    r#type: TypeName::Primitive(reader_primitive),
                    ..
                }),
            ) => self.resolve_primitives(*writer_primitive, *reader_primitive, reader_schema),
            (
                Schema::TypeName(TypeName::Primitive(writer_primitive)),
                Schema::Type(Type {
                    r#type: TypeName::Primitive(reader_primitive),
                    ..
                }),
            ) => self.resolve_primitives(*writer_primitive, *reader_primitive, reader_schema),
            (
                Schema::Type(Type {
                    r#type: TypeName::Primitive(writer_primitive),
                    ..
                }),
                Schema::TypeName(TypeName::Primitive(reader_primitive)),
            ) => self.resolve_primitives(*writer_primitive, *reader_primitive, reader_schema),
            (
                Schema::Complex(ComplexType::Record(writer_record)),
                Schema::Complex(ComplexType::Record(reader_record)),
            ) => self.resolve_records(writer_record, reader_record, namespace),
            (Schema::Union(writer_variants), Schema::Union(reader_variants)) => {
                self.resolve_nullable_union(writer_variants, reader_variants, namespace)
            }
            // if both sides are the same complex kind (non-record), adopt the reader type.
            // This aligns with Avro spec: arrays, maps, and enums resolve recursively;
            // for identical shapes we can just parse the reader schema.
            (Schema::Complex(ComplexType::Array(_)), Schema::Complex(ComplexType::Array(_)))
            | (Schema::Complex(ComplexType::Map(_)), Schema::Complex(ComplexType::Map(_)))
            | (Schema::Complex(ComplexType::Fixed(_)), Schema::Complex(ComplexType::Fixed(_)))
            | (Schema::Complex(ComplexType::Enum(_)), Schema::Complex(ComplexType::Enum(_))) => {
                self.parse_type(reader_schema, namespace)
            }
            // Named-type references (equal on both sides) – parse reader side.
            (Schema::TypeName(TypeName::Ref(_)), Schema::TypeName(TypeName::Ref(_)))
            | (
                Schema::Type(Type {
                    r#type: TypeName::Ref(_),
                    ..
                }),
                Schema::Type(Type {
                    r#type: TypeName::Ref(_),
                    ..
                }),
            )
            | (
                Schema::TypeName(TypeName::Ref(_)),
                Schema::Type(Type {
                    r#type: TypeName::Ref(_),
                    ..
                }),
            )
            | (
                Schema::Type(Type {
                    r#type: TypeName::Ref(_),
                    ..
                }),
                Schema::TypeName(TypeName::Ref(_)),
            ) => self.parse_type(reader_schema, namespace),
            _ => Err(ArrowError::NotYetImplemented(
                "Other resolutions not yet implemented".to_string(),
            )),
        }
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

    fn resolve_nullable_union(
        &mut self,
        writer_variants: &[Schema<'a>],
        reader_variants: &[Schema<'a>],
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        // Only support unions with exactly two branches, one of which is `null` on both sides
        if writer_variants.len() != 2 || reader_variants.len() != 2 {
            return Err(ArrowError::NotYetImplemented(
                "Only 2-branch unions are supported for schema resolution".to_string(),
            ));
        }
        let is_null = |s: &Schema<'a>| {
            matches!(
                s,
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Null))
            )
        };
        let w_null_pos = writer_variants.iter().position(is_null);
        let r_null_pos = reader_variants.iter().position(is_null);
        match (w_null_pos, r_null_pos) {
            (Some(wp), Some(rp)) => {
                // Extract a non-null branch on each side
                let w_nonnull = &writer_variants[1 - wp];
                let r_nonnull = &reader_variants[1 - rp];
                // Resolve the non-null branch
                let mut dt = self.make_data_type(w_nonnull, Some(r_nonnull), namespace)?;
                // Adopt reader union null ordering
                dt.nullability = Some(match rp {
                    0 => Nullability::NullFirst,
                    1 => Nullability::NullSecond,
                    _ => unreachable!(),
                });
                Ok(dt)
            }
            _ => Err(ArrowError::NotYetImplemented(
                "Union resolution requires both writer and reader to be nullable unions"
                    .to_string(),
            )),
        }
    }

    fn resolve_records(
        &mut self,
        writer_record: &Record<'a>,
        reader_record: &Record<'a>,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        // Names must match or be aliased
        let names_match = writer_record.name == reader_record.name
            || reader_record.aliases.contains(&writer_record.name)
            || writer_record.aliases.contains(&reader_record.name);
        if !names_match {
            return Err(ArrowError::ParseError(format!(
                "Record name mismatch writer={}, reader={}",
                writer_record.name, reader_record.name
            )));
        }
        let writer_ns = writer_record.namespace.or(namespace);
        let reader_ns = reader_record.namespace.or(namespace);
        // Map writer field name -> index
        let mut writer_index_map =
            HashMap::<&str, usize>::with_capacity(writer_record.fields.len());
        for (idx, write_field) in writer_record.fields.iter().enumerate() {
            writer_index_map.insert(write_field.name, idx);
        }
        // Prepare outputs
        let mut reader_fields: Vec<AvroField> = Vec::with_capacity(reader_record.fields.len());
        let mut writer_to_reader: Vec<Option<usize>> = vec![None; writer_record.fields.len()];
        let mut skip_fields: Vec<Option<AvroDataType>> = vec![None; writer_record.fields.len()];
        //let mut default_fields: Vec<usize> = Vec::new();
        // Build reader fields and mapping
        for (reader_idx, r_field) in reader_record.fields.iter().enumerate() {
            if let Some(&writer_idx) = writer_index_map.get(r_field.name) {
                // Field exists in writer: resolve types (including promotions and union-of-null)
                let w_schema = &writer_record.fields[writer_idx].r#type;
                let resolved_dt =
                    self.make_data_type(w_schema, Some(&r_field.r#type), reader_ns)?;
                reader_fields.push(AvroField {
                    name: r_field.name.to_string(),
                    data_type: resolved_dt,
                });
                writer_to_reader[writer_idx] = Some(reader_idx);
            } else {
                return Err(ArrowError::NotYetImplemented(
                    "New fields from reader with default values not yet implemented".to_string(),
                ));
            }
        }
        // Any writer fields not mapped should be skipped
        for (writer_idx, writer_field) in writer_record.fields.iter().enumerate() {
            if writer_to_reader[writer_idx].is_none() {
                // Parse writer field type to know how to skip data
                let writer_dt = self.parse_type(&writer_field.r#type, writer_ns)?;
                skip_fields[writer_idx] = Some(writer_dt);
            }
        }
        // Implement writer-only fields to skip in Follow-up PR here
        // Build resolved record AvroDataType
        let resolved = AvroDataType::new_with_resolution(
            Codec::Struct(Arc::from(reader_fields)),
            reader_record.attributes.field_metadata(),
            None,
            Some(ResolutionInfo::Record(ResolvedRecord {
                writer_to_reader: Arc::from(writer_to_reader),
                default_fields: Arc::default(),
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
}
