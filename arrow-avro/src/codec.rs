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

use crate::schema::{Attributes, ComplexType, PrimitiveType, Record, Schema, TypeName};
use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, SchemaBuilder, SchemaRef,
    TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

/// Avro types are not nullable, with nullability instead encoded as a union
/// where one of the variants is the null type.
///
/// To accommodate this we special case two-variant unions where one of the
/// variants is the null type, and use this to derive arrow's notion of nullability
#[derive(Debug, Copy, Clone)]
pub enum Nullability {
    /// The nulls are encoded as the first union variant
    NullFirst,
    /// The nulls are encoded as the second union variant
    NullSecond,
}

#[cfg(feature = "canonical_extension_types")]
fn with_extension_type(codec: &Codec, field: Field) -> Field {
    match codec {
        Codec::Uuid => field.with_extension_type(arrow_schema::extension::Uuid),
        _ => field,
    }
}

/// An Avro datatype mapped to the arrow data model
#[derive(Debug, Clone)]
pub struct AvroDataType {
    nullability: Option<Nullability>,
    metadata: HashMap<String, String>,
    codec: Codec,
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
#[derive(Debug, Clone)]
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
}

impl<'a> TryFrom<&Schema<'a>> for AvroField {
    type Error = ArrowError;

    fn try_from(schema: &Schema<'a>) -> Result<Self, Self::Error> {
        match schema {
            Schema::Complex(ComplexType::Record(r)) => {
                let mut resolver = Resolver::default();
                let data_type = make_data_type(schema, None, &mut resolver, false, false)?;
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
    schema: &'a Schema<'a>,
    use_utf8view: bool,
    strict_mode: bool,
}

impl<'a> AvroFieldBuilder<'a> {
    /// Creates a new [`AvroFieldBuilder`]
    pub fn new(schema: &'a Schema<'a>) -> Self {
        Self {
            schema,
            use_utf8view: false,
            strict_mode: false,
        }
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
        match self.schema {
            Schema::Complex(ComplexType::Record(r)) => {
                let mut resolver = Resolver::default();
                let data_type = make_data_type(
                    self.schema,
                    None,
                    &mut resolver,
                    self.use_utf8view,
                    self.strict_mode,
                )?;
                Ok(AvroField {
                    name: r.name.to_string(),
                    data_type,
                })
            }
            _ => Err(ArrowError::ParseError(format!(
                "Expected a Record schema to build an AvroField, but got {:?}",
                self.schema
            ))),
        }
    }
}
/// An Avro encoding
///
/// <https://avro.apache.org/docs/1.11.1/specification/#encodings>
#[derive(Debug, Clone)]
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
        self.map.insert((name, namespace.unwrap_or("")), schema);
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

/// Parses a [`AvroDataType`] from the provided [`Schema`] and the given `name` and `namespace`
///
/// `name`: is name used to refer to `schema` in its parent
/// `namespace`: an optional qualifier used as part of a type hierarchy
/// If the data type is a string, convert to use Utf8View if requested
///
/// This function is used during the schema conversion process to determine whether
/// string data should be represented as StringArray (default) or StringViewArray.
///
/// `use_utf8view`: if true, use Utf8View instead of Utf8 for string types
///
/// See [`Resolver`] for more information
fn make_data_type<'a>(
    schema: &Schema<'a>,
    namespace: Option<&'a str>,
    resolver: &mut Resolver<'a>,
    use_utf8view: bool,
    strict_mode: bool,
) -> Result<AvroDataType, ArrowError> {
    match schema {
        Schema::TypeName(TypeName::Primitive(p)) => {
            let codec: Codec = (*p).into();
            let codec = codec.with_utf8view(use_utf8view);
            Ok(AvroDataType {
                nullability: None,
                metadata: Default::default(),
                codec,
            })
        }
        Schema::TypeName(TypeName::Ref(name)) => resolver.resolve(name, namespace),
        Schema::Union(f) => {
            // Special case the common case of nullable primitives
            let null = f
                .iter()
                .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)));
            match (f.len() == 2, null) {
                (true, Some(0)) => {
                    let mut field =
                        make_data_type(&f[1], namespace, resolver, use_utf8view, strict_mode)?;
                    field.nullability = Some(Nullability::NullFirst);
                    Ok(field)
                }
                (true, Some(1)) => {
                    if strict_mode {
                        return Err(ArrowError::SchemaError(
                            "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
                                .to_string(),
                        ));
                    }
                    let mut field =
                        make_data_type(&f[0], namespace, resolver, use_utf8view, strict_mode)?;
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
                            data_type: make_data_type(
                                &field.r#type,
                                namespace,
                                resolver,
                                use_utf8view,
                                strict_mode,
                            )?,
                        })
                    })
                    .collect::<Result<_, ArrowError>>()?;
                let field = AvroDataType {
                    nullability: None,
                    codec: Codec::Struct(fields),
                    metadata: r.attributes.field_metadata(),
                };
                resolver.register(r.name, namespace, field.clone());
                Ok(field)
            }
            ComplexType::Array(a) => {
                let mut field = make_data_type(
                    a.items.as_ref(),
                    namespace,
                    resolver,
                    use_utf8view,
                    strict_mode,
                )?;
                Ok(AvroDataType {
                    nullability: None,
                    metadata: a.attributes.field_metadata(),
                    codec: Codec::List(Arc::new(field)),
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
                        }
                    }
                    _ => AvroDataType {
                        nullability: None,
                        metadata: md,
                        codec: Codec::Fixed(size),
                    },
                };
                resolver.register(f.name, namespace, field.clone());
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
                metadata.insert("avro.enum.symbols".to_string(), symbols_json);
                let field = AvroDataType {
                    nullability: None,
                    metadata,
                    codec: Codec::Enum(symbols),
                };
                resolver.register(e.name, namespace, field.clone());
                Ok(field)
            }
            ComplexType::Map(m) => {
                let val =
                    make_data_type(&m.values, namespace, resolver, use_utf8view, strict_mode)?;
                Ok(AvroDataType {
                    nullability: None,
                    metadata: m.attributes.field_metadata(),
                    codec: Codec::Map(Arc::new(val)),
                })
            }
        },
        Schema::Type(t) => {
            let mut field = make_data_type(
                &Schema::TypeName(t.r#type.clone()),
                namespace,
                resolver,
                use_utf8view,
                strict_mode,
            )?;

            // https://avro.apache.org/docs/1.11.1/specification/#logical-types
            match (t.attributes.logical_type, &mut field.codec) {
                (Some("decimal"), c @ Codec::Binary) => {
                    let (prec, sc, _) = parse_decimal_attributes(&t.attributes, None, false)?;
                    *c = Codec::Decimal(prec, Some(sc), None);
                }
                (Some("date"), c @ Codec::Int32) => *c = Codec::Date32,
                (Some("time-millis"), c @ Codec::Int32) => *c = Codec::TimeMillis,
                (Some("time-micros"), c @ Codec::Int64) => *c = Codec::TimeMicros,
                (Some("timestamp-millis"), c @ Codec::Int64) => *c = Codec::TimestampMillis(true),
                (Some("timestamp-micros"), c @ Codec::Int64) => *c = Codec::TimestampMicros(true),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        Attributes, ComplexType, Fixed, PrimitiveType, Record, Schema, Type, TypeName,
    };
    use serde_json;
    use std::collections::HashMap;

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

    #[test]
    fn test_date_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Int, "date");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::Date32));
    }

    #[test]
    fn test_time_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Int, "time-millis");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::TimeMillis));
    }

    #[test]
    fn test_time_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "time-micros");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::TimeMicros));
    }

    #[test]
    fn test_timestamp_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "timestamp-millis");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMillis(true)));
    }

    #[test]
    fn test_timestamp_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "timestamp-micros");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMicros(true)));
    }

    #[test]
    fn test_local_timestamp_millis_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "local-timestamp-millis");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert!(matches!(result.codec, Codec::TimestampMillis(false)));
    }

    #[test]
    fn test_local_timestamp_micros_logical_type() {
        let schema = create_schema_with_logical_type(PrimitiveType::Long, "local-timestamp-micros");

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

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

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

        assert_eq!(
            result.metadata.get("logicalType"),
            Some(&"custom-type".to_string())
        );
    }

    #[test]
    fn test_string_with_utf8view_enabled() {
        let schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, true, false).unwrap();

        assert!(matches!(result.codec, Codec::Utf8View));
    }

    #[test]
    fn test_string_without_utf8view_enabled() {
        let schema = Schema::TypeName(TypeName::Primitive(PrimitiveType::String));

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, false).unwrap();

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

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, true, false).unwrap();

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

        let mut resolver = Resolver::default();
        let result = make_data_type(&schema, None, &mut resolver, false, true);

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
}
