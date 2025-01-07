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
    Array, Attributes, ComplexType, Enum, Fixed, Map, PrimitiveType, Record, RecordField, Schema,
    TypeName,
};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType::*;
use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, SchemaBuilder, SchemaRef,
    TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE,
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

/// An Avro datatype mapped to the arrow data model
#[derive(Debug, Clone)]
pub struct AvroDataType {
    nullability: Option<Nullability>,
    metadata: HashMap<String, String>,
    codec: Codec,
}

impl AvroDataType {
    /// Create a new AvroDataType with the given parts.
    pub fn new(
        codec: Codec,
        nullability: Option<Nullability>,
        metadata: HashMap<String, String>,
    ) -> Self {
        AvroDataType {
            codec,
            nullability,
            metadata,
        }
    }

    /// Create a new AvroDataType from a `Codec`, with default (no) nullability and empty metadata.
    pub fn from_codec(codec: Codec) -> Self {
        Self::new(codec, None, Default::default())
    }

    /// Returns an arrow [`Field`] with the given name
    pub fn field_with_name(&self, name: &str) -> Field {
        let d = self.codec.data_type();
        Field::new(name, d, self.nullability.is_some()).with_metadata(self.metadata.clone())
    }

    /// Return a reference to the inner `Codec`.
    pub fn codec(&self) -> &Codec {
        &self.codec
    }

    /// Return the nullability for this Avro type, if any.
    pub fn nullability(&self) -> Option<Nullability> {
        self.nullability
    }

    /// Convert this `AvroDataType`, which encapsulates an Arrow data type (`codec`)
    /// plus nullability and metadata, back into an Avro `Schema<'a>`.
    ///
    /// - If `metadata["namespace"]` is present, we'll store it in the resulting schema for named types
    ///   (record, enum, fixed).
    pub fn to_avro_schema<'a>(&'a self, name: &'a str) -> Schema<'a> {
        let inner_schema = self.codec.to_avro_schema(name);
        // If the field is nullable in Arrow, wrap Avro schema in a union: ["null", <type>].
        if let Some(_) = self.nullability {
            Schema::Union(vec![
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                maybe_add_namespace(inner_schema, self),
            ])
        } else {
            maybe_add_namespace(inner_schema, self)
        }
    }
}

/// If this is a named complex type (Record, Enum, Fixed), attach `namespace`
/// from `dt.metadata["namespace"]` if present. Otherwise, return as-is.
fn maybe_add_namespace<'a>(mut schema: Schema<'a>, dt: &'a AvroDataType) -> Schema<'a> {
    let ns = dt.metadata.get("namespace");
    if let Some(ns_str) = ns {
        if let Schema::Complex(ref mut c) = schema {
            match c {
                ComplexType::Record(r) => {
                    r.namespace = Some(ns_str);
                }
                ComplexType::Enum(e) => {
                    e.namespace = Some(ns_str);
                }
                ComplexType::Fixed(f) => {
                    f.namespace = Some(ns_str);
                }
                // Arrays and Maps do not have a namespace field, so do nothing
                _ => {}
            }
        }
    }
    schema
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

    /// Returns the name of this field
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
                let data_type = make_data_type(schema, None, &mut resolver)?;
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

/// An Avro encoding
///
/// <https://avro.apache.org/docs/1.11.1/specification/#encodings>
#[derive(Debug, Clone)]
pub enum Codec {
    Null,
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Binary,
    Utf8,
    Date32,
    TimeMillis,
    TimeMicros,
    /// TimestampMillis(is_utc)
    TimestampMillis(bool),
    /// TimestampMicros(is_utc)
    TimestampMicros(bool),
    Fixed(i32),
    List(Arc<AvroDataType>),
    Struct(Arc<[AvroField]>),
    Interval,
    /// In Arrow, use Dictionary(Int32, Utf8) for Enum.
    Enum(Vec<String>),
    Map(Arc<AvroDataType>),
    Decimal(usize, Option<usize>, Option<usize>),
}

impl Codec {
    /// Convert this to an Arrow `DataType`
    fn data_type(&self) -> DataType {
        match self {
            Self::Null => Null,
            Self::Boolean => Boolean,
            Self::Int32 => Int32,
            Self::Int64 => Int64,
            Self::Float32 => Float32,
            Self::Float64 => Float64,
            Self::Binary => Binary,
            Self::Utf8 => Utf8,
            Self::Date32 => Date32,
            Self::TimeMillis => Time32(TimeUnit::Millisecond),
            Self::TimeMicros => Time64(TimeUnit::Microsecond),
            Self::TimestampMillis(is_utc) => {
                Timestamp(TimeUnit::Millisecond, is_utc.then(|| "+00:00".into()))
            }
            Self::TimestampMicros(is_utc) => {
                Timestamp(TimeUnit::Microsecond, is_utc.then(|| "+00:00".into()))
            }
            Self::Interval => Interval(IntervalUnit::MonthDayNano),
            Self::Fixed(size) => FixedSizeBinary(*size),
            Self::List(f) => List(Arc::new(f.field_with_name(Field::LIST_FIELD_DEFAULT_NAME))),
            Self::Struct(f) => Struct(f.iter().map(|x| x.field()).collect()),
            Self::Enum(_symbols) => {
                // Produce a Dictionary type with index = Int32, value = Utf8
                Dictionary(Box::new(Int32), Box::new(Utf8))
            }
            Self::Map(values) => Map(
                Arc::new(Field::new(
                    "entries",
                    Struct(Fields::from(vec![
                        Field::new("key", Utf8, false),
                        values.field_with_name("value"),
                    ])),
                    false,
                )),
                false,
            ),
            Self::Decimal(precision, scale, size) => match size {
                Some(s) if *s > 16 && *s <= 32 => {
                    Decimal256(*precision as u8, scale.unwrap_or(0) as i8)
                }
                Some(s) if *s <= 16 => Decimal128(*precision as u8, scale.unwrap_or(0) as i8),
                _ => {
                    // Note: Infer based on precision when size is None
                    if *precision <= DECIMAL128_MAX_PRECISION as usize
                        && scale.unwrap_or(0) <= DECIMAL128_MAX_SCALE as usize
                    {
                        Decimal128(*precision as u8, scale.unwrap_or(0) as i8)
                    } else {
                        Decimal256(*precision as u8, scale.unwrap_or(0) as i8)
                    }
                }
            },
        }
    }

    /// Convert this `Codec` variant to an Avro `Schema<'a>`.
    pub fn to_avro_schema<'a>(&'a self, name: &'a str) -> Schema<'a> {
        match self {
            Codec::Null => Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
            Codec::Boolean => Schema::TypeName(TypeName::Primitive(PrimitiveType::Boolean)),
            Codec::Int32 => Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
            Codec::Int64 => Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
            Codec::Float32 => Schema::TypeName(TypeName::Primitive(PrimitiveType::Float)),
            Codec::Float64 => Schema::TypeName(TypeName::Primitive(PrimitiveType::Double)),
            Codec::Binary => Schema::TypeName(TypeName::Primitive(PrimitiveType::Bytes)),
            Codec::Utf8 => Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
            // date32 => Avro int + logicalType=date
            Codec::Date32 => Schema::Type(crate::schema::Type {
                r#type: TypeName::Primitive(PrimitiveType::Int),
                attributes: Attributes {
                    logical_type: Some("date"),
                    additional: Default::default(),
                },
            }),
            // time-millis => Avro int with logicalType=time-millis
            Codec::TimeMillis => Schema::Type(crate::schema::Type {
                r#type: TypeName::Primitive(PrimitiveType::Int),
                attributes: Attributes {
                    logical_type: Some("time-millis"),
                    additional: Default::default(),
                },
            }),
            // time-micros => Avro long with logicalType=time-micros
            Codec::TimeMicros => Schema::Type(crate::schema::Type {
                r#type: TypeName::Primitive(PrimitiveType::Long),
                attributes: Attributes {
                    logical_type: Some("time-micros"),
                    additional: Default::default(),
                },
            }),
            // timestamp-millis => Avro long with logicalType=timestamp-millis or local-timestamp-millis
            Codec::TimestampMillis(is_utc) => {
                let lt = if *is_utc {
                    Some("timestamp-millis")
                } else {
                    Some("local-timestamp-millis")
                };
                Schema::Type(crate::schema::Type {
                    r#type: TypeName::Primitive(PrimitiveType::Long),
                    attributes: Attributes {
                        logical_type: lt,
                        additional: Default::default(),
                    },
                })
            }
            // timestamp-micros => Avro long with logicalType=timestamp-micros or local-timestamp-micros
            Codec::TimestampMicros(is_utc) => {
                let lt = if *is_utc {
                    Some("timestamp-micros")
                } else {
                    Some("local-timestamp-micros")
                };
                Schema::Type(crate::schema::Type {
                    r#type: TypeName::Primitive(PrimitiveType::Long),
                    attributes: Attributes {
                        logical_type: lt,
                        additional: Default::default(),
                    },
                })
            }
            Codec::Interval => Schema::Type(crate::schema::Type {
                r#type: TypeName::Primitive(PrimitiveType::Bytes),
                attributes: Attributes {
                    logical_type: Some("duration"),
                    additional: Default::default(),
                },
            }),
            Codec::Fixed(size) => {
                // Convert Arrow FixedSizeBinary => Avro fixed with name & size
                Schema::Complex(ComplexType::Fixed(Fixed {
                    name,
                    namespace: None,
                    aliases: vec![],
                    size: *size as usize,
                    attributes: Attributes::default(),
                }))
            }
            Codec::List(item_type) => {
                // Avro array with "items" recursively derived
                let items_schema = item_type.to_avro_schema("items");
                Schema::Complex(ComplexType::Array(Array {
                    items: Box::new(items_schema),
                    attributes: Attributes::default(),
                }))
            }
            Codec::Struct(fields) => {
                // Avro record with nested fields
                let record_fields = fields
                    .iter()
                    .map(|f| {
                        let child_schema = f.data_type().to_avro_schema(f.name());
                        AvroFieldDef {
                            name: f.name(),
                            doc: None,
                            r#type: child_schema,
                            default: None,
                        }
                    })
                    .collect();
                Schema::Complex(ComplexType::Record(Record {
                    name,
                    namespace: None,
                    doc: None,
                    aliases: vec![],
                    fields: record_fields,
                    attributes: Attributes::default(),
                }))
            }
            Codec::Enum(symbols) => {
                // If there's a namespace in metadata, we will apply it later in maybe_add_namespace.
                Schema::Complex(ComplexType::Enum(Enum {
                    name,
                    namespace: None,
                    doc: None,
                    aliases: vec![],
                    symbols: symbols.iter().map(|s| s.as_str()).collect(),
                    default: None,
                    attributes: Attributes::default(),
                }))
            }
            Codec::Map(values) => {
                let val_schema = values.to_avro_schema("values");
                Schema::Complex(ComplexType::Map(AvroMap {
                    values: Box::new(val_schema),
                    attributes: Attributes::default(),
                }))
            }
            Codec::Decimal(precision, scale, size) => {
                // If size is Some(n), produce Avro "fixed", else "bytes".
                if let Some(n) = size {
                    Schema::Complex(ComplexType::Fixed(Fixed {
                        name,
                        namespace: None,
                        aliases: vec![],
                        size: *n,
                        attributes: Attributes {
                            logical_type: Some("decimal"),
                            additional: HashMap::from([
                                ("precision", serde_json::json!(*precision)),
                                ("scale", serde_json::json!(scale.unwrap_or(0))),
                                ("size", serde_json::json!(*n)),
                            ]),
                        },
                    }))
                } else {
                    // "type":"bytes", "logicalType":"decimal"
                    Schema::Type(crate::schema::Type {
                        r#type: TypeName::Primitive(PrimitiveType::Bytes),
                        attributes: Attributes {
                            logical_type: Some("decimal"),
                            additional: HashMap::from([
                                ("precision", serde_json::json!(*precision)),
                                ("scale", serde_json::json!(scale.unwrap_or(0))),
                            ]),
                        },
                    })
                }
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
fn make_data_type<'a>(
    schema: &Schema<'a>,
    namespace: Option<&'a str>,
    resolver: &mut Resolver<'a>,
) -> Result<AvroDataType, ArrowError> {
    match schema {
        Schema::TypeName(TypeName::Primitive(p)) => Ok(AvroDataType {
            nullability: None,
            metadata: Default::default(),
            codec: (*p).into(),
        }),
        Schema::TypeName(TypeName::Ref(name)) => resolver.resolve(name, namespace),
        Schema::Union(f) => {
            // Special case the common case of nullable primitives or single-type
            let null = f
                .iter()
                .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)));
            match (f.len() == 2, null) {
                (true, Some(0)) => {
                    let mut field = make_data_type(&f[1], namespace, resolver)?;
                    field.nullability = Some(Nullability::NullFirst);
                    Ok(field)
                }
                (true, Some(1)) => {
                    let mut field = make_data_type(&f[0], namespace, resolver)?;
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
                            data_type: make_data_type(&field.r#type, namespace, resolver)?,
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
                let mut field = make_data_type(a.items.as_ref(), namespace, resolver)?;
                Ok(AvroDataType {
                    nullability: None,
                    metadata: a.attributes.field_metadata(),
                    codec: Codec::List(Arc::new(field)),
                })
            }
            ComplexType::Fixed(f) => {
                // Possibly decimal with logicalType=decimal
                let size = f.size.try_into().map_err(|e| {
                    ArrowError::ParseError(format!("Overflow converting size to i32: {e}"))
                })?;
                if let Some("decimal") = f.attributes.logical_type {
                    let precision = f
                        .attributes
                        .additional
                        .get("precision")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| {
                            ArrowError::ParseError("Decimal requires precision".to_string())
                        })?;
                    let size_val = f
                        .attributes
                        .additional
                        .get("size")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| {
                            ArrowError::ParseError("Decimal requires size".to_string())
                        })?;
                    let scale = f
                        .attributes
                        .additional
                        .get("scale")
                        .and_then(|v| v.as_u64())
                        .or_else(|| Some(0));
                    let field = AvroDataType {
                        nullability: None,
                        metadata: f.attributes.field_metadata(),
                        codec: Codec::Decimal(
                            precision as usize,
                            Some(scale.unwrap_or(0) as usize),
                            Some(size_val as usize),
                        ),
                    };
                    resolver.register(f.name, namespace, field.clone());
                    Ok(field)
                } else {
                    let field = AvroDataType {
                        nullability: None,
                        metadata: f.attributes.field_metadata(),
                        codec: Codec::Fixed(size),
                    };
                    resolver.register(f.name, namespace, field.clone());
                    Ok(field)
                }
            }
            ComplexType::Enum(e) => {
                let symbols = e
                    .symbols
                    .iter()
                    .map(|sym| sym.to_string())
                    .collect::<Vec<_>>();
                let field = AvroDataType {
                    nullability: None,
                    metadata: e.attributes.field_metadata(),
                    codec: Codec::Enum(symbols),
                };
                resolver.register(e.name, namespace, field.clone());
                Ok(field)
            }
            ComplexType::Map(m) => {
                let values_data_type = make_data_type(m.values.as_ref(), namespace, resolver)?;
                let field = AvroDataType {
                    nullability: None,
                    metadata: m.attributes.field_metadata(),
                    codec: Codec::Map(Arc::new(values_data_type)),
                };
                Ok(field)
            }
        },
        Schema::Type(t) => {
            // Possibly decimal, or other logical types
            let mut field =
                make_data_type(&Schema::TypeName(t.r#type.clone()), namespace, resolver)?;
            match (t.attributes.logical_type, &mut field.codec) {
                (Some("decimal"), c @ Codec::Fixed(_)) => {
                    *c = Codec::Decimal(
                        t.attributes
                            .additional
                            .get("precision")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(10) as usize,
                        Some(
                            t.attributes
                                .additional
                                .get("scale")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0) as usize,
                        ),
                        Some(
                            t.attributes
                                .additional
                                .get("size")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0) as usize,
                        ),
                    );
                }
                (Some("decimal"), c @ Codec::Binary) => {
                    *c = Codec::Decimal(
                        t.attributes
                            .additional
                            .get("precision")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(10) as usize,
                        Some(
                            t.attributes
                                .additional
                                .get("scale")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0) as usize,
                        ),
                        None,
                    );
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
                (Some("duration"), c @ Codec::Fixed(12)) => *c = Codec::Interval,
                (Some(logical), _) => {
                    // Insert unrecognized logical type into metadata
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

/// Convert an Arrow `Field` into an `AvroField`.
pub fn arrow_field_to_avro_field(arrow_field: &Field) -> AvroField {
    let codec = arrow_type_to_codec(arrow_field.data_type());
    let nullability = if arrow_field.is_nullable() {
        Some(Nullability::NullFirst)
    } else {
        None
    };
    let mut metadata = arrow_field.metadata().clone();
    let avro_data_type = AvroDataType {
        nullability,
        metadata,
        codec,
    };
    AvroField {
        name: arrow_field.name().clone(),
        data_type: avro_data_type,
    }
}

/// Maps an Arrow `DataType` to a `Codec`.
fn arrow_type_to_codec(dt: &DataType) -> Codec {
    match dt {
        Null => Codec::Null,
        Boolean => Codec::Boolean,
        Int8 | Int16 | Int32 => Codec::Int32,
        Int64 => Codec::Int64,
        Float32 => Codec::Float32,
        Float64 => Codec::Float64,
        Utf8 => Codec::Utf8,
        Binary | LargeBinary => Codec::Binary,
        Date32 => Codec::Date32,
        Time32(TimeUnit::Millisecond) => Codec::TimeMillis,
        Time64(TimeUnit::Microsecond) => Codec::TimeMicros,
        Timestamp(TimeUnit::Millisecond, None) => Codec::TimestampMillis(false),
        Timestamp(TimeUnit::Microsecond, None) => Codec::TimestampMicros(false),
        Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
            Codec::TimestampMillis(true)
        }
        Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            Codec::TimestampMicros(true)
        }
        FixedSizeBinary(n) => Codec::Fixed(*n),
        Decimal128(prec, scale) => Codec::Decimal(*prec as usize, Some(*scale as usize), Some(16)),
        Decimal256(prec, scale) => Codec::Decimal(*prec as usize, Some(*scale as usize), Some(32)),
        Dictionary(index_type, value_type) => {
            if let Utf8 = **value_type {
                Codec::Enum(vec![])
            } else {
                // Fallback to Utf8
                Codec::Utf8
            }
        }
        Map(field, _keys_sorted) => {
            if let Struct(child_fields) = field.data_type() {
                let value_field = &child_fields[1];
                let sub_codec = arrow_type_to_codec(value_field.data_type());
                Codec::Map(Arc::new(AvroDataType {
                    nullability: value_field.is_nullable().then(|| Nullability::NullFirst),
                    metadata: value_field.metadata().clone(),
                    codec: sub_codec,
                }))
            } else {
                Codec::Map(Arc::new(AvroDataType::from_codec(Codec::Utf8)))
            }
        }
        Struct(child_fields) => {
            let avro_fields: Vec<AvroField> = child_fields
                .iter()
                .map(|f_ref| arrow_field_to_avro_field(f_ref.as_ref()))
                .collect();
            Codec::Struct(Arc::from(avro_fields))
        }
        _ => Codec::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_decimal256_tuple_variant_fixed() {
        let c = arrow_type_to_codec(&Decimal256(60, 3));
        match c {
            Codec::Decimal(p, s, Some(32)) => {
                assert_eq!(p, 60);
                assert_eq!(s, Some(3));
            }
            _ => panic!("Expected decimal(60,3,Some(32))"),
        }
        let avro_dt = AvroDataType::from_codec(c);
        let avro_schema = avro_dt.to_avro_schema("FixedDec");
        let j = serde_json::to_value(&avro_schema).unwrap();
        let expected = json!({
            "type": "fixed",
            "name": "FixedDec",
            "aliases": [],
            "size": 32,
            "logicalType": "decimal",
            "precision": 60,
            "scale": 3
        });
        assert_eq!(j, expected);
    }

    #[test]
    fn test_decimal128_tuple_variant_fixed() {
        let c = Codec::Decimal(6, Some(2), Some(4));
        let dt = c.data_type();
        match dt {
            Decimal128(p, s) => {
                assert_eq!(p, 6);
                assert_eq!(s, 2);
            }
            _ => panic!("Expected decimal(6,2) arrow type"),
        }
        let avro_dt = AvroDataType::from_codec(c);
        let schema = avro_dt.to_avro_schema("FixedDec");
        let j = serde_json::to_value(&schema).unwrap();
        let expected = json!({
            "type": "fixed",
            "name": "FixedDec",
            "aliases": [],
            "size": 4,
            "logicalType": "decimal",
            "precision": 6,
            "scale": 2,
        });
        assert_eq!(j, expected);
    }

    #[test]
    fn test_decimal_size_decision() {
        let codec = Codec::Decimal(10, Some(3), Some(16));
        let dt = codec.data_type();
        match dt {
            Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 3);
            }
            _ => panic!("Expected Decimal128"),
        }
        let codec = Codec::Decimal(18, Some(4), Some(32));
        let dt = codec.data_type();
        match dt {
            Decimal256(precision, scale) => {
                assert_eq!(precision, 18);
                assert_eq!(scale, 4);
            }
            _ => panic!("Expected Decimal256"),
        }
        let codec = Codec::Decimal(8, Some(2), None);
        let dt = codec.data_type();
        match dt {
            Decimal128(precision, scale) => {
                assert_eq!(precision, 8);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal128"),
        }
    }

    #[test]
    fn test_avro_data_type_new_and_from_codec() {
        let dt1 = AvroDataType::new(
            Codec::Int32,
            Some(Nullability::NullFirst),
            HashMap::from([("namespace".into(), "my.ns".into())]),
        );
        let actual_str = format!("{:?}", dt1.nullability());
        let expected_str = format!("{:?}", Some(Nullability::NullFirst));
        assert_eq!(actual_str, expected_str);
        let actual_str2 = format!("{:?}", dt1.codec());
        let expected_str2 = format!("{:?}", &Codec::Int32);
        assert_eq!(actual_str2, expected_str2);
        assert_eq!(dt1.metadata.get("namespace"), Some(&"my.ns".to_string()));
        let dt2 = AvroDataType::from_codec(Codec::Float64);
        let actual_str4 = format!("{:?}", dt2.codec());
        let expected_str4 = format!("{:?}", &Codec::Float64);
        assert_eq!(actual_str4, expected_str4);
        assert!(dt2.metadata.is_empty());
    }

    #[test]
    fn test_avro_data_type_field_with_name() {
        let dt = AvroDataType::new(
            Codec::Binary,
            None,
            HashMap::from([("something".into(), "else".into())]),
        );
        let f = dt.field_with_name("bin_col");
        assert_eq!(f.name(), "bin_col");
        assert_eq!(f.data_type(), &Binary);
        assert!(!f.is_nullable());
        assert_eq!(f.metadata().get("something"), Some(&"else".to_string()));
    }

    #[test]
    fn test_avro_data_type_to_avro_schema_with_namespace_record() {
        let mut meta = HashMap::new();
        meta.insert("namespace".to_string(), "com.example".to_string());
        let fields = Arc::from(vec![
            AvroField {
                name: "id".to_string(),
                data_type: AvroDataType::from_codec(Codec::Int32),
            },
            AvroField {
                name: "label".to_string(),
                data_type: AvroDataType::new(
                    Codec::Utf8,
                    Some(Nullability::NullFirst),
                    Default::default(),
                ),
            },
        ]);
        let top_level = AvroDataType::new(Codec::Struct(fields), None, meta);
        let avro_schema = top_level.to_avro_schema("TopRecord");
        let json_val = serde_json::to_value(&avro_schema).unwrap();
        let expected = json!({
            "type": "record",
            "name": "TopRecord",
            "namespace": "com.example",
            "doc": null,
            "logicalType": null,
            "aliases": [],
            "fields": [
                { "name": "id", "doc": null, "type": "int" },
                { "name": "label", "doc": null, "type": ["null","string"] }
            ],
        });
        assert_eq!(json_val, expected);
    }

    #[test]
    fn test_avro_data_type_to_avro_schema_with_namespace_enum() {
        let mut meta = HashMap::new();
        meta.insert("namespace".to_string(), "com.example.enum".to_string());

        let enum_dt = AvroDataType::new(
            Codec::Enum(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
            None,
            meta,
        );
        let avro_schema = enum_dt.to_avro_schema("MyEnum");
        let json_val = serde_json::to_value(&avro_schema).unwrap();
        let expected = json!({
            "type": "enum",
            "name": "MyEnum",
            "logicalType": null,
            "namespace": "com.example.enum",
            "doc": null,
            "aliases": [],
            "symbols": ["A","B","C"]
        });
        assert_eq!(json_val, expected);
    }

    #[test]
    fn test_avro_data_type_to_avro_schema_with_namespace_fixed() {
        let mut meta = HashMap::new();
        meta.insert("namespace".to_string(), "com.example.fixed".to_string());
        let fixed_dt = AvroDataType::new(Codec::Fixed(8), None, meta);
        let avro_schema = fixed_dt.to_avro_schema("MyFixed");
        let json_val = serde_json::to_value(&avro_schema).unwrap();
        let expected = json!({
            "type": "fixed",
            "name": "MyFixed",
            "logicalType": null,
            "namespace": "com.example.fixed",
            "aliases": [],
            "size": 8
        });
        assert_eq!(json_val, expected);
    }

    #[test]
    fn test_avro_field() {
        let field_codec = AvroDataType::from_codec(Codec::Int64);
        let avro_field = AvroField {
            name: "long_col".to_string(),
            data_type: field_codec.clone(),
        };
        assert_eq!(avro_field.name(), "long_col");
        let actual_str = format!("{:?}", avro_field.data_type().codec());
        let expected_str = format!("{:?}", &Codec::Int64);
        assert_eq!(actual_str, expected_str, "Codec debug output mismatch");
        let arrow_field = avro_field.field();
        assert_eq!(arrow_field.name(), "long_col");
        assert_eq!(arrow_field.data_type(), &Int64);
        assert!(!arrow_field.is_nullable());
    }

    #[test]
    fn test_arrow_field_to_avro_field() {
        let arrow_field = Field::new("test_meta", Utf8, true).with_metadata(HashMap::from([(
            "namespace".to_string(),
            "arrow_meta_ns".to_string(),
        )]));
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert_eq!(avro_field.name(), "test_meta");
        let actual_str = format!("{:?}", avro_field.data_type().codec());
        let expected_str = format!("{:?}", &Codec::Utf8);
        assert_eq!(actual_str, expected_str);
        let actual_str = format!("{:?}", avro_field.data_type().nullability());
        let expected_str = format!("{:?}", Some(Nullability::NullFirst));
        assert_eq!(actual_str, expected_str);
        assert_eq!(
            avro_field.data_type().metadata.get("namespace"),
            Some(&"arrow_meta_ns".to_string())
        );
    }

    #[test]
    fn test_codec_struct() {
        let fields = Arc::from(vec![
            AvroField {
                name: "a".to_string(),
                data_type: AvroDataType::from_codec(Codec::Boolean),
            },
            AvroField {
                name: "b".to_string(),
                data_type: AvroDataType::from_codec(Codec::Float64),
            },
        ]);
        let codec = Codec::Struct(fields);
        let dt = codec.data_type();
        match dt {
            Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "a");
                assert_eq!(fields[0].data_type(), &Boolean);
                assert_eq!(fields[1].name(), "b");
                assert_eq!(fields[1].data_type(), &Float64);
            }
            _ => panic!("Expected Struct data type"),
        }
    }

    #[test]
    fn test_codec_fixedsizebinary() {
        let codec = Codec::Fixed(12);
        let dt = codec.data_type();
        match dt {
            FixedSizeBinary(n) => assert_eq!(n, 12),
            _ => panic!("Expected FixedSizeBinary(12)"),
        }
    }

    #[test]
    fn test_utc_timestamp_millis() {
        let arrow_field = Field::new(
            "utc_ts_ms",
            Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        let codec = avro_field.data_type().codec();
        assert!(
            matches!(codec, Codec::TimestampMillis(true)),
            "Expected Codec::TimestampMillis(true), got: {:?}",
            codec
        );
    }

    #[test]
    fn test_utc_timestamp_micros() {
        let arrow_field = Field::new(
            "utc_ts_us",
            Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        let codec = avro_field.data_type().codec();
        assert!(
            matches!(codec, Codec::TimestampMicros(true)),
            "Expected Codec::TimestampMicros(true), got: {:?}",
            codec
        );
    }

    #[test]
    fn test_local_timestamp_millis() {
        let arrow_field = Field::new("local_ts_ms", Timestamp(TimeUnit::Millisecond, None), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        let codec = avro_field.data_type().codec();
        assert!(
            matches!(codec, Codec::TimestampMillis(false)),
            "Expected Codec::TimestampMillis(false), got: {:?}",
            codec
        );
    }

    #[test]
    fn test_local_timestamp_micros() {
        let arrow_field = Field::new("local_ts_us", Timestamp(TimeUnit::Microsecond, None), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        let codec = avro_field.data_type().codec();
        assert!(
            matches!(codec, Codec::TimestampMicros(false)),
            "Expected Codec::TimestampMicros(false), got: {:?}",
            codec
        );
    }
}
