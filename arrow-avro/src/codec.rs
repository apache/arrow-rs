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
    Attributes, ComplexType, PrimitiveType, Schema, TypeName, Array, Fixed, Map, Record,
    Field as AvroFieldDef
};
use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, IntervalUnit, SchemaBuilder, SchemaRef, TimeUnit,
};
use arrow_array::{ArrayRef, Int32Array, StringArray, StructArray, RecordBatch};
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
    /// This helps you construct it from outside `codec.rs` without exposing internals.
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

    pub fn from_codec(codec: Codec) -> Self {
        Self::new(codec, None, Default::default())
    }

    /// Returns an arrow [`Field`] with the given name
    pub fn field_with_name(&self, name: &str) -> Field {
        let d = self.codec.data_type();
        Field::new(name, d, self.nullability.is_some()).with_metadata(self.metadata.clone())
    }

    pub fn codec(&self) -> &Codec {
        &self.codec
    }

    pub fn nullability(&self) -> Option<Nullability> {
        self.nullability
    }

    /// Convert this `AvroDataType`, which encapsulates an Arrow data type (`codec`)
    /// plus nullability, back into an Avro `Schema<'a>`.
    pub fn to_avro_schema<'a>(&'a self, name: &'a str) -> Schema<'a> {
        let inner_schema = self.codec.to_avro_schema(name);

        // If the field is nullable in Arrow, wrap Avro schema in a union: ["null", <type>].
        // Otherwise, return the schema as-is.
        if let Some(_) = self.nullability {
            Schema::Union(vec![
                Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)),
                inner_schema,
            ])
        } else {
            inner_schema
        }
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
            Self::List(f) => {
                DataType::List(Arc::new(f.field_with_name(Field::LIST_FIELD_DEFAULT_NAME)))
            }
            Self::Struct(f) => DataType::Struct(f.iter().map(|x| x.field()).collect()),
        }
    }

    /// Convert this `Codec` variant to an Avro `Schema<'a>`.
    /// More work needed to handle `decimal`, `enum`, `map`, etc.
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

            // timestamp-millis => Avro long with logicalType=timestamp-millis
            Codec::TimestampMillis(is_utc) => {
                // TODO `is_utc` or store it in metadata
                Schema::Type(crate::schema::Type {
                    r#type: TypeName::Primitive(PrimitiveType::Long),
                    attributes: Attributes {
                        logical_type: Some("timestamp-millis"),
                        additional: Default::default(),
                    },
                })
            }

            // timestamp-micros => Avro long with logicalType=timestamp-micros
            Codec::TimestampMicros(is_utc) => {
                Schema::Type(crate::schema::Type {
                    r#type: TypeName::Primitive(PrimitiveType::Long),
                    attributes: Attributes {
                        logical_type: Some("timestamp-micros"),
                        additional: Default::default(),
                    },
                })
            }

            Codec::Interval => {
                Schema::Type(crate::schema::Type {
                    r#type: TypeName::Primitive(PrimitiveType::Bytes),
                    attributes: Attributes {
                        logical_type: Some("duration"),
                        additional: Default::default(),
                    },
                })
            }

            Codec::Fixed(size) => {
                // Convert Arrow FixedSizeBinary => Avro fixed with a known name & size
                // TODO namespace/aliases.
                Schema::Complex(ComplexType::Fixed(Fixed {
                    name,
                    namespace: None,    // TODO namespace implementation
                    aliases: vec![],    // TODO alias implementation
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
                        // For each `AvroField`, get its Avro schema
                        let child_schema = f.data_type().to_avro_schema(f.name());
                        AvroFieldDef {
                            name: f.name(),  // Avro field name
                            doc: None,
                            r#type: child_schema,
                            default: None,
                        }
                    })
                    .collect();

                Schema::Complex(ComplexType::Record(Record {
                    name,
                    namespace: None, // TODO follow up for namespace implementation
                    doc: None,
                    aliases: vec![], // TODO follow up for alias implementation
                    fields: record_fields,
                    attributes: Attributes::default(),
                }))
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
///
/// See [`Resolver`] for more information
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
            // Special case the common case of nullable primitives
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
                let size = f.size.try_into().map_err(|e| {
                    ArrowError::ParseError(format!("Overflow converting size to i32: {e}"))
                })?;

                let field = AvroDataType {
                    nullability: None,
                    metadata: f.attributes.field_metadata(),
                    codec: Codec::Fixed(size),
                };
                resolver.register(f.name, namespace, field.clone());
                Ok(field)
            }
            ComplexType::Enum(e) => Err(ArrowError::NotYetImplemented(format!(
                "Enum of {e:?} not currently supported"
            ))),
            ComplexType::Map(m) => Err(ArrowError::NotYetImplemented(format!(
                "Map of {m:?} not currently supported"
            ))),
        },
        Schema::Type(t) => {
            let mut field =
                make_data_type(&Schema::TypeName(t.r#type.clone()), namespace, resolver)?;

            // https://avro.apache.org/docs/1.11.1/specification/#logical-types
            match (t.attributes.logical_type, &mut field.codec) {
                (Some("decimal"), c @ Codec::Fixed(_)) => {
                    return Err(ArrowError::NotYetImplemented(
                        "Decimals are not currently supported".to_string(),
                    ))
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


/// Convert an Arrow `Field` into an `AvroField`.
pub(crate) fn arrow_field_to_avro_field(arrow_field: &Field) -> AvroField {
    // TODO advanced metadata logic here
    let codec = arrow_type_to_codec(arrow_field.data_type());
    // Set nullability if the Arrow field is nullable
    let nullability = if arrow_field.is_nullable() {
        Some(Nullability::NullFirst)
    } else {
        None
    };
    let avro_data_type = AvroDataType {
        nullability,
        metadata: arrow_field.metadata().clone(),
        codec,
    };
    AvroField {
        name: arrow_field.name().clone(),
        data_type: avro_data_type,
    }
}

/// Maps an Arrow `DataType` to a `Codec`:
fn arrow_type_to_codec(dt: &DataType) -> Codec {
    use arrow_schema::DataType::*;
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
        Timestamp(TimeUnit::Millisecond, _) => Codec::TimestampMillis(true),
        Timestamp(TimeUnit::Microsecond, _) => Codec::TimestampMicros(true),
        FixedSizeBinary(n) => Codec::Fixed(*n as i32),

        List(field) => {
            // Recursively create Codec for the child item
            let child_codec = arrow_type_to_codec(field.data_type());
            Codec::List(Arc::new(AvroDataType {
                nullability: None,
                metadata: Default::default(),
                codec: child_codec,
            }))
        }
        Struct(child_fields) => {
            let avro_fields: Vec<AvroField> = child_fields
                .iter()
                .map(|fref| arrow_field_to_avro_field(fref.as_ref()))
                .collect();
            Codec::Struct(Arc::from(avro_fields))
        }
        _ => {
            // TODO handle more arrow types (e.g. decimal, map, union, etc.)
            Codec::Utf8
        }
    }
}
