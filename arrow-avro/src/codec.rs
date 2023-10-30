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

use crate::schema::{ComplexType, PrimitiveType, Record, Schema, TypeName};
use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, IntervalUnit, SchemaBuilder, SchemaRef, TimeUnit,
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
enum Nulls {
    /// The nulls are encoded as the first union variant
    NullFirst,
    /// The nulls are encoded as the second union variant
    NullSecond,
}

/// An Avro field mapped to the arrow data model
#[derive(Debug, Clone)]
pub struct AvroField {
    nulls: Option<Nulls>,
    meta: Arc<AvroFieldMeta>,
}

#[derive(Debug, Clone)]
struct AvroFieldMeta {
    name: String,
    metadata: HashMap<String, String>,
    codec: Codec,
}

impl AvroField {
    /// Returns the arrow [`Field`]
    pub fn field(&self) -> Field {
        let d = self.meta.codec.data_type();
        Field::new(&self.meta.name, d, self.nulls.is_some())
            .with_metadata(self.meta.metadata.clone())
    }

    /// Returns the [`Codec`]
    pub fn codec(&self) -> &Codec {
        &self.meta.codec
    }
}

impl<'a> TryFrom<&Schema<'a>> for AvroField {
    type Error = ArrowError;

    fn try_from(schema: &Schema<'a>) -> Result<Self, Self::Error> {
        let mut resolver = Resolver::default();
        make_field(schema, "item", None, &mut resolver)
    }
}

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
    /// Timestamp (is_utc)
    TimestampMillis(bool),
    TimestampMicros(bool),
    Fixed(i32),
    List(Arc<AvroField>),
    Struct(Arc<[AvroField]>),
    Duration,
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
            Self::Duration => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Fixed(size) => DataType::FixedSizeBinary(*size),
            Self::List(f) => DataType::List(Arc::new(f.field())),
            Self::Struct(f) => DataType::Struct(f.iter().map(|x| x.field()).collect()),
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

#[derive(Debug, Default)]
struct Resolver<'a> {
    map: HashMap<(&'a str, &'a str), AvroField>,
}

impl<'a> Resolver<'a> {
    fn register(&mut self, name: &'a str, namespace: Option<&'a str>, schema: AvroField) {
        self.map.insert((name, namespace.unwrap_or("")), schema);
    }

    fn resolve(&self, name: &str, namespace: Option<&'a str>) -> Result<AvroField, ArrowError> {
        let (namespace, name) = name
            .rsplit_once('.')
            .unwrap_or_else(|| (namespace.unwrap_or(""), name));

        self.map
            .get(&(namespace, name))
            .ok_or_else(|| ArrowError::ParseError(format!("Failed to resolve {namespace}.{name}")))
            .cloned()
    }
}

fn make_field<'a>(
    schema: &Schema<'a>,
    name: &'a str,
    namespace: Option<&'a str>,
    resolver: &mut Resolver<'a>,
) -> Result<AvroField, ArrowError> {
    match schema {
        Schema::TypeName(TypeName::Primitive(p)) => Ok(AvroField {
            nulls: None,
            meta: Arc::new(AvroFieldMeta {
                name: name.to_string(),
                metadata: Default::default(),
                codec: (*p).into(),
            }),
        }),
        Schema::TypeName(TypeName::Ref(name)) => resolver.resolve(name, namespace),
        Schema::Union(f) => {
            let null = f
                .iter()
                .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)));
            match (f.len() == 2, null) {
                (true, Some(0)) => {
                    let mut field = make_field(&f[1], name, namespace, resolver)?;
                    field.nulls = Some(Nulls::NullFirst);
                    Ok(field)
                }
                (true, Some(1)) => {
                    let mut field = make_field(&f[0], name, namespace, resolver)?;
                    field.nulls = Some(Nulls::NullSecond);
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
                    .map(|field| make_field(&field.r#type, field.name, namespace, resolver))
                    .collect::<Result<_, _>>()?;

                let field = AvroField {
                    nulls: None,
                    meta: Arc::new(AvroFieldMeta {
                        name: r.name.to_string(),
                        codec: Codec::Struct(fields),
                        metadata: extract_metadata(&r.attributes.additional),
                    }),
                };
                resolver.register(name, namespace, field.clone());
                Ok(field)
            }
            ComplexType::Array(a) => {
                let mut field = make_field(a.items.as_ref(), "item", namespace, resolver)?;
                Ok(AvroField {
                    nulls: None,
                    meta: Arc::new(AvroFieldMeta {
                        name: name.to_string(),
                        metadata: extract_metadata(&a.attributes.additional),
                        codec: Codec::List(Arc::new(field)),
                    }),
                })
            }
            ComplexType::Fixed(f) => {
                let size = f.size.try_into().map_err(|e| {
                    ArrowError::ParseError(format!("Overflow converting size to i32: {e}"))
                })?;

                let field = AvroField {
                    nulls: None,
                    meta: Arc::new(AvroFieldMeta {
                        name: f.name.to_string(),
                        metadata: extract_metadata(&f.attributes.additional),
                        codec: Codec::Fixed(size),
                    }),
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
            let mut field = make_field(
                &Schema::TypeName(t.r#type.clone()),
                name,
                namespace,
                resolver,
            )?;
            let meta = Arc::make_mut(&mut field.meta);

            // https://avro.apache.org/docs/1.11.1/specification/#logical-types
            match (t.attributes.logical_type, &mut meta.codec) {
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
                (Some("duration"), c @ Codec::Fixed(12)) => *c = Codec::Duration,
                (Some(logical), _) => {
                    // Insert unrecognized logical type into metadata map
                    meta.metadata.insert("logicalType".into(), logical.into());
                }
                (None, _) => {}
            }

            if !t.attributes.additional.is_empty() {
                for (k, v) in &t.attributes.additional {
                    meta.metadata.insert(k.to_string(), v.to_string());
                }
            }
            Ok(field)
        }
    }
}

fn extract_metadata(metadata: &HashMap<&str, serde_json::Value>) -> HashMap<String, String> {
    metadata
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
