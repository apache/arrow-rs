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
    /// Returns an arrow [`Field`] with the given name
    pub fn field_with_name(&self, name: &str) -> Field {
        let d = self.codec.data_type();
        Field::new(name, d, self.nullability.is_some()).with_metadata(self.metadata.clone())
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
    /// Represents Avro array type, maps to Arrow's List data type
    List(Arc<AvroDataType>),
    /// Represents Avro record type, maps to Arrow's Struct data type
    Struct(Arc<[AvroField]>),
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
