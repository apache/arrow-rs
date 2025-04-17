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

use crate::schema::{Attributes, ComplexType, PrimitiveType, Schema, TypeName};
use arrow_schema::DataType::*;
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Avro types are not nullable, with nullability instead encoded as a union
/// where one of the variants is the null type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Nullability {
    /// The nulls are encoded as the first union variant => `[ "null", T ]`
    NullFirst,
    /// The nulls are encoded as the second union variant => `[ T, "null" ]`
    ///
    /// **Important**: In Impala’s out-of-spec approach, branch=0 => null, branch=1 => decode T.
    /// This is reversed from the typical “standard” Avro interpretation for `[T,"null"]`.
    ///
    /// <https://issues.apache.org/jira/browse/IMPALA-635>
    NullSecond,
}

/// An Avro datatype mapped to the arrow data model
#[derive(Debug, Clone)]
pub struct AvroDataType {
    pub nullability: Option<Nullability>,
    pub metadata: Arc<HashMap<String, String>>,
    pub codec: Codec,
}

impl AvroDataType {
    /// Returns an arrow [`Field`] with the given name, applying `nullability` if present.
    pub fn field_with_name(&self, name: &str) -> Field {
        let is_nullable = self.nullability.is_some();
        let metadata = Arc::try_unwrap(self.metadata.clone()).unwrap_or_else(|arc| (*arc).clone());
        Field::new(name, self.codec.data_type(), is_nullable).with_metadata(metadata)
    }
}

/// A named [`AvroDataType`]
#[derive(Debug, Clone)]
pub struct AvroField {
    name: String,
    data_type: AvroDataType,
    default: Option<serde_json::Value>,
}

impl AvroField {
    /// Returns the arrow [`Field`]
    pub fn field(&self) -> Field {
        let mut fld = self.data_type.field_with_name(&self.name);
        if let Some(def_val) = &self.default {
            if !def_val.is_null() {
                let mut md = fld.metadata().clone();
                md.insert("avro.default".to_string(), def_val.to_string());
                fld = fld.with_metadata(md);
            }
        }
        fld
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
                Ok(Self {
                    data_type,
                    name: r.name.to_string(),
                    default: None,
                })
            }
            _ => Err(ArrowError::ParseError(format!(
                "Expected record got {schema:?}"
            ))),
        }
    }
}

/// An Avro encoding
#[derive(Debug, Clone)]
pub enum Codec {
    /// Primitive
    Null,
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Binary,
    String,
    /// Complex
    Record(Arc<[AvroField]>),
    Enum(Arc<[String]>, Arc<[i32]>),
    Array(Arc<AvroDataType>),
    Map(Arc<AvroDataType>),
    Fixed(i32),
    /// Logical
    Decimal(usize, Option<usize>, Option<usize>),
    Uuid,
    Date32,
    TimeMillis,
    TimeMicros,
    TimestampMillis(bool),
    TimestampMicros(bool),
    Duration,
}

impl Codec {
    /// Convert this to an Arrow `DataType`
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            // Primitives
            Self::Null => Null,
            Self::Boolean => Boolean,
            Self::Int32 => Int32,
            Self::Int64 => Int64,
            Self::Float32 => Float32,
            Self::Float64 => Float64,
            Self::Binary => Binary,
            Self::String => Utf8,
            Self::Record(fields) => {
                let arrow_fields: Vec<Field> = fields.iter().map(|f| f.field()).collect();
                Struct(arrow_fields.into())
            }
            Self::Enum(_, _) => Dictionary(Box::new(Int32), Box::new(Utf8)),
            Self::Array(child_type) => {
                let child_dt = child_type.codec.data_type();
                let child_md = Arc::try_unwrap(child_type.metadata.clone())
                    .unwrap_or_else(|arc| (*arc).clone());
                let child_field = Field::new(Field::LIST_FIELD_DEFAULT_NAME, child_dt, true)
                    .with_metadata(child_md);
                List(Arc::new(child_field))
            }
            Self::Map(value_type) => {
                let val_dt = value_type.codec.data_type();
                let val_md = Arc::try_unwrap(value_type.metadata.clone())
                    .unwrap_or_else(|arc| (*arc).clone());
                let val_field = Field::new("value", val_dt, true).with_metadata(val_md);
                Map(
                    Arc::new(Field::new(
                        "entries",
                        Struct(Fields::from(vec![
                            Field::new("key", Utf8, false),
                            val_field,
                        ])),
                        false,
                    )),
                    false,
                )
            }
            Self::Fixed(sz) => FixedSizeBinary(*sz),
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
                    Decimal256(p, s)
                } else {
                    Decimal128(p, s)
                }
            }
            Self::Uuid => FixedSizeBinary(16),
            Self::Date32 => Date32,
            Self::TimeMillis => Time32(TimeUnit::Millisecond),
            Self::TimeMicros => Time64(TimeUnit::Microsecond),
            Self::TimestampMillis(is_utc) => {
                Timestamp(TimeUnit::Millisecond, is_utc.then(|| "+00:00".into()))
            }
            Self::TimestampMicros(is_utc) => {
                Timestamp(TimeUnit::Microsecond, is_utc.then(|| "+00:00".into()))
            }
            Self::Duration => Interval(IntervalUnit::MonthDayNano),
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
            PrimitiveType::String => Self::String,
        }
    }
}

/// Resolves Avro type names to [`AvroDataType`]
#[derive(Default, Debug)]
struct Resolver<'a> {
    map: HashMap<(&'a str, &'a str), AvroDataType>,
}

impl<'a> Resolver<'a> {
    fn register(&mut self, name: &'a str, namespace: Option<&'a str>, dt: AvroDataType) {
        let ns = namespace.unwrap_or("");
        self.map.insert((name, ns), dt);
    }

    fn resolve(
        &self,
        full_name: &str,
        namespace: Option<&'a str>,
    ) -> Result<AvroDataType, ArrowError> {
        let (ns, nm) = match full_name.rsplit_once('.') {
            Some((a, b)) => (a, b),
            None => (namespace.unwrap_or(""), full_name),
        };
        self.map
            .get(&(nm, ns))
            .cloned()
            .ok_or_else(|| ArrowError::ParseError(format!("Failed to resolve {ns}.{nm}")))
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

/// Parses a [`AvroDataType`] from the provided [`Schema`], plus optional `namespace`.
fn make_data_type<'a>(
    schema: &Schema<'a>,
    namespace: Option<&'a str>,
    resolver: &mut Resolver<'a>,
) -> Result<AvroDataType, ArrowError> {
    match schema {
        Schema::TypeName(TypeName::Primitive(p)) => Ok(AvroDataType {
            nullability: None,
            metadata: Arc::new(Default::default()),
            codec: (*p).into(),
        }),
        Schema::TypeName(TypeName::Ref(name)) => resolver.resolve(name, namespace),
        Schema::Union(u) => {
            let null_count = u
                .iter()
                .filter(|x| *x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)))
                .count();
            if null_count == 1 && u.len() == 2 {
                let null_idx = u
                    .iter()
                    .position(|x| x == &Schema::TypeName(TypeName::Primitive(PrimitiveType::Null)))
                    .unwrap();
                let other_idx = if null_idx == 0 { 1 } else { 0 };
                let mut dt = make_data_type(&u[other_idx], namespace, resolver)?;
                dt.nullability = if null_idx == 0 {
                    Some(Nullability::NullFirst)
                } else {
                    Some(Nullability::NullSecond)
                };
                Ok(dt)
            } else {
                Err(ArrowError::NotYetImplemented(format!(
                    "Union of {u:?} not currently supported"
                )))
            }
        }

        Schema::Complex(c) => match c {
            ComplexType::Record(r) => {
                let ns = r.namespace.or(namespace);
                let fields = r
                    .fields
                    .iter()
                    .map(|f| {
                        let data_type = make_data_type(&f.r#type, ns, resolver)?;
                        Ok::<AvroField, ArrowError>(AvroField {
                            name: f.name.to_string(),
                            data_type,
                            default: f.default.clone(),
                        })
                    })
                    .collect::<Result<Vec<AvroField>, ArrowError>>()?;
                let rec = AvroDataType {
                    nullability: None,
                    metadata: Arc::new(r.attributes.field_metadata()),
                    codec: Codec::Record(Arc::from(fields)),
                };
                resolver.register(r.name, ns, rec.clone());
                Ok(rec)
            }
            ComplexType::Enum(e) => {
                // Insert "avro.enum.symbols" into metadata so we can preserve it.
                let mut md = e.attributes.field_metadata();
                if let Ok(symbols_json) = serde_json::to_string(&e.symbols) {
                    md.insert("avro.enum.symbols".to_string(), symbols_json);
                }

                let en = AvroDataType {
                    nullability: None,
                    metadata: Arc::new(md),
                    codec: Codec::Enum(
                        Arc::from(e.symbols.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
                        Arc::from(vec![]),
                    ),
                };
                resolver.register(e.name, namespace, en.clone());
                Ok(en)
            }
            ComplexType::Array(a) => {
                let child = make_data_type(&a.items, namespace, resolver)?;
                Ok(AvroDataType {
                    nullability: None,
                    metadata: Arc::new(a.attributes.field_metadata()),
                    codec: Codec::Array(Arc::new(child)),
                })
            }
            ComplexType::Map(m) => {
                let val = make_data_type(&m.values, namespace, resolver)?;
                Ok(AvroDataType {
                    nullability: None,
                    metadata: Arc::new(m.attributes.field_metadata()),
                    codec: Codec::Map(Arc::new(val)),
                })
            }
            ComplexType::Fixed(fx) => {
                let size = fx.size as i32;
                if let Some("decimal") = fx.attributes.logical_type {
                    let (precision, scale, _) =
                        parse_decimal_attributes(&fx.attributes, Some(size as usize), true)?;
                    let dec = AvroDataType {
                        nullability: None,
                        metadata: Arc::new(fx.attributes.field_metadata()),
                        codec: Codec::Decimal(precision, Some(scale), Some(size as usize)),
                    };
                    resolver.register(fx.name, namespace, dec.clone());
                    Ok(dec)
                } else {
                    let fixed_dt = AvroDataType {
                        nullability: None,
                        metadata: Arc::new(fx.attributes.field_metadata()),
                        codec: Codec::Fixed(size),
                    };
                    resolver.register(fx.name, namespace, fixed_dt.clone());
                    Ok(fixed_dt)
                }
            }
        },

        Schema::Type(t) => {
            let mut dt = make_data_type(&Schema::TypeName(t.r#type.clone()), namespace, resolver)?;
            match (t.attributes.logical_type, &mut dt.codec) {
                (Some("decimal"), Codec::Fixed(sz)) => {
                    let (prec, sc, size_opt) =
                        parse_decimal_attributes(&t.attributes, Some(*sz as usize), false)?;
                    if let Some(sz_actual) = size_opt {
                        *sz = sz_actual as i32;
                    }
                    dt.codec = Codec::Decimal(prec, Some(sc), Some(*sz as usize));
                }
                (Some("decimal"), Codec::Binary) => {
                    let (prec, sc, _) = parse_decimal_attributes(&t.attributes, None, false)?;
                    dt.codec = Codec::Decimal(prec, Some(sc), None);
                }
                (Some("uuid"), Codec::String) => {
                    dt.codec = Codec::Uuid;
                }
                (Some("date"), Codec::Int32) => {
                    dt.codec = Codec::Date32;
                }
                (Some("time-millis"), Codec::Int32) => {
                    dt.codec = Codec::TimeMillis;
                }
                (Some("time-micros"), Codec::Int64) => {
                    dt.codec = Codec::TimeMicros;
                }
                (Some("timestamp-millis"), Codec::Int64) => {
                    dt.codec = Codec::TimestampMillis(true);
                }
                (Some("timestamp-micros"), Codec::Int64) => {
                    dt.codec = Codec::TimestampMicros(true);
                }
                (Some("local-timestamp-millis"), Codec::Int64) => {
                    dt.codec = Codec::TimestampMillis(false);
                }
                (Some("local-timestamp-micros"), Codec::Int64) => {
                    dt.codec = Codec::TimestampMicros(false);
                }
                (Some("duration"), Codec::Fixed(12)) => {
                    dt.codec = Codec::Duration;
                }
                (Some(other), _) => {
                    if !dt.metadata.contains_key("logicalType") {
                        let mut arc_map = (*dt.metadata).clone();
                        arc_map.insert("logicalType".into(), other.into());
                        dt.metadata = Arc::new(arc_map);
                    }
                }
                (None, _) => {}
            }
            for (k, v) in &t.attributes.additional {
                let mut arc_map = (*dt.metadata).clone();
                arc_map.insert(k.to_string(), v.to_string());
                dt.metadata = Arc::new(arc_map);
            }
            Ok(dt)
        }
    }
}
