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
    /// Create a new AvroDataType with the given parts.
    pub fn new(
        codec: Codec,
        nullability: Option<Nullability>,
        metadata: HashMap<String, String>,
    ) -> Self {
        AvroDataType {
            codec,
            nullability,
            metadata: Arc::new(metadata),
        }
    }

    /// Create a new AvroDataType from a `Codec`, with default (no) nullability and empty metadata.
    pub fn from_codec(codec: Codec) -> Self {
        Self::new(codec, None, Default::default())
    }

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
                let en = AvroDataType {
                    nullability: None,
                    metadata: Arc::new(e.attributes.field_metadata()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, IntervalUnit, TimeUnit};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn arrow_field_to_avro_field(field: &Field) -> AvroField {
        let codec = arrow_type_to_codec(field.data_type());
        let top_null = field.is_nullable().then_some(Nullability::NullFirst);
        let data_type = AvroDataType {
            nullability: top_null,
            metadata: Arc::new(field.metadata().clone()),
            codec,
        };
        AvroField {
            name: field.name().to_string(),
            data_type,
            default: None,
        }
    }

    fn arrow_type_to_codec(dt: &DataType) -> Codec {
        match dt {
            Null => Codec::Null,
            Boolean => Codec::Boolean,
            Int8 | Int16 | Int32 => Codec::Int32,
            Int64 => Codec::Int64,
            Float32 => Codec::Float32,
            Float64 => Codec::Float64,
            Binary | LargeBinary => Codec::Binary,
            Utf8 => Codec::String,
            Struct(fields) => {
                let avro_fields: Vec<AvroField> = fields
                    .iter()
                    .map(|fref| arrow_field_to_avro_field(fref.as_ref()))
                    .collect();
                Codec::Record(Arc::from(avro_fields))
            }
            Dictionary(dict_ty, val_ty) => {
                if let Int32 = &**dict_ty {
                    if let Utf8 = &**val_ty {
                        return Codec::Enum(Arc::from(Vec::new()), Arc::from(Vec::new()));
                    }
                }
                Codec::String
            }
            List(item_field) => {
                let item_codec = arrow_type_to_codec(item_field.data_type());
                let child_nullability = item_field.is_nullable().then_some(Nullability::NullFirst);
                let child_dt = AvroDataType {
                    codec: item_codec,
                    nullability: child_nullability,
                    metadata: Arc::new(item_field.metadata().clone()),
                };
                Codec::Array(Arc::new(child_dt))
            }
            Map(entries_field, _keys_sorted) => {
                if let Struct(struct_fields) = entries_field.data_type() {
                    let val_field = &struct_fields[1];
                    let val_codec = arrow_type_to_codec(val_field.data_type());
                    let val_nullability = val_field.is_nullable().then_some(Nullability::NullFirst);
                    let val_dt = AvroDataType {
                        codec: val_codec,
                        nullability: val_nullability,
                        metadata: Arc::new(val_field.metadata().clone()),
                    };
                    Codec::Map(Arc::new(val_dt))
                } else {
                    Codec::Map(Arc::new(AvroDataType::from_codec(Codec::String)))
                }
            }
            FixedSizeBinary(n) => Codec::Fixed(*n),
            Decimal128(p, s) => Codec::Decimal(*p as usize, Some(*s as usize), Some(16)),
            Decimal256(p, s) => Codec::Decimal(*p as usize, Some(*s as usize), Some(32)),
            Date32 => Codec::Date32,
            Time32(TimeUnit::Millisecond) => Codec::TimeMillis,
            Time64(TimeUnit::Microsecond) => Codec::TimeMicros,
            Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                Codec::TimestampMillis(true)
            }
            Timestamp(TimeUnit::Millisecond, None) => Codec::TimestampMillis(false),
            Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
                Codec::TimestampMicros(true)
            }
            Timestamp(TimeUnit::Microsecond, None) => Codec::TimestampMicros(false),
            Interval(IntervalUnit::MonthDayNano) => Codec::Duration,
            _ => Codec::String,
        }
    }

    #[test]
    fn test_skip_avro_default_null_in_metadata() {
        let dt = AvroDataType::from_codec(Codec::Int32);
        let field = AvroField {
            name: "test_col".into(),
            data_type: dt,
            default: Some(json!(null)),
        };
        let arrow_field = field.field();
        assert!(arrow_field.metadata().get("avro.default").is_none());
    }

    #[test]
    fn test_store_avro_default_nonnull_in_metadata() {
        let dt = AvroDataType::from_codec(Codec::Int32);
        let field = AvroField {
            name: "test_col".into(),
            data_type: dt,
            default: Some(json!(42)),
        };
        let arrow_field = field.field();
        let md = arrow_field.metadata();
        let got = md.get("avro.default").cloned();
        assert_eq!(got, Some("42".to_string()));
    }

    #[test]
    fn test_no_default_metadata_if_none() {
        let dt = AvroDataType::from_codec(Codec::String);
        let field = AvroField {
            name: "col".to_string(),
            data_type: dt,
            default: None,
        };
        let arrow_field = field.field();
        assert!(arrow_field.metadata().get("avro.default").is_none());
    }

    #[test]
    fn test_avro_field() {
        let field_codec = AvroDataType::from_codec(Codec::Int64);
        let avro_field = AvroField {
            name: "long_col".to_string(),
            data_type: field_codec.clone(),
            default: None,
        };
        assert_eq!(avro_field.name(), "long_col");
        let actual_str = format!("{:?}", avro_field.data_type().codec);
        let expected_str = format!("{:?}", &Codec::Int64);
        assert_eq!(actual_str, expected_str, "Codec debug output mismatch");
        let arrow_field = avro_field.field();
        assert_eq!(arrow_field.name(), "long_col");
        assert_eq!(arrow_field.data_type(), &Int64);
        assert!(!arrow_field.is_nullable());
    }

    #[test]
    fn test_avro_field_with_default() {
        let field_codec = AvroDataType::from_codec(Codec::Int32);
        let default_value = json!(123);
        let avro_field = AvroField {
            name: "int_col".to_string(),
            data_type: field_codec.clone(),
            default: Some(default_value.clone()),
        };
        let arrow_field = avro_field.field();
        let metadata = arrow_field.metadata();
        assert_eq!(
            metadata.get("avro.default").unwrap(),
            &default_value.to_string()
        );
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
    fn test_arrow_field_to_avro_field() {
        let arrow_field = Field::new("Null", Null, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Null));

        let arrow_field = Field::new("Boolean", Boolean, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Boolean));

        let arrow_field = Field::new("Int32", Int32, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Int32));

        let arrow_field = Field::new("Int64", Int64, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Int64));

        let arrow_field = Field::new("Float32", Float32, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Float32));

        let arrow_field = Field::new("Float64", Float64, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Float64));

        let arrow_field = Field::new("Binary", Binary, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Binary));

        let arrow_field = Field::new("Utf8", Utf8, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::String));

        let arrow_field = Field::new("Decimal128", Decimal128(1, 2), true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::Decimal(1, Some(2), Some(16))
        ));

        let arrow_field = Field::new("Decimal256", Decimal256(1, 2), true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::Decimal(1, Some(2), Some(32))
        ));

        let arrow_field = Field::new("Date32", Date32, true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Date32));

        let arrow_field = Field::new("Time32", Time32(TimeUnit::Millisecond), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::TimeMillis));

        let arrow_field = Field::new("Time32", Time64(TimeUnit::Microsecond), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::TimeMicros));

        let arrow_field = Field::new(
            "utc_ts_ms",
            Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::TimestampMillis(true)
        ));

        let arrow_field = Field::new(
            "utc_ts_us",
            Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::TimestampMicros(true)
        ));

        let arrow_field = Field::new("local_ts_ms", Timestamp(TimeUnit::Millisecond, None), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::TimestampMillis(false)
        ));

        let arrow_field = Field::new("local_ts_us", Timestamp(TimeUnit::Microsecond, None), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(
            avro_field.data_type().codec,
            Codec::TimestampMicros(false)
        ));

        let arrow_field = Field::new("Interval", Interval(IntervalUnit::MonthDayNano), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Duration));

        let arrow_field = Field::new(
            "Struct",
            Struct(
                vec![
                    Field::new("a", Boolean, false),
                    Field::new("b", Float64, false),
                ]
                .into(),
            ),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "a");
                assert!(matches!(fields[0].data_type().codec, Codec::Boolean));
                assert_eq!(fields[1].name(), "b");
                assert!(matches!(fields[1].data_type().codec, Codec::Float64));
            }
            _ => panic!("Expected Record data type"),
        }

        let arrow_field = Field::new(
            "DictionaryEnum",
            Dictionary(Box::new(Int32), Box::new(Utf8)),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::Enum(_, _)));

        let arrow_field = Field::new(
            "DictionaryString",
            Dictionary(Box::new(Utf8), Box::new(Boolean)),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert!(matches!(avro_field.data_type().codec, Codec::String));

        let field = Field::new("Utf8", Utf8, true);
        let arrow_field = Field::new("Array with nullable items", List(Arc::new(field)), true);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        if let Codec::Array(avro_data_type) = &avro_field.data_type().codec {
            assert_eq!(avro_data_type.nullability, Some(Nullability::NullFirst));
            assert_eq!(avro_data_type.metadata.len(), 0);
            assert!(matches!(avro_data_type.codec, Codec::String));
        } else {
            panic!("Expected Codec::Array");
        }

        let field = Field::new("Utf8", Utf8, false);
        let arrow_field = Field::new(
            "Array with non-nullable items",
            List(Arc::new(field)),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        if let Codec::Array(avro_data_type) = &avro_field.data_type().codec {
            assert!(avro_data_type.nullability.is_none());
            assert_eq!(avro_data_type.metadata.len(), 0);
            assert!(matches!(avro_data_type.codec, Codec::String));
        } else {
            panic!("Expected Codec::Array");
        }

        let entries_field = Field::new(
            "entries",
            Struct(
                vec![
                    Field::new("key", Utf8, false),
                    Field::new("value", Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        let arrow_field = Field::new(
            "Map with nullable items",
            Map(Arc::new(entries_field), true),
            true,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        if let Codec::Map(avro_data_type) = &avro_field.data_type().codec {
            assert_eq!(avro_data_type.nullability, Some(Nullability::NullFirst));
            assert_eq!(avro_data_type.metadata.len(), 0);
            assert!(matches!(avro_data_type.codec, Codec::String));
        } else {
            panic!("Expected Codec::Map");
        }

        let arrow_field = Field::new(
            "Utf8",
            Struct(
                vec![
                    Field::new("key", Utf8, false),
                    Field::new("value", Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        let arrow_field = Field::new(
            "Map with non-nullable items",
            Map(Arc::new(arrow_field), false),
            false,
        );
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        if let Codec::Map(avro_data_type) = &avro_field.data_type().codec {
            assert!(avro_data_type.nullability.is_none());
            assert_eq!(avro_data_type.metadata.len(), 0);
            assert!(matches!(avro_data_type.codec, Codec::String));
        } else {
            panic!("Expected Codec::Map");
        }
        let arrow_field = Field::new("FixedSizeBinary", FixedSizeBinary(8), false);
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        let codec = &avro_field.data_type().codec;
        assert!(matches!(codec, Codec::Fixed(8)));
    }

    #[test]
    fn test_arrow_field_to_avro_field_meta_namespace() {
        let arrow_field = Field::new("test_meta", Utf8, true).with_metadata(HashMap::from([(
            "namespace".to_string(),
            "arrow_meta_ns".to_string(),
        )]));
        let avro_field = arrow_field_to_avro_field(&arrow_field);
        assert_eq!(avro_field.name(), "test_meta");
        let actual_str = format!("{:?}", avro_field.data_type().codec);
        let expected_str = format!("{:?}", &Codec::String);
        assert_eq!(actual_str, expected_str);
        let actual_str = format!("{:?}", avro_field.data_type().nullability);
        let expected_str = format!("{:?}", Some(Nullability::NullFirst));
        assert_eq!(actual_str, expected_str);
        assert_eq!(
            avro_field.data_type().metadata.get("namespace"),
            Some(&"arrow_meta_ns".to_string())
        );
    }

    #[test]
    fn test_union_long_null() {
        let json_schema = r#"
        {
            "type": "record",
            "name": "test_long_null",
            "fields": [
                {"name": "f0", "type": ["long", "null"]}
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "f0");
                let child_dt = fields[0].data_type();
                assert_eq!(child_dt.nullability, Some(Nullability::NullSecond));
                assert!(matches!(child_dt.codec, Codec::Int64));
            }
            _ => panic!("Expected a record with a single [long,null] field"),
        }
        let mut resolver = Resolver::default();
        let top_dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &top_dt.codec {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "f0");
            let child_dt = fields[0].data_type();
            assert_eq!(child_dt.nullability, Some(Nullability::NullSecond));
            assert!(matches!(child_dt.codec, Codec::Int64));
        } else {
            panic!("Expected a record with a single [long,null] field (make_data_type)");
        }
    }

    #[test]
    fn test_union_array_of_int_null() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"test_array_int_null",
            "fields":[
                {"name":"arr","type":[{"type":"array","items":["int","null"]},"null"]}
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "arr");
                let child_dt = fields[0].data_type();
                assert_eq!(child_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Array(item_type) = &child_dt.codec {
                    assert_eq!(item_type.nullability, Some(Nullability::NullSecond));
                    assert!(matches!(item_type.codec, Codec::Int32));
                } else {
                    panic!("Expected Codec::Array for 'arr' field");
                }
            }
            _ => panic!("Expected a record with a single union array field"),
        }
        let mut resolver = Resolver::default();
        let top_dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &top_dt.codec {
            assert_eq!(fields.len(), 1);
            let arr_dt = fields[0].data_type();
            assert_eq!(arr_dt.nullability, Some(Nullability::NullSecond));
            if let Codec::Array(item_type) = &arr_dt.codec {
                assert_eq!(item_type.nullability, Some(Nullability::NullSecond));
                assert!(matches!(item_type.codec, Codec::Int32));
            } else {
                panic!("Expected Codec::Array (make_data_type)");
            }
        } else {
            panic!("Expected record (make_data_type)");
        }
    }

    #[test]
    fn test_union_nested_array_of_int_null() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"test_nested_array_int_null",
            "fields":[
                {
                    "name":"nested_arr",
                    "type":[
                        {
                            "type":"array",
                            "items":[
                                {
                                    "type":"array",
                                    "items":["int","null"]
                                },
                                "null"
                            ]
                        },
                        "null"
                    ]
                }
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "nested_arr");
                let outer_dt = fields[0].data_type();
                assert_eq!(outer_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Array(mid_dt) = &outer_dt.codec {
                    assert_eq!(mid_dt.nullability, Some(Nullability::NullSecond));
                    if let Codec::Array(inner_dt) = &mid_dt.codec {
                        assert_eq!(inner_dt.nullability, Some(Nullability::NullSecond));
                        assert!(matches!(inner_dt.codec, Codec::Int32));
                    } else {
                        panic!("Expected inner Codec::Array for nested_arr");
                    }
                } else {
                    panic!("Expected outer Codec::Array for nested_arr");
                }
            }
            _ => panic!("Expected a record with a single nested union array field"),
        }
        let mut resolver = Resolver::default();
        let top_dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &top_dt.codec {
            assert_eq!(fields.len(), 1);
            let outer_dt = fields[0].data_type();
            assert_eq!(outer_dt.nullability, Some(Nullability::NullSecond));
            if let Codec::Array(mid_dt) = &outer_dt.codec {
                assert_eq!(mid_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Array(inner_dt) = &mid_dt.codec {
                    assert_eq!(inner_dt.nullability, Some(Nullability::NullSecond));
                    assert!(matches!(inner_dt.codec, Codec::Int32));
                } else {
                    panic!("Expected inner array (make_data_type)");
                }
            } else {
                panic!("Expected outer array (make_data_type)");
            }
        } else {
            panic!("Expected record (make_data_type)");
        }
    }

    #[test]
    fn test_union_map_of_int_null() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"test_map_int_null",
            "fields":[
                {"name":"map_field","type":[{"type":"map","values":["int","null"]},"null"]}
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();

        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "map_field");
                let map_dt = fields[0].data_type();
                assert_eq!(map_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Map(value_type) = &map_dt.codec {
                    assert_eq!(value_type.nullability, Some(Nullability::NullSecond));
                    assert!(matches!(value_type.codec, Codec::Int32));
                } else {
                    panic!("Expected Codec::Map for map_field");
                }
            }
            _ => panic!("Expected a record with a single union map field"),
        }
        let mut resolver = Resolver::default();
        let top_dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &top_dt.codec {
            assert_eq!(fields.len(), 1);
            let map_dt = fields[0].data_type();
            assert_eq!(map_dt.nullability, Some(Nullability::NullSecond));
            if let Codec::Map(val_dt) = &map_dt.codec {
                assert_eq!(val_dt.nullability, Some(Nullability::NullSecond));
                assert!(matches!(val_dt.codec, Codec::Int32));
            } else {
                panic!("Expected map in make_data_type");
            }
        } else {
            panic!("Expected record in make_data_type");
        }
    }

    #[test]
    fn test_union_map_array_of_int_null() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"test_map_array_int_null",
            "fields":[
                {
                   "name":"map_arr",
                   "type":[
                      {
                         "type":"array",
                         "items":[
                            {
                               "type":"map",
                               "values":["int","null"]
                            },
                            "null"
                         ]
                      },
                      "null"
                   ]
                }
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "map_arr");
                let outer_dt = fields[0].data_type();
                assert_eq!(outer_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Array(map_dt) = &outer_dt.codec {
                    assert_eq!(map_dt.nullability, Some(Nullability::NullSecond));
                    if let Codec::Map(val_dt) = &map_dt.codec {
                        assert_eq!(val_dt.nullability, Some(Nullability::NullSecond));
                        assert!(matches!(val_dt.codec, Codec::Int32));
                    } else {
                        panic!("Expected Codec::Map for map_arr items");
                    }
                } else {
                    panic!("Expected Codec::Array for map_arr");
                }
            }
            _ => panic!("Expected a record with a single union array-of-map field"),
        }
        let mut resolver = Resolver::default();
        let top_dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &top_dt.codec {
            assert_eq!(fields.len(), 1);
            let outer_dt = fields[0].data_type();
            assert_eq!(outer_dt.nullability, Some(Nullability::NullSecond));
            if let Codec::Array(map_dt) = &outer_dt.codec {
                assert_eq!(map_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Map(val_dt) = &map_dt.codec {
                    assert_eq!(val_dt.nullability, Some(Nullability::NullSecond));
                    assert!(matches!(val_dt.codec, Codec::Int32));
                } else {
                    panic!("Expected Codec::Map in make_data_type");
                }
            } else {
                panic!("Expected Codec::Array in make_data_type");
            }
        } else {
            panic!("Expected record in make_data_type");
        }
    }

    #[test]
    fn test_union_nested_struct_out_of_spec() {
        let json_schema = r#"
        {
            "type":"record","name":"topLevelRecord","fields":[
                {"name":"nested_struct","type":[
                    {
                        "type":"record",
                        "name":"nested_struct",
                        "namespace":"topLevelRecord",
                        "fields":[
                            {"name":"A","type":["int","null"]},
                            {"name":"b","type":[{"type":"array","items":["int","null"]},"null"]}
                        ]
                    },
                    "null"
                ]}
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();
        match &avro_field.data_type().codec {
            Codec::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "nested_struct");
                let ns_dt = fields[0].data_type();
                assert_eq!(ns_dt.nullability, Some(Nullability::NullSecond));
                if let Codec::Record(nested_fields) = &ns_dt.codec {
                    assert_eq!(nested_fields.len(), 2);
                    let field_a_dt = nested_fields[0].data_type();
                    assert_eq!(field_a_dt.nullability, Some(Nullability::NullSecond));
                    assert!(matches!(field_a_dt.codec, Codec::Int32));
                } else {
                    panic!("Expected nested_struct to be a Record");
                }
            }
            _ => panic!("Expected top-level record with a single union-based nested_struct"),
        }
        let mut resolver = Resolver::default();
        let dt = super::make_data_type(&schema, None, &mut resolver).unwrap();
        if let Codec::Record(fields) = &dt.codec {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "nested_struct");
            let ns_dt = fields[0].data_type();
            assert_eq!(ns_dt.nullability, Some(Nullability::NullSecond));
            if let Codec::Record(nested_fields) = &ns_dt.codec {
                assert_eq!(nested_fields.len(), 2);
                let field_a_dt = nested_fields[0].data_type();
                assert_eq!(field_a_dt.nullability, Some(Nullability::NullSecond));
                assert!(matches!(field_a_dt.codec, Codec::Int32));
            } else {
                panic!("Expected nested_struct to be a Record (make_data_type)");
            }
        } else {
            panic!("Expected top-level record (make_data_type)");
        }
    }
}
