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

use crate::schema::{ComplexType, PrimitiveType, Schema, TypeName};
use arrow_schema::DataType::*;
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE,
};
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
}

/// If this is a named complex type (Record, Enum, Fixed), attach `namespace`
/// from `dt.metadata["namespace"]` if present. Otherwise, return as-is.
fn maybe_add_namespace<'a>(mut schema: Schema<'a>, dt: &'a AvroDataType) -> Schema<'a> {
    if let Some(ns_str) = dt.metadata.get("namespace") {
        if let Schema::Complex(ref mut c) = schema {
            match c {
                ComplexType::Record(r) => r.namespace = Some(ns_str),
                ComplexType::Enum(e) => e.namespace = Some(ns_str),
                ComplexType::Fixed(f) => f.namespace = Some(ns_str),
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
    Decimal(usize, Option<usize>, Option<usize>),
    Uuid,
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
    Duration,
    /// In Arrow, use Dictionary(Int32, Utf8) for Enum.
    Enum(Vec<String>),
    Map(Arc<AvroDataType>),
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
            Self::Decimal(precision, scale, size) => match size {
                Some(s) if *s > 16 => Decimal256(*precision as u8, scale.unwrap_or(0) as i8),
                Some(s) => Decimal128(*precision as u8, scale.unwrap_or(0) as i8),
                None if *precision <= DECIMAL128_MAX_PRECISION as usize
                    && scale.unwrap_or(0) <= DECIMAL128_MAX_SCALE as usize =>
                {
                    Decimal128(*precision as u8, scale.unwrap_or(0) as i8)
                }
                _ => Decimal256(*precision as u8, scale.unwrap_or(0) as i8),
            },
            // arrow-rs does not support the UUID Canonical Extension Type yet, so this is a temporary workaround.
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
                        .or(Some(0));
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
                (Some("uuid"), c @ Codec::Utf8) => *c = Codec::Uuid,
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
        // arrow-rs does not support the UUID Canonical Extension Type yet, so this mapping is not possible.
        // It is unsafe to assume all FixedSizeBinary(16) are UUIDs.
        // Uuid => Codec::Uuid,
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
                    nullability: value_field.is_nullable().then_some(Nullability::NullFirst),
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
    use arrow_schema::Field;
    use std::sync::Arc;

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
