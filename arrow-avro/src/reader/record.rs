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

//! Avro Decoder for Arrow types.

use crate::codec::{
    AvroDataType, AvroField, AvroLiteral, Codec, Promotion, ResolutionInfo, ResolvedRecord,
    ResolvedUnion,
};
use crate::reader::cursor::AvroCursor;
use crate::schema::Nullability;
#[cfg(feature = "small_decimals")]
use arrow_array::builder::{Decimal32Builder, Decimal64Builder};
use arrow_array::builder::{Decimal128Builder, Decimal256Builder, IntervalMonthDayNanoBuilder};
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::*;
use arrow_schema::{
    ArrowError, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, Field as ArrowField,
    FieldRef, Fields, Schema as ArrowSchema, SchemaRef, UnionFields, UnionMode,
};
#[cfg(feature = "small_decimals")]
use arrow_schema::{DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION};
#[cfg(feature = "avro_custom_types")]
use arrow_select::take::{TakeOptions, take};
use std::cmp::Ordering;
use std::sync::Arc;
use strum_macros::AsRefStr;
use uuid::Uuid;

const DEFAULT_CAPACITY: usize = 1024;

/// Runtime plan for decoding reader-side `["null", T]` types.
#[derive(Clone, Copy, Debug)]
enum NullablePlan {
    /// Writer actually wrote a union (branch tag present).
    ReadTag,
    /// Writer wrote a single (non-union) value resolved to the non-null branch
    /// of the reader union; do NOT read a branch tag, but apply any promotion.
    FromSingle { promotion: Promotion },
}

/// Macro to decode a decimal payload for a given width and integer type.
macro_rules! decode_decimal {
    ($size:expr, $buf:expr, $builder:expr, $N:expr, $Int:ty) => {{
        let bytes = read_decimal_bytes_be::<{ $N }>($buf, $size)?;
        $builder.append_value(<$Int>::from_be_bytes(bytes));
    }};
}

/// Macro to finish a decimal builder into an array with precision/scale and nulls.
macro_rules! flush_decimal {
    ($builder:expr, $precision:expr, $scale:expr, $nulls:expr, $ArrayTy:ty) => {{
        let (_, vals, _) = $builder.finish().into_parts();
        let dec = <$ArrayTy>::try_new(vals, $nulls)?
            .with_precision_and_scale(*$precision as u8, $scale.unwrap_or(0) as i8)
            .map_err(|e| ArrowError::ParseError(e.to_string()))?;
        Arc::new(dec) as ArrayRef
    }};
}

/// Macro to append a default decimal value from two's-complement big-endian bytes
/// into the corresponding decimal builder, with compile-time constructed error text.
macro_rules! append_decimal_default {
    ($lit:expr, $builder:expr, $N:literal, $Int:ty, $name:literal) => {{
        match $lit {
            AvroLiteral::Bytes(b) => {
                let ext = sign_cast_to::<$N>(b)?;
                let val = <$Int>::from_be_bytes(ext);
                $builder.append_value(val);
                Ok(())
            }
            _ => Err(ArrowError::InvalidArgumentError(
                concat!(
                    "Default for ",
                    $name,
                    " must be bytes (two's-complement big-endian)"
                )
                .to_string(),
            )),
        }
    }};
}

/// Decodes avro encoded data into [`RecordBatch`]
#[derive(Debug)]
pub(crate) struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
    projector: Option<Projector>,
}

impl RecordDecoder {
    /// Creates a new [`RecordDecoder`] from the provided [`AvroDataType`] with additional options.
    ///
    /// This method allows you to customize how the Avro data is decoded into Arrow arrays.
    ///
    /// # Arguments
    /// * `data_type` - The Avro data type to decode.
    /// * `use_utf8view` - A flag indicating whether to use `Utf8View` for string types.
    ///
    /// # Errors
    /// This function will return an error if the provided `data_type` is not a `Record`.
    pub(crate) fn try_new_with_options(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        match data_type.codec() {
            Codec::Struct(reader_fields) => {
                // Build Arrow schema fields and per-child decoders
                let mut arrow_fields = Vec::with_capacity(reader_fields.len());
                let mut encodings = Vec::with_capacity(reader_fields.len());
                for avro_field in reader_fields.iter() {
                    arrow_fields.push(avro_field.field());
                    encodings.push(Decoder::try_new(avro_field.data_type())?);
                }
                let projector = match data_type.resolution.as_ref() {
                    Some(ResolutionInfo::Record(rec)) => {
                        Some(ProjectorBuilder::try_new(rec, reader_fields).build()?)
                    }
                    _ => None,
                };
                Ok(Self {
                    schema: Arc::new(ArrowSchema::new(arrow_fields)),
                    fields: encodings,
                    projector,
                })
            }
            other => Err(ArrowError::ParseError(format!(
                "Expected record got {other:?}"
            ))),
        }
    }

    /// Returns the decoder's `SchemaRef`
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decode `count` records from `buf`
    pub(crate) fn decode(&mut self, buf: &[u8], count: usize) -> Result<usize, ArrowError> {
        let mut cursor = AvroCursor::new(buf);
        match self.projector.as_mut() {
            Some(proj) => {
                for _ in 0..count {
                    proj.project_record(&mut cursor, &mut self.fields)?;
                }
            }
            None => {
                for _ in 0..count {
                    for field in &mut self.fields {
                        field.decode(&mut cursor)?;
                    }
                }
            }
        }
        Ok(cursor.position())
    }

    /// Flush the decoded records into a [`RecordBatch`]
    pub(crate) fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let arrays = self
            .fields
            .iter_mut()
            .map(|x| x.flush(None))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

#[derive(Debug)]
struct EnumResolution {
    mapping: Arc<[i32]>,
    default_index: i32,
}

#[derive(Debug, AsRefStr)]
enum Decoder {
    Null(usize),
    Boolean(BooleanBufferBuilder),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    #[cfg(feature = "avro_custom_types")]
    DurationSecond(Vec<i64>),
    #[cfg(feature = "avro_custom_types")]
    DurationMillisecond(Vec<i64>),
    #[cfg(feature = "avro_custom_types")]
    DurationMicrosecond(Vec<i64>),
    #[cfg(feature = "avro_custom_types")]
    DurationNanosecond(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Date32(Vec<i32>),
    TimeMillis(Vec<i32>),
    TimeMicros(Vec<i64>),
    TimestampMillis(bool, Vec<i64>),
    TimestampMicros(bool, Vec<i64>),
    TimestampNanos(bool, Vec<i64>),
    Int32ToInt64(Vec<i64>),
    Int32ToFloat32(Vec<f32>),
    Int32ToFloat64(Vec<f64>),
    Int64ToFloat32(Vec<f32>),
    Int64ToFloat64(Vec<f64>),
    Float32ToFloat64(Vec<f64>),
    BytesToString(OffsetBufferBuilder<i32>, Vec<u8>),
    StringToBytes(OffsetBufferBuilder<i32>, Vec<u8>),
    Binary(OffsetBufferBuilder<i32>, Vec<u8>),
    /// String data encoded as UTF-8 bytes, mapped to Arrow's StringArray
    String(OffsetBufferBuilder<i32>, Vec<u8>),
    /// String data encoded as UTF-8 bytes, but mapped to Arrow's StringViewArray
    StringView(OffsetBufferBuilder<i32>, Vec<u8>),
    Array(FieldRef, OffsetBufferBuilder<i32>, Box<Decoder>),
    Record(Fields, Vec<Decoder>, Option<Projector>),
    Map(
        FieldRef,
        OffsetBufferBuilder<i32>,
        OffsetBufferBuilder<i32>,
        Vec<u8>,
        Box<Decoder>,
    ),
    Fixed(i32, Vec<u8>),
    Enum(Vec<i32>, Arc<[String]>, Option<EnumResolution>),
    Duration(IntervalMonthDayNanoBuilder),
    Uuid(Vec<u8>),
    #[cfg(feature = "small_decimals")]
    Decimal32(usize, Option<usize>, Option<usize>, Decimal32Builder),
    #[cfg(feature = "small_decimals")]
    Decimal64(usize, Option<usize>, Option<usize>, Decimal64Builder),
    Decimal128(usize, Option<usize>, Option<usize>, Decimal128Builder),
    Decimal256(usize, Option<usize>, Option<usize>, Decimal256Builder),
    #[cfg(feature = "avro_custom_types")]
    RunEndEncoded(u8, usize, Box<Decoder>),
    Union(UnionDecoder),
    Nullable(Nullability, NullBufferBuilder, Box<Decoder>, NullablePlan),
}

impl Decoder {
    fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        if let Some(ResolutionInfo::Union(info)) = data_type.resolution.as_ref() {
            if info.writer_is_union && !info.reader_is_union {
                let mut clone = data_type.clone();
                clone.resolution = None; // Build target base decoder without Union resolution
                let target = Box::new(Self::try_new_internal(&clone)?);
                let decoder = Self::Union(
                    UnionDecoderBuilder::new()
                        .with_resolved_union(info.clone())
                        .with_target(target)
                        .build()?,
                );
                return Ok(decoder);
            }
        }
        Self::try_new_internal(data_type)
    }

    fn try_new_internal(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        // Extract just the Promotion (if any) to simplify pattern matching
        let promotion = match data_type.resolution.as_ref() {
            Some(ResolutionInfo::Promotion(p)) => Some(p),
            _ => None,
        };
        let decoder = match (data_type.codec(), promotion) {
            (Codec::Int64, Some(Promotion::IntToLong)) => {
                Self::Int32ToInt64(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Float32, Some(Promotion::IntToFloat)) => {
                Self::Int32ToFloat32(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Float64, Some(Promotion::IntToDouble)) => {
                Self::Int32ToFloat64(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Float32, Some(Promotion::LongToFloat)) => {
                Self::Int64ToFloat32(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Float64, Some(Promotion::LongToDouble)) => {
                Self::Int64ToFloat64(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Float64, Some(Promotion::FloatToDouble)) => {
                Self::Float32ToFloat64(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Utf8, Some(Promotion::BytesToString))
            | (Codec::Utf8View, Some(Promotion::BytesToString)) => Self::BytesToString(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            (Codec::Binary, Some(Promotion::StringToBytes)) => Self::StringToBytes(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            (Codec::Null, _) => Self::Null(0),
            (Codec::Boolean, _) => Self::Boolean(BooleanBufferBuilder::new(DEFAULT_CAPACITY)),
            (Codec::Int32, _) => Self::Int32(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Int64, _) => Self::Int64(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Float32, _) => Self::Float32(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Float64, _) => Self::Float64(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Binary, _) => Self::Binary(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            (Codec::Utf8, _) => Self::String(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            (Codec::Utf8View, _) => Self::StringView(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            (Codec::Date32, _) => Self::Date32(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::TimeMillis, _) => Self::TimeMillis(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::TimeMicros, _) => Self::TimeMicros(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::TimestampMillis(is_utc), _) => {
                Self::TimestampMillis(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::TimestampMicros(is_utc), _) => {
                Self::TimestampMicros(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::TimestampNanos(is_utc), _) => {
                Self::TimestampNanos(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            #[cfg(feature = "avro_custom_types")]
            (Codec::DurationNanos, _) => {
                Self::DurationNanosecond(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            #[cfg(feature = "avro_custom_types")]
            (Codec::DurationMicros, _) => {
                Self::DurationMicrosecond(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            #[cfg(feature = "avro_custom_types")]
            (Codec::DurationMillis, _) => {
                Self::DurationMillisecond(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            #[cfg(feature = "avro_custom_types")]
            (Codec::DurationSeconds, _) => {
                Self::DurationSecond(Vec::with_capacity(DEFAULT_CAPACITY))
            }
            (Codec::Fixed(sz), _) => Self::Fixed(*sz, Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Decimal(precision, scale, size), _) => {
                let p = *precision;
                let s = *scale;
                let prec = p as u8;
                let scl = s.unwrap_or(0) as i8;
                #[cfg(feature = "small_decimals")]
                {
                    if p <= DECIMAL32_MAX_PRECISION as usize {
                        let builder = Decimal32Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal32(p, s, *size, builder)
                    } else if p <= DECIMAL64_MAX_PRECISION as usize {
                        let builder = Decimal64Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal64(p, s, *size, builder)
                    } else if p <= DECIMAL128_MAX_PRECISION as usize {
                        let builder = Decimal128Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal128(p, s, *size, builder)
                    } else if p <= DECIMAL256_MAX_PRECISION as usize {
                        let builder = Decimal256Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal256(p, s, *size, builder)
                    } else {
                        return Err(ArrowError::ParseError(format!(
                            "Decimal precision {p} exceeds maximum supported"
                        )));
                    }
                }
                #[cfg(not(feature = "small_decimals"))]
                {
                    if p <= DECIMAL128_MAX_PRECISION as usize {
                        let builder = Decimal128Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal128(p, s, *size, builder)
                    } else if p <= DECIMAL256_MAX_PRECISION as usize {
                        let builder = Decimal256Builder::with_capacity(DEFAULT_CAPACITY)
                            .with_precision_and_scale(prec, scl)?;
                        Self::Decimal256(p, s, *size, builder)
                    } else {
                        return Err(ArrowError::ParseError(format!(
                            "Decimal precision {p} exceeds maximum supported"
                        )));
                    }
                }
            }
            (Codec::Interval, _) => Self::Duration(IntervalMonthDayNanoBuilder::new()),
            (Codec::List(item), _) => {
                let decoder = Self::try_new(item)?;
                Self::Array(
                    Arc::new(item.field_with_name("item")),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Box::new(decoder),
                )
            }
            (Codec::Enum(symbols), _) => {
                let res = match data_type.resolution.as_ref() {
                    Some(ResolutionInfo::EnumMapping(mapping)) => Some(EnumResolution {
                        mapping: mapping.mapping.clone(),
                        default_index: mapping.default_index,
                    }),
                    _ => None,
                };
                Self::Enum(Vec::with_capacity(DEFAULT_CAPACITY), symbols.clone(), res)
            }
            (Codec::Struct(fields), _) => {
                let mut arrow_fields = Vec::with_capacity(fields.len());
                let mut encodings = Vec::with_capacity(fields.len());
                for avro_field in fields.iter() {
                    let encoding = Self::try_new(avro_field.data_type())?;
                    arrow_fields.push(avro_field.field());
                    encodings.push(encoding);
                }
                let projector =
                    if let Some(ResolutionInfo::Record(rec)) = data_type.resolution.as_ref() {
                        Some(ProjectorBuilder::try_new(rec, fields).build()?)
                    } else {
                        None
                    };
                Self::Record(arrow_fields.into(), encodings, projector)
            }
            (Codec::Map(child), _) => {
                let val_field = child.field_with_name("value");
                let map_field = Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        ArrowField::new("key", DataType::Utf8, false),
                        val_field,
                    ])),
                    false,
                ));
                let val_dec = Self::try_new(child)?;
                Self::Map(
                    map_field,
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                    Box::new(val_dec),
                )
            }
            (Codec::Uuid, _) => Self::Uuid(Vec::with_capacity(DEFAULT_CAPACITY)),
            (Codec::Union(encodings, fields, UnionMode::Dense), _) => {
                let decoders = encodings
                    .iter()
                    .map(Self::try_new_internal)
                    .collect::<Result<Vec<_>, _>>()?;
                if fields.len() != decoders.len() {
                    return Err(ArrowError::SchemaError(format!(
                        "Union has {} fields but {} decoders",
                        fields.len(),
                        decoders.len()
                    )));
                }
                // Proactive guard: if a user provides a union with more branches than
                // a 32-bit Avro index can address, fail fast with a clear message.
                let branch_count = decoders.len();
                let max_addr = (i32::MAX as usize) + 1;
                if branch_count > max_addr {
                    return Err(ArrowError::SchemaError(format!(
                        "Union has {branch_count} branches, which exceeds the maximum addressable \
                         branches by an Avro int tag ({} + 1).",
                        i32::MAX
                    )));
                }
                let mut builder = UnionDecoderBuilder::new()
                    .with_fields(fields.clone())
                    .with_branches(decoders);
                if let Some(ResolutionInfo::Union(info)) = data_type.resolution.as_ref() {
                    if info.reader_is_union {
                        builder = builder.with_resolved_union(info.clone());
                    }
                }
                Self::Union(builder.build()?)
            }
            (Codec::Union(_, _, _), _) => {
                return Err(ArrowError::NotYetImplemented(
                    "Sparse Arrow unions are not yet supported".to_string(),
                ));
            }
            #[cfg(feature = "avro_custom_types")]
            (Codec::RunEndEncoded(values_dt, width_bits_or_bytes), _) => {
                let inner = Self::try_new(values_dt)?;
                let byte_width: u8 = match *width_bits_or_bytes {
                    2 | 4 | 8 => *width_bits_or_bytes,
                    16 => 2,
                    32 => 4,
                    64 => 8,
                    other => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Unsupported run-end width {other} for RunEndEncoded; \
                             expected 16/32/64 bits or 2/4/8 bytes"
                        )));
                    }
                };
                Self::RunEndEncoded(byte_width, 0, Box::new(inner))
            }
        };
        Ok(match data_type.nullability() {
            Some(nullability) => {
                // Default to reading a union branch tag unless the resolution proves otherwise.
                let mut plan = NullablePlan::ReadTag;
                if let Some(ResolutionInfo::Union(info)) = data_type.resolution.as_ref() {
                    if !info.writer_is_union && info.reader_is_union {
                        if let Some(Some((_reader_idx, promo))) = info.writer_to_reader.first() {
                            plan = NullablePlan::FromSingle { promotion: *promo };
                        }
                    }
                }
                Self::Nullable(
                    nullability,
                    NullBufferBuilder::new(DEFAULT_CAPACITY),
                    Box::new(decoder),
                    plan,
                )
            }
            None => decoder,
        })
    }

    /// Append a null record
    fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            Self::Null(count) => *count += 1,
            Self::Boolean(b) => b.append(false),
            Self::Int32(v) | Self::Date32(v) | Self::TimeMillis(v) => v.push(0),
            Self::Int64(v)
            | Self::Int32ToInt64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v)
            | Self::TimestampNanos(_, v) => v.push(0),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSecond(v)
            | Self::DurationMillisecond(v)
            | Self::DurationMicrosecond(v)
            | Self::DurationNanosecond(v) => v.push(0),
            Self::Float32(v) | Self::Int32ToFloat32(v) | Self::Int64ToFloat32(v) => v.push(0.),
            Self::Float64(v)
            | Self::Int32ToFloat64(v)
            | Self::Int64ToFloat64(v)
            | Self::Float32ToFloat64(v) => v.push(0.),
            Self::Binary(offsets, _)
            | Self::String(offsets, _)
            | Self::StringView(offsets, _)
            | Self::BytesToString(offsets, _)
            | Self::StringToBytes(offsets, _) => {
                offsets.push_length(0);
            }
            Self::Uuid(v) => {
                v.extend([0; 16]);
            }
            Self::Array(_, offsets, _) => {
                offsets.push_length(0);
            }
            Self::Record(_, e, _) => {
                for encoding in e.iter_mut() {
                    encoding.append_null()?;
                }
            }
            Self::Map(_, _koff, moff, _, _) => {
                moff.push_length(0);
            }
            Self::Fixed(sz, accum) => {
                accum.extend(std::iter::repeat_n(0u8, *sz as usize));
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal32(_, _, _, builder) => builder.append_value(0),
            #[cfg(feature = "small_decimals")]
            Self::Decimal64(_, _, _, builder) => builder.append_value(0),
            Self::Decimal128(_, _, _, builder) => builder.append_value(0),
            Self::Decimal256(_, _, _, builder) => builder.append_value(i256::ZERO),
            Self::Enum(indices, _, _) => indices.push(0),
            Self::Duration(builder) => builder.append_null(),
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(_, len, inner) => {
                *len += 1;
                inner.append_null()?;
            }
            Self::Union(u) => u.append_null()?,
            Self::Nullable(_, null_buffer, inner, _) => {
                null_buffer.append(false);
                inner.append_null()?;
            }
        }
        Ok(())
    }

    /// Append a single default literal into the decoder's buffers
    fn append_default(&mut self, lit: &AvroLiteral) -> Result<(), ArrowError> {
        match self {
            Self::Nullable(_, nb, inner, _) => {
                if matches!(lit, AvroLiteral::Null) {
                    nb.append(false);
                    inner.append_null()
                } else {
                    nb.append(true);
                    inner.append_default(lit)
                }
            }
            Self::Null(count) => match lit {
                AvroLiteral::Null => {
                    *count += 1;
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Non-null default for null type".to_string(),
                )),
            },
            Self::Boolean(b) => match lit {
                AvroLiteral::Boolean(v) => {
                    b.append(*v);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for boolean must be boolean".to_string(),
                )),
            },
            Self::Int32(v) | Self::Date32(v) | Self::TimeMillis(v) => match lit {
                AvroLiteral::Int(i) => {
                    v.push(*i);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for int32/date32/time-millis must be int".to_string(),
                )),
            },
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSecond(v)
            | Self::DurationMillisecond(v)
            | Self::DurationMicrosecond(v)
            | Self::DurationNanosecond(v) => match lit {
                AvroLiteral::Long(i) => {
                    v.push(*i);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for duration long must be long".to_string(),
                )),
            },
            Self::Int64(v)
            | Self::Int32ToInt64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v)
            | Self::TimestampNanos(_, v) => match lit {
                AvroLiteral::Long(i) => {
                    v.push(*i);
                    Ok(())
                }
                AvroLiteral::Int(i) => {
                    v.push(*i as i64);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for long/time-micros/timestamp must be long or int".to_string(),
                )),
            },
            Self::Float32(v) | Self::Int32ToFloat32(v) | Self::Int64ToFloat32(v) => match lit {
                AvroLiteral::Float(f) => {
                    v.push(*f);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for float must be float".to_string(),
                )),
            },
            Self::Float64(v)
            | Self::Int32ToFloat64(v)
            | Self::Int64ToFloat64(v)
            | Self::Float32ToFloat64(v) => match lit {
                AvroLiteral::Double(f) => {
                    v.push(*f);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for double must be double".to_string(),
                )),
            },
            Self::Binary(offsets, values) | Self::StringToBytes(offsets, values) => match lit {
                AvroLiteral::Bytes(b) => {
                    offsets.push_length(b.len());
                    values.extend_from_slice(b);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for bytes must be bytes".to_string(),
                )),
            },
            Self::BytesToString(offsets, values)
            | Self::String(offsets, values)
            | Self::StringView(offsets, values) => match lit {
                AvroLiteral::String(s) => {
                    let b = s.as_bytes();
                    offsets.push_length(b.len());
                    values.extend_from_slice(b);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for string must be string".to_string(),
                )),
            },
            Self::Uuid(values) => match lit {
                AvroLiteral::String(s) => {
                    let uuid = Uuid::try_parse(s).map_err(|e| {
                        ArrowError::InvalidArgumentError(format!("Invalid UUID default: {s} ({e})"))
                    })?;
                    values.extend_from_slice(uuid.as_bytes());
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for uuid must be string".to_string(),
                )),
            },
            Self::Fixed(sz, accum) => match lit {
                AvroLiteral::Bytes(b) => {
                    if b.len() != *sz as usize {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Fixed default length {} does not match size {sz}",
                            b.len(),
                        )));
                    }
                    accum.extend_from_slice(b);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for fixed must be bytes".to_string(),
                )),
            },
            #[cfg(feature = "small_decimals")]
            Self::Decimal32(_, _, _, builder) => {
                append_decimal_default!(lit, builder, 4, i32, "decimal32")
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal64(_, _, _, builder) => {
                append_decimal_default!(lit, builder, 8, i64, "decimal64")
            }
            Self::Decimal128(_, _, _, builder) => {
                append_decimal_default!(lit, builder, 16, i128, "decimal128")
            }
            Self::Decimal256(_, _, _, builder) => {
                append_decimal_default!(lit, builder, 32, i256, "decimal256")
            }
            Self::Duration(builder) => match lit {
                AvroLiteral::Bytes(b) => {
                    if b.len() != 12 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Duration default must be exactly 12 bytes, got {}",
                            b.len()
                        )));
                    }
                    let months = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                    let days = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
                    let millis = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
                    let nanos = (millis as i64) * 1_000_000;
                    builder.append_value(IntervalMonthDayNano::new(
                        months as i32,
                        days as i32,
                        nanos,
                    ));
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for duration must be 12-byte little-endian months/days/millis"
                        .to_string(),
                )),
            },
            Self::Array(_, offsets, inner) => match lit {
                AvroLiteral::Array(items) => {
                    offsets.push_length(items.len());
                    for item in items {
                        inner.append_default(item)?;
                    }
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for array must be an array literal".to_string(),
                )),
            },
            Self::Map(_, koff, moff, kdata, valdec) => match lit {
                AvroLiteral::Map(entries) => {
                    moff.push_length(entries.len());
                    for (k, v) in entries {
                        let kb = k.as_bytes();
                        koff.push_length(kb.len());
                        kdata.extend_from_slice(kb);
                        valdec.append_default(v)?;
                    }
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for map must be a map/object literal".to_string(),
                )),
            },
            Self::Enum(indices, symbols, _) => match lit {
                AvroLiteral::Enum(sym) => {
                    let pos = symbols.iter().position(|s| s == sym).ok_or_else(|| {
                        ArrowError::InvalidArgumentError(format!(
                            "Enum default symbol {sym:?} not in reader symbols"
                        ))
                    })?;
                    indices.push(pos as i32);
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for enum must be a symbol".to_string(),
                )),
            },
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(_, len, inner) => {
                *len += 1;
                inner.append_default(lit)
            }
            Self::Union(u) => u.append_default(lit),
            Self::Record(field_meta, decoders, projector) => match lit {
                AvroLiteral::Map(entries) => {
                    for (i, dec) in decoders.iter_mut().enumerate() {
                        let name = field_meta[i].name();
                        if let Some(sub) = entries.get(name) {
                            dec.append_default(sub)?;
                        } else if let Some(proj) = projector.as_ref() {
                            proj.project_default(dec, i)?;
                        } else {
                            dec.append_null()?;
                        }
                    }
                    Ok(())
                }
                AvroLiteral::Null => {
                    for (i, dec) in decoders.iter_mut().enumerate() {
                        if let Some(proj) = projector.as_ref() {
                            proj.project_default(dec, i)?;
                        } else {
                            dec.append_null()?;
                        }
                    }
                    Ok(())
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Default for record must be a map/object or null".to_string(),
                )),
            },
        }
    }

    /// Decode a single record from `buf`
    fn decode(&mut self, buf: &mut AvroCursor<'_>) -> Result<(), ArrowError> {
        match self {
            Self::Null(x) => *x += 1,
            Self::Boolean(values) => values.append(buf.get_bool()?),
            Self::Int32(values) | Self::Date32(values) | Self::TimeMillis(values) => {
                values.push(buf.get_int()?)
            }
            Self::Int64(values)
            | Self::TimeMicros(values)
            | Self::TimestampMillis(_, values)
            | Self::TimestampMicros(_, values)
            | Self::TimestampNanos(_, values) => values.push(buf.get_long()?),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSecond(values)
            | Self::DurationMillisecond(values)
            | Self::DurationMicrosecond(values)
            | Self::DurationNanosecond(values) => values.push(buf.get_long()?),
            Self::Float32(values) => values.push(buf.get_float()?),
            Self::Float64(values) => values.push(buf.get_double()?),
            Self::Int32ToInt64(values) => values.push(buf.get_int()? as i64),
            Self::Int32ToFloat32(values) => values.push(buf.get_int()? as f32),
            Self::Int32ToFloat64(values) => values.push(buf.get_int()? as f64),
            Self::Int64ToFloat32(values) => values.push(buf.get_long()? as f32),
            Self::Int64ToFloat64(values) => values.push(buf.get_long()? as f64),
            Self::Float32ToFloat64(values) => values.push(buf.get_float()? as f64),
            Self::StringToBytes(offsets, values)
            | Self::BytesToString(offsets, values)
            | Self::Binary(offsets, values)
            | Self::String(offsets, values)
            | Self::StringView(offsets, values) => {
                let data = buf.get_bytes()?;
                offsets.push_length(data.len());
                values.extend_from_slice(data);
            }
            Self::Uuid(values) => {
                let s_bytes = buf.get_bytes()?;
                let s = std::str::from_utf8(s_bytes).map_err(|e| {
                    ArrowError::ParseError(format!("UUID bytes are not valid UTF-8: {e}"))
                })?;
                let uuid = Uuid::try_parse(s)
                    .map_err(|e| ArrowError::ParseError(format!("Failed to parse uuid: {e}")))?;
                values.extend_from_slice(uuid.as_bytes());
            }
            Self::Array(_, off, encoding) => {
                let total_items = read_blocks(buf, |cursor| encoding.decode(cursor))?;
                off.push_length(total_items);
            }
            Self::Record(_, encodings, None) => {
                for encoding in encodings {
                    encoding.decode(buf)?;
                }
            }
            Self::Record(_, encodings, Some(proj)) => {
                proj.project_record(buf, encodings)?;
            }
            Self::Map(_, koff, moff, kdata, valdec) => {
                let newly_added = read_blocks(buf, |cur| {
                    let kb = cur.get_bytes()?;
                    koff.push_length(kb.len());
                    kdata.extend_from_slice(kb);
                    valdec.decode(cur)
                })?;
                moff.push_length(newly_added);
            }
            Self::Fixed(sz, accum) => {
                let fx = buf.get_fixed(*sz as usize)?;
                accum.extend_from_slice(fx);
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal32(_, _, size, builder) => {
                decode_decimal!(size, buf, builder, 4, i32);
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal64(_, _, size, builder) => {
                decode_decimal!(size, buf, builder, 8, i64);
            }
            Self::Decimal128(_, _, size, builder) => {
                decode_decimal!(size, buf, builder, 16, i128);
            }
            Self::Decimal256(_, _, size, builder) => {
                decode_decimal!(size, buf, builder, 32, i256);
            }
            Self::Enum(indices, _, None) => {
                indices.push(buf.get_int()?);
            }
            Self::Enum(indices, _, Some(res)) => {
                let raw = buf.get_int()?;
                let resolved = usize::try_from(raw)
                    .ok()
                    .and_then(|idx| res.mapping.get(idx).copied())
                    .filter(|&idx| idx >= 0)
                    .unwrap_or(res.default_index);
                if resolved >= 0 {
                    indices.push(resolved);
                } else {
                    return Err(ArrowError::ParseError(format!(
                        "Enum symbol index {raw} not resolvable and no default provided",
                    )));
                }
            }
            Self::Duration(builder) => {
                let b = buf.get_fixed(12)?;
                let months = u32::from_le_bytes(b[0..4].try_into().unwrap());
                let days = u32::from_le_bytes(b[4..8].try_into().unwrap());
                let millis = u32::from_le_bytes(b[8..12].try_into().unwrap());
                let nanos = (millis as i64) * 1_000_000;
                builder.append_value(IntervalMonthDayNano::new(months as i32, days as i32, nanos));
            }
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(_, len, inner) => {
                *len += 1;
                inner.decode(buf)?;
            }
            Self::Union(u) => u.decode(buf)?,
            Self::Nullable(order, nb, encoding, plan) => {
                match *plan {
                    NullablePlan::FromSingle { promotion } => {
                        encoding.decode_with_promotion(buf, promotion)?;
                        nb.append(true);
                    }
                    NullablePlan::ReadTag => {
                        let branch = buf.read_vlq()?;
                        let is_not_null = match *order {
                            Nullability::NullFirst => branch != 0,
                            Nullability::NullSecond => branch == 0,
                        };
                        if is_not_null {
                            // It is important to decode before appending to null buffer in case of decode error
                            encoding.decode(buf)?;
                        } else {
                            encoding.append_null()?;
                        }
                        nb.append(is_not_null);
                    }
                }
            }
        }
        Ok(())
    }

    fn decode_with_promotion(
        &mut self,
        buf: &mut AvroCursor<'_>,
        promotion: Promotion,
    ) -> Result<(), ArrowError> {
        #[cfg(feature = "avro_custom_types")]
        if let Self::RunEndEncoded(_, len, inner) = self {
            *len += 1;
            return inner.decode_with_promotion(buf, promotion);
        }

        macro_rules! promote_numeric_to {
            ($variant:ident, $getter:ident, $to:ty) => {{
                match self {
                    Self::$variant(v) => {
                        let x = buf.$getter()?;
                        v.push(x as $to);
                        Ok(())
                    }
                    other => Err(ArrowError::ParseError(format!(
                        "Promotion {promotion} target mismatch: expected {}, got {}",
                        stringify!($variant),
                        <Self as ::std::convert::AsRef<str>>::as_ref(other)
                    ))),
                }
            }};
        }
        match promotion {
            Promotion::Direct => self.decode(buf),
            Promotion::IntToLong => promote_numeric_to!(Int64, get_int, i64),
            Promotion::IntToFloat => promote_numeric_to!(Float32, get_int, f32),
            Promotion::IntToDouble => promote_numeric_to!(Float64, get_int, f64),
            Promotion::LongToFloat => promote_numeric_to!(Float32, get_long, f32),
            Promotion::LongToDouble => promote_numeric_to!(Float64, get_long, f64),
            Promotion::FloatToDouble => promote_numeric_to!(Float64, get_float, f64),
            Promotion::StringToBytes => match self {
                Self::Binary(offsets, values) | Self::StringToBytes(offsets, values) => {
                    let data = buf.get_bytes()?;
                    offsets.push_length(data.len());
                    values.extend_from_slice(data);
                    Ok(())
                }
                other => Err(ArrowError::ParseError(format!(
                    "Promotion {promotion} target mismatch: expected bytes (Binary/StringToBytes), got {}",
                    <Self as AsRef<str>>::as_ref(other)
                ))),
            },
            Promotion::BytesToString => match self {
                Self::String(offsets, values)
                | Self::StringView(offsets, values)
                | Self::BytesToString(offsets, values) => {
                    let data = buf.get_bytes()?;
                    offsets.push_length(data.len());
                    values.extend_from_slice(data);
                    Ok(())
                }
                other => Err(ArrowError::ParseError(format!(
                    "Promotion {promotion} target mismatch: expected string (String/StringView/BytesToString), got {}",
                    <Self as AsRef<str>>::as_ref(other)
                ))),
            },
        }
    }

    /// Flush decoded records to an [`ArrayRef`]
    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            Self::Nullable(_, n, e, _) => e.flush(n.finish())?,
            Self::Null(size) => Arc::new(NullArray::new(std::mem::replace(size, 0))),
            Self::Boolean(b) => Arc::new(BooleanArray::new(b.finish(), nulls)),
            Self::Int32(values) => Arc::new(flush_primitive::<Int32Type>(values, nulls)),
            Self::Date32(values) => Arc::new(flush_primitive::<Date32Type>(values, nulls)),
            Self::Int64(values) => Arc::new(flush_primitive::<Int64Type>(values, nulls)),
            Self::TimeMillis(values) => {
                Arc::new(flush_primitive::<Time32MillisecondType>(values, nulls))
            }
            Self::TimeMicros(values) => {
                Arc::new(flush_primitive::<Time64MicrosecondType>(values, nulls))
            }
            Self::TimestampMillis(is_utc, values) => Arc::new(
                flush_primitive::<TimestampMillisecondType>(values, nulls)
                    .with_timezone_opt(is_utc.then(|| "+00:00")),
            ),
            Self::TimestampMicros(is_utc, values) => Arc::new(
                flush_primitive::<TimestampMicrosecondType>(values, nulls)
                    .with_timezone_opt(is_utc.then(|| "+00:00")),
            ),
            Self::TimestampNanos(is_utc, values) => Arc::new(
                flush_primitive::<TimestampNanosecondType>(values, nulls)
                    .with_timezone_opt(is_utc.then(|| "+00:00")),
            ),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSecond(values) => {
                Arc::new(flush_primitive::<DurationSecondType>(values, nulls))
            }
            #[cfg(feature = "avro_custom_types")]
            Self::DurationMillisecond(values) => {
                Arc::new(flush_primitive::<DurationMillisecondType>(values, nulls))
            }
            #[cfg(feature = "avro_custom_types")]
            Self::DurationMicrosecond(values) => {
                Arc::new(flush_primitive::<DurationMicrosecondType>(values, nulls))
            }
            #[cfg(feature = "avro_custom_types")]
            Self::DurationNanosecond(values) => {
                Arc::new(flush_primitive::<DurationNanosecondType>(values, nulls))
            }
            Self::Float32(values) => Arc::new(flush_primitive::<Float32Type>(values, nulls)),
            Self::Float64(values) => Arc::new(flush_primitive::<Float64Type>(values, nulls)),
            Self::Int32ToInt64(values) => Arc::new(flush_primitive::<Int64Type>(values, nulls)),
            Self::Int32ToFloat32(values) | Self::Int64ToFloat32(values) => {
                Arc::new(flush_primitive::<Float32Type>(values, nulls))
            }
            Self::Int32ToFloat64(values)
            | Self::Int64ToFloat64(values)
            | Self::Float32ToFloat64(values) => {
                Arc::new(flush_primitive::<Float64Type>(values, nulls))
            }
            Self::StringToBytes(offsets, values) | Self::Binary(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Arc::new(BinaryArray::try_new(offsets, values, nulls)?)
            }
            Self::BytesToString(offsets, values) | Self::String(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Arc::new(StringArray::try_new(offsets, values, nulls)?)
            }
            Self::StringView(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values);
                let array = StringArray::try_new(offsets, values.into(), nulls.clone())?;
                let values: Vec<&str> = (0..array.len())
                    .map(|i| {
                        if array.is_valid(i) {
                            array.value(i)
                        } else {
                            ""
                        }
                    })
                    .collect();
                Arc::new(StringViewArray::from(values))
            }
            Self::Array(field, offsets, values) => {
                let values = values.flush(None)?;
                let offsets = flush_offsets(offsets);
                Arc::new(ListArray::try_new(field.clone(), offsets, values, nulls)?)
            }
            Self::Record(fields, encodings, _) => {
                let arrays = encodings
                    .iter_mut()
                    .map(|x| x.flush(None))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(StructArray::try_new(fields.clone(), arrays, nulls)?)
            }
            Self::Map(map_field, k_off, m_off, kdata, valdec) => {
                let moff = flush_offsets(m_off);
                let koff = flush_offsets(k_off);
                let kd = flush_values(kdata).into();
                let val_arr = valdec.flush(None)?;
                let key_arr = StringArray::try_new(koff, kd, None)?;
                if key_arr.len() != val_arr.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Map keys length ({}) != map values length ({})",
                        key_arr.len(),
                        val_arr.len()
                    )));
                }
                let final_len = moff.len() - 1;
                if let Some(n) = &nulls {
                    if n.len() != final_len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Map array null buffer length {} != final map length {final_len}",
                            n.len()
                        )));
                    }
                }
                let entries_fields = match map_field.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    other => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Map entries field must be a Struct, got {other:?}"
                        )));
                    }
                };
                let entries_struct =
                    StructArray::try_new(entries_fields, vec![Arc::new(key_arr), val_arr], None)?;
                let map_arr =
                    MapArray::try_new(map_field.clone(), moff, entries_struct, nulls, false)?;
                Arc::new(map_arr)
            }
            Self::Fixed(sz, accum) => {
                let b: Buffer = flush_values(accum).into();
                let arr = FixedSizeBinaryArray::try_new(*sz, b, nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Arc::new(arr)
            }
            Self::Uuid(values) => {
                let arr = FixedSizeBinaryArray::try_new(16, std::mem::take(values).into(), nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Arc::new(arr)
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal32(precision, scale, _, builder) => {
                flush_decimal!(builder, precision, scale, nulls, Decimal32Array)
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal64(precision, scale, _, builder) => {
                flush_decimal!(builder, precision, scale, nulls, Decimal64Array)
            }
            Self::Decimal128(precision, scale, _, builder) => {
                flush_decimal!(builder, precision, scale, nulls, Decimal128Array)
            }
            Self::Decimal256(precision, scale, _, builder) => {
                flush_decimal!(builder, precision, scale, nulls, Decimal256Array)
            }
            Self::Enum(indices, symbols, _) => flush_dict(indices, symbols, nulls)?,
            Self::Duration(builder) => {
                let (_, vals, _) = builder.finish().into_parts();
                let vals = IntervalMonthDayNanoArray::try_new(vals, nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Arc::new(vals)
            }
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(width, len, inner) => {
                let values = inner.flush(nulls)?;
                let n = *len;
                let arr = values.as_ref();
                let mut run_starts: Vec<usize> = Vec::with_capacity(n);
                if n > 0 {
                    run_starts.push(0);
                    for i in 1..n {
                        if !values_equal_at(arr, i - 1, i) {
                            run_starts.push(i);
                        }
                    }
                }
                if n > (u32::MAX as usize) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "RunEndEncoded length {n} exceeds maximum supported by UInt32 indices for take",
                    )));
                }
                let run_count = run_starts.len();
                let take_idx: PrimitiveArray<UInt32Type> =
                    run_starts.iter().map(|&s| s as u32).collect();
                let per_run_values = if run_count == 0 {
                    values.slice(0, 0)
                } else {
                    take(arr, &take_idx, Option::from(TakeOptions::default())).map_err(|e| {
                        ArrowError::ParseError(format!("take() for REE values failed: {e}"))
                    })?
                };

                macro_rules! build_run_array {
                    ($Native:ty, $ArrowTy:ty) => {{
                        let mut ends: Vec<$Native> = Vec::with_capacity(run_count);
                        for (idx, &_start) in run_starts.iter().enumerate() {
                            let end = if idx + 1 < run_count {
                                run_starts[idx + 1]
                            } else {
                                n
                            };
                            ends.push(end as $Native);
                        }
                        let ends: PrimitiveArray<$ArrowTy> = ends.into_iter().collect();
                        let run_arr = RunArray::<$ArrowTy>::try_new(&ends, per_run_values.as_ref())
                            .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                        Arc::new(run_arr) as ArrayRef
                    }};
                }
                match *width {
                    2 => {
                        if n > i16::MAX as usize {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "RunEndEncoded length {n} exceeds i16::MAX for run end width 2"
                            )));
                        }
                        build_run_array!(i16, Int16Type)
                    }
                    4 => build_run_array!(i32, Int32Type),
                    8 => build_run_array!(i64, Int64Type),
                    other => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Unsupported run-end width {other} for RunEndEncoded"
                        )));
                    }
                }
            }
            Self::Union(u) => u.flush(nulls)?,
        })
    }
}

// A lookup table for resolving fields between writer and reader schemas during record projection.
#[derive(Debug)]
struct DispatchLookupTable {
    // Maps each reader field index `r` to the corresponding writer field index.
    //
    // Semantics:
    // - `to_reader[r] >= 0`: The value is an index into the writer's fields. The value from
    //   the writer field is decoded, and `promotion[r]` is applied.
    // - `to_reader[r] == NO_SOURCE` (-1): No matching writer field exists. The reader field's
    //   default value is used.
    //
    // Representation (`i8`):
    // `i8` is used for a dense, cache-friendly dispatch table, consistent with Arrow's use of
    // `i8` for union type IDs. This requires that writer field indices do not exceed `i8::MAX`.
    //
    // Invariants:
    // - `to_reader.len() == promotion.len()` and matches the reader field count.
    // - If `to_reader[r] == NO_SOURCE`, `promotion[r]` is ignored.
    to_reader: Box<[i8]>,
    // For each reader field `r`, specifies the `Promotion` to apply to the writer's value.
    //
    // This is used when a writer field's type can be promoted to a reader field's type
    // (e.g., `Int` to `Long`). It is ignored if `to_reader[r] == NO_SOURCE`.
    promotion: Box<[Promotion]>,
}

// Sentinel used in `DispatchLookupTable::to_reader` to mark
// "no matching writer field".
const NO_SOURCE: i8 = -1;

impl DispatchLookupTable {
    fn from_writer_to_reader(
        promotion_map: &[Option<(usize, Promotion)>],
    ) -> Result<Self, ArrowError> {
        let mut to_reader = Vec::with_capacity(promotion_map.len());
        let mut promotion = Vec::with_capacity(promotion_map.len());
        for map in promotion_map {
            match *map {
                Some((idx, promo)) => {
                    let idx_i8 = i8::try_from(idx).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "Reader branch index {idx} exceeds i8 range (max {})",
                            i8::MAX
                        ))
                    })?;
                    to_reader.push(idx_i8);
                    promotion.push(promo);
                }
                None => {
                    to_reader.push(NO_SOURCE);
                    promotion.push(Promotion::Direct);
                }
            }
        }
        Ok(Self {
            to_reader: to_reader.into_boxed_slice(),
            promotion: promotion.into_boxed_slice(),
        })
    }

    // Resolve a writer branch index to (reader_idx, promotion)
    #[inline]
    fn resolve(&self, writer_index: usize) -> Option<(usize, Promotion)> {
        let reader_index = *self.to_reader.get(writer_index)?;
        (reader_index >= 0).then(|| (reader_index as usize, self.promotion[writer_index]))
    }
}

#[derive(Debug)]
struct UnionDecoder {
    fields: UnionFields,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    branches: Vec<Decoder>,
    counts: Vec<i32>,
    reader_type_codes: Vec<i8>,
    default_emit_idx: usize,
    null_emit_idx: usize,
    plan: UnionReadPlan,
}

impl Default for UnionDecoder {
    fn default() -> Self {
        Self {
            fields: UnionFields::empty(),
            type_ids: Vec::new(),
            offsets: Vec::new(),
            branches: Vec::new(),
            counts: Vec::new(),
            reader_type_codes: Vec::new(),
            default_emit_idx: 0,
            null_emit_idx: 0,
            plan: UnionReadPlan::Passthrough,
        }
    }
}

#[derive(Debug)]
enum UnionReadPlan {
    ReaderUnion {
        lookup_table: DispatchLookupTable,
    },
    FromSingle {
        reader_idx: usize,
        promotion: Promotion,
    },
    ToSingle {
        target: Box<Decoder>,
        lookup_table: DispatchLookupTable,
    },
    Passthrough,
}

impl UnionDecoder {
    fn try_new(
        fields: UnionFields,
        branches: Vec<Decoder>,
        resolved: Option<ResolvedUnion>,
    ) -> Result<Self, ArrowError> {
        let reader_type_codes = fields.iter().map(|(tid, _)| tid).collect::<Vec<i8>>();
        let null_branch = branches.iter().position(|b| matches!(b, Decoder::Null(_)));
        let default_emit_idx = 0;
        let null_emit_idx = null_branch.unwrap_or(default_emit_idx);
        let branch_len = branches.len().max(reader_type_codes.len());
        // Guard against impractically large unions that cannot be indexed by an Avro int
        let max_addr = (i32::MAX as usize) + 1;
        if branches.len() > max_addr {
            return Err(ArrowError::SchemaError(format!(
                "Reader union has {} branches, which exceeds the maximum addressable \
                 branches by an Avro int tag ({} + 1).",
                branches.len(),
                i32::MAX
            )));
        }
        Ok(Self {
            fields,
            type_ids: Vec::with_capacity(DEFAULT_CAPACITY),
            offsets: Vec::with_capacity(DEFAULT_CAPACITY),
            branches,
            counts: vec![0; branch_len],
            reader_type_codes,
            default_emit_idx,
            null_emit_idx,
            plan: Self::plan_from_resolved(resolved)?,
        })
    }

    fn try_new_from_writer_union(
        info: ResolvedUnion,
        target: Box<Decoder>,
    ) -> Result<Self, ArrowError> {
        // This constructor is only for writer-union to single-type resolution
        debug_assert!(info.writer_is_union && !info.reader_is_union);
        let lookup_table = DispatchLookupTable::from_writer_to_reader(&info.writer_to_reader)?;
        Ok(Self {
            plan: UnionReadPlan::ToSingle {
                target,
                lookup_table,
            },
            ..Self::default()
        })
    }

    fn plan_from_resolved(resolved: Option<ResolvedUnion>) -> Result<UnionReadPlan, ArrowError> {
        let Some(info) = resolved else {
            return Ok(UnionReadPlan::Passthrough);
        };
        match (info.writer_is_union, info.reader_is_union) {
            (true, true) => {
                let lookup_table =
                    DispatchLookupTable::from_writer_to_reader(&info.writer_to_reader)?;
                Ok(UnionReadPlan::ReaderUnion { lookup_table })
            }
            (false, true) => {
                let Some(&(reader_idx, promotion)) =
                    info.writer_to_reader.first().and_then(Option::as_ref)
                else {
                    return Err(ArrowError::SchemaError(
                        "Writer type does not match any reader union branch".to_string(),
                    ));
                };
                Ok(UnionReadPlan::FromSingle {
                    reader_idx,
                    promotion,
                })
            }
            (true, false) => Err(ArrowError::InvalidArgumentError(
                "UnionDecoder::try_new cannot build writer-union to single; use UnionDecoderBuilder with a target"
                    .to_string(),
            )),
            // (false, false) is invalid and should never be constructed by the resolver.
            _ => Err(ArrowError::SchemaError(
                "ResolvedUnion constructed for non-union sides; resolver should return None"
                    .to_string(),
            )),
        }
    }

    #[inline]
    fn read_tag(buf: &mut AvroCursor<'_>) -> Result<usize, ArrowError> {
        // Avro unions are encoded by first writing the zero-based branch index.
        // In Avro 1.11.1 this is specified as an *int*; older specs said *long*,
        // but both use zig-zag varint encoding, so decoding as long is compatible
        // with either form and widely used in practice.
        let raw = buf.get_long()?;
        if raw < 0 {
            return Err(ArrowError::ParseError(format!(
                "Negative union branch index {raw}"
            )));
        }
        usize::try_from(raw).map_err(|_| {
            ArrowError::ParseError(format!(
                "Union branch index {raw} does not fit into usize on this platform ({}-bit)",
                (usize::BITS as usize)
            ))
        })
    }

    #[inline]
    fn emit_to(&mut self, reader_idx: usize) -> Result<&mut Decoder, ArrowError> {
        let branches_len = self.branches.len();
        let Some(reader_branch) = self.branches.get_mut(reader_idx) else {
            return Err(ArrowError::ParseError(format!(
                "Union branch index {reader_idx} out of range ({branches_len} branches)"
            )));
        };
        self.type_ids.push(self.reader_type_codes[reader_idx]);
        self.offsets.push(self.counts[reader_idx]);
        self.counts[reader_idx] += 1;
        Ok(reader_branch)
    }

    #[inline]
    fn on_decoder<F>(&mut self, fallback_idx: usize, action: F) -> Result<(), ArrowError>
    where
        F: FnOnce(&mut Decoder) -> Result<(), ArrowError>,
    {
        if let UnionReadPlan::ToSingle { target, .. } = &mut self.plan {
            return action(target);
        }
        let reader_idx = match &self.plan {
            UnionReadPlan::FromSingle { reader_idx, .. } => *reader_idx,
            _ => fallback_idx,
        };
        self.emit_to(reader_idx).and_then(action)
    }

    fn append_null(&mut self) -> Result<(), ArrowError> {
        self.on_decoder(self.null_emit_idx, |decoder| decoder.append_null())
    }

    fn append_default(&mut self, lit: &AvroLiteral) -> Result<(), ArrowError> {
        self.on_decoder(self.default_emit_idx, |decoder| decoder.append_default(lit))
    }

    fn decode(&mut self, buf: &mut AvroCursor<'_>) -> Result<(), ArrowError> {
        let (reader_idx, promotion) = match &mut self.plan {
            UnionReadPlan::Passthrough => (Self::read_tag(buf)?, Promotion::Direct),
            UnionReadPlan::ReaderUnion { lookup_table } => {
                let idx = Self::read_tag(buf)?;
                lookup_table.resolve(idx).ok_or_else(|| {
                    ArrowError::ParseError(format!(
                        "Union branch index {idx} not resolvable by reader schema"
                    ))
                })?
            }
            UnionReadPlan::FromSingle {
                reader_idx,
                promotion,
            } => (*reader_idx, *promotion),
            UnionReadPlan::ToSingle {
                target,
                lookup_table,
            } => {
                let idx = Self::read_tag(buf)?;
                return match lookup_table.resolve(idx) {
                    Some((_, promotion)) => target.decode_with_promotion(buf, promotion),
                    None => Err(ArrowError::ParseError(format!(
                        "Writer union branch {idx} does not resolve to reader type"
                    ))),
                };
            }
        };
        let decoder = self.emit_to(reader_idx)?;
        decoder.decode_with_promotion(buf, promotion)
    }

    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        if let UnionReadPlan::ToSingle { target, .. } = &mut self.plan {
            return target.flush(nulls);
        }
        debug_assert!(
            nulls.is_none(),
            "UnionArray does not accept a validity bitmap; \
                     nulls should have been materialized as a Null child during decode"
        );
        let children = self
            .branches
            .iter_mut()
            .map(|d| d.flush(None))
            .collect::<Result<Vec<_>, _>>()?;
        let arr = UnionArray::try_new(
            self.fields.clone(),
            flush_values(&mut self.type_ids).into_iter().collect(),
            Some(flush_values(&mut self.offsets).into_iter().collect()),
            children,
        )
        .map_err(|e| ArrowError::ParseError(e.to_string()))?;
        Ok(Arc::new(arr))
    }
}

#[derive(Debug, Default)]
struct UnionDecoderBuilder {
    fields: Option<UnionFields>,
    branches: Option<Vec<Decoder>>,
    resolved: Option<ResolvedUnion>,
    target: Option<Box<Decoder>>,
}

impl UnionDecoderBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn with_fields(mut self, fields: UnionFields) -> Self {
        self.fields = Some(fields);
        self
    }

    fn with_branches(mut self, branches: Vec<Decoder>) -> Self {
        self.branches = Some(branches);
        self
    }

    fn with_resolved_union(mut self, resolved_union: ResolvedUnion) -> Self {
        self.resolved = Some(resolved_union);
        self
    }

    fn with_target(mut self, target: Box<Decoder>) -> Self {
        self.target = Some(target);
        self
    }

    fn build(self) -> Result<UnionDecoder, ArrowError> {
        match (self.resolved, self.fields, self.branches, self.target) {
            (resolved, Some(fields), Some(branches), None) => {
                UnionDecoder::try_new(fields, branches, resolved)
            }
            (Some(info), None, None, Some(target))
                if info.writer_is_union && !info.reader_is_union =>
            {
                UnionDecoder::try_new_from_writer_union(info, target)
            }
            _ => Err(ArrowError::InvalidArgumentError(
                "Invalid UnionDecoderBuilder configuration: expected either \
                 (fields + branches + resolved) with no target for reader-unions, or \
                 (resolved + target) with no fields/branches for writer-union to single."
                    .to_string(),
            )),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum NegativeBlockBehavior {
    ProcessItems,
    SkipBySize,
}

#[inline]
fn skip_blocks(
    buf: &mut AvroCursor,
    mut skip_item: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    process_blockwise(
        buf,
        move |c| skip_item(c),
        NegativeBlockBehavior::SkipBySize,
    )
}

#[inline]
fn flush_dict(
    indices: &mut Vec<i32>,
    symbols: &[String],
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let keys = flush_primitive::<Int32Type>(indices, nulls);
    let values = Arc::new(StringArray::from_iter_values(
        symbols.iter().map(|s| s.as_str()),
    ));
    DictionaryArray::try_new(keys, values)
        .map_err(|e| ArrowError::ParseError(e.to_string()))
        .map(|arr| Arc::new(arr) as ArrayRef)
}

#[inline]
fn read_blocks(
    buf: &mut AvroCursor,
    decode_entry: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    process_blockwise(buf, decode_entry, NegativeBlockBehavior::ProcessItems)
}

#[inline]
fn process_blockwise(
    buf: &mut AvroCursor,
    mut on_item: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
    negative_behavior: NegativeBlockBehavior,
) -> Result<usize, ArrowError> {
    let mut total = 0usize;
    loop {
        // Read the block count
        //  positive = that many items
        //  negative = that many items + read block size
        //  See: https://avro.apache.org/docs/1.11.1/specification/#maps
        let block_count = buf.get_long()?;
        match block_count.cmp(&0) {
            Ordering::Equal => break,
            Ordering::Less => {
                let count = (-block_count) as usize;
                // A negative count is followed by a long of the size in bytes
                let size_in_bytes = buf.get_long()? as usize;
                match negative_behavior {
                    NegativeBlockBehavior::ProcessItems => {
                        // Process items one-by-one after reading size
                        for _ in 0..count {
                            on_item(buf)?;
                        }
                    }
                    NegativeBlockBehavior::SkipBySize => {
                        // Skip the entire block payload at once
                        let _ = buf.get_fixed(size_in_bytes)?;
                    }
                }
                total += count;
            }
            Ordering::Greater => {
                let count = block_count as usize;
                for _ in 0..count {
                    on_item(buf)?;
                }
                total += count;
            }
        }
    }
    Ok(total)
}

#[inline]
fn flush_values<T>(values: &mut Vec<T>) -> Vec<T> {
    std::mem::replace(values, Vec::with_capacity(DEFAULT_CAPACITY))
}

#[inline]
fn flush_offsets(offsets: &mut OffsetBufferBuilder<i32>) -> OffsetBuffer<i32> {
    std::mem::replace(offsets, OffsetBufferBuilder::new(DEFAULT_CAPACITY)).finish()
}

#[inline]
fn flush_primitive<T: ArrowPrimitiveType>(
    values: &mut Vec<T::Native>,
    nulls: Option<NullBuffer>,
) -> PrimitiveArray<T> {
    PrimitiveArray::new(flush_values(values).into(), nulls)
}

#[inline]
fn read_decimal_bytes_be<const N: usize>(
    buf: &mut AvroCursor<'_>,
    size: &Option<usize>,
) -> Result<[u8; N], ArrowError> {
    match size {
        Some(n) if *n == N => {
            let raw = buf.get_fixed(N)?;
            let mut arr = [0u8; N];
            arr.copy_from_slice(raw);
            Ok(arr)
        }
        Some(n) => {
            let raw = buf.get_fixed(*n)?;
            sign_cast_to::<N>(raw)
        }
        None => {
            let raw = buf.get_bytes()?;
            sign_cast_to::<N>(raw)
        }
    }
}

/// Sign-extend or (when larger) validate-and-truncate a big-endian two's-complement
/// integer into exactly `N` bytes. This matches Avro's decimal binary encoding:
/// the payload is a big-endian two's-complement integer, and when narrowing it must
/// be representable without changing sign or value.
///
/// If `raw.len() < N`, the value is sign-extended.
/// If `raw.len() > N`, all truncated leading bytes must match the sign-extension byte
/// and the MSB of the first kept byte must match the sign (to avoid silent overflow).
#[inline]
fn sign_cast_to<const N: usize>(raw: &[u8]) -> Result<[u8; N], ArrowError> {
    let len = raw.len();
    // Fast path: exact width, just copy
    if len == N {
        let mut out = [0u8; N];
        out.copy_from_slice(raw);
        return Ok(out);
    }
    // Determine sign byte from MSB of first byte (empty => positive)
    let first = raw.first().copied().unwrap_or(0u8);
    let sign_byte = if (first & 0x80) == 0 { 0x00 } else { 0xFF };
    // Pre-fill with sign byte to support sign extension
    let mut out = [sign_byte; N];
    if len > N {
        // Validate truncation: all dropped leading bytes must equal sign_byte,
        // and the MSB of the first kept byte must match the sign.
        let extra = len - N;
        // Any non-sign byte in the truncated prefix indicates overflow
        if raw[..extra].iter().any(|&b| b != sign_byte) {
            return Err(ArrowError::ParseError(format!(
                "Decimal value with {} bytes cannot be represented in {} bytes without overflow",
                len, N
            )));
        }
        if N > 0 {
            let first_kept = raw[extra];
            let sign_bit_mismatch = ((first_kept ^ sign_byte) & 0x80) != 0;
            if sign_bit_mismatch {
                return Err(ArrowError::ParseError(format!(
                    "Decimal value with {} bytes cannot be represented in {} bytes without overflow",
                    len, N
                )));
            }
        }
        out.copy_from_slice(&raw[extra..]);
        return Ok(out);
    }
    out[N - len..].copy_from_slice(raw);
    Ok(out)
}

#[cfg(feature = "avro_custom_types")]
#[inline]
fn values_equal_at(arr: &dyn Array, i: usize, j: usize) -> bool {
    match (arr.is_null(i), arr.is_null(j)) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => {
            let a = arr.slice(i, 1);
            let b = arr.slice(j, 1);
            a == b
        }
    }
}

#[derive(Debug)]
struct Projector {
    writer_to_reader: Arc<[Option<usize>]>,
    skip_decoders: Vec<Option<Skipper>>,
    field_defaults: Vec<Option<AvroLiteral>>,
    default_injections: Arc<[(usize, AvroLiteral)]>,
}

#[derive(Debug)]
struct ProjectorBuilder<'a> {
    rec: &'a ResolvedRecord,
    reader_fields: Arc<[AvroField]>,
}

impl<'a> ProjectorBuilder<'a> {
    #[inline]
    fn try_new(rec: &'a ResolvedRecord, reader_fields: &Arc<[AvroField]>) -> Self {
        Self {
            rec,
            reader_fields: reader_fields.clone(),
        }
    }

    #[inline]
    fn build(self) -> Result<Projector, ArrowError> {
        let reader_fields = self.reader_fields;
        let mut field_defaults: Vec<Option<AvroLiteral>> = Vec::with_capacity(reader_fields.len());
        for avro_field in reader_fields.as_ref() {
            if let Some(ResolutionInfo::DefaultValue(lit)) =
                avro_field.data_type().resolution.as_ref()
            {
                field_defaults.push(Some(lit.clone()));
            } else {
                field_defaults.push(None);
            }
        }
        let mut default_injections: Vec<(usize, AvroLiteral)> =
            Vec::with_capacity(self.rec.default_fields.len());
        for &idx in self.rec.default_fields.as_ref() {
            let lit = field_defaults
                .get(idx)
                .and_then(|lit| lit.clone())
                .unwrap_or(AvroLiteral::Null);
            default_injections.push((idx, lit));
        }
        let mut skip_decoders: Vec<Option<Skipper>> =
            Vec::with_capacity(self.rec.skip_fields.len());
        for datatype in self.rec.skip_fields.as_ref() {
            let skipper = match datatype {
                Some(datatype) => Some(Skipper::from_avro(datatype)?),
                None => None,
            };
            skip_decoders.push(skipper);
        }
        Ok(Projector {
            writer_to_reader: self.rec.writer_to_reader.clone(),
            skip_decoders,
            field_defaults,
            default_injections: default_injections.into(),
        })
    }
}

impl Projector {
    #[inline]
    fn project_default(&self, decoder: &mut Decoder, index: usize) -> Result<(), ArrowError> {
        // SAFETY: `index` is obtained by listing the reader's record fields (i.e., from
        // `decoders.iter_mut().enumerate()`), and `field_defaults` was built in
        // `ProjectorBuilder::build` to have exactly one element per reader field.
        // Therefore, `index < self.field_defaults.len()` always holds here, so
        // `self.field_defaults[index]` cannot panic. We only take an immutable reference
        // via `.as_ref()`, and `self` is borrowed immutably.
        if let Some(default_literal) = self.field_defaults[index].as_ref() {
            decoder.append_default(default_literal)
        } else {
            decoder.append_null()
        }
    }

    #[inline]
    fn project_record(
        &mut self,
        buf: &mut AvroCursor<'_>,
        encodings: &mut [Decoder],
    ) -> Result<(), ArrowError> {
        debug_assert_eq!(
            self.writer_to_reader.len(),
            self.skip_decoders.len(),
            "internal invariant: mapping and skipper lists must have equal length"
        );
        for (i, (mapping, skipper_opt)) in self
            .writer_to_reader
            .iter()
            .zip(self.skip_decoders.iter_mut())
            .enumerate()
        {
            match (mapping, skipper_opt.as_mut()) {
                (Some(reader_index), _) => encodings[*reader_index].decode(buf)?,
                (None, Some(skipper)) => skipper.skip(buf)?,
                (None, None) => {
                    return Err(ArrowError::SchemaError(format!(
                        "No skipper available for writer-only field at index {i}",
                    )));
                }
            }
        }
        for (reader_index, lit) in self.default_injections.as_ref() {
            encodings[*reader_index].append_default(lit)?;
        }
        Ok(())
    }
}

/// Lightweight skipper for non‑projected writer fields
/// (fields present in the writer schema but omitted by the reader/projection);
/// per Avro 1.11.1 schema resolution these fields are ignored.
///
/// <https://avro.apache.org/docs/1.11.1/specification/#schema-resolution>
#[derive(Debug)]
enum Skipper {
    Null,
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Bytes,
    String,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    Fixed(usize),
    Decimal(Option<usize>),
    UuidString,
    Enum,
    DurationFixed12,
    List(Box<Skipper>),
    Map(Box<Skipper>),
    Struct(Vec<Skipper>),
    Union(Vec<Skipper>),
    Nullable(Nullability, Box<Skipper>),
    #[cfg(feature = "avro_custom_types")]
    RunEndEncoded(Box<Skipper>),
}

impl Skipper {
    fn from_avro(dt: &AvroDataType) -> Result<Self, ArrowError> {
        let mut base = match dt.codec() {
            Codec::Null => Self::Null,
            Codec::Boolean => Self::Boolean,
            Codec::Int32 | Codec::Date32 | Codec::TimeMillis => Self::Int32,
            Codec::Int64 => Self::Int64,
            Codec::TimeMicros => Self::TimeMicros,
            Codec::TimestampMillis(_) => Self::TimestampMillis,
            Codec::TimestampMicros(_) => Self::TimestampMicros,
            Codec::TimestampNanos(_) => Self::TimestampNanos,
            #[cfg(feature = "avro_custom_types")]
            Codec::DurationNanos
            | Codec::DurationMicros
            | Codec::DurationMillis
            | Codec::DurationSeconds => Self::Int64,
            Codec::Float32 => Self::Float32,
            Codec::Float64 => Self::Float64,
            Codec::Binary => Self::Bytes,
            Codec::Utf8 | Codec::Utf8View => Self::String,
            Codec::Fixed(sz) => Self::Fixed(*sz as usize),
            Codec::Decimal(_, _, size) => Self::Decimal(*size),
            Codec::Uuid => Self::UuidString, // encoded as string
            Codec::Enum(_) => Self::Enum,
            Codec::List(item) => Self::List(Box::new(Skipper::from_avro(item)?)),
            Codec::Struct(fields) => Self::Struct(
                fields
                    .iter()
                    .map(|f| Skipper::from_avro(f.data_type()))
                    .collect::<Result<_, _>>()?,
            ),
            Codec::Map(values) => Self::Map(Box::new(Skipper::from_avro(values)?)),
            Codec::Interval => Self::DurationFixed12,
            Codec::Union(encodings, _, _) => {
                let max_addr = (i32::MAX as usize) + 1;
                if encodings.len() > max_addr {
                    return Err(ArrowError::SchemaError(format!(
                        "Writer union has {} branches, which exceeds the maximum addressable \
                         branches by an Avro int tag ({} + 1).",
                        encodings.len(),
                        i32::MAX
                    )));
                }
                Self::Union(
                    encodings
                        .iter()
                        .map(Skipper::from_avro)
                        .collect::<Result<_, _>>()?,
                )
            }
            #[cfg(feature = "avro_custom_types")]
            Codec::RunEndEncoded(inner, _w) => {
                Self::RunEndEncoded(Box::new(Skipper::from_avro(inner)?))
            }
        };
        if let Some(n) = dt.nullability() {
            base = Self::Nullable(n, Box::new(base));
        }
        Ok(base)
    }

    fn skip(&mut self, buf: &mut AvroCursor<'_>) -> Result<(), ArrowError> {
        match self {
            Self::Null => Ok(()),
            Self::Boolean => {
                buf.get_bool()?;
                Ok(())
            }
            Self::Int32 => {
                buf.get_int()?;
                Ok(())
            }
            Self::Int64
            | Self::TimeMicros
            | Self::TimestampMillis
            | Self::TimestampMicros
            | Self::TimestampNanos => {
                buf.get_long()?;
                Ok(())
            }
            Self::Float32 => {
                buf.get_float()?;
                Ok(())
            }
            Self::Float64 => {
                buf.get_double()?;
                Ok(())
            }
            Self::Bytes | Self::String | Self::UuidString => {
                buf.get_bytes()?;
                Ok(())
            }
            Self::Fixed(sz) => {
                buf.get_fixed(*sz)?;
                Ok(())
            }
            Self::Decimal(size) => {
                if let Some(s) = size {
                    buf.get_fixed(*s)
                } else {
                    buf.get_bytes()
                }?;
                Ok(())
            }
            Self::Enum => {
                buf.get_int()?;
                Ok(())
            }
            Self::DurationFixed12 => {
                buf.get_fixed(12)?;
                Ok(())
            }
            Self::List(item) => {
                skip_blocks(buf, |c| item.skip(c))?;
                Ok(())
            }
            Self::Map(value) => {
                skip_blocks(buf, |c| {
                    c.get_bytes()?; // key
                    value.skip(c)
                })?;
                Ok(())
            }
            Self::Struct(fields) => {
                for f in fields.iter_mut() {
                    f.skip(buf)?
                }
                Ok(())
            }
            Self::Union(encodings) => {
                // Union tag must be ZigZag-decoded
                let raw = buf.get_long()?;
                if raw < 0 {
                    return Err(ArrowError::ParseError(format!(
                        "Negative union branch index {raw}"
                    )));
                }
                let idx: usize = usize::try_from(raw).map_err(|_| {
                    ArrowError::ParseError(format!(
                        "Union branch index {raw} does not fit into usize on this platform ({}-bit)",
                        (usize::BITS as usize)
                    ))
                })?;
                let Some(encoding) = encodings.get_mut(idx) else {
                    return Err(ArrowError::ParseError(format!(
                        "Union branch index {idx} out of range for skipper ({} branches)",
                        encodings.len()
                    )));
                };
                encoding.skip(buf)
            }
            Self::Nullable(order, inner) => {
                let branch = buf.read_vlq()?;
                let is_not_null = match *order {
                    Nullability::NullFirst => branch != 0,
                    Nullability::NullSecond => branch == 0,
                };
                if is_not_null {
                    inner.skip(buf)?;
                }
                Ok(())
            }
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(inner) => inner.skip(buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::AvroFieldBuilder;
    use crate::schema::{Attributes, ComplexType, Field, PrimitiveType, Record, Schema, TypeName};
    use arrow_array::cast::AsArray;
    use indexmap::IndexMap;
    use std::collections::HashMap;

    fn encode_avro_int(value: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut v = (value << 1) ^ (value >> 31);
        while v & !0x7F != 0 {
            buf.push(((v & 0x7F) | 0x80) as u8);
            v >>= 7;
        }
        buf.push(v as u8);
        buf
    }

    fn encode_avro_long(value: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut v = (value << 1) ^ (value >> 63);
        while v & !0x7F != 0 {
            buf.push(((v & 0x7F) | 0x80) as u8);
            v >>= 7;
        }
        buf.push(v as u8);
        buf
    }

    fn encode_avro_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut buf = encode_avro_long(bytes.len() as i64);
        buf.extend_from_slice(bytes);
        buf
    }

    fn avro_from_codec(codec: Codec) -> AvroDataType {
        AvroDataType::new(codec, Default::default(), None)
    }

    fn resolved_root_datatype(
        writer: Schema<'static>,
        reader: Schema<'static>,
        use_utf8view: bool,
        strict_mode: bool,
    ) -> AvroDataType {
        // Wrap writer schema in a single-field record
        let writer_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: writer,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));

        // Wrap reader schema in a single-field record
        let reader_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: reader,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));

        // Build resolved record, then extract the inner field's resolved AvroDataType
        let field = AvroFieldBuilder::new(&writer_record)
            .with_reader_schema(&reader_record)
            .with_utf8view(use_utf8view)
            .with_strict_mode(strict_mode)
            .build()
            .expect("schema resolution should succeed");

        match field.data_type().codec() {
            Codec::Struct(fields) => fields[0].data_type().clone(),
            other => panic!("expected wrapper struct, got {other:?}"),
        }
    }

    fn decoder_for_promotion(
        writer: PrimitiveType,
        reader: PrimitiveType,
        use_utf8view: bool,
    ) -> Decoder {
        let ws = Schema::TypeName(TypeName::Primitive(writer));
        let rs = Schema::TypeName(TypeName::Primitive(reader));
        let dt = resolved_root_datatype(ws, rs, use_utf8view, false);
        Decoder::try_new(&dt).unwrap()
    }

    fn make_avro_dt(codec: Codec, nullability: Option<Nullability>) -> AvroDataType {
        AvroDataType::new(codec, HashMap::new(), nullability)
    }

    #[cfg(feature = "avro_custom_types")]
    fn encode_vlq_u64(mut x: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
        while x >= 0x80 {
            out.push((x as u8) | 0x80);
            x >>= 7;
        }
        out.push(x as u8);
        out
    }

    #[test]
    fn test_union_resolution_writer_union_reader_union_reorder_and_promotion_dense() {
        let ws = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
        ]);
        let rs = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
        ]);

        let dt = resolved_root_datatype(ws, rs, false, false);
        let mut dec = Decoder::try_new(&dt).unwrap();

        let mut rec1 = encode_avro_long(0);
        rec1.extend(encode_avro_int(7));
        let mut cur1 = AvroCursor::new(&rec1);
        dec.decode(&mut cur1).unwrap();

        let mut rec2 = encode_avro_long(1);
        rec2.extend(encode_avro_bytes("abc".as_bytes()));
        let mut cur2 = AvroCursor::new(&rec2);
        dec.decode(&mut cur2).unwrap();

        let arr = dec.flush(None).unwrap();
        let ua = arr
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("dense union output");

        assert_eq!(
            ua.type_id(0),
            1,
            "first value must select reader 'long' branch"
        );
        assert_eq!(ua.value_offset(0), 0);

        assert_eq!(
            ua.type_id(1),
            0,
            "second value must select reader 'string' branch"
        );
        assert_eq!(ua.value_offset(1), 0);

        let long_child = ua.child(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(long_child.len(), 1);
        assert_eq!(long_child.value(0), 7);

        let str_child = ua.child(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_child.len(), 1);
        assert_eq!(str_child.value(0), "abc");
    }

    #[test]
    fn test_union_resolution_writer_union_reader_nonunion_promotion_int_to_long() {
        let ws = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
        ]);
        let rs = Schema::TypeName(TypeName::Primitive(PrimitiveType::Long));

        let dt = resolved_root_datatype(ws, rs, false, false);
        let mut dec = Decoder::try_new(&dt).unwrap();

        let mut data = encode_avro_long(0);
        data.extend(encode_avro_int(5));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();

        let arr = dec.flush(None).unwrap();
        let out = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out.value(0), 5);
    }

    #[test]
    fn test_union_resolution_writer_union_reader_nonunion_mismatch_errors() {
        let ws = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
        ]);
        let rs = Schema::TypeName(TypeName::Primitive(PrimitiveType::Long));

        let dt = resolved_root_datatype(ws, rs, false, false);
        let mut dec = Decoder::try_new(&dt).unwrap();

        let mut data = encode_avro_long(1);
        data.extend(encode_avro_bytes("z".as_bytes()));
        let mut cur = AvroCursor::new(&data);
        let res = dec.decode(&mut cur);
        assert!(
            res.is_err(),
            "expected error when writer union branch does not resolve to reader non-union type"
        );
    }

    #[test]
    fn test_union_resolution_writer_nonunion_reader_union_selects_matching_branch() {
        let ws = Schema::TypeName(TypeName::Primitive(PrimitiveType::Int));
        let rs = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
        ]);

        let dt = resolved_root_datatype(ws, rs, false, false);
        let mut dec = Decoder::try_new(&dt).unwrap();

        let data = encode_avro_int(6);
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();

        let arr = dec.flush(None).unwrap();
        let ua = arr
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("dense union output");
        assert_eq!(ua.len(), 1);
        assert_eq!(
            ua.type_id(0),
            1,
            "must resolve to reader 'long' branch (type_id 1)"
        );
        assert_eq!(ua.value_offset(0), 0);

        let long_child = ua.child(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(long_child.len(), 1);
        assert_eq!(long_child.value(0), 6);

        let str_child = ua.child(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_child.len(), 0, "string branch must be empty");
    }

    #[test]
    fn test_union_resolution_writer_union_reader_union_unmapped_branch_errors() {
        let ws = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Int)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Boolean)),
        ]);
        let rs = Schema::Union(vec![
            Schema::TypeName(TypeName::Primitive(PrimitiveType::String)),
            Schema::TypeName(TypeName::Primitive(PrimitiveType::Long)),
        ]);

        let dt = resolved_root_datatype(ws, rs, false, false);
        let mut dec = Decoder::try_new(&dt).unwrap();

        let mut data = encode_avro_long(1);
        data.push(1);
        let mut cur = AvroCursor::new(&data);
        let res = dec.decode(&mut cur);
        assert!(
            res.is_err(),
            "expected error for unmapped writer 'boolean' branch"
        );
    }

    #[test]
    fn test_schema_resolution_promotion_int_to_long() {
        let mut dec = decoder_for_promotion(PrimitiveType::Int, PrimitiveType::Long, false);
        assert!(matches!(dec, Decoder::Int32ToInt64(_)));
        for v in [0, 1, -2, 123456] {
            let data = encode_avro_int(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 0);
        assert_eq!(a.value(1), 1);
        assert_eq!(a.value(2), -2);
        assert_eq!(a.value(3), 123456);
    }

    #[test]
    fn test_schema_resolution_promotion_int_to_float() {
        let mut dec = decoder_for_promotion(PrimitiveType::Int, PrimitiveType::Float, false);
        assert!(matches!(dec, Decoder::Int32ToFloat32(_)));
        for v in [0, 42, -7] {
            let data = encode_avro_int(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(a.value(0), 0.0);
        assert_eq!(a.value(1), 42.0);
        assert_eq!(a.value(2), -7.0);
    }

    #[test]
    fn test_schema_resolution_promotion_int_to_double() {
        let mut dec = decoder_for_promotion(PrimitiveType::Int, PrimitiveType::Double, false);
        assert!(matches!(dec, Decoder::Int32ToFloat64(_)));
        for v in [1, -1, 10_000] {
            let data = encode_avro_int(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a.value(0), 1.0);
        assert_eq!(a.value(1), -1.0);
        assert_eq!(a.value(2), 10_000.0);
    }

    #[test]
    fn test_schema_resolution_promotion_long_to_float() {
        let mut dec = decoder_for_promotion(PrimitiveType::Long, PrimitiveType::Float, false);
        assert!(matches!(dec, Decoder::Int64ToFloat32(_)));
        for v in [0_i64, 1_000_000_i64, -123_i64] {
            let data = encode_avro_long(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(a.value(0), 0.0);
        assert_eq!(a.value(1), 1_000_000.0);
        assert_eq!(a.value(2), -123.0);
    }

    #[test]
    fn test_schema_resolution_promotion_long_to_double() {
        let mut dec = decoder_for_promotion(PrimitiveType::Long, PrimitiveType::Double, false);
        assert!(matches!(dec, Decoder::Int64ToFloat64(_)));
        for v in [2_i64, -2_i64, 9_223_372_i64] {
            let data = encode_avro_long(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a.value(0), 2.0);
        assert_eq!(a.value(1), -2.0);
        assert_eq!(a.value(2), 9_223_372.0);
    }

    #[test]
    fn test_schema_resolution_promotion_float_to_double() {
        let mut dec = decoder_for_promotion(PrimitiveType::Float, PrimitiveType::Double, false);
        assert!(matches!(dec, Decoder::Float32ToFloat64(_)));
        for v in [0.5_f32, -3.25_f32, 1.0e6_f32] {
            let data = v.to_le_bytes().to_vec();
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a.value(0), 0.5_f64);
        assert_eq!(a.value(1), -3.25_f64);
        assert_eq!(a.value(2), 1.0e6_f64);
    }

    #[test]
    fn test_schema_resolution_promotion_bytes_to_string_utf8() {
        let mut dec = decoder_for_promotion(PrimitiveType::Bytes, PrimitiveType::String, false);
        assert!(matches!(dec, Decoder::BytesToString(_, _)));
        for s in ["hello", "world", "héllo"] {
            let data = encode_avro_bytes(s.as_bytes());
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), "hello");
        assert_eq!(a.value(1), "world");
        assert_eq!(a.value(2), "héllo");
    }

    #[test]
    fn test_schema_resolution_promotion_bytes_to_string_utf8view_enabled() {
        let mut dec = decoder_for_promotion(PrimitiveType::Bytes, PrimitiveType::String, true);
        assert!(matches!(dec, Decoder::BytesToString(_, _)));
        let data = encode_avro_bytes("abc".as_bytes());
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), "abc");
    }

    #[test]
    fn test_schema_resolution_promotion_string_to_bytes() {
        let mut dec = decoder_for_promotion(PrimitiveType::String, PrimitiveType::Bytes, false);
        assert!(matches!(dec, Decoder::StringToBytes(_, _)));
        for s in ["", "abc", "data"] {
            let data = encode_avro_bytes(s.as_bytes());
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(a.value(0), b"");
        assert_eq!(a.value(1), b"abc");
        assert_eq!(a.value(2), "data".as_bytes());
    }

    #[test]
    fn test_schema_resolution_no_promotion_passthrough_int() {
        let ws = Schema::TypeName(TypeName::Primitive(PrimitiveType::Int));
        let rs = Schema::TypeName(TypeName::Primitive(PrimitiveType::Int));
        // Wrap both in a synthetic single-field record and resolve with AvroFieldBuilder
        let writer_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: ws,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));
        let reader_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: rs,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));
        let field = AvroFieldBuilder::new(&writer_record)
            .with_reader_schema(&reader_record)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build()
            .unwrap();
        // Extract the resolved inner field's AvroDataType
        let dt = match field.data_type().codec() {
            Codec::Struct(fields) => fields[0].data_type().clone(),
            other => panic!("expected wrapper struct, got {other:?}"),
        };
        let mut dec = Decoder::try_new(&dt).unwrap();
        assert!(matches!(dec, Decoder::Int32(_)));
        for v in [7, -9] {
            let data = encode_avro_int(v);
            let mut cur = AvroCursor::new(&data);
            dec.decode(&mut cur).unwrap();
        }
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 7);
        assert_eq!(a.value(1), -9);
    }

    #[test]
    fn test_schema_resolution_illegal_promotion_int_to_boolean_errors() {
        let ws = Schema::TypeName(TypeName::Primitive(PrimitiveType::Int));
        let rs = Schema::TypeName(TypeName::Primitive(PrimitiveType::Boolean));
        let writer_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: ws,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));
        let reader_record = Schema::Complex(ComplexType::Record(Record {
            name: "Root",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![Field {
                name: "v",
                r#type: rs,
                default: None,
                doc: None,
                aliases: vec![],
            }],
            attributes: Attributes::default(),
        }));
        let res = AvroFieldBuilder::new(&writer_record)
            .with_reader_schema(&reader_record)
            .with_utf8view(false)
            .with_strict_mode(false)
            .build();
        assert!(res.is_err(), "expected error for illegal promotion");
    }

    #[test]
    fn test_map_decoding_one_entry() {
        let value_type = avro_from_codec(Codec::Utf8);
        let map_type = avro_from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Encode a single map with one entry: {"hello": "world"}
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_bytes(b"hello")); // key
        data.extend_from_slice(&encode_avro_bytes(b"world")); // value
        data.extend_from_slice(&encode_avro_long(0));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1); // one map
        assert_eq!(map_arr.value_length(0), 1);
        let entries = map_arr.value(0);
        let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_entries.len(), 1);
        let key_arr = struct_entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val_arr = struct_entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_arr.value(0), "hello");
        assert_eq!(val_arr.value(0), "world");
    }

    #[test]
    fn test_map_decoding_empty() {
        let value_type = avro_from_codec(Codec::Utf8);
        let map_type = avro_from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        let data = encode_avro_long(0);
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);
        assert_eq!(map_arr.value_length(0), 0);
    }

    #[test]
    fn test_fixed_decoding() {
        let avro_type = avro_from_codec(Codec::Fixed(3));
        let mut decoder = Decoder::try_new(&avro_type).expect("Failed to create decoder");

        let data1 = [1u8, 2, 3];
        let mut cursor1 = AvroCursor::new(&data1);
        decoder
            .decode(&mut cursor1)
            .expect("Failed to decode data1");
        assert_eq!(cursor1.position(), 3, "Cursor should advance by fixed size");
        let data2 = [4u8, 5, 6];
        let mut cursor2 = AvroCursor::new(&data2);
        decoder
            .decode(&mut cursor2)
            .expect("Failed to decode data2");
        assert_eq!(cursor2.position(), 3, "Cursor should advance by fixed size");
        let array = decoder.flush(None).expect("Failed to flush decoder");
        assert_eq!(array.len(), 2, "Array should contain two items");
        let fixed_size_binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Failed to downcast to FixedSizeBinaryArray");
        assert_eq!(
            fixed_size_binary_array.value_length(),
            3,
            "Fixed size of binary values should be 3"
        );
        assert_eq!(
            fixed_size_binary_array.value(0),
            &[1, 2, 3],
            "First item mismatch"
        );
        assert_eq!(
            fixed_size_binary_array.value(1),
            &[4, 5, 6],
            "Second item mismatch"
        );
    }

    #[test]
    fn test_fixed_decoding_empty() {
        let avro_type = avro_from_codec(Codec::Fixed(5));
        let mut decoder = Decoder::try_new(&avro_type).expect("Failed to create decoder");

        let array = decoder
            .flush(None)
            .expect("Failed to flush decoder for empty input");

        assert_eq!(array.len(), 0, "Array should be empty");
        let fixed_size_binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Failed to downcast to FixedSizeBinaryArray for empty array");

        assert_eq!(
            fixed_size_binary_array.value_length(),
            5,
            "Fixed size of binary values should be 5 as per type"
        );
    }

    #[test]
    fn test_uuid_decoding() {
        let avro_type = avro_from_codec(Codec::Uuid);
        let mut decoder = Decoder::try_new(&avro_type).expect("Failed to create decoder");
        let uuid_str = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
        let data = encode_avro_bytes(uuid_str.as_bytes());
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).expect("Failed to decode data");
        assert_eq!(
            cursor.position(),
            data.len(),
            "Cursor should advance by varint size + data size"
        );
        let array = decoder.flush(None).expect("Failed to flush decoder");
        let fixed_size_binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Array should be a FixedSizeBinaryArray");
        assert_eq!(fixed_size_binary_array.len(), 1);
        assert_eq!(fixed_size_binary_array.value_length(), 16);
        let expected_bytes = [
            0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e,
            0x6b, 0xf6,
        ];
        assert_eq!(fixed_size_binary_array.value(0), &expected_bytes);
    }

    #[test]
    fn test_array_decoding() {
        let item_dt = avro_from_codec(Codec::Int32);
        let list_dt = avro_from_codec(Codec::List(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt).unwrap();
        let mut row1 = Vec::new();
        row1.extend_from_slice(&encode_avro_long(2));
        row1.extend_from_slice(&encode_avro_int(10));
        row1.extend_from_slice(&encode_avro_int(20));
        row1.extend_from_slice(&encode_avro_long(0));
        let row2 = encode_avro_long(0);
        let mut cursor = AvroCursor::new(&row1);
        decoder.decode(&mut cursor).unwrap();
        let mut cursor2 = AvroCursor::new(&row2);
        decoder.decode(&mut cursor2).unwrap();
        let array = decoder.flush(None).unwrap();
        let list_arr = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 2);
        let offsets = list_arr.value_offsets();
        assert_eq!(offsets, &[0, 2, 2]);
        let values = list_arr.values();
        let int_arr = values.as_primitive::<Int32Type>();
        assert_eq!(int_arr.len(), 2);
        assert_eq!(int_arr.value(0), 10);
        assert_eq!(int_arr.value(1), 20);
    }

    #[test]
    fn test_array_decoding_with_negative_block_count() {
        let item_dt = avro_from_codec(Codec::Int32);
        let list_dt = avro_from_codec(Codec::List(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt).unwrap();
        let mut data = encode_avro_long(-3);
        data.extend_from_slice(&encode_avro_long(12));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(2));
        data.extend_from_slice(&encode_avro_int(3));
        data.extend_from_slice(&encode_avro_long(0));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let list_arr = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);
        assert_eq!(list_arr.value_length(0), 3);
        let values = list_arr.values().as_primitive::<Int32Type>();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn test_nested_array_decoding() {
        let inner_ty = avro_from_codec(Codec::List(Arc::new(avro_from_codec(Codec::Int32))));
        let nested_ty = avro_from_codec(Codec::List(Arc::new(inner_ty.clone())));
        let mut decoder = Decoder::try_new(&nested_ty).unwrap();
        let mut buf = Vec::new();
        buf.extend(encode_avro_long(1));
        buf.extend(encode_avro_long(2));
        buf.extend(encode_avro_int(5));
        buf.extend(encode_avro_int(6));
        buf.extend(encode_avro_long(0));
        buf.extend(encode_avro_long(0));
        let mut cursor = AvroCursor::new(&buf);
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        let outer = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(outer.len(), 1);
        assert_eq!(outer.value_length(0), 1);
        let inner = outer.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner.value_length(0), 2);
        let values = inner
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[5, 6]);
    }

    #[test]
    fn test_array_decoding_empty_array() {
        let value_type = avro_from_codec(Codec::Utf8);
        let map_type = avro_from_codec(Codec::List(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        let data = encode_avro_long(0);
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let list_arr = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);
        assert_eq!(list_arr.value_length(0), 0);
    }

    #[test]
    fn test_decimal_decoding_fixed256() {
        let dt = avro_from_codec(Codec::Decimal(50, Some(2), Some(32)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        let row1 = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x30, 0x39,
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0x85,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        let dec = arr.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(dec.len(), 2);
        assert_eq!(dec.value_as_string(0), "123.45");
        assert_eq!(dec.value_as_string(1), "-1.23");
    }

    #[test]
    fn test_decimal_decoding_fixed128() {
        let dt = avro_from_codec(Codec::Decimal(28, Some(2), Some(16)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        let row1 = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x30, 0x39,
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0x85,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        let dec = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.len(), 2);
        assert_eq!(dec.value_as_string(0), "123.45");
        assert_eq!(dec.value_as_string(1), "-1.23");
    }

    #[test]
    fn test_decimal_decoding_fixed32_from_32byte_fixed_storage() {
        let dt = avro_from_codec(Codec::Decimal(5, Some(2), Some(32)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        let row1 = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x30, 0x39,
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0x85,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        #[cfg(feature = "small_decimals")]
        {
            let dec = arr.as_any().downcast_ref::<Decimal32Array>().unwrap();
            assert_eq!(dec.len(), 2);
            assert_eq!(dec.value_as_string(0), "123.45");
            assert_eq!(dec.value_as_string(1), "-1.23");
        }
        #[cfg(not(feature = "small_decimals"))]
        {
            let dec = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(dec.len(), 2);
            assert_eq!(dec.value_as_string(0), "123.45");
            assert_eq!(dec.value_as_string(1), "-1.23");
        }
    }

    #[test]
    fn test_decimal_decoding_fixed32_from_16byte_fixed_storage() {
        let dt = avro_from_codec(Codec::Decimal(5, Some(2), Some(16)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        let row1 = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x30, 0x39,
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0x85,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();

        let arr = decoder.flush(None).unwrap();
        #[cfg(feature = "small_decimals")]
        {
            let dec = arr.as_any().downcast_ref::<Decimal32Array>().unwrap();
            assert_eq!(dec.len(), 2);
            assert_eq!(dec.value_as_string(0), "123.45");
            assert_eq!(dec.value_as_string(1), "-1.23");
        }
        #[cfg(not(feature = "small_decimals"))]
        {
            let dec = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(dec.len(), 2);
            assert_eq!(dec.value_as_string(0), "123.45");
            assert_eq!(dec.value_as_string(1), "-1.23");
        }
    }

    #[test]
    fn test_decimal_decoding_bytes_with_nulls() {
        let dt = avro_from_codec(Codec::Decimal(4, Some(1), None));
        let inner = Decoder::try_new(&dt).unwrap();
        let mut decoder = Decoder::Nullable(
            Nullability::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner),
            NullablePlan::ReadTag,
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_bytes(&[0x04, 0xD2]));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_bytes(&[0xFB, 0x2E]));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        #[cfg(feature = "small_decimals")]
        {
            let dec_arr = arr.as_any().downcast_ref::<Decimal32Array>().unwrap();
            assert_eq!(dec_arr.len(), 3);
            assert!(dec_arr.is_valid(0));
            assert!(!dec_arr.is_valid(1));
            assert!(dec_arr.is_valid(2));
            assert_eq!(dec_arr.value_as_string(0), "123.4");
            assert_eq!(dec_arr.value_as_string(2), "-123.4");
        }
        #[cfg(not(feature = "small_decimals"))]
        {
            let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(dec_arr.len(), 3);
            assert!(dec_arr.is_valid(0));
            assert!(!dec_arr.is_valid(1));
            assert!(dec_arr.is_valid(2));
            assert_eq!(dec_arr.value_as_string(0), "123.4");
            assert_eq!(dec_arr.value_as_string(2), "-123.4");
        }
    }

    #[test]
    fn test_decimal_decoding_bytes_with_nulls_fixed_size_narrow_result() {
        let dt = avro_from_codec(Codec::Decimal(6, Some(2), Some(16)));
        let inner = Decoder::try_new(&dt).unwrap();
        let mut decoder = Decoder::Nullable(
            Nullability::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner),
            NullablePlan::ReadTag,
        );
        let row1 = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0xE2, 0x40,
        ];
        let row3 = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
            0x1D, 0xC0,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row1);
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row3);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        #[cfg(feature = "small_decimals")]
        {
            let dec_arr = arr.as_any().downcast_ref::<Decimal32Array>().unwrap();
            assert_eq!(dec_arr.len(), 3);
            assert!(dec_arr.is_valid(0));
            assert!(!dec_arr.is_valid(1));
            assert!(dec_arr.is_valid(2));
            assert_eq!(dec_arr.value_as_string(0), "1234.56");
            assert_eq!(dec_arr.value_as_string(2), "-1234.56");
        }
        #[cfg(not(feature = "small_decimals"))]
        {
            let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(dec_arr.len(), 3);
            assert!(dec_arr.is_valid(0));
            assert!(!dec_arr.is_valid(1));
            assert!(dec_arr.is_valid(2));
            assert_eq!(dec_arr.value_as_string(0), "1234.56");
            assert_eq!(dec_arr.value_as_string(2), "-1234.56");
        }
    }

    #[test]
    fn test_enum_decoding() {
        let symbols: Arc<[String]> = vec!["A", "B", "C"].into_iter().map(String::from).collect();
        let avro_type = avro_from_codec(Codec::Enum(symbols.clone()));
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(2));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(1));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict_array.len(), 3);
        let values = dict_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "A");
        assert_eq!(values.value(1), "B");
        assert_eq!(values.value(2), "C");
        assert_eq!(dict_array.keys().values(), &[2, 0, 1]);
    }

    #[test]
    fn test_enum_decoding_with_nulls() {
        let symbols: Arc<[String]> = vec!["X", "Y"].into_iter().map(String::from).collect();
        let enum_codec = Codec::Enum(symbols.clone());
        let avro_type =
            AvroDataType::new(enum_codec, Default::default(), Some(Nullability::NullFirst));
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_int(0));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict_array.len(), 3);
        assert!(dict_array.is_valid(0));
        assert!(dict_array.is_null(1));
        assert!(dict_array.is_valid(2));
        let expected_keys = Int32Array::from(vec![Some(1), None, Some(0)]);
        assert_eq!(dict_array.keys(), &expected_keys);
        let values = dict_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "X");
        assert_eq!(values.value(1), "Y");
    }

    #[test]
    fn test_duration_decoding_with_nulls() {
        let duration_codec = Codec::Interval;
        let avro_type = AvroDataType::new(
            duration_codec,
            Default::default(),
            Some(Nullability::NullFirst),
        );
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        // First value: 1 month, 2 days, 3 millis
        data.extend_from_slice(&encode_avro_long(1)); // not null
        let mut duration1 = Vec::new();
        duration1.extend_from_slice(&1u32.to_le_bytes());
        duration1.extend_from_slice(&2u32.to_le_bytes());
        duration1.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&duration1);
        // Second value: null
        data.extend_from_slice(&encode_avro_long(0)); // null
        data.extend_from_slice(&encode_avro_long(1)); // not null
        let mut duration2 = Vec::new();
        duration2.extend_from_slice(&4u32.to_le_bytes());
        duration2.extend_from_slice(&5u32.to_le_bytes());
        duration2.extend_from_slice(&6u32.to_le_bytes());
        data.extend_from_slice(&duration2);
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let interval_array = array
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(interval_array.len(), 3);
        assert!(interval_array.is_valid(0));
        assert!(interval_array.is_null(1));
        assert!(interval_array.is_valid(2));
        let expected = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano {
                months: 1,
                days: 2,
                nanoseconds: 3_000_000,
            }),
            None,
            Some(IntervalMonthDayNano {
                months: 4,
                days: 5,
                nanoseconds: 6_000_000,
            }),
        ]);
        assert_eq!(interval_array, &expected);
    }

    #[test]
    fn test_duration_decoding_empty() {
        let duration_codec = Codec::Interval;
        let avro_type = AvroDataType::new(duration_codec, Default::default(), None);
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let array = decoder.flush(None).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn test_duration_seconds_decoding() {
        let avro_type = AvroDataType::new(Codec::DurationSeconds, Default::default(), None);
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        // Three values: 0, -1, 2
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_avro_long(-1));
        data.extend_from_slice(&encode_avro_long(2));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let dur = array
            .as_any()
            .downcast_ref::<DurationSecondArray>()
            .unwrap();
        assert_eq!(dur.values(), &[0, -1, 2]);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn test_duration_milliseconds_decoding() {
        let avro_type = AvroDataType::new(Codec::DurationMillis, Default::default(), None);
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        for v in [1i64, 0, -2] {
            data.extend_from_slice(&encode_avro_long(v));
        }
        let mut cursor = AvroCursor::new(&data);
        for _ in 0..3 {
            decoder.decode(&mut cursor).unwrap();
        }
        let array = decoder.flush(None).unwrap();
        let dur = array
            .as_any()
            .downcast_ref::<DurationMillisecondArray>()
            .unwrap();
        assert_eq!(dur.values(), &[1, 0, -2]);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn test_duration_microseconds_decoding() {
        let avro_type = AvroDataType::new(Codec::DurationMicros, Default::default(), None);
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        for v in [5i64, -6, 7] {
            data.extend_from_slice(&encode_avro_long(v));
        }
        let mut cursor = AvroCursor::new(&data);
        for _ in 0..3 {
            decoder.decode(&mut cursor).unwrap();
        }
        let array = decoder.flush(None).unwrap();
        let dur = array
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .unwrap();
        assert_eq!(dur.values(), &[5, -6, 7]);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn test_duration_nanoseconds_decoding() {
        let avro_type = AvroDataType::new(Codec::DurationNanos, Default::default(), None);
        let mut decoder = Decoder::try_new(&avro_type).unwrap();
        let mut data = Vec::new();
        for v in [8i64, 9, -10] {
            data.extend_from_slice(&encode_avro_long(v));
        }
        let mut cursor = AvroCursor::new(&data);
        for _ in 0..3 {
            decoder.decode(&mut cursor).unwrap();
        }
        let array = decoder.flush(None).unwrap();
        let dur = array
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .unwrap();
        assert_eq!(dur.values(), &[8, 9, -10]);
    }

    #[test]
    fn test_nullable_decode_error_bitmap_corruption() {
        // Nullable Int32 with ['T','null'] encoding (NullSecond)
        let avro_type = AvroDataType::new(
            Codec::Int32,
            Default::default(),
            Some(Nullability::NullSecond),
        );
        let mut decoder = Decoder::try_new(&avro_type).unwrap();

        // Row 1: union branch 1 (null)
        let mut row1 = Vec::new();
        row1.extend_from_slice(&encode_avro_int(1));

        // Row 2: union branch 0 (non-null) but missing the int payload -> decode error
        let mut row2 = Vec::new();
        row2.extend_from_slice(&encode_avro_int(0)); // branch = 0 => non-null

        // Row 3: union branch 0 (non-null) with correct int payload -> should succeed
        let mut row3 = Vec::new();
        row3.extend_from_slice(&encode_avro_int(0)); // branch
        row3.extend_from_slice(&encode_avro_int(42)); // actual value

        decoder.decode(&mut AvroCursor::new(&row1)).unwrap();
        assert!(decoder.decode(&mut AvroCursor::new(&row2)).is_err()); // decode error
        decoder.decode(&mut AvroCursor::new(&row3)).unwrap();

        let array = decoder.flush(None).unwrap();

        // Should contain 2 elements: row1 (null) and row3 (42)
        assert_eq!(array.len(), 2);
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(int_array.is_null(0)); // row1 is null
        assert_eq!(int_array.value(1), 42); // row3 value is 42
    }

    #[test]
    fn test_enum_mapping_reordered_symbols() {
        let reader_symbols: Arc<[String]> =
            vec!["B".to_string(), "C".to_string(), "A".to_string()].into();
        let mapping: Arc<[i32]> = Arc::from(vec![2, 0, 1]);
        let default_index: i32 = -1;
        let mut dec = Decoder::Enum(
            Vec::with_capacity(DEFAULT_CAPACITY),
            reader_symbols.clone(),
            Some(EnumResolution {
                mapping,
                default_index,
            }),
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(2));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        dec.decode(&mut cur).unwrap();
        dec.decode(&mut cur).unwrap();
        let arr = dec.flush(None).unwrap();
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let expected_keys = Int32Array::from(vec![2, 0, 1]);
        assert_eq!(dict.keys(), &expected_keys);
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "B");
        assert_eq!(values.value(1), "C");
        assert_eq!(values.value(2), "A");
    }

    #[test]
    fn test_enum_mapping_unknown_symbol_and_out_of_range_fall_back_to_default() {
        let reader_symbols: Arc<[String]> = vec!["A".to_string(), "B".to_string()].into();
        let default_index: i32 = 1;
        let mapping: Arc<[i32]> = Arc::from(vec![0, 1]);
        let mut dec = Decoder::Enum(
            Vec::with_capacity(DEFAULT_CAPACITY),
            reader_symbols.clone(),
            Some(EnumResolution {
                mapping,
                default_index,
            }),
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(99));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        dec.decode(&mut cur).unwrap();
        dec.decode(&mut cur).unwrap();
        let arr = dec.flush(None).unwrap();
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let expected_keys = Int32Array::from(vec![0, 1, 1]);
        assert_eq!(dict.keys(), &expected_keys);
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "A");
        assert_eq!(values.value(1), "B");
    }

    #[test]
    fn test_enum_mapping_unknown_symbol_without_default_errors() {
        let reader_symbols: Arc<[String]> = vec!["A".to_string()].into();
        let default_index: i32 = -1; // indicates no default at type-level
        let mapping: Arc<[i32]> = Arc::from(vec![-1]);
        let mut dec = Decoder::Enum(
            Vec::with_capacity(DEFAULT_CAPACITY),
            reader_symbols,
            Some(EnumResolution {
                mapping,
                default_index,
            }),
        );
        let data = encode_avro_int(0);
        let mut cur = AvroCursor::new(&data);
        let err = dec
            .decode(&mut cur)
            .expect_err("expected decode error for unresolved enum without default");
        let msg = err.to_string();
        assert!(
            msg.contains("not resolvable") && msg.contains("no default"),
            "unexpected error message: {msg}"
        );
    }

    fn make_record_resolved_decoder(
        reader_fields: &[(&str, DataType, bool)],
        writer_to_reader: Vec<Option<usize>>,
        skip_decoders: Vec<Option<Skipper>>,
    ) -> Decoder {
        let mut field_refs: Vec<FieldRef> = Vec::with_capacity(reader_fields.len());
        let mut encodings: Vec<Decoder> = Vec::with_capacity(reader_fields.len());
        for (name, dt, nullable) in reader_fields {
            field_refs.push(Arc::new(ArrowField::new(*name, dt.clone(), *nullable)));
            let enc = match dt {
                DataType::Int32 => Decoder::Int32(Vec::new()),
                DataType::Int64 => Decoder::Int64(Vec::new()),
                DataType::Utf8 => {
                    Decoder::String(OffsetBufferBuilder::new(DEFAULT_CAPACITY), Vec::new())
                }
                other => panic!("Unsupported test reader field type: {other:?}"),
            };
            encodings.push(enc);
        }
        let fields: Fields = field_refs.into();
        Decoder::Record(
            fields,
            encodings,
            Some(Projector {
                writer_to_reader: Arc::from(writer_to_reader),
                skip_decoders,
                field_defaults: vec![None; reader_fields.len()],
                default_injections: Arc::from(Vec::<(usize, AvroLiteral)>::new()),
            }),
        )
    }

    #[test]
    fn test_skip_writer_trailing_field_int32() {
        let mut dec = make_record_resolved_decoder(
            &[("id", arrow_schema::DataType::Int32, false)],
            vec![Some(0), None],
            vec![None, Some(super::Skipper::Int32)],
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(7));
        data.extend_from_slice(&encode_avro_int(999));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        assert_eq!(cur.position(), data.len());
        let arr = dec.flush(None).unwrap();
        let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_arr.len(), 1);
        let id = struct_arr
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.value(0), 7);
    }

    #[test]
    fn test_skip_writer_middle_field_string() {
        let mut dec = make_record_resolved_decoder(
            &[
                ("id", DataType::Int32, false),
                ("score", DataType::Int64, false),
            ],
            vec![Some(0), None, Some(1)],
            vec![None, Some(Skipper::String), None],
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(42));
        data.extend_from_slice(&encode_avro_bytes(b"abcdef"));
        data.extend_from_slice(&encode_avro_long(1000));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        assert_eq!(cur.position(), data.len());
        let arr = dec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let id = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let score = s
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(0), 42);
        assert_eq!(score.value(0), 1000);
    }

    #[test]
    fn test_skip_writer_array_with_negative_block_count_fast() {
        let mut dec = make_record_resolved_decoder(
            &[("id", DataType::Int32, false)],
            vec![None, Some(0)],
            vec![Some(super::Skipper::List(Box::new(Skipper::Int32))), None],
        );
        let mut array_payload = Vec::new();
        array_payload.extend_from_slice(&encode_avro_int(1));
        array_payload.extend_from_slice(&encode_avro_int(2));
        array_payload.extend_from_slice(&encode_avro_int(3));
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(-3));
        data.extend_from_slice(&encode_avro_long(array_payload.len() as i64));
        data.extend_from_slice(&array_payload);
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_avro_int(5));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        assert_eq!(cur.position(), data.len());
        let arr = dec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let id = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.len(), 1);
        assert_eq!(id.value(0), 5);
    }

    #[test]
    fn test_skip_writer_map_with_negative_block_count_fast() {
        let mut dec = make_record_resolved_decoder(
            &[("id", DataType::Int32, false)],
            vec![None, Some(0)],
            vec![Some(Skipper::Map(Box::new(Skipper::Int32))), None],
        );
        let mut entries = Vec::new();
        entries.extend_from_slice(&encode_avro_bytes(b"k1"));
        entries.extend_from_slice(&encode_avro_int(10));
        entries.extend_from_slice(&encode_avro_bytes(b"k2"));
        entries.extend_from_slice(&encode_avro_int(20));
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(-2));
        data.extend_from_slice(&encode_avro_long(entries.len() as i64));
        data.extend_from_slice(&entries);
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_avro_int(123));
        let mut cur = AvroCursor::new(&data);
        dec.decode(&mut cur).unwrap();
        assert_eq!(cur.position(), data.len());
        let arr = dec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let id = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.len(), 1);
        assert_eq!(id.value(0), 123);
    }

    #[test]
    fn test_skip_writer_nullable_field_union_nullfirst() {
        let mut dec = make_record_resolved_decoder(
            &[("id", DataType::Int32, false)],
            vec![None, Some(0)],
            vec![
                Some(super::Skipper::Nullable(
                    Nullability::NullFirst,
                    Box::new(super::Skipper::Int32),
                )),
                None,
            ],
        );
        let mut row1 = Vec::new();
        row1.extend_from_slice(&encode_avro_long(0));
        row1.extend_from_slice(&encode_avro_int(5));
        let mut row2 = Vec::new();
        row2.extend_from_slice(&encode_avro_long(1));
        row2.extend_from_slice(&encode_avro_int(123));
        row2.extend_from_slice(&encode_avro_int(7));
        let mut cur1 = AvroCursor::new(&row1);
        let mut cur2 = AvroCursor::new(&row2);
        dec.decode(&mut cur1).unwrap();
        dec.decode(&mut cur2).unwrap();
        assert_eq!(cur1.position(), row1.len());
        assert_eq!(cur2.position(), row2.len());
        let arr = dec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let id = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.len(), 2);
        assert_eq!(id.value(0), 5);
        assert_eq!(id.value(1), 7);
    }

    fn make_dense_union_avro(
        children: Vec<(Codec, &'_ str, DataType)>,
        type_ids: Vec<i8>,
    ) -> AvroDataType {
        let mut avro_children: Vec<AvroDataType> = Vec::with_capacity(children.len());
        let mut fields: Vec<arrow_schema::Field> = Vec::with_capacity(children.len());
        for (codec, name, dt) in children.into_iter() {
            avro_children.push(AvroDataType::new(codec, Default::default(), None));
            fields.push(arrow_schema::Field::new(name, dt, true));
        }
        let union_fields = UnionFields::try_new(type_ids, fields).unwrap();
        let union_codec = Codec::Union(avro_children.into(), union_fields, UnionMode::Dense);
        AvroDataType::new(union_codec, Default::default(), None)
    }

    #[test]
    fn test_union_dense_two_children_custom_type_ids() {
        let union_dt = make_dense_union_avro(
            vec![
                (Codec::Int32, "i", DataType::Int32),
                (Codec::Utf8, "s", DataType::Utf8),
            ],
            vec![2, 5],
        );
        let mut dec = Decoder::try_new(&union_dt).unwrap();
        let mut r1 = Vec::new();
        r1.extend_from_slice(&encode_avro_long(0));
        r1.extend_from_slice(&encode_avro_int(7));
        let mut r2 = Vec::new();
        r2.extend_from_slice(&encode_avro_long(1));
        r2.extend_from_slice(&encode_avro_bytes(b"x"));
        let mut r3 = Vec::new();
        r3.extend_from_slice(&encode_avro_long(0));
        r3.extend_from_slice(&encode_avro_int(-1));
        dec.decode(&mut AvroCursor::new(&r1)).unwrap();
        dec.decode(&mut AvroCursor::new(&r2)).unwrap();
        dec.decode(&mut AvroCursor::new(&r3)).unwrap();
        let array = dec.flush(None).unwrap();
        let ua = array
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("expected UnionArray");
        assert_eq!(ua.len(), 3);
        assert_eq!(ua.type_id(0), 2);
        assert_eq!(ua.type_id(1), 5);
        assert_eq!(ua.type_id(2), 2);
        assert_eq!(ua.value_offset(0), 0);
        assert_eq!(ua.value_offset(1), 0);
        assert_eq!(ua.value_offset(2), 1);
        let int_child = ua
            .child(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int child");
        assert_eq!(int_child.len(), 2);
        assert_eq!(int_child.value(0), 7);
        assert_eq!(int_child.value(1), -1);
        let str_child = ua
            .child(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string child");
        assert_eq!(str_child.len(), 1);
        assert_eq!(str_child.value(0), "x");
    }

    #[test]
    fn test_union_dense_with_null_and_string_children() {
        let union_dt = make_dense_union_avro(
            vec![
                (Codec::Null, "n", DataType::Null),
                (Codec::Utf8, "s", DataType::Utf8),
            ],
            vec![42, 7],
        );
        let mut dec = Decoder::try_new(&union_dt).unwrap();
        let r1 = encode_avro_long(0);
        let mut r2 = Vec::new();
        r2.extend_from_slice(&encode_avro_long(1));
        r2.extend_from_slice(&encode_avro_bytes(b"abc"));
        let r3 = encode_avro_long(0);
        dec.decode(&mut AvroCursor::new(&r1)).unwrap();
        dec.decode(&mut AvroCursor::new(&r2)).unwrap();
        dec.decode(&mut AvroCursor::new(&r3)).unwrap();
        let array = dec.flush(None).unwrap();
        let ua = array
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("expected UnionArray");
        assert_eq!(ua.len(), 3);
        assert_eq!(ua.type_id(0), 42);
        assert_eq!(ua.type_id(1), 7);
        assert_eq!(ua.type_id(2), 42);
        assert_eq!(ua.value_offset(0), 0);
        assert_eq!(ua.value_offset(1), 0);
        assert_eq!(ua.value_offset(2), 1);
        let null_child = ua
            .child(42)
            .as_any()
            .downcast_ref::<NullArray>()
            .expect("null child");
        assert_eq!(null_child.len(), 2);
        let str_child = ua
            .child(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string child");
        assert_eq!(str_child.len(), 1);
        assert_eq!(str_child.value(0), "abc");
    }

    #[test]
    fn test_union_decode_negative_branch_index_errors() {
        let union_dt = make_dense_union_avro(
            vec![
                (Codec::Int32, "i", DataType::Int32),
                (Codec::Utf8, "s", DataType::Utf8),
            ],
            vec![0, 1],
        );
        let mut dec = Decoder::try_new(&union_dt).unwrap();
        let row = encode_avro_long(-1); // decodes back to -1
        let err = dec
            .decode(&mut AvroCursor::new(&row))
            .expect_err("expected error for negative branch index");
        let msg = err.to_string();
        assert!(
            msg.contains("Negative union branch index"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_union_decode_out_of_range_branch_index_errors() {
        let union_dt = make_dense_union_avro(
            vec![
                (Codec::Int32, "i", DataType::Int32),
                (Codec::Utf8, "s", DataType::Utf8),
            ],
            vec![10, 11],
        );
        let mut dec = Decoder::try_new(&union_dt).unwrap();
        let row = encode_avro_long(2);
        let err = dec
            .decode(&mut AvroCursor::new(&row))
            .expect_err("expected error for out-of-range branch index");
        let msg = err.to_string();
        assert!(
            msg.contains("out of range"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_union_sparse_mode_not_supported() {
        let children: Vec<AvroDataType> = vec![
            AvroDataType::new(Codec::Int32, Default::default(), None),
            AvroDataType::new(Codec::Utf8, Default::default(), None),
        ];
        let uf = UnionFields::try_new(
            vec![1, 3],
            vec![
                arrow_schema::Field::new("i", DataType::Int32, true),
                arrow_schema::Field::new("s", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let codec = Codec::Union(children.into(), uf, UnionMode::Sparse);
        let dt = AvroDataType::new(codec, Default::default(), None);
        let err = Decoder::try_new(&dt).expect_err("sparse union should not be supported");
        let msg = err.to_string();
        assert!(
            msg.contains("Sparse Arrow unions are not yet supported"),
            "unexpected error message: {msg}"
        );
    }

    fn make_record_decoder_with_projector_defaults(
        reader_fields: &[(&str, DataType, bool)],
        field_defaults: Vec<Option<AvroLiteral>>,
        default_injections: Vec<(usize, AvroLiteral)>,
        writer_to_reader_len: usize,
    ) -> Decoder {
        assert_eq!(
            field_defaults.len(),
            reader_fields.len(),
            "field_defaults must have one entry per reader field"
        );
        let mut field_refs: Vec<FieldRef> = Vec::with_capacity(reader_fields.len());
        let mut encodings: Vec<Decoder> = Vec::with_capacity(reader_fields.len());
        for (name, dt, nullable) in reader_fields {
            field_refs.push(Arc::new(ArrowField::new(*name, dt.clone(), *nullable)));
            let enc = match dt {
                DataType::Int32 => Decoder::Int32(Vec::with_capacity(DEFAULT_CAPACITY)),
                DataType::Int64 => Decoder::Int64(Vec::with_capacity(DEFAULT_CAPACITY)),
                DataType::Utf8 => Decoder::String(
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                ),
                other => panic!("Unsupported test field type in helper: {other:?}"),
            };
            encodings.push(enc);
        }
        let fields: Fields = field_refs.into();
        let skip_decoders: Vec<Option<Skipper>> =
            (0..writer_to_reader_len).map(|_| None::<Skipper>).collect();
        let projector = Projector {
            writer_to_reader: Arc::from(vec![None; writer_to_reader_len]),
            skip_decoders,
            field_defaults,
            default_injections: Arc::from(default_injections),
        };
        Decoder::Record(fields, encodings, Some(projector))
    }

    #[test]
    fn test_default_append_int32_and_int64_from_int_and_long() {
        let mut d_i32 = Decoder::Int32(Vec::with_capacity(DEFAULT_CAPACITY));
        d_i32.append_default(&AvroLiteral::Int(42)).unwrap();
        let arr = d_i32.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a.value(0), 42);
        let mut d_i64 = Decoder::Int64(Vec::with_capacity(DEFAULT_CAPACITY));
        d_i64.append_default(&AvroLiteral::Int(5)).unwrap();
        d_i64.append_default(&AvroLiteral::Long(7)).unwrap();
        let arr64 = d_i64.flush(None).unwrap();
        let a64 = arr64.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a64.len(), 2);
        assert_eq!(a64.value(0), 5);
        assert_eq!(a64.value(1), 7);
    }

    #[test]
    fn test_default_append_floats_and_doubles() {
        let mut d_f32 = Decoder::Float32(Vec::with_capacity(DEFAULT_CAPACITY));
        d_f32.append_default(&AvroLiteral::Float(1.5)).unwrap();
        let arr32 = d_f32.flush(None).unwrap();
        let a = arr32.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(a.value(0), 1.5);
        let mut d_f64 = Decoder::Float64(Vec::with_capacity(DEFAULT_CAPACITY));
        d_f64.append_default(&AvroLiteral::Double(2.25)).unwrap();
        let arr64 = d_f64.flush(None).unwrap();
        let b = arr64.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(b.value(0), 2.25);
    }

    #[test]
    fn test_default_append_string_and_bytes() {
        let mut d_str = Decoder::String(
            OffsetBufferBuilder::new(DEFAULT_CAPACITY),
            Vec::with_capacity(DEFAULT_CAPACITY),
        );
        d_str
            .append_default(&AvroLiteral::String("hi".into()))
            .unwrap();
        let s_arr = d_str.flush(None).unwrap();
        let arr = s_arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hi");
        let mut d_bytes = Decoder::Binary(
            OffsetBufferBuilder::new(DEFAULT_CAPACITY),
            Vec::with_capacity(DEFAULT_CAPACITY),
        );
        d_bytes
            .append_default(&AvroLiteral::Bytes(vec![1, 2, 3]))
            .unwrap();
        let b_arr = d_bytes.flush(None).unwrap();
        let barr = b_arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(barr.value(0), &[1, 2, 3]);
        let mut d_str_err = Decoder::String(
            OffsetBufferBuilder::new(DEFAULT_CAPACITY),
            Vec::with_capacity(DEFAULT_CAPACITY),
        );
        let err = d_str_err
            .append_default(&AvroLiteral::Bytes(vec![0x61, 0x62]))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Default for string must be string"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_default_append_nullable_int32_null_and_value() {
        let inner = Decoder::Int32(Vec::with_capacity(DEFAULT_CAPACITY));
        let mut dec = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner),
            NullablePlan::ReadTag,
        );
        dec.append_default(&AvroLiteral::Null).unwrap();
        dec.append_default(&AvroLiteral::Int(11)).unwrap();
        let arr = dec.flush(None).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 2);
        assert!(a.is_null(0));
        assert_eq!(a.value(1), 11);
    }

    #[test]
    fn test_default_append_array_of_ints() {
        let list_dt = avro_from_codec(Codec::List(Arc::new(avro_from_codec(Codec::Int32))));
        let mut d = Decoder::try_new(&list_dt).unwrap();
        let items = vec![
            AvroLiteral::Int(1),
            AvroLiteral::Int(2),
            AvroLiteral::Int(3),
        ];
        d.append_default(&AvroLiteral::Array(items)).unwrap();
        let arr = d.flush(None).unwrap();
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list.value_length(0), 3);
        let vals = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_default_append_map_string_to_int() {
        let map_dt = avro_from_codec(Codec::Map(Arc::new(avro_from_codec(Codec::Int32))));
        let mut d = Decoder::try_new(&map_dt).unwrap();
        let mut m: IndexMap<String, AvroLiteral> = IndexMap::new();
        m.insert("k1".to_string(), AvroLiteral::Int(10));
        m.insert("k2".to_string(), AvroLiteral::Int(20));
        d.append_default(&AvroLiteral::Map(m)).unwrap();
        let arr = d.flush(None).unwrap();
        let map = arr.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 2);
        let binding = map.value(0);
        let entries = binding.as_any().downcast_ref::<StructArray>().unwrap();
        let k = entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let v = entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let keys: std::collections::HashSet<&str> = (0..k.len()).map(|i| k.value(i)).collect();
        assert_eq!(keys, ["k1", "k2"].into_iter().collect());
        let vals: std::collections::HashSet<i32> = (0..v.len()).map(|i| v.value(i)).collect();
        assert_eq!(vals, [10, 20].into_iter().collect());
    }

    #[test]
    fn test_default_append_enum_by_symbol() {
        let symbols: Arc<[String]> = vec!["A".into(), "B".into(), "C".into()].into();
        let mut d = Decoder::Enum(Vec::with_capacity(DEFAULT_CAPACITY), symbols.clone(), None);
        d.append_default(&AvroLiteral::Enum("B".into())).unwrap();
        let arr = d.flush(None).unwrap();
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict.len(), 1);
        let expected = Int32Array::from(vec![1]);
        assert_eq!(dict.keys(), &expected);
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(1), "B");
    }

    #[test]
    fn test_default_append_uuid_and_type_error() {
        let mut d = Decoder::Uuid(Vec::with_capacity(DEFAULT_CAPACITY));
        let uuid_str = "123e4567-e89b-12d3-a456-426614174000";
        d.append_default(&AvroLiteral::String(uuid_str.into()))
            .unwrap();
        let arr_ref = d.flush(None).unwrap();
        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(arr.value_length(), 16);
        assert_eq!(arr.len(), 1);
        let mut d2 = Decoder::Uuid(Vec::with_capacity(DEFAULT_CAPACITY));
        let err = d2
            .append_default(&AvroLiteral::Bytes(vec![0u8; 16]))
            .unwrap_err();
        assert!(
            err.to_string().contains("Default for uuid must be string"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_default_append_fixed_and_length_mismatch() {
        let mut d = Decoder::Fixed(4, Vec::with_capacity(DEFAULT_CAPACITY));
        d.append_default(&AvroLiteral::Bytes(vec![1, 2, 3, 4]))
            .unwrap();
        let arr_ref = d.flush(None).unwrap();
        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(arr.value_length(), 4);
        assert_eq!(arr.value(0), &[1, 2, 3, 4]);
        let mut d_err = Decoder::Fixed(4, Vec::with_capacity(DEFAULT_CAPACITY));
        let err = d_err
            .append_default(&AvroLiteral::Bytes(vec![1, 2, 3]))
            .unwrap_err();
        assert!(
            err.to_string().contains("Fixed default length"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_default_append_duration_and_length_validation() {
        let dt = avro_from_codec(Codec::Interval);
        let mut d = Decoder::try_new(&dt).unwrap();
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        d.append_default(&AvroLiteral::Bytes(bytes)).unwrap();
        let arr_ref = d.flush(None).unwrap();
        let arr = arr_ref
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(arr.len(), 1);
        let v = arr.value(0);
        assert_eq!(v.months, 1);
        assert_eq!(v.days, 2);
        assert_eq!(v.nanoseconds, 3_000_000);
        let mut d_err = Decoder::try_new(&avro_from_codec(Codec::Interval)).unwrap();
        let err = d_err
            .append_default(&AvroLiteral::Bytes(vec![0u8; 11]))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Duration default must be exactly 12 bytes"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_default_append_decimal256_from_bytes() {
        let dt = avro_from_codec(Codec::Decimal(50, Some(2), Some(32)));
        let mut d = Decoder::try_new(&dt).unwrap();
        let pos: [u8; 32] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x30, 0x39,
        ];
        d.append_default(&AvroLiteral::Bytes(pos.to_vec())).unwrap();
        let neg: [u8; 32] = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0x85,
        ];
        d.append_default(&AvroLiteral::Bytes(neg.to_vec())).unwrap();
        let arr = d.flush(None).unwrap();
        let dec = arr.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(dec.len(), 2);
        assert_eq!(dec.value_as_string(0), "123.45");
        assert_eq!(dec.value_as_string(1), "-1.23");
    }

    #[test]
    fn test_record_append_default_map_missing_fields_uses_projector_field_defaults() {
        let field_defaults = vec![None, Some(AvroLiteral::String("hi".into()))];
        let mut rec = make_record_decoder_with_projector_defaults(
            &[("a", DataType::Int32, false), ("b", DataType::Utf8, false)],
            field_defaults,
            vec![],
            0,
        );
        let mut map: IndexMap<String, AvroLiteral> = IndexMap::new();
        map.insert("a".to_string(), AvroLiteral::Int(7));
        rec.append_default(&AvroLiteral::Map(map)).unwrap();
        let arr = rec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let a = s
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = s
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), 7);
        assert_eq!(b.value(0), "hi");
    }

    #[test]
    fn test_record_append_default_null_uses_projector_field_defaults() {
        let field_defaults = vec![
            Some(AvroLiteral::Int(5)),
            Some(AvroLiteral::String("x".into())),
        ];
        let mut rec = make_record_decoder_with_projector_defaults(
            &[("a", DataType::Int32, false), ("b", DataType::Utf8, false)],
            field_defaults,
            vec![],
            0,
        );
        rec.append_default(&AvroLiteral::Null).unwrap();
        let arr = rec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let a = s
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = s
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), 5);
        assert_eq!(b.value(0), "x");
    }

    #[test]
    fn test_record_append_default_missing_fields_without_projector_defaults_yields_type_nulls_or_empties()
     {
        let fields = vec![("a", DataType::Int32, true), ("b", DataType::Utf8, true)];
        let mut field_refs: Vec<FieldRef> = Vec::new();
        let mut encoders: Vec<Decoder> = Vec::new();
        for (name, dt, nullable) in &fields {
            field_refs.push(Arc::new(ArrowField::new(*name, dt.clone(), *nullable)));
        }
        let enc_a = Decoder::Nullable(
            Nullability::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(Decoder::Int32(Vec::with_capacity(DEFAULT_CAPACITY))),
            NullablePlan::ReadTag,
        );
        let enc_b = Decoder::Nullable(
            Nullability::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(Decoder::String(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            )),
            NullablePlan::ReadTag,
        );
        encoders.push(enc_a);
        encoders.push(enc_b);
        let projector = Projector {
            writer_to_reader: Arc::from(vec![]),
            skip_decoders: vec![],
            field_defaults: vec![None, None], // no defaults -> append_null
            default_injections: Arc::from(Vec::<(usize, AvroLiteral)>::new()),
        };
        let mut rec = Decoder::Record(field_refs.into(), encoders, Some(projector));
        let mut map: IndexMap<String, AvroLiteral> = IndexMap::new();
        map.insert("a".to_string(), AvroLiteral::Int(9));
        rec.append_default(&AvroLiteral::Map(map)).unwrap();
        let arr = rec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let a = s
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = s
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(a.is_valid(0));
        assert_eq!(a.value(0), 9);
        assert!(b.is_null(0));
    }

    #[test]
    fn test_projector_default_injection_when_writer_lacks_fields() {
        let defaults = vec![None, None];
        let injections = vec![
            (0, AvroLiteral::Int(99)),
            (1, AvroLiteral::String("alice".into())),
        ];
        let mut rec = make_record_decoder_with_projector_defaults(
            &[
                ("id", DataType::Int32, false),
                ("name", DataType::Utf8, false),
            ],
            defaults,
            injections,
            0,
        );
        rec.decode(&mut AvroCursor::new(&[])).unwrap();
        let arr = rec.flush(None).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let id = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name = s
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), 99);
        assert_eq!(name.value(0), "alice");
    }

    #[test]
    fn union_type_ids_are_not_child_indexes() {
        let encodings: Vec<AvroDataType> =
            vec![avro_from_codec(Codec::Int32), avro_from_codec(Codec::Utf8)];
        let fields: UnionFields = [
            (42_i8, Arc::new(ArrowField::new("a", DataType::Int32, true))),
            (7_i8, Arc::new(ArrowField::new("b", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect();
        let dt = avro_from_codec(Codec::Union(
            encodings.into(),
            fields.clone(),
            UnionMode::Dense,
        ));
        let mut dec = Decoder::try_new(&dt).expect("decoder");
        let mut b1 = encode_avro_long(1);
        b1.extend(encode_avro_bytes("hi".as_bytes()));
        dec.decode(&mut AvroCursor::new(&b1)).expect("decode b1");
        let mut b0 = encode_avro_long(0);
        b0.extend(encode_avro_int(5));
        dec.decode(&mut AvroCursor::new(&b0)).expect("decode b0");
        let arr = dec.flush(None).expect("flush");
        let ua = arr.as_any().downcast_ref::<UnionArray>().expect("union");
        assert_eq!(ua.len(), 2);
        assert_eq!(ua.type_id(0), 7, "type id must come from UnionFields");
        assert_eq!(ua.type_id(1), 42, "type id must come from UnionFields");
        assert_eq!(ua.value_offset(0), 0);
        assert_eq!(ua.value_offset(1), 0);
        let utf8_child = ua.child(7).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(utf8_child.len(), 1);
        assert_eq!(utf8_child.value(0), "hi");
        let int_child = ua.child(42).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_child.len(), 1);
        assert_eq!(int_child.value(0), 5);
        let type_ids: Vec<i8> = fields.iter().map(|(tid, _)| tid).collect();
        assert_eq!(type_ids, vec![42_i8, 7_i8]);
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn skipper_from_avro_maps_custom_duration_variants_to_int64() -> Result<(), ArrowError> {
        for codec in [
            Codec::DurationNanos,
            Codec::DurationMicros,
            Codec::DurationMillis,
            Codec::DurationSeconds,
        ] {
            let dt = make_avro_dt(codec.clone(), None);
            let s = Skipper::from_avro(&dt)?;
            match s {
                Skipper::Int64 => {}
                other => panic!("expected Int64 skipper for {:?}, got {:?}", codec, other),
            }
        }
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn skipper_skip_consumes_one_long_for_custom_durations() -> Result<(), ArrowError> {
        let values: [i64; 7] = [0, 1, -1, 150, -150, i64::MAX / 3, i64::MIN / 3];
        for codec in [
            Codec::DurationNanos,
            Codec::DurationMicros,
            Codec::DurationMillis,
            Codec::DurationSeconds,
        ] {
            let dt = make_avro_dt(codec.clone(), None);
            let mut s = Skipper::from_avro(&dt)?;
            for &v in &values {
                let bytes = encode_avro_long(v);
                let mut cursor = AvroCursor::new(&bytes);
                s.skip(&mut cursor)?;
                assert_eq!(
                    cursor.position(),
                    bytes.len(),
                    "did not consume all bytes for {:?} value {}",
                    codec,
                    v
                );
            }
        }
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn skipper_nullable_custom_duration_respects_null_first() -> Result<(), ArrowError> {
        let dt = make_avro_dt(Codec::DurationNanos, Some(Nullability::NullFirst));
        let mut s = Skipper::from_avro(&dt)?;
        match &s {
            Skipper::Nullable(Nullability::NullFirst, inner) => match **inner {
                Skipper::Int64 => {}
                ref other => panic!("expected inner Int64, got {:?}", other),
            },
            other => panic!("expected Nullable(NullFirst, Int64), got {:?}", other),
        }
        {
            let buf = encode_vlq_u64(0);
            let mut cursor = AvroCursor::new(&buf);
            s.skip(&mut cursor)?;
            assert_eq!(cursor.position(), 1, "expected to consume only tag=0");
        }
        {
            let mut buf = encode_vlq_u64(1);
            buf.extend(encode_avro_long(0));
            let mut cursor = AvroCursor::new(&buf);
            s.skip(&mut cursor)?;
            assert_eq!(cursor.position(), 2, "expected to consume tag=1 + long(0)");
        }

        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn skipper_nullable_custom_duration_respects_null_second() -> Result<(), ArrowError> {
        let dt = make_avro_dt(Codec::DurationMicros, Some(Nullability::NullSecond));
        let mut s = Skipper::from_avro(&dt)?;
        match &s {
            Skipper::Nullable(Nullability::NullSecond, inner) => match **inner {
                Skipper::Int64 => {}
                ref other => panic!("expected inner Int64, got {:?}", other),
            },
            other => panic!("expected Nullable(NullSecond, Int64), got {:?}", other),
        }
        {
            let buf = encode_vlq_u64(1);
            let mut cursor = AvroCursor::new(&buf);
            s.skip(&mut cursor)?;
            assert_eq!(cursor.position(), 1, "expected to consume only tag=1");
        }
        {
            let mut buf = encode_vlq_u64(0);
            buf.extend(encode_avro_long(-1));
            let mut cursor = AvroCursor::new(&buf);
            s.skip(&mut cursor)?;
            assert_eq!(
                cursor.position(),
                1 + encode_avro_long(-1).len(),
                "expected to consume tag=0 + long(-1)"
            );
        }
        Ok(())
    }

    #[test]
    fn skipper_interval_is_fixed12_and_skips_12_bytes() -> Result<(), ArrowError> {
        let dt = make_avro_dt(Codec::Interval, None);
        let mut s = Skipper::from_avro(&dt)?;
        match s {
            Skipper::DurationFixed12 => {}
            other => panic!("expected DurationFixed12, got {:?}", other),
        }
        let payload = vec![0u8; 12];
        let mut cursor = AvroCursor::new(&payload);
        s.skip(&mut cursor)?;
        assert_eq!(cursor.position(), 12, "expected to consume 12 fixed bytes");
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_width16_int32_basic_grouping() {
        use arrow_array::RunArray;
        use std::sync::Arc;
        let inner = avro_from_codec(Codec::Int32);
        let ree = AvroDataType::new(
            Codec::RunEndEncoded(Arc::new(inner), 16),
            Default::default(),
            None,
        );
        let mut dec = Decoder::try_new(&ree).expect("create REE decoder");
        for v in [1, 1, 1, 2, 2, 3, 3, 3, 3] {
            let bytes = encode_avro_int(v);
            dec.decode(&mut AvroCursor::new(&bytes)).expect("decode");
        }
        let arr = dec.flush(None).expect("flush");
        let ra = arr
            .as_any()
            .downcast_ref::<RunArray<Int16Type>>()
            .expect("RunArray<Int16Type>");
        assert_eq!(ra.len(), 9);
        assert_eq!(ra.run_ends().values(), &[3, 5, 9]);
        let vals = ra
            .values()
            .as_ref()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("values Int32");
        assert_eq!(vals.values(), &[1, 2, 3]);
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_width32_nullable_values_group_nulls() {
        use arrow_array::RunArray;
        use std::sync::Arc;
        let inner = AvroDataType::new(
            Codec::Int32,
            Default::default(),
            Some(Nullability::NullSecond),
        );
        let ree = AvroDataType::new(
            Codec::RunEndEncoded(Arc::new(inner), 32),
            Default::default(),
            None,
        );
        let mut dec = Decoder::try_new(&ree).expect("create REE decoder");
        let seq: [Option<i32>; 8] = [
            None,
            None,
            Some(7),
            Some(7),
            Some(7),
            None,
            Some(5),
            Some(5),
        ];
        for item in seq {
            let mut bytes = Vec::new();
            match item {
                None => bytes.extend_from_slice(&encode_vlq_u64(1)),
                Some(v) => {
                    bytes.extend_from_slice(&encode_vlq_u64(0));
                    bytes.extend_from_slice(&encode_avro_int(v));
                }
            }
            dec.decode(&mut AvroCursor::new(&bytes)).expect("decode");
        }
        let arr = dec.flush(None).expect("flush");
        let ra = arr
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .expect("RunArray<Int32Type>");
        assert_eq!(ra.len(), 8);
        assert_eq!(ra.run_ends().values(), &[2, 5, 6, 8]);
        let vals = ra
            .values()
            .as_ref()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("values Int32 (nullable)");
        assert_eq!(vals.len(), 4);
        assert!(vals.is_null(0));
        assert_eq!(vals.value(1), 7);
        assert!(vals.is_null(2));
        assert_eq!(vals.value(3), 5);
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_decode_with_promotion_int_to_double_via_nullable_from_single() {
        use arrow_array::RunArray;
        let inner_values = Decoder::Float64(Vec::with_capacity(DEFAULT_CAPACITY));
        let ree = Decoder::RunEndEncoded(
            8, /* bytes => Int64 run-ends */
            0,
            Box::new(inner_values),
        );
        let mut dec = Decoder::Nullable(
            Nullability::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(ree),
            NullablePlan::FromSingle {
                promotion: Promotion::IntToDouble,
            },
        );
        for v in [1, 1, 2, 2, 2] {
            let bytes = encode_avro_int(v);
            dec.decode(&mut AvroCursor::new(&bytes)).expect("decode");
        }
        let arr = dec.flush(None).expect("flush");
        let ra = arr
            .as_any()
            .downcast_ref::<RunArray<Int64Type>>()
            .expect("RunArray<Int64Type>");
        assert_eq!(ra.len(), 5);
        assert_eq!(ra.run_ends().values(), &[2, 5]);
        let vals = ra
            .values()
            .as_ref()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("values Float64");
        assert_eq!(vals.values(), &[1.0, 2.0]);
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_unsupported_run_end_width_errors() {
        use std::sync::Arc;
        let inner = avro_from_codec(Codec::Int32);
        let dt = AvroDataType::new(
            Codec::RunEndEncoded(Arc::new(inner), 3),
            Default::default(),
            None,
        );
        let err = Decoder::try_new(&dt).expect_err("must reject unsupported width");
        let msg = err.to_string();
        assert!(
            msg.contains("Unsupported run-end width")
                && msg.contains("16/32/64 bits or 2/4/8 bytes"),
            "unexpected error message: {msg}"
        );
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_empty_input_is_empty_runarray() {
        use arrow_array::RunArray;
        use std::sync::Arc;
        let inner = avro_from_codec(Codec::Utf8);
        let dt = AvroDataType::new(
            Codec::RunEndEncoded(Arc::new(inner), 4),
            Default::default(),
            None,
        );
        let mut dec = Decoder::try_new(&dt).expect("create REE decoder");
        let arr = dec.flush(None).expect("flush");
        let ra = arr
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .expect("RunArray<Int32Type>");
        assert_eq!(ra.len(), 0);
        assert_eq!(ra.run_ends().len(), 0);
        assert_eq!(ra.values().len(), 0);
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_strings_grouping_width32_bits() {
        use arrow_array::RunArray;
        use std::sync::Arc;
        let inner = avro_from_codec(Codec::Utf8);
        let dt = AvroDataType::new(
            Codec::RunEndEncoded(Arc::new(inner), 32),
            Default::default(),
            None,
        );
        let mut dec = Decoder::try_new(&dt).expect("create REE decoder");
        for s in ["a", "a", "bb", "bb", "bb", "a"] {
            let bytes = encode_avro_bytes(s.as_bytes());
            dec.decode(&mut AvroCursor::new(&bytes)).expect("decode");
        }
        let arr = dec.flush(None).expect("flush");
        let ra = arr
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .expect("RunArray<Int32Type>");
        assert_eq!(ra.run_ends().values(), &[2, 5, 6]);
        let vals = ra
            .values()
            .as_ref()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("values String");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals.value(0), "a");
        assert_eq!(vals.value(1), "bb");
        assert_eq!(vals.value(2), "a");
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_no_custom_types_feature_smoke_decodes_plain_int32() {
        let dt = avro_from_codec(Codec::Int32);
        let mut dec = Decoder::try_new(&dt).expect("create Int32 decoder");
        for v in [1, 2, 3] {
            let bytes = encode_avro_int(v);
            dec.decode(&mut AvroCursor::new(&bytes)).expect("decode");
        }
        let arr = dec.flush(None).expect("flush");
        let a = arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(a.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_timestamp_nanos_decoding_utc() {
        let avro_type = avro_from_codec(Codec::TimestampNanos(true));
        let mut decoder = Decoder::try_new(&avro_type).expect("create TimestampNanos decoder");
        let mut data = Vec::new();
        for v in [0_i64, 1_i64, -1_i64, 1_234_567_890_i64] {
            data.extend_from_slice(&encode_avro_long(v));
        }
        let mut cur = AvroCursor::new(&data);
        for _ in 0..4 {
            decoder.decode(&mut cur).expect("decode nanos ts");
        }
        let array = decoder.flush(None).expect("flush nanos ts");
        let ts = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("TimestampNanosecondArray");
        assert_eq!(ts.values(), &[0, 1, -1, 1_234_567_890]);
        match ts.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => {
                assert_eq!(tz.as_deref(), Some("+00:00"));
            }
            other => panic!("expected Timestamp(Nanosecond, Some(\"+00:00\")), got {other:?}"),
        }
    }

    #[test]
    fn test_timestamp_nanos_decoding_local() {
        let avro_type = avro_from_codec(Codec::TimestampNanos(false));
        let mut decoder = Decoder::try_new(&avro_type).expect("create TimestampNanos decoder");
        let mut data = Vec::new();
        for v in [10_i64, 20_i64, -30_i64] {
            data.extend_from_slice(&encode_avro_long(v));
        }
        let mut cur = AvroCursor::new(&data);
        for _ in 0..3 {
            decoder.decode(&mut cur).expect("decode nanos ts");
        }
        let array = decoder.flush(None).expect("flush nanos ts");
        let ts = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("TimestampNanosecondArray");
        assert_eq!(ts.values(), &[10, 20, -30]);
        match ts.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => {
                assert_eq!(tz.as_deref(), None);
            }
            other => panic!("expected Timestamp(Nanosecond, None), got {other:?}"),
        }
    }

    #[test]
    fn test_timestamp_nanos_decoding_with_nulls() {
        let avro_type = AvroDataType::new(
            Codec::TimestampNanos(false),
            Default::default(),
            Some(Nullability::NullFirst),
        );
        let mut decoder = Decoder::try_new(&avro_type).expect("create nullable TimestampNanos");
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_long(42));
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_long(-7));
        let mut cur = AvroCursor::new(&data);
        for _ in 0..3 {
            decoder.decode(&mut cur).expect("decode nullable nanos ts");
        }
        let array = decoder.flush(None).expect("flush nullable nanos ts");
        let ts = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("TimestampNanosecondArray");
        assert_eq!(ts.len(), 3);
        assert!(ts.is_valid(0));
        assert!(ts.is_null(1));
        assert!(ts.is_valid(2));
        assert_eq!(ts.value(0), 42);
        assert_eq!(ts.value(2), -7);
        match ts.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => {
                assert_eq!(tz.as_deref(), None);
            }
            other => panic!("expected Timestamp(Nanosecond, None), got {other:?}"),
        }
    }
}
