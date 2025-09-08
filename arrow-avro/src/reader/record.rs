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

use crate::codec::{
    AvroDataType, AvroField, AvroLiteral, Codec, Promotion, ResolutionInfo, ResolvedRecord,
};
use crate::reader::block::{Block, BlockDecoder};
use crate::reader::cursor::AvroCursor;
use crate::schema::Nullability;
use arrow_array::builder::{
    Decimal128Builder, Decimal256Builder, Decimal32Builder, Decimal64Builder,
    IntervalMonthDayNanoBuilder, StringViewBuilder,
};
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::*;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, FieldRef, Fields, Schema as ArrowSchema, SchemaRef,
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
#[cfg(feature = "small_decimals")]
use arrow_schema::{DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION};
use std::cmp::Ordering;
use std::sync::Arc;
use uuid::Uuid;

const DEFAULT_CAPACITY: usize = 1024;

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
        let dec = <$ArrayTy>::new(vals, $nulls)
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

#[derive(Debug)]
pub(crate) struct RecordDecoderBuilder<'a> {
    data_type: &'a AvroDataType,
    use_utf8view: bool,
}

impl<'a> RecordDecoderBuilder<'a> {
    pub(crate) fn new(data_type: &'a AvroDataType) -> Self {
        Self {
            data_type,
            use_utf8view: false,
        }
    }

    pub(crate) fn with_utf8_view(mut self, use_utf8view: bool) -> Self {
        self.use_utf8view = use_utf8view;
        self
    }

    /// Builds the `RecordDecoder`.
    pub(crate) fn build(self) -> Result<RecordDecoder, ArrowError> {
        RecordDecoder::try_new_with_options(self.data_type, self.use_utf8view)
    }
}

/// Decodes avro encoded data into [`RecordBatch`]
#[derive(Debug)]
pub(crate) struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
    use_utf8view: bool,
    projector: Option<Projector>,
}

impl RecordDecoder {
    /// Creates a new `RecordDecoderBuilder` for configuring a `RecordDecoder`.
    pub(crate) fn new(data_type: &'_ AvroDataType) -> Self {
        RecordDecoderBuilder::new(data_type).build().unwrap()
    }

    /// Create a new [`RecordDecoder`] from the provided [`AvroDataType`] with default options
    pub(crate) fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        RecordDecoderBuilder::new(data_type)
            .with_utf8_view(true)
            .build()
    }

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
    pub(crate) fn try_new_with_options(
        data_type: &AvroDataType,
        use_utf8view: bool,
    ) -> Result<Self, ArrowError> {
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
                    Some(ResolutionInfo::Record(rec)) => Some(
                        ProjectorBuilder::try_new(rec)
                            .with_reader_fields(reader_fields)
                            .build()?,
                    ),
                    _ => None,
                };
                Ok(Self {
                    schema: Arc::new(ArrowSchema::new(arrow_fields)),
                    fields: encodings,
                    use_utf8view,
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

#[derive(Debug)]
enum Decoder {
    Null(usize),
    Boolean(BooleanBufferBuilder),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Date32(Vec<i32>),
    TimeMillis(Vec<i32>),
    TimeMicros(Vec<i64>),
    TimestampMillis(bool, Vec<i64>),
    TimestampMicros(bool, Vec<i64>),
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
    Nullable(Nullability, NullBufferBuilder, Box<Decoder>),
}

impl Decoder {
    fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
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
                        Some(
                            ProjectorBuilder::try_new(rec)
                                .with_reader_fields(fields)
                                .build()?,
                        )
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
        };
        Ok(match data_type.nullability() {
            Some(nullability) => Self::Nullable(
                nullability,
                NullBufferBuilder::new(DEFAULT_CAPACITY),
                Box::new(decoder),
            ),
            None => decoder,
        })
    }

    /// Append a null record
    fn append_null(&mut self) {
        match self {
            Self::Null(count) => *count += 1,
            Self::Boolean(b) => b.append(false),
            Self::Int32(v) | Self::Date32(v) | Self::TimeMillis(v) => v.push(0),
            Self::Int64(v)
            | Self::Int32ToInt64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v) => v.push(0),
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
            Self::Array(_, offsets, e) => {
                offsets.push_length(0);
            }
            Self::Record(_, e, _) => e.iter_mut().for_each(|e| e.append_null()),
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
            Self::Nullable(_, null_buffer, inner) => {
                null_buffer.append(false);
                inner.append_null();
            }
        }
    }

    /// Append a single default literal into the decoder's buffers
    fn append_default(&mut self, lit: &AvroLiteral) -> Result<(), ArrowError> {
        match self {
            Self::Nullable(_, nb, inner) => {
                if matches!(lit, AvroLiteral::Null) {
                    nb.append(false);
                    inner.append_null();
                    Ok(())
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
            Self::Int64(v)
            | Self::Int32ToInt64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v) => match lit {
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
            Self::Record(field_meta, decoders, projector) => match lit {
                AvroLiteral::Map(entries) => {
                    for (i, dec) in decoders.iter_mut().enumerate() {
                        let name = field_meta[i].name();
                        if let Some(sub) = entries.get(name) {
                            dec.append_default(sub)?;
                        } else if let Some(proj) = projector.as_ref() {
                            proj.project_default(dec, i)?;
                        } else {
                            dec.append_null();
                        }
                    }
                    Ok(())
                }
                AvroLiteral::Null => {
                    for (i, dec) in decoders.iter_mut().enumerate() {
                        if let Some(proj) = projector.as_ref() {
                            proj.project_default(dec, i)?;
                        } else {
                            dec.append_null();
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
            | Self::TimestampMicros(_, values) => values.push(buf.get_long()?),
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
            Self::Nullable(order, nb, encoding) => {
                let branch = buf.read_vlq()?;
                let is_not_null = match *order {
                    Nullability::NullFirst => branch != 0,
                    Nullability::NullSecond => branch == 0,
                };
                if is_not_null {
                    // It is important to decode before appending to null buffer in case of decode error
                    encoding.decode(buf)?;
                } else {
                    encoding.append_null();
                }
                nb.append(is_not_null);
            }
        }
        Ok(())
    }

    /// Flush decoded records to an [`ArrayRef`]
    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            Self::Nullable(_, n, e) => e.flush(n.finish())?,
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
                Arc::new(BinaryArray::new(offsets, values, nulls))
            }
            Self::BytesToString(offsets, values) | Self::String(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Arc::new(StringArray::new(offsets, values, nulls))
            }
            Self::StringView(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values);
                let array = StringArray::new(offsets, values.into(), nulls.clone());
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
                Arc::new(ListArray::new(field.clone(), offsets, values, nulls))
            }
            Self::Record(fields, encodings, _) => {
                let arrays = encodings
                    .iter_mut()
                    .map(|x| x.flush(None))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(StructArray::new(fields.clone(), arrays, nulls))
            }
            Self::Map(map_field, k_off, m_off, kdata, valdec) => {
                let moff = flush_offsets(m_off);
                let koff = flush_offsets(k_off);
                let kd = flush_values(kdata).into();
                let val_arr = valdec.flush(None)?;
                let key_arr = StringArray::new(koff, kd, None);
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
                        )))
                    }
                };
                let entries_struct =
                    StructArray::new(entries_fields, vec![Arc::new(key_arr), val_arr], None);
                let map_arr = MapArray::new(map_field.clone(), moff, entries_struct, nulls, false);
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
        })
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
    reader_fields: Option<Arc<[AvroField]>>,
}

impl<'a> ProjectorBuilder<'a> {
    #[inline]
    fn try_new(rec: &'a ResolvedRecord) -> Self {
        Self {
            rec,
            reader_fields: None,
        }
    }

    #[inline]
    fn with_reader_fields(mut self, reader_fields: &Arc<[AvroField]>) -> Self {
        self.reader_fields = Some(reader_fields.clone());
        self
    }

    #[inline]
    fn build(self) -> Result<Projector, ArrowError> {
        let reader_fields = self.reader_fields.ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "ProjectorBuilder requires reader_fields to be provided".to_string(),
            )
        })?;
        let mut field_defaults: Vec<Option<AvroLiteral>> = Vec::with_capacity(reader_fields.len());
        for avro_field in reader_fields.iter() {
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
        for &idx in self.rec.default_fields.iter() {
            let lit = field_defaults
                .get(idx)
                .and_then(|lit| lit.clone())
                .unwrap_or(AvroLiteral::Null);
            default_injections.push((idx, lit));
        }
        let mut skip_decoders: Vec<Option<Skipper>> =
            Vec::with_capacity(self.rec.skip_fields.len());
        for datatype in self.rec.skip_fields.iter() {
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
            decoder.append_null();
            Ok(())
        }
    }

    #[inline]
    fn project_record(
        &mut self,
        buf: &mut AvroCursor<'_>,
        encodings: &mut [Decoder],
    ) -> Result<(), ArrowError> {
        let n_writer = self.writer_to_reader.len();
        let n_injections = self.default_injections.len();
        for index in 0..(n_writer + n_injections) {
            if index < n_writer {
                match (
                    self.writer_to_reader[index],
                    self.skip_decoders[index].as_mut(),
                ) {
                    (Some(reader_index), _) => encodings[reader_index].decode(buf)?,
                    (None, Some(skipper)) => skipper.skip(buf)?,
                    (None, None) => {
                        return Err(ArrowError::SchemaError(format!(
                            "No skipper available for writer-only field at index {index}",
                        )));
                    }
                }
            } else {
                let (reader_index, ref lit) = self.default_injections[index - n_writer];
                encodings[reader_index].append_default(lit)?;
            }
        }
        Ok(())
    }
}

/// Lightweight skipper for non‑projected writer fields
/// (fields present in the writer schema but omitted by the reader/projection);
/// per Avro 1.11.1 schema resolution these fields are ignored.
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
    Date32,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Fixed(usize),
    Decimal(Option<usize>),
    UuidString,
    Enum,
    DurationFixed12,
    List(Box<Skipper>),
    Map(Box<Skipper>),
    Struct(Vec<Skipper>),
    Nullable(Nullability, Box<Skipper>),
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
            _ => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Skipper not implemented for codec {:?}",
                    dt.codec()
                )));
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
            Self::Int32 | Self::Date32 | Self::TimeMillis => {
                buf.get_int()?;
                Ok(())
            }
            Self::Int64 | Self::TimeMicros | Self::TimestampMillis | Self::TimestampMicros => {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::AvroField;
    use crate::schema::{PrimitiveType, Schema, TypeName};
    use arrow_array::{
        cast::AsArray, Array, Decimal128Array, Decimal256Array, Decimal32Array, DictionaryArray,
        FixedSizeBinaryArray, IntervalMonthDayNanoArray, ListArray, MapArray, StringArray,
        StructArray,
    };

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

    fn decoder_for_promotion(
        writer: PrimitiveType,
        reader: PrimitiveType,
        use_utf8view: bool,
    ) -> Decoder {
        let ws = Schema::TypeName(TypeName::Primitive(writer));
        let rs = Schema::TypeName(TypeName::Primitive(reader));
        let field =
            AvroField::resolve_from_writer_and_reader(&ws, &rs, use_utf8view, false).unwrap();
        Decoder::try_new(field.data_type()).unwrap()
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
        let field = AvroField::resolve_from_writer_and_reader(&ws, &rs, false, false).unwrap();
        let mut dec = Decoder::try_new(field.data_type()).unwrap();
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
        let res = AvroField::resolve_from_writer_and_reader(&ws, &rs, false, false);
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
}
