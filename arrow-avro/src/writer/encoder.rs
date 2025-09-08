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

//! Avro Encoder for Arrow types.

use crate::codec::{AvroDataType, AvroField, Codec};
use crate::schema::Nullability;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use arrow_array::{
    Array, Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray,
    FixedSizeBinaryArray, GenericBinaryArray, GenericListArray, GenericStringArray, LargeListArray,
    ListArray, MapArray, OffsetSizeTrait, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema as ArrowSchema, TimeUnit};
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

/// Encode a single Avro-`long` using ZigZag + variable length, buffered.
///
/// Spec: <https://avro.apache.org/docs/1.11.1/specification/#binary-encoding>
#[inline]
pub fn write_long<W: Write + ?Sized>(out: &mut W, value: i64) -> Result<(), ArrowError> {
    let mut zz = ((value << 1) ^ (value >> 63)) as u64;
    // At most 10 bytes for 64-bit varint
    let mut buf = [0u8; 10];
    let mut i = 0;
    while (zz & !0x7F) != 0 {
        buf[i] = ((zz & 0x7F) as u8) | 0x80;
        i += 1;
        zz >>= 7;
    }
    buf[i] = (zz & 0x7F) as u8;
    i += 1;
    out.write_all(&buf[..i])
        .map_err(|e| ArrowError::IoError(format!("write long: {e}"), e))
}

#[inline]
fn write_int<W: Write + ?Sized>(out: &mut W, value: i32) -> Result<(), ArrowError> {
    write_long(out, value as i64)
}

#[inline]
fn write_len_prefixed<W: Write + ?Sized>(out: &mut W, bytes: &[u8]) -> Result<(), ArrowError> {
    write_long(out, bytes.len() as i64)?;
    out.write_all(bytes)
        .map_err(|e| ArrowError::IoError(format!("write bytes: {e}"), e))
}

#[inline]
fn write_bool<W: Write + ?Sized>(out: &mut W, v: bool) -> Result<(), ArrowError> {
    out.write_all(&[if v { 1 } else { 0 }])
        .map_err(|e| ArrowError::IoError(format!("write bool: {e}"), e))
}

/// Minimal two's-complement big-endian representation helper for Avro decimal (bytes).
///
/// For positive numbers, trim leading 0x00 while the next byte's MSB is 0.
/// For negative numbers, trim leading 0xFF while the next byte's MSB is 1.
/// The resulting slice still encodes the same signed value.
///
/// See Avro spec: decimal over `bytes` uses two's-complement big-endian
/// representation of the unscaled integer value. 1.11.1 specification.
#[inline]
fn minimal_twos_complement(be: &[u8]) -> &[u8] {
    if be.is_empty() {
        return be;
    }
    let mut i = 0usize;
    let sign = (be[0] & 0x80) != 0;
    while i + 1 < be.len() {
        let b = be[i];
        let next = be[i + 1];
        let trim_pos = !sign && b == 0x00 && (next & 0x80) == 0;
        let trim_neg = sign && b == 0xFF && (next & 0x80) != 0;
        if trim_pos || trim_neg {
            i += 1;
        } else {
            break;
        }
    }
    &be[i..]
}

/// Sign-extend (or validate/truncate) big-endian integer bytes to exactly `n` bytes.
///
/// If `src_be` is longer than `n`, ensure that dropped leading bytes are all sign bytes,
/// and that the MSB of the first kept byte matches the sign; otherwise return an overflow error.
/// If shorter than `n`, left-pad with the sign byte.
///
/// Used for Avro decimal over `fixed(N)`.
#[inline]
fn sign_extend_to_exact(src_be: &[u8], n: usize) -> Result<Vec<u8>, ArrowError> {
    let len = src_be.len();
    let sign_byte = if len > 0 && (src_be[0] & 0x80) != 0 {
        0xFF
    } else {
        0x00
    };
    if len == n {
        return Ok(src_be.to_vec());
    }
    if len > n {
        let extra = len - n;
        if src_be[..extra].iter().any(|&b| b != sign_byte) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Decimal value with {} bytes cannot be represented in {} bytes without overflow",
                len, n
            )));
        }
        if n > 0 {
            let first_kept = src_be[extra];
            if ((first_kept ^ sign_byte) & 0x80) != 0 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Decimal value with {} bytes cannot be represented in {} bytes without overflow",
                    len, n
                )));
            }
        }
        return Ok(src_be[extra..].to_vec());
    }
    let mut out = vec![sign_byte; n];
    out[n - len..].copy_from_slice(src_be);
    Ok(out)
}

/// Write the union branch index for an optional field.
///
/// Branch index is 0-based per Avro unions:
/// - Null-first (default): null => 0, value => 1
/// - Null-second (Impala): value => 0, null => 1
fn write_optional_index<W: Write + ?Sized>(
    out: &mut W,
    is_null: bool,
    null_order: Nullability,
) -> Result<(), ArrowError> {
    let byte = union_value_branch_byte(null_order, is_null);
    out.write_all(&[byte])
        .map_err(|e| ArrowError::IoError(format!("write union branch: {e}"), e))
}

#[derive(Debug, Clone)]
enum NullState {
    NonNullable,
    NullableNoNulls {
        union_value_byte: u8,
    },
    Nullable {
        nulls: NullBuffer,
        null_order: Nullability,
    },
}

/// Arrow to Avro FieldEncoder:
/// - Holds the inner `Encoder` (by value)
/// - Carries the per-site nullability **state** as a single enum that enforces invariants
pub struct FieldEncoder<'a> {
    encoder: Encoder<'a>,
    null_state: NullState,
}

impl<'a> FieldEncoder<'a> {
    fn make_encoder(
        array: &'a dyn Array,
        field: &Field,
        plan: &FieldPlan,
        nullability: Option<Nullability>,
    ) -> Result<Self, ArrowError> {
        let encoder = match plan {
            FieldPlan::Struct { encoders } => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| ArrowError::SchemaError("Expected StructArray".into()))?;
                Encoder::Struct(Box::new(StructEncoder::try_new(arr, encoders)?))
            }
            FieldPlan::List {
                items_nullability,
                item_plan,
            } => match array.data_type() {
                DataType::List(_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected ListArray".into()))?;
                    Encoder::List(Box::new(ListEncoder32::try_new(
                        arr,
                        *items_nullability,
                        item_plan.as_ref(),
                    )?))
                }
                DataType::LargeList(_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected LargeListArray".into()))?;
                    Encoder::LargeList(Box::new(ListEncoder64::try_new(
                        arr,
                        *items_nullability,
                        item_plan.as_ref(),
                    )?))
                }
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro array site requires Arrow List/LargeList, found: {other:?}"
                    )))
                }
            },
            FieldPlan::Scalar => match array.data_type() {
                DataType::Boolean => Encoder::Boolean(BooleanEncoder(array.as_boolean())),
                DataType::Utf8 => {
                    Encoder::Utf8(Utf8GenericEncoder::<i32>(array.as_string::<i32>()))
                }
                DataType::LargeUtf8 => {
                    Encoder::Utf8Large(Utf8GenericEncoder::<i64>(array.as_string::<i64>()))
                }
                DataType::Int32 => Encoder::Int(IntEncoder(array.as_primitive::<Int32Type>())),
                DataType::Int64 => Encoder::Long(LongEncoder(array.as_primitive::<Int64Type>())),
                DataType::Float32 => {
                    Encoder::Float32(F32Encoder(array.as_primitive::<Float32Type>()))
                }
                DataType::Float64 => {
                    Encoder::Float64(F64Encoder(array.as_primitive::<Float64Type>()))
                }
                DataType::Binary => Encoder::Binary(BinaryEncoder(array.as_binary::<i32>())),
                DataType::LargeBinary => {
                    Encoder::LargeBinary(BinaryEncoder(array.as_binary::<i64>()))
                }
                DataType::FixedSizeBinary(len) => {
                    // Decide between Avro `fixed` (raw bytes) and `uuid` logical string
                    // based on Field metadata, mirroring schema generation rules.
                    let arr = array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            ArrowError::SchemaError("Expected FixedSizeBinaryArray".into())
                        })?;
                    let md = field.metadata();
                    let is_uuid = md.get("logicalType").is_some_and(|v| v == "uuid")
                        || (*len == 16
                        && md.get("ARROW:extension:name").is_some_and(|v| v == "uuid"));
                    if is_uuid {
                        if *len != 16 {
                            return Err(ArrowError::InvalidArgumentError(
                                "logicalType=uuid requires FixedSizeBinary(16)".into(),
                            ));
                        }
                        Encoder::Uuid(UuidEncoder(arr))
                    } else {
                        Encoder::Fixed(FixedEncoder(arr))
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => Encoder::Timestamp(LongEncoder(
                    array.as_primitive::<TimestampMicrosecondType>(),
                )),
                DataType::Interval(unit) => match unit {
                    IntervalUnit::MonthDayNano => {
                        Encoder::IntervalMonthDayNano(IntervalMonthDayNanoEncoder(
                            array.as_primitive::<IntervalMonthDayNanoType>(),
                        ))
                    }
                    IntervalUnit::YearMonth => {
                        Encoder::IntervalYearMonth(IntervalYearMonthEncoder(
                            array.as_primitive::<IntervalYearMonthType>(),
                        ))
                    }
                    IntervalUnit::DayTime => Encoder::IntervalDayTime(IntervalDayTimeEncoder(
                        array.as_primitive::<IntervalDayTimeType>(),
                    )),
                }
                DataType::Duration(_) => {
                    return Err(ArrowError::NotYetImplemented(
                        "Avro writer: Arrow Duration(TimeUnit) has no standard Avro mapping; cast to Interval(MonthDayNano) to use Avro 'duration'".into(),
                    ));
                }
                // Composite or mismatched types under scalar plan
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::Map(_, _)
                | DataType::Struct(_)
                | DataType::Dictionary(_, _)
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro scalar site incompatible with Arrow type: {:?}",
                        array.data_type()
                    )))
                }
                other => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Avro scalar type not yet supported: {other:?}"
                    )));
                }
            },
            FieldPlan::Decimal {size} => match array.data_type() {
                DataType::Decimal32(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal32Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal32Array".into()))?;
                    Encoder::Decimal32(DecimalEncoder::<4, Decimal32Array>::new(arr, *size))
                }
                DataType::Decimal64(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal64Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal32Array".into()))?;
                    Encoder::Decimal64(DecimalEncoder::<8, Decimal64Array>::new(arr, *size))
                }
                DataType::Decimal128(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal32Array".into()))?;
                    Encoder::Decimal128(DecimalEncoder::<16, Decimal128Array>::new(arr, *size))
                }
                DataType::Decimal256(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal32Array".into()))?;
                    Encoder::Decimal256(DecimalEncoder::<32, Decimal256Array>::new(arr, *size))
                }
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro decimal site requires Arrow Decimal 32, 64, 128, or 256, found: {other:?}"
                    )))
                }
            },
            FieldPlan::Map { values_nullability,
                value_plan } => {
                let arr = array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| ArrowError::SchemaError("Expected MapArray".into()))?;
                Encoder::Map(Box::new(MapEncoder::try_new(arr, *values_nullability, value_plan.as_ref())?))
            }
            FieldPlan::Enum { symbols} => match array.data_type() {
                DataType::Dictionary(key_dt, value_dt) => {
                    // Enforce the same shape we validated during plan build:
                    if **key_dt != DataType::Int32 || **value_dt != DataType::Utf8 {
                        return Err(ArrowError::SchemaError(
                            "Avro enum requires Dictionary<Int32, Utf8>".into(),
                        ));
                    }
                    let dict = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .ok_or_else(|| {
                            ArrowError::SchemaError("Expected DictionaryArray<Int32>".into())
                        })?;

                    // Dictionary values must exactly match schema `symbols` (order & content)
                    let values = dict
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            ArrowError::SchemaError("Dictionary values must be Utf8".into())
                        })?;
                    if values.len() != symbols.len() {
                        return Err(ArrowError::SchemaError(format!(
                            "Enum symbol length {} != dictionary size {}",
                            symbols.len(),
                            values.len()
                        )));
                    }
                    for i in 0..values.len() {
                        if values.value(i) != symbols[i].as_str() {
                            return Err(ArrowError::SchemaError(format!(
                                "Enum symbol mismatch at {i}: schema='{}' dict='{}'",
                                symbols[i],
                                values.value(i)
                            )));
                        }
                    }
                    // Keys are the Avro enum indices (zero-based position in `symbols`).
                    let keys = dict.keys();
                    Encoder::Enum(EnumEncoder { keys })
                }
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro enum site requires DataType::Dictionary, found: {other:?}"
                    )))
                }
            }
            other => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Avro writer: {other:?} not yet supported",
                )));
            }
        };
        // Compute the effective null state from writer-declared nullability and data nulls.
        let null_state = match (nullability, array.null_count() > 0) {
            (None, false) => NullState::NonNullable,
            (None, true) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Avro site '{}' is non-nullable, but array contains nulls",
                    field.name()
                )));
            }
            (Some(order), false) => {
                // Optimization: drop any bitmap; emit a constant "value" branch byte.
                NullState::NullableNoNulls {
                    union_value_byte: union_value_branch_byte(order, false),
                }
            }
            (Some(null_order), true) => {
                let Some(nulls) = array.nulls().cloned() else {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Array for Avro site '{}' reports nulls but has no null buffer",
                        field.name()
                    )));
                };
                NullState::Nullable { nulls, null_order }
            }
        };
        Ok(Self {
            encoder,
            null_state,
        })
    }

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        match &self.null_state {
            NullState::NonNullable => {}
            NullState::NullableNoNulls { union_value_byte } => out
                .write_all(&[*union_value_byte])
                .map_err(|e| ArrowError::IoError(format!("write union value branch: {e}"), e))?,
            NullState::Nullable { nulls, null_order } if nulls.is_null(idx) => {
                return write_optional_index(out, true, *null_order); // no value to write
            }
            NullState::Nullable { null_order, .. } => {
                write_optional_index(out, false, *null_order)?;
            }
        }
        self.encoder.encode(out, idx)
    }
}

fn union_value_branch_byte(null_order: Nullability, is_null: bool) -> u8 {
    let nulls_first = null_order == Nullability::default();
    if nulls_first == is_null {
        0x00
    } else {
        0x02
    }
}

/// Per‑site encoder plan for a field. This mirrors the Avro structure, so nested
/// optional branch order can be honored exactly as declared by the schema.
#[derive(Debug, Clone)]
enum FieldPlan {
    /// Non-nested scalar/logical type
    Scalar,
    /// Record/Struct with Avro‑ordered children
    Struct { encoders: Vec<FieldBinding> },
    /// Array with item‑site nullability and nested plan
    List {
        items_nullability: Option<Nullability>,
        item_plan: Box<FieldPlan>,
    },
    /// Avro map with value‑site nullability and nested plan
    Map {
        values_nullability: Option<Nullability>,
        value_plan: Box<FieldPlan>,
    },
    /// Avro decimal logical type (bytes or fixed). `size=None` => bytes(decimal), `Some(n)` => fixed(n)
    Decimal { size: Option<usize> },
    /// Avro enum; maps to Arrow Dictionary<Int32, Utf8> with dictionary values
    /// exactly equal and ordered as the Avro enum `symbols`.
    Enum { symbols: Arc<[String]> },
}

#[derive(Debug, Clone)]
struct FieldBinding {
    /// Index of the Arrow field/column associated with this Avro field site
    arrow_index: usize,
    /// Nullability/order for this site (None for required fields)
    nullability: Option<Nullability>,
    /// Nested plan for this site
    plan: FieldPlan,
}

/// Builder for `RecordEncoder` write plan
#[derive(Debug)]
pub struct RecordEncoderBuilder<'a> {
    avro_root: &'a AvroField,
    arrow_schema: &'a ArrowSchema,
}

impl<'a> RecordEncoderBuilder<'a> {
    /// Create a new builder from the Avro root and Arrow schema.
    pub fn new(avro_root: &'a AvroField, arrow_schema: &'a ArrowSchema) -> Self {
        Self {
            avro_root,
            arrow_schema,
        }
    }

    /// Build the `RecordEncoder` by walking the Avro **record** root in Avro order,
    /// resolving each field to an Arrow index by name.
    pub fn build(self) -> Result<RecordEncoder, ArrowError> {
        let avro_root_dt = self.avro_root.data_type();
        let Codec::Struct(root_fields) = avro_root_dt.codec() else {
            return Err(ArrowError::SchemaError(
                "Top-level Avro schema must be a record/struct".into(),
            ));
        };
        let mut columns = Vec::with_capacity(root_fields.len());
        for root_field in root_fields.as_ref() {
            let name = root_field.name();
            let arrow_index = self.arrow_schema.index_of(name).map_err(|e| {
                ArrowError::SchemaError(format!("Schema mismatch for field '{name}': {e}"))
            })?;
            columns.push(FieldBinding {
                arrow_index,
                nullability: root_field.data_type().nullability(),
                plan: FieldPlan::build(
                    root_field.data_type(),
                    self.arrow_schema.field(arrow_index),
                )?,
            });
        }
        Ok(RecordEncoder { columns })
    }
}

/// A pre-computed plan for encoding a `RecordBatch` to Avro.
///
/// Derived from an Avro schema and an Arrow schema. It maps
/// top-level Avro fields to Arrow columns and contains a nested encoding plan
/// for each column.
#[derive(Debug, Clone)]
pub struct RecordEncoder {
    columns: Vec<FieldBinding>,
}

impl RecordEncoder {
    fn prepare_for_batch<'a>(
        &'a self,
        batch: &'a RecordBatch,
    ) -> Result<Vec<FieldEncoder<'a>>, ArrowError> {
        let schema_binding = batch.schema();
        let fields = schema_binding.fields();
        let arrays = batch.columns();
        let mut out = Vec::with_capacity(self.columns.len());
        for col_plan in self.columns.iter() {
            let arrow_index = col_plan.arrow_index;
            let array = arrays.get(arrow_index).ok_or_else(|| {
                ArrowError::SchemaError(format!("Column index {arrow_index} out of range"))
            })?;
            let field = fields[arrow_index].as_ref();
            let encoder = prepare_value_site_encoder(
                array.as_ref(),
                field,
                col_plan.nullability,
                &col_plan.plan,
            )?;
            out.push(encoder);
        }
        Ok(out)
    }

    /// Encode a `RecordBatch` using this encoder plan.
    ///
    /// Tip: Wrap `out` in a `std::io::BufWriter` to reduce the overhead of many small writes.
    pub fn encode<W: Write>(&self, out: &mut W, batch: &RecordBatch) -> Result<(), ArrowError> {
        let mut column_encoders = self.prepare_for_batch(batch)?;
        for row in 0..batch.num_rows() {
            for encoder in column_encoders.iter_mut() {
                encoder.encode(out, row)?;
            }
        }
        Ok(())
    }
}

fn find_struct_child_index(fields: &arrow_schema::Fields, name: &str) -> Option<usize> {
    fields.iter().position(|f| f.name() == name)
}

fn find_map_value_field_index(fields: &arrow_schema::Fields) -> Option<usize> {
    // Prefer common Arrow field names; fall back to second child if exactly two
    find_struct_child_index(fields, "value")
        .or_else(|| find_struct_child_index(fields, "values"))
        .or_else(|| if fields.len() == 2 { Some(1) } else { None })
}

impl FieldPlan {
    fn build(avro_dt: &AvroDataType, arrow_field: &Field) -> Result<Self, ArrowError> {
        match avro_dt.codec() {
            Codec::Struct(avro_fields) => {
                let fields = match arrow_field.data_type() {
                    DataType::Struct(struct_fields) => struct_fields,
                    other => {
                        return Err(ArrowError::SchemaError(format!(
                            "Avro struct maps to Arrow Struct, found: {other:?}"
                        )))
                    }
                };
                let mut encoders = Vec::with_capacity(avro_fields.len());
                for avro_field in avro_fields.iter() {
                    let name = avro_field.name().to_string();
                    let idx = find_struct_child_index(fields, &name).ok_or_else(|| {
                        ArrowError::SchemaError(format!(
                            "Struct field '{name}' not present in Arrow field '{}'",
                            arrow_field.name()
                        ))
                    })?;
                    encoders.push(FieldBinding {
                        arrow_index: idx,
                        nullability: avro_field.data_type().nullability(),
                        plan: FieldPlan::build(avro_field.data_type(), fields[idx].as_ref())?,
                    });
                }
                Ok(FieldPlan::Struct { encoders })
            }
            Codec::List(items_dt) => match arrow_field.data_type() {
                DataType::List(field_ref) => Ok(FieldPlan::List {
                    items_nullability: items_dt.nullability(),
                    item_plan: Box::new(FieldPlan::build(items_dt.as_ref(), field_ref.as_ref())?),
                }),
                DataType::LargeList(field_ref) => Ok(FieldPlan::List {
                    items_nullability: items_dt.nullability(),
                    item_plan: Box::new(FieldPlan::build(items_dt.as_ref(), field_ref.as_ref())?),
                }),
                other => Err(ArrowError::SchemaError(format!(
                    "Avro array maps to Arrow List/LargeList, found: {other:?}"
                ))),
            },
            Codec::Map(values_dt) => {
                // Avro map -> Arrow DataType::Map(entries_struct, sorted)
                let entries_field = match arrow_field.data_type() {
                    DataType::Map(entries, _sorted) => entries.as_ref(),
                    other => {
                        return Err(ArrowError::SchemaError(format!(
                            "Avro map maps to Arrow DataType::Map, found: {other:?}"
                        )))
                    }
                };
                let entries_struct_fields = match entries_field.data_type() {
                    DataType::Struct(fs) => fs,
                    other => {
                        return Err(ArrowError::SchemaError(format!(
                            "Arrow Map entries must be Struct, found: {other:?}"
                        )))
                    }
                };
                let value_idx =
                    find_map_value_field_index(entries_struct_fields).ok_or_else(|| {
                        ArrowError::SchemaError("Map entries struct missing value field".into())
                    })?;
                let value_field = entries_struct_fields[value_idx].as_ref();
                let value_plan = FieldPlan::build(values_dt.as_ref(), value_field)?;
                Ok(FieldPlan::Map {
                    values_nullability: values_dt.nullability(),
                    value_plan: Box::new(value_plan),
                })
            }
            Codec::Enum(symbols) => match arrow_field.data_type() {
                DataType::Dictionary(key_dt, value_dt) => {
                    // Enforce the exact reader-compatible shape: Dictionary<Int32, Utf8>
                    if **key_dt != DataType::Int32 {
                        return Err(ArrowError::SchemaError(
                            "Avro enum requires Dictionary<Int32, Utf8>".into(),
                        ));
                    }
                    if **value_dt != DataType::Utf8 {
                        return Err(ArrowError::SchemaError(
                            "Avro enum requires Dictionary<Int32, Utf8>".into(),
                        ));
                    }
                    Ok(FieldPlan::Enum {
                        symbols: symbols.clone(),
                    })
                }
                other => Err(ArrowError::SchemaError(format!(
                    "Avro enum maps to Arrow Dictionary<Int32, Utf8>, found: {other:?}"
                ))),
            },
            // decimal site (bytes or fixed(N)) with precision/scale validation
            Codec::Decimal(precision, scale_opt, fixed_size_opt) => {
                let (ap, as_) = match arrow_field.data_type() {
                    DataType::Decimal32(p, s) => (*p as usize, *s as i32),
                    DataType::Decimal64(p, s) => (*p as usize, *s as i32),
                    DataType::Decimal128(p, s) => (*p as usize, *s as i32),
                    DataType::Decimal256(p, s) => (*p as usize, *s as i32),
                    other => {
                        return Err(ArrowError::SchemaError(format!(
                            "Avro decimal requires Arrow decimal, got {other:?} for field '{}'",
                            arrow_field.name()
                        )))
                    }
                };
                let sc = scale_opt.unwrap_or(0) as i32; // Avro scale defaults to 0 if absent
                if ap != *precision || as_ != sc {
                    return Err(ArrowError::SchemaError(format!(
                        "Decimal precision/scale mismatch for field '{}': Avro({precision},{sc}) vs Arrow({ap},{as_})",
                        arrow_field.name()
                    )));
                }
                Ok(FieldPlan::Decimal {
                    size: *fixed_size_opt,
                })
            }
            Codec::Interval => match arrow_field.data_type() {
                DataType::Interval(IntervalUnit::MonthDayNano | IntervalUnit::YearMonth | IntervalUnit::DayTime
                ) => Ok(FieldPlan::Scalar),
                other => Err(ArrowError::SchemaError(format!(
                    "Avro duration logical type requires Arrow Interval(MonthDayNano), found: {other:?}"
                ))),
            }
            _ => Ok(FieldPlan::Scalar),
        }
    }
}

enum Encoder<'a> {
    Boolean(BooleanEncoder<'a>),
    Int(IntEncoder<'a, Int32Type>),
    Long(LongEncoder<'a, Int64Type>),
    Timestamp(LongEncoder<'a, TimestampMicrosecondType>),
    Float32(F32Encoder<'a>),
    Float64(F64Encoder<'a>),
    Binary(BinaryEncoder<'a, i32>),
    LargeBinary(BinaryEncoder<'a, i64>),
    Utf8(Utf8Encoder<'a>),
    Utf8Large(Utf8LargeEncoder<'a>),
    List(Box<ListEncoder32<'a>>),
    LargeList(Box<ListEncoder64<'a>>),
    Struct(Box<StructEncoder<'a>>),
    /// Avro `fixed` encoder (raw bytes, no length)
    Fixed(FixedEncoder<'a>),
    /// Avro `uuid` logical type encoder (string with RFC‑4122 hyphenated text)
    Uuid(UuidEncoder<'a>),
    /// Avro `duration` logical type (Arrow Interval(MonthDayNano)) encoder
    IntervalMonthDayNano(IntervalMonthDayNanoEncoder<'a>),
    /// Avro `duration` logical type (Arrow Interval(YearMonth)) encoder
    IntervalYearMonth(IntervalYearMonthEncoder<'a>),
    /// Avro `duration` logical type (Arrow Interval(DayTime)) encoder
    IntervalDayTime(IntervalDayTimeEncoder<'a>),
    /// Avro `enum` encoder: writes the key (int) as the enum index.
    Enum(EnumEncoder<'a>),
    Decimal32(Decimal32Encoder<'a>),
    Decimal64(Decimal64Encoder<'a>),
    Decimal128(Decimal128Encoder<'a>),
    Decimal256(Decimal256Encoder<'a>),
    Map(Box<MapEncoder<'a>>),
}

impl<'a> Encoder<'a> {
    /// Encode the value at `idx`.
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        match self {
            Encoder::Boolean(e) => e.encode(out, idx),
            Encoder::Int(e) => e.encode(out, idx),
            Encoder::Long(e) => e.encode(out, idx),
            Encoder::Timestamp(e) => e.encode(out, idx),
            Encoder::Float32(e) => e.encode(out, idx),
            Encoder::Float64(e) => e.encode(out, idx),
            Encoder::Binary(e) => e.encode(out, idx),
            Encoder::LargeBinary(e) => e.encode(out, idx),
            Encoder::Utf8(e) => e.encode(out, idx),
            Encoder::Utf8Large(e) => e.encode(out, idx),
            Encoder::List(e) => e.encode(out, idx),
            Encoder::LargeList(e) => e.encode(out, idx),
            Encoder::Struct(e) => e.encode(out, idx),
            Encoder::Fixed(e) => (e).encode(out, idx),
            Encoder::Uuid(e) => (e).encode(out, idx),
            Encoder::IntervalMonthDayNano(e) => (e).encode(out, idx),
            Encoder::IntervalYearMonth(e) => (e).encode(out, idx),
            Encoder::IntervalDayTime(e) => (e).encode(out, idx),
            Encoder::Enum(e) => (e).encode(out, idx),
            Encoder::Decimal32(e) => (e).encode(out, idx),
            Encoder::Decimal64(e) => (e).encode(out, idx),
            Encoder::Decimal128(e) => (e).encode(out, idx),
            Encoder::Decimal256(e) => (e).encode(out, idx),
            Encoder::Map(e) => (e).encode(out, idx),
        }
    }
}

struct BooleanEncoder<'a>(&'a arrow_array::BooleanArray);
impl BooleanEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        write_bool(out, self.0.value(idx))
    }
}

/// Generic Avro `int` encoder for primitive arrays with `i32` native values.
struct IntEncoder<'a, P: ArrowPrimitiveType<Native = i32>>(&'a PrimitiveArray<P>);
impl<'a, P: ArrowPrimitiveType<Native = i32>> IntEncoder<'a, P> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        write_int(out, self.0.value(idx))
    }
}

/// Generic Avro `long` encoder for primitive arrays with `i64` native values.
struct LongEncoder<'a, P: ArrowPrimitiveType<Native = i64>>(&'a PrimitiveArray<P>);
impl<'a, P: ArrowPrimitiveType<Native = i64>> LongEncoder<'a, P> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        write_long(out, self.0.value(idx))
    }
}

/// Unified binary encoder generic over offset size (i32/i64).
struct BinaryEncoder<'a, O: OffsetSizeTrait>(&'a GenericBinaryArray<O>);
impl<'a, O: OffsetSizeTrait> BinaryEncoder<'a, O> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        write_len_prefixed(out, self.0.value(idx))
    }
}

struct F32Encoder<'a>(&'a arrow_array::Float32Array);
impl F32Encoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        // Avro float: 4 bytes, IEEE-754 little-endian
        let bits = self.0.value(idx).to_bits();
        out.write_all(&bits.to_le_bytes())
            .map_err(|e| ArrowError::IoError(format!("write f32: {e}"), e))
    }
}

struct F64Encoder<'a>(&'a arrow_array::Float64Array);
impl F64Encoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        // Avro double: 8 bytes, IEEE-754 little-endian
        let bits = self.0.value(idx).to_bits();
        out.write_all(&bits.to_le_bytes())
            .map_err(|e| ArrowError::IoError(format!("write f64: {e}"), e))
    }
}

struct Utf8GenericEncoder<'a, O: OffsetSizeTrait>(&'a GenericStringArray<O>);

impl<'a, O: OffsetSizeTrait> Utf8GenericEncoder<'a, O> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        write_len_prefixed(out, self.0.value(idx).as_bytes())
    }
}

type Utf8Encoder<'a> = Utf8GenericEncoder<'a, i32>;
type Utf8LargeEncoder<'a> = Utf8GenericEncoder<'a, i64>;

/// Internal key array kind used by Map encoder.
enum KeyKind<'a> {
    Utf8(&'a GenericStringArray<i32>),
    LargeUtf8(&'a GenericStringArray<i64>),
}
struct MapEncoder<'a> {
    map: &'a MapArray,
    keys: KeyKind<'a>,
    values: FieldEncoder<'a>,
    keys_offset: usize,
    values_offset: usize,
}

fn encode_map_entries<W, O>(
    out: &mut W,
    keys: &GenericStringArray<O>,
    keys_offset: usize,
    start: usize,
    end: usize,
    mut write_item: impl FnMut(&mut W, usize) -> Result<(), ArrowError>,
) -> Result<(), ArrowError>
where
    W: Write + ?Sized,
    O: OffsetSizeTrait,
{
    encode_blocked_range(out, start, end, |out, j| {
        let j_key = j.saturating_sub(keys_offset);
        write_len_prefixed(out, keys.value(j_key).as_bytes())?;
        write_item(out, j)
    })
}

impl<'a> MapEncoder<'a> {
    fn try_new(
        map: &'a MapArray,
        values_nullability: Option<Nullability>,
        value_plan: &FieldPlan,
    ) -> Result<Self, ArrowError> {
        let keys_arr = map.keys();
        let keys_kind = match keys_arr.data_type() {
            DataType::Utf8 => KeyKind::Utf8(keys_arr.as_string::<i32>()),
            DataType::LargeUtf8 => KeyKind::LargeUtf8(keys_arr.as_string::<i64>()),
            other => {
                return Err(ArrowError::SchemaError(format!(
                    "Avro map requires string keys; Arrow key type must be Utf8/LargeUtf8, found: {other:?}"
                )))
            }
        };

        let entries_struct_fields = match map.data_type() {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fs) => fs,
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "Arrow Map entries must be Struct, found: {other:?}"
                    )))
                }
            },
            _ => {
                return Err(ArrowError::SchemaError(
                    "Expected MapArray with DataType::Map".into(),
                ))
            }
        };

        let v_idx = find_map_value_field_index(entries_struct_fields).ok_or_else(|| {
            ArrowError::SchemaError("Map entries struct missing value field".into())
        })?;
        let value_field = entries_struct_fields[v_idx].as_ref();

        let values_enc = prepare_value_site_encoder(
            map.values().as_ref(),
            value_field,
            values_nullability,
            value_plan,
        )?;

        Ok(Self {
            map,
            keys: keys_kind,
            values: values_enc,
            keys_offset: keys_arr.offset(),
            values_offset: map.values().offset(),
        })
    }

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let offsets = self.map.offsets();
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;

        let mut write_item = |out: &mut W, j: usize| {
            let j_val = j.saturating_sub(self.values_offset);
            self.values.encode(out, j_val)
        };

        match self.keys {
            KeyKind::Utf8(arr) => {
                encode_map_entries(out, arr, self.keys_offset, start, end, write_item)
            }
            KeyKind::LargeUtf8(arr) => {
                encode_map_entries(out, arr, self.keys_offset, start, end, write_item)
            }
        }
    }
}

struct StructEncoder<'a> {
    encoders: Vec<FieldEncoder<'a>>,
}

impl<'a> StructEncoder<'a> {
    fn try_new(
        array: &'a StructArray,
        field_bindings: &[FieldBinding],
    ) -> Result<Self, ArrowError> {
        let DataType::Struct(fields) = array.data_type() else {
            return Err(ArrowError::SchemaError("Expected Struct".into()));
        };
        let mut encoders = Vec::with_capacity(field_bindings.len());
        for field_binding in field_bindings {
            let idx = field_binding.arrow_index;
            let column = array.columns().get(idx).ok_or_else(|| {
                ArrowError::SchemaError(format!("Struct child index {idx} out of range"))
            })?;
            let field = fields.get(idx).ok_or_else(|| {
                ArrowError::SchemaError(format!("Struct child index {idx} out of range"))
            })?;
            let encoder = prepare_value_site_encoder(
                column.as_ref(),
                field,
                field_binding.nullability,
                &field_binding.plan,
            )?;
            encoders.push(encoder);
        }
        Ok(Self { encoders })
    }

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        for encoder in self.encoders.iter_mut() {
            encoder.encode(out, idx)?;
        }
        Ok(())
    }
}

/// Encode a blocked range of items with Avro array block framing.
///
/// `write_item` must take `(out, index)` to maintain the "out-first" convention.
fn encode_blocked_range<W: Write + ?Sized, F>(
    out: &mut W,
    start: usize,
    end: usize,
    mut write_item: F,
) -> Result<(), ArrowError>
where
    F: FnMut(&mut W, usize) -> Result<(), ArrowError>,
{
    let len = end.saturating_sub(start);
    if len == 0 {
        // Zero-length terminator per Avro spec.
        write_long(out, 0)?;
        return Ok(());
    }
    // Emit a single positive block for performance, then the end marker.
    write_long(out, len as i64)?;
    for row in start..end {
        write_item(out, row)?;
    }
    write_long(out, 0)?;
    Ok(())
}

struct ListEncoder<'a, O: OffsetSizeTrait> {
    list: &'a GenericListArray<O>,
    values: FieldEncoder<'a>,
    values_offset: usize,
}

type ListEncoder32<'a> = ListEncoder<'a, i32>;
type ListEncoder64<'a> = ListEncoder<'a, i64>;

impl<'a, O: OffsetSizeTrait> ListEncoder<'a, O> {
    fn try_new(
        list: &'a GenericListArray<O>,
        items_nullability: Option<Nullability>,
        item_plan: &FieldPlan,
    ) -> Result<Self, ArrowError> {
        let child_field = match list.data_type() {
            DataType::List(field) => field.as_ref(),
            DataType::LargeList(field) => field.as_ref(),
            _ => {
                return Err(ArrowError::SchemaError(
                    "Expected List or LargeList for ListEncoder".into(),
                ))
            }
        };
        let values_enc = prepare_value_site_encoder(
            list.values().as_ref(),
            child_field,
            items_nullability,
            item_plan,
        )?;
        Ok(Self {
            list,
            values: values_enc,
            values_offset: list.values().offset(),
        })
    }

    fn encode_list_range<W: Write + ?Sized>(
        &mut self,
        out: &mut W,
        start: usize,
        end: usize,
    ) -> Result<(), ArrowError> {
        encode_blocked_range(out, start, end, |out, row| {
            self.values
                .encode(out, row.saturating_sub(self.values_offset))
        })
    }

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let offsets = self.list.offsets();
        let start = offsets[idx].to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Error converting offset[{idx}] to usize"))
        })?;
        let end = offsets[idx + 1].to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Error converting offset[{}] to usize",
                idx + 1
            ))
        })?;
        self.encode_list_range(out, start, end)
    }
}

fn prepare_value_site_encoder<'a>(
    values_array: &'a dyn Array,
    value_field: &Field,
    nullability: Option<Nullability>,
    plan: &FieldPlan,
) -> Result<FieldEncoder<'a>, ArrowError> {
    // Effective nullability is computed here from the writer-declared site nullability and data.
    FieldEncoder::make_encoder(values_array, value_field, plan, nullability)
}

/// Avro `fixed` encoder for Arrow `FixedSizeBinaryArray`.
/// Spec: a fixed is encoded as exactly `size` bytes, with no length prefix.
struct FixedEncoder<'a>(&'a FixedSizeBinaryArray);
impl FixedEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let v = self.0.value(idx); // &[u8] of fixed width
        out.write_all(v)
            .map_err(|e| ArrowError::IoError(format!("write fixed bytes: {e}"), e))
    }
}

/// Avro UUID logical type encoder: Arrow FixedSizeBinary(16) → Avro string (UUID).
/// Spec: uuid is a logical type over string (RFC‑4122). We output hyphenated form.
struct UuidEncoder<'a>(&'a FixedSizeBinaryArray);
impl UuidEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let v = self.0.value(idx);
        if v.len() != 16 {
            return Err(ArrowError::InvalidArgumentError(
                "logicalType=uuid requires FixedSizeBinary(16)".into(),
            ));
        }
        let u = Uuid::from_slice(v)
            .map_err(|e| ArrowError::InvalidArgumentError(format!("Invalid UUID bytes: {e}")))?;
        let mut tmp = [0u8; uuid::fmt::Hyphenated::LENGTH];
        let s = u.hyphenated().encode_lower(&mut tmp);
        write_len_prefixed(out, s.as_bytes())
    }
}

/// Avro `duration` encoder for Arrow `Interval(IntervalUnit::MonthDayNano)`.
/// Spec: `duration` annotates Avro fixed(12) with three **little‑endian u32**:
/// months, days, milliseconds (no negatives).
struct IntervalMonthDayNanoEncoder<'a>(&'a PrimitiveArray<IntervalMonthDayNanoType>);
impl IntervalMonthDayNanoEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let native = self.0.value(idx);
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(native);
        if months < 0 || days < 0 || nanos < 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' cannot encode negative months/days/nanoseconds".into(),
            ));
        }
        if nanos % 1_000_000 != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' requires whole milliseconds; nanoseconds must be divisible by 1_000_000"
                    .into(),
            ));
        }
        let millis = nanos / 1_000_000;
        if millis > u32::MAX as i64 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' milliseconds exceed u32::MAX".into(),
            ));
        }
        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&(months as u32).to_le_bytes());
        buf[4..8].copy_from_slice(&(days as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&(millis as u32).to_le_bytes());
        out.write_all(&buf)
            .map_err(|e| ArrowError::IoError(format!("write duration: {e}"), e))
    }
}

/// Avro `duration` encoder for Arrow `Interval(IntervalUnit::YearMonth)`.
struct IntervalYearMonthEncoder<'a>(&'a PrimitiveArray<IntervalYearMonthType>);
impl IntervalYearMonthEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let months_i32 = self.0.value(idx);

        if months_i32 < 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' cannot encode negative months".into(),
            ));
        }

        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&(months_i32 as u32).to_le_bytes());
        // Days and Milliseconds are zero, so their bytes are already 0.
        // buf[4..8] is [0, 0, 0, 0]
        // buf[8..12] is [0, 0, 0, 0]

        out.write_all(&buf)
            .map_err(|e| ArrowError::IoError(format!("write duration: {e}"), e))
    }
}

/// Avro `duration` encoder for Arrow `Interval(IntervalUnit::DayTime)`.
struct IntervalDayTimeEncoder<'a>(&'a PrimitiveArray<IntervalDayTimeType>);
impl IntervalDayTimeEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        // A DayTime interval is a packed (days: i32, milliseconds: i32).
        let native = self.0.value(idx);
        let (days, millis) = IntervalDayTimeType::to_parts(native);

        if days < 0 || millis < 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' cannot encode negative days or milliseconds".into(),
            ));
        }

        // (months=0, days, millis)
        let mut buf = [0u8; 12];
        // Months is zero. buf[0..4] is already [0, 0, 0, 0].
        buf[4..8].copy_from_slice(&(days as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&(millis as u32).to_le_bytes());

        out.write_all(&buf)
            .map_err(|e| ArrowError::IoError(format!("write duration: {e}"), e))
    }
}

/// Minimal trait to obtain a big-endian fixed-size byte array for a decimal's
/// unscaled integer value at `idx`.
trait DecimalBeBytes<const N: usize> {
    fn value_be_bytes(&self, idx: usize) -> [u8; N];
}

impl DecimalBeBytes<4> for Decimal32Array {
    fn value_be_bytes(&self, idx: usize) -> [u8; 4] {
        self.value(idx).to_be_bytes()
    }
}
impl DecimalBeBytes<8> for Decimal64Array {
    fn value_be_bytes(&self, idx: usize) -> [u8; 8] {
        self.value(idx).to_be_bytes()
    }
}
impl DecimalBeBytes<16> for Decimal128Array {
    fn value_be_bytes(&self, idx: usize) -> [u8; 16] {
        self.value(idx).to_be_bytes()
    }
}
impl DecimalBeBytes<32> for Decimal256Array {
    fn value_be_bytes(&self, idx: usize) -> [u8; 32] {
        // Arrow i256 → [u8; 32] big-endian
        self.value(idx).to_be_bytes()
    }
}

/// Generic Avro decimal encoder over Arrow decimal arrays.
/// - When `fixed_size` is `None` → Avro `bytes(decimal)`; writes the minimal
///   two's-complement representation with a length prefix.
/// - When `Some(n)` → Avro `fixed(n, decimal)`; sign-extends (or validates)
///   to exactly `n` bytes and writes them directly.
struct DecimalEncoder<'a, const N: usize, A: DecimalBeBytes<N>> {
    arr: &'a A,
    fixed_size: Option<usize>,
}

impl<'a, const N: usize, A: DecimalBeBytes<N>> DecimalEncoder<'a, N, A> {
    fn new(arr: &'a A, fixed_size: Option<usize>) -> Self {
        Self { arr, fixed_size }
    }

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let be = self.arr.value_be_bytes(idx);
        match self.fixed_size {
            Some(n) => {
                let bytes = sign_extend_to_exact(&be, n)?;
                out.write_all(&bytes)
                    .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e))
            }
            None => write_len_prefixed(out, minimal_twos_complement(&be)),
        }
    }
}

type Decimal32Encoder<'a> = DecimalEncoder<'a, 4, Decimal32Array>;
type Decimal64Encoder<'a> = DecimalEncoder<'a, 8, Decimal64Array>;
type Decimal128Encoder<'a> = DecimalEncoder<'a, 16, Decimal128Array>;
type Decimal256Encoder<'a> = DecimalEncoder<'a, 32, Decimal256Array>;

/// Avro `enum` encoder for Arrow `DictionaryArray<Int32, Utf8>`.
///
/// Per Avro spec, an enum is encoded as an **int** equal to the
/// zero-based position of the symbol in the schema’s `symbols` list.
/// We validate at construction that the dictionary values equal the symbols,
/// so we can directly write the key value here.
struct EnumEncoder<'a> {
    keys: &'a PrimitiveArray<Int32Type>,
}
impl EnumEncoder<'_> {
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, row: usize) -> Result<(), ArrowError> {
        let idx = self.keys.value(row);
        write_int(out, idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Fields};

    fn zigzag_i64(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }

    fn varint(mut x: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while (x & !0x7f) != 0 {
            out.push(((x & 0x7f) as u8) | 0x80);
            x >>= 7;
        }
        out.push((x & 0x7f) as u8);
        out
    }

    fn avro_long_bytes(v: i64) -> Vec<u8> {
        varint(zigzag_i64(v))
    }

    fn avro_len_prefixed_bytes(payload: &[u8]) -> Vec<u8> {
        let mut out = avro_long_bytes(payload.len() as i64);
        out.extend_from_slice(payload);
        out
    }

    fn encode_all(
        array: &dyn Array,
        plan: &FieldPlan,
        nullability: Option<Nullability>,
    ) -> Vec<u8> {
        let field = Field::new("f", array.data_type().clone(), true);
        let mut enc = FieldEncoder::make_encoder(array, &field, plan, nullability).unwrap();
        let mut out = Vec::new();
        for i in 0..array.len() {
            enc.encode(&mut out, i).unwrap();
        }
        out
    }

    fn assert_bytes_eq(actual: &[u8], expected: &[u8]) {
        if actual != expected {
            let to_hex = |b: &[u8]| {
                b.iter()
                    .map(|x| format!("{:02X}", x))
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            panic!(
                "mismatch\n  expected: [{}]\n    actual: [{}]",
                to_hex(expected),
                to_hex(actual)
            );
        }
    }

    #[test]
    fn binary_encoder() {
        let values: Vec<&[u8]> = vec![b"", b"ab", b"\x00\xFF"];
        let arr = BinaryArray::from_vec(values);
        let mut expected = Vec::new();
        for payload in [b"" as &[u8], b"ab", b"\x00\xFF"] {
            expected.extend(avro_len_prefixed_bytes(payload));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn large_binary_encoder() {
        let values: Vec<&[u8]> = vec![b"xyz", b""];
        let arr = LargeBinaryArray::from_vec(values);
        let mut expected = Vec::new();
        for payload in [b"xyz" as &[u8], b""] {
            expected.extend(avro_len_prefixed_bytes(payload));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn utf8_encoder() {
        let arr = StringArray::from(vec!["", "A", "BC"]);
        let mut expected = Vec::new();
        for s in ["", "A", "BC"] {
            expected.extend(avro_len_prefixed_bytes(s.as_bytes()));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn large_utf8_encoder() {
        let arr = LargeStringArray::from(vec!["hello", ""]);
        let mut expected = Vec::new();
        for s in ["hello", ""] {
            expected.extend(avro_len_prefixed_bytes(s.as_bytes()));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn list_encoder_int32() {
        // Build ListArray [[1,2], [], [3]]
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = vec![0, 2, 2, 3];
        let list = ListArray::new(
            Field::new("item", DataType::Int32, true).into(),
            arrow_buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(values) as ArrayRef,
            None,
        );
        // Avro array encoding per row
        let mut expected = Vec::new();
        // row 0: block len 2, items 1,2 then 0
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_long_bytes(1));
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_long_bytes(0));
        // row 1: empty
        expected.extend(avro_long_bytes(0));
        // row 2: one item 3
        expected.extend(avro_long_bytes(1));
        expected.extend(avro_long_bytes(3));
        expected.extend(avro_long_bytes(0));

        let plan = FieldPlan::List {
            items_nullability: None,
            item_plan: Box::new(FieldPlan::Scalar),
        };
        let got = encode_all(&list, &plan, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn struct_encoder_two_fields() {
        // Struct { a: Int32, b: Utf8 }
        let a = Int32Array::from(vec![1, 2]);
        let b = StringArray::from(vec!["x", "y"]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let struct_arr = StructArray::new(
            fields.clone(),
            vec![Arc::new(a) as ArrayRef, Arc::new(b) as ArrayRef],
            None,
        );
        let plan = FieldPlan::Struct {
            encoders: vec![
                FieldBinding {
                    arrow_index: 0,
                    nullability: None,
                    plan: FieldPlan::Scalar,
                },
                FieldBinding {
                    arrow_index: 1,
                    nullability: None,
                    plan: FieldPlan::Scalar,
                },
            ],
        };
        let got = encode_all(&struct_arr, &plan, None);
        // Expected: rows concatenated: a then b
        let mut expected = Vec::new();
        expected.extend(avro_long_bytes(1)); // a=1
        expected.extend(avro_len_prefixed_bytes(b"x")); // b="x"
        expected.extend(avro_long_bytes(2)); // a=2
        expected.extend(avro_len_prefixed_bytes(b"y")); // b="y"
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn enum_encoder_dictionary() {
        // symbols: ["A","B","C"], keys [2,0,1]
        let dict_values = StringArray::from(vec!["A", "B", "C"]);
        let keys = Int32Array::from(vec![2, 0, 1]);
        let dict =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(dict_values) as ArrayRef).unwrap();
        let symbols = Arc::<[String]>::from(
            vec!["A".to_string(), "B".to_string(), "C".to_string()].into_boxed_slice(),
        );
        let plan = FieldPlan::Enum { symbols };
        let got = encode_all(&dict, &plan, None);
        let mut expected = Vec::new();
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_long_bytes(0));
        expected.extend(avro_long_bytes(1));
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn decimal_bytes_and_fixed() {
        // Decimal64 with small positives and negatives
        let dec = Decimal64Array::from(vec![1i64, -1i64, 0i64])
            .with_precision_and_scale(10, 0)
            .unwrap();
        // bytes(decimal): minimal two's complement length-prefixed
        let plan_bytes = FieldPlan::Decimal { size: None };
        let got_bytes = encode_all(&dec, &plan_bytes, None);
        // 1 -> 0x01; -1 -> 0xFF; 0 -> 0x00
        let mut expected_bytes = Vec::new();
        expected_bytes.extend(avro_len_prefixed_bytes(
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01][7..],
        )); // 0x01
        expected_bytes.extend(avro_len_prefixed_bytes(&[0xFF]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x00]));
        assert_bytes_eq(&got_bytes, &expected_bytes);

        // fixed(8): sign-extend to 8 bytes as-is
        let plan_fixed = FieldPlan::Decimal { size: Some(8) };
        let got_fixed = encode_all(&dec, &plan_fixed, None);
        let mut expected_fixed = Vec::new();
        expected_fixed.extend_from_slice(&1i64.to_be_bytes());
        expected_fixed.extend_from_slice(&(-1i64).to_be_bytes());
        expected_fixed.extend_from_slice(&0i64.to_be_bytes());
        assert_bytes_eq(&got_fixed, &expected_fixed);
    }

    #[test]
    fn map_encoder_string_keys_int_values() {
        // Build MapArray with two rows
        // Row0: {"k1":1, "k2":2}
        // Row1: {}
        let keys = StringArray::from(vec!["k1", "k2"]);
        let values = Int32Array::from(vec![1, 2]);
        let entries_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries = StructArray::new(
            entries_fields,
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0i32, 2, 2].into());
        let map = MapArray::new(
            Field::new("entries", entries.data_type().clone(), false).into(),
            offsets,
            entries,
            None,
            false,
        );
        let plan = FieldPlan::Map {
            values_nullability: None,
            value_plan: Box::new(FieldPlan::Scalar),
        };
        let got = encode_all(&map, &plan, None);
        // Expected Avro per row: arrays of key,value
        let mut expected = Vec::new();
        // Row0: block 2 then pairs
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_len_prefixed_bytes(b"k1"));
        expected.extend(avro_long_bytes(1));
        expected.extend(avro_len_prefixed_bytes(b"k2"));
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_long_bytes(0));
        // Row1: empty
        expected.extend(avro_long_bytes(0));
        assert_bytes_eq(&got, &expected);
    }
}
