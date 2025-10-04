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
use crate::schema::{Fingerprint, Nullability, Prefix};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float32Type, Float64Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use arrow_array::{
    Array, Decimal128Array, Decimal256Array, DictionaryArray, FixedSizeBinaryArray,
    GenericBinaryArray, GenericListArray, GenericStringArray, LargeListArray, ListArray, MapArray,
    OffsetSizeTrait, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
#[cfg(feature = "small_decimals")]
use arrow_array::{Decimal32Array, Decimal64Array};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema as ArrowSchema, TimeUnit};
use serde::Serialize;
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

/// Encode a single Avro-`long` using ZigZag + variable length, buffered.
///
/// Spec: <https://avro.apache.org/docs/1.11.1/specification/#binary-encoding>
#[inline]
pub(crate) fn write_long<W: Write + ?Sized>(out: &mut W, value: i64) -> Result<(), ArrowError> {
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
/// For positive numbers, trim leading 0x00 until an essential byte is reached.
/// For negative numbers, trim leading 0xFF until an essential byte is reached.
/// The resulting slice still encodes the same signed value.
///
/// See Avro spec: decimal over `bytes` uses two's-complement big-endian
/// representation of the unscaled integer value. 1.11.1 specification.
#[inline]
fn minimal_twos_complement(be: &[u8]) -> &[u8] {
    if be.is_empty() {
        return be;
    }
    let sign_byte = if (be[0] & 0x80) != 0 { 0xFF } else { 0x00 };
    let mut k = 0usize;
    while k < be.len() && be[k] == sign_byte {
        k += 1;
    }
    if k == 0 {
        return be;
    }
    if k == be.len() {
        return &be[be.len() - 1..];
    }
    let drop = if ((be[k] ^ sign_byte) & 0x80) == 0 {
        k
    } else {
        k - 1
    };
    &be[drop..]
}

/// Sign-extend (or validate/truncate) big-endian integer bytes to exactly `n` bytes.
///
///
/// - If shorter than `n`, the slice is sign-extended by left-padding with the
///   sign byte (`0x00` for positive, `0xFF` for negative).
/// - If longer than `n`, the slice is truncated from the left. An overflow error
///   is returned if any of the truncated bytes are not redundant sign bytes,
///   or if the resulting value's sign bit would differ from the original.
/// - If the slice is already `n` bytes long, it is copied.
///
/// Used for encoding Avro decimal values into `fixed(N)` fields.
#[inline]
fn write_sign_extended<W: Write + ?Sized>(
    out: &mut W,
    src_be: &[u8],
    n: usize,
) -> Result<(), ArrowError> {
    let len = src_be.len();
    if len == n {
        return out
            .write_all(src_be)
            .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e));
    }
    let sign_byte = if len > 0 && (src_be[0] & 0x80) != 0 {
        0xFF
    } else {
        0x00
    };
    if len > n {
        let extra = len - n;
        if n == 0 && src_be.iter().all(|&b| b == sign_byte) {
            return Ok(());
        }
        // All truncated bytes must equal the sign byte, and the MSB of the first
        // retained byte must match the sign (otherwise overflow).
        if src_be[..extra].iter().any(|&b| b != sign_byte)
            || ((src_be[extra] ^ sign_byte) & 0x80) != 0
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Decimal value with {len} bytes cannot be represented in {n} bytes without overflow",
            )));
        }
        return out
            .write_all(&src_be[extra..])
            .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e));
    }
    // len < n: prepend sign bytes (sign extension) then the payload
    let pad_len = n - len;
    // Fixed-size stack pads to avoid heap allocation on the hot path
    const ZPAD: [u8; 64] = [0x00; 64];
    const FPAD: [u8; 64] = [0xFF; 64];
    let pad = if sign_byte == 0x00 {
        &ZPAD[..]
    } else {
        &FPAD[..]
    };
    // Emit padding in 64‑byte chunks (minimizes write calls without allocating),
    // then write the original bytes.
    let mut rem = pad_len;
    while rem >= pad.len() {
        out.write_all(pad)
            .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e))?;
        rem -= pad.len();
    }
    if rem > 0 {
        out.write_all(&pad[..rem])
            .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e))?;
    }
    out.write_all(src_be)
        .map_err(|e| ArrowError::IoError(format!("write decimal fixed: {e}"), e))
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
                    let arr = array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            ArrowError::SchemaError("Expected FixedSizeBinaryArray".into())
                        })?;
                    Encoder::Fixed(FixedEncoder(arr))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => Encoder::Timestamp(LongEncoder(
                    array.as_primitive::<TimestampMicrosecondType>(),
                )),
                DataType::Interval(unit) => match unit {
                    IntervalUnit::MonthDayNano => {
                        Encoder::IntervalMonthDayNano(DurationEncoder(
                            array.as_primitive::<IntervalMonthDayNanoType>(),
                        ))
                    }
                    IntervalUnit::YearMonth => {
                        Encoder::IntervalYearMonth(DurationEncoder(
                            array.as_primitive::<IntervalYearMonthType>(),
                        ))
                    }
                    IntervalUnit::DayTime => Encoder::IntervalDayTime(DurationEncoder(
                        array.as_primitive::<IntervalDayTimeType>(),
                    )),
                }
                DataType::Duration(tu) => {
                    match tu {
                        TimeUnit::Second => Encoder::DurationSeconds(LongEncoder(
                            array.as_primitive::<DurationSecondType>(),
                        )),
                        TimeUnit::Millisecond => Encoder::DurationMillis(LongEncoder(
                            array.as_primitive::<DurationMillisecondType>(),
                        )),
                        TimeUnit::Microsecond => Encoder::DurationMicros(LongEncoder(
                            array.as_primitive::<DurationMicrosecondType>(),
                        )),
                        TimeUnit::Nanosecond => Encoder::DurationNanos(LongEncoder(
                            array.as_primitive::<DurationNanosecondType>(),
                        )),
                    }
                }
                other => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Avro scalar type not yet supported: {other:?}"
                    )));
                }
            },
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
            FieldPlan::Decimal {size} => match array.data_type() {
                #[cfg(feature = "small_decimals")]
                DataType::Decimal32(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal32Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal32Array".into()))?;
                    Encoder::Decimal32(DecimalEncoder::<4, Decimal32Array>::new(arr, *size))
                }
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal64Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal64Array".into()))?;
                    Encoder::Decimal64(DecimalEncoder::<8, Decimal64Array>::new(arr, *size))
                }
                DataType::Decimal128(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal128Array".into()))?;
                    Encoder::Decimal128(DecimalEncoder::<16, Decimal128Array>::new(arr, *size))
                }
                DataType::Decimal256(_,_) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .ok_or_else(|| ArrowError::SchemaError("Expected Decimal256Array".into()))?;
                    Encoder::Decimal256(DecimalEncoder::<32, Decimal256Array>::new(arr, *size))
                }
                other => {
                    return Err(ArrowError::SchemaError(format!(
                        "Avro decimal site requires Arrow Decimal 32, 64, 128, or 256, found: {other:?}"
                    )))
                }
            },
            FieldPlan::Uuid => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| ArrowError::SchemaError("Expected FixedSizeBinaryArray".into()))?;
                Encoder::Uuid(UuidEncoder(arr))
            }
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
    /// Avro decimal logical type (bytes or fixed). `size=None` => bytes(decimal), `Some(n)` => fixed(n)
    Decimal { size: Option<usize> },
    /// Avro UUID logical type (fixed)
    Uuid,
    /// Avro map with value‑site nullability and nested plan
    Map {
        values_nullability: Option<Nullability>,
        value_plan: Box<FieldPlan>,
    },
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
pub(crate) struct RecordEncoderBuilder<'a> {
    avro_root: &'a AvroField,
    arrow_schema: &'a ArrowSchema,
    fingerprint: Option<Fingerprint>,
}

impl<'a> RecordEncoderBuilder<'a> {
    /// Create a new builder from the Avro root and Arrow schema.
    pub(crate) fn new(avro_root: &'a AvroField, arrow_schema: &'a ArrowSchema) -> Self {
        Self {
            avro_root,
            arrow_schema,
            fingerprint: None,
        }
    }

    pub(crate) fn with_fingerprint(mut self, fingerprint: Option<Fingerprint>) -> Self {
        self.fingerprint = fingerprint;
        self
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
        Ok(RecordEncoder {
            columns,
            prefix: self.fingerprint.map(|fp| fp.make_prefix()),
        })
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
    /// Optional pre-built, variable-length prefix written before each record.
    prefix: Option<Prefix>,
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
        let n = batch.num_rows();
        match self.prefix {
            Some(prefix) => {
                for row in 0..n {
                    out.write_all(prefix.as_slice())
                        .map_err(|e| ArrowError::IoError(format!("write prefix: {e}"), e))?;
                    for enc in column_encoders.iter_mut() {
                        enc.encode(out, row)?;
                    }
                }
            }
            None => {
                for row in 0..n {
                    for enc in column_encoders.iter_mut() {
                        enc.encode(out, row)?;
                    }
                }
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
        if let DataType::FixedSizeBinary(len) = arrow_field.data_type() {
            // Extension-based detection (only when the feature is enabled)
            let ext_is_uuid = {
                #[cfg(feature = "canonical_extension_types")]
                {
                    matches!(
                        arrow_field.extension_type_name(),
                        Some("arrow.uuid") | Some("uuid")
                    )
                }
                #[cfg(not(feature = "canonical_extension_types"))]
                {
                    false
                }
            };
            let md_is_uuid = arrow_field
                .metadata()
                .get("logicalType")
                .map(|s| s.as_str())
                == Some("uuid");
            if ext_is_uuid || md_is_uuid {
                if *len != 16 {
                    return Err(ArrowError::InvalidArgumentError(
                        "logicalType=uuid requires FixedSizeBinary(16)".into(),
                    ));
                }
                return Ok(FieldPlan::Uuid);
            }
        }
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
                    #[cfg(feature = "small_decimals")]
                    DataType::Decimal32(p, s) => (*p as usize, *s as i32),
                    #[cfg(feature = "small_decimals")]
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
    DurationSeconds(LongEncoder<'a, DurationSecondType>),
    DurationMillis(LongEncoder<'a, DurationMillisecondType>),
    DurationMicros(LongEncoder<'a, DurationMicrosecondType>),
    DurationNanos(LongEncoder<'a, DurationNanosecondType>),
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
    IntervalMonthDayNano(DurationEncoder<'a, IntervalMonthDayNanoType>),
    /// Avro `duration` logical type (Arrow Interval(YearMonth)) encoder
    IntervalYearMonth(DurationEncoder<'a, IntervalYearMonthType>),
    /// Avro `duration` logical type (Arrow Interval(DayTime)) encoder
    IntervalDayTime(DurationEncoder<'a, IntervalDayTimeType>),
    #[cfg(feature = "small_decimals")]
    Decimal32(Decimal32Encoder<'a>),
    #[cfg(feature = "small_decimals")]
    Decimal64(Decimal64Encoder<'a>),
    Decimal128(Decimal128Encoder<'a>),
    Decimal256(Decimal256Encoder<'a>),
    /// Avro `enum` encoder: writes the key (int) as the enum index.
    Enum(EnumEncoder<'a>),
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
            Encoder::DurationSeconds(e) => e.encode(out, idx),
            Encoder::DurationMicros(e) => e.encode(out, idx),
            Encoder::DurationMillis(e) => e.encode(out, idx),
            Encoder::DurationNanos(e) => e.encode(out, idx),
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
            #[cfg(feature = "small_decimals")]
            Encoder::Decimal32(e) => (e).encode(out, idx),
            #[cfg(feature = "small_decimals")]
            Encoder::Decimal64(e) => (e).encode(out, idx),
            Encoder::Decimal128(e) => (e).encode(out, idx),
            Encoder::Decimal256(e) => (e).encode(out, idx),
            Encoder::Map(e) => (e).encode(out, idx),
            Encoder::Enum(e) => (e).encode(out, idx),
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

    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let offsets = self.map.offsets();
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;

        let mut write_item = |out: &mut W, j: usize| {
            let j_val = j.saturating_sub(self.values_offset);
            self.values.encode(out, j_val)
        };

        match self.keys {
            KeyKind::Utf8(arr) => MapEncoder::<'a>::encode_map_entries(
                out,
                arr,
                self.keys_offset,
                start,
                end,
                write_item,
            ),
            KeyKind::LargeUtf8(arr) => MapEncoder::<'a>::encode_map_entries(
                out,
                arr,
                self.keys_offset,
                start,
                end,
                write_item,
            ),
        }
    }
}

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
        write_int(out, self.keys.value(row))
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
        let mut buf = [0u8; 1 + uuid::fmt::Hyphenated::LENGTH];
        buf[0] = 0x48;
        let v = self.0.value(idx);
        let u = Uuid::from_slice(v)
            .map_err(|e| ArrowError::InvalidArgumentError(format!("Invalid UUID bytes: {e}")))?;
        let _ = u.hyphenated().encode_lower(&mut buf[1..]);
        out.write_all(&buf)
            .map_err(|e| ArrowError::IoError(format!("write uuid: {e}"), e))
    }
}

#[derive(Copy, Clone)]
struct DurationParts {
    months: u32,
    days: u32,
    millis: u32,
}
/// Trait mapping an Arrow interval native value to Avro duration `(months, days, millis)`.
trait IntervalToDurationParts: ArrowPrimitiveType {
    fn duration_parts(native: Self::Native) -> Result<DurationParts, ArrowError>;
}
impl IntervalToDurationParts for IntervalMonthDayNanoType {
    fn duration_parts(native: Self::Native) -> Result<DurationParts, ArrowError> {
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
        Ok(DurationParts {
            months: months as u32,
            days: days as u32,
            millis: millis as u32,
        })
    }
}
impl IntervalToDurationParts for IntervalYearMonthType {
    fn duration_parts(native: Self::Native) -> Result<DurationParts, ArrowError> {
        if native < 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' cannot encode negative months".into(),
            ));
        }
        Ok(DurationParts {
            months: native as u32,
            days: 0,
            millis: 0,
        })
    }
}
impl IntervalToDurationParts for IntervalDayTimeType {
    fn duration_parts(native: Self::Native) -> Result<DurationParts, ArrowError> {
        let (days, millis) = IntervalDayTimeType::to_parts(native);
        if days < 0 || millis < 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Avro 'duration' cannot encode negative days or milliseconds".into(),
            ));
        }
        Ok(DurationParts {
            months: 0,
            days: days as u32,
            millis: millis as u32,
        })
    }
}
/// Single generic encoder used for all three interval units.
/// Writes Avro `fixed(12)` as three little-endian u32 values in one call.
struct DurationEncoder<'a, P: ArrowPrimitiveType + IntervalToDurationParts>(&'a PrimitiveArray<P>);
impl<'a, P: ArrowPrimitiveType + IntervalToDurationParts> DurationEncoder<'a, P> {
    #[inline(always)]
    fn encode<W: Write + ?Sized>(&mut self, out: &mut W, idx: usize) -> Result<(), ArrowError> {
        let parts = P::duration_parts(self.0.value(idx))?;
        let months = parts.months.to_le_bytes();
        let days = parts.days.to_le_bytes();
        let ms = parts.millis.to_le_bytes();
        // SAFETY
        // - Endianness & layout: Avro's `duration` logical type is encoded as fixed(12)
        //   with three *little-endian* unsigned 32-bit integers in order: (months, days, millis).
        //   We explicitly materialize exactly those 12 bytes.
        // - In-bounds indexing: `to_le_bytes()` on `u32` returns `[u8; 4]` by contract,
        //   therefore, the constant indices 0..=3 used below are *always* in-bounds.
        //   Rust will panic on out-of-bounds indexing, but there is no such path here;
        //   the compiler can also elide the bound checks for constant, provably in-range
        //   indices. [std docs; Rust Performance Book on bounds-check elimination]
        // - Memory safety: The `[u8; 12]` array is built on the stack by value, with no
        //   aliasing and no uninitialized memory. There is no `unsafe`.
        // - I/O: `write_all(&buf)` is fallible and its `Result` is propagated and mapped
        //   into `ArrowError`, so I/O errors are reported, not panicked.
        // Consequently, constructing `buf` with the constant indices below is safe and
        // panic-free under these validated preconditions.
        let buf = [
            months[0], months[1], months[2], months[3], days[0], days[1], days[2], days[3], ms[0],
            ms[1], ms[2], ms[3],
        ];
        out.write_all(&buf)
            .map_err(|e| ArrowError::IoError(format!("write duration: {e}"), e))
    }
}

/// Minimal trait to obtain a big-endian fixed-size byte array for a decimal's
/// unscaled integer value at `idx`.
trait DecimalBeBytes<const N: usize> {
    fn value_be_bytes(&self, idx: usize) -> [u8; N];
}
#[cfg(feature = "small_decimals")]
impl DecimalBeBytes<4> for Decimal32Array {
    fn value_be_bytes(&self, idx: usize) -> [u8; 4] {
        self.value(idx).to_be_bytes()
    }
}
#[cfg(feature = "small_decimals")]
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
            Some(n) => write_sign_extended(out, &be, n),
            None => write_len_prefixed(out, minimal_twos_complement(&be)),
        }
    }
}

#[cfg(feature = "small_decimals")]
type Decimal32Encoder<'a> = DecimalEncoder<'a, 4, Decimal32Array>;
#[cfg(feature = "small_decimals")]
type Decimal64Encoder<'a> = DecimalEncoder<'a, 8, Decimal64Array>;
type Decimal128Encoder<'a> = DecimalEncoder<'a, 16, Decimal128Array>;
type Decimal256Encoder<'a> = DecimalEncoder<'a, 32, Decimal256Array>;

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

    fn duration_fixed12(months: u32, days: u32, millis: u32) -> [u8; 12] {
        let m = months.to_le_bytes();
        let d = days.to_le_bytes();
        let ms = millis.to_le_bytes();
        [
            m[0], m[1], m[2], m[3], d[0], d[1], d[2], d[3], ms[0], ms[1], ms[2], ms[3],
        ]
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
        // Use Decimal128 with small positives and negatives
        let dec = Decimal128Array::from(vec![1i128, -1i128, 0i128])
            .with_precision_and_scale(20, 0)
            .unwrap();
        // bytes(decimal): minimal two's complement length-prefixed
        let plan_bytes = FieldPlan::Decimal { size: None };
        let got_bytes = encode_all(&dec, &plan_bytes, None);
        // 1 -> 0x01; -1 -> 0xFF; 0 -> 0x00
        let mut expected_bytes = Vec::new();
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x01]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0xFF]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x00]));
        assert_bytes_eq(&got_bytes, &expected_bytes);

        let plan_fixed = FieldPlan::Decimal { size: Some(16) };
        let got_fixed = encode_all(&dec, &plan_fixed, None);
        let mut expected_fixed = Vec::new();
        expected_fixed.extend_from_slice(&1i128.to_be_bytes());
        expected_fixed.extend_from_slice(&(-1i128).to_be_bytes());
        expected_fixed.extend_from_slice(&0i128.to_be_bytes());
        assert_bytes_eq(&got_fixed, &expected_fixed);
    }

    #[test]
    fn decimal_bytes_256() {
        use arrow_buffer::i256;
        // Use Decimal256 with small positives and negatives
        let dec = Decimal256Array::from(vec![
            i256::from_i128(1),
            i256::from_i128(-1),
            i256::from_i128(0),
        ])
        .with_precision_and_scale(76, 0)
        .unwrap();
        // bytes(decimal): minimal two's complement length-prefixed
        let plan_bytes = FieldPlan::Decimal { size: None };
        let got_bytes = encode_all(&dec, &plan_bytes, None);
        // 1 -> 0x01; -1 -> 0xFF; 0 -> 0x00
        let mut expected_bytes = Vec::new();
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x01]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0xFF]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x00]));
        assert_bytes_eq(&got_bytes, &expected_bytes);

        // fixed(32): 32-byte big-endian two's complement
        let plan_fixed = FieldPlan::Decimal { size: Some(32) };
        let got_fixed = encode_all(&dec, &plan_fixed, None);
        let mut expected_fixed = Vec::new();
        expected_fixed.extend_from_slice(&i256::from_i128(1).to_be_bytes());
        expected_fixed.extend_from_slice(&i256::from_i128(-1).to_be_bytes());
        expected_fixed.extend_from_slice(&i256::from_i128(0).to_be_bytes());
        assert_bytes_eq(&got_fixed, &expected_fixed);
    }

    #[cfg(feature = "small_decimals")]
    #[test]
    fn decimal_bytes_and_fixed_32() {
        // Use Decimal32 with small positives and negatives
        let dec = Decimal32Array::from(vec![1i32, -1i32, 0i32])
            .with_precision_and_scale(9, 0)
            .unwrap();
        // bytes(decimal)
        let plan_bytes = FieldPlan::Decimal { size: None };
        let got_bytes = encode_all(&dec, &plan_bytes, None);
        let mut expected_bytes = Vec::new();
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x01]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0xFF]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x00]));
        assert_bytes_eq(&got_bytes, &expected_bytes);
        // fixed(4)
        let plan_fixed = FieldPlan::Decimal { size: Some(4) };
        let got_fixed = encode_all(&dec, &plan_fixed, None);
        let mut expected_fixed = Vec::new();
        expected_fixed.extend_from_slice(&1i32.to_be_bytes());
        expected_fixed.extend_from_slice(&(-1i32).to_be_bytes());
        expected_fixed.extend_from_slice(&0i32.to_be_bytes());
        assert_bytes_eq(&got_fixed, &expected_fixed);
    }

    #[cfg(feature = "small_decimals")]
    #[test]
    fn decimal_bytes_and_fixed_64() {
        // Use Decimal64 with small positives and negatives
        let dec = Decimal64Array::from(vec![1i64, -1i64, 0i64])
            .with_precision_and_scale(18, 0)
            .unwrap();
        // bytes(decimal)
        let plan_bytes = FieldPlan::Decimal { size: None };
        let got_bytes = encode_all(&dec, &plan_bytes, None);
        let mut expected_bytes = Vec::new();
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x01]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0xFF]));
        expected_bytes.extend(avro_len_prefixed_bytes(&[0x00]));
        assert_bytes_eq(&got_bytes, &expected_bytes);
        // fixed(8)
        let plan_fixed = FieldPlan::Decimal { size: Some(8) };
        let got_fixed = encode_all(&dec, &plan_fixed, None);
        let mut expected_fixed = Vec::new();
        expected_fixed.extend_from_slice(&1i64.to_be_bytes());
        expected_fixed.extend_from_slice(&(-1i64).to_be_bytes());
        expected_fixed.extend_from_slice(&0i64.to_be_bytes());
        assert_bytes_eq(&got_fixed, &expected_fixed);
    }

    #[test]
    fn float32_and_float64_encoders() {
        let f32a = Float32Array::from(vec![0.0f32, -1.5f32, f32::from_bits(0x7fc00000)]); // includes a quiet NaN bit pattern
        let f64a = Float64Array::from(vec![0.0f64, -2.25f64]);
        // f32 expected
        let mut expected32 = Vec::new();
        for v in [0.0f32, -1.5f32, f32::from_bits(0x7fc00000)] {
            expected32.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        let got32 = encode_all(&f32a, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got32, &expected32);
        // f64 expected
        let mut expected64 = Vec::new();
        for v in [0.0f64, -2.25f64] {
            expected64.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        let got64 = encode_all(&f64a, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got64, &expected64);
    }

    #[test]
    fn long_encoder_int64() {
        let arr = Int64Array::from(vec![0i64, 1i64, -1i64, 2i64, -2i64, i64::MIN + 1]);
        let mut expected = Vec::new();
        for v in [0, 1, -1, 2, -2, i64::MIN + 1] {
            expected.extend(avro_long_bytes(v));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn fixed_encoder_plain() {
        // Two values of width 4
        let data = [[0xDE, 0xAD, 0xBE, 0xEF], [0x00, 0x01, 0x02, 0x03]];
        let values: Vec<Vec<u8>> = data.iter().map(|x| x.to_vec()).collect();
        let arr = FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        let mut expected = Vec::new();
        expected.extend_from_slice(&data[0]);
        expected.extend_from_slice(&data[1]);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn uuid_encoder_test() {
        // Happy path
        let u = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let bytes = *u.as_bytes();
        let arr_ok = FixedSizeBinaryArray::try_from_iter(vec![bytes.to_vec()].into_iter()).unwrap();
        // Expected: length 36 (0x48) followed by hyphenated lowercase text
        let mut expected = Vec::new();
        expected.push(0x48);
        expected.extend_from_slice(u.hyphenated().to_string().as_bytes());
        let got = encode_all(&arr_ok, &FieldPlan::Uuid, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn uuid_encoder_error() {
        // Invalid UUID bytes: wrong length
        let arr =
            FixedSizeBinaryArray::try_new(10, arrow_buffer::Buffer::from(vec![0u8; 10]), None)
                .unwrap();
        let plan = FieldPlan::Uuid;

        let field = Field::new("f", arr.data_type().clone(), true);
        let mut enc = FieldEncoder::make_encoder(&arr, &field, &plan, None).unwrap();
        let mut out = Vec::new();
        let err = enc.encode(&mut out, 0).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(msg) => {
                assert!(msg.contains("Invalid UUID bytes"))
            }
            other => panic!("expected InvalidArgumentError, got {other:?}"),
        }
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

    #[test]
    fn list64_encoder_int32() {
        // LargeList [[1,2,3], []]
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets: Vec<i64> = vec![0, 3, 3];
        let list = LargeListArray::new(
            Field::new("item", DataType::Int32, true).into(),
            arrow_buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(values) as ArrayRef,
            None,
        );
        let plan = FieldPlan::List {
            items_nullability: None,
            item_plan: Box::new(FieldPlan::Scalar),
        };
        let got = encode_all(&list, &plan, None);
        // Expected one block of 3 and then 0, then empty 0
        let mut expected = Vec::new();
        expected.extend(avro_long_bytes(3));
        expected.extend(avro_long_bytes(1));
        expected.extend(avro_long_bytes(2));
        expected.extend(avro_long_bytes(3));
        expected.extend(avro_long_bytes(0));
        expected.extend(avro_long_bytes(0));
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn int_encoder_test() {
        let ints = Int32Array::from(vec![0, -1, 2]);
        let mut expected_i = Vec::new();
        for v in [0i32, -1, 2] {
            expected_i.extend(avro_long_bytes(v as i64));
        }
        let got_i = encode_all(&ints, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got_i, &expected_i);
    }

    #[test]
    fn boolean_encoder_test() {
        let bools = BooleanArray::from(vec![true, false]);
        let mut expected_b = Vec::new();
        expected_b.extend_from_slice(&[1]);
        expected_b.extend_from_slice(&[0]);
        let got_b = encode_all(&bools, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got_b, &expected_b);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn duration_encoding_seconds() {
        let arr: PrimitiveArray<DurationSecondType> = vec![0i64, -1, 2].into();
        let mut expected = Vec::new();
        for v in [0i64, -1, 2] {
            expected.extend_from_slice(&avro_long_bytes(v));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn duration_encoding_milliseconds() {
        let arr: PrimitiveArray<DurationMillisecondType> = vec![1i64, 0, -2].into();
        let mut expected = Vec::new();
        for v in [1i64, 0, -2] {
            expected.extend_from_slice(&avro_long_bytes(v));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn duration_encoding_microseconds() {
        let arr: PrimitiveArray<DurationMicrosecondType> = vec![5i64, -6, 7].into();
        let mut expected = Vec::new();
        for v in [5i64, -6, 7] {
            expected.extend_from_slice(&avro_long_bytes(v));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    #[cfg(feature = "avro_custom_types")]
    fn duration_encoding_nanoseconds() {
        let arr: PrimitiveArray<DurationNanosecondType> = vec![8i64, 9, -10].into();
        let mut expected = Vec::new();
        for v in [8i64, 9, -10] {
            expected.extend_from_slice(&avro_long_bytes(v));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn duration_encoder_year_month_happy_path() {
        let arr: PrimitiveArray<IntervalYearMonthType> = vec![0i32, 1i32, 25i32].into();
        let mut expected = Vec::new();
        for m in [0u32, 1u32, 25u32] {
            expected.extend_from_slice(&duration_fixed12(m, 0, 0));
        }
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn duration_encoder_year_month_rejects_negative() {
        let arr: PrimitiveArray<IntervalYearMonthType> = vec![-1i32].into();
        let field = Field::new("f", DataType::Interval(IntervalUnit::YearMonth), true);
        let mut enc = FieldEncoder::make_encoder(&arr, &field, &FieldPlan::Scalar, None).unwrap();
        let mut out = Vec::new();
        let err = enc.encode(&mut out, 0).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(msg) => {
                assert!(msg.contains("cannot encode negative months"))
            }
            other => panic!("expected InvalidArgumentError, got {other:?}"),
        }
    }

    #[test]
    fn duration_encoder_day_time_happy_path() {
        let v0 = IntervalDayTimeType::make_value(2, 500); // days=2, millis=500
        let v1 = IntervalDayTimeType::make_value(0, 0);
        let arr: PrimitiveArray<IntervalDayTimeType> = vec![v0, v1].into();
        let mut expected = Vec::new();
        expected.extend_from_slice(&duration_fixed12(0, 2, 500));
        expected.extend_from_slice(&duration_fixed12(0, 0, 0));
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn duration_encoder_day_time_rejects_negative() {
        let bad = IntervalDayTimeType::make_value(-1, 0);
        let arr: PrimitiveArray<IntervalDayTimeType> = vec![bad].into();
        let field = Field::new("f", DataType::Interval(IntervalUnit::DayTime), true);
        let mut enc = FieldEncoder::make_encoder(&arr, &field, &FieldPlan::Scalar, None).unwrap();
        let mut out = Vec::new();
        let err = enc.encode(&mut out, 0).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(msg) => {
                assert!(msg.contains("cannot encode negative days"))
            }
            other => panic!("expected InvalidArgumentError, got {other:?}"),
        }
    }

    #[test]
    fn duration_encoder_month_day_nano_happy_path() {
        let v0 = IntervalMonthDayNanoType::make_value(1, 2, 3_000_000); // -> millis = 3
        let v1 = IntervalMonthDayNanoType::make_value(0, 0, 0);
        let arr: PrimitiveArray<IntervalMonthDayNanoType> = vec![v0, v1].into();
        let mut expected = Vec::new();
        expected.extend_from_slice(&duration_fixed12(1, 2, 3));
        expected.extend_from_slice(&duration_fixed12(0, 0, 0));
        let got = encode_all(&arr, &FieldPlan::Scalar, None);
        assert_bytes_eq(&got, &expected);
    }

    #[test]
    fn duration_encoder_month_day_nano_rejects_non_ms_multiple() {
        let bad = IntervalMonthDayNanoType::make_value(0, 0, 1);
        let arr: PrimitiveArray<IntervalMonthDayNanoType> = vec![bad].into();
        let field = Field::new("f", DataType::Interval(IntervalUnit::MonthDayNano), true);
        let mut enc = FieldEncoder::make_encoder(&arr, &field, &FieldPlan::Scalar, None).unwrap();
        let mut out = Vec::new();
        let err = enc.encode(&mut out, 0).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(msg) => {
                assert!(msg.contains("requires whole milliseconds") || msg.contains("divisible"))
            }
            other => panic!("expected InvalidArgumentError, got {other:?}"),
        }
    }

    #[test]
    fn minimal_twos_complement_test() {
        let pos = [0x00, 0x00, 0x01];
        assert_eq!(minimal_twos_complement(&pos), &pos[2..]);
        let neg = [0xFF, 0xFF, 0x80]; // negative minimal is 0x80
        assert_eq!(minimal_twos_complement(&neg), &neg[2..]);
        let zero = [0x00, 0x00, 0x00];
        assert_eq!(minimal_twos_complement(&zero), &zero[2..]);
    }

    #[test]
    fn write_sign_extend_test() {
        let mut out = Vec::new();
        write_sign_extended(&mut out, &[0x01], 4).unwrap();
        assert_eq!(out, vec![0x00, 0x00, 0x00, 0x01]);
        out.clear();
        write_sign_extended(&mut out, &[0xFF], 4).unwrap();
        assert_eq!(out, vec![0xFF, 0xFF, 0xFF, 0xFF]);
        out.clear();
        // truncation success (sign bytes only removed)
        write_sign_extended(&mut out, &[0xFF, 0xFF, 0x80], 2).unwrap();
        assert_eq!(out, vec![0xFF, 0x80]);
        out.clear();
        // truncation overflow
        let err = write_sign_extended(&mut out, &[0x01, 0x00], 1).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(_) => {}
            _ => panic!("expected InvalidArgumentError"),
        }
    }

    #[test]
    fn duration_month_day_nano_overflow_millis() {
        // nanos leading to millis > u32::MAX
        let nanos = ((u64::from(u32::MAX) + 1) * 1_000_000) as i64;
        let v = IntervalMonthDayNanoType::make_value(0, 0, nanos);
        let arr: PrimitiveArray<IntervalMonthDayNanoType> = vec![v].into();
        let field = Field::new("f", DataType::Interval(IntervalUnit::MonthDayNano), true);
        let mut enc = FieldEncoder::make_encoder(&arr, &field, &FieldPlan::Scalar, None).unwrap();
        let mut out = Vec::new();
        let err = enc.encode(&mut out, 0).unwrap_err();
        match err {
            ArrowError::InvalidArgumentError(msg) => assert!(msg.contains("exceed u32::MAX")),
            _ => panic!("expected InvalidArgumentError"),
        }
    }

    #[test]
    fn fieldplan_decimal_precision_scale_mismatch_errors() {
        // Avro expects (10,2), Arrow has (12,2)
        use crate::codec::Codec;
        use std::collections::HashMap;
        let arrow_field = Field::new("d", DataType::Decimal128(12, 2), true);
        let avro_dt = AvroDataType::new(Codec::Decimal(10, Some(2), None), HashMap::new(), None);
        let err = FieldPlan::build(&avro_dt, &arrow_field).unwrap_err();
        match err {
            ArrowError::SchemaError(msg) => {
                assert!(msg.contains("Decimal precision/scale mismatch"))
            }
            _ => panic!("expected SchemaError"),
        }
    }
}
