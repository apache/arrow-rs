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
    ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type, IntervalMonthDayNanoType,
    TimestampMicrosecondType,
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

/// Plan reference passed to the unified encoder constructor (required).
type PlanRef<'p> = &'p FieldPlan;

/// Encode a single Avro-`long` using ZigZag + variable length, buffered.
///
/// Spec: <https://avro.apache.org/docs/1.11.1/specification/#binary-encoding>
#[inline]
pub fn write_long<W: Write + ?Sized>(writer: &mut W, value: i64) -> Result<(), ArrowError> {
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
    writer
        .write_all(&buf[..i])
        .map_err(|e| ArrowError::IoError(format!("write long: {e}"), e))
}

#[inline]
fn write_int<W: Write + ?Sized>(writer: &mut W, value: i32) -> Result<(), ArrowError> {
    write_long(writer, value as i64)
}

#[inline]
fn write_len_prefixed<W: Write + ?Sized>(writer: &mut W, bytes: &[u8]) -> Result<(), ArrowError> {
    write_long(writer, bytes.len() as i64)?;
    writer
        .write_all(bytes)
        .map_err(|e| ArrowError::IoError(format!("write bytes: {e}"), e))
}

#[inline]
fn write_bool<W: Write + ?Sized>(writer: &mut W, v: bool) -> Result<(), ArrowError> {
    writer
        .write_all(&[if v { 1 } else { 0 }])
        .map_err(|e| ArrowError::IoError(format!("write bool: {e}"), e))
}

/// Write the union branch index for an optional field.
///
/// Branch index is 0-based per Avro unions:
/// - Null-first (default): null => 0, value => 1
/// - Null-second (Impala): value => 0, null => 1
#[inline]
fn write_optional_index<W: Write + ?Sized>(
    writer: &mut W,
    is_null: bool,
    order: Nullability,
) -> Result<(), ArrowError> {
    // For NullFirst: null => 0x00, value => 0x02
    // For NullSecond: value => 0x00, null => 0x02
    let byte = match (order, is_null) {
        (Nullability::NullFirst, true) | (Nullablility::NullSecond, false) => 0x00,
        (Nullability::NullFirst, false) | (Nullability::NullSecond, true) => 0x02,
    };
    writer
        .write_all(&[byte])
        .map_err(|e| ArrowError::IoError(format!("write union branch: {e}"), e))
}

/// Per‑site encoder plan for a field. This mirrors Avro structure so nested
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
}

#[derive(Debug, Clone)]
struct FieldBinding {
    /// Index of the Arrow field/column associated with this Avro field site
    arrow_index: usize,
    /// Nullability/order for this site (None if required)
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
        let root_fields = match avro_root_dt.codec() {
            Codec::Struct(fields) => fields,
            _ => {
                return Err(ArrowError::SchemaError(
                    "Top-level Avro schema must be a record/struct".into(),
                ))
            }
        };
        let mut columns = Vec::with_capacity(root_fields.len());
        for root_field in root_fields.iter() {
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
    #[inline]
    pub fn encode<W: Write>(&self, batch: &RecordBatch, out: &mut W) -> Result<(), ArrowError> {
        let mut column_encoders = self.prepare_for_batch(batch)?;
        for row in 0..batch.num_rows() {
            for encoder in column_encoders.iter_mut() {
                encoder.encode(row, out)?;
            }
        }
        Ok(())
    }
}

fn find_struct_child_index(fields: &arrow_schema::Fields, name: &str) -> Option<usize> {
    fields.iter().position(|f| f.name() == name)
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
}

impl<'a> Encoder<'a> {
    /// Encode the value at `idx`.
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        match self {
            Encoder::Boolean(e) => e.encode(idx, out),
            Encoder::Int(e) => e.encode(idx, out),
            Encoder::Long(e) => e.encode(idx, out),
            Encoder::Timestamp(e) => e.encode(idx, out),
            Encoder::Float32(e) => e.encode(idx, out),
            Encoder::Float64(e) => e.encode(idx, out),
            Encoder::Binary(e) => e.encode(idx, out),
            Encoder::LargeBinary(e) => e.encode(idx, out),
            Encoder::Utf8(e) => e.encode(idx, out),
            Encoder::Utf8Large(e) => e.encode(idx, out),
            Encoder::List(e) => e.encode(idx, out),
            Encoder::LargeList(e) => e.encode(idx, out),
            Encoder::Struct(e) => e.encode(idx, out),
        }
    }
}

struct BooleanEncoder<'a>(&'a arrow_array::BooleanArray);
impl BooleanEncoder<'_> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        write_bool(out, self.0.value(idx))
    }
}

/// Generic Avro `int` encoder for primitive arrays with `i32` native values.
struct IntEncoder<'a, P: ArrowPrimitiveType<Native = i32>>(&'a PrimitiveArray<P>);
impl<'a, P: ArrowPrimitiveType<Native = i32>> IntEncoder<'a, P> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        write_int(out, self.0.value(idx))
    }
}

/// Generic Avro `long` encoder for primitive arrays with `i64` native values.
struct LongEncoder<'a, P: ArrowPrimitiveType<Native = i64>>(&'a PrimitiveArray<P>);
impl<'a, P: ArrowPrimitiveType<Native = i64>> LongEncoder<'a, P> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        write_long(out, self.0.value(idx))
    }
}

/// Unified binary encoder generic over offset size (i32/i64).
struct BinaryEncoder<'a, O: OffsetSizeTrait>(&'a GenericBinaryArray<O>);
impl<'a, O: OffsetSizeTrait> BinaryEncoder<'a, O> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        write_len_prefixed(out, self.0.value(idx))
    }
}

struct F32Encoder<'a>(&'a arrow_array::Float32Array);
impl F32Encoder<'_> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        // Avro float: 4 bytes, IEEE-754 little-endian
        let bits = self.0.value(idx).to_bits();
        out.write_all(&bits.to_le_bytes())
            .map_err(|e| ArrowError::IoError(format!("write f32: {e}"), e))
    }
}

struct F64Encoder<'a>(&'a arrow_array::Float64Array);
impl F64Encoder<'_> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        // Avro double: 8 bytes, IEEE-754 little-endian
        let bits = self.0.value(idx).to_bits();
        out.write_all(&bits.to_le_bytes())
            .map_err(|e| ArrowError::IoError(format!("write f64: {e}"), e))
    }
}

struct Utf8GenericEncoder<'a, O: OffsetSizeTrait>(&'a GenericStringArray<O>);

impl<'a, O: OffsetSizeTrait> Utf8GenericEncoder<'a, O> {
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        write_len_prefixed(out, self.0.value(idx).as_bytes())
    }
}

type Utf8Encoder<'a> = Utf8GenericEncoder<'a, i32>;
type Utf8LargeEncoder<'a> = Utf8GenericEncoder<'a, i64>;

/// Unified field encoder:
/// - Holds the inner `Encoder` (by value)
/// - Tracks the column/site null buffer and whether any nulls exist
/// - Carries per-site Avro `Nullability` and precomputed union branch (fast path)
pub struct FieldEncoder<'a> {
    encoder: Encoder<'a>,
    nulls: Option<NullBuffer>,
    has_nulls: bool,
    nullability: Option<Nullability>,
    /// Precomputed constant branch byte if the site is nullable but contains no nulls
    pre: Option<u8>,
}

impl<'a> FieldEncoder<'a> {
    fn make_encoder(
        array: &'a dyn Array,
        field: &Field,
        plan: PlanRef<'_>,
    ) -> Result<Self, ArrowError> {
        let nulls = array.nulls().cloned();
        let has_nulls = array.null_count() > 0;
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
                DataType::Timestamp(TimeUnit::Microsecond, _) => Encoder::Timestamp(LongEncoder(
                    array.as_primitive::<TimestampMicrosecondType>(),
                )),
                other => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Avro scalar type not yet supported: {other:?}"
                    )));
                }
            },
            other => {
                return Err(ArrowError::NotYetImplemented(
                    "Avro writer: {other:?} not yet supported".into(),
                ));
            }
        };
        Ok(Self {
            encoder,
            nulls,
            has_nulls,
            nullability: None,
            pre: None,
        })
    }

    #[inline]
    fn has_nulls(&self) -> bool {
        self.has_nulls
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        self.nulls
            .as_ref()
            .is_some_and(|null_buffer| null_buffer.is_null(idx))
    }

    #[inline]
    fn with_effective_nullability(mut self, order: Option<Nullability>) -> Self {
        self.nullability = order;
        self.pre = self.precomputed_union_value_branch(order);
        self
    }

    #[inline]
    fn precomputed_union_value_branch(&self, order: Option<Nullability>) -> Option<u8> {
        match (order, self.has_nulls()) {
            (Some(Nullability::NullFirst), false) => Some(0x02), // value branch index 1
            (Some(Nullability::NullSecond), false) => Some(0x00), // value branch index 0
            _ => None,
        }
    }

    #[inline]
    fn encode_inner<W: Write + ?Sized>(
        &mut self,
        idx: usize,
        out: &mut W,
    ) -> Result<(), ArrowError> {
        self.encoder.encode(idx, out)
    }

    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        if let Some(b) = self.pre {
            out
                .write_all(&[b])
                .map_err(|e| ArrowError::IoError(format!("write union value branch: {e}"), e))?;
        } else if let Some(null_order) = self.nullability {
            let is_null = self.is_null(idx);
            write_optional_index(out, is_null, null_order)?;
            if is_null {
                return Ok(());
            }
        }
        self.encode_inner(idx, out)
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
        let fields = match array.data_type() {
            DataType::Struct(struct_fields) => struct_fields,
            _ => return Err(ArrowError::SchemaError("Expected Struct".into())),
        };
        let mut encoders = Vec::with_capacity(field_bindings.len());
        for field_binding in field_bindings {
            let idx = field_binding.arrow_index;
            let column = array.columns().get(idx).ok_or_else(|| {
                ArrowError::SchemaError(format!("Struct child index {idx} out of range"))
            })?;
            let field = fields
                .get(idx)
                .ok_or_else(|| {
                    ArrowError::SchemaError(format!("Struct child index {idx} out of range"))
                })?
                .as_ref();
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

    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        for encoder in self.encoders.iter_mut() {
            encoder.encode(idx, out)?;
        }
        Ok(())
    }
}

#[inline]
fn encode_blocked_range<W: Write + ?Sized, F>(
    out: &mut W,
    start: usize,
    end: usize,
    mut write_item: F,
) -> Result<(), ArrowError>
where
    F: FnMut(usize, &mut W) -> Result<(), ArrowError>,
{
    let len = end.saturating_sub(start);
    if len == 0 {
        // Zero-length terminator per Avro spec
        write_long(out, 0)?;
        return Ok(());
    }
    // Emit a single positive block for performance, then the end marker.
    write_long(out, len as i64)?;
    for j in start..end {
        write_item(j, out)?;
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

    #[inline]
    fn encode_list_range<W: Write + ?Sized>(
        &mut self,
        out: &mut W,
        start: usize,
        end: usize,
    ) -> Result<(), ArrowError> {
        encode_blocked_range(out, start, end, |row, out| {
            self.values
                .encode(row.saturating_sub(self.values_offset), out)
        })
    }

    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
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

#[inline]
fn prepare_value_site_encoder<'a>(
    values_array: &'a dyn Array,
    value_field: &Field,
    site_nullability: Option<Nullability>,
    plan: PlanRef<'_>,
) -> Result<FieldEncoder<'a>, ArrowError> {
    // Effective nullability is exactly the site's Avro-declared nullability.
    Ok(FieldEncoder::make_encoder(values_array, value_field, plan)?
        .with_effective_nullability(site_nullability))
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

    fn encode_all(array: &dyn Array, plan: &FieldPlan, site: Option<Nullability>) -> Vec<u8> {
        let field = Field::new("f", array.data_type().clone(), true);
        let mut enc = FieldEncoder::make_encoder(array, &field, plan)
            .unwrap()
            .with_effective_nullability(site);
        let mut out = Vec::new();
        for i in 0..array.len() {
            enc.encode(i, &mut out).unwrap();
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
}
