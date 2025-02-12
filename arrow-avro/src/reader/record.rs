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

use crate::codec::{AvroDataType, Codec, Nullability};
use crate::reader::cursor::AvroCursor;
use arrow_array::builder::{Decimal128Builder, Decimal256Builder, PrimitiveBuilder};
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::*;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, FieldRef, Fields, IntervalUnit,
    Schema as ArrowSchema, SchemaRef, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use std::cmp::Ordering;
use std::sync::Arc;

const DEFAULT_CAPACITY: usize = 1024;

/// A decoder that converts Avro-encoded data into an Arrow [`RecordBatch`].
#[derive(Debug)]
pub struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
}

impl RecordDecoder {
    /// Create a new [`RecordDecoder`] from an [`AvroDataType`] that must be a `Record`.
    ///
    /// - `strict_mode`: if `true`, we will throw an error if we encounter
    ///   a union of the form `[T, "null"]` (i.e. `Nullability::NullSecond`).
    pub fn try_new(data_type: &AvroDataType, strict_mode: bool) -> Result<Self, ArrowError> {
        match Decoder::try_new(data_type, strict_mode)? {
            Decoder::Record(fields, decoders) => Ok(Self {
                schema: Arc::new(ArrowSchema::new(fields)),
                fields: decoders,
            }),
            other => Err(ArrowError::ParseError(format!(
                "Expected record got {other:?}"
            ))),
        }
    }

    /// Return the [`SchemaRef`] describing the Arrow schema of rows produced by this decoder.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decode `count` Avro records from `buf`.
    ///
    /// This accumulates data in internal buffers. Once done reading, call
    /// [`Self::flush`] to yield an Arrow [`RecordBatch`].
    pub fn decode(&mut self, buf: &[u8], count: usize) -> Result<usize, ArrowError> {
        let mut cursor = AvroCursor::new(buf);
        for _ in 0..count {
            for field in &mut self.fields {
                field.decode(&mut cursor)?;
            }
        }
        Ok(cursor.position())
    }

    /// Flush into a [`RecordBatch`],
    ///
    /// We collect arrays from each `Decoder` and build a new [`RecordBatch`].
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let arrays = self
            .fields
            .iter_mut()
            .map(|d| d.flush(None))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// For 2-branch unions we store either `[null, T]` or `[T, null]`.
///
/// - `NullFirst`: `[null, T]` => branch=0 => null, branch=1 => decode T
/// - `NullSecond`: `[T, null]` => branch=0 => decode T, branch=1 => null
#[derive(Debug, Copy, Clone)]
enum UnionOrder {
    NullFirst,
    NullSecond,
}

#[derive(Debug)]
enum Decoder {
    /// Primitive Types
    Null(usize),
    Boolean(BooleanBufferBuilder),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Binary(OffsetBufferBuilder<i32>, Vec<u8>),
    String(OffsetBufferBuilder<i32>, Vec<u8>),
    /// Complex Types
    Record(Fields, Vec<Decoder>),
    Enum(Arc<[String]>, Vec<i32>),
    List(FieldRef, OffsetBufferBuilder<i32>, Box<Decoder>),
    Map(
        FieldRef,
        OffsetBufferBuilder<i32>,
        OffsetBufferBuilder<i32>,
        Vec<u8>,
        Box<Decoder>,
    ),
    Nullable(UnionOrder, NullBufferBuilder, Box<Decoder>),
    Fixed(i32, Vec<u8>),
    /// Logical Types
    Decimal(usize, Option<usize>, Option<usize>, DecimalBuilder),
    Date32(Vec<i32>),
    TimeMillis(Vec<i32>),
    TimeMicros(Vec<i64>),
    TimestampMillis(bool, Vec<i64>),
    TimestampMicros(bool, Vec<i64>),
    Interval(Vec<IntervalMonthDayNano>),
}

impl Decoder {
    fn try_new(data_type: &AvroDataType, strict_mode: bool) -> Result<Self, ArrowError> {
        let base = match &data_type.codec {
            Codec::Null => Self::Null(0),
            Codec::Boolean => Self::Boolean(BooleanBufferBuilder::new(DEFAULT_CAPACITY)),
            Codec::Int32 => Self::Int32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Int64 => Self::Int64(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Float32 => Self::Float32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Float64 => Self::Float64(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Binary => Self::Binary(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            Codec::String => Self::String(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            Codec::Record(avro_fields) => {
                let mut fields = Vec::with_capacity(avro_fields.len());
                let mut children = Vec::with_capacity(avro_fields.len());
                for f in avro_fields.iter() {
                    // Recursively build a Decoder for each child
                    let child = Self::try_new(f.data_type(), strict_mode)?;
                    fields.push(f.field());
                    children.push(child);
                }
                Self::Record(fields.into(), children)
            }
            Codec::Enum(syms, _) => {
                Self::Enum(Arc::clone(syms), Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::Array(child) => {
                let child_dec = Self::try_new(child, strict_mode)?;
                let item_field = child.field_with_name("item").with_nullable(true);
                Self::List(
                    Arc::new(item_field),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Box::new(child_dec),
                )
            }
            Codec::Map(child) => {
                let val_field = child.field_with_name("value").with_nullable(true);
                let map_field = Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        ArrowField::new("key", DataType::Utf8, false),
                        val_field,
                    ])),
                    false,
                ));
                let valdec = Self::try_new(child, strict_mode)?;
                Self::Map(
                    map_field,
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                    Box::new(valdec),
                )
            }
            Codec::Fixed(sz) => Self::Fixed(*sz, Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Decimal(p, s, size) => {
                let b = DecimalBuilder::new(*p, *s, *size)?;
                Self::Decimal(*p, *s, *size, b)
            }
            Codec::Uuid => Self::Fixed(16, Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Date32 => Self::Date32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMillis => Self::TimeMillis(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMicros => Self::TimeMicros(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimestampMillis(utc) => {
                Self::TimestampMillis(*utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::TimestampMicros(utc) => {
                Self::TimestampMicros(*utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::Duration => Self::Interval(Vec::with_capacity(DEFAULT_CAPACITY)),
        };
        let union_order = match data_type.nullability {
            None => None,
            Some(Nullability::NullFirst) => Some(UnionOrder::NullFirst),
            Some(Nullability::NullSecond) => {
                if strict_mode {
                    return Err(ArrowError::ParseError(
                        "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
                            .to_string(),
                    ));
                }
                Some(UnionOrder::NullSecond)
            }
        };
        let decoder = match union_order {
            Some(order) => Decoder::Nullable(
                order,
                NullBufferBuilder::new(DEFAULT_CAPACITY),
                Box::new(base),
            ),
            None => base,
        };
        Ok(decoder)
    }

    fn append_null(&mut self) {
        match self {
            Self::Null(n) => *n += 1,
            Self::Boolean(b) => b.append(false),
            Self::Int32(v) | Self::Date32(v) | Self::TimeMillis(v) => v.push(0),
            Self::Int64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v) => v.push(0),
            Self::Float32(v) => v.push(0.0),
            Self::Float64(v) => v.push(0.0),
            Self::Binary(off, _) | Self::String(off, _) => off.push_length(0),
            Self::Record(_, children) => {
                for c in children {
                    c.append_null();
                }
            }
            Self::Enum(_, idxs) => idxs.push(0),
            Self::List(_, off, _) => {
                off.push_length(0);
            }
            Self::Map(_, _koff, moff, _kdata, _valdec) => {
                moff.push_length(0);
            }
            Self::Nullable(_, nb, child) => {
                nb.append(false);
                child.append_null();
            }
            Self::Fixed(sz, accum) => {
                accum.extend(std::iter::repeat(0u8).take(*sz as usize));
            }
            Self::Decimal(_, _, _, db) => {
                let _ = db.append_null();
            }
            Self::Interval(ivals) => {
                ivals.push(IntervalMonthDayNano {
                    months: 0,
                    days: 0,
                    nanoseconds: 0,
                });
            }
        }
    }

    fn decode(&mut self, buf: &mut AvroCursor) -> Result<(), ArrowError> {
        match self {
            Self::Null(n) => {
                *n += 1;
            }
            Self::Boolean(b) => {
                b.append(buf.get_bool()?);
            }
            Self::Int32(v) => {
                v.push(buf.get_int()?);
            }
            Self::Int64(v) => {
                v.push(buf.get_long()?);
            }
            Self::Float32(vals) => {
                vals.push(buf.get_float()?);
            }
            Self::Float64(vals) => {
                vals.push(buf.get_double()?);
            }
            Self::Binary(off, data) | Self::String(off, data) => {
                let bytes = buf.get_bytes()?;
                off.push_length(bytes.len());
                data.extend_from_slice(bytes);
            }
            Self::Record(_, children) => {
                for c in children {
                    c.decode(buf)?;
                }
            }
            Self::Enum(_, idxs) => {
                idxs.push(buf.get_int()?);
            }
            Self::List(_, off, child) => {
                let total_items = read_array_blocks(buf, |cursor| child.decode(cursor))?;
                off.push_length(total_items);
            }
            Self::Map(_, koff, moff, kdata, valdec) => {
                let newly_added = read_map_blocks(buf, |cur| {
                    let kb = cur.get_bytes()?;
                    koff.push_length(kb.len());
                    kdata.extend_from_slice(kb);
                    valdec.decode(cur)
                })?;
                moff.push_length(newly_added);
            }
            Self::Nullable(order, nb, child) => {
                let branch = buf.get_int()?;
                match order {
                    UnionOrder::NullFirst => {
                        if branch == 0 {
                            nb.append(false);
                            child.append_null();
                        } else {
                            nb.append(true);
                            child.decode(buf)?;
                        }
                    }
                    UnionOrder::NullSecond => {
                        if branch == 0 {
                            nb.append(true);
                            child.decode(buf)?;
                        } else {
                            nb.append(false);
                            child.append_null();
                        }
                    }
                }
            }
            Self::Fixed(sz, accum) => {
                let fx = buf.get_fixed(*sz as usize)?;
                accum.extend_from_slice(fx);
            }
            Self::Decimal(_, _, fsz, db) => {
                let raw = match *fsz {
                    Some(n) => buf.get_fixed(n)?,
                    None => buf.get_bytes()?,
                };
                db.append_bytes(raw)?;
            }
            Self::Date32(vals) => vals.push(buf.get_int()?),
            Self::TimeMillis(vals) => vals.push(buf.get_int()?),
            Self::TimeMicros(vals) => vals.push(buf.get_long()?),
            Self::TimestampMillis(_, vals) => vals.push(buf.get_long()?),
            Self::TimestampMicros(_, vals) => vals.push(buf.get_long()?),
            Self::Interval(ivals) => {
                let x = buf.get_fixed(12)?;
                let months = i32::from_le_bytes(x[0..4].try_into().unwrap());
                let days = i32::from_le_bytes(x[4..8].try_into().unwrap());
                let ms = i32::from_le_bytes(x[8..12].try_into().unwrap());
                let nanos = ms as i64 * 1_000_000;
                ivals.push(IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds: nanos,
                });
            }
        }
        Ok(())
    }

    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<Arc<dyn Array>, ArrowError> {
        match self {
            Self::Null(count) => {
                let c = std::mem::replace(count, 0);
                Ok(Arc::new(NullArray::new(c)) as Arc<dyn Array>)
            }
            Self::Boolean(b) => {
                let bits = b.finish();
                Ok(Arc::new(BooleanArray::new(bits, nulls)) as Arc<dyn Array>)
            }
            Self::Int32(v) => {
                let arr = flush_primitive::<Int32Type>(v, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Date32(v) => {
                let arr = flush_primitive::<Date32Type>(v, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Int64(v) => {
                let arr = flush_primitive::<Int64Type>(v, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Float32(v) => {
                let arr = flush_primitive::<Float32Type>(v, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Float64(v) => {
                let arr = flush_primitive::<Float64Type>(v, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Binary(off, data) => {
                let offsets = flush_offsets(off);
                let vals = flush_values(data).into();
                let arr = BinaryArray::new(offsets, vals, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::String(off, data) => {
                let offsets = flush_offsets(off);
                let vals = flush_values(data).into();
                let arr = StringArray::new(offsets, vals, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Record(fields, children) => {
                let mut child_arrays = Vec::with_capacity(children.len());
                for c in children {
                    child_arrays.push(c.flush(None)?);
                }
                let first_len = match child_arrays.first() {
                    Some(a) => a.len(),
                    None => 0,
                };
                for (i, arr) in child_arrays.iter().enumerate() {
                    if arr.len() != first_len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Inconsistent struct child length for field #{i}. Expected {first_len}, got {}",
                            arr.len()
                        )));
                    }
                }
                if let Some(n) = &nulls {
                    if n.len() != first_len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Struct null buffer length {} != struct fields length {first_len}",
                            n.len()
                        )));
                    }
                }
                let sarr = StructArray::new(fields.clone(), child_arrays, nulls);
                Ok(Arc::new(sarr) as Arc<dyn Array>)
            }
            Self::Enum(symbols, idxs) => {
                let dict_vals = StringArray::from_iter_values(symbols.iter());
                let i32arr = match nulls {
                    Some(nb) => {
                        let buff = Buffer::from_slice_ref(&*idxs);
                        PrimitiveArray::<Int32Type>::try_new(
                            arrow_buffer::ScalarBuffer::from(buff),
                            Some(nb),
                        )?
                    }
                    None => Int32Array::from_iter_values(idxs.iter().cloned()),
                };
                idxs.clear();
                let d = DictionaryArray::<Int32Type>::try_new(i32arr, Arc::new(dict_vals))?;
                Ok(Arc::new(d) as Arc<dyn Array>)
            }
            Self::List(item_field, off, child) => {
                let c = child.flush(None)?;
                let offsets = flush_offsets(off);
                let final_len = offsets.len() - 1;
                if let Some(n) = &nulls {
                    if n.len() != final_len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "List array null buffer length {} != final list length {final_len}",
                            n.len()
                        )));
                    }
                }
                let larr = ListArray::new(item_field.clone(), offsets, c, nulls);
                Ok(Arc::new(larr) as Arc<dyn Array>)
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
                let entries_struct = StructArray::new(
                    Fields::from(vec![
                        Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                        Arc::new(ArrowField::new("value", val_arr.data_type().clone(), true)),
                    ]),
                    vec![Arc::new(key_arr), val_arr],
                    None,
                );
                let map_arr = MapArray::new(map_field.clone(), moff, entries_struct, nulls, false);
                Ok(Arc::new(map_arr) as Arc<dyn Array>)
            }
            Self::Nullable(_, nb_builder, child) => {
                let mask = nb_builder.finish();
                child.flush(mask)
            }
            Self::Fixed(sz, accum) => {
                let b: Buffer = flush_values(accum).into();
                let arr = FixedSizeBinaryArray::try_new(*sz, b, nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Decimal(precision, scale, sz, builder) => {
                let p = *precision;
                let s = scale.unwrap_or(0);
                let new_b = DecimalBuilder::new(p, *scale, *sz)?;
                let old = std::mem::replace(builder, new_b);
                let arr = old.finish(nulls, p, s)?;
                Ok(arr)
            }
            Self::TimeMillis(vals) => {
                let arr = flush_primitive::<Time32MillisecondType>(vals, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::TimeMicros(vals) => {
                let arr = flush_primitive::<Time64MicrosecondType>(vals, nulls);
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::TimestampMillis(is_utc, vals) => {
                let arr = flush_primitive::<TimestampMillisecondType>(vals, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into()));
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::TimestampMicros(is_utc, vals) => {
                let arr = flush_primitive::<TimestampMicrosecondType>(vals, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into()));
                Ok(Arc::new(arr) as Arc<dyn Array>)
            }
            Self::Interval(ivals) => {
                let len = ivals.len();
                let mut b = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(len);
                for v in ivals.drain(..) {
                    b.append_value(v);
                }
                let arr = b
                    .finish()
                    .with_data_type(DataType::Interval(IntervalUnit::MonthDayNano));
                if let Some(nb) = nulls {
                    let arr_data = arr.into_data().into_builder().nulls(Some(nb));
                    let arr_data = arr_data.build()?;
                    Ok(
                        Arc::new(PrimitiveArray::<IntervalMonthDayNanoType>::from(arr_data))
                            as Arc<dyn Array>,
                    )
                } else {
                    Ok(Arc::new(arr) as Arc<dyn Array>)
                }
            }
        }
    }
}

fn read_array_blocks(
    buf: &mut AvroCursor,
    decode_item: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    read_blockwise_items(buf, true, decode_item)
}

fn read_map_blocks(
    buf: &mut AvroCursor,
    decode_entry: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    read_blockwise_items(buf, true, decode_entry)
}

fn read_blockwise_items(
    buf: &mut AvroCursor,
    read_size_after_negative: bool,
    mut decode_fn: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    let mut total = 0usize;
    loop {
        let blk = buf.get_long()?;
        match blk.cmp(&0) {
            Ordering::Equal => break,
            Ordering::Less => {
                let cnt = (-blk) as usize;
                if read_size_after_negative {
                    let _size_in_bytes = buf.get_long()?;
                }
                for _ in 0..cnt {
                    decode_fn(buf)?;
                }
                total += cnt;
            }
            Ordering::Greater => {
                let cnt = blk as usize;
                for _i in 0..cnt {
                    decode_fn(buf)?;
                }
                total += cnt;
            }
        }
    }
    Ok(total)
}

fn flush_primitive<T: ArrowPrimitiveType>(
    vals: &mut Vec<T::Native>,
    nb: Option<NullBuffer>,
) -> PrimitiveArray<T> {
    PrimitiveArray::new(std::mem::take(vals).into(), nb)
}

fn flush_offsets(ob: &mut OffsetBufferBuilder<i32>) -> OffsetBuffer<i32> {
    std::mem::replace(ob, OffsetBufferBuilder::new(DEFAULT_CAPACITY)).finish()
}

fn flush_values<T>(vec: &mut Vec<T>) -> Vec<T> {
    std::mem::replace(vec, Vec::with_capacity(DEFAULT_CAPACITY))
}

/// A builder for Avro decimal, either 128-bit or 256-bit.
#[derive(Debug)]
enum DecimalBuilder {
    Decimal128(Decimal128Builder),
    Decimal256(Decimal256Builder),
}

impl DecimalBuilder {
    fn new(
        precision: usize,
        scale: Option<usize>,
        size: Option<usize>,
    ) -> Result<Self, ArrowError> {
        let prec = precision as u8;
        let scl = scale.unwrap_or(0) as i8;
        if let Some(s) = size {
            if s <= 16 {
                return Ok(Self::Decimal128(
                    Decimal128Builder::new().with_precision_and_scale(prec, scl)?,
                ));
            }
            if s <= 32 {
                return Ok(Self::Decimal256(
                    Decimal256Builder::new().with_precision_and_scale(prec, scl)?,
                ));
            }
            return Err(ArrowError::ParseError(format!(
                "Unsupported decimal size: {s:?}"
            )));
        }
        if precision <= DECIMAL128_MAX_PRECISION as usize {
            Ok(Self::Decimal128(
                Decimal128Builder::new().with_precision_and_scale(prec, scl)?,
            ))
        } else if precision <= DECIMAL256_MAX_PRECISION as usize {
            Ok(Self::Decimal256(
                Decimal256Builder::new().with_precision_and_scale(prec, scl)?,
            ))
        } else {
            Err(ArrowError::ParseError(format!(
                "Decimal precision {} exceeds maximum supported",
                precision
            )))
        }
    }

    fn append_bytes(&mut self, raw: &[u8]) -> Result<(), ArrowError> {
        match self {
            Self::Decimal128(b) => {
                let ext = sign_extend_to_16(raw)?;
                let val = i128::from_be_bytes(ext);
                b.append_value(val);
            }
            Self::Decimal256(b) => {
                let ext = sign_extend_to_32(raw)?;
                let val = i256::from_be_bytes(ext);
                b.append_value(val);
            }
        }
        Ok(())
    }

    fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            Self::Decimal128(b) => {
                let zero = [0u8; 16];
                b.append_value(i128::from_be_bytes(zero));
            }
            Self::Decimal256(b) => {
                let zero = [0u8; 32];
                b.append_value(i256::from_be_bytes(zero));
            }
        }
        Ok(())
    }

    fn finish(
        self,
        nb: Option<NullBuffer>,
        precision: usize,
        scale: usize,
    ) -> Result<Arc<dyn Array>, ArrowError> {
        match self {
            Self::Decimal128(mut b) => {
                let arr = b.finish();
                let vals = arr.values().clone();
                let dec = Decimal128Array::new(vals, nb)
                    .with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(dec))
            }
            Self::Decimal256(mut b) => {
                let arr = b.finish();
                let vals = arr.values().clone();
                let dec = Decimal256Array::new(vals, nb)
                    .with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(dec))
            }
        }
    }
}

fn sign_extend_to_16(raw: &[u8]) -> Result<[u8; 16], ArrowError> {
    let ext = sign_extend(raw, 16);
    if ext.len() != 16 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend to 16 bytes, got {} bytes",
            ext.len()
        )));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&ext);
    Ok(arr)
}

fn sign_extend_to_32(raw: &[u8]) -> Result<[u8; 32], ArrowError> {
    let ext = sign_extend(raw, 32);
    if ext.len() != 32 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend to 32 bytes, got {} bytes",
            ext.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&ext);
    Ok(arr)
}

fn sign_extend(raw: &[u8], target_len: usize) -> Vec<u8> {
    if raw.is_empty() {
        return vec![0; target_len];
    }
    let sign_bit = raw[0] & 0x80;
    let mut out = Vec::with_capacity(target_len);
    if sign_bit != 0 {
        out.resize(target_len - raw.len(), 0xFF);
    } else {
        out.resize(target_len - raw.len(), 0x00);
    }
    out.extend_from_slice(raw);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::AvroField;
    use crate::schema::Schema;
    use arrow_array::{cast::AsArray, Array, ListArray, MapArray, StructArray};
    use std::sync::Arc;

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
        let mut out = encode_avro_long(bytes.len() as i64);
        out.extend_from_slice(bytes);
        out
    }

    fn encode_union_branch(branch_idx: i32) -> Vec<u8> {
        encode_avro_int(branch_idx)
    }

    fn encode_array<T>(items: &[T], mut encode_item: impl FnMut(&T) -> Vec<u8>) -> Vec<u8> {
        let mut out = Vec::new();
        if !items.is_empty() {
            out.extend_from_slice(&encode_avro_long(items.len() as i64));
            for it in items {
                out.extend_from_slice(&encode_item(it));
            }
        }
        out.extend_from_slice(&encode_avro_long(0));
        out
    }

    fn encode_map(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut out = Vec::new();
        if !entries.is_empty() {
            out.extend_from_slice(&encode_avro_long(entries.len() as i64));
            for (k, val) in entries {
                out.extend_from_slice(&encode_avro_bytes(k.as_bytes()));
                out.extend_from_slice(val);
            }
        }
        out.extend_from_slice(&encode_avro_long(0));
        out
    }

    #[test]
    fn test_union_primitive_long_null_record_decoder() {
        let json_schema = r#"
        {
            "type": "record",
            "name": "topLevelRecord",
            "fields": [
                {
                    "name": "id",
                    "type": ["long","null"]
                }
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_union_branch(0));
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_union_branch(1));
        let used = record_decoder.decode(&data, 2).unwrap();
        assert_eq!(used, data.len());
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let arr = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 1);
        assert!(arr.is_null(1));
    }

    #[test]
    fn test_union_array_of_int_null_record_decoder() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"topLevelRecord",
            "fields":[
                {
                    "name":"int_array",
                    "type":[
                        {
                            "type":"array",
                            "items":[ "int", "null" ]
                        },
                        "null"
                    ]
                }
            ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();

        fn encode_int_or_null(opt_val: &Option<i32>) -> Vec<u8> {
            match opt_val {
                Some(v) => {
                    let mut out = encode_union_branch(0);
                    out.extend_from_slice(&encode_avro_int(*v));
                    out
                }
                None => encode_union_branch(1),
            }
        }

        data.extend_from_slice(&encode_union_branch(0));
        let row1_values = vec![Some(1), Some(2), Some(3)];
        data.extend_from_slice(&encode_array(&row1_values, encode_int_or_null));
        data.extend_from_slice(&encode_union_branch(0));
        let row2_values = vec![None, Some(1), Some(2), None, Some(3), None];
        data.extend_from_slice(&encode_array(&row2_values, encode_int_or_null));
        data.extend_from_slice(&encode_union_branch(0));
        data.extend_from_slice(&encode_avro_long(0));
        data.extend_from_slice(&encode_union_branch(1));
        record_decoder.decode(&data, 4).unwrap();
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 4);
        let list_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(list_arr.is_null(3));
        {
            let start = list_arr.value_offsets()[0] as usize;
            let end = list_arr.value_offsets()[1] as usize;
            let child = list_arr.values().as_primitive::<Int32Type>();
            assert_eq!(end - start, 3);
            assert_eq!(child.value(start), 1);
            assert_eq!(child.value(start + 1), 2);
            assert_eq!(child.value(start + 2), 3);
        }
        {
            let start = list_arr.value_offsets()[1] as usize;
            let end = list_arr.value_offsets()[2] as usize;
            let child = list_arr.values().as_primitive::<Int32Type>();
            assert_eq!(end - start, 6);
            // index-by-index
            assert!(child.is_null(start)); // None
            assert_eq!(child.value(start + 1), 1); // Some(1)
            assert_eq!(child.value(start + 2), 2);
            assert!(child.is_null(start + 3));
            assert_eq!(child.value(start + 4), 3);
            assert!(child.is_null(start + 5));
        }
        {
            let start = list_arr.value_offsets()[2] as usize;
            let end = list_arr.value_offsets()[3] as usize;
            assert_eq!(end - start, 0);
        }
    }

    #[test]
    fn test_union_nested_array_of_int_null_record_decoder() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"topLevelRecord",
            "fields":[
                {
                    "name":"int_array_Array",
                    "type":[
                        {
                            "type":"array",
                            "items":[
                                {
                                    "type":"array",
                                    "items":[
                                        "int",
                                        "null"
                                    ]
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
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();

        fn encode_inner(vals: &[Option<i32>]) -> Vec<u8> {
            encode_array(vals, |o| match o {
                Some(v) => {
                    let mut out = encode_union_branch(0);
                    out.extend_from_slice(&encode_avro_int(*v));
                    out
                }
                None => encode_union_branch(1),
            })
        }

        data.extend_from_slice(&encode_union_branch(0));
        {
            let outer_vals: Vec<Option<Vec<Option<i32>>>> =
                vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), None])];
            data.extend_from_slice(&encode_array(&outer_vals, |maybe_arr| match maybe_arr {
                Some(vlist) => {
                    let mut out = encode_union_branch(0);
                    out.extend_from_slice(&encode_inner(vlist));
                    out
                }
                None => encode_union_branch(1),
            }));
        }
        data.extend_from_slice(&encode_union_branch(0));
        {
            let outer_vals: Vec<Option<Vec<Option<i32>>>> = vec![None];
            data.extend_from_slice(&encode_array(&outer_vals, |maybe_arr| match maybe_arr {
                Some(vlist) => {
                    let mut out = encode_union_branch(0);
                    out.extend_from_slice(&encode_inner(vlist));
                    out
                }
                None => encode_union_branch(1),
            }));
        }
        data.extend_from_slice(&encode_union_branch(1));
        record_decoder.decode(&data, 3).unwrap();
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let outer_list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(outer_list.is_null(2));
        assert!(!outer_list.is_null(0));
        let start = outer_list.value_offsets()[0] as usize;
        let end = outer_list.value_offsets()[1] as usize;
        assert_eq!(end - start, 2);
        let start2 = outer_list.value_offsets()[1] as usize;
        let end2 = outer_list.value_offsets()[2] as usize;
        assert_eq!(end2 - start2, 1);
        let subitem_arr = outer_list.value(1);
        let sub_list = subitem_arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(sub_list.len(), 1);
        assert!(sub_list.is_null(0));
    }

    #[test]
    fn test_union_map_of_int_null_record_decoder() {
        let json_schema = r#"
        {
            "type":"record",
            "name":"topLevelRecord",
            "fields":[
                {
                    "name":"int_map",
                    "type":[
                        {
                            "type":"map",
                            "values":[
                                "int",
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
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_union_branch(0));
        let row1_map = vec![
            ("k1", {
                let mut out = encode_union_branch(0);
                out.extend_from_slice(&encode_avro_int(1));
                out
            }),
            ("k2", encode_union_branch(1)),
        ];
        data.extend_from_slice(&encode_map(&row1_map));
        data.extend_from_slice(&encode_union_branch(0));
        let empty: [(&str, Vec<u8>); 0] = [];
        data.extend_from_slice(&encode_map(&empty));
        data.extend_from_slice(&encode_union_branch(1));
        record_decoder.decode(&data, 3).unwrap();
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let map_arr = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 3);
        assert!(map_arr.is_null(2));
        assert_eq!(map_arr.value_length(0), 2);
        let binding = map_arr.value(0);
        let struct_arr = binding.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_arr.column(0).as_string::<i32>();
        let vals = struct_arr.column(1).as_primitive::<Int32Type>();
        assert_eq!(keys.value(0), "k1");
        assert_eq!(vals.value(0), 1);
        assert_eq!(keys.value(1), "k2");
        assert!(vals.is_null(1));
        assert_eq!(map_arr.value_length(1), 0);
    }

    #[test]
    fn test_union_map_array_of_int_null_record_decoder() {
        let json_schema = r#"
        {
            "type": "record",
            "name": "topLevelRecord",
            "fields": [
                {
                    "name": "int_Map_Array",
                    "type": [
                        {
                            "type": "array",
                            "items": [
                                {
                                    "type": "map",
                                    "values": [
                                        "int",
                                        "null"
                                    ]
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
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();
        fn encode_map_int_null(entries: &[(&str, Option<i32>)]) -> Vec<u8> {
            let items: Vec<(&str, Vec<u8>)> = entries
                .iter()
                .map(|(k, v)| {
                    let val = match v {
                        Some(x) => {
                            let mut out = encode_union_branch(0);
                            out.extend_from_slice(&encode_avro_int(*x));
                            out
                        }
                        None => encode_union_branch(1),
                    };
                    (*k, val)
                })
                .collect();
            encode_map(&items)
        }
        data.extend_from_slice(&encode_union_branch(0));
        {
            let mut arr_buf = encode_avro_long(1);
            {
                let mut item_buf = encode_union_branch(0);
                item_buf.extend_from_slice(&encode_map_int_null(&[("k1", Some(1))]));
                arr_buf.extend_from_slice(&item_buf);
            }
            arr_buf.extend_from_slice(&encode_avro_long(0));
            data.extend_from_slice(&arr_buf);
        }
        data.extend_from_slice(&encode_union_branch(0));
        {
            let mut arr_buf = encode_avro_long(2); // 2 items
            arr_buf.extend_from_slice(&encode_union_branch(1));
            {
                let mut item1 = encode_union_branch(0);
                item1.extend_from_slice(&encode_map_int_null(&[("k2", None)]));
                arr_buf.extend_from_slice(&item1);
            }
            arr_buf.extend_from_slice(&encode_avro_long(0)); // end
            data.extend_from_slice(&arr_buf);
        }
        data.extend_from_slice(&encode_union_branch(1));
        record_decoder.decode(&data, 3).unwrap();
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let outer_list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(outer_list.is_null(2));
        {
            let start = outer_list.value_offsets()[0] as usize;
            let end = outer_list.value_offsets()[1] as usize;
            assert_eq!(end - start, 1);
            let subarr = outer_list.value(0);
            let sublist = subarr.as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(sublist.len(), 1);
            assert!(!sublist.is_null(0));
            let sub_value_0 = sublist.value(0);
            let struct_arr = sub_value_0.as_any().downcast_ref::<StructArray>().unwrap();
            let keys = struct_arr.column(0).as_string::<i32>();
            let vals = struct_arr.column(1).as_primitive::<Int32Type>();
            assert_eq!(keys.value(0), "k1");
            assert_eq!(vals.value(0), 1);
        }
    }

    #[test]
    fn test_union_nested_struct_out_of_spec_record_decoder() {
        let json_schema = r#"
    {
        "type":"record",
        "name":"topLevelRecord",
        "fields":[
            {
                "name":"nested_struct",
                "type":[
                    {
                        "type":"record",
                        "name":"nested_struct",
                        "namespace":"topLevelRecord",
                        "fields":[
                            {
                                "name":"A",
                                "type":[
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "name":"b",
                                "type":[
                                    {
                                        "type":"array",
                                        "items":[
                                            "int",
                                            "null"
                                        ]
                                    },
                                    "null"
                                ]
                            }
                        ]
                    },
                    "null"
                ]
            }
        ]
    }
    "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_record = AvroField::try_from(&schema).unwrap();
        let mut record_decoder = RecordDecoder::try_new(avro_record.data_type(), false).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_union_branch(0));
        data.extend_from_slice(&encode_union_branch(0));
        data.extend_from_slice(&encode_avro_int(7));
        data.extend_from_slice(&encode_union_branch(0));
        let row1_b = [Some(1), Some(2)];
        data.extend_from_slice(&encode_array(&row1_b, |val| match val {
            Some(x) => {
                let mut out = encode_union_branch(0);
                out.extend_from_slice(&encode_avro_int(*x));
                out
            }
            None => encode_union_branch(1),
        }));
        data.extend_from_slice(&encode_union_branch(0));
        data.extend_from_slice(&encode_union_branch(1));
        data.extend_from_slice(&encode_union_branch(1));
        data.extend_from_slice(&encode_union_branch(1));
        record_decoder.decode(&data, 3).unwrap();
        let batch = record_decoder.flush().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(col.is_null(2));
        let field_a = col.column(0).as_primitive::<Int32Type>();
        let field_b = col.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(field_a.value(0), 7);
        {
            let start = field_b.value_offsets()[0] as usize;
            let end = field_b.value_offsets()[1] as usize;
            let values = field_b.values().as_primitive::<Int32Type>();
            assert_eq!(end - start, 2);
            assert_eq!(values.value(start), 1);
            assert_eq!(values.value(start + 1), 2);
        }
        assert!(field_a.is_null(1));
        assert!(field_b.is_null(1));
    }

    #[test]
    fn test_record_decoder_default_metadata() {
        use crate::codec::AvroField;
        use crate::schema::Schema;
        let json_schema = r#"
        {
          "type": "record",
          "name": "TestRecord",
          "fields": [
              {"name": "default_int", "type": "int", "default": 42}
          ]
        }
        "#;
        let schema: Schema = serde_json::from_str(json_schema).unwrap();
        let avro_record = AvroField::try_from(&schema).unwrap();
        let record_decoder = RecordDecoder::try_new(avro_record.data_type(), true).unwrap();
        let arrow_schema = record_decoder.schema();
        assert_eq!(arrow_schema.fields().len(), 1);
        let field = arrow_schema.field(0);
        let metadata = field.metadata();
        assert_eq!(metadata.get("avro.default").unwrap(), "42");
    }

    #[test]
    fn test_fixed_decoding() {
        let dt = AvroDataType::from_codec(Codec::Fixed(4));
        let mut dec = Decoder::try_new(&dt, true).unwrap();
        let row1 = [0xDE, 0xAD, 0xBE, 0xEF];
        let row2 = [0x01, 0x23, 0x45, 0x67];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap();
        dec.decode(&mut cursor).unwrap();
        let arr = dec.flush(None).unwrap();
        let fsb = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(fsb.len(), 2);
        assert_eq!(fsb.value_length(), 4);
        assert_eq!(fsb.value(0), row1);
        assert_eq!(fsb.value(1), row2);
    }

    #[test]
    fn test_fixed_with_nulls() {
        let dt = AvroDataType::from_codec(Codec::Fixed(2));
        let child = Decoder::try_new(&dt, true).unwrap();
        let mut dec = Decoder::Nullable(
            UnionOrder::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(child),
        );
        let row1 = [0x11, 0x22];
        let row3 = [0x55, 0x66];
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row1);
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row3);
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap(); // Row1
        dec.decode(&mut cursor).unwrap(); // Row2 (null)
        dec.decode(&mut cursor).unwrap(); // Row3
        let arr = dec.flush(None).unwrap();
        let fsb = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(fsb.len(), 3);
        assert!(fsb.is_valid(0));
        assert!(!fsb.is_valid(1));
        assert!(fsb.is_valid(2));
        assert_eq!(fsb.value_length(), 2);
        assert_eq!(fsb.value(0), row1);
        assert_eq!(fsb.value(2), row3);
    }

    #[test]
    fn test_interval_decoding() {
        let dt = AvroDataType::from_codec(Codec::Duration);
        let mut dec = Decoder::try_new(&dt, true).unwrap();
        let row1 = [
            0x01, 0x00, 0x00, 0x00, // months=1
            0x02, 0x00, 0x00, 0x00, // days=2
            0x64, 0x00, 0x00, 0x00, // ms=100
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, // months=-1
            0x0A, 0x00, 0x00, 0x00, // days=10
            0x0F, 0x27, 0x00, 0x00, // ms=9999
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap();
        dec.decode(&mut cursor).unwrap();
        let arr = dec.flush(None).unwrap();
        let intervals = arr
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(intervals.len(), 2);
        let val0 = intervals.value(0);
        assert_eq!(val0.months, 1);
        assert_eq!(val0.days, 2);
        assert_eq!(val0.nanoseconds, 100_000_000);
        let val1 = intervals.value(1);
        assert_eq!(val1.months, -1);
        assert_eq!(val1.days, 10);
        assert_eq!(val1.nanoseconds, 9_999_000_000);
    }

    #[test]
    fn test_interval_decoding_with_nulls() {
        let dt = AvroDataType::from_codec(Codec::Duration);
        let child = Decoder::try_new(&dt, true).unwrap();
        let mut dec = Decoder::Nullable(
            UnionOrder::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(child),
        );
        let row1 = [
            0x02, 0x00, 0x00, 0x00, // months=2
            0x03, 0x00, 0x00, 0x00, // days=3
            0xF4, 0x01, 0x00, 0x00, // ms=500
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row1);
        data.extend_from_slice(&encode_avro_int(1));
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap(); // Row1
        dec.decode(&mut cursor).unwrap(); // Row2 (null)
        let arr = dec.flush(None).unwrap();
        let intervals = arr
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(intervals.len(), 2);
        assert!(intervals.is_valid(0));
        assert!(!intervals.is_valid(1));
        let val0 = intervals.value(0);
        assert_eq!(val0.months, 2);
        assert_eq!(val0.days, 3);
        assert_eq!(val0.nanoseconds, 500_000_000);
    }

    #[test]
    fn test_enum_decoding() {
        let symbols = Arc::new(["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()]);
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols, Arc::new([])));
        let mut decoder = Decoder::try_new(&enum_dt, true).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(2));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let dict_arr = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict_arr.len(), 3);
        let keys = dict_arr.keys();
        assert_eq!(keys.value(0), 1);
        assert_eq!(keys.value(1), 0);
        assert_eq!(keys.value(2), 2);
        let dict_values = dict_arr.values().as_string::<i32>();
        assert_eq!(dict_values.value(0), "RED");
        assert_eq!(dict_values.value(1), "GREEN");
        assert_eq!(dict_values.value(2), "BLUE");
    }

    #[test]
    fn test_enum_decoding_with_nulls() {
        let symbols = ["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(Arc::new(symbols), Arc::new([])));
        let inner_decoder = Decoder::try_new(&enum_dt, true).unwrap();
        let mut nullable_decoder = Decoder::Nullable(
            UnionOrder::NullSecond,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner_decoder),
        );
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_int(0));
        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap();
        nullable_decoder.decode(&mut cursor).unwrap();
        nullable_decoder.decode(&mut cursor).unwrap();
        let array = nullable_decoder.flush(None).unwrap();
        let dict_arr = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict_arr.len(), 3);
        assert!(dict_arr.is_valid(0));
        assert!(!dict_arr.is_valid(1));
        assert!(dict_arr.is_valid(2));
        let dict_values = dict_arr.values().as_string::<i32>();
        assert_eq!(dict_values.value(0), "RED");
        assert_eq!(dict_values.value(1), "GREEN");
        assert_eq!(dict_values.value(2), "BLUE");
    }

    #[test]
    fn test_map_decoding_one_entry() {
        let value_type = AvroDataType::from_codec(Codec::String);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type, true).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_bytes(b"hello"));
        data.extend_from_slice(&encode_avro_bytes(b"world"));
        data.extend_from_slice(&encode_avro_long(0));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);
        assert_eq!(map_arr.value_length(0), 1);
        let struct_arr = map_arr.value(0);
        assert_eq!(struct_arr.len(), 1);
        let keys = struct_arr.column(0).as_string::<i32>();
        let vals = struct_arr.column(1).as_string::<i32>();
        assert_eq!(keys.value(0), "hello");
        assert_eq!(vals.value(0), "world");
    }

    #[test]
    fn test_map_decoding_empty() {
        let value_type = AvroDataType::from_codec(Codec::String);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type, true).unwrap();
        let data = encode_avro_long(0);
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);
        assert_eq!(map_arr.value_length(0), 0);
    }

    #[test]
    fn test_decimal_decoding_fixed128() {
        let dt = AvroDataType::from_codec(Codec::Decimal(5, Some(2), Some(16)));
        let mut decoder = Decoder::try_new(&dt, true).unwrap();
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
    fn test_decimal_decoding_bytes_with_nulls() {
        let dt = AvroDataType::from_codec(Codec::Decimal(4, Some(1), None));
        let inner = Decoder::try_new(&dt, true).unwrap();
        let mut decoder = Decoder::Nullable(
            UnionOrder::NullSecond,
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
        decoder.decode(&mut cursor).unwrap(); // row1
        decoder.decode(&mut cursor).unwrap(); // row2
        decoder.decode(&mut cursor).unwrap(); // row3
        let arr = decoder.flush(None).unwrap();
        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 3);
        assert!(dec_arr.is_valid(0));
        assert!(!dec_arr.is_valid(1));
        assert!(dec_arr.is_valid(2));
        assert_eq!(dec_arr.value_as_string(0), "123.4");
        assert_eq!(dec_arr.value_as_string(2), "-123.4");
    }

    #[test]
    fn test_decimal_decoding_bytes_with_nulls_fixed_size() {
        let dt = AvroDataType::from_codec(Codec::Decimal(6, Some(2), Some(16)));
        let inner = Decoder::try_new(&dt, true).unwrap();
        let mut decoder = Decoder::Nullable(
            UnionOrder::NullSecond,
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
        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 3);
        assert!(dec_arr.is_valid(0));
        assert!(!dec_arr.is_valid(1));
        assert!(dec_arr.is_valid(2));
        assert_eq!(dec_arr.value_as_string(0), "1234.56");
        assert_eq!(dec_arr.value_as_string(2), "-1234.56");
    }

    #[test]
    fn test_list_decoding() {
        let item_dt = AvroDataType::from_codec(Codec::Int32);
        let list_dt = AvroDataType::from_codec(Codec::Array(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt, true).unwrap();
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
    fn test_list_decoding_with_negative_block_count() {
        let item_dt = AvroDataType::from_codec(Codec::Int32);
        let list_dt = AvroDataType::from_codec(Codec::Array(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt, true).unwrap();
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
}
