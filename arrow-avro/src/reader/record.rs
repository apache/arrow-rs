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
