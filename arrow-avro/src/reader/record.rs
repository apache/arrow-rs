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
use crate::reader::block::{Block, BlockDecoder};
use crate::reader::cursor::AvroCursor;
use crate::reader::header::Header;
use crate::schema::*;
use arrow_array::builder::{Decimal128Builder, Decimal256Builder, PrimitiveBuilder};
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::*;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, FieldRef, Fields, IntervalUnit, Schema as ArrowSchema,
    SchemaRef, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

/// The default capacity used for internal buffers
const DEFAULT_CAPACITY: usize = 1024;

/// A decoder that converts Avro-encoded data into an Arrow [`RecordBatch`].
pub struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
}

impl RecordDecoder {
    /// Create a new [`RecordDecoder`] from an [`AvroDataType`] expected to be a `Record`.
    pub fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        match Decoder::try_new(data_type)? {
            Decoder::Record(fields, encodings) => Ok(Self {
                schema: Arc::new(ArrowSchema::new(fields)),
                fields: encodings,
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

    /// Flush the accumulated data into a [`RecordBatch`], clearing internal state.
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let arrays = self
            .fields
            .iter_mut()
            .map(|x| x.flush(None))
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Decoder for Avro data of various shapes.
#[derive(Debug)]
enum Decoder {
    /// Avro `null`
    Null(usize),
    /// Avro `boolean`
    Boolean(BooleanBufferBuilder),
    /// Avro `int` => i32
    Int32(Vec<i32>),
    /// Avro `long` => i64
    Int64(Vec<i64>),
    /// Avro `float` => f32
    Float32(Vec<f32>),
    /// Avro `double` => f64
    Float64(Vec<f64>),
    /// Avro `date` => Date32
    Date32(Vec<i32>),
    /// Avro `time-millis` => Time32(Millisecond)
    TimeMillis(Vec<i32>),
    /// Avro `time-micros` => Time64(Microsecond)
    TimeMicros(Vec<i64>),
    /// Avro `timestamp-millis` (bool = UTC?)
    TimestampMillis(bool, Vec<i64>),
    /// Avro `timestamp-micros` (bool = UTC?)
    TimestampMicros(bool, Vec<i64>),
    /// Avro `bytes` => Arrow Binary
    Binary(OffsetBufferBuilder<i32>, Vec<u8>),
    /// Avro `string` => Arrow String
    String(OffsetBufferBuilder<i32>, Vec<u8>),
    /// Avro `fixed(n)` => Arrow `FixedSizeBinaryArray`
    Fixed(i32, Vec<u8>),
    /// Avro `interval` => Arrow `IntervalMonthDayNanoType` (12 bytes)
    Interval(Vec<IntervalMonthDayNano>),
    /// Avro `array<T>`
    List(FieldRef, OffsetBufferBuilder<i32>, Box<Decoder>),
    /// Avro `record`
    Record(Fields, Vec<Decoder>),
    /// Avro union that includes `null`
    Nullable(Nullability, NullBufferBuilder, Box<Decoder>),
    /// Avro `enum` => Dictionary(int32 -> string)
    Enum(Vec<String>, Vec<i32>),
    /// Avro `map<T>`
    Map(
        FieldRef,
        OffsetBufferBuilder<i32>,
        OffsetBufferBuilder<i32>,
        Vec<u8>,
        Box<Decoder>,
        usize,
    ),
    /// Avro decimal => Arrow decimal
    Decimal(usize, Option<usize>, Option<usize>, DecimalBuilder),
}

impl Decoder {
    /// Checks if the Decoder is nullable, i.e. wrapped in `Nullable`.
    fn is_nullable(&self) -> bool {
        matches!(self, Decoder::Nullable(_, _, _))
    }

    /// Create a `Decoder` from an [`AvroDataType`].
    fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        let decoder = match data_type.codec() {
            Codec::Null => Decoder::Null(0),
            Codec::Boolean => Decoder::Boolean(BooleanBufferBuilder::new(DEFAULT_CAPACITY)),
            Codec::Int32 => Decoder::Int32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Int64 => Decoder::Int64(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Float32 => Decoder::Float32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Float64 => Decoder::Float64(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Binary => Decoder::Binary(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            Codec::Utf8 => Decoder::String(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            Codec::Date32 => Decoder::Date32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMillis => Decoder::TimeMillis(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMicros => Decoder::TimeMicros(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimestampMillis(is_utc) => {
                Decoder::TimestampMillis(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::TimestampMicros(is_utc) => {
                Decoder::TimestampMicros(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::Fixed(n) => Decoder::Fixed(*n, Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Interval => Decoder::Interval(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::List(item) => {
                let item_decoder = Box::new(Self::try_new(item)?);
                Decoder::List(
                    Arc::new(item.field_with_name("item")),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    item_decoder,
                )
            }
            Codec::Struct(avro_fields) => {
                let mut arrow_fields = Vec::with_capacity(avro_fields.len());
                let mut decoders = Vec::with_capacity(avro_fields.len());
                for avro_field in avro_fields.iter() {
                    let d = Self::try_new(avro_field.data_type())?;
                    arrow_fields.push(avro_field.field());
                    decoders.push(d);
                }
                Decoder::Record(arrow_fields.into(), decoders)
            }
            Codec::Enum(symbols) => {
                Decoder::Enum(symbols.clone(), Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::Map(value_type) => {
                let map_field = Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                        Arc::new(value_type.field_with_name("value")),
                    ])),
                    false,
                ));
                Decoder::Map(
                    map_field,
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                    Box::new(Self::try_new(value_type)?),
                    0,
                )
            }
            Codec::Decimal(precision, scale, size) => {
                let builder = DecimalBuilder::new(*precision, *scale, *size)?;
                Decoder::Decimal(*precision, *scale, *size, builder)
            }
        };

        // Wrap in Nullable if needed
        match data_type.nullability() {
            Some(nb) => Ok(Decoder::Nullable(
                nb,
                NullBufferBuilder::new(DEFAULT_CAPACITY),
                Box::new(decoder),
            )),
            None => Ok(decoder),
        }
    }

    /// Append a null to this decoder.
    fn append_null(&mut self) {
        match self {
            Decoder::Null(n) => {
                *n += 1;
            }
            Decoder::Boolean(b) => {
                b.append(false);
            }
            Decoder::Int32(v) | Decoder::Date32(v) | Decoder::TimeMillis(v) => {
                v.push(0);
            }
            Decoder::Int64(v)
            | Decoder::TimeMicros(v)
            | Decoder::TimestampMillis(_, v)
            | Decoder::TimestampMicros(_, v) => {
                v.push(0);
            }
            Decoder::Float32(v) => {
                v.push(0.0);
            }
            Decoder::Float64(v) => {
                v.push(0.0);
            }
            Decoder::Binary(off, _) | Decoder::String(off, _) => {
                off.push_length(0);
            }
            Decoder::Fixed(fsize, buf) => {
                // For a null, push `fsize` zeroed bytes
                let n = *fsize as usize;
                buf.extend(std::iter::repeat(0u8).take(n));
            }
            Decoder::Interval(intervals) => {
                // null => store a 12-byte zero => months=0, days=0, nanos=0
                intervals.push(IntervalMonthDayNano {
                    months: 0,
                    days: 0,
                    nanoseconds: 0,
                });
            }
            Decoder::List(_, off, child) => {
                off.push_length(0);
                child.append_null();
            }
            Decoder::Record(_, children) => {
                for c in children.iter_mut() {
                    c.append_null();
                }
            }
            Decoder::Enum(_, indices) => {
                indices.push(0);
            }
            Decoder::Map(
                _,
                key_off,
                map_off,
                _,
                _,
                entry_count,
            ) => {
                key_off.push_length(0);
                map_off.push_length(*entry_count);
            }
            Decoder::Decimal(_, _, _, builder) => {
                let _ = builder.append_null();
            }
            Decoder::Nullable(_, _, _) => { /* The null bit is stored in the NullBufferBuilder */ }
        }
    }

    /// Decode a single row of data from `buf`.
    fn decode(&mut self, buf: &mut AvroCursor<'_>) -> Result<(), ArrowError> {
        match self {
            Decoder::Null(count) => {
                *count += 1;
            }
            Decoder::Boolean(values) => {
                values.append(buf.get_bool()?);
            }
            Decoder::Int32(values) => {
                values.push(buf.get_int()?);
            }
            Decoder::Date32(values) => {
                values.push(buf.get_int()?);
            }
            Decoder::Int64(values) => {
                values.push(buf.get_long()?);
            }
            Decoder::TimeMillis(values) => {
                values.push(buf.get_int()?);
            }
            Decoder::TimeMicros(values) => {
                values.push(buf.get_long()?);
            }
            Decoder::TimestampMillis(_, values) => {
                values.push(buf.get_long()?);
            }
            Decoder::TimestampMicros(_, values) => {
                values.push(buf.get_long()?);
            }
            Decoder::Float32(values) => {
                values.push(buf.get_float()?);
            }
            Decoder::Float64(values) => {
                values.push(buf.get_double()?);
            }
            Decoder::Binary(off, data) | Decoder::String(off, data) => {
                let bytes = buf.get_bytes()?;
                off.push_length(bytes.len());
                data.extend_from_slice(bytes);
            }
            Decoder::Fixed(fsize, accum) => {
                let raw = buf.get_fixed(*fsize as usize)?;
                accum.extend_from_slice(raw);
            }
            Decoder::Interval(intervals) => {
                let raw = buf.get_fixed(12)?;
                let months = i32::from_le_bytes(raw[0..4].try_into().unwrap());
                let days = i32::from_le_bytes(raw[4..8].try_into().unwrap());
                let millis = i32::from_le_bytes(raw[8..12].try_into().unwrap());
                let nanos = millis as i64 * 1_000_000;
                let val = IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds: nanos,
                };
                intervals.push(val);
            }
            Decoder::List(_, off, child) => {
                let total_items = read_array_blocks(buf, |b| child.decode(b))?;
                off.push_length(total_items);
            }
            Decoder::Record(_, children) => {
                for c in children.iter_mut() {
                    c.decode(buf)?;
                }
            }
            Decoder::Nullable(_, nulls, child) => {
                let branch_index = buf.get_int()?;
                match branch_index {
                    0 => {
                        nulls.append(true);
                        child.decode(buf)?;
                    }
                    1 => {
                        nulls.append(false);
                        child.append_null();
                    }
                    other => {
                        return Err(ArrowError::ParseError(format!(
                            "Unsupported union branch index {other} for Nullable"
                        )));
                    }
                }
            }
            Decoder::Enum(_, indices) => {
                let idx = buf.get_int()?;
                indices.push(idx);
            }
            Decoder::Map(_, key_off, map_off, key_data, val_decoder, entry_count) => {
                let newly_added = read_map_blocks(buf, |b| {
                    let kb = b.get_bytes()?;
                    key_off.push_length(kb.len());
                    key_data.extend_from_slice(kb);
                    val_decoder.decode(b)
                })?;
                *entry_count += newly_added;
                map_off.push_length(*entry_count);
            }
            Decoder::Decimal(_, _, size, builder) => {
                if let Some(sz) = *size {
                    let raw = buf.get_fixed(sz)?;
                    builder.append_bytes(raw)?;
                } else {
                    let variable = buf.get_bytes()?;
                    builder.append_bytes(variable)?;
                }
            }
        }
        Ok(())
    }

    /// Flush buffered data into an [`ArrayRef`], optionally applying `nulls`.
    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        match self {
            // For a nullable wrapper => flush the child with the built null buffer
            Decoder::Nullable(_, nb, child) => {
                let mask = nb.finish();
                child.flush(mask)
            }
            // Null => produce NullArray
            Decoder::Null(len) => {
                let count = std::mem::replace(len, 0);
                Ok(Arc::new(NullArray::new(count)))
            }
            // boolean => flush to BooleanArray
            Decoder::Boolean(b) => {
                let bits = b.finish();
                Ok(Arc::new(BooleanArray::new(bits, nulls)))
            }
            // int32 => flush to Int32Array
            Decoder::Int32(vals) => {
                let arr = flush_primitive::<Int32Type>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // date32 => flush to Date32Array
            Decoder::Date32(vals) => {
                let arr = flush_primitive::<Date32Type>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // int64 => flush to Int64Array
            Decoder::Int64(vals) => {
                let arr = flush_primitive::<Int64Type>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // time-millis => Time32Millisecond
            Decoder::TimeMillis(vals) => {
                let arr = flush_primitive::<Time32MillisecondType>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // time-micros => Time64Microsecond
            Decoder::TimeMicros(vals) => {
                let arr = flush_primitive::<Time64MicrosecondType>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // timestamp-millis => TimestampMillisecond
            Decoder::TimestampMillis(is_utc, vals) => {
                let arr = flush_primitive::<TimestampMillisecondType>(vals, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into()));
                Ok(Arc::new(arr))
            }
            // timestamp-micros => TimestampMicrosecond
            Decoder::TimestampMicros(is_utc, vals) => {
                let arr = flush_primitive::<TimestampMicrosecondType>(vals, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into()));
                Ok(Arc::new(arr))
            }
            // float32 => flush to Float32Array
            Decoder::Float32(vals) => {
                let arr = flush_primitive::<Float32Type>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // float64 => flush to Float64Array
            Decoder::Float64(vals) => {
                let arr = flush_primitive::<Float64Type>(vals, nulls);
                Ok(Arc::new(arr))
            }
            // Avro bytes => BinaryArray
            Decoder::Binary(off, data) => {
                let offsets = flush_offsets(off);
                let values = flush_values(data).into();
                Ok(Arc::new(BinaryArray::new(offsets, values, nulls)))
            }
            // Avro string => StringArray
            Decoder::String(off, data) => {
                let offsets = flush_offsets(off);
                let values = flush_values(data).into();
                Ok(Arc::new(StringArray::new(offsets, values, nulls)))
            }
            // Avro fixed => FixedSizeBinaryArray
            Decoder::Fixed(fsize, raw) => {
                let size = *fsize;
                let buf: Buffer = flush_values(raw).into();
                let total_len = buf.len() / (size as usize);
                let array = FixedSizeBinaryArray::try_new(size, buf, nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Ok(Arc::new(array))
            }
            // Avro interval => IntervalMonthDayNanoType
            Decoder::Interval(vals) => {
                let data_len = vals.len();
                let mut builder = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(data_len);
                for v in vals.drain(..) {
                    builder.append_value(v);
                }
                let arr = builder.finish().with_data_type(DataType::Interval(IntervalUnit::MonthDayNano));
                if let Some(nb) = nulls {
                    // "merge" the newly built array with the nulls
                    let arr_data = arr.into_data().into_builder().nulls(Some(nb));
                    let arr_data = unsafe { arr_data.build_unchecked() };
                    Ok(Arc::new(PrimitiveArray::<IntervalMonthDayNanoType>::from(arr_data)))
                } else {
                    Ok(Arc::new(arr))
                }
            }
            // Avro array => ListArray
            Decoder::List(field, off, item_dec) => {
                let child_arr = item_dec.flush(None)?;
                let offsets = flush_offsets(off);
                let arr = ListArray::new(field.clone(), offsets, child_arr, nulls);
                Ok(Arc::new(arr))
            }
            // Avro record => StructArray
            Decoder::Record(fields, children) => {
                let mut arrays = Vec::with_capacity(children.len());
                for c in children.iter_mut() {
                    let a = c.flush(None)?;
                    arrays.push(a);
                }
                Ok(Arc::new(StructArray::new(fields.clone(), arrays, nulls)))
            }
            // Avro enum => DictionaryArray<int32 -> utf8>
            Decoder::Enum(symbols, indices) => {
                let dict_values = StringArray::from_iter_values(symbols.iter());
                let idxs: Int32Array = match nulls {
                    Some(b) => {
                        let buff = Buffer::from_slice_ref(&indices);
                        PrimitiveArray::<Int32Type>::try_new(
                            arrow_buffer::ScalarBuffer::from(buff),
                            Some(b),
                        )?
                    }
                    None => Int32Array::from_iter_values(indices.iter().cloned()),
                };
                let dict = DictionaryArray::<Int32Type>::try_new(idxs, Arc::new(dict_values))?;
                indices.clear(); // reset
                Ok(Arc::new(dict))
            }
            // Avro map => MapArray
            Decoder::Map(field, key_off, map_off, key_data, val_dec, entry_count) => {
                let moff = flush_offsets(map_off);
                let koff = flush_offsets(key_off);
                let kd = flush_values(key_data).into();
                let val_arr = val_dec.flush(None)?;
                let is_nullable = matches!(**val_dec, Decoder::Nullable(_, _, _));
                let key_arr = StringArray::new(koff, kd, None);
                let struct_fields = vec![
                    Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                    Arc::new(ArrowField::new(
                        "value",
                        val_arr.data_type().clone(),
                        is_nullable,
                    )),
                ];
                let entries = StructArray::new(
                    Fields::from(struct_fields),
                    vec![Arc::new(key_arr), val_arr],
                    None,
                );
                let map_arr = MapArray::new(field.clone(), moff, entries, nulls, false);
                *entry_count = 0;
                Ok(Arc::new(map_arr))
            }
            // Avro decimal => Arrow decimal
            Decoder::Decimal(prec, sc, sz, builder) => {
                let precision = *prec;
                let scale = sc.unwrap_or(0);
                let new_builder = DecimalBuilder::new(precision, *sc, *sz)?;
                let old_builder = std::mem::replace(builder, new_builder);
                let arr = old_builder.finish(nulls, precision, scale)?;
                Ok(arr)
            }
        }
    }
}

/// Decode an Avro array in blocks until a 0 block_count signals end.
fn read_array_blocks(
    buf: &mut AvroCursor,
    mut decode_item: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    let mut total_items = 0usize;
    loop {
        let block_count = buf.get_long()?;
        if block_count == 0 {
            break;
        } else if block_count < 0 {
            let item_count = (-block_count) as usize;
            let _block_size = buf.get_long()?; // “block size” is read but not used
            for _ in 0..item_count {
                decode_item(buf)?;
            }
            total_items += item_count;
        } else {
            let item_count = block_count as usize;
            for _ in 0..item_count {
                decode_item(buf)?;
            }
            total_items += item_count;
        }
    }
    Ok(total_items)
}

/// Decode an Avro map in blocks until 0 block_count => end.
fn read_map_blocks(
    buf: &mut AvroCursor,
    mut decode_entry: impl FnMut(&mut AvroCursor) -> Result<(), ArrowError>,
) -> Result<usize, ArrowError> {
    let block_count = buf.get_long()?;
    if block_count <= 0 {
        Ok(0)
    } else {
        let n = block_count as usize;
        for _ in 0..n {
            decode_entry(buf)?;
        }
        Ok(n)
    }
}

/// Flush a [`Vec<T>`] of primitive values to a [`PrimitiveArray`], applying optional `nulls`.
#[inline]
fn flush_primitive<T: ArrowPrimitiveType>(
    values: &mut Vec<T::Native>,
    nulls: Option<NullBuffer>,
) -> PrimitiveArray<T> {
    PrimitiveArray::new(flush_values(values).into(), nulls)
}

/// Flush an [`OffsetBufferBuilder`].
#[inline]
fn flush_offsets(offsets: &mut OffsetBufferBuilder<i32>) -> OffsetBuffer<i32> {
    std::mem::replace(offsets, OffsetBufferBuilder::new(DEFAULT_CAPACITY)).finish()
}

/// Take ownership of `values`.
#[inline]
fn flush_values<T>(values: &mut Vec<T>) -> Vec<T> {
    std::mem::replace(values, Vec::with_capacity(DEFAULT_CAPACITY))
}

/// A builder for Avro decimal, either 128-bit or 256-bit.
#[derive(Debug)]
enum DecimalBuilder {
    Decimal128(Decimal128Builder),
    Decimal256(Decimal256Builder),
}

impl DecimalBuilder {
    /// Create a new DecimalBuilder given precision, scale, and optional byte-size (`fixed`).
    fn new(
        precision: usize,
        scale: Option<usize>,
        size: Option<usize>,
    ) -> Result<Self, ArrowError> {
        match size {
            Some(s) if s > 16 && s <= 32 => Ok(Self::Decimal256(
                Decimal256Builder::new()
                    .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
            )),
            Some(s) if s <= 16 => Ok(Self::Decimal128(
                Decimal128Builder::new()
                    .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
            )),
            None => {
                // infer from precision
                if precision <= DECIMAL128_MAX_PRECISION as usize {
                    Ok(Self::Decimal128(
                        Decimal128Builder::new()
                            .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
                    ))
                } else if precision <= DECIMAL256_MAX_PRECISION as usize {
                    Ok(Self::Decimal256(
                        Decimal256Builder::new()
                            .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
                    ))
                } else {
                    Err(ArrowError::ParseError(format!(
                        "Decimal precision {} exceeds maximum supported",
                        precision
                    )))
                }
            }
            _ => Err(ArrowError::ParseError(format!(
                "Unsupported decimal size: {:?}",
                size
            ))),
        }
    }

    /// Append sign-extended bytes to this decimal builder
    fn append_bytes(&mut self, raw: &[u8]) -> Result<(), ArrowError> {
        match self {
            Self::Decimal128(b) => {
                let padded = sign_extend_to_16(raw)?;
                let val = i128::from_be_bytes(padded);
                b.append_value(val);
            }
            Self::Decimal256(b) => {
                let padded = sign_extend_to_32(raw)?;
                let val = i256::from_be_bytes(padded);
                b.append_value(val);
            }
        }
        Ok(())
    }

    /// Append a null decimal value (0)
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

    /// Finish building the decimal array, returning an [`ArrayRef`].
    fn finish(
        self,
        nulls: Option<NullBuffer>,
        precision: usize,
        scale: usize,
    ) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Decimal128(mut b) => {
                let arr = b.finish();
                let vals = arr.values().clone();
                let dec = Decimal128Array::new(vals, nulls)
                    .with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(dec))
            }
            Self::Decimal256(mut b) => {
                let arr = b.finish();
                let vals = arr.values().clone();
                let dec = Decimal256Array::new(vals, nulls)
                    .with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(dec))
            }
        }
    }
}

/// Sign-extend `raw` to 16 bytes.
fn sign_extend_to_16(raw: &[u8]) -> Result<[u8; 16], ArrowError> {
    let extended = sign_extend(raw, 16);
    if extended.len() != 16 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend to 16 bytes, got {} bytes",
            extended.len()
        )));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&extended);
    Ok(arr)
}

/// Sign-extend `raw` to 32 bytes.
fn sign_extend_to_32(raw: &[u8]) -> Result<[u8; 32], ArrowError> {
    let extended = sign_extend(raw, 32);
    if extended.len() != 32 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend to 32 bytes, got {} bytes",
            extended.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&extended);
    Ok(arr)
}

/// Sign-extend the first byte to produce `target_len` bytes total.
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

/// Convenience helper to build a field with `name`, `DataType` and `nullable`.
fn field_with_type(name: &str, dt: DataType, nullable: bool) -> FieldRef {
    Arc::new(ArrowField::new(name, dt, nullable))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        cast::AsArray, Array, ArrayRef, Decimal128Array, Decimal256Array, DictionaryArray,
        FixedSizeBinaryArray, Int32Array, IntervalMonthDayNanoArray, ListArray, MapArray,
        StringArray, StructArray,
    };
    use arrow_buffer::Buffer;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
    use serde_json::json;
    use std::iter;

    // ---------------
    // Zig-Zag Helpers
    // ---------------
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

    // -----------------
    // Test Fixed
    // -----------------
    #[test]
    fn test_fixed_decoding() {
        // `fixed(4)` => Arrow FixedSizeBinary(4)
        let dt = AvroDataType::from_codec(Codec::Fixed(4));
        let mut dec = Decoder::try_new(&dt).unwrap();
        // 2 rows, each row => 4 bytes
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
        // Avro union => [ fixed(2), null]
        let dt = AvroDataType::from_codec(Codec::Fixed(2));
        let child = Decoder::try_new(&dt).unwrap();
        let mut dec = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(child),
        );
        // Decode 3 rows: row1 => branch=0 => [0x00], then 2 bytes
        // row2 => branch=1 => null => [0x02]
        // row3 => branch=0 => 2 bytes
        let row1 = [0x11, 0x22];
        let row3 = [0x55, 0x66];
        let mut data = Vec::new();
        // row1 => union=0 => child => 2 bytes
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row1);
        // row2 => union=1 => null
        data.extend_from_slice(&encode_avro_int(1));
        // row3 => union=0 => child => 2 bytes
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row3);
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap(); // row1
        dec.decode(&mut cursor).unwrap(); // row2 => null
        dec.decode(&mut cursor).unwrap(); // row3
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

    // -----------------
    // Test Interval
    // -----------------
    #[test]
    fn test_interval_decoding() {
        // Avro interval => 12 bytes => [ months i32, days i32, ms i32 ]
        // decode 2 rows => row1 => months=1, days=2, ms=100 => row2 => months=-1, days=10, ms=9999
        let dt = AvroDataType::from_codec(Codec::Interval);
        let mut dec = Decoder::try_new(&dt).unwrap();
        // row1 => months=1 => 01,00,00,00, days=2 => 02,00,00,00, ms=100 => 64,00,00,00
        // row2 => months=-1 => 0xFF,0xFF,0xFF,0xFF, days=10 => 0x0A,0x00,0x00,0x00, ms=9999 => 0x0F,0x27,0x00,0x00
        let row1 = [0x01, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x64, 0x00, 0x00, 0x00];
        let row2 = [0xFF, 0xFF, 0xFF, 0xFF,
            0x0A, 0x00, 0x00, 0x00,
            0x0F, 0x27, 0x00, 0x00];
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
        // row0 => months=1, days=2, ms=100 => nanos=100_000_000
        // row1 => months=-1, days=10, ms=9999 => nanos=9999_000_000
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
        // Avro union => [ interval, null]
        let dt = AvroDataType::from_codec(Codec::Interval);
        let child = Decoder::try_new(&dt).unwrap();
        let mut dec = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(child),
        );
        // We'll decode 2 rows: row1 => interval => months=2, days=3, ms=500 => row2 => null
        // row1 => union=0 => child => 12 bytes
        // row2 => union=1 => null => no data
        let row1 = [0x02, 0x00, 0x00, 0x00,   // months=2
            0x03, 0x00, 0x00, 0x00,   // days=3
            0xF4, 0x01, 0x00, 0x00];  // ms=500 => nanos=500_000_000
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(0)); // union=0 => child
        data.extend_from_slice(&row1);
        data.extend_from_slice(&encode_avro_int(1)); // union=1 => null
        let mut cursor = AvroCursor::new(&data);
        dec.decode(&mut cursor).unwrap(); // row1
        dec.decode(&mut cursor).unwrap(); // row2 => null
        let arr = dec.flush(None).unwrap();
        let intervals = arr.as_any().downcast_ref::<IntervalMonthDayNanoArray>().unwrap();
        assert_eq!(intervals.len(), 2);
        assert!(intervals.is_valid(0));
        assert!(!intervals.is_valid(1));
        let val0 = intervals.value(0);
        assert_eq!(val0.months, 2);
        assert_eq!(val0.days, 3);
        assert_eq!(val0.nanoseconds, 500_000_000);
    }

    // -------------------
    // Tests for Enum
    // -------------------
    #[test]
    fn test_enum_decoding() {
        let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols.clone()));
        let mut decoder = Decoder::try_new(&enum_dt).unwrap();
        // Encode the indices [1, 0, 2] => zigzag => 1->2, 0->0, 2->4
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(1)); // => [2]
        data.extend_from_slice(&encode_avro_int(0)); // => [0]
        data.extend_from_slice(&encode_avro_int(2)); // => [4]
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap(); // => GREEN
        decoder.decode(&mut cursor).unwrap(); // => RED
        decoder.decode(&mut cursor).unwrap(); // => BLUE
        let array = decoder.flush(None).unwrap();
        let dict_arr = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
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
        // Union => [Enum(...), null]
        // "child" => branch_index=0 => [0x00], "null" => 1 => [0x02]
        let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols.clone()));
        let mut inner_decoder = Decoder::try_new(&enum_dt).unwrap();
        let mut nullable_decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner_decoder),
        );
        // Indices: [1, null, 2] => in Avro union
        let mut data = Vec::new();
        // Row1 => union branch=0 => child => [0x00]
        data.extend_from_slice(&encode_avro_int(0));
        // Then child's enum index=1 => [0x02]
        data.extend_from_slice(&encode_avro_int(1));
        // Row2 => union branch=1 => null => [0x02]
        data.extend_from_slice(&encode_avro_int(1));
        // Row3 => union branch=0 => child => [0x00]
        data.extend_from_slice(&encode_avro_int(0));
        // Then child's enum index=2 => [0x04]
        data.extend_from_slice(&encode_avro_int(2));
        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap(); // => GREEN
        nullable_decoder.decode(&mut cursor).unwrap(); // => null
        nullable_decoder.decode(&mut cursor).unwrap(); // => BLUE
        let array = nullable_decoder.flush(None).unwrap();
        let dict_arr = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(dict_arr.len(), 3);
        // [GREEN, null, BLUE]
        assert!(dict_arr.is_valid(0));
        assert!(!dict_arr.is_valid(1));
        assert!(dict_arr.is_valid(2));
        let keys = dict_arr.keys();
        // keys.value(0) => 1 => GREEN
        // keys.value(2) => 2 => BLUE
        let dict_values = dict_arr.values().as_string::<i32>();
        assert_eq!(dict_values.value(0), "RED");
        assert_eq!(dict_values.value(1), "GREEN");
        assert_eq!(dict_values.value(2), "BLUE");
    }

    // -------------------
    // Tests for Map
    // -------------------
    #[test]
    fn test_map_decoding_one_entry() {
        let value_type = AvroDataType::from_codec(Codec::Utf8);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Encode a single map with one entry: {"hello": "world"}
        let mut data = Vec::new();
        // block_count=1 => zigzag => [0x02]
        data.extend_from_slice(&encode_avro_long(1));
        data.extend_from_slice(&encode_avro_bytes(b"hello")); // key
        data.extend_from_slice(&encode_avro_bytes(b"world")); // value
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
        // block_count=0 => empty map
        let value_type = AvroDataType::from_codec(Codec::Utf8);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Encode an empty map => block_count=0 => [0x00]
        let data = encode_avro_long(0);
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);
        assert_eq!(map_arr.value_length(0), 0);
    }

    // -------------------
    // Tests for Decimal
    // -------------------
    #[test]
    fn test_decimal_decoding_fixed128() {
        let dt = AvroDataType::from_codec(Codec::Decimal(5, Some(2), Some(16)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        // Row1 => 123.45 => unscaled=12345 => i128 0x000...3039
        // Row2 => -1.23  => unscaled=-123  => i128 0xFFFF...FF85
        let row1 = [
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x30, 0x39,
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0x85,
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
        // Avro union => [ Decimal(4,1), null ]
        // child => index=0 => [0x00], null => index=1 => [0x02]
        let dt = AvroDataType::from_codec(Codec::Decimal(4, Some(1), None));
        let mut inner = Decoder::try_new(&dt).unwrap();
        let mut decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner),
        );
        // Decode three rows: [123.4, null, -123.4]
        let mut data = Vec::new();
        // Row1 => child => [0x00], then decimal => e.g. 0x04D2 => 1234 => "123.4"
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_bytes(&[0x04, 0xD2]));
        // Row2 => null => [0x02]
        data.extend_from_slice(&encode_avro_int(1));
        // Row3 => child => [0x00], then decimal => 0xFB2E => -1234 => "-123.4"
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&encode_avro_bytes(&[0xFB, 0x2E]));
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 3);
        assert_eq!(dec_arr.is_valid(0), true);
        assert_eq!(dec_arr.is_valid(1), false);
        assert_eq!(dec_arr.is_valid(2), true);
        assert_eq!(dec_arr.value_as_string(0), "123.4");
        assert_eq!(dec_arr.value_as_string(2), "-123.4");
    }

    #[test]
    fn test_decimal_decoding_bytes_with_nulls_fixed_size() {
        // Avro union => [Decimal(6,2,16), null]
        let dt = AvroDataType::from_codec(Codec::Decimal(6, Some(2), Some(16)));
        let mut inner = Decoder::try_new(&dt).unwrap();
        let mut decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(inner),
        );
        // Decode [1234.56, null, -1234.56]
        let row1 = [
            0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00, 0x00,0x01,0xE2,0x40
        ];
        let row3 = [
            0xFF,0xFF,0xFF,0xFF, 0xFF,0xFF,0xFF,0xFF,
            0xFF,0xFF,0xFF,0xFF, 0xFF,0xFE,0x1D,0xC0
        ];
        let mut data = Vec::new();
        // Row1 => child => [0x00]
        data.extend_from_slice(&encode_avro_int(0));
        data.extend_from_slice(&row1);
        // Row2 => null => [0x02]
        data.extend_from_slice(&encode_avro_int(1));
        // Row3 => child => [0x00]
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

    // -------------------
    // Tests for List
    // -------------------
    #[test]
    fn test_list_decoding() {
        // Avro array => block1(count=2), item1, item2, block2(count=0 => end)
        //
        // 1. Create 2 rows:
        // Row1 => [10, 20]
        // Row2 => [ ]
        //
        // 2. flush => should yield 2-element array => first row has 2 items, second row has 0 items
        let item_dt = AvroDataType::from_codec(Codec::Int32);
        let list_dt = AvroDataType::from_codec(Codec::List(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt).unwrap();
        // Row1 => block_count=2 => item=10 => item=20 => block_count=0 => end
        //  - 2 => zigzag => [0x04]
        //  - item=10 => zigzag => [0x14]
        //  - item=20 => zigzag => [0x28]
        //  - 0 => [0x00]
        let mut row1 = Vec::new();
        row1.extend_from_slice(&encode_avro_long(2)); // block_count=2
        row1.extend_from_slice(&encode_avro_int(10)); // item=10
        row1.extend_from_slice(&encode_avro_int(20)); // item=20
        row1.extend_from_slice(&encode_avro_long(0)); // end of array
        // Row2 => block_count=0 => empty array
        let mut row2 = Vec::new();
        row2.extend_from_slice(&encode_avro_long(0));
        let mut cursor = AvroCursor::new(&row1);
        decoder.decode(&mut cursor).unwrap();
        let mut cursor2 = AvroCursor::new(&row2);
        decoder.decode(&mut cursor2).unwrap();
        let array = decoder.flush(None).unwrap();
        let list_arr = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 2);
        // row0 => 2 items => [10, 20]
        // row1 => 0 items
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
        // Start with single row => [1, 2, 3]
        // We'll store them in a single negative block => block_count=-3 => #items=3
        // Then read block_size => let's pretend it's 9 bytes, etc. Then the items.
        // Then a block_count=0 => done
        let item_dt = AvroDataType::from_codec(Codec::Int32);
        let list_dt = AvroDataType::from_codec(Codec::List(Arc::new(item_dt)));
        let mut decoder = Decoder::try_new(&list_dt).unwrap();
        // block_count=-3 => zigzag => (-3 << 1) ^ (-3 >> 63)
        //   => -6 ^ -1 => ...
        // Encode directly with `encode_avro_long(-3)`.
        let mut data = encode_avro_long(-3);
        // Next => block_size => let's pretend 12 => encode_avro_long(12)
        data.extend_from_slice(&encode_avro_long(12));
        // Then 3 items => [1, 2, 3]
        data.extend_from_slice(&encode_avro_int(1));
        data.extend_from_slice(&encode_avro_int(2));
        data.extend_from_slice(&encode_avro_int(3));
        // Then block_count=0 => done
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
