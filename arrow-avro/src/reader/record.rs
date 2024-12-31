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
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::*;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef, Fields, Schema as ArrowSchema, SchemaRef, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};
use std::collections::HashMap;
use std::io::Read;
use std::ptr::null;
use std::sync::Arc;
use arrow_array::builder::{Decimal128Builder, Decimal256Builder};

/// Decodes avro encoded data into [`RecordBatch`]
pub struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
}

impl RecordDecoder {
    pub fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        match Decoder::try_new(data_type)? {
            Decoder::Record(fields, encodings) => Ok(Self {
                schema: Arc::new(ArrowSchema::new(fields)),
                fields: encodings,
            }),
            encoding => Err(ArrowError::ParseError(format!(
                "Expected record got {encoding:?}"
            ))),
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Decode `count` records from `buf`
    pub fn decode(&mut self, buf: &[u8], count: usize) -> Result<usize, ArrowError> {
        let mut cursor = AvroCursor::new(buf);
        for _ in 0..count {
            for field in &mut self.fields {
                field.decode(&mut cursor)?;
            }
        }
        Ok(cursor.position())
    }

    /// Flush the decoded records into a [`RecordBatch`]
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let arrays = self
            .fields
            .iter_mut()
            .map(|x| x.flush(None))
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Enum representing different decoders for various data types.
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
    Binary(OffsetBufferBuilder<i32>, Vec<u8>),
    String(OffsetBufferBuilder<i32>, Vec<u8>),
    List(FieldRef, OffsetBufferBuilder<i32>, Box<Decoder>),
    Record(Fields, Vec<Decoder>),
    Nullable(Nullability, NullBufferBuilder, Box<Decoder>),
    Enum(Vec<String>, Vec<i32>),
    Map(
        FieldRef,
        OffsetBufferBuilder<i32>, // key_offsets
        OffsetBufferBuilder<i32>, // map_offsets
        Vec<u8>,                  // key_data
        Box<Decoder>,             // values_decoder_inner
        usize,                    // current_entry_count
    ),
    Decimal(usize, Option<usize>, Option<usize>, DecimalBuilder),
}

impl Decoder {
    /// Checks if the Decoder is nullable.
    fn is_nullable(&self) -> bool {
        matches!(self, Decoder::Nullable(_, _, _))
    }

    /// Creates a new `Decoder` based on the provided `AvroDataType`.
    fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        let nyi = |s: &str| Err(ArrowError::NotYetImplemented(s.to_string()));

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
            Codec::Fixed(_) => return nyi("decoding fixed"),
            Codec::Interval => return nyi("decoding interval"),
            Codec::List(item) => {
                let decoder = Box::new(Self::try_new(item)?);
                Decoder::List(
                    Arc::new(item.field_with_name("item")),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    decoder,
                )
            }
            Codec::Struct(fields) => {
                let mut arrow_fields = Vec::with_capacity(fields.len());
                let mut encodings = Vec::with_capacity(fields.len());
                for avro_field in fields.iter() {
                    let encoding = Self::try_new(avro_field.data_type())?;
                    arrow_fields.push(avro_field.field());
                    encodings.push(encoding);
                }
                Decoder::Record(arrow_fields.into(), encodings)
            }
            Codec::Enum(symbols) => Decoder::Enum(symbols.clone(), Vec::with_capacity(DEFAULT_CAPACITY)),
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
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY), // key_offsets
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY), // map_offsets
                    Vec::with_capacity(DEFAULT_CAPACITY),        // key_data
                    Box::new(Self::try_new(value_type)?),      // values_decoder_inner
                    0,                                           // current_entry_count
                )
            }
            Codec::Decimal(precision, scale, size) => {
                let builder = DecimalBuilder::new(*precision, *scale, *size)?;
                Decoder::Decimal(*precision, *scale, *size, builder)
            }
        };

        // Wrap the decoder in Nullable if necessary
        match data_type.nullability() {
            Some(nullability) => Ok(Decoder::Nullable(
                nullability,
                NullBufferBuilder::new(DEFAULT_CAPACITY),
                Box::new(decoder),
            )),
            None => Ok(decoder),
        }
    }

    /// Appends a null value to the decoder.
    fn append_null(&mut self) {
        match self {
            Decoder::Null(count) => *count += 1,
            Decoder::Boolean(b) => b.append(false),
            Decoder::Int32(v) | Decoder::Date32(v) | Decoder::TimeMillis(v) => v.push(0),
            Decoder::Int64(v)
            | Decoder::TimeMicros(v)
            | Decoder::TimestampMillis(_, v)
            | Decoder::TimestampMicros(_, v) => v.push(0),
            Decoder::Float32(v) => v.push(0.0),
            Decoder::Float64(v) => v.push(0.0),
            Decoder::Binary(offsets, _) | Decoder::String(offsets, _) => {
                offsets.push_length(0);
            }
            Decoder::List(_, offsets, e) => {
                offsets.push_length(0);
                e.append_null();
            }
            Decoder::Record(_, encodings) => {
                for encoding in encodings.iter_mut() {
                    encoding.append_null();
                }
            }
            Decoder::Enum(_, indices) => {
                // Append a placeholder index for null entries
                indices.push(0);
            }
            Decoder::Map(
                _,
                key_offsets,
                map_offsets_builder,
                key_data,
                values_decoder_inner,
                current_entry_count,
            ) => {
                key_offsets.push_length(0);
                map_offsets_builder.push_length(*current_entry_count);
            }
            Decoder::Decimal(_, _, _, builder) => {
                builder.append_null();
            }
            Decoder::Nullable(_, _, _) => { /* Nulls are handled by the Nullable variant */ }
        }
    }

    /// Decodes a single record from the provided buffer `buf`.
    fn decode(&mut self, buf: &mut AvroCursor<'_>) -> Result<(), ArrowError> {
        match self {
            Decoder::Null(x) => *x += 1,
            Decoder::Boolean(values) => values.append(buf.get_bool()?),
            Decoder::Int32(values) => values.push(buf.get_int()?),
            Decoder::Date32(values) => values.push(buf.get_int()?),
            Decoder::Int64(values) => values.push(buf.get_long()?),
            Decoder::TimeMillis(values) => values.push(buf.get_int()?),
            Decoder::TimeMicros(values) => values.push(buf.get_long()?),
            Decoder::TimestampMillis(is_utc, values) => {
                values.push(buf.get_long()?);
            }
            Decoder::TimestampMicros(is_utc, values) => {
                values.push(buf.get_long()?);
            }
            Decoder::Float32(values) => values.push(buf.get_float()?),
            Decoder::Float64(values) => values.push(buf.get_double()?),
            Decoder::Binary(offsets, values) | Decoder::String(offsets, values) => {
                let data = buf.get_bytes()?;
                offsets.push_length(data.len());
                values.extend_from_slice(data);
            }
            Decoder::List(_, _, _) => {
                return Err(ArrowError::NotYetImplemented(
                    "Decoding ListArray".to_string(),
                ));
            }
            Decoder::Record(fields, encodings) => {
                for encoding in encodings.iter_mut() {
                    encoding.decode(buf)?;
                }
            }
            Decoder::Nullable(_, nulls, e) => {
                let is_valid = buf.get_bool()?;
                nulls.append(is_valid);
                match is_valid {
                    true => e.decode(buf)?,
                    false => e.append_null(),
                }
            }
            Decoder::Enum(symbols, indices) => {
                // Enums are encoded as zero-based indices using zigzag encoding
                let index = buf.get_int()?;
                indices.push(index);
            }
            Decoder::Map(
                field,
                key_offsets,
                map_offsets_builder,
                key_data,
                values_decoder_inner,
                current_entry_count,
            ) => {
                let block_count = buf.get_long()?;
                if block_count <= 0 {
                    // Push the current_entry_count without changes
                    map_offsets_builder.push_length(*current_entry_count);
                } else {
                    let n = block_count as usize;
                    for _ in 0..n {
                        let key_bytes = buf.get_bytes()?;
                        key_offsets.push_length(key_bytes.len());
                        key_data.extend_from_slice(key_bytes);
                        values_decoder_inner.decode(buf)?;
                    }
                    // Update the current_entry_count and push to map_offsets_builder
                    *current_entry_count += n;
                    map_offsets_builder.push_length(*current_entry_count);
                }
            }
            Decoder::Decimal(_precision, _scale, _size, builder) => {
                if let Some(size) = _size {
                    // Fixed-size decimal
                    let raw = buf.get_fixed(*size)?;
                    builder.append_bytes(raw)?;
                } else {
                    // Variable-size decimal
                    let bytes = buf.get_bytes()?;
                    builder.append_bytes(bytes)?;
                }
            }
        }
        Ok(())
    }

    /// Flushes decoded records to an [`ArrayRef`].
    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        match self {
            Decoder::Nullable(_, n, e) => e.flush(n.finish()),
            Decoder::Null(size) => Ok(Arc::new(NullArray::new(std::mem::replace(size, 0)))),
            Decoder::Boolean(b) => Ok(Arc::new(BooleanArray::new(b.finish(), nulls))),
            Decoder::Int32(values) => Ok(Arc::new(flush_primitive::<Int32Type>(values, nulls))),
            Decoder::Date32(values) => Ok(Arc::new(flush_primitive::<Date32Type>(values, nulls))),
            Decoder::Int64(values) => Ok(Arc::new(flush_primitive::<Int64Type>(values, nulls))),
            Decoder::TimeMillis(values) => {
                Ok(Arc::new(flush_primitive::<Time32MillisecondType>(values, nulls)))
            }
            Decoder::TimeMicros(values) => {
                Ok(Arc::new(flush_primitive::<Time64MicrosecondType>(values, nulls)))
            }
            Decoder::TimestampMillis(is_utc, values) => Ok(Arc::new(
                flush_primitive::<TimestampMillisecondType>(values, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into())),
            )),
            Decoder::TimestampMicros(is_utc, values) => Ok(Arc::new(
                flush_primitive::<TimestampMicrosecondType>(values, nulls)
                    .with_timezone_opt::<Arc<str>>(is_utc.then(|| "+00:00".into())),
            )),
            Decoder::Float32(values) => Ok(Arc::new(flush_primitive::<Float32Type>(values, nulls))),
            Decoder::Float64(values) => Ok(Arc::new(flush_primitive::<Float64Type>(values, nulls))),
            Decoder::Binary(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Ok(Arc::new(BinaryArray::new(offsets, values, nulls)))
            }
            Decoder::String(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Ok(Arc::new(StringArray::new(offsets, values, nulls)))
            }
            Decoder::List(field, offsets, values) => {
                let values = values.flush(None)?;
                let offsets = flush_offsets(offsets);
                Ok(Arc::new(ListArray::new(field.clone(), offsets, values, nulls)))
            }
            Decoder::Record(fields, encodings) => {
                let arrays = encodings
                    .iter_mut()
                    .map(|x| x.flush(None))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(StructArray::new(fields.clone(), arrays, nulls)))
            }
            Decoder::Enum(symbols, indices) => {
                let dict_values = StringArray::from_iter_values(symbols.iter());
                let indices_array: Int32Array = match nulls {
                    Some(buf) => {
                        let buffer = arrow_buffer::Buffer::from_slice_ref(&indices);
                        PrimitiveArray::<Int32Type>::try_new(
                            arrow_buffer::ScalarBuffer::from(buffer),
                            Some(buf.clone()),
                        )?
                    }
                    None => Int32Array::from_iter_values(indices.iter().cloned()),
                };
                let dict_array = DictionaryArray::<Int32Type>::try_new(
                    indices_array,
                    Arc::new(dict_values),
                )?;
                Ok(Arc::new(dict_array))
            }
            Decoder::Map(
                field,
                key_offsets_builder,
                map_offsets_builder,
                key_data,
                values_decoder_inner,
                current_entry_count,
            ) => {
                let map_offsets = flush_offsets(map_offsets_builder);
                let key_offsets = flush_offsets(key_offsets_builder);
                let key_data = flush_values(key_data).into();
                let key_array = StringArray::new(key_offsets, key_data, None);
                let val_array = values_decoder_inner.flush(None)?;
                let is_nullable = matches!(**values_decoder_inner, Decoder::Nullable(_, _, _));
                let struct_fields = vec![
                    Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                    Arc::new(ArrowField::new(
                        "value",
                        val_array.data_type().clone(),
                        is_nullable,
                    )),
                ];
                let struct_array = StructArray::new(
                    Fields::from(struct_fields),
                    vec![Arc::new(key_array), val_array],
                    None,
                );
                let map_array = MapArray::new(
                    field.clone(),
                    map_offsets.clone(),
                    struct_array.clone(),
                    nulls,
                    false,
                );
                Ok(Arc::new(map_array))
            }
            Decoder::Decimal(_precision, _scale, _size, builder) => {
                let precision = *_precision;
                let scale = _scale.unwrap_or(0); // Default scale if None
                let size = _size.clone();
                let builder = std::mem::replace(
                    builder,
                    DecimalBuilder::new(precision, *_scale, *_size)?,
                );
                Ok(builder.finish(nulls, precision, scale)?) // Pass precision and scale
            }

        }
    }
}

/// Helper to build a field with a given type
fn field_with_type(name: &str, dt: DataType, nullable: bool) -> FieldRef {
    Arc::new(ArrowField::new(name, dt, nullable))
}

/// Extends raw bytes to the target length with sign extension.
fn sign_extend(raw: &[u8], target_len: usize) -> Vec<u8> {
    if raw.is_empty() {
        return vec![0; target_len];
    }
    let sign_bit = raw[0] & 0x80;
    let mut extended = Vec::with_capacity(target_len);
    if sign_bit != 0 {
        extended.resize(target_len - raw.len(), 0xFF);
    } else {
        extended.resize(target_len - raw.len(), 0x00);
    }
    extended.extend_from_slice(raw);
    extended
}

/// Extends raw bytes to 16 bytes (for Decimal128).
fn extend_to_16_bytes(raw: &[u8]) -> Result<[u8; 16], ArrowError> {
    let extended = sign_extend(raw, 16);
    if extended.len() != 16 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend bytes to 16 bytes: got {} bytes",
            extended.len()
        )));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&extended);
    Ok(arr)
}

/// Extends raw bytes to 32 bytes (for Decimal256).
fn extend_to_32_bytes(raw: &[u8]) -> Result<[u8; 32], ArrowError> {
    let extended = sign_extend(raw, 32);
    if extended.len() != 32 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend bytes to 32 bytes: got {} bytes",
            extended.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&extended);
    Ok(arr)
}

/// Enum representing the builder for Decimal arrays.
#[derive(Debug)]
enum DecimalBuilder {
    Decimal128(Decimal128Builder),
    Decimal256(Decimal256Builder),
}

impl DecimalBuilder {
    /// Initializes a new `DecimalBuilder` based on precision, scale, and size.
    fn new(
        precision: usize,
        scale: Option<usize>,
        size: Option<usize>,
    ) -> Result<Self, ArrowError> {
        match size {
            Some(s) if s > 16 && s <= 32 => {
                // Decimal256
                Ok(Self::Decimal256(
                    Decimal256Builder::new()
                        .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
                ))
            }
            Some(s) if s <= 16 => {
                // Decimal128
                Ok(Self::Decimal128(
                    Decimal128Builder::new()
                        .with_precision_and_scale(precision as u8, scale.unwrap_or(0) as i8)?,
                ))
            }
            None => {
                // Infer based on precision
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

    /// Appends bytes to the decimal builder.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), ArrowError> {
        match self {
            DecimalBuilder::Decimal128(b) => {
                let padded = extend_to_16_bytes(bytes)?;
                let value = i128::from_be_bytes(padded);
                b.append_value(value);
            }
            DecimalBuilder::Decimal256(b) => {
                let padded = extend_to_32_bytes(bytes)?;
                let value = i256::from_be_bytes(padded);
                b.append_value(value);
            }
        }
        Ok(())
    }

    /// Appends a null value to the decimal builder by appending placeholder bytes.
    fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            DecimalBuilder::Decimal128(b) => {
                // Append zeroed bytes as placeholder
                let placeholder = [0u8; 16];
                let value = i128::from_be_bytes(placeholder);
                b.append_value(value);
            }
            DecimalBuilder::Decimal256(b) => {
                // Append zeroed bytes as placeholder
                let placeholder = [0u8; 32];
                let value = i256::from_be_bytes(placeholder);
                b.append_value(value);
            }
        }
        Ok(())
    }

    /// Finalizes the decimal array and returns it as an `ArrayRef`.
    fn finish(self, nulls: Option<NullBuffer>, precision: usize, scale: usize) -> Result<ArrayRef, ArrowError> {
        match self {
            DecimalBuilder::Decimal128(mut b) => {
                let array = b.finish();
                let values = array.values().clone();
                let decimal_array = Decimal128Array::new(
                    values,
                    nulls,
                ).with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(decimal_array))
            }
            DecimalBuilder::Decimal256(mut b) => {
                let array = b.finish();
                let values = array.values().clone();
                let decimal_array = Decimal256Array::new(
                    values,
                    nulls,
                ).with_precision_and_scale(precision as u8, scale as i8)?;
                Ok(Arc::new(decimal_array))
            }
        }
    }
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

const DEFAULT_CAPACITY: usize = 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, Int32Array, MapArray, StringArray, StructArray,
        Decimal128Array, Decimal256Array, DictionaryArray,
    };
    use arrow_buffer::Buffer;
    use arrow_schema::{Field as ArrowField, DataType as ArrowDataType};
    use serde_json::json;
    use arrow_array::cast::AsArray;

    /// Helper functions for encoding test data.
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

    #[test]
    fn test_enum_decoding() {
        let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols.clone()));
        let mut decoder = Decoder::try_new(&enum_dt).unwrap();
        // Encode the indices [1, 0, 2] using zigzag encoding
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_int(1)); // Encodes to [2]
        data.extend_from_slice(&encode_avro_int(0)); // Encodes to [0]
        data.extend_from_slice(&encode_avro_int(2)); // Encodes to [4]
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
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
    fn test_map_decoding_one_entry() {
        let value_type = AvroDataType::from_codec(Codec::Utf8);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Encode a single map with one entry: {"hello": "world"}
        // Avro encoding for a map:
        // - block_count: 1 (encoded as [2] due to ZigZag)
        // - keys: "hello" (encoded with length prefix)
        // - values: "world" (encoded with length prefix)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1)); // block_count = 1
        data.extend_from_slice(&encode_avro_bytes(b"hello")); // key = "hello"
        data.extend_from_slice(&encode_avro_bytes(b"world")); // value = "world"
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1); // One map
        assert_eq!(map_arr.value_length(0), 1); // One entry in the map
        let entries = map_arr.value(0);
        let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_entries.len(), 1); // One entry in StructArray
        let key = struct_entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = struct_entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key.value(0), "hello"); // Verify Key
        assert_eq!(value.value(0), "world"); // Verify Value
    }

    #[test]
    fn test_map_decoding_empty() {
        let value_type = AvroDataType::from_codec(Codec::Utf8);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Encode an empty map
        // Avro encoding for an empty map:
        // - block_count: 0 (encoded as [0] due to ZigZag)
        let data = encode_avro_long(0); // block_count = 0
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1); // One map
        assert_eq!(map_arr.value_length(0), 0); // Zero entries in the map
        let entries = map_arr.value(0);
        let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_entries.len(), 0); // Zero entries in StructArray
        let key = struct_entries
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = struct_entries
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key.len(), 0);
        assert_eq!(value.len(), 0);
    }

    #[test]
    fn test_decimal_decoding_fixed128() {
        let dt = AvroDataType::from_codec(Codec::Decimal(5, Some(2), Some(16)));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        // Row1: 123.45 => unscaled: 12345 => i128: 0x00000000000000000000000000003039
        // Row2: -1.23 => unscaled: -123 => i128: 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF85
        let row1 = [
            0x00, 0x00, 0x00, 0x00, // First 8 bytes
            0x00, 0x00, 0x00, 0x00, // Next 8 bytes
            0x00, 0x00, 0x00, 0x00, // Next 8 bytes
            0x00, 0x00, 0x30, 0x39, // Last 8 bytes: 0x3039 = 12345
        ];
        let row2 = [
            0xFF, 0xFF, 0xFF, 0xFF, // First 8 bytes (two's complement)
            0xFF, 0xFF, 0xFF, 0xFF, // Next 8 bytes
            0xFF, 0xFF, 0xFF, 0xFF, // Next 8 bytes
            0xFF, 0xFF, 0xFF, 0x85, // Last 8 bytes: 0xFFFFFF85 = -123
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&row1);
        data.extend_from_slice(&row2);
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        decoder.decode(&mut AvroCursor::new(&data[16..])).unwrap();
        let arr = decoder.flush(None).unwrap();
        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 2);
        assert_eq!(dec_arr.value_as_string(0), "123.45");
        assert_eq!(dec_arr.value_as_string(1), "-1.23");
    }

    #[test]
    fn test_decimal_decoding_bytes_with_nulls() {
        let dt = AvroDataType::from_codec(Codec::Decimal(4, Some(1), None));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        // Wrap the decimal in a Nullable decoder
        let mut nullable_decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(decoder),
        );
        // Row1: 123.4 => unscaled: 1234 => bytes: [0x04, 0xD2]
        // Row2: null
        // Row3: -123.4 => unscaled: -1234 => bytes: [0xFB, 0x2E]
        let mut data = Vec::new();
        // Row1: valid
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_bytes(&[0x04, 0xD2])); // 0x04D2 = 1234
        // Row2: null
        data.extend_from_slice(&[0u8]); // is_valid = false
        // Row3: valid
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_bytes(&[0xFB, 0x2E])); // 0xFB2E = -1234
        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap(); // Row1: 123.4
        nullable_decoder.decode(&mut cursor).unwrap(); // Row2: null
        nullable_decoder.decode(&mut cursor).unwrap(); // Row3: -123.4
        let array = nullable_decoder.flush(None).unwrap();
        let dec_arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
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
        let mut decoder = Decoder::try_new(&dt).unwrap();
        // Wrap the decimal in a Nullable decoder
        let mut nullable_decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(decoder),
        );
        // Correct Byte Encoding:
        // Row1: 1234.56 => unscaled: 123456 => bytes: [0x00; 12] + [0x00, 0x01, 0xE2, 0x40]
        // Row2: null
        // Row3: -1234.56 => unscaled: -123456 => bytes: [0xFF; 12] + [0xFE, 0x1D, 0xC0, 0x00]
        let row1_bytes = &[
            0x00, 0x00, 0x00, 0x00, // First 4 bytes
            0x00, 0x00, 0x00, 0x00, // Next 4 bytes
            0x00, 0x00, 0x00, 0x01, // Next 4 bytes
            0xE2, 0x40, 0x00, 0x00, // Last 4 bytes
        ];
        let row3_bytes = &[
            0xFF, 0xFF, 0xFF, 0xFF, // First 4 bytes (two's complement)
            0xFF, 0xFF, 0xFF, 0xFF, // Next 4 bytes
            0xFF, 0xFF, 0xFE, 0x1D, // Next 4 bytes
            0xC0, 0x00, 0x00, 0x00, // Last 4 bytes
        ];

        let mut data = Vec::new();
        // Row1: valid
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(row1_bytes); // 1234.56
        // Row2: null
        data.extend_from_slice(&[0u8]); // is_valid = false
        // Row3: valid
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(row3_bytes); // -1234.56

        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap(); // Row1: 1234.56
        nullable_decoder.decode(&mut cursor).unwrap(); // Row2: null
        nullable_decoder.decode(&mut cursor).unwrap(); // Row3: -1234.56

        let array = nullable_decoder.flush(None).unwrap();
        let dec_arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 3);
        assert!(dec_arr.is_valid(0));
        assert!(!dec_arr.is_valid(1));
        assert!(dec_arr.is_valid(2));
        assert_eq!(dec_arr.value_as_string(0), "1234.56");
        assert_eq!(dec_arr.value_as_string(2), "-1234.56");
    }

    #[test]
    fn test_enum_decoding_with_nulls() {
        let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols.clone()));
        let mut decoder = Decoder::try_new(&enum_dt).unwrap();

        // Wrap the enum in a Nullable decoder
        let mut nullable_decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(decoder),
        );

        // Encode the indices [1, null, 2] using ZigZag encoding
        // Indices: 1 -> [2], null -> no index, 2 -> [4]
        let mut data = Vec::new();
        // Row1: valid (1)
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_int(1)); // Encodes to [2]
        // Row2: null
        data.extend_from_slice(&[0u8]); // is_valid = false
        // Row3: valid (2)
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_int(2)); // Encodes to [4]

        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap(); // Row1: RED
        nullable_decoder.decode(&mut cursor).unwrap(); // Row2: null
        nullable_decoder.decode(&mut cursor).unwrap(); // Row3: BLUE

        let array = nullable_decoder.flush(None).unwrap();
        let dict_arr = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();

        assert_eq!(dict_arr.len(), 3);
        let keys = dict_arr.keys();
        let validity = dict_arr.is_valid(0); // Correctly access the null buffer

        assert_eq!(keys.value(0), 1);
        assert_eq!(keys.value(1), 0); // Placeholder index for null
        assert_eq!(keys.value(2), 2);

        assert!(dict_arr.is_valid(0));
        assert!(!dict_arr.is_valid(1)); // Ensure the second entry is null
        assert!(dict_arr.is_valid(2));

        let dict_values = dict_arr.values().as_string::<i32>();
        assert_eq!(dict_values.value(0), "RED");
        assert_eq!(dict_values.value(1), "GREEN");
        assert_eq!(dict_values.value(2), "BLUE");
    }

    #[test]
    fn test_enum_with_nullable_entries() {
        let symbols = vec!["APPLE".to_string(), "BANANA".to_string(), "CHERRY".to_string()];
        let enum_dt = AvroDataType::from_codec(Codec::Enum(symbols.clone()));
        let mut decoder = Decoder::try_new(&enum_dt).unwrap();

        // Wrap the enum in a Nullable decoder
        let mut nullable_decoder = Decoder::Nullable(
            Nullability::NullFirst,
            NullBufferBuilder::new(DEFAULT_CAPACITY),
            Box::new(decoder),
        );

        // Encode the indices [0, null, 2, 1] using ZigZag encoding
        let mut data = Vec::new();
        // Row1: valid (0) -> "APPLE"
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_int(0)); // Encodes to [0]
        // Row2: null
        data.extend_from_slice(&[0u8]); // is_valid = false
        // Row3: valid (2) -> "CHERRY"
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_int(2)); // Encodes to [4]
        // Row4: valid (1) -> "BANANA"
        data.extend_from_slice(&[1u8]); // is_valid = true
        data.extend_from_slice(&encode_avro_int(1)); // Encodes to [2]

        let mut cursor = AvroCursor::new(&data);
        nullable_decoder.decode(&mut cursor).unwrap(); // Row1: APPLE
        nullable_decoder.decode(&mut cursor).unwrap(); // Row2: null
        nullable_decoder.decode(&mut cursor).unwrap(); // Row3: CHERRY
        nullable_decoder.decode(&mut cursor).unwrap(); // Row4: BANANA

        let array = nullable_decoder.flush(None).unwrap();
        let dict_arr = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();

        assert_eq!(dict_arr.len(), 4);
        let keys = dict_arr.keys();
        let validity = dict_arr.is_valid(0); // Correctly access the null buffer

        assert_eq!(keys.value(0), 0);
        assert_eq!(keys.value(1), 0); // Placeholder index for null
        assert_eq!(keys.value(2), 2);
        assert_eq!(keys.value(3), 1);

        assert!(dict_arr.is_valid(0));
        assert!(!dict_arr.is_valid(1)); // Ensure the second entry is null
        assert!(dict_arr.is_valid(2));
        assert!(dict_arr.is_valid(3));

        let dict_values = dict_arr.values().as_string::<i32>();
        assert_eq!(dict_values.value(0), "APPLE");
        assert_eq!(dict_values.value(1), "BANANA");
        assert_eq!(dict_values.value(2), "CHERRY");
    }
}