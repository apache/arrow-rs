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
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef, Fields, Schema as ArrowSchema, SchemaRef, TimeUnit};
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
    Map(FieldRef, OffsetBufferBuilder<i32>, OffsetBufferBuilder<i32>, Vec<u8>, Box<Decoder>, usize),
    Decimal(usize, usize, Option<usize>, Vec<Vec<u8>>),
}

impl Decoder {
    /// Checks if the Decoder is nullable
    fn is_nullable(&self) -> bool {
        matches!(self, Decoder::Nullable(_, _, _))
    }

    fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        let nyi = |s: &str| Err(ArrowError::NotYetImplemented(s.to_string()));

        let decoder = match data_type.codec() {
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
            Codec::Utf8 => Self::String(
                OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                Vec::with_capacity(DEFAULT_CAPACITY),
            ),
            Codec::Date32 => Self::Date32(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMillis => Self::TimeMillis(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimeMicros => Self::TimeMicros(Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::TimestampMillis(is_utc) => {
                Self::TimestampMillis(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::TimestampMicros(is_utc) => {
                Self::TimestampMicros(*is_utc, Vec::with_capacity(DEFAULT_CAPACITY))
            }
            Codec::Fixed(_) => return nyi("decoding fixed"),
            Codec::Interval => return nyi("decoding interval"),
            Codec::List(item) => {
                let decoder = Self::try_new(item)?;
                Self::List(
                    Arc::new(item.field_with_name("item")),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Box::new(decoder),
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
                Self::Record(arrow_fields.into(), encodings)
            }
            Codec::Enum(symbols) => {
                Decoder::Enum(
                    symbols.clone(),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                )
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
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY), // key_offsets
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),  // map_offsets
                    Vec::with_capacity(DEFAULT_CAPACITY),         // key_data
                    Box::new(Self::try_new(value_type)?),       // values_decoder_inner
                    0,                                           // current_entry_count
                )
            }
            Codec::Decimal(precision, scale, size) => {
                Decoder::Decimal(
                    *precision,
                    scale.unwrap_or(0),
                    *size,
                    Vec::with_capacity(DEFAULT_CAPACITY),
                )
            }
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
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v) => v.push(0),
            Self::Float32(v) => v.push(0.),
            Self::Float64(v) => v.push(0.),
            Self::Binary(offsets, _) | Self::String(offsets, _) => offsets.push_length(0),
            Self::List(_, offsets, e) => {
                offsets.push_length(0);
                e.append_null();
            }
            Self::Record(_, e) => e.iter_mut().for_each(|e| e.append_null()),
            Self::Nullable(_, _, _) => unreachable!("Nulls cannot be nested"),
            Self::Enum(_, _) => {
                // For Enum, appending a null is not straightforward. Handle accordingly if needed.
            }
            Self::Map(
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
            Self::Decimal(_, _, _, _) => {
                // For Decimal, appending a null doesn't make sense as per current implementation
            }
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
            Self::Binary(offsets, values) | Self::String(offsets, values) => {
                let data = buf.get_bytes()?;
                offsets.push_length(data.len());
                values.extend_from_slice(data);
            }
            Self::List(_, _, _) => {
                return Err(ArrowError::NotYetImplemented(
                    "Decoding ListArray".to_string(),
                ))
            }
            Self::Record(_, encodings) => {
                for encoding in encodings {
                    encoding.decode(buf)?;
                }
            }
            Self::Nullable(nullability, nulls, e) => {
                let is_valid = buf.get_bool()? == matches!(nullability, Nullability::NullFirst);
                nulls.append(is_valid);
                match is_valid {
                    true => e.decode(buf)?,
                    false => e.append_null(),
                }
            }
            Self::Enum(symbols, indices) => {
                // Encodes enum by writing its zero-based index as an int
                let index = buf.get_int()?;
                indices.push(index);
            }
            Self::Map(
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
            Self::Decimal(
                precision,
                scale,
                size,
                data
            ) => {
                let raw = if let Some(fixed_len) = size {
                    // get_fixed used to get exactly fixed_len bytes
                    buf.get_fixed(*fixed_len)?
                } else {
                    // get_bytes used for variable-length
                    buf.get_bytes()?
                };
                data.push(raw.to_vec());
            }
        }
        Ok(())
    }

    /// Flush decoded records to an [`ArrayRef`]
    fn flush(&mut self, nulls: Option<NullBuffer>) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Nullable(_, n, e) => e.flush(n.finish()),
            Self::Null(size) => Ok(Arc::new(NullArray::new(std::mem::replace(size, 0)))),
            Self::Boolean(b) => Ok(Arc::new(BooleanArray::new(b.finish(), nulls))),
            Self::Int32(values) => Ok(Arc::new(flush_primitive::<Int32Type>(values, nulls))),
            Self::Date32(values) => Ok(Arc::new(flush_primitive::<Date32Type>(values, nulls))),
            Self::Int64(values) => Ok(Arc::new(flush_primitive::<Int64Type>(values, nulls))),
            Self::TimeMillis(values) => {
                Ok(Arc::new(flush_primitive::<Time32MillisecondType>(values, nulls)))
            }
            Self::TimeMicros(values) => {
                Ok(Arc::new(flush_primitive::<Time64MicrosecondType>(values, nulls)))
            }
            Self::TimestampMillis(is_utc, values) => Ok(Arc::new(
                flush_primitive::<TimestampMillisecondType>(values, nulls)
                    .with_timezone_opt(is_utc.then(|| "+00:00")),
            )),
            Self::TimestampMicros(is_utc, values) => Ok(Arc::new(
                flush_primitive::<TimestampMicrosecondType>(values, nulls)
                    .with_timezone_opt(is_utc.then(|| "+00:00")),
            )),
            Self::Float32(values) => Ok(Arc::new(flush_primitive::<Float32Type>(values, nulls))),
            Self::Float64(values) => Ok(Arc::new(flush_primitive::<Float64Type>(values, nulls))),
            Self::Binary(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Ok(Arc::new(BinaryArray::new(offsets, values, nulls)))
            }
            Self::String(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Ok(Arc::new(StringArray::new(offsets, values, nulls)))
            }
            Self::List(field, offsets, values) => {
                let values = values.flush(None)?;
                let offsets = flush_offsets(offsets);
                Ok(Arc::new(ListArray::new(field.clone(), offsets, values, nulls)))
            }
            Self::Record(fields, encodings) => {
                let arrays = encodings
                    .iter_mut()
                    .map(|x| x.flush(None))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(StructArray::new(fields.clone(), arrays, nulls)))
            }
            Self::Enum(symbols, indices) => {
                let dict_values = StringArray::from_iter_values(symbols.iter());
                let flushed_indices = flush_values(indices); // Vec<i32>
                let indices_array: Int32Array = match nulls {
                    Some(buf) => {
                        let buffer = Buffer::from_slice_ref(&flushed_indices);
                        PrimitiveArray::<Int32Type>::try_new(ScalarBuffer::from(buffer), Some(buf.clone()))?
                    },
                    None => {
                        Int32Array::from_iter_values(flushed_indices)
                    }
                };
                let dict_array = DictionaryArray::<Int32Type>::try_new(
                    indices_array,
                    Arc::new(dict_values),
                )?;
                Ok(Arc::new(dict_array))
            }
            Self::Map(
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
                    Arc::new(ArrowField::new("value", val_array.data_type().clone(), is_nullable)),
                ];
                let struct_array = StructArray::new(
                    Fields::from(struct_fields),
                    vec![Arc::new(key_array), val_array],
                    None,
                );
                let map_array = MapArray::new(field.clone(), map_offsets.clone(), struct_array.clone(), nulls, false);
                Ok(Arc::new(map_array))
            }
            Self::Decimal(
                precision,
                scale,
                size,
                data,
            ) => {
                let mut array_builder = DecimalBuilder::new(*precision, *scale, *size)?;
                for raw in data.drain(..) {
                    if let Some(s) = size {
                        if raw.len() < *s {
                            let extended = sign_extend(&raw, *s);
                            array_builder.append_bytes(&extended)?;
                            continue;
                        }
                    }
                    array_builder.append_bytes(&raw)?;
                }
                let arr = array_builder.finish()?;
                Ok(Arc::new(arr))
            }
        }
    }
}

/// Helper to build a field with a given type
fn field_with_type(name: &str, dt: DataType, nullable: bool) -> FieldRef {
    Arc::new(ArrowField::new(name, dt, nullable))
}

fn sign_extend(raw: &[u8], target_len: usize) -> Vec<u8> {
    if raw.is_empty() {
        return vec![0; target_len];
    }
    let sign_bit = raw[0] & 0x80;
    let mut extended = Vec::with_capacity(target_len);
    if sign_bit != 0 {  // negative
        extended.resize(target_len - raw.len(), 0xFF);
    } else {            // positive
        extended.resize(target_len - raw.len(), 0x00);
    }
    extended.extend_from_slice(raw);
    extended
}

/// Extend raw bytes to 16 bytes (for Decimal128)
fn extend_to_16_bytes(raw: &[u8]) -> Result<[u8; 16], ArrowError> {
    let extended = sign_extend(raw, 16);
    if extended.len() != 16 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend bytes to 16 bytes: got {} bytes",
            extended.len()
        )));
    }
    Ok(extended.try_into().unwrap())
}

/// Extend raw bytes to 32 bytes (for Decimal256)
fn extend_to_32_bytes(raw: &[u8]) -> Result<[u8; 32], ArrowError> {
    let extended = sign_extend(raw, 32);
    if extended.len() != 32 {
        return Err(ArrowError::ParseError(format!(
            "Failed to extend bytes to 32 bytes: got {} bytes",
            extended.len()
        )));
    }
    Ok(extended.try_into().unwrap())
}

/// Trait for building decimal arrays
enum DecimalBuilder {
    Decimal128(Decimal128Builder),
    Decimal256(Decimal256Builder),
}

impl DecimalBuilder {

    fn new(precision: usize, scale: usize, size: Option<usize>) -> Result<Self, ArrowError> {
        match size {
            Some(s) if s > 16 => {
                // decimal256
                Ok(Self::Decimal256(Decimal256Builder::new().with_precision_and_scale(precision as u8, scale as i8)?))
            }
            _ => {
                // decimal128
                Ok(Self::Decimal128(Decimal128Builder::new().with_precision_and_scale(precision as u8, scale as i8)?))
            }
        }
    }

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

    fn finish(self) -> Result<ArrayRef, ArrowError> {
        match self {
            DecimalBuilder::Decimal128(mut b) => Ok(Arc::new(b.finish())),
            DecimalBuilder::Decimal256(mut b) => Ok(Arc::new(b.finish())),
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
    use arrow_array::cast::AsArray;
    use arrow_schema::{Field as ArrowField, DataType as ArrowDataType};

    /// Helper functions for encoding test data
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
        // Avro encoding for a map:
        // - block_count: 1 (number of entries)
        // - keys: "hello" (5 bytes)
        // - values: "world" (5 bytes)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_avro_long(1));         // block_count = 1
        data.extend_from_slice(&encode_avro_bytes(b"hello")); // key = "hello"
        data.extend_from_slice(&encode_avro_bytes(b"world")); // value = "world"
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1); // Verify 1 map
        assert_eq!(map_arr.value_length(0), 1); // Verify 1 entry in the map
        let entries = map_arr.value(0);
        let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_entries.len(), 1); // Verify 1 entry in StructArray
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
        assert_eq!(key.value(0), "hello");  // Verify Key
        assert_eq!(value.value(0), "world"); // Verify Value
    }

    #[test]
    fn test_map_decoding_empty() {
        let value_type = AvroDataType::from_codec(Codec::Utf8);
        let map_type = AvroDataType::from_codec(Codec::Map(Arc::new(value_type)));
        let mut decoder = Decoder::try_new(&map_type).unwrap();
        // Avro encoding for an empty map:
        // - block_count: 0 (no entries)
        let data = encode_avro_long(0); // block_count = 0
        decoder.decode(&mut AvroCursor::new(&data)).unwrap();
        let array = decoder.flush(None).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1); // Verify 1 map
        assert_eq!(map_arr.value_length(0), 0); // Verify 0 entries in the map
        let entries = map_arr.value(0);
        let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_entries.len(), 0); // // Verify 0 entries StructArray
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
    fn test_decimal_decoding_bytes() {
        let dt = AvroDataType::from_codec(Codec::Decimal(4, Some(1), None));
        let mut decoder = Decoder::try_new(&dt).unwrap();
        let unscaled_row1: i128 = 1234;   // 123.4
        let unscaled_row2: i128 = -1234;  // -123.4
        // Note: convert unscaled values to big-endian bytes
        let bytes_row1 = unscaled_row1.to_be_bytes();
        let bytes_row2 = unscaled_row2.to_be_bytes();
        // Row1: 1234 => 0x04D2 (2 bytes)
        // Row2: -1234 => two's complement of 0x04D2 = 0xFB2E (2 bytes)
        let row1_bytes = &bytes_row1[14..16]; // Last 2 bytes
        let row2_bytes = &bytes_row2[14..16]; // Last 2 bytes
        let mut data = Vec::new();
        // Encode row1
        data.extend_from_slice(&encode_avro_long(2)); // Length=2
        data.extend_from_slice(row1_bytes);          // 0x04, 0xD2
        // Encode row2
        data.extend_from_slice(&encode_avro_long(2)); // Length=2
        data.extend_from_slice(row2_bytes);          // 0xFB, 0x2E
        let mut cursor = AvroCursor::new(&data);
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        let arr = decoder.flush(None).unwrap();
        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.len(), 2);
        assert_eq!(dec_arr.value_as_string(0), "123.4");
        assert_eq!(dec_arr.value_as_string(1), "-123.4");
    }
}
