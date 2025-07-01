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
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, FieldRef, Fields, Schema as ArrowSchema, SchemaRef,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

/// Decodes avro encoded data into [`RecordBatch`]
#[derive(Debug)]
pub struct RecordDecoder {
    schema: SchemaRef,
    fields: Vec<Decoder>,
    use_utf8view: bool,
    strict_mode: bool,
}

impl RecordDecoder {
    /// Create a new [`RecordDecoder`] from the provided [`AvroDataType`] with default options
    pub fn try_new(data_type: &AvroDataType) -> Result<Self, ArrowError> {
        Self::try_new_with_options(data_type, false, false)
    }

    /// Creates a new [`RecordDecoder`] from the provided [`AvroDataType`] with additional options.
    ///
    /// This method allows you to customize how the Avro data is decoded into Arrow arrays.
    ///
    /// # Arguments
    /// * `data_type` - The Avro data type to decode.
    /// * `use_utf8view` - A flag indicating whether to use `Utf8View` for string types.
    /// * `strict_mode` - A flag to enable strict decoding, returning an error if the data
    ///   does not conform to the schema.
    ///
    /// # Errors
    /// This function will return an error if the provided `data_type` is not a `Record`.
    pub fn try_new_with_options(
        data_type: &AvroDataType,
        use_utf8view: bool,
        strict_mode: bool,
    ) -> Result<Self, ArrowError> {
        match Decoder::try_new(data_type)? {
            Decoder::Record(fields, encodings) => Ok(Self {
                schema: Arc::new(ArrowSchema::new(fields)),
                fields: encodings,
                use_utf8view,
                strict_mode,
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
    /// String data encoded as UTF-8 bytes, mapped to Arrow's StringArray
    String(OffsetBufferBuilder<i32>, Vec<u8>),
    /// String data encoded as UTF-8 bytes, but mapped to Arrow's StringViewArray
    StringView(OffsetBufferBuilder<i32>, Vec<u8>),
    Array(FieldRef, OffsetBufferBuilder<i32>, Box<Decoder>),
    Record(Fields, Vec<Decoder>),
    Map(
        FieldRef,
        OffsetBufferBuilder<i32>,
        OffsetBufferBuilder<i32>,
        Vec<u8>,
        Box<Decoder>,
    ),
    Fixed(i32, Vec<u8>),
    Nullable(Nullability, NullBufferBuilder, Box<Decoder>),
}

impl Decoder {
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
            Codec::Utf8View => Self::StringView(
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
            Codec::Fixed(sz) => Self::Fixed(*sz, Vec::with_capacity(DEFAULT_CAPACITY)),
            Codec::Interval => return nyi("decoding interval"),
            Codec::List(item) => {
                let decoder = Self::try_new(item)?;
                Self::Array(
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
                let val_dec = Self::try_new(child)?;
                Self::Map(
                    map_field,
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    OffsetBufferBuilder::new(DEFAULT_CAPACITY),
                    Vec::with_capacity(DEFAULT_CAPACITY),
                    Box::new(val_dec),
                )
            }
            Codec::Uuid => Self::Fixed(16, Vec::with_capacity(DEFAULT_CAPACITY)),
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
            Self::Binary(offsets, _) | Self::String(offsets, _) | Self::StringView(offsets, _) => {
                offsets.push_length(0);
            }
            Self::Array(_, offsets, e) => {
                offsets.push_length(0);
                e.append_null();
            }
            Self::Record(_, e) => e.iter_mut().for_each(|e| e.append_null()),
            Self::Map(_, _koff, moff, _, _) => {
                moff.push_length(0);
            }
            Self::Nullable(_, _, _) => unreachable!("Nulls cannot be nested"),
            Self::Fixed(sz, accum) => {
                accum.extend(std::iter::repeat(0u8).take(*sz as usize));
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
            Self::Binary(offsets, values)
            | Self::String(offsets, values)
            | Self::StringView(offsets, values) => {
                let data = buf.get_bytes()?;
                offsets.push_length(data.len());
                values.extend_from_slice(data);
            }
            Self::Array(_, off, encoding) => {
                let total_items = read_blocks(buf, |cursor| encoding.decode(cursor))?;
                off.push_length(total_items);
            }
            Self::Record(_, encodings) => {
                for encoding in encodings {
                    encoding.decode(buf)?;
                }
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
            Self::Nullable(nullability, nulls, e) => {
                let is_valid = buf.get_bool()? == matches!(nullability, Nullability::NullFirst);
                nulls.append(is_valid);
                match is_valid {
                    true => e.decode(buf)?,
                    false => e.append_null(),
                }
            }
            Self::Fixed(sz, accum) => {
                let fx = buf.get_fixed(*sz as usize)?;
                accum.extend_from_slice(fx);
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
            Self::Binary(offsets, values) => {
                let offsets = flush_offsets(offsets);
                let values = flush_values(values).into();
                Arc::new(BinaryArray::new(offsets, values, nulls))
            }
            Self::String(offsets, values) => {
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
            Self::Record(fields, encodings) => {
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
                let entries_struct = StructArray::new(
                    Fields::from(vec![
                        Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                        Arc::new(ArrowField::new("value", val_arr.data_type().clone(), true)),
                    ]),
                    vec![Arc::new(key_arr), val_arr],
                    None,
                );
                let map_arr = MapArray::new(map_field.clone(), moff, entries_struct, nulls, false);
                Arc::new(map_arr)
            }
            Self::Fixed(sz, accum) => {
                let b: Buffer = flush_values(accum).into();
                let arr = FixedSizeBinaryArray::try_new(*sz, b, nulls)
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                Arc::new(arr)
            }
        })
    }
}

fn read_blocks(
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
        // Read the block count
        //  positive = that many items
        //  negative = that many items + read block size
        //  See: https://avro.apache.org/docs/1.11.1/specification/#maps
        let block_count = buf.get_long()?;
        match block_count.cmp(&0) {
            Ordering::Equal => break,
            Ordering::Less => {
                // If block_count is negative, read the absolute value of count,
                // then read the block size as a long and discard
                let count = (-block_count) as usize;
                if read_size_after_negative {
                    let _size_in_bytes = buf.get_long()?;
                }
                for _ in 0..count {
                    decode_fn(buf)?;
                }
                total += count;
            }
            Ordering::Greater => {
                // If block_count is positive, decode that many items
                let count = block_count as usize;
                for _i in 0..count {
                    decode_fn(buf)?;
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

const DEFAULT_CAPACITY: usize = 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        cast::AsArray, Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
        IntervalMonthDayNanoArray, ListArray, MapArray, StringArray, StructArray,
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

        let data1 = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut cursor1 = AvroCursor::new(&data1);
        decoder
            .decode(&mut cursor1)
            .expect("Failed to decode data1");
        assert_eq!(
            cursor1.position(),
            16,
            "Cursor should advance by fixed size"
        );
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
}
