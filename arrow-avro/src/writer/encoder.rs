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

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::OffsetSizeTrait;
use arrow_array::{Array, GenericBinaryArray, PrimitiveArray, RecordBatch};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, FieldRef, TimeUnit};
use std::io::Write;

/// Behavior knobs for the Avro encoder.
///
/// When `impala_mode` is `true`, optional/nullable values are encoded
/// as Avro unions with **null second** (`[T, "null"]`). When `false`
/// (default), we use **null first** (`["null", T]`).
#[derive(Debug, Clone, Copy, Default)]
pub struct EncoderOptions {
    impala_mode: bool, // Will be fully implemented in a follow-up PR
}

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
fn write_optional_branch<W: Write + ?Sized>(
    writer: &mut W,
    is_null: bool,
    impala_mode: bool,
) -> Result<(), ArrowError> {
    let branch = if impala_mode == is_null { 1 } else { 0 };
    write_int(writer, branch)
}

/// Encode a `RecordBatch` in Avro binary format using **default options**.
pub fn encode_record_batch<W: Write>(batch: &RecordBatch, out: &mut W) -> Result<(), ArrowError> {
    encode_record_batch_with_options(batch, out, &EncoderOptions::default())
}

/// Encode a `RecordBatch` with explicit `EncoderOptions`.
pub fn encode_record_batch_with_options<W: Write>(
    batch: &RecordBatch,
    out: &mut W,
    opts: &EncoderOptions,
) -> Result<(), ArrowError> {
    let mut encoders = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| Ok((field.is_nullable(), make_encoder(array.as_ref())?)))
        .collect::<Result<Vec<_>, ArrowError>>()?;
    (0..batch.num_rows()).try_for_each(|row| {
        encoders.iter_mut().try_for_each(|(is_nullable, enc)| {
            if *is_nullable {
                let is_null = enc.is_null(row);
                write_optional_branch(out, is_null, opts.impala_mode)?;
                if is_null {
                    return Ok(());
                }
            }
            enc.encode(row, out)
        })
    })
}

/// Enum for static dispatch of concrete encoders.
enum Encoder<'a> {
    Boolean(BooleanEncoder<'a>),
    Int(IntEncoder<'a, Int32Type>),
    Long(LongEncoder<'a, Int64Type>),
    Timestamp(LongEncoder<'a, TimestampMicrosecondType>),
    Float32(F32Encoder<'a>),
    Float64(F64Encoder<'a>),
    Binary(BinaryEncoder<'a, i32>),
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
        }
    }
}

/// An encoder + a null buffer for nullable fields.
pub struct NullableEncoder<'a> {
    encoder: Encoder<'a>,
    nulls: Option<NullBuffer>,
}

impl<'a> NullableEncoder<'a> {
    /// Create a new nullable encoder, wrapping a non-null encoder and a null buffer.
    #[inline]
    fn new(encoder: Encoder<'a>, nulls: Option<NullBuffer>) -> Self {
        Self { encoder, nulls }
    }

    /// Encode the value at `idx`, assuming it's not-null.
    #[inline]
    fn encode<W: Write + ?Sized>(&mut self, idx: usize, out: &mut W) -> Result<(), ArrowError> {
        self.encoder.encode(idx, out)
    }

    /// Check if the value at `idx` is null.
    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| nulls.is_null(idx))
    }
}

/// Creates an Avro encoder for the given `array`.
pub fn make_encoder<'a>(array: &'a dyn Array) -> Result<NullableEncoder<'a>, ArrowError> {
    let nulls = array.nulls().cloned();
    let enc = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            NullableEncoder::new(Encoder::Boolean(BooleanEncoder(arr)), nulls)
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<Int32Type>();
            NullableEncoder::new(Encoder::Int(IntEncoder(arr)), nulls)
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<Int64Type>();
            NullableEncoder::new(Encoder::Long(LongEncoder(arr)), nulls)
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            NullableEncoder::new(Encoder::Float32(F32Encoder(arr)), nulls)
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            NullableEncoder::new(Encoder::Float64(F64Encoder(arr)), nulls)
        }
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            NullableEncoder::new(Encoder::Binary(BinaryEncoder(arr)), nulls)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array.as_primitive::<TimestampMicrosecondType>();
            NullableEncoder::new(Encoder::Timestamp(LongEncoder(arr)), nulls)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Unsupported data type for Avro encoding in slim build: {other:?}"
            )))
        }
    };
    Ok(enc)
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
