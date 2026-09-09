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

//! Compatibility interfaces for the experimental concrete encoders.
//!
//! Writer families use the private implementations directly, not these adapters.

#![expect(
    deprecated,
    reason = "Implement the deprecated concrete encoder interfaces"
)]

use super::byte_array::{ByteArrayDeltaEncoder, ByteArrayDeltaLengthEncoder};
use super::{Encoder, PlainEncoderImpl};
use crate::basic::{Encoding, Type};
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, DataType, FixedLenByteArray};
use crate::errors::Result;
use bytes::Bytes;
use std::marker::PhantomData;

/// PLAIN encoding for any Parquet physical type.
///
/// Values are stored back to back: Boolean values are bit-packed, numbers are
/// little-endian, BYTE_ARRAY values have a four-byte length prefix, and
/// FIXED_LEN_BYTE_ARRAY values have no length prefix.
///
/// This compatibility interface retains descriptor-free construction and the
/// generic `T: DataType` contract. Prefer [`super::get_encoder`] for new code.
#[deprecated(
    since = "60.0.0",
    note = "Use get_encoder with Encoding::PLAIN instead"
)]
pub struct PlainEncoder<T: DataType> {
    inner: PlainEncoderImpl<T>,
}

impl<T: DataType> Default for PlainEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> PlainEncoder<T> {
    /// Creates a new PLAIN encoder.
    pub fn new() -> Self {
        Self {
            inner: PlainEncoderImpl::new(),
        }
    }
}

impl<T: DataType> Encoder<T> for PlainEncoder<T> {
    #[inline]
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        <T::T as ParquetValueType>::encode(
            values,
            &mut self.inner.buffer,
            &mut self.inner.bit_writer,
        )
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.inner.buffer.len() + self.inner.bit_writer.bytes_written()
    }

    fn estimated_memory_size(&self) -> usize {
        self.inner.buffer.capacity() + self.inner.bit_writer.estimated_memory_size()
    }

    #[inline]
    fn flush_buffer(&mut self) -> Result<Bytes> {
        self.inner
            .buffer
            .extend_from_slice(self.inner.bit_writer.flush_buffer());
        self.inner.bit_writer.clear();
        Ok(std::mem::take(&mut self.inner.buffer).into())
    }
}

/// DELTA_LENGTH_BYTE_ARRAY encoding for BYTE_ARRAY values.
///
/// Lengths are delta-binary-packed, followed by the raw payload bytes.
/// Prefer [`super::get_encoder`] for new code.
#[deprecated(
    since = "60.0.0",
    note = "Use get_encoder with Encoding::DELTA_LENGTH_BYTE_ARRAY instead"
)]
pub struct DeltaLengthByteArrayEncoder<T: DataType> {
    inner: ByteArrayDeltaLengthEncoder,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for DeltaLengthByteArrayEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> DeltaLengthByteArrayEncoder<T> {
    /// Creates a new delta length byte array encoder.
    pub fn new() -> Self {
        Self {
            inner: ByteArrayDeltaLengthEncoder::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for DeltaLengthByteArrayEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        // Preserve the historical type check, including empty FLBA inputs.
        ensure_phys_ty!(
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY,
            "DeltaLengthByteArrayEncoder only supports ByteArrayType"
        );
        for value in values {
            let value = value.as_any().downcast_ref::<ByteArray>().unwrap();
            self.inner.put_value(value.data())?;
        }
        Ok(())
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::DELTA_LENGTH_BYTE_ARRAY
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.inner.estimated_data_encoded_size()
    }

    fn estimated_memory_size(&self) -> usize {
        self.inner.estimated_memory_size()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        ensure_phys_ty!(
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY,
            "DeltaLengthByteArrayEncoder only supports ByteArrayType"
        );
        self.inner.flush_buffer()
    }
}

/// DELTA_BYTE_ARRAY encoding for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values.
///
/// Prefix lengths are delta-binary-packed, followed by suffixes encoded with
/// DELTA_LENGTH_BYTE_ARRAY. Prefer [`super::get_encoder`] for new code.
#[deprecated(
    since = "60.0.0",
    note = "Use get_encoder with Encoding::DELTA_BYTE_ARRAY instead"
)]
pub struct DeltaByteArrayEncoder<T: DataType> {
    inner: ByteArrayDeltaEncoder,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for DeltaByteArrayEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> DeltaByteArrayEncoder<T> {
    /// Creates a new delta byte array encoder.
    pub fn new() -> Self {
        Self {
            inner: ByteArrayDeltaEncoder::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for DeltaByteArrayEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        for value in values {
            let value = value.as_any();
            let bytes = match T::get_physical_type() {
                Type::BYTE_ARRAY => value.downcast_ref::<ByteArray>().unwrap().data(),
                Type::FIXED_LEN_BYTE_ARRAY => {
                    value.downcast_ref::<FixedLenByteArray>().unwrap().data()
                }
                _ => panic!(
                    "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
                ),
            };
            self.inner.put_value(bytes)?;
        }
        Ok(())
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BYTE_ARRAY
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.inner.estimated_data_encoded_size()
    }

    fn estimated_memory_size(&self) -> usize {
        self.inner.estimated_memory_size()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        match T::get_physical_type() {
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => self.inner.flush_buffer(),
            _ => panic!(
                "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
            ),
        }
    }
}
