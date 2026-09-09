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

use bytes::Bytes;

use super::{
    DeltaBitPackEncoder, DictEncoder, DictionaryStorage, DictionaryValue, Encoder, EncodingFamily,
    unsupported_column_encoding,
};
use crate::basic::{Encoding, Type};
use crate::data_type::private::byte_array_length;
use crate::data_type::{ByteArray, ByteArrayType, DataType, FixedLenByteArrayType, Int32Type};
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use crate::util::interner::{Interner, Storage};
use crate::util::prefix::common_prefix_length;

#[inline(always)]
pub(crate) fn append_plain_value(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&(value.len() as u32).to_ne_bytes());
    buffer.extend_from_slice(value);
}

/// Byte-array dictionary values stored directly in their final PLAIN page.
#[derive(Debug, Default)]
pub struct ByteArrayDictionaryStorage {
    page: Vec<u8>,
    values: Vec<std::ops::Range<usize>>,
}

impl Storage for ByteArrayDictionaryStorage {
    type Key = u64;
    type Value = [u8];

    fn get(&self, idx: Self::Key) -> &Self::Value {
        &self.page[self.values[idx as usize].clone()]
    }

    fn push(&mut self, value: &Self::Value) -> Self::Key {
        let key = self.values.len();
        self.page.reserve(4 + value.len());
        let start = self.page.len() + 4;
        append_plain_value(&mut self.page, value);
        self.values.push(start..self.page.len());
        key as u64
    }

    fn estimated_memory_size(&self) -> usize {
        self.page.capacity()
            + self.values.capacity() * std::mem::size_of::<std::ops::Range<usize>>()
    }
}

impl DictionaryStorage<ByteArray> for Interner<ByteArrayDictionaryStorage> {
    fn new(_desc: &ColumnDescPtr) -> Self {
        Self::default()
    }

    #[inline]
    fn intern(&mut self, value: &ByteArray) -> Result<u64> {
        byte_array_length(value.len())?;
        Ok(Interner::intern(self, value.data()))
    }

    #[inline(always)]
    fn intern_bytes(&mut self, bytes: &[u8], _make: impl FnOnce() -> ByteArray) -> Result<u64> {
        byte_array_length(bytes.len())?;
        Ok(Interner::intern(self, bytes))
    }

    fn len_and_size(&self) -> (usize, usize) {
        (self.storage().values.len(), self.storage().page.len())
    }

    fn visit_uniques(&self, mut visit: impl FnMut(&[u8])) {
        let storage = self.storage();
        for range in &storage.values {
            visit(&storage.page[range.clone()]);
        }
    }

    fn estimated_memory_size(&self) -> usize {
        Interner::estimated_memory_size(self)
    }

    fn write_dict<D: DataType<T = ByteArray>>(&self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(&self.storage().page))
    }

    fn into_dict<D: DataType<T = ByteArray>>(self) -> Result<Bytes> {
        Ok(Bytes::from(self.into_inner().page))
    }
}

impl DictionaryValue for ByteArray {
    type Storage = Interner<ByteArrayDictionaryStorage>;
}

/// Encodes byte-array values with dictionary, PLAIN, DELTA_LENGTH_BYTE_ARRAY,
/// or DELTA_BYTE_ARRAY encoding.
pub enum ByteArrayEncodingFamily {
    Dictionary(DictEncoder<ByteArrayType>),
    Plain(ByteArrayPlainEncoder),
    DeltaLength(ByteArrayDeltaLengthEncoder),
    Delta(ByteArrayDeltaEncoder),
}

#[derive(Default)]
pub struct ByteArrayPlainEncoder {
    buffer: Vec<u8>,
}

impl ByteArrayPlainEncoder {
    #[inline(always)]
    pub(crate) fn put_value(&mut self, value: &[u8]) {
        append_plain_value(&mut self.buffer, value);
    }

    #[inline(always)]
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }

    pub(crate) fn estimated_data_encoded_size(&self) -> usize {
        self.buffer.len()
    }

    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.buffer.capacity()
    }

    pub(crate) fn flush_buffer(&mut self) -> Bytes {
        std::mem::take(&mut self.buffer).into()
    }
}

/// The shared `[delta lengths][payload]` component used by both byte-array
/// delta encodings.
pub struct ByteArrayDeltaLengthEncoder {
    data: Vec<u8>,
    lengths: Box<DeltaBitPackEncoder<Int32Type>>,
}

impl Default for ByteArrayDeltaLengthEncoder {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            lengths: Box::new(DeltaBitPackEncoder::new()),
        }
    }
}

impl ByteArrayDeltaLengthEncoder {
    #[inline(always)]
    pub(crate) fn put_value(&mut self, value: &[u8]) -> Result<()> {
        self.lengths.put_i64(value.len() as i64)?;
        self.data.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn estimated_data_encoded_size(&self) -> usize {
        self.data.len() + self.lengths.estimated_data_encoded_size()
    }

    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.data.capacity() + self.lengths.estimated_memory_size()
    }

    pub(crate) fn flush_buffer(&mut self) -> Result<Bytes> {
        self.flush_prefixed(&[])
    }

    fn flush_prefixed(&mut self, prefix: &[u8]) -> Result<Bytes> {
        let lengths = self.lengths.flush_buffer()?;
        let mut out = Vec::with_capacity(prefix.len() + lengths.len() + self.data.len());
        out.extend_from_slice(prefix);
        out.extend_from_slice(&lengths);
        out.extend_from_slice(&self.data);
        self.data.clear();
        Ok(out.into())
    }
}

/// The DELTA_BYTE_ARRAY implementation shared by variable and fixed-length
/// byte-array writers.
pub struct ByteArrayDeltaEncoder {
    last_value: Vec<u8>,
    prefix_lengths: Box<DeltaBitPackEncoder<Int32Type>>,
    suffixes: ByteArrayDeltaLengthEncoder,
}

impl Default for ByteArrayDeltaEncoder {
    fn default() -> Self {
        Self {
            last_value: Vec::new(),
            prefix_lengths: Box::new(DeltaBitPackEncoder::new()),
            suffixes: ByteArrayDeltaLengthEncoder::default(),
        }
    }
}

impl ByteArrayDeltaEncoder {
    #[inline(always)]
    pub(crate) fn put_value(&mut self, value: &[u8]) -> Result<()> {
        let prefix_length = common_prefix_length(&self.last_value, value);

        self.last_value.clear();
        self.last_value.extend_from_slice(value);
        self.prefix_lengths.put_i64(prefix_length as i64)?;
        self.suffixes.put_value(&value[prefix_length..])
    }

    pub(crate) fn estimated_data_encoded_size(&self) -> usize {
        self.prefix_lengths.estimated_data_encoded_size()
            + self.suffixes.estimated_data_encoded_size()
    }

    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.last_value.capacity()
            + self.prefix_lengths.estimated_memory_size()
            + self.suffixes.estimated_memory_size()
    }

    pub(crate) fn flush_buffer(&mut self) -> Result<Bytes> {
        let prefix_lengths = self.prefix_lengths.flush_buffer()?;
        let out = self.suffixes.flush_prefixed(&prefix_lengths)?;
        self.last_value.clear();
        Ok(out)
    }
}

impl Encoder<FixedLenByteArrayType> for ByteArrayDeltaEncoder {
    fn put(
        &mut self,
        values: &[<FixedLenByteArrayType as crate::data_type::DataType>::T],
    ) -> Result<()> {
        for value in values {
            self.put_value(value.data())?;
        }
        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BYTE_ARRAY
    }

    fn estimated_data_encoded_size(&self) -> usize {
        ByteArrayDeltaEncoder::estimated_data_encoded_size(self)
    }

    fn estimated_memory_size(&self) -> usize {
        ByteArrayDeltaEncoder::estimated_memory_size(self)
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        ByteArrayDeltaEncoder::flush_buffer(self)
    }
}

impl ByteArrayEncodingFamily {
    fn from_encoding(encoding: Encoding, _descr: &ColumnDescPtr) -> Result<Self> {
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(Default::default())),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => Ok(Self::DeltaLength(Default::default())),
            Encoding::DELTA_BYTE_ARRAY => Ok(Self::Delta(Default::default())),
            e => Err(unsupported_column_encoding(e, Type::BYTE_ARRAY)),
        }
    }
}

impl<D: DataType<T = ByteArray>> Encoder<D> for ByteArrayEncodingFamily {
    fn put(&mut self, values: &[ByteArray]) -> Result<()> {
        match self {
            Self::Dictionary(encoder) => Encoder::<ByteArrayType>::put(encoder, values),
            Self::Plain(encoder) => {
                let mut payload = 0usize;
                for value in values {
                    byte_array_length(value.len())?;
                    payload = payload.saturating_add(value.len());
                }
                encoder.reserve(payload.saturating_add(values.len().saturating_mul(4)));
                for value in values {
                    encoder.put_value(value.data());
                }
                Ok(())
            }
            Self::DeltaLength(encoder) => {
                for value in values {
                    encoder.put_value(value.data())?;
                }
                Ok(())
            }
            Self::Delta(encoder) => {
                for value in values {
                    encoder.put_value(value.data())?;
                }
                Ok(())
            }
        }
    }

    fn encoding(&self) -> Encoding {
        match self {
            Self::Dictionary(encoder) => Encoder::<ByteArrayType>::encoding(encoder),
            Self::Plain(_) => Encoding::PLAIN,
            Self::DeltaLength(_) => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Self::Delta(_) => Encoding::DELTA_BYTE_ARRAY,
        }
    }

    fn estimated_data_encoded_size(&self) -> usize {
        match self {
            Self::Dictionary(encoder) => {
                Encoder::<ByteArrayType>::estimated_data_encoded_size(encoder)
            }
            Self::Plain(encoder) => encoder.estimated_data_encoded_size(),
            Self::DeltaLength(encoder) => encoder.estimated_data_encoded_size(),
            Self::Delta(encoder) => encoder.estimated_data_encoded_size(),
        }
    }

    fn estimated_memory_size(&self) -> usize {
        match self {
            Self::Dictionary(encoder) => Encoder::<ByteArrayType>::estimated_memory_size(encoder),
            Self::Plain(encoder) => encoder.estimated_memory_size(),
            Self::DeltaLength(encoder) => encoder.estimated_memory_size(),
            Self::Delta(encoder) => encoder.estimated_memory_size(),
        }
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        match self {
            Self::Dictionary(encoder) => Encoder::<ByteArrayType>::flush_buffer(encoder),
            Self::Plain(encoder) => Ok(encoder.flush_buffer()),
            Self::DeltaLength(encoder) => encoder.flush_buffer(),
            Self::Delta(encoder) => encoder.flush_buffer(),
        }
    }
}

impl_dictionary_encoding_family!(ByteArrayEncodingFamily, ByteArrayType);
