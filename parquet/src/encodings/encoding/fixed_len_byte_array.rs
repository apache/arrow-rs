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

//! Fixed-length byte-array native batches and encoder adapters.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;
use crate::util::interner::{Interner, Storage};

/// Fixed-length dictionary values stored directly in their final PLAIN page.
///
/// Unlike the generic typed dictionary storage, this representation owns one
/// contiguous allocation rather than one heap-backed `FixedLenByteArray` per
/// distinct value. Since PLAIN encodes fixed-length values back-to-back, the
/// storage can be moved directly into the dictionary page when encoding ends.
#[derive(Debug)]
pub struct FixedLenByteArrayDictionaryStorage {
    page: Vec<u8>,
    type_length: usize,
    value_count: usize,
}

impl Storage for FixedLenByteArrayDictionaryStorage {
    type Key = u64;
    type Value = [u8];

    #[inline]
    fn get(&self, idx: Self::Key) -> &Self::Value {
        let start = idx as usize * self.type_length;
        &self.page[start..start + self.type_length]
    }

    fn push(&mut self, value: &Self::Value) -> Self::Key {
        debug_assert_eq!(value.len(), self.type_length);
        let key = self.value_count;
        self.page.extend_from_slice(value);
        self.value_count += 1;
        key as u64
    }

    fn estimated_memory_size(&self) -> usize {
        self.page.capacity()
    }
}

impl DictionaryStorage<FixedLenByteArray> for Interner<FixedLenByteArrayDictionaryStorage> {
    fn new(desc: &ColumnDescPtr) -> Self {
        let type_length = desc.type_length() as usize;
        debug_assert!(type_length > 0);
        Self::new(FixedLenByteArrayDictionaryStorage {
            page: Vec::new(),
            type_length,
            value_count: 0,
        })
    }

    #[inline]
    fn intern(&mut self, value: &FixedLenByteArray) -> Result<u64> {
        debug_assert_eq!(value.len(), self.storage().type_length);
        Ok(Interner::intern(self, value.data()))
    }

    #[inline(always)]
    fn intern_bytes(
        &mut self,
        bytes: &[u8],
        _make: impl FnOnce() -> FixedLenByteArray,
    ) -> Result<u64> {
        debug_assert_eq!(bytes.len(), self.storage().type_length);
        Ok(Interner::intern(self, bytes))
    }

    fn len_and_size(&self) -> (usize, usize) {
        (self.storage().value_count, self.storage().page.len())
    }

    fn visit_uniques(&self, mut visit: impl FnMut(&[u8])) {
        let storage = self.storage();
        for index in 0..storage.value_count {
            visit(storage.get(index as u64));
        }
    }

    fn estimated_memory_size(&self) -> usize {
        Interner::estimated_memory_size(self)
    }

    fn write_dict<D: DataType<T = FixedLenByteArray>>(&self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(&self.storage().page))
    }

    fn into_dict<D: DataType<T = FixedLenByteArray>>(self) -> Result<Bytes> {
        Ok(Bytes::from(self.into_inner().page))
    }
}

impl DictionaryValue for FixedLenByteArray {
    type Storage = Interner<FixedLenByteArrayDictionaryStorage>;
}

/// Borrowed fixed-length byte-array values.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PackedFixedLenByteArrayBatch<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) type_length: usize,
    pub(super) len: usize,
}

impl<'a> PackedFixedLenByteArrayBatch<'a> {
    pub(crate) fn new(bytes: &'a [u8], type_length: usize, len: usize) -> Self {
        assert_eq!(
            bytes.len(),
            type_length
                .checked_mul(len)
                .expect("fixed-length byte-array values length overflow")
        );
        Self {
            bytes,
            type_length,
            len,
        }
    }

    pub(crate) fn iter(self) -> impl ExactSizeIterator<Item = &'a [u8]> {
        (0..self.len).map(move |idx| {
            let start = idx * self.type_length;
            &self.bytes[start..start + self.type_length]
        })
    }
}

/// Encoder extension for raw fixed-length byte-array values.
#[doc(hidden)]
pub(crate) trait FixedLenByteArrayEncoder: Encoder<FixedLenByteArrayType> {
    fn put_fixed_len_byte_array_batch(
        &mut self,
        values: PackedFixedLenByteArrayBatch<'_>,
    ) -> Result<()> {
        for value in values.iter() {
            let value = FixedLenByteArray::from(ByteArray::from(value.to_vec()));
            self.put(std::slice::from_ref(&value))?;
        }
        Ok(())
    }

    /// Reserve room for `additional_bytes` of appended fixed-length values.
    fn reserve_fixed_len(&mut self, _additional_bytes: usize) {}

    /// Append one fixed-length value to the encoder.
    fn append_fixed_len_value(&mut self, value: &[u8]) -> Result<()> {
        self.put_fixed_len_byte_array_batch(PackedFixedLenByteArrayBatch::new(
            value,
            value.len(),
            1,
        ))
    }
}

impl FixedLenByteArrayEncoder for FixedLenByteArrayEncodingFamily {
    fn put_fixed_len_byte_array_batch(
        &mut self,
        values: PackedFixedLenByteArrayBatch<'_>,
    ) -> Result<()> {
        match self {
            Self::Dictionary(_) => {
                unreachable!("dictionary variant is not routed through the fallback encoder")
            }
            Self::Plain(e) => e.put_fixed_len_byte_array_batch(values),
            Self::DeltaByteArray(e) => e.put_fixed_len_byte_array_batch(values),
            Self::ByteStreamSplit(e) => e.put_fixed_len_byte_array_batch(values),
        }
    }

    fn reserve_fixed_len(&mut self, additional_bytes: usize) {
        match self {
            Self::Dictionary(_) => {
                unreachable!("dictionary variant is not routed through the fallback encoder")
            }
            Self::Plain(e) => e.reserve_fixed_len(additional_bytes),
            Self::DeltaByteArray(e) => e.reserve_fixed_len(additional_bytes),
            Self::ByteStreamSplit(e) => e.reserve_fixed_len(additional_bytes),
        }
    }

    fn append_fixed_len_value(&mut self, value: &[u8]) -> Result<()> {
        match self {
            Self::Dictionary(_) => {
                unreachable!("dictionary variant is not routed through the fallback encoder")
            }
            Self::Plain(e) => e.append_fixed_len_value(value),
            Self::DeltaByteArray(e) => e.append_fixed_len_value(value),
            Self::ByteStreamSplit(e) => e.append_fixed_len_value(value),
        }
    }
}
impl FixedLenByteArrayEncodingFamily {
    pub(super) fn from_encoding(encoding: Encoding, descr: &ColumnDescPtr) -> Result<Self> {
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainEncoder::new())),
            Encoding::DELTA_BYTE_ARRAY => Ok(Self::DeltaByteArray(Default::default())),
            Encoding::BYTE_STREAM_SPLIT => Ok(Self::ByteStreamSplit(
                VariableWidthByteStreamSplitEncoder::new(descr.type_length()),
            )),
            e => Err(unsupported_column_encoding(e, Type::FIXED_LEN_BYTE_ARRAY)),
        }
    }
}

impl FixedLenByteArrayEncoder for PlainEncoder<FixedLenByteArrayType> {
    #[inline]
    fn put_fixed_len_byte_array_batch(
        &mut self,
        values: PackedFixedLenByteArrayBatch<'_>,
    ) -> Result<()> {
        self.buffer.extend_from_slice(values.bytes);
        Ok(())
    }

    /// PLAIN stores fixed-length values back-to-back, so each streamed value is
    /// appended straight into the page buffer — one copy, no intermediate buffer.
    #[cfg(feature = "arrow")]
    #[inline]
    fn reserve_fixed_len(&mut self, additional_bytes: usize) {
        self.buffer.reserve(additional_bytes);
    }

    #[cfg(feature = "arrow")]
    #[inline]
    fn append_fixed_len_value(&mut self, value: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(value);
        Ok(())
    }
}
