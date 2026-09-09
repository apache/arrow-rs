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

use super::dict_encoder::{DictionaryStorage, DictionaryValue};
use crate::data_type::private::byte_array_length;
use crate::data_type::{ByteArray, DataType};
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use crate::util::interner::{Interner, Storage};

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
