// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use bytes::Bytes;

use super::dict_encoder::{DictionaryStorage, DictionaryValue};
use crate::data_type::{ByteArray, DataType};
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;

mod interner;
pub(crate) use interner::ByteArrayInterner;

/// Byte-array dictionary values stored directly in their final PLAIN page.
///
/// Share the Arrow writer's cached-hash and short-value interning with physical
/// column writers, without changing dictionary IDs or copying the emitted page.
#[derive(Debug, Default)]
pub struct ByteArrayDictionaryStorage(ByteArrayInterner);

impl DictionaryStorage<ByteArray> for ByteArrayDictionaryStorage {
    fn new(_desc: &ColumnDescPtr) -> Self {
        Self::default()
    }

    #[inline]
    fn intern(&mut self, value: &ByteArray) -> Result<u64> {
        self.0.intern(value.data())
    }

    #[inline]
    fn intern_bytes(&mut self, bytes: &[u8], _make: impl FnOnce() -> ByteArray) -> Result<u64> {
        self.0.intern(bytes)
    }

    fn len_and_size(&self) -> (usize, usize) {
        (self.0.num_unique_values(), self.0.page.len())
    }

    fn visit_uniques(&self, mut visit: impl FnMut(&[u8])) {
        for value in self.0.iter() {
            visit(value);
        }
    }

    fn estimated_memory_size(&self) -> usize {
        self.0.estimated_memory_size()
    }

    fn write_dict<D: DataType<T = ByteArray>>(&self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(&self.0.page))
    }

    fn into_dict<D: DataType<T = ByteArray>>(self) -> Result<Bytes> {
        Ok(Bytes::from(self.0.page))
    }
}

impl DictionaryValue for ByteArray {
    type Storage = ByteArrayDictionaryStorage;
}
