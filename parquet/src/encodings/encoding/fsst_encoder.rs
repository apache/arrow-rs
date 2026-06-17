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

use std::marker::PhantomData;

use bytes::Bytes;

use crate::basic::{Encoding, Type};
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, DataType};
use crate::encodings::encoding::Encoder;
use crate::encodings::fsst::{FSST_LENGTH_PREFIX_BYTES, SymbolTable};
use crate::errors::{ParquetError, Result};

/// Encoder for the [`FSST`](Encoding::FSST) encoding.
///
/// Values are buffered until [`flush_buffer`](Encoder::flush_buffer), at which
/// point a [`SymbolTable`] is trained over them and each value is compressed.
///
/// The flushed page layout is:
/// 1. the serialized [`SymbolTable`] header, followed by
/// 2. for each value: a little-endian `u32` compressed length, then the
///    compressed bytes.
///
/// Only [`Type::BYTE_ARRAY`] is supported.
pub struct FsstEncoder<T: DataType> {
    /// Raw values buffered until the symbol table can be trained at flush time.
    values: Vec<ByteArray>,
    /// Running total of buffered raw bytes, for O(1) size estimates.
    buffered_bytes: usize,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for FsstEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> FsstEncoder<T> {
    /// Creates a new FSST encoder.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            buffered_bytes: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for FsstEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        if T::get_physical_type() != Type::BYTE_ARRAY {
            return Err(general_err!("FsstEncoder only supports ByteArrayType"));
        }
        self.values.reserve(values.len());
        for value in values {
            let byte_array = value
                .as_any()
                .downcast_ref::<ByteArray>()
                .ok_or_else(|| general_err!("FsstEncoder only supports ByteArrayType"))?;
            self.buffered_bytes += byte_array.len();
            self.values.push(byte_array.clone());
        }
        Ok(())
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::FSST
    }

    fn estimated_data_encoded_size(&self) -> usize {
        // Loose estimate: the raw buffered bytes. The trained codec typically
        // compresses below this, but the symbol table is only built at flush.
        self.buffered_bytes
    }

    fn estimated_memory_size(&self) -> usize {
        self.buffered_bytes
            + self.values.len() * std::mem::size_of::<ByteArray>()
            + std::mem::size_of::<Self>()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        if T::get_physical_type() != Type::BYTE_ARRAY {
            return Err(general_err!("FsstEncoder only supports ByteArrayType"));
        }
        // Parquet counts values per page with an i32, so the encoder cannot
        // produce more than that in a single flush.
        if self.values.len() > i32::MAX as usize {
            return Err(general_err!(
                "FSST can encode at most i32::MAX values, got {}",
                self.values.len()
            ));
        }

        let table = SymbolTable::train(self.values.iter().map(|v| v.data()));

        // Pre-size to the known header layout plus a per-value length prefix; the
        // compressed payload usually fits within the buffered raw size.
        let capacity = table.serialized_size()
            + self.values.len() * FSST_LENGTH_PREFIX_BYTES
            + self.buffered_bytes;
        let mut out = Vec::with_capacity(capacity);
        table.serialize(&mut out);

        let mut compressed = Vec::new();
        for value in &self.values {
            compressed.clear();
            table.compress(value.data(), &mut compressed);
            out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
        }

        self.values.clear();
        self.buffered_bytes = 0;
        Ok(out.into())
    }
}
