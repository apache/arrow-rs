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
use crate::data_type::{ByteArray, DataType, Int32Type};
use crate::encodings::encoding::{DeltaBitPackEncoder, Encoder};
use crate::encodings::fsst::{FSST_LENGTH_ENCODING_DELTA, SymbolTable, write_uleb128};
use crate::errors::{ParquetError, Result};

/// Encoder for the [`FSST`](Encoding::FSST) encoding.
///
/// Values are buffered until [`flush_buffer`](Encoder::flush_buffer), at which
/// point a [`SymbolTable`] is trained over them and each value is compressed.
///
/// The flushed page has four contiguous sections:
/// 1. a header of three ULEB128 values — the length-array encoding id, the
///    length-array byte size, and the symbol-table byte size;
/// 2. the per-value compressed lengths, Delta-Binary-Packed;
/// 3. the serialized [`SymbolTable`]; and
/// 4. the concatenated compressed values (boundaries come from the lengths).
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

        // Compress each value, concatenating the payload and collecting the
        // per-value compressed lengths for the length array.
        let mut payload = Vec::with_capacity(self.buffered_bytes);
        let mut lengths: Vec<i32> = Vec::with_capacity(self.values.len());
        let mut compressed = Vec::new();
        for value in &self.values {
            compressed.clear();
            table.compress(value.data(), &mut compressed);
            lengths.push(compressed.len() as i32);
            payload.extend_from_slice(&compressed);
        }

        // Section 2: length array, Delta-Binary-Packed.
        let mut length_encoder = DeltaBitPackEncoder::<Int32Type>::new();
        length_encoder.put(&lengths)?;
        let length_bytes = length_encoder.flush_buffer()?;

        // Section 3: symbol table.
        let mut symbol_bytes = Vec::with_capacity(table.serialized_size());
        table.serialize(&mut symbol_bytes);

        // Section 1 (header) + sections 2-4, contiguous.
        let mut out =
            Vec::with_capacity(8 + length_bytes.len() + symbol_bytes.len() + payload.len());
        write_uleb128(&mut out, FSST_LENGTH_ENCODING_DELTA as u64);
        write_uleb128(&mut out, length_bytes.len() as u64);
        write_uleb128(&mut out, symbol_bytes.len() as u64);
        out.extend_from_slice(&length_bytes);
        out.extend_from_slice(&symbol_bytes);
        out.extend_from_slice(&payload);

        self.values.clear();
        self.buffered_bytes = 0;
        Ok(out.into())
    }
}
