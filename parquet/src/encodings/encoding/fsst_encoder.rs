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
use crate::encodings::fsst::{FSST_OFFSET_ENCODING_DELTA, SymbolTable};
use crate::errors::{ParquetError, Result};

/// Encoder for the [`FSST`](Encoding::FSST) encoding.
///
/// Values are buffered until [`flush_buffer`](Encoder::flush_buffer), at which
/// point a [`SymbolTable`] is trained over them and each value is compressed.
///
/// The flushed page body follows the FSST spec proposal:
/// 1. the symbol-table body (spec §3.3). The spec places this in a dedicated
///    `SYMBOL_TABLE_PAGE` shared by all data pages of a column chunk; until
///    that page type is plumbed through, it is emitted at the front of each
///    data page as a self-describing interim stand-in;
/// 2. the FSST header (spec §4.4): `offset_encoding` (`u8`), `num_values`
///    (`i32` LE), `offset_array_length` (`i32` LE);
/// 3. the offset array (spec §4.5): cumulative end offsets into the data
///    section, Delta-Binary-Packed; and
/// 4. the data section: concatenated compressed values.
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
        // The FSST header stores num_values as an i32 (spec §4.4).
        if self.values.len() > i32::MAX as usize {
            return Err(general_err!(
                "FSST can encode at most i32::MAX values, got {}",
                self.values.len()
            ));
        }

        let table = SymbolTable::train(self.values.iter().map(|v| v.data()));

        // Compress each value, concatenating the data section and collecting
        // the cumulative end offset of each value (spec §4.5).
        let mut data_section = Vec::with_capacity(self.buffered_bytes);
        let mut end_offsets: Vec<i32> = Vec::with_capacity(self.values.len());
        let mut compressed = Vec::new();
        for value in &self.values {
            compressed.clear();
            table.compress(value.data(), &mut compressed);
            data_section.extend_from_slice(&compressed);
            // The data section is bounded to i32::MAX bytes (spec §4.6), so
            // every end offset fits in an i32.
            let end: i32 = data_section
                .len()
                .try_into()
                .map_err(|_| general_err!("FSST: data section exceeds i32::MAX bytes"))?;
            end_offsets.push(end);
        }

        // Offset array, Delta-Binary-Packed.
        let mut offset_encoder = DeltaBitPackEncoder::<Int32Type>::new();
        offset_encoder.put(&end_offsets)?;
        let offset_bytes = offset_encoder.flush_buffer()?;
        let offset_array_length: i32 = offset_bytes
            .len()
            .try_into()
            .map_err(|_| general_err!("FSST: offset array exceeds i32::MAX bytes"))?;

        // Symbol-table body (interim in-page placement, see type-level docs).
        let mut symbol_bytes = Vec::with_capacity(table.serialized_size());
        table.serialize(&mut symbol_bytes);

        let mut out = Vec::with_capacity(
            symbol_bytes.len() + 9 + offset_bytes.len() + data_section.len(),
        );
        out.extend_from_slice(&symbol_bytes);
        // FSST header (spec §4.4).
        out.push(FSST_OFFSET_ENCODING_DELTA);
        out.extend_from_slice(&(self.values.len() as i32).to_le_bytes());
        out.extend_from_slice(&offset_array_length.to_le_bytes());
        out.extend_from_slice(&offset_bytes);
        out.extend_from_slice(&data_section);

        self.values.clear();
        self.buffered_bytes = 0;
        Ok(out.into())
    }
}
