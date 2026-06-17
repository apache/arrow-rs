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
use crate::data_type::DataType;
use crate::data_type::private::ParquetValueType;
use crate::encodings::decoding::Decoder;
use crate::encodings::fsst::{FSST_LENGTH_PREFIX_BYTES, SymbolTable};
use crate::errors::{ParquetError, Result};

/// Decoder for the [`FSST`](Encoding::FSST) encoding.
///
/// See [`FsstEncoder`](crate::encodings::encoding::fsst_encoder::FsstEncoder)
/// for the page layout. Only [`Type::BYTE_ARRAY`] is supported.
pub struct FsstDecoder<T: DataType> {
    /// Symbol table parsed from the page header.
    table: SymbolTable,
    /// Compressed payload following the symbol-table header.
    data: Bytes,
    /// Offset into `data` pointing at the next value's length prefix.
    offset: usize,
    /// Number of values still to be produced.
    num_values: usize,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for FsstDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> FsstDecoder<T> {
    /// Creates a new FSST decoder.
    pub fn new() -> Self {
        Self {
            table: SymbolTable::default(),
            data: Bytes::new(),
            offset: 0,
            num_values: 0,
            _phantom: PhantomData,
        }
    }

    /// Reads the little-endian `u32` length prefix at the current offset and
    /// advances past it, returning the length of the next compressed value.
    fn read_len(&mut self) -> Result<usize> {
        let end = self.offset + FSST_LENGTH_PREFIX_BYTES;
        let bytes = self
            .data
            .get(self.offset..end)
            .ok_or_else(|| general_err!("FSST: truncated length prefix"))?;
        let len = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
        self.offset = end;
        Ok(len)
    }
}

impl<T: DataType> Decoder<T> for FsstDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        if T::get_physical_type() != Type::BYTE_ARRAY {
            return Err(general_err!("FsstDecoder only supports ByteArrayType"));
        }
        let (table, consumed) = SymbolTable::deserialize(&data)?;
        self.table = table;
        self.data = data.slice(consumed..);
        self.offset = 0;
        self.num_values = num_values;
        Ok(())
    }

    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let to_read = buffer.len().min(self.num_values);
        let mut decompressed = Vec::new();
        for item in buffer.iter_mut().take(to_read) {
            let len = self.read_len()?;
            let end = self.offset + len;
            let compressed = self
                .data
                .get(self.offset..end)
                .ok_or_else(|| general_err!("FSST: truncated compressed value"))?;
            decompressed.clear();
            self.table.decompress(compressed, &mut decompressed)?;
            self.offset = end;
            item.set_from_bytes(Bytes::copy_from_slice(&decompressed));
        }
        self.num_values -= to_read;
        Ok(to_read)
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::FSST
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let to_skip = num_values.min(self.num_values);
        for _ in 0..to_skip {
            let len = self.read_len()?;
            self.offset += len;
        }
        self.num_values -= to_skip;
        Ok(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use crate::basic::Encoding;
    use crate::data_type::{ByteArray, ByteArrayType};
    use crate::encodings::decoding::Decoder;
    use crate::encodings::encoding::Encoder;
    use crate::encodings::encoding::fsst_encoder::FsstEncoder;

    use super::FsstDecoder;

    #[test]
    fn encode_decode_roundtrip() {
        let values: Vec<ByteArray> = vec![
            ByteArray::from("hello"),
            ByteArray::from("parquet"),
            ByteArray::from(""),
            ByteArray::from("fsst"),
        ];

        let mut encoder = FsstEncoder::<ByteArrayType>::new();
        encoder.put(&values).unwrap();
        assert_eq!(encoder.encoding(), Encoding::FSST);
        let buffer = encoder.flush_buffer().unwrap();

        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        decoder.set_data(buffer, values.len()).unwrap();
        assert_eq!(decoder.values_left(), values.len());

        let mut out = vec![ByteArray::default(); values.len()];
        let read = decoder.get(&mut out).unwrap();
        assert_eq!(read, values.len());
        assert_eq!(out, values);
        assert_eq!(decoder.values_left(), 0);
    }

    #[test]
    fn skip_values() {
        let values: Vec<ByteArray> =
            vec![ByteArray::from("a"), ByteArray::from("bb"), ByteArray::from("ccc")];

        let mut encoder = FsstEncoder::<ByteArrayType>::new();
        encoder.put(&values).unwrap();
        let buffer = encoder.flush_buffer().unwrap();

        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        decoder.set_data(buffer, values.len()).unwrap();

        assert_eq!(decoder.skip(2).unwrap(), 2);
        let mut out = vec![ByteArray::default(); 1];
        assert_eq!(decoder.get(&mut out).unwrap(), 1);
        assert_eq!(out[0], values[2]);
    }
}
