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
use crate::data_type::{DataType, Int32Type};
use crate::encodings::decoding::{Decoder, DeltaBitPackDecoder};
use crate::encodings::fsst::{FSST_LENGTH_ENCODING_DELTA, SymbolTable, read_uleb128};
use crate::errors::{ParquetError, Result};

/// Decoder for the [`FSST`](Encoding::FSST) encoding.
///
/// See [`FsstEncoder`](crate::encodings::encoding::fsst_encoder::FsstEncoder)
/// for the page layout. Only [`Type::BYTE_ARRAY`] is supported.
pub struct FsstDecoder<T: DataType> {
    /// Symbol table parsed from the page.
    table: SymbolTable,
    /// Concatenated compressed payload (section 4).
    data: Bytes,
    /// Per-value compressed lengths (section 2), decoded up front.
    lengths: Vec<i32>,
    /// Index of the next value to produce.
    cursor: usize,
    /// Byte offset into `data` of the value at `cursor`.
    offset: usize,
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
            lengths: Vec::new(),
            cursor: 0,
            offset: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Decoder<T> for FsstDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        if T::get_physical_type() != Type::BYTE_ARRAY {
            return Err(general_err!("FsstDecoder only supports ByteArrayType"));
        }

        // Section 1: header.
        let mut pos = 0;
        let length_encoding = read_uleb128(&data, &mut pos)?;
        if length_encoding != FSST_LENGTH_ENCODING_DELTA as u64 {
            return Err(general_err!(
                "FSST: unsupported length-array encoding {length_encoding}"
            ));
        }
        let length_size = read_uleb128(&data, &mut pos)? as usize;
        let symbol_size = read_uleb128(&data, &mut pos)? as usize;

        let length_end = pos + length_size;
        let symbol_end = length_end + symbol_size;
        let symbol_bytes = data
            .get(length_end..symbol_end)
            .ok_or_else(|| general_err!("FSST: truncated symbol table"))?;

        // Section 2: length array, Delta-Binary-Packed.
        let mut lengths = vec![0i32; num_values];
        if num_values > 0 {
            let length_bytes = data
                .slice(pos..length_end);
            let mut length_decoder = DeltaBitPackDecoder::<Int32Type>::new();
            length_decoder.set_data(length_bytes, num_values)?;
            length_decoder.get(&mut lengths)?;
        }

        // Section 3: symbol table.
        let (table, _) = SymbolTable::deserialize(symbol_bytes)?;

        // Section 4: compressed payload.
        self.table = table;
        self.data = data.slice(symbol_end..);
        self.lengths = lengths;
        self.cursor = 0;
        self.offset = 0;
        Ok(())
    }

    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let to_read = buffer.len().min(self.lengths.len() - self.cursor);
        let mut decompressed = Vec::new();
        for item in buffer.iter_mut().take(to_read) {
            let len = self.lengths[self.cursor] as usize;
            let end = self.offset + len;
            let compressed = self
                .data
                .get(self.offset..end)
                .ok_or_else(|| general_err!("FSST: truncated compressed value"))?;
            decompressed.clear();
            self.table.decompress(compressed, &mut decompressed)?;
            item.set_from_bytes(Bytes::copy_from_slice(&decompressed));
            self.offset = end;
            self.cursor += 1;
        }
        Ok(to_read)
    }

    fn values_left(&self) -> usize {
        self.lengths.len() - self.cursor
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::FSST
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let to_skip = num_values.min(self.lengths.len() - self.cursor);
        for _ in 0..to_skip {
            self.offset += self.lengths[self.cursor] as usize;
            self.cursor += 1;
        }
        Ok(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

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
    fn roundtrip_many_values_exercises_length_array() {
        let values: Vec<ByteArray> = (0..1000)
            .map(|i| ByteArray::from(format!("https://example.com/item/{i}").as_str()))
            .collect();

        let mut encoder = FsstEncoder::<ByteArrayType>::new();
        encoder.put(&values).unwrap();
        let buffer = encoder.flush_buffer().unwrap();

        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        decoder.set_data(buffer, values.len()).unwrap();

        let mut out = vec![ByteArray::default(); values.len()];
        assert_eq!(decoder.get(&mut out).unwrap(), values.len());
        assert_eq!(out, values);
    }

    #[test]
    fn rejects_unknown_length_encoding() {
        // A header whose first ULEB128 (length-array encoding id) is not the
        // expected Delta-Binary-Packed id must be rejected.
        let bogus = Bytes::from_static(&[99, 0, 0]);
        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(bogus, 0).is_err());
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
