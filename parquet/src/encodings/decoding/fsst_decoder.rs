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
use crate::encodings::fsst::{
    FSST_OFFSET_ENCODING_DELTA, FSST_OFFSET_ENCODING_PLAIN, SymbolTable,
};
use crate::errors::{ParquetError, Result};

/// Size, in bytes, of the FSST header (spec §4.4): `offset_encoding` (`u8`) +
/// `num_values` (`i32` LE) + `offset_array_length` (`i32` LE).
const FSST_HEADER_LEN: usize = 1 + 4 + 4;

/// Decoder for the [`FSST`](Encoding::FSST) encoding.
///
/// See [`FsstEncoder`](crate::encodings::encoding::fsst_encoder::FsstEncoder)
/// for the page layout. Only [`Type::BYTE_ARRAY`] is supported.
pub struct FsstDecoder<T: DataType> {
    /// Symbol table parsed from the page.
    table: SymbolTable,
    /// Data section: concatenated compressed values.
    data: Bytes,
    /// Cumulative end offsets into `data`, one per value (spec §4.5).
    end_offsets: Vec<i32>,
    /// Index of the next value to produce.
    cursor: usize,
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
            end_offsets: Vec::new(),
            cursor: 0,
            _phantom: PhantomData,
        }
    }

    /// Byte offset into the data section where the value at `cursor` starts:
    /// the end offset of the previous value, or `0` for the first.
    fn value_start(&self) -> usize {
        match self.cursor {
            0 => 0,
            i => self.end_offsets[i - 1] as usize,
        }
    }
}

impl<T: DataType> Decoder<T> for FsstDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        if T::get_physical_type() != Type::BYTE_ARRAY {
            return Err(general_err!("FsstDecoder only supports ByteArrayType"));
        }

        // Symbol-table body (spec §3.3), self-describing, at the front of the
        // page as an interim stand-in for the SYMBOL_TABLE_PAGE.
        let (table, table_len) = SymbolTable::deserialize(&data)?;

        // FSST header (spec §4.4).
        let header = data
            .get(table_len..table_len + FSST_HEADER_LEN)
            .ok_or_else(|| general_err!("FSST: truncated header"))?;
        let offset_encoding = header[0];
        let stored_num_values = i32::from_le_bytes(header[1..5].try_into().unwrap());
        let offset_array_length = i32::from_le_bytes(header[5..9].try_into().unwrap());
        if stored_num_values < 0 {
            return Err(general_err!("FSST: negative num_values {}", stored_num_values));
        }
        if offset_array_length < 0 {
            return Err(general_err!(
                "FSST: negative offset_array_length {}",
                offset_array_length
            ));
        }
        let stored_num_values = stored_num_values as usize;
        if stored_num_values > num_values {
            return Err(general_err!(
                "FSST: header claims {} values but page holds at most {}",
                stored_num_values,
                num_values
            ));
        }

        let offsets_start = table_len + FSST_HEADER_LEN;
        let offsets_end = offsets_start + offset_array_length as usize;
        if data.len() < offsets_end {
            return Err(general_err!("FSST: truncated offset array"));
        }

        // Offset array (spec §4.5): cumulative end offsets into the data
        // section, either plain little-endian i32 or Delta-Binary-Packed.
        let mut end_offsets = vec![0i32; stored_num_values];
        if stored_num_values > 0 {
            match offset_encoding {
                FSST_OFFSET_ENCODING_PLAIN => {
                    let expected = stored_num_values * 4;
                    if offset_array_length as usize != expected {
                        return Err(general_err!(
                            "FSST: plain offset array is {} bytes, expected {}",
                            offset_array_length,
                            expected
                        ));
                    }
                    for (i, chunk) in data[offsets_start..offsets_end].chunks_exact(4).enumerate() {
                        end_offsets[i] = i32::from_le_bytes(chunk.try_into().unwrap());
                    }
                }
                FSST_OFFSET_ENCODING_DELTA => {
                    let mut offset_decoder = DeltaBitPackDecoder::<Int32Type>::new();
                    offset_decoder.set_data(data.slice(offsets_start..offsets_end), stored_num_values)?;
                    offset_decoder.get(&mut end_offsets)?;
                }
                other => {
                    return Err(general_err!("FSST: unsupported offset encoding {}", other));
                }
            }
        }

        let data_section_len = data.len() - offsets_end;

        // Spec §4.5/§10.2: end offsets are cumulative, so they must be
        // non-negative, non-decreasing, and end exactly at the data section's
        // end; anything else is corruption.
        let mut prev = 0i32;
        for &end in &end_offsets {
            if end < prev {
                return Err(general_err!(
                    "FSST: offset array is not monotonically non-decreasing"
                ));
            }
            prev = end;
        }
        if prev as usize != data_section_len {
            return Err(general_err!(
                "FSST: last end offset {} does not match data section size {}",
                prev,
                data_section_len
            ));
        }

        self.table = table;
        self.data = data.slice(offsets_end..);
        self.end_offsets = end_offsets;
        self.cursor = 0;
        Ok(())
    }

    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let to_read = buffer.len().min(self.values_left());
        let mut decompressed = Vec::new();
        for item in buffer.iter_mut().take(to_read) {
            let start = self.value_start();
            let end = self.end_offsets[self.cursor] as usize;
            // start <= end <= data.len() was validated in set_data.
            decompressed.clear();
            self.table.decompress(&self.data[start..end], &mut decompressed)?;
            item.set_from_bytes(Bytes::copy_from_slice(&decompressed));
            self.cursor += 1;
        }
        Ok(to_read)
    }

    fn values_left(&self) -> usize {
        self.end_offsets.len() - self.cursor
    }

    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::FSST
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        // End offsets are cumulative, so skipping is O(1) (spec §10.2).
        let to_skip = num_values.min(self.values_left());
        self.cursor += to_skip;
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

    /// Empty symbol-table body (spec §3.3): symbol_count 0 + zeroed histogram.
    const EMPTY_SYMBOL_TABLE: [u8; 9] = [0; 9];

    /// Build a page body with an empty symbol table, a plain-encoded offset
    /// array, and the given data section.
    fn plain_page(end_offsets: &[i32], data_section: &[u8]) -> Bytes {
        let mut page = EMPTY_SYMBOL_TABLE.to_vec();
        page.push(0); // offset_encoding: PLAIN
        page.extend_from_slice(&(end_offsets.len() as i32).to_le_bytes());
        page.extend_from_slice(&((end_offsets.len() * 4) as i32).to_le_bytes());
        for end in end_offsets {
            page.extend_from_slice(&end.to_le_bytes());
        }
        page.extend_from_slice(data_section);
        page.into()
    }

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
    fn roundtrip_many_values_exercises_offset_array() {
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
    fn decodes_plain_offset_array() {
        // With an empty symbol table every byte is escaped: "a" -> FF 61,
        // "b" -> FF 62; end offsets [2, 4].
        let page = plain_page(&[2, 4], &[0xFF, b'a', 0xFF, b'b']);

        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        decoder.set_data(page, 2).unwrap();

        let mut out = vec![ByteArray::default(); 2];
        assert_eq!(decoder.get(&mut out).unwrap(), 2);
        assert_eq!(out, vec![ByteArray::from("a"), ByteArray::from("b")]);
    }

    #[test]
    fn rejects_unknown_offset_encoding() {
        let mut page = EMPTY_SYMBOL_TABLE.to_vec();
        page.push(99); // offset_encoding: reserved
        page.extend_from_slice(&1i32.to_le_bytes()); // num_values
        page.extend_from_slice(&4i32.to_le_bytes()); // offset_array_length
        page.extend_from_slice(&0i32.to_le_bytes()); // offset array

        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(page.into(), 1).is_err());
    }

    #[test]
    fn rejects_non_monotonic_offsets() {
        let page = plain_page(&[4, 2], &[0xFF, b'a', 0xFF, b'b']);
        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(page, 2).is_err());
    }

    #[test]
    fn rejects_offsets_not_covering_data_section() {
        // Last end offset (2) leaves 2 trailing bytes unaccounted for.
        let page = plain_page(&[2], &[0xFF, b'a', 0xFF, b'b']);
        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(page, 1).is_err());

        // Last end offset (6) points past the end of the data section.
        let page = plain_page(&[6], &[0xFF, b'a', 0xFF, b'b']);
        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(page, 1).is_err());
    }

    #[test]
    fn rejects_truncated_header() {
        let mut page = EMPTY_SYMBOL_TABLE.to_vec();
        page.push(0); // offset_encoding only; num_values/offset_array_length missing
        let mut decoder = FsstDecoder::<ByteArrayType>::new();
        assert!(decoder.set_data(page.into(), 0).is_err());
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
