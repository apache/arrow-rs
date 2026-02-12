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

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::decoding::Decoder;
use crate::errors::{ParquetError, Result};

const ALP_HEADER_SIZE: usize = 8;
const ALP_VERSION: u8 = 1;
const ALP_COMPRESSION_MODE: u8 = 0;
const ALP_INTEGER_ENCODING_FOR_BIT_PACK: u8 = 0;
const ALP_MAX_LOG_VECTOR_SIZE: u8 = 16;

#[derive(Debug, Clone, Copy)]
struct AlpHeader {
    version: u8,
    compression_mode: u8,
    integer_encoding: u8,
    log_vector_size: u8,
    num_elements: u32,
}

#[derive(Debug)]
struct AlpPageLayout {
    header: AlpHeader,
    offsets: Vec<u32>,
}

fn parse_alp_page_layout(data: &[u8]) -> Result<AlpPageLayout> {
    if data.len() < ALP_HEADER_SIZE {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for header, got {}",
            ALP_HEADER_SIZE,
            data.len()
        ));
    }

    let header = AlpHeader {
        version: data[0],
        compression_mode: data[1],
        integer_encoding: data[2],
        log_vector_size: data[3],
        num_elements: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
    };

    if header.version != ALP_VERSION {
        return Err(general_err!(
            "Invalid ALP page: unsupported version {}, expected {}",
            header.version,
            ALP_VERSION
        ));
    }

    if header.compression_mode != ALP_COMPRESSION_MODE {
        return Err(general_err!(
            "Invalid ALP page: unsupported compression mode {}",
            header.compression_mode
        ));
    }

    if header.integer_encoding != ALP_INTEGER_ENCODING_FOR_BIT_PACK {
        return Err(general_err!(
            "Invalid ALP page: unsupported integer encoding {}",
            header.integer_encoding
        ));
    }

    if header.log_vector_size > ALP_MAX_LOG_VECTOR_SIZE {
        return Err(general_err!(
            "Invalid ALP page: log_vector_size {} exceeds max {}",
            header.log_vector_size,
            ALP_MAX_LOG_VECTOR_SIZE
        ));
    }

    let vector_size = 1usize << header.log_vector_size;
    let num_vectors = if header.num_elements == 0 {
        0
    } else {
        (header.num_elements as usize).div_ceil(vector_size)
    };

    let offsets_len = num_vectors
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| general_err!("Invalid ALP page: offsets length overflow"))?;
    let offsets_end = ALP_HEADER_SIZE
        .checked_add(offsets_len)
        .ok_or_else(|| general_err!("Invalid ALP page: header + offsets length overflow"))?;

    if data.len() < offsets_end {
        return Err(general_err!(
            "Invalid ALP page: expected at least {} bytes for {} offsets, got {}",
            offsets_end,
            num_vectors,
            data.len()
        ));
    }

    let body_len = data.len() - ALP_HEADER_SIZE;
    let mut offsets = Vec::with_capacity(num_vectors);
    for i in 0..num_vectors {
        let start = ALP_HEADER_SIZE + i * 4;
        let offset = u32::from_le_bytes([
            data[start],
            data[start + 1],
            data[start + 2],
            data[start + 3],
        ]);
        if offset as usize >= body_len {
            return Err(general_err!(
                "Invalid ALP page: vector offset {} out of bounds for body length {}",
                offset,
                body_len
            ));
        }
        offsets.push(offset);
    }

    Ok(AlpPageLayout { header, offsets })
}

pub(crate) struct AlpDecoder<T: DataType> {
    num_values: usize,
    layout: Option<AlpPageLayout>,
    _marker: PhantomData<T>,
}

impl<T: DataType> AlpDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            num_values: 0,
            layout: None,
            _marker: PhantomData,
        }
    }
}

impl<T: DataType> Decoder<T> for AlpDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        let layout = parse_alp_page_layout(data.as_ref())?;
        if layout.header.num_elements as usize != num_values {
            return Err(general_err!(
                "Invalid ALP page: header num_elements {} does not match page num_values {}",
                layout.header.num_elements,
                num_values
            ));
        }

        self.num_values = num_values;
        self.layout = Some(layout);
        Ok(())
    }

    fn get(&mut self, _buffer: &mut [T::T]) -> Result<usize> {
        // Parsing succeeds in set_data; value decoding is introduced in a follow-up step.
        let num_vectors = self
            .layout
            .as_ref()
            .map(|layout| layout.offsets.len())
            .unwrap_or(0);
        Err(nyi_err!(
            "Encoding ALP page layout parsed ({} vectors), value decoding is not implemented",
            num_vectors
        ))
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::ALP
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let skipped = num_values.min(self.num_values);
        self.num_values -= skipped;
        Ok(skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_alp_page_bytes(
        version: u8,
        compression_mode: u8,
        integer_encoding: u8,
        log_vector_size: u8,
        num_elements: u32,
        offsets: &[u32],
        body_tail_len: usize,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(ALP_HEADER_SIZE + offsets.len() * 4 + body_tail_len);
        out.push(version);
        out.push(compression_mode);
        out.push(integer_encoding);
        out.push(log_vector_size);
        out.extend_from_slice(&num_elements.to_le_bytes());
        for offset in offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend(std::iter::repeat_n(0u8, body_tail_len));
        out
    }

    #[test]
    fn test_parse_alp_page_layout_valid() {
        // num_elements=4 with vector_size=4 -> one vector, one offset entry.
        let data = make_alp_page_bytes(1, 0, 0, 2, 4, &[4], 8);
        let parsed = parse_alp_page_layout(&data).unwrap();
        assert_eq!(parsed.header.version, 1);
        assert_eq!(parsed.header.num_elements, 4);
        assert_eq!(parsed.offsets, vec![4]);
    }

    #[test]
    fn test_parse_alp_page_layout_short_header() {
        let err = parse_alp_page_layout(&[0, 1, 2]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: expected at least 8 bytes for header")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_log_vector_size() {
        let data = make_alp_page_bytes(1, 0, 0, 17, 1, &[4], 8);
        let err = parse_alp_page_layout(&data).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: log_vector_size 17 exceeds max 16")
        );
    }

    #[test]
    fn test_parse_alp_page_layout_invalid_integer_encoding() {
        let data = make_alp_page_bytes(1, 0, 1, 2, 1, &[4], 8);
        let err = parse_alp_page_layout(&data).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid ALP page: unsupported integer encoding 1")
        );
    }
}
