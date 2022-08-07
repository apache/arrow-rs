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

use crate::error::{ArrowError, Result};
use crate::ipc::CompressionType;
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionCodecType {
    Lz4Frame,
    Zstd,
}

impl TryFrom<CompressionType> for CompressionCodecType {
    fn try_from(compression_type: CompressionType) -> Self {
        match compression_type {
            CompressionType::ZSTD => CompressionCodecType::Zstd,
            CompressionType::LZ4_FRAME => CompressionCodecType::Lz4Frame,
            other_type => {
                return ArrowError::InvalidArgumentError(format!(
                    "compression type {:?} not supported ",
                    compression_type
                ))
            }
        }
    }
}

impl From<CompressionCodecType> for CompressionType {
    fn from(codec: CompressionCodecType) -> Self {
        match codec {
            CompressionCodecType::Lz4Frame => CompressionType::LZ4_FRAME,
            CompressionCodecType::Zstd => CompressionType::ZSTD,
        }
    }
}

impl CompressionCodecType {
    pub fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        match self {
            CompressionCodecType::Lz4Frame => {
                let mut encoder = lz4::EncoderBuilder::new().build(output)?;
                encoder.write_all(input)?;
                match encoder.finish().1 {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                }
            }
            CompressionCodecType::Zstd => {
                let mut encoder = zstd::Encoder::new(output, 0)?;
                encoder.write_all(input)?;
                match encoder.finish() {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    pub fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<usize> {
        let result: Result<usize> = match self {
            CompressionCodecType::Lz4Frame => {
                let mut decoder = lz4::Decoder::new(input)?;
                match decoder.read_to_end(output) {
                    Ok(size) => Ok(size),
                    Err(e) => Err(e.into()),
                }
            }
            CompressionCodecType::Zstd => {
                let mut decoder = zstd::Decoder::new(input)?;
                match decoder.read_to_end(output) {
                    Ok(size) => Ok(size),
                    Err(e) => Err(e.into()),
                }
            }
        };
        result
    }
}

/// Get the uncompressed length
/// Notes:
///   -1: indicate that the data that follows is not compressed
///    0: indicate that there is no data
///   positive number: indicate the uncompressed length for the following data
#[inline]
fn read_uncompressed_size(buffer: &[u8]) -> i64 {
    let len_buffer = &buffer[0..8];
    // 64-bit little-endian signed integer
    i64::from_le_bytes(len_buffer.try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_compression() {
        let input_bytes = "hello lz4".as_bytes();
        let codec: CompressionCodecType = CompressionCodecType::Lz4Frame;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec.compress(input_bytes, &mut output_bytes).unwrap();
        let mut result_output_bytes: Vec<u8> = Vec::new();
        codec
            .decompress(output_bytes.as_slice(), &mut result_output_bytes)
            .unwrap();
        assert_eq!(input_bytes, result_output_bytes.as_slice());
    }

    #[test]
    fn test_zstd_compression() {
        let input_bytes = "hello zstd".as_bytes();
        let codec: CompressionCodecType = CompressionCodecType::Zstd;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec.compress(input_bytes, &mut output_bytes).unwrap();
        let mut result_output_bytes: Vec<u8> = Vec::new();
        codec
            .decompress(output_bytes.as_slice(), &mut result_output_bytes)
            .unwrap();
        assert_eq!(input_bytes, result_output_bytes.as_slice());
    }
}
