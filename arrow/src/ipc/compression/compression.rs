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

use crate::error::Result;
use crate::ipc::CompressionType;
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionCodecType {
    NoCompression,
    Lz4Frame,
    ZSTD,
}

impl From<CompressionType> for CompressionCodecType {
    fn from(compression_type: CompressionType) -> Self {
        match compression_type {
            CompressionType::ZSTD => CompressionCodecType::ZSTD,
            CompressionType::LZ4_FRAME => CompressionCodecType::Lz4Frame,
            _ => CompressionCodecType::NoCompression,
        }
    }
}

impl Into<CompressionType> for CompressionCodecType {
    fn into(self) -> CompressionType {
        match self {
            CompressionCodecType::NoCompression => CompressionType(-1),
            CompressionCodecType::Lz4Frame => CompressionType::LZ4_FRAME,
            CompressionCodecType::ZSTD => CompressionType::ZSTD,
        }
    }
}

impl CompressionCodecType {
    pub fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        match self {
            CompressionCodecType::Lz4Frame => {
                let mut encoder = lz4::EncoderBuilder::new().build(output).unwrap();
                encoder.write_all(input).unwrap();
                encoder.finish().1.unwrap();
                Ok(())
            }
            CompressionCodecType::ZSTD => {
                let mut encoder = zstd::Encoder::new(output, 0).unwrap();
                encoder.write_all(input).unwrap();
                encoder.finish().unwrap();
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<usize> {
        let result: Result<usize> = match self {
            CompressionCodecType::Lz4Frame => {
                let mut decoder = lz4::Decoder::new(input)?;
                let size = decoder.read_to_end(output).unwrap();
                Ok(size)
            }
            CompressionCodecType::ZSTD => {
                let mut decoder = zstd::Decoder::new(input)?;
                let size = decoder.read_to_end(output).unwrap();
                Ok(size)
            }
            _ => Ok(input.len()),
        };
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::ipc::compression::compression::CompressionCodecType;

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
        let codec: CompressionCodecType = CompressionCodecType::ZSTD;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec.compress(input_bytes, &mut output_bytes).unwrap();
        let mut result_output_bytes: Vec<u8> = Vec::new();
        codec
            .decompress(output_bytes.as_slice(), &mut result_output_bytes)
            .unwrap();
        assert_eq!(input_bytes, result_output_bytes.as_slice());
    }
}
