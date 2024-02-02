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

use arrow_schema::ArrowError;
use flate2::read;
use std::io;
use std::io::Read;

/// The metadata key used for storing the JSON encoded [`CompressionCodec`]
pub const CODEC_METADATA_KEY: &str = "avro.codec";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CompressionCodec {
    Deflate,
    Snappy,
    ZStandard,
}

impl CompressionCodec {
    pub(crate) fn decompress(&self, block: &[u8]) -> Result<Vec<u8>, ArrowError> {
        match self {
            #[cfg(feature = "deflate")]
            CompressionCodec::Deflate => {
                let mut decoder = read::DeflateDecoder::new(block);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            #[cfg(not(feature = "deflate"))]
            CompressionCodec::Deflate => Err(ArrowError::ParseError(
                "Deflate codec requires deflate feature".to_string(),
            )),
            #[cfg(feature = "snappy")]
            CompressionCodec::Snappy => {
                // Each compressed block is followed by the 4-byte, big-endian CRC32
                // checksum of the uncompressed data in the block.
                let crc = &block[block.len() - 4..];
                let block = &block[..block.len() - 4];

                let mut decoder = snap::raw::Decoder::new();
                let decoded = decoder
                    .decompress_vec(block)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                let checksum = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(&decoded);
                if checksum != u32::from_be_bytes(crc.try_into().unwrap()) {
                    return Err(ArrowError::ParseError("Snappy CRC mismatch".to_string()));
                }
                Ok(decoded)
            }
            #[cfg(not(feature = "snappy"))]
            CompressionCodec::Snappy => Err(ArrowError::ParseError(
                "Snappy codec requires snappy feature".to_string(),
            )),

            #[cfg(feature = "zstd")]
            CompressionCodec::ZStandard => {
                let mut decoder = zstd::Decoder::new(block)?;
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            #[cfg(not(feature = "zstd"))]
            CompressionCodec::ZStandard => Err(ArrowError::ParseError(
                "ZStandard codec requires zstd feature".to_string(),
            )),
        }
    }
}
