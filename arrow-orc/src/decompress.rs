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

//! Handle generic decompression of ORC files.

use std::io::Read;

use crate::errors::{OrcError, Result};
use crate::proto;

/// Supported generic compression types.
/// Compression block size indicates maximum size of each compression chunk.
/// No chunk will decompress to larger than thus block size.
// TODO: use compression block size for other variants too
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum CompressionType {
    Lz4 { compression_block_size: u64 },
    Lzo,
    Snappy,
    Zlib,
    Zstd,
}

impl CompressionType {
    pub fn from_proto(
        value: proto::CompressionKind,
        compression_block_size: Option<u64>,
    ) -> Result<Option<Self>> {
        let ct = match (value, compression_block_size) {
            (proto::CompressionKind::None, None) => None,
            (proto::CompressionKind::Zlib, Some(_size)) => Some(CompressionType::Zlib),
            (proto::CompressionKind::Snappy, Some(_size)) => Some(CompressionType::Snappy),
            (proto::CompressionKind::Lzo, Some(_size)) => Some(CompressionType::Lzo),
            (proto::CompressionKind::Lz4, Some(compression_block_size)) => {
                Some(CompressionType::Lz4 {
                    compression_block_size,
                })
            }
            (proto::CompressionKind::Zstd, Some(_size)) => Some(CompressionType::Zstd),
            _ => {
                return Err(OrcError::Corrupted(
                    "Invalid compression settings".to_string(),
                ))
            }
        };
        Ok(ct)
    }
}

/// ORC files are compressed in blocks, with a 3 byte header at the start
/// of these blocks indicating the length of the block and whether it's
/// compressed or not.
fn decode_header(bytes: [u8; 3]) -> CompressionHeader {
    let bytes = [bytes[0], bytes[1], bytes[2], 0];
    let length = u32::from_le_bytes(bytes);
    let is_original = length & 1 == 1;
    // to clear the is_original bit
    let length = length >> 1;
    if is_original {
        CompressionHeader::Original(length)
    } else {
        CompressionHeader::Compressed(length)
    }
}

/// Indicates length of block and whether it's compressed or not.
#[derive(Debug, PartialEq, Eq)]
enum CompressionHeader {
    Original(u32),
    Compressed(u32),
}

/// Use to decompress a reader of bytes, according to ORC specification:
///
/// - Bytes are grouped into blocks
/// - Each block has a 3 byte header, indicating length of block and if
///   the block is compressed or the uncompressed original bytes
pub struct Decompressor<R: Read> {
    reader: R,
    decompressed_block: Vec<u8>,
    block_start_index: usize,
    compression_type: CompressionType,
}

impl<R: Read> Decompressor<R> {
    pub fn new(reader: R, compression_type: CompressionType) -> Self {
        Self {
            reader,
            decompressed_block: vec![],
            block_start_index: 0,
            compression_type,
        }
    }

    fn process_compressed_block(&mut self, compressed_block: &[u8]) -> Result<()> {
        self.decompressed_block.clear();
        match self.compression_type {
            CompressionType::Lzo => {
                let decompressed = lzokay_native::decompress_all(compressed_block, None)?;
                self.decompressed_block.extend(decompressed);
            }
            CompressionType::Lz4 {
                compression_block_size,
            } => {
                let decompressed =
                    lz4_flex::block::decompress(compressed_block, compression_block_size as usize)?;
                self.decompressed_block.extend(decompressed);
            }
            CompressionType::Snappy => {
                let len = snap::raw::decompress_len(compressed_block)?;
                self.decompressed_block.resize(len, 0);
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress(compressed_block, &mut self.decompressed_block)?;
            }
            CompressionType::Zlib => {
                let mut reader = flate2::read::DeflateDecoder::new(compressed_block);
                reader.read_to_end(&mut self.decompressed_block)?;
            }
            CompressionType::Zstd => {
                zstd::stream::copy_decode(compressed_block, &mut self.decompressed_block)?;
            }
        };
        Ok(())
    }
}

impl<R: Read> Read for Decompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // if finished copying from decompressed_block
        // grab next block
        if self.block_start_index >= self.decompressed_block.len() {
            let mut header = [0; 3];
            let size = self.reader.read(&mut header[..1])?;
            if size == 0 {
                // exhausted
                return Ok(0);
            }
            // otherwise get other header bytes
            self.reader.read_exact(&mut header[1..])?;

            match decode_header(header) {
                CompressionHeader::Original(len) => {
                    self.decompressed_block.resize(len as usize, 0);
                    self.reader.read_exact(&mut self.decompressed_block)?;
                }
                CompressionHeader::Compressed(len) => {
                    let mut compressed = vec![0; len as usize];
                    self.reader.read_exact(&mut compressed)?;
                    self.process_compressed_block(&compressed)?;
                }
            };
            self.block_start_index = 0;
        }

        // copy out the decompressed bytes
        let bytes_written = buf
            .len()
            .min(self.decompressed_block.len() - self.block_start_index);
        let end = self.block_start_index + bytes_written;
        buf[..bytes_written].copy_from_slice(&self.decompressed_block[self.block_start_index..end]);
        self.block_start_index = end;
        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_header() {
        let header = [0x40, 0x0d, 0x03];
        let actual = decode_header(header);
        let expected = CompressionHeader::Compressed(100_000);
        assert_eq!(expected, actual);

        let header = [0x0b, 0x00, 0x00];
        let actual = decode_header(header);
        let expected = CompressionHeader::Original(5);
        assert_eq!(expected, actual);
    }
}
