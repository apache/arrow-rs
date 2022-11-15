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

//! Bloom filter implementation specific to Parquet, as described
//! in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md)

use crate::errors::ParquetError;
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::reader::ChunkReader;
use crate::format::{
    BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash, BloomFilterHeader,
};
use bytes::Buf;
use std::hash::Hasher;
use std::sync::Arc;
use thrift::protocol::{TCompactInputProtocol, TSerializable};
use twox_hash::XxHash64;

/// Salt as defined in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md#technical-approach)
const SALT: [u32; 8] = [
    0x47b6137b_u32,
    0x44974d91_u32,
    0x8824ad5b_u32,
    0xa2b7289d_u32,
    0x705495c7_u32,
    0x2df1424b_u32,
    0x9efc4947_u32,
    0x5c6bfb31_u32,
];

/// Each block is 256 bits, broken up into eight contiguous "words", each consisting of 32 bits.
/// Each word is thought of as an array of bits; each bit is either "set" or "not set".
type Block = [u32; 8];

/// takes as its argument a single unsigned 32-bit integer and returns a block in which each
/// word has exactly one bit set.
fn mask(x: u32) -> Block {
    let mut result = [0_u32; 8];
    for i in 0..8 {
        // wrapping instead of checking for overflow
        let y = x.wrapping_mul(SALT[i]);
        let y = y >> 27;
        result[i] = 1 << y;
    }
    result
}

/// setting every bit in the block that was also set in the result from mask
fn block_insert(block: &mut Block, hash: u32) {
    let mask = mask(hash);
    for i in 0..8 {
        block[i] |= mask[i];
    }
}

/// returns true when every bit that is set in the result of mask is also set in the block.
fn block_check(block: &Block, hash: u32) -> bool {
    let mask = mask(hash);
    for i in 0..8 {
        if block[i] & mask[i] == 0 {
            return false;
        }
    }
    true
}

/// A split block Bloom filter
pub struct Sbbf(Vec<Block>);

const SBBF_HEADER_SIZE_ESTIMATE: usize = 20;

/// given an initial offset, and a chunk reader, try to read out a bloom filter header by trying
/// one or more iterations, returns both the header and the offset after it (for bitset).
fn chunk_read_bloom_filter_header_and_offset<R: ChunkReader>(
    offset: u64,
    reader: Arc<R>,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let buffer = reader.get_bytes(offset as u64, SBBF_HEADER_SIZE_ESTIMATE)?;
    let mut buf_reader = buffer.reader();
    let mut prot = TCompactInputProtocol::new(&mut buf_reader);
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot).map_err(|e| {
        ParquetError::General(format!("Could not read bloom filter header: {}", e))
    })?;
    let bitset_offset =
        offset + (SBBF_HEADER_SIZE_ESTIMATE - buf_reader.into_inner().remaining()) as u64;
    Ok((header, bitset_offset))
}

impl Sbbf {
    fn new(bitset: &[u8]) -> Self {
        let data = bitset
            .chunks_exact(4 * 8)
            .map(|chunk| {
                let mut block = [0_u32; 8];
                for (i, word) in chunk.chunks_exact(4).enumerate() {
                    block[i] = u32::from_le_bytes(word.try_into().unwrap());
                }
                block
            })
            .collect::<Vec<Block>>();
        Self(data)
    }

    pub fn read_from_column_chunk<R: ChunkReader>(
        column_metadata: &ColumnChunkMetaData,
        reader: Arc<R>,
    ) -> Result<Option<Self>, ParquetError> {
        let offset: u64 = if let Some(offset) = column_metadata.bloom_filter_offset() {
            offset.try_into().map_err(|_| {
                ParquetError::General("Bloom filter offset is invalid".to_string())
            })?
        } else {
            return Ok(None);
        };

        let (header, bitset_offset) =
            chunk_read_bloom_filter_header_and_offset(offset, reader.clone())?;

        match header.algorithm {
            BloomFilterAlgorithm::BLOCK(_) => {
                // this match exists to future proof the singleton algorithm enum
            }
        }
        match header.compression {
            BloomFilterCompression::UNCOMPRESSED(_) => {
                // this match exists to future proof the singleton compression enum
            }
        }
        match header.hash {
            BloomFilterHash::XXHASH(_) => {
                // this match exists to future proof the singleton hash enum
            }
        }
        // length in bytes
        let length: usize = header.num_bytes.try_into().map_err(|_| {
            ParquetError::General("Bloom filter length is invalid".to_string())
        })?;
        let bitset = reader.get_bytes(bitset_offset, length)?;
        Ok(Some(Self::new(&bitset)))
    }

    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        // unchecked_mul is unstable, but in reality this is safe, we'd just use saturating mul
        // but it will not saturate
        (((hash >> 32).saturating_mul(self.0.len() as u64)) >> 32) as usize
    }

    /// Insert a hash into the filter
    pub fn insert(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        let block = &mut self.0[block_index];
        block_insert(block, hash as u32);
    }

    /// Check if a hash is in the filter. May return
    /// true for values that was never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    pub fn check(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        let block = &self.0[block_index];
        block_check(block, hash as u32)
    }
}

// per spec we use xxHash with seed=0
const SEED: u64 = 0;

pub fn hash_bytes<A: AsRef<[u8]>>(value: A) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(value.as_ref());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_bytes(b""), 17241709254077376921);
    }

    #[test]
    fn test_mask_set_quick_check() {
        for i in 0..1_000_000 {
            let result = mask(i);
            assert!(result.iter().all(|&x| x.count_ones() == 1));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000_000 {
            let mut block = [0_u32; 8];
            block_insert(&mut block, i);
            assert!(block_check(&block, i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = Sbbf(vec![[0_u32; 8]; 1_000]);
        for i in 0..1_000_000 {
            sbbf.insert(i);
            assert!(sbbf.check(i));
        }
    }

    #[test]
    fn test_with_fixture() {
        // bloom filter produced by parquet-mr/spark for a column of i64 f"a{i}" for i in 0..10
        let bitset: &[u8] = &[
            200, 1, 80, 20, 64, 68, 8, 109, 6, 37, 4, 67, 144, 80, 96, 32, 8, 132, 43,
            33, 0, 5, 99, 65, 2, 0, 224, 44, 64, 78, 96, 4,
        ];
        let sbbf = Sbbf::new(bitset);
        for a in 0..10i64 {
            let value = format!("a{}", a);
            let hash = hash_bytes(value);
            assert!(sbbf.check(hash));
        }
    }
}
