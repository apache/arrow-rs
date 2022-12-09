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
//! in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md).

use crate::data_type::AsBytes;
use crate::errors::ParquetError;
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::reader::ChunkReader;
use crate::format::{
    BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash, BloomFilterHeader,
    SplitBlockAlgorithm, Uncompressed, XxHash,
};
use bytes::{Buf, Bytes};
use std::hash::Hasher;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use thrift::protocol::{
    TCompactInputProtocol, TCompactOutputProtocol, TOutputProtocol, TSerializable,
};
use twox_hash::XxHash64;

/// Salt as defined in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md#technical-approach).
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

/// A split block Bloom filter. The creation of this structure is based on the
/// [`crate::file::properties::BloomFilterProperties`] struct set via [`crate::file::properties::WriterProperties`] and
/// is thus hidden by default.
#[derive(Debug, Clone)]
pub struct Sbbf(Vec<Block>);

const SBBF_HEADER_SIZE_ESTIMATE: usize = 20;

/// given an initial offset, and a [ChunkReader], try to read out a bloom filter header and return
/// both the header and the offset after it (for bitset).
fn chunk_read_bloom_filter_header_and_offset<R: ChunkReader>(
    offset: u64,
    reader: Arc<R>,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let buffer = reader.get_bytes(offset as u64, SBBF_HEADER_SIZE_ESTIMATE)?;
    let (header, length) = read_bloom_filter_header_and_length(buffer)?;
    Ok((header, offset + length))
}

/// given a [Bytes] buffer, try to read out a bloom filter header and return both the header and
/// length of the header.
#[inline]
fn read_bloom_filter_header_and_length(
    buffer: Bytes,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let total_length = buffer.len();
    let mut buf_reader = buffer.reader();
    let mut prot = TCompactInputProtocol::new(&mut buf_reader);
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot).map_err(|e| {
        ParquetError::General(format!("Could not read bloom filter header: {}", e))
    })?;
    Ok((
        header,
        (total_length - buf_reader.into_inner().remaining()) as u64,
    ))
}

pub(crate) const BITSET_MIN_LENGTH: usize = 32;
pub(crate) const BITSET_MAX_LENGTH: usize = 128 * 1024 * 1024;

#[inline]
fn optimal_num_of_bytes(num_bytes: usize) -> usize {
    let num_bytes = num_bytes.min(BITSET_MAX_LENGTH);
    let num_bytes = num_bytes.max(BITSET_MIN_LENGTH);
    num_bytes.next_power_of_two()
}

// see http://algo2.iti.kit.edu/documents/cacheefficientbloomfilters-jea.pdf
// given fpp = (1 - e^(-k * n / m)) ^ k
// we have m = - k * n / ln(1 - fpp ^ (1 / k))
// where k = number of hash functions, m = number of bits, n = number of distinct values
#[inline]
fn num_of_bits_from_ndv_fpp(ndv: u64, fpp: f64) -> usize {
    let num_bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    num_bits as usize
}

impl Sbbf {
    /// Create a new [Sbbf] with given number of distinct values and false positive probability.
    /// Will panic if `fpp` is greater than 1.0 or less than 0.0.
    pub(crate) fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self, ParquetError> {
        if !(0.0..1.0).contains(&fpp) {
            return Err(ParquetError::General(format!(
                "False positive probability must be between 0.0 and 1.0, got {}",
                fpp
            )));
        }
        let num_bits = num_of_bits_from_ndv_fpp(ndv, fpp);
        Ok(Self::new_with_num_of_bytes(num_bits / 8))
    }

    /// Create a new [Sbbf] with given number of bytes, the exact number of bytes will be adjusted
    /// to the next power of two bounded by [BITSET_MIN_LENGTH] and [BITSET_MAX_LENGTH].
    pub(crate) fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        let bitset = vec![0_u8; num_bytes];
        Self::new(&bitset)
    }

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

    /// Write the bloom filter data (header and then bitset) to the output
    pub(crate) fn write<W: Write>(&self, writer: W) -> Result<(), ParquetError> {
        // Use a BufWriter to avoid costs of writing individual blocks
        let mut writer = BufWriter::new(writer);
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        let header = self.header();
        header.write_to_out_protocol(&mut protocol).map_err(|e| {
            ParquetError::General(format!("Could not write bloom filter header: {}", e))
        })?;
        protocol.flush()?;
        self.write_bitset(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Write the bitset in serialized form to the writer.
    fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        for block in &self.0 {
            for word in block {
                writer.write_all(&word.to_le_bytes()).map_err(|e| {
                    ParquetError::General(format!(
                        "Could not write bloom filter bit set: {}",
                        e
                    ))
                })?;
            }
        }
        Ok(())
    }

    /// Create and populate [`BloomFilterHeader`] from this bitset for writing to serialized form
    fn header(&self) -> BloomFilterHeader {
        BloomFilterHeader {
            // 8 i32 per block, 4 bytes per i32
            num_bytes: self.0.len() as i32 * 4 * 8,
            algorithm: BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}),
            hash: BloomFilterHash::XXHASH(XxHash {}),
            compression: BloomFilterCompression::UNCOMPRESSED(Uncompressed {}),
        }
    }

    /// Read a new bloom filter from the given offset in the given reader.
    pub(crate) fn read_from_column_chunk<R: ChunkReader>(
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

    /// Insert an [AsBytes] value into the filter
    pub fn insert<T: AsBytes + ?Sized>(&mut self, value: &T) {
        self.insert_hash(hash_as_bytes(value));
    }

    /// Insert a hash into the filter
    fn insert_hash(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        let block = &mut self.0[block_index];
        block_insert(block, hash as u32);
    }

    /// Check if an [AsBytes] value is probably present or definitely absent in the filter
    pub fn check<T: AsBytes>(&self, value: &T) -> bool {
        self.check_hash(hash_as_bytes(value))
    }

    /// Check if a hash is in the filter. May return
    /// true for values that was never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    fn check_hash(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        let block = &self.0[block_index];
        block_check(block, hash as u32)
    }
}

// per spec we use xxHash with seed=0
const SEED: u64 = 0;

#[inline]
fn hash_as_bytes<A: AsBytes + ?Sized>(value: &A) -> u64 {
    let mut hasher = XxHash64::with_seed(SEED);
    hasher.write(value.as_bytes());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{
        BloomFilterAlgorithm, BloomFilterCompression, SplitBlockAlgorithm, Uncompressed,
        XxHash,
    };

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_as_bytes(""), 17241709254077376921);
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
            sbbf.insert(&i);
            assert!(sbbf.check(&i));
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
            assert!(sbbf.check(&value.as_str()));
        }
    }

    /// test the assumption that bloom filter header size should not exceed SBBF_HEADER_SIZE_ESTIMATE
    /// essentially we are testing that the struct is packed with 4 i32 fields, each can be 1-5 bytes
    /// so altogether it'll be 20 bytes at most.
    #[test]
    fn test_bloom_filter_header_size_assumption() {
        let buffer: &[u8; 16] =
            &[21, 64, 28, 28, 0, 0, 28, 28, 0, 0, 28, 28, 0, 0, 0, 99];
        let (
            BloomFilterHeader {
                algorithm,
                compression,
                hash,
                num_bytes,
            },
            read_length,
        ) = read_bloom_filter_header_and_length(Bytes::copy_from_slice(buffer)).unwrap();
        assert_eq!(read_length, 15);
        assert_eq!(
            algorithm,
            BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {})
        );
        assert_eq!(
            compression,
            BloomFilterCompression::UNCOMPRESSED(Uncompressed {})
        );
        assert_eq!(hash, BloomFilterHash::XXHASH(XxHash {}));
        assert_eq!(num_bytes, 32_i32);
        assert_eq!(20, SBBF_HEADER_SIZE_ESTIMATE);
    }

    #[test]
    fn test_optimal_num_of_bytes() {
        for (input, expected) in &[
            (0, 32),
            (9, 32),
            (31, 32),
            (32, 32),
            (33, 64),
            (99, 128),
            (1024, 1024),
            (999_000_000, 128 * 1024 * 1024),
        ] {
            assert_eq!(*expected, optimal_num_of_bytes(*input));
        }
    }

    #[test]
    fn test_num_of_bits_from_ndv_fpp() {
        for (fpp, ndv, num_bits) in &[
            (0.1, 10, 57),
            (0.01, 10, 96),
            (0.001, 10, 146),
            (0.1, 100, 577),
            (0.01, 100, 968),
            (0.001, 100, 1460),
            (0.1, 1000, 5772),
            (0.01, 1000, 9681),
            (0.001, 1000, 14607),
            (0.1, 10000, 57725),
            (0.01, 10000, 96815),
            (0.001, 10000, 146076),
            (0.1, 100000, 577254),
            (0.01, 100000, 968152),
            (0.001, 100000, 1460769),
            (0.1, 1000000, 5772541),
            (0.01, 1000000, 9681526),
            (0.001, 1000000, 14607697),
            (1e-50, 1_000_000_000_000, 14226231280773240832),
        ] {
            assert_eq!(*num_bits, num_of_bits_from_ndv_fpp(*ndv, *fpp) as u64);
        }
    }
}
