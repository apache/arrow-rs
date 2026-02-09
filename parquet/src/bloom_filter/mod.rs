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
//! in the [spec][parquet-bf-spec].
//!
//! # Bloom Filter Size
//!
//! Parquet uses the [Split Block Bloom Filter][sbbf-paper] (SBBF) as its bloom filter
//! implementation. For each column upon which bloom filters are enabled, the offset and length of an SBBF
//! is stored in  the metadata for each row group in the parquet file. The size of each filter is
//! initialized using a calculation based on the desired number of distinct values (NDV) and false
//! positive probability (FPP). The FPP for a SBBF can be approximated as<sup>[1][bf-formulae]</sup>:
//!
//! ```text
//! f = (1 - e^(-k * n / m))^k
//! ```
//!
//! Where, `f` is the FPP, `k` the number of hash functions, `n` the NDV, and `m` the total number
//! of bits in the bloom filter. This can be re-arranged to determine the total number of bits
//! required to achieve a given FPP and NDV:
//!
//! ```text
//! m = -k * n / ln(1 - f^(1/k))
//! ```
//!
//! SBBFs use eight hash functions to cleanly fit in SIMD lanes<sup>[2][sbbf-paper]</sup>, therefore
//! `k` is set to 8. The SBBF will spread those `m` bits accross a set of `b` blocks that
//! are each 256 bits, i.e., 32 bytes, in size. The number of blocks is chosen as:
//!
//! ```text
//! b = NP2(m/8) / 32
//! ```
//!
//! Where, `NP2` denotes *the next power of two*, and `m` is divided by 8 to be represented as bytes.
//!
//! # Cache-Efficient Bloom Filter Sizing
//!
//! The block structure of SBBFs introduces overhead due to the Poisson distribution of elements
//! across blocks. Following the approach in "Cache-, hash-, and space-efficient bloom
//! filters"<sup>[3][cache-efficient-bf]</sup>, we apply a compensation factor to the theoretical
//! bits-per-element calculation to account for this overhead.
//!
//! The compensation is based on Table I from the paper, which provides empirically-determined
//! adjustment factors for different theoretical bits-per-element values. We use linear interpolation
//! between consecutive integer entries in the table to provide precise compensation values.
//! This ensures that the actual false positive rate matches the requested rate while maintaining
//! cache efficiency.
//!
//! Here is a table of calculated sizes for various FPP and NDV:
//!
//! | NDV       | FPP       | b       | Size (KB) |
//! |-----------|-----------|---------|-----------|
//! | 10,000    | 0.1       | 256     | 8         |
//! | 10,000    | 0.01      | 512     | 16        |
//! | 10,000    | 0.001     | 1,024   | 32        |
//! | 10,000    | 0.0001    | 1,024   | 32        |
//! | 100,000   | 0.1       | 4,096   | 128       |
//! | 100,000   | 0.01      | 4,096   | 128       |
//! | 100,000   | 0.001     | 8,192   | 256       |
//! | 100,000   | 0.0001    | 16,384  | 512       |
//! | 100,000   | 0.00001   | 16,384  | 512       |
//! | 1,000,000 | 0.1       | 32,768  | 1,024     |
//! | 1,000,000 | 0.01      | 65,536  | 2,048     |
//! | 1,000,000 | 0.001     | 65,536  | 2,048     |
//! | 1,000,000 | 0.0001    | 131,072 | 4,096     |
//! | 1,000,000 | 0.00001   | 131,072 | 4,096     |
//! | 1,000,000 | 0.000001  | 262,144 | 8,192     |
//!
//! [parquet-bf-spec]: https://github.com/apache/parquet-format/blob/master/BloomFilter.md
//! [sbbf-paper]: https://arxiv.org/pdf/2101.01719
//! [bf-formulae]: http://tfk.mit.edu/pdf/bloom.pdf
//! [cache-efficient-bf]: https://dl.acm.org/doi/10.1145/1498698.1594230

use crate::basic::{BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash};
use crate::data_type::AsBytes;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::reader::ChunkReader;
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    ThriftSliceInputProtocol, WriteThrift, WriteThriftField,
};
use crate::thrift_struct;
use bytes::Bytes;
use std::io::Write;
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

thrift_struct!(
/// Bloom filter header is stored at beginning of Bloom filter data of each column
/// and followed by its bitset.
///
pub struct BloomFilterHeader {
  /// The size of bitset in bytes
  1: required i32 num_bytes;
  /// The algorithm for setting bits.
  2: required BloomFilterAlgorithm algorithm;
  /// The hash function used for Bloom filter
  3: required BloomFilterHash hash;
  /// The compression used in the Bloom filter
  4: required BloomFilterCompression compression;
}
);

/// Each block is 256 bits, broken up into eight contiguous "words", each consisting of 32 bits.
/// Each word is thought of as an array of bits; each bit is either "set" or "not set".
#[derive(Debug, Copy, Clone)]
#[repr(transparent)]
struct Block([u32; 8]);
impl Block {
    const ZERO: Block = Block([0; 8]);

    /// takes as its argument a single unsigned 32-bit integer and returns a block in which each
    /// word has exactly one bit set.
    fn mask(x: u32) -> Self {
        let mut result = [0_u32; 8];
        for i in 0..8 {
            // wrapping instead of checking for overflow
            let y = x.wrapping_mul(SALT[i]);
            let y = y >> 27;
            result[i] = 1 << y;
        }
        Self(result)
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn to_ne_bytes(self) -> [u8; 32] {
        // SAFETY: [u32; 8] and [u8; 32] have the same size and neither has invalid bit patterns.
        unsafe { std::mem::transmute(self.0) }
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn to_le_bytes(self) -> [u8; 32] {
        self.swap_bytes().to_ne_bytes()
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn swap_bytes(mut self) -> Self {
        self.0.iter_mut().for_each(|x| *x = x.swap_bytes());
        self
    }

    /// setting every bit in the block that was also set in the result from mask
    fn insert(&mut self, hash: u32) {
        let mask = Self::mask(hash);
        for i in 0..8 {
            self[i] |= mask[i];
        }
    }

    /// returns true when every bit that is set in the result of mask is also set in the block.
    fn check(&self, hash: u32) -> bool {
        let mask = Self::mask(hash);
        for i in 0..8 {
            if self[i] & mask[i] == 0 {
                return false;
            }
        }
        true
    }
}

impl std::ops::Index<usize> for Block {
    type Output = u32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl std::ops::IndexMut<usize> for Block {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

/// A split block Bloom filter.
///
/// The creation of this structure is based on the [`crate::file::properties::BloomFilterProperties`]
/// struct set via [`crate::file::properties::WriterProperties`] and is thus hidden by default.
#[derive(Debug, Clone)]
pub struct Sbbf(Vec<Block>);

pub(crate) const SBBF_HEADER_SIZE_ESTIMATE: usize = 20;

/// given an initial offset, and a byte buffer, try to read out a bloom filter header and return
/// both the header and the offset after it (for bitset).
pub(crate) fn chunk_read_bloom_filter_header_and_offset(
    offset: u64,
    buffer: Bytes,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let (header, length) = read_bloom_filter_header_and_length(buffer)?;
    Ok((header, offset + length))
}

/// given a [Bytes] buffer, try to read out a bloom filter header and return both the header and
/// length of the header.
#[inline]
pub(crate) fn read_bloom_filter_header_and_length(
    buffer: Bytes,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    read_bloom_filter_header_and_length_from_bytes(buffer.as_ref())
}

/// Given a byte slice, try to read out a bloom filter header and return both the header and
/// length of the header.
#[inline]
fn read_bloom_filter_header_and_length_from_bytes(
    buffer: &[u8],
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let total_length = buffer.len();
    let mut prot = ThriftSliceInputProtocol::new(buffer);
    let header = BloomFilterHeader::read_thrift(&mut prot)
        .map_err(|e| ParquetError::General(format!("Could not read bloom filter header: {e}")))?;
    Ok((header, (total_length - prot.as_slice().len()) as u64))
}

pub(crate) const BITSET_MIN_LENGTH: usize = 32;
pub(crate) const BITSET_MAX_LENGTH: usize = 128 * 1024 * 1024;

#[inline]
fn optimal_num_of_bytes(num_bytes: usize) -> usize {
    let num_bytes = num_bytes.min(BITSET_MAX_LENGTH);
    let num_bytes = num_bytes.max(BITSET_MIN_LENGTH);
    num_bytes.next_power_of_two()
}

/// Compensation table from Table I of the paper referenced in the module documentation.
/// Maps theoretical bits-per-element (c) to compensated bits-per-element (c') for B=512.
const COMPENSATION_TABLE: &[(f64, f64)] = &[
    (5.0, 6.0),
    (6.0, 7.0),
    (7.0, 8.0),
    (8.0, 9.0),
    (9.0, 10.0),
    (10.0, 11.0),
    (11.0, 12.0),
    (12.0, 13.0),
    (13.0, 14.0),
    (14.0, 16.0),
    (15.0, 17.0),
    (16.0, 18.0),
    (17.0, 20.0),
    (18.0, 21.0),
    (19.0, 23.0),
    (20.0, 25.0),
    (21.0, 26.0),
    (22.0, 28.0),
    (23.0, 30.0),
    (24.0, 32.0),
    (25.0, 35.0),
    (26.0, 38.0),
    (27.0, 40.0),
    (28.0, 44.0),
    (29.0, 48.0),
    (30.0, 51.0),
    (31.0, 58.0),
    (32.0, 64.0),
    (33.0, 74.0),
    (34.0, 90.0),
];

/// Calculates the number of bits needed for a Split Block Bloom Filter given NDV and FPP.
///
/// Uses the compensation approach described in the module documentation to account for
/// block structure overhead. The calculation:
/// 1. Determines the theoretical bits-per-element (c) for an ideal Bloom filter with k=8
/// 2. Applies compensation factors using linear interpolation between table entries
/// 3. Multiplies by NDV to get the total number of bits needed
#[inline]
fn num_of_bits_from_ndv_fpp(ndv: u64, fpp: f64) -> usize {
    // Calculate the theoretical 'c' (bits per element) for an IDEAL filter.
    // With k=8 fixed: c = -k / ln(1 - fpp^(1/k))
    let k = 8.0;
    let theoretical_c = -k / (1.0 - fpp.powf(1.0 / k)).ln();

    // Apply compensation using linear interpolation between table entries
    let compensated_c = if theoretical_c <= COMPENSATION_TABLE[0].0 {
        // Below table range, use first entry
        COMPENSATION_TABLE[0].1
    } else if theoretical_c >= COMPENSATION_TABLE[COMPENSATION_TABLE.len() - 1].0 {
        // Beyond c=34, SBBF efficiency drops off a cliff
        theoretical_c * 2.65
    } else {
        // Find the two table entries to interpolate between
        let idx = COMPENSATION_TABLE
            .iter()
            .position(|(c, _)| *c >= theoretical_c)
            .unwrap(); // Safe because we checked bounds above

        if idx == 0 || COMPENSATION_TABLE[idx].0 == theoretical_c {
            // Exact match or at start
            COMPENSATION_TABLE[idx].1
        } else {
            // Linear interpolation between consecutive entries
            let (c_low, c_prime_low) = COMPENSATION_TABLE[idx - 1];
            let (c_high, c_prime_high) = COMPENSATION_TABLE[idx];
            let ratio = (theoretical_c - c_low) / (c_high - c_low);
            c_prime_low + ratio * (c_prime_high - c_prime_low)
        }
    };

    (ndv as f64 * compensated_c).ceil() as usize
}

impl Sbbf {
    /// Create a new [Sbbf] with given number of distinct values and false positive probability.
    /// Will return an error if `fpp` is greater than or equal to 1.0 or less than 0.0.
    pub(crate) fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self, ParquetError> {
        if !(0.0..1.0).contains(&fpp) {
            return Err(ParquetError::General(format!(
                "False positive probability must be between 0.0 and 1.0, got {fpp}"
            )));
        }
        let num_bits = num_of_bits_from_ndv_fpp(ndv, fpp);
        Ok(Self::new_with_num_of_bytes(num_bits / 8))
    }

    /// Create a new [Sbbf] with given number of bytes, the exact number of bytes will be adjusted
    /// to the next power of two bounded by [BITSET_MIN_LENGTH] and [BITSET_MAX_LENGTH].
    pub(crate) fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        assert_eq!(num_bytes % size_of::<Block>(), 0);
        let num_blocks = num_bytes / size_of::<Block>();
        let bitset = vec![Block::ZERO; num_blocks];
        Self(bitset)
    }

    pub(crate) fn new(bitset: &[u8]) -> Self {
        let data = bitset
            .chunks_exact(4 * 8)
            .map(|chunk| {
                let mut block = Block::ZERO;
                for (i, word) in chunk.chunks_exact(4).enumerate() {
                    block[i] = u32::from_le_bytes(word.try_into().unwrap());
                }
                block
            })
            .collect::<Vec<Block>>();
        Self(data)
    }

    /// Write the bloom filter data (header and then bitset) to the output. This doesn't
    /// flush the writer in order to boost performance of bulk writing all blocks. Caller
    /// must remember to flush the writer.
    /// This method usually is used in conjunction with [`Self::from_bytes`] for serialization/deserialization.
    pub fn write<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        let mut protocol = ThriftCompactOutputProtocol::new(&mut writer);
        self.header().write_thrift(&mut protocol).map_err(|e| {
            ParquetError::General(format!("Could not write bloom filter header: {e}"))
        })?;
        self.write_bitset(&mut writer)?;
        Ok(())
    }

    /// Write the bitset in serialized form to the writer.
    #[cfg(not(target_endian = "little"))]
    fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        for block in &self.0 {
            writer
                .write_all(block.to_le_bytes().as_slice())
                .map_err(|e| {
                    ParquetError::General(format!("Could not write bloom filter bit set: {e}"))
                })?;
        }
        Ok(())
    }

    /// Write the bitset in serialized form to the writer.
    #[cfg(target_endian = "little")]
    fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        // Safety: Block is repr(transparent) and [u32; 8] can be reinterpreted as [u8; 32].
        let slice = unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const u8,
                self.0.len() * size_of::<Block>(),
            )
        };
        writer.write_all(slice).map_err(|e| {
            ParquetError::General(format!("Could not write bloom filter bit set: {e}"))
        })?;
        Ok(())
    }

    /// Create and populate [`BloomFilterHeader`] from this bitset for writing to serialized form
    fn header(&self) -> BloomFilterHeader {
        BloomFilterHeader {
            // 8 i32 per block, 4 bytes per i32
            num_bytes: self.0.len() as i32 * 4 * 8,
            algorithm: BloomFilterAlgorithm::BLOCK,
            hash: BloomFilterHash::XXHASH,
            compression: BloomFilterCompression::UNCOMPRESSED,
        }
    }

    /// Read a new bloom filter from the given offset in the given reader.
    pub fn read_from_column_chunk<R: ChunkReader>(
        column_metadata: &ColumnChunkMetaData,
        reader: &R,
    ) -> Result<Option<Self>, ParquetError> {
        let offset: u64 = if let Some(offset) = column_metadata.bloom_filter_offset() {
            offset
                .try_into()
                .map_err(|_| ParquetError::General("Bloom filter offset is invalid".to_string()))?
        } else {
            return Ok(None);
        };

        let buffer = match column_metadata.bloom_filter_length() {
            Some(length) => reader.get_bytes(offset, length as usize),
            None => reader.get_bytes(offset, SBBF_HEADER_SIZE_ESTIMATE),
        }?;

        let (header, bitset_offset) =
            chunk_read_bloom_filter_header_and_offset(offset, buffer.clone())?;

        match header.algorithm {
            BloomFilterAlgorithm::BLOCK => {
                // this match exists to future proof the singleton algorithm enum
            }
        }
        match header.compression {
            BloomFilterCompression::UNCOMPRESSED => {
                // this match exists to future proof the singleton compression enum
            }
        }
        match header.hash {
            BloomFilterHash::XXHASH => {
                // this match exists to future proof the singleton hash enum
            }
        }

        let bitset = match column_metadata.bloom_filter_length() {
            Some(_) => buffer.slice((bitset_offset - offset) as usize..),
            None => {
                let bitset_length: usize = header.num_bytes.try_into().map_err(|_| {
                    ParquetError::General("Bloom filter length is invalid".to_string())
                })?;
                reader.get_bytes(bitset_offset, bitset_length)?
            }
        };

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
        self.0[block_index].insert(hash as u32)
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
        self.0[block_index].check(hash as u32)
    }

    /// Return the total in memory size of this bloom filter in bytes
    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<Block>()
    }

    /// Reads a Sbff from Thrift encoded bytes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use parquet::errors::Result;
    /// # use parquet::bloom_filter::Sbbf;
    /// # fn main() -> Result<()> {
    /// // In a real application, you would read serialized bloom filter bytes from a cache.
    /// // This example demonstrates the deserialization process.
    /// // Assuming you have bloom filter bytes from a Parquet file:
    /// # let serialized_bytes: Vec<u8> = vec![];
    /// let bloom_filter = Sbbf::from_bytes(&serialized_bytes)?;
    /// // Now you can use the bloom filter to check for values
    /// if bloom_filter.check(&"some_value") {
    ///     println!("Value might be present (or false positive)");
    /// } else {
    ///     println!("Value is definitely not present");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ParquetError> {
        let (header, header_len) = read_bloom_filter_header_and_length_from_bytes(bytes)?;

        let bitset_length: u64 = header
            .num_bytes
            .try_into()
            .map_err(|_| ParquetError::General("Bloom filter length is invalid".to_string()))?;

        // Validate that bitset consumes all remaining bytes
        if header_len + bitset_length != bytes.len() as u64 {
            return Err(ParquetError::General(format!(
                "Bloom filter data contains extra bytes: expected {} total bytes, got {}",
                header_len + bitset_length,
                bytes.len()
            )));
        }

        let start = header_len as usize;
        let end = (header_len + bitset_length) as usize;
        let bitset = bytes
            .get(start..end)
            .ok_or_else(|| ParquetError::General("Bloom filter bitset is invalid".to_string()))?;

        Ok(Self::new(bitset))
    }
}

// per spec we use xxHash with seed=0
const SEED: u64 = 0;

#[inline]
fn hash_as_bytes<A: AsBytes + ?Sized>(value: &A) -> u64 {
    XxHash64::oneshot(SEED, value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact FPP calculation from Putze et al. equation 3.
    /// This is a direct port of the C `libfilter_block_fpp_detail` function for validation purposes.
    /// See https://github.com/jbapple/libfilter/blob/master/c/lib/util.c
    ///
    /// For Parquet SBBF:
    /// - word_bits = 32 (each word is 32 bits)
    /// - bucket_words = 8 (8 words per block, i.e., 256 bits per block)
    /// - hash_bits = 32 (32-bit hash for block selection)
    fn exact_fpp_from_formula(ndv: u64, bytes: usize) -> f64 {
        const CHAR_BIT: usize = 8;
        const WORD_BITS: f64 = 32.0;
        const BUCKET_WORDS: f64 = 8.0;
        const HASH_BITS: f64 = 32.0;

        // Edge cases from the C implementation
        if ndv == 0 {
            return 0.0;
        }
        if bytes == 0 {
            return 1.0;
        }
        let bits = bytes * CHAR_BIT;
        if ndv as f64 / bits as f64 > 3.0 {
            return 1.0;
        }

        let mut result = 0.0;

        // Lambda parameter: average number of elements per block
        // lam = bucket_words * word_bits / ((bytes * CHAR_BIT) / ndv)
        //     = bucket_words * word_bits * ndv / (bytes * CHAR_BIT)
        let lam = BUCKET_WORDS * WORD_BITS * (ndv as f64) / (bits as f64);
        let loglam = lam.ln();
        let log1collide = -HASH_BITS * 2.0_f64.ln();

        const MAX_J: u64 = 10000;
        for j in 0..MAX_J {
            let i = MAX_J - 1 - j;
            let i_f64 = i as f64;

            // logp: log of Poisson probability P(X = i) where X ~ Poisson(lam)
            // P(X = i) = (lam^i * e^(-lam)) / i!
            // log(P(X = i)) = i*log(lam) - lam - log(i!)
            let logp = i_f64 * loglam - lam - libm::lgamma(i_f64 + 1.0);

            // Probability that a block with i elements is saturated (all bits set)
            // After inserting i elements, each of bucket_words bits has probability
            // (1 - 1/word_bits)^i of being 0. We need all bucket_words bits to have
            // at least one bit set.
            let logfinner = if i == 0 {
                f64::NEG_INFINITY // log(0) = -infinity, contributes 0 to result
            } else {
                BUCKET_WORDS * (-(1.0 - 1.0 / WORD_BITS).powf(i_f64)).log1p()
            };

            // Probability of hash collision (two elements map to same block)
            let logcollide = if i == 0 {
                f64::NEG_INFINITY
            } else {
                i_f64.ln() + log1collide
            };

            result += logp.exp() * logfinner.exp() + logp.exp() * logcollide.exp();
        }

        result.min(1.0)
    }

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_as_bytes(""), 17241709254077376921);
    }

    #[test]
    fn test_mask_set_quick_check() {
        for i in 0..1_000_000 {
            let result = Block::mask(i);
            assert!(result.0.iter().all(|&x| x.is_power_of_two()));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000_000 {
            let mut block = Block::ZERO;
            block.insert(i);
            assert!(block.check(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = Sbbf(vec![Block::ZERO; 1_000]);
        for i in 0..1_000_000 {
            sbbf.insert(&i);
            assert!(sbbf.check(&i));
        }
    }

    #[test]
    fn test_with_fixture() {
        // bloom filter produced by parquet-mr/spark for a column of i64 f"a{i}" for i in 0..10
        let bitset: &[u8] = &[
            200, 1, 80, 20, 64, 68, 8, 109, 6, 37, 4, 67, 144, 80, 96, 32, 8, 132, 43, 33, 0, 5,
            99, 65, 2, 0, 224, 44, 64, 78, 96, 4,
        ];
        let sbbf = Sbbf::new(bitset);
        for a in 0..10i64 {
            let value = format!("a{a}");
            assert!(sbbf.check(&value.as_str()));
        }
    }

    /// test the assumption that bloom filter header size should not exceed SBBF_HEADER_SIZE_ESTIMATE
    /// essentially we are testing that the struct is packed with 4 i32 fields, each can be 1-5 bytes
    /// so altogether it'll be 20 bytes at most.
    #[test]
    fn test_bloom_filter_header_size_assumption() {
        let buffer: &[u8; 16] = &[21, 64, 28, 28, 0, 0, 28, 28, 0, 0, 28, 28, 0, 0, 0, 99];
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
        assert_eq!(algorithm, BloomFilterAlgorithm::BLOCK);
        assert_eq!(compression, BloomFilterCompression::UNCOMPRESSED);
        assert_eq!(hash, BloomFilterHash::XXHASH);
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
            (0.1, 10, 68),
            (0.01, 10, 107),
            (0.001, 10, 167),
            (0.0001, 10, 261),
            (0.00001, 10, 497),
            (0.1, 100, 678),
            (0.01, 100, 1069),
            (0.001, 100, 1661),
            (0.0001, 100, 2610),
            (0.00001, 100, 4967),
            (0.1, 1000, 6773),
            (0.01, 1000, 10682),
            (0.001, 1000, 16608),
            (0.0001, 1000, 26091),
            (0.00001, 1000, 49667),
            (0.1, 10000, 67726),
            (0.01, 10000, 106816),
            (0.001, 10000, 166077),
            (0.0001, 10000, 260909),
            (0.00001, 10000, 496665),
            (0.1, 100000, 677255),
            (0.01, 100000, 1068153),
            (0.001, 100000, 1660770),
            (0.0001, 100000, 2609082),
            (0.00001, 100000, 4966647),
            (0.1, 1000000, 6772542),
            (0.01, 1000000, 10681527),
            (0.001, 1000000, 16607698),
            (0.0001, 1000000, 26090819),
            (0.00001, 1000000, 49666467),
        ] {
            assert_eq!(*num_bits, num_of_bits_from_ndv_fpp(*ndv, *fpp) as u64);
        }
    }

    #[test]
    fn test_sbbf_write_round_trip() {
        // Create a bloom filter with a 32-byte bitset (minimum size)
        let bitset_bytes = vec![0u8; 32];
        let mut original = Sbbf::new(&bitset_bytes);

        // Insert some test values
        let test_values = ["hello", "world", "rust", "parquet", "bloom", "filter"];
        for value in &test_values {
            original.insert(value);
        }

        // Serialize to bytes
        let mut output = Vec::new();
        original.write(&mut output).unwrap();

        // Validate header was written correctly
        let mut protocol = ThriftSliceInputProtocol::new(&output);
        let header = BloomFilterHeader::read_thrift(&mut protocol).unwrap();
        assert_eq!(header.num_bytes, bitset_bytes.len() as i32);
        assert_eq!(header.algorithm, BloomFilterAlgorithm::BLOCK);
        assert_eq!(header.hash, BloomFilterHash::XXHASH);
        assert_eq!(header.compression, BloomFilterCompression::UNCOMPRESSED);

        // Deserialize using from_bytes
        let reconstructed = Sbbf::from_bytes(&output).unwrap();

        // Most importantly: verify the bloom filter WORKS correctly after round-trip
        // Note: bloom filters can have false positives, but should never have false negatives
        // So we can't assert !check(), but we should verify inserted values are found
        for value in &test_values {
            assert!(
                reconstructed.check(value),
                "Value '{}' should be present after round-trip",
                value
            );
        }
    }

    /// Test that the actual false positive probability matches the expected FPP
    /// for various configurations of NDV and FPP.
    #[test]
    fn test_actual_fpp_matches_expected() {
        // Test configurations: (expected_fpp, ndv, num_tests)
        let test_cases = [
            (0.01, 10_000, 100_000),
            (0.001, 50_000, 200_000),
            (0.01, 100_000, 200_000),
        ];

        for (expected_fpp, ndv, num_tests) in test_cases {
            println!("Testing FPP={expected_fpp}, NDV={ndv}");

            // Create bloom filter with specified NDV and FPP
            let mut sbbf = Sbbf::new_with_ndv_fpp(ndv, expected_fpp).unwrap();

            // Insert exactly NDV elements (using format "inserted_{i}")
            for i in 0..ndv {
                let value = format!("inserted_{i}");
                sbbf.insert(value.as_str());
            }

            // Verify inserted elements are all found (no false negatives)
            for i in 0..ndv.min(1000) {
                let value = format!("inserted_{i}");
                assert!(
                    sbbf.check(&value.as_str()),
                    "False negative detected for inserted value: {value}"
                );
            }

            // Test non-inserted elements to measure actual FPP
            let mut false_positives = 0;
            for i in 0..num_tests {
                let value = format!("not_inserted_{i}");
                if sbbf.check(&value.as_str()) {
                    false_positives += 1;
                }
            }

            let actual_fpp = false_positives as f64 / num_tests as f64;
            println!("  Expected FPP: {expected_fpp}, Actual FPP: {actual_fpp}");

            // Verify actual FPP is not worse than expected by a large margin
            // The compensation table often results in better (lower) FPP than theoretical,
            // so we mainly check that we don't exceed the expected FPP by too much
            let upper_tolerance = 3.0;
            let upper_bound = expected_fpp * upper_tolerance;

            assert!(
                actual_fpp <= upper_bound,
                "Actual FPP {actual_fpp} exceeds upper bound {upper_bound} (expected FPP: {expected_fpp})"
            );

            // Log if the filter performs significantly better than expected
            if actual_fpp < expected_fpp * 0.5 {
                println!(
                    "  Note: Filter performs {:.1}x better than expected",
                    expected_fpp / actual_fpp
                );
            }
        }
    }

    /// Test that the bloom filter correctly handles no false negatives
    /// while allowing false positives.
    #[test]
    fn test_no_false_negatives() {
        let ndv = 10_000;
        let fpp = 0.01;
        let mut sbbf = Sbbf::new_with_ndv_fpp(ndv, fpp).unwrap();

        // Insert elements
        let inserted_elements: Vec<String> = (0..ndv).map(|i| format!("element_{i}")).collect();
        for elem in &inserted_elements {
            sbbf.insert(elem.as_str());
        }

        // Verify every inserted element is found (no false negatives allowed)
        for elem in &inserted_elements {
            assert!(
                sbbf.check(&elem.as_str()),
                "False negative detected: bloom filter must never report false negatives"
            );
        }
    }

    /// Test edge cases for FPP values
    #[test]
    fn test_fpp_edge_cases() {
        // Very low FPP
        let sbbf = Sbbf::new_with_ndv_fpp(1000, 0.0001).unwrap();
        assert!(!sbbf.0.is_empty());

        // High FPP (but still valid)
        let sbbf = Sbbf::new_with_ndv_fpp(1000, 0.5).unwrap();
        assert!(!sbbf.0.is_empty());

        // Invalid FPP values should error (>= 1.0 or negative)
        assert!(Sbbf::new_with_ndv_fpp(1000, 1.0).is_err());
        assert!(Sbbf::new_with_ndv_fpp(1000, 1.5).is_err());
        assert!(Sbbf::new_with_ndv_fpp(1000, -0.1).is_err());
    }

    /// Test that the compensation table approach produces FPP close to the exact formula.
    /// This validates that our table-based approximation is accurate.
    #[test]
    fn test_compensation_table_vs_exact_formula() {
        // Test various (FPP, NDV) configurations
        let test_cases = [
            (0.1, 1_000),
            (0.01, 1_000),
            (0.001, 1_000),
            (0.0001, 1_000),
            (0.1, 10_000),
            (0.01, 10_000),
            (0.001, 10_000),
            (0.0001, 10_000),
            (0.00001, 10_000),
            (0.1, 100_000),
            (0.01, 100_000),
            (0.001, 100_000),
            (0.0001, 100_000),
            (0.00001, 100_000),
            (0.1, 1_000_000),
            (0.01, 1_000_000),
            (0.001, 1_000_000),
            (0.0001, 1_000_000),
            (0.00001, 1_000_000),
        ];

        for (target_fpp, ndv) in test_cases {
            // Use compensation table approach to determine size
            let num_bits = num_of_bits_from_ndv_fpp(ndv, target_fpp);
            let num_bytes = optimal_num_of_bytes(num_bits / 8);

            // Calculate actual FPP using exact formula
            let exact_fpp = exact_fpp_from_formula(ndv, num_bytes);

            println!(
                "NDV: {ndv:7}, Target FPP: {target_fpp:.6}, Bytes: {num_bytes:8}, Exact FPP: {exact_fpp:.6}"
            );

            // The compensation table should produce an FPP that's at or below the target
            // We allow some tolerance for floating point arithmetic
            // The goal is to not significantly exceed the target FPP
            assert!(
                exact_fpp <= target_fpp * 1.05,
                "Exact FPP {exact_fpp:.6} significantly exceeds target {target_fpp:.6} for NDV={ndv}"
            );
        }
    }

    /// Test that demonstrates the compensation table achieves better (lower) FPP than naive calculation.
    /// This shows why the compensation is necessary for block bloom filters.
    ///
    /// The naive implementation often FAILS to hit the target FPP (actual > target),
    /// while the improved table lookup version succeeds at hitting the target FPP.
    #[test]
    fn test_compensation_benefit() {
        // Use a matrix to define test cases as rows of (fpp, ndv)
        let test_cases_matrix: [[(f64, u64); 3]; 5] = [
            [(0.1, 100), (0.1, 1_000), (0.1, 10_000)],
            [(0.01, 100), (0.01, 1_000), (0.01, 10_000)],
            [(0.001, 100), (0.001, 1_000), (0.001, 10_000)],
            [(0.0001, 100), (0.0001, 1_000), (0.0001, 10_000)],
            [(0.00001, 100), (0.00001, 1_000), (0.00001, 10_000)],
        ];
        // Flatten the matrix into a Vec for processing
        let test_cases: Vec<(f64, u64)> = test_cases_matrix
            .iter()
            .flat_map(|row| row.iter().copied())
            .collect();

        let mut naive_failures = 0;
        let mut compensated_failures = 0;

        for (target_fpp, ndv) in &test_cases {
            let target_fpp = *target_fpp;
            let ndv = *ndv;

            // Naive calculation (without compensation)
            let k = 8.0;
            let theoretical_c = -k / (1.0 - target_fpp.powf(1.0 / k)).ln();
            let naive_bits = (ndv as f64 * theoretical_c).ceil() as usize;
            let naive_bytes = optimal_num_of_bytes(naive_bits / 8);
            let naive_exact_fpp = exact_fpp_from_formula(ndv, naive_bytes);

            // With compensation table
            let compensated_bits = num_of_bits_from_ndv_fpp(ndv, target_fpp);
            let compensated_bytes = optimal_num_of_bytes(compensated_bits / 8);
            let compensated_exact_fpp = exact_fpp_from_formula(ndv, compensated_bytes);

            // Track failures: when actual FPP exceeds target FPP (with small tolerance for rounding)
            let tolerance = 1.01; // Allow 1% tolerance for rounding errors
            let naive_fails = naive_exact_fpp > target_fpp * tolerance;
            let compensated_fails = compensated_exact_fpp > target_fpp * tolerance;

            if naive_fails {
                naive_failures += 1;
            }
            if compensated_fails {
                compensated_failures += 1;
            }

            println!("\nNDV: {ndv}, Target FPP: {target_fpp}");
            println!(
                "  Naive: {} bytes, actual FPP: {:.6} {}",
                naive_bytes,
                naive_exact_fpp,
                if naive_fails {
                    "❌ EXCEEDS TARGET"
                } else {
                    "✓"
                }
            );
            println!(
                "  Compensated: {} bytes, actual FPP: {:.6} {}",
                compensated_bytes,
                compensated_exact_fpp,
                if compensated_fails {
                    "❌ EXCEEDS TARGET"
                } else {
                    "✓ meets target"
                }
            );

            // The compensated version should always be better than or equal to the naive version
            assert!(
                compensated_exact_fpp <= naive_exact_fpp,
                "Compensated FPP {compensated_exact_fpp:.6} should be <= naive FPP {naive_exact_fpp:.6}"
            );
        }

        println!("\n=== Summary ===");
        println!(
            "Naive implementation failures (exceeded target FPP): {}/{}",
            naive_failures,
            test_cases.len()
        );
        println!(
            "Compensated implementation failures: {}/{}",
            compensated_failures,
            test_cases.len()
        );

        // Assert that the naive implementation fails on at least some test cases
        assert!(
            naive_failures > 0,
            "Expected naive implementation to fail to hit target FPP on some test cases"
        );

        // Assert that the compensated implementation has fewer failures than naive
        assert!(
            compensated_failures < naive_failures,
            "Compensated implementation should have fewer failures than naive (compensated: {}, naive: {})",
            compensated_failures,
            naive_failures
        );
    }
}
