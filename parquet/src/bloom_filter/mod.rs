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
//! # Bloom Filter Folding
//!
//! When the NDV is not known ahead of time, bloom filters support a **folding mode** that
//! eliminates the need to guess NDV upfront. See [`Sbbf::fold_to_target_fpp`] for details
//! on the algorithm and its mathematical basis.
//!
//! [parquet-bf-spec]: https://github.com/apache/parquet-format/blob/master/BloomFilter.md
//! [sbbf-paper]: https://arxiv.org/pdf/2101.01719
//! [bf-formulae]: http://tfk.mit.edu/pdf/bloom.pdf

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

impl std::ops::BitOr for Block {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        let mut result = [0u32; 8];
        for (i, item) in result.iter_mut().enumerate() {
            *item = self.0[i] | rhs.0[i];
        }
        Self(result)
    }
}

impl std::ops::BitOrAssign for Block {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        for i in 0..8 {
            self.0[i] |= rhs.0[i];
        }
    }
}

impl Block {
    /// Count the total number of set bits across all 8 words.
    ///
    /// Computes popcount on each word separately and sums. Keeping the popcount
    /// separate from the OR allows the compiler to batch SIMD popcount instructions
    /// (e.g., `cnt.16b` on ARM NEON) instead of interleaving them with OR operations.
    #[inline]
    fn count_ones(self) -> u32 {
        // Written as a fold over the array so the compiler sees 8 independent
        // popcount operations it can vectorize into cnt.16b + horizontal sum.
        self.0.iter().map(|w| w.count_ones()).sum()
    }
}

/// A split block Bloom filter (SBBF).
///
/// An SBBF partitions its bit space into fixed-size 256-bit (32-byte) blocks, each fitting in a
/// single CPU cache line. Each block contains eight 32-bit words, aligned with SIMD lanes for
/// parallel bit manipulation. When checking membership, only one block is accessed per query,
/// eliminating the cache-miss penalty of standard Bloom filters.
///
/// ## Sizing and folding
///
/// Filters are initially sized for a maximum expected number of distinct values (NDV) via
/// [`Sbbf::new_with_ndv_fpp`]. After all values are inserted, the filter is compacted by
/// calling [`Sbbf::fold_to_target_fpp`], which folds the filter down to the smallest size
/// that still meets the target false positive probability.
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

/// The minimum number of bytes for a bloom filter bitset.
pub const BITSET_MIN_LENGTH: usize = 32;
/// The maximum number of bytes for a bloom filter bitset.
pub const BITSET_MAX_LENGTH: usize = 128 * 1024 * 1024;

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
    /// Will return an error if `fpp` is greater than or equal to 1.0 or less than 0.0.
    pub fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self, ParquetError> {
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
    pub fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        assert_eq!(num_bytes % size_of::<Block>(), 0);
        let num_blocks = num_bytes / size_of::<Block>();
        let bitset = vec![Block::ZERO; num_blocks];
        Self(bitset)
    }

    /// Creates a new [Sbbf] from a raw byte slice.
    pub fn new(bitset: &[u8]) -> Self {
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
    pub fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
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
    pub fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
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
    pub fn check<T: AsBytes + ?Sized>(&self, value: &T) -> bool {
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

    /// Returns the number of blocks in this bloom filter.
    pub fn num_blocks(&self) -> usize {
        self.0.len()
    }

    /// Fold the bloom filter down to the smallest size that still meets the target FPP
    /// (False Positive Percentage).
    ///
    /// Repeatedly halves the filter by merging adjacent block pairs via bitwise OR,
    /// stopping when the next fold would cause the estimated FPP to exceed `target_fpp`, or
    /// when the filter reaches the minimum size of 1 block (32 bytes).
    ///
    /// ## How it works
    ///
    /// SBBFs use multiplicative hashing for block selection:
    ///
    /// ```text
    /// block_index = ((hash >> 32) * num_blocks) >> 32
    /// ```
    ///
    /// When `num_blocks` is halved, the new index becomes `floor(original_index / 2)`, so
    /// blocks `2i` and `2i+1` map to the same position. Each fold merges **adjacent** pairs:
    ///
    /// ```text
    /// folded[i] = blocks[2*i] | blocks[2*i + 1]
    /// ```
    ///
    /// This differs from standard Bloom filter folding, which merges the two halves
    /// (`B[i] | B[i + m/2]`) because standard filters use modular hashing where
    /// `h(x) mod (m/2)` maps indices `i` and `i + m/2` to the same position.
    ///
    /// ## Correctness
    ///
    /// Folding **never introduces false negatives**. Every bit that was set in the original
    /// filter remains set in the folded filter (via bitwise OR). The only effect is a controlled
    /// increase in FPP as set bits from different blocks are merged together.
    ///
    /// ## References
    ///
    /// - Sailhan, F. & Stehr, M-O. "Folding and Unfolding Bloom Filters",
    ///   IEEE iThings 2012. <https://doi.org/10.1109/GreenCom.2012.16>
    pub fn fold_to_target_fpp(&mut self, target_fpp: f64) {
        let num_folds = self.num_folds_for_target_fpp(target_fpp);
        if num_folds > 0 {
            self.fold_n(num_folds);
        }
    }

    /// Determine how many folds can be applied without exceeding `target_fpp`.
    ///
    /// Computes the average per-block fill rate in a single pass (no allocation),
    /// then analytically estimates the FPP at each fold level.
    ///
    /// When two blocks with independent fill rate `f` are OR'd, the expected fill
    /// of the merged block is `1 - (1-f)^2`. After `k` folds (merging `2^k` blocks):
    ///
    /// ```text
    /// f_k = 1 - (1 - f)^(2^k)
    /// ```
    ///
    /// SBBF membership checks perform `k=8` bit checks within one 256-bit block,
    /// so the estimated FPP at fold level k is `f_k^8`.
    fn num_folds_for_target_fpp(&self, target_fpp: f64) -> u32 {
        let len = self.0.len();
        if len < 2 {
            return 0;
        }

        // Single pass: compute average per-block fill rate.
        let total_set_bits: u64 = self.0.iter().map(|b| u64::from(b.count_ones())).sum();
        let avg_fill = total_set_bits as f64 / (len as f64 * 256.0);

        // Empty filter: can fold all the way down.
        if avg_fill == 0.0 {
            return len.trailing_zeros();
        }

        // Find max folds where estimated FPP stays within target.
        // f_k = 1 - (1 - avg_fill)^(2^k), FPP_k = f_k^8
        assert!(len.is_power_of_two(), "Number of blocks must be a power of 2 for folding");
        let max_folds = len.trailing_zeros(); // log2(len) since len is power of 2
        let one_minus_f = 1.0 - avg_fill;
        let mut num_folds = 0u32;
        let mut one_minus_fk = one_minus_f; // (1-f)^1 initially

        for _ in 0..max_folds {
            // After one more fold: (1-f)^(2^(k+1)) = ((1-f)^(2^k))^2
            one_minus_fk = one_minus_fk * one_minus_fk;
            let fk = 1.0 - one_minus_fk;
            let estimated_fpp = fk.powi(8);
            if estimated_fpp > target_fpp {
                break;
            }
            num_folds += 1;
        }

        num_folds
    }

    /// Fold the filter `num_folds` times in a single pass.
    ///
    /// Merges groups of `2^num_folds` adjacent blocks via bitwise OR, producing
    /// `len / 2^num_folds` output blocks. The original allocation is reused.
    ///
    /// # Panics
    ///
    /// Panics if `num_folds` is 0 or would reduce the filter below 1 block.
    fn fold_n(&mut self, num_folds: u32) {
        assert!(num_folds > 0, "num_folds must be at least 1");
        let len = self.0.len();
        let group_size = 1usize << num_folds;
        assert!(
            group_size <= len,
            "Cannot fold {num_folds} times: need at least {group_size} blocks, have {len}"
        );
        let new_len = len / group_size;
        for i in 0..new_len {
            let start = i * group_size;
            let mut merged = self.0[start];
            for j in 1..group_size {
                merged |= self.0[start + j];
            }
            self.0[i] = merged;
        }
        self.0.truncate(new_len);
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

    #[test]
    fn test_fold_n_halves_block_count() {
        let mut sbbf = Sbbf::new_with_num_of_bytes(1024); // 32 blocks
        assert_eq!(sbbf.num_blocks(), 32);
        sbbf.fold_n(1);
        assert_eq!(sbbf.num_blocks(), 16);
        sbbf.fold_n(1);
        assert_eq!(sbbf.num_blocks(), 8);
    }

    #[test]
    fn test_fold_preserves_inserted_values() {
        // Create a large filter, insert values, fold, verify no false negatives
        let mut sbbf = Sbbf::new_with_num_of_bytes(32 * 1024); // 32KB = 1024 blocks
        let values: Vec<String> = (0..1000).map(|i| format!("value_{i}")).collect();
        for v in &values {
            sbbf.insert(v.as_str());
        }

        // Fold several times
        let original_blocks = sbbf.num_blocks();
        sbbf.fold_to_target_fpp(0.05);
        assert!(
            sbbf.num_blocks() < original_blocks,
            "should have folded at least once"
        );

        // All inserted values must still be found (no false negatives)
        for v in &values {
            assert!(
                sbbf.check(v.as_str()),
                "Value '{}' missing after folding (false negative!)",
                v
            );
        }
    }

    #[test]
    fn test_fold_to_target_fpp_stops_before_exceeding_target() {
        let mut sbbf = Sbbf::new_with_num_of_bytes(64 * 1024); // 64KB
        // Insert enough values to set some bits
        for i in 0..5000 {
            sbbf.insert(&i);
        }

        let target_fpp = 0.01;
        sbbf.fold_to_target_fpp(target_fpp);

        // After folding, the estimated FPP should be at or below target
        // (the current state should not exceed target — we stopped before that would happen)
        let total_bits = (sbbf.num_blocks() * 256) as f64;
        let set_bits: u64 = sbbf
            .0
            .iter()
            .flat_map(|b| b.0.iter())
            .map(|w| w.count_ones() as u64)
            .sum();
        let fill = set_bits as f64 / total_bits;
        let current_fpp = fill.powi(8);
        assert!(
            current_fpp <= target_fpp,
            "FPP {current_fpp} exceeds target {target_fpp}"
        );
    }

    #[test]
    fn test_fold_empty_filter_folds_to_minimum() {
        // An empty filter has fill=0, so estimated FPP is always 0 — should fold all the way down
        let mut sbbf = Sbbf::new_with_num_of_bytes(1024); // 32 blocks
        sbbf.fold_to_target_fpp(0.01);
        assert_eq!(sbbf.num_blocks(), 1);
    }

    #[test]
    #[should_panic(expected = "Cannot fold 1 times: need at least 2 blocks, have 1")]
    fn test_fold_n_panics_at_minimum_size() {
        let mut sbbf = Sbbf::new_with_num_of_bytes(32); // 1 block (minimum)
        sbbf.fold_n(1);
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

    /*
        Ok, so the following is trying to prove in simple terms that folding an SBBF and
        building a fresh smaller SBBF from scratch produces the exact same bits

        If you insert the same values into a 512-block filter and fold it to 256 blocks,
        you get a bit-for-bit identical result to just inserting those values into a
        256-block filter directly. The fold doesn't lose information or scramble anything,
        it's like you had known the right size all along

        This works because of the 2 lemmas:
        1. when you half the filter, each hash's block index divides cleanly by 2
        so the hash that went to block `i` in the big filter goes to block `i/2` in the small one
        which is exactly where the fold puts it
        > this is trivial since floor(x/2) == floor(floor(x) / 2) is a basic math fact

        2. the bit pattern set _within_ a block depends only on the lower 32 bits of the hash,
        which doesn't change with filter size. So the same bits get set regardless!
        > structually trivial, mask() takes a u32 and uses only the SALT constants.


        When you combine it together, every hash sets the same bits in the same destination block
        whether you fold or build fresh. Therefore the filters are bit-identical
    */
    #[test]
    fn test_sbbf_folded_equals_fresh() {
        let values = (0..5000).map(|i| format!("elem_{i}")).collect::<Vec<_>>();
        let hashes = values
            .iter()
            .map(|v| hash_as_bytes(v.as_str()))
            .collect::<Vec<_>>();

        for num_blocks in [64, 256, 1024] {
            let half = num_blocks / 2;

            // original filter
            let mut original = Sbbf::new_with_num_of_bytes(num_blocks * 32);
            assert_eq!(original.num_blocks(), num_blocks);
            for &h in &hashes {
                original.insert_hash(h);
            }

            for &h in hashes.iter() {
                let mask = Block::mask(h as u32);

                // step 1: element's block in original
                let orig_idx = original.hash_to_block_index(h);
                assert!(orig_idx < num_blocks);

                // step 2 (lemma 1): destination in N/2 filter
                let fresh_idx = {
                    let tmp = Sbbf(vec![Block::ZERO; half]);
                    tmp.hash_to_block_index(h)
                };

                let folded_idx = orig_idx / 2;
                assert_eq!(fresh_idx, folded_idx,);

                // step 3 (lemma 2): mask is the same
                for w in 0..8 {
                    assert_ne!(original.0[orig_idx].0[w] & mask.0[w], 0,);
                }
            }

            // verify the actual blocks match
            let mut folded = original.clone();
            folded.fold_n(1);
            assert_eq!(folded.num_blocks(), half);

            let mut fresh = Sbbf::new_with_num_of_bytes(half * 32);
            for &h in &hashes {
                fresh.insert_hash(h);
            }

            for j in 0..half {
                assert_eq!(
                    folded.0[j].0, fresh.0[j].0,
                    "Step 4 failed: block {j} differs (N={num_blocks}→{half})"
                );
            }
        }
    }

    /// show multi-step folding.
    ///
    /// You can apply the above inductively, folding k times from N blocks produces a filter bit-identical to a fresh N/2^k filter
    #[test]
    fn test_multi_step_fold() {
        let values = (0..3000).map(|i| format!("x_{i}")).collect::<Vec<_>>();

        let mut filter = Sbbf::new_with_num_of_bytes(512 * 32);
        for v in &values {
            filter.insert(v.as_str());
        }

        for expected_blocks in [256, 128, 64, 32, 16, 8, 4, 2, 1] {
            filter.fold_n(1);
            assert_eq!(filter.num_blocks(), expected_blocks);

            let mut fresh = Sbbf::new_with_num_of_bytes(expected_blocks * 32);
            for v in &values {
                fresh.insert(v.as_str());
            }
            for (fb, rb) in filter.0.iter().zip(fresh.0.iter()) {
                assert_eq!(fb.0, rb.0,);
            }
        }
    }

    /// test that the fpp estimator's overestimation doesn't cause fold_to_target_fpp
    /// to produce significantly oversized filters
    ///
    /// compare the final size after folding against the theoretical optimal size
    #[test]
    fn test_fold_size_vs_optimal_fixed_size() {
        for (ndv, target_fpp) in [
            (1000, 0.05),
            (1000, 0.01),
            (5000, 0.05),
            (5000, 0.01),
            (10000, 0.05),
        ] {
            let values = (0..ndv).map(|i| format!("d_{i}")).collect::<Vec<_>>();

            let mut folded = Sbbf::new_with_num_of_bytes(128 * 1024); // 128KB
            for v in &values {
                folded.insert(v.as_str());
            }
            folded.fold_to_target_fpp(target_fpp);

            let folded_bytes = folded.num_blocks() * 32;

            let optimal = Sbbf::new_with_ndv_fpp(ndv as u64, target_fpp).unwrap();
            let optimal_bytes = optimal.num_blocks() * 32;

            let ratio = folded_bytes as f64 / optimal_bytes as f64;

            assert_eq!(ratio, 1.0);
        }
    }

    /// verify that a folded sbbf has the same empirical fpp as a fresh filter of the same size
    /// this bridges the bit-identity proof above with the FPP guarantee from the folding paper
    ///     since the bits are identical, the false-positive rate must be too
    ///
    /// we measure fpp empirically by probing with values that were never inserted
    /// and counting how many are incorrectly marked as present
    #[test]
    fn test_folded_fpp_matches_fresh_fpp() {
        let ndv = 2000;
        let num_probes = 50_000;
        let inserted = (0..ndv)
            .map(|i| format!("ins_{i}"))
            .collect::<Vec<String>>();

        // probe values that were NOT inserted (different prefix guarantees no overlap)
        let probes = (0..num_probes)
            .map(|i| format!("probe_{i}"))
            .collect::<Vec<String>>();

        // build a large filter and fold it down several times
        let mut folded = Sbbf::new_with_num_of_bytes(512 * 32); // 512 blocks
        for v in &inserted {
            folded.insert(v.as_str());
        }

        // check FPP at each fold level
        for expected_blocks in [256, 128, 64, 32, 16, 8, 4, 2, 1] {
            folded.fold_n(1);
            assert_eq!(folded.num_blocks(), expected_blocks);

            // build a fresh filter of the same size with the same values
            let mut fresh = Sbbf::new_with_num_of_bytes(expected_blocks * 32);
            for v in &inserted {
                fresh.insert(v.as_str());
            }

            // measure empirical FPP on both
            let mut folded_fp = 0u64;
            let mut fresh_fp = 0u64;
            for p in &probes {
                if folded.check(p.as_str()) {
                    folded_fp += 1;
                }
                if fresh.check(p.as_str()) {
                    fresh_fp += 1;
                }
            }

            // bit-identity means these must be exactly equal
            assert_eq!(folded_fp, fresh_fp);
        }
    }
}
