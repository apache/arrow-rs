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

//! Optional, fast bloom filter implementation using sbbf_rs
//! in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md).

use crate::data_type::AsBytes;
use crate::errors::ParquetError;
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::reader::ChunkReader;
use crate::format::{
    BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash, BloomFilterHeader,
    SplitBlockAlgorithm, Uncompressed, XxHash,
};
use sbbf_rs_safe::Filter;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol, TSerializable};

use super::{
    chunk_read_bloom_filter_header_and_offset, hash_as_bytes, num_of_bits_from_ndv_fpp,
    optimal_num_of_bytes,
};

/// A split block Bloom filter. The creation of this structure is based on the
/// [`crate::file::properties::BloomFilterProperties`] struct set via [`crate::file::properties::WriterProperties`] and
/// is thus hidden by default.
#[derive(Debug, Clone)]
pub struct Sbbf(Filter);

// see http://algo2.iti.kit.edu/documents/cacheefficientbloomfilters-jea.pdf
// given fpp = (1 - e^(-k * n / m)) ^ k
// we have m = - k * n / ln(1 - fpp ^ (1 / k))
// where k = number of hash functions, m = number of bits, n = number of distinct values

impl Sbbf {
    /// Create a new [Sbbf] with given number of distinct values and false positive probability.
    /// Will panic if `fpp` is greater than 1.0 or less than 0.0.
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
        Self(Filter::new(8, num_bytes))
    }

    #[cfg(test)]
    pub(crate) fn new(bitset: &[u8]) -> Self {
        Self(Filter::from_bytes(bitset).unwrap())
    }

    /// Write the bloom filter data (header and then bitset) to the output. This doesn't
    /// flush the writer in order to boost performance of bulk writing all blocks. Caller
    /// must remember to flush the writer.
    pub(crate) fn write<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        let header = self.header();
        header.write_to_out_protocol(&mut protocol).map_err(|e| {
            ParquetError::General(format!("Could not write bloom filter header: {e}"))
        })?;
        protocol.flush()?;
        self.write_bitset(&mut writer)?;
        Ok(())
    }

    /// Write the bitset in serialized form to the writer.
    fn write_bitset<W: Write>(&self, mut writer: W) -> Result<(), ParquetError> {
        writer.write_all(self.0.as_bytes()).map_err(|e| {
            ParquetError::General(format!("Could not write bloom filter bit set: {e}"))
        })?;
        Ok(())
    }

    /// Create and populate [`BloomFilterHeader`] from this bitset for writing to serialized form
    fn header(&self) -> BloomFilterHeader {
        BloomFilterHeader {
            // 8 i32 per block, 4 bytes per i32
            num_bytes: i32::try_from(self.0.as_bytes().len()).unwrap(),
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

        let mut filter = Filter::new(8, length);

        reader
            .get_read(bitset_offset)?
            .read_exact(filter.as_bytes_mut())?;

        Ok(Some(Self(filter)))
    }

    /// Insert an [AsBytes] value into the filter
    #[inline]
    pub fn insert<T: AsBytes + ?Sized>(&mut self, value: &T) {
        self.insert_hash(hash_as_bytes(value));
    }

    /// Insert a hash into the filter.
    /// Returns true if the filter might have already contained the hash.
    #[inline]
    fn insert_hash(&mut self, hash: u64) -> bool {
        self.0.insert_hash(hash)
    }

    /// Check if an [AsBytes] value is probably present or definitely absent in the filter
    #[inline]
    pub fn check<T: AsBytes>(&self, value: &T) -> bool {
        self.check_hash(hash_as_bytes(value))
    }

    /// Check if a hash is in the filter. May return
    /// true for values that was never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    #[inline]
    fn check_hash(&self, hash: u64) -> bool {
        self.0.contains_hash(hash)
    }
}
