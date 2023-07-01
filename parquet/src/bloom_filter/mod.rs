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
use crate::file::reader::ChunkReader;
use crate::format::BloomFilterHeader;
use bytes::{Buf, Bytes};
use std::sync::Arc;
use thrift::protocol::{TCompactInputProtocol, TSerializable};
use xxhash_rust::xxh64::xxh64;

#[cfg(not(feature = "fast_bloom_filter"))]
mod default;
#[cfg(not(feature = "fast_bloom_filter"))]
pub use default::*;

#[cfg(feature = "fast_bloom_filter")]
mod fast;
#[cfg(feature = "fast_bloom_filter")]
pub use fast::*;

const SBBF_HEADER_SIZE_ESTIMATE: usize = 20;

/// given an initial offset, and a [ChunkReader], try to read out a bloom filter header and return
/// both the header and the offset after it (for bitset).
fn chunk_read_bloom_filter_header_and_offset<R: ChunkReader>(
    offset: u64,
    reader: Arc<R>,
) -> Result<(BloomFilterHeader, u64), ParquetError> {
    let buffer = reader.get_bytes(offset, SBBF_HEADER_SIZE_ESTIMATE)?;
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
        ParquetError::General(format!("Could not read bloom filter header: {e}"))
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

// per spec we use xxHash with seed=0
const SEED: u64 = 0;

#[inline(always)]
fn hash_as_bytes<A: AsBytes + ?Sized>(value: &A) -> u64 {
    xxh64(value.as_bytes(), SEED)
}

#[inline]
fn num_of_bits_from_ndv_fpp(ndv: u64, fpp: f64) -> usize {
    let num_bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    num_bits as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{
        BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash,
        SplitBlockAlgorithm, Uncompressed, XxHash,
    };

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_as_bytes(""), 17241709254077376921);
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
            let value = format!("a{a}");
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
