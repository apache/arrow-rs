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

use std::hash::Hasher;
use twox_hash::XxHash64;

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
pub(crate) struct Sbbf(Vec<Block>);

impl Sbbf {
    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        // unchecked_mul is unstable, but in reality this is safe, we'd just use saturating mul
        // but it will not saturate
        (((hash >> 32).saturating_mul(self.0.len() as u64)) >> 32) as usize
    }

    /// Insert a hash into the filter
    pub(crate) fn insert(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        let block = &mut self.0[block_index];
        block_insert(block, hash as u32);
    }

    /// Check if a hash is in the filter
    pub(crate) fn check(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        let block = &self.0[block_index];
        block_check(block, hash as u32)
    }
}

// per spec we use xxHash with seed=0
const SEED: u64 = 0;

fn hash_bytes<A: AsRef<[u8]>>(value: A) -> u64 {
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
}
