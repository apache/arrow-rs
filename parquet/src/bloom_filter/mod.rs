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
type SBBF = Vec<Block>;

#[inline]
fn hash_to_block_index(sbbf: &SBBF, hash: u32) -> usize {
  // unchecked_mul is unstable, but in reality this is safe, we'd just use saturating mul
  // but it will not saturate
  ((hash as usize) >> 32).saturating_mul(sbbf.len()) >> 32
}

fn sbbf_insert(sbbf: &mut SBBF, hash: u32) {
  let block_index = hash_to_block_index(sbbf, hash);
  let block = &mut sbbf[block_index];
  block_insert(block, hash);
}

/// The filter_check operation uses the same method as filter_insert to select a block to operate
/// on, then uses the least significant 32 bits of its argument as an argument to block_check called
/// on that block, returning the result.
fn sbbf_check(sbbf: &SBBF, hash: u32) -> bool {
  let block_index = hash_to_block_index(sbbf, hash);
  let block = &sbbf[block_index];
  block_check(block, hash)
}

#[cfg(test)]
mod tests {
  use super::*;

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
    let mut sbbf = vec![[0_u32; 8]; 1_000];
    for i in 0..1_000_000 {
      sbbf_insert(&mut sbbf, i);
      assert!(sbbf_check(&sbbf, i));
    }
  }
}
