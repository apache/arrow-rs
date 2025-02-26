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

use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
use std::ops::Range;

/// Counts the number of set bits in the provided range
pub fn count_set_bits(bytes: &[u8], range: Range<usize>) -> usize {
    let unaligned = UnalignedBitChunk::new(bytes, range.start, range.end - range.start);
    unaligned.count_ones()
}

/// Iterates through the set bit positions in `bytes` in reverse order
pub fn iter_set_bits_rev(bytes: &[u8]) -> impl Iterator<Item = usize> + '_ {
    let bit_length = bytes.len() * 8;
    let unaligned = UnalignedBitChunk::new(bytes, 0, bit_length);
    let mut chunk_end_idx = bit_length + unaligned.lead_padding() + unaligned.trailing_padding();

    let iter = unaligned
        .prefix()
        .into_iter()
        .chain(unaligned.chunks().iter().cloned())
        .chain(unaligned.suffix());

    iter.rev().flat_map(move |mut chunk| {
        let chunk_idx = chunk_end_idx - 64;
        chunk_end_idx = chunk_idx;
        std::iter::from_fn(move || {
            if chunk != 0 {
                let bit_pos = 63 - chunk.leading_zeros();
                chunk ^= 1 << bit_pos;
                return Some(chunk_idx + (bit_pos as usize));
            }
            None
        })
    })
}

/// Performs big endian sign extension
pub fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::BooleanBufferBuilder;
    use rand::{prelude::*, rng};

    #[test]
    fn test_bit_fns() {
        let mut rng = rng();
        let mask_length = rng.random_range(1..1024);
        let bools: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() & 1 == 0))
            .take(mask_length)
            .collect();

        let mut nulls = BooleanBufferBuilder::new(mask_length);
        bools.iter().for_each(|b| nulls.append(*b));

        let actual: Vec<_> = iter_set_bits_rev(nulls.as_slice()).collect();
        let expected: Vec<_> = bools
            .iter()
            .enumerate()
            .rev()
            .filter_map(|(x, y)| y.then_some(x))
            .collect();
        assert_eq!(actual, expected);

        assert_eq!(iter_set_bits_rev(&[]).count(), 0);
        assert_eq!(count_set_bits(&[], 0..0), 0);
        assert_eq!(count_set_bits(&[0xFF], 1..1), 0);

        for _ in 0..20 {
            let start = rng.random_range(0..bools.len());
            let end = rng.random_range(start..bools.len());

            let actual = count_set_bits(nulls.as_slice(), start..end);
            let expected = bools[start..end].iter().filter(|x| **x).count();

            assert_eq!(actual, expected);
        }
    }
}
