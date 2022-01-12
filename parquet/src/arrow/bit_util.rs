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

use arrow::util::bit_chunk_iterator::BitChunks;
use std::ops::Range;

/// Counts the number of set bits in the provided range
pub fn count_set_bits(bytes: &[u8], range: Range<usize>) -> usize {
    let mut count = 0_usize;
    let chunks = BitChunks::new(bytes, range.start, range.end - range.start);
    chunks.iter().for_each(|chunk| {
        count += chunk.count_ones() as usize;
    });
    count += chunks.remainder_bits().count_ones() as usize;
    count
}

/// Iterates through the set bit positions in `bytes` in reverse order
pub fn iter_set_bits_rev(bytes: &[u8]) -> impl Iterator<Item = usize> + '_ {
    let (mut byte_idx, mut in_progress) = match bytes.len() {
        0 => (0, 0),
        len => (len - 1, bytes[len - 1]),
    };

    std::iter::from_fn(move || loop {
        if in_progress != 0 {
            let bit_pos = 7 - in_progress.leading_zeros();
            in_progress ^= 1 << bit_pos;
            return Some((byte_idx << 3) + (bit_pos as usize));
        }

        if byte_idx == 0 {
            return None;
        }

        byte_idx -= 1;
        in_progress = bytes[byte_idx];
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanBufferBuilder;
    use rand::prelude::*;

    #[test]
    fn test_bit_fns() {
        let mut rng = thread_rng();
        let mask_length = rng.gen_range(1..20);
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
            .filter_map(|(x, y)| y.then(|| x))
            .collect();
        assert_eq!(actual, expected);

        assert_eq!(iter_set_bits_rev(&[]).count(), 0);
        assert_eq!(count_set_bits(&[], 0..0), 0);
        assert_eq!(count_set_bits(&[0xFF], 1..1), 0);

        for _ in 0..20 {
            let start = rng.gen_range(0..bools.len());
            let end = rng.gen_range(start..bools.len());

            let actual = count_set_bits(nulls.as_slice(), start..end);
            let expected = bools[start..end].iter().filter(|x| **x).count();

            assert_eq!(actual, expected);
        }
    }
}
