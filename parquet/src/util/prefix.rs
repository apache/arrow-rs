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

//! Shared-prefix scanning for `DELTA_BYTE_ARRAY`.

/// Returns the length in bytes of the longest common prefix of `a` and `b`.
///
/// `DELTA_BYTE_ARRAY` stores each value as the number of leading bytes it
/// shares with its predecessor plus the remaining suffix, so this runs once
/// per value written with that encoding. When consecutive values are
/// near-identical — the case the encoding exists for — the scan covers
/// essentially the whole value, which makes its throughput, not its overhead,
/// the thing that matters.
#[inline]
pub(crate) fn common_prefix_length(a: &[u8], b: &[u8]) -> usize {
    // Comparing a block at a time rather than a byte at a time is what keeps
    // this off the critical path. 32 is the widest block that both aarch64
    // and x86-64 still expand inline: at 64 bytes x86-64 drops to an
    // out-of-line `bcmp` call, which costs more than the extra width buys.
    // Measured on aarch64, every width from 16 up performs the same (~15x a
    // byte-wise loop over a 2 MiB shared prefix), so this sits in the middle
    // of a flat optimum rather than on a tuned peak.
    const BLOCK: usize = 32;

    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);

    let mut matched = 0;
    for (x, y) in a.chunks_exact(BLOCK).zip(b.chunks_exact(BLOCK)) {
        if x != y {
            break;
        }
        matched += BLOCK;
    }

    // At most one block plus the sub-block tail is left unresolved: either the
    // block the loop stopped on, or the remainder `chunks_exact` never yielded.
    matched
        + a[matched..]
            .iter()
            .zip(&b[matched..])
            .take_while(|(x, y)| x == y)
            .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The definition, kept deliberately naive, to check the fast path against.
    fn naive(a: &[u8], b: &[u8]) -> usize {
        let mut i = 0;
        while i < a.len() && i < b.len() && a[i] == b[i] {
            i += 1;
        }
        i
    }

    #[test]
    fn test_common_prefix_length_edge_cases() {
        assert_eq!(common_prefix_length(b"", b""), 0);
        assert_eq!(common_prefix_length(b"", b"abc"), 0);
        assert_eq!(common_prefix_length(b"abc", b""), 0);
        assert_eq!(common_prefix_length(b"abc", b"xyz"), 0);
        assert_eq!(common_prefix_length(b"abc", b"abc"), 3);
        // One value a strict prefix of the other, in both orders.
        assert_eq!(common_prefix_length(b"abc", b"abcdef"), 3);
        assert_eq!(common_prefix_length(b"abcdef", b"abc"), 3);
    }

    #[test]
    fn test_common_prefix_length_around_block_boundaries() {
        // Mismatches placed on, either side of, and well past the 32-byte
        // block boundary the scan steps in.
        for len in [31, 32, 33, 63, 64, 65, 127, 128, 129, 1024] {
            for mismatch in 0..=len {
                let a = vec![b'x'; len];
                let mut b = a.clone();
                if mismatch < len {
                    b[mismatch] = b'y';
                }
                let expected = if mismatch < len { mismatch } else { len };
                assert_eq!(
                    common_prefix_length(&a, &b),
                    expected,
                    "len={len} mismatch={mismatch}"
                );
                assert_eq!(common_prefix_length(&a, &b), naive(&a, &b));
            }
        }
    }

    #[test]
    fn test_common_prefix_length_unequal_lengths() {
        // Result is capped by the shorter value even when the longer one
        // continues to match, across block boundaries.
        for a_len in 0..80usize {
            for b_len in 0..80usize {
                let a = vec![b'x'; a_len];
                let b = vec![b'x'; b_len];
                assert_eq!(common_prefix_length(&a, &b), a_len.min(b_len));
                assert_eq!(common_prefix_length(&a, &b), naive(&a, &b));
            }
        }
    }

    #[test]
    fn test_common_prefix_length_matches_naive_on_varied_data() {
        // Non-uniform bytes, so a block compare cannot accidentally succeed
        // on data a byte-wise scan would reject.
        let a: Vec<u8> = (0..500u32).map(|i| (i * 7 % 251) as u8).collect();
        for mismatch in 0..a.len() {
            let mut b = a.clone();
            b[mismatch] = b[mismatch].wrapping_add(1);
            assert_eq!(common_prefix_length(&a, &b), naive(&a, &b));
            assert_eq!(common_prefix_length(&a, &b), mismatch);
        }
    }
}
