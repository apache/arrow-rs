// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utilities for comparing byte slices a word at a time

use std::cmp::Ordering;

/// Returns the number of leading bytes that `a` and `b` have in common
///
/// ```
/// # use arrow_bitutils::byte_util::common_prefix_len;
/// assert_eq!(common_prefix_len(b"apple", b"apricot"), 2);
/// assert_eq!(common_prefix_len(b"app", b"apple"), 3);
/// assert_eq!(common_prefix_len(b"", b"apple"), 0);
/// ```
// Out of line so that callers don't inline the scan. It also has to stay a leaf: a stack frame
// here measured slower on short values whose first 8 bytes are equal (apache/arrow-rs#12460).
#[inline(never)]
pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);

    // 32 bytes is the widest block that both x86-64 and aarch64 compare inline. At 64 bytes
    // x86-64 calls `bcmp` (apache/arrow-rs#10549).
    let mut i = 0;
    for (x, y) in a.as_chunks::<32>().0.iter().zip(b.as_chunks::<32>().0) {
        if x != y {
            break;
        }
        i += 32;
    }

    // `i <= n` always. `min` tells the compiler so, which removes a bounds check whose panic
    // call would give this function a stack frame.
    let i_start = i.min(n);
    // Little-endian, so that the first differing byte is the lowest set byte of the XOR
    let (a_words, b_words) = (
        a[i_start..].as_chunks::<8>().0,
        b[i_start..].as_chunks::<8>().0,
    );
    for (x, y) in a_words.iter().zip(b_words) {
        let diff = u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y);
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }

    // Fewer than 8 bytes are left. The last word starts with bytes already compared equal, and
    // `trailing_zeros` of 0 is 64, so equal words return `n` without a branch.
    if let (Some(x), Some(y)) = (a.last_chunk::<8>(), b.last_chunk::<8>()) {
        let diff = u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y);
        return n - 8 + (diff.trailing_zeros() / 8) as usize;
    }
    if let (Some(x), Some(y)) = (a.first_chunk::<4>(), b.first_chunk::<4>()) {
        let diff = u32::from_le_bytes(*x) ^ u32::from_le_bytes(*y);
        if diff != 0 {
            return (diff.trailing_zeros() / 8) as usize;
        }
        // Both slices have at least 4 bytes, as the `first_chunk` match above checked
        let (x, y) = (a.last_chunk::<4>().unwrap(), b.last_chunk::<4>().unwrap());
        let diff = u32::from_le_bytes(*x) ^ u32::from_le_bytes(*y);
        return n - 4 + (diff.trailing_zeros() / 8) as usize;
    }
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Compares `a` and `b` in lexicographic order
///
/// Returns the same result as `a.cmp(b)`.
///
/// ```
/// # use arrow_bitutils::byte_util::cmp_bytes;
/// # use std::cmp::Ordering;
/// assert_eq!(cmp_bytes(b"apple", b"apricot"), Ordering::Less);
/// assert_eq!(cmp_bytes(b"ab\0", b"ab"), Ordering::Greater);
/// assert_eq!(cmp_bytes(b"", b""), Ordering::Equal);
/// ```
// Most values that differ do so in their first 8 bytes, which one integer comparison orders, so
// that comparison is inlined into the caller's loop. Everything else goes to `memcmp`, which
// beats a hand-written scan on long shared prefixes (apache/arrow-rs#12460). `#[inline]` is not
// enough: LLVM declines it in large callers such as `ByteArrayEncoder::write_gather`.
#[inline(always)]
pub fn cmp_bytes(a: &[u8], b: &[u8]) -> Ordering {
    if let (Some(x), Some(y)) = (a.first_chunk::<8>(), b.first_chunk::<8>())
        && x != y
    {
        return u64::from_be_bytes(*x).cmp(&u64::from_be_bytes(*y));
    }
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    fn naive_common_prefix_len(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b).take_while(|(x, y)| x == y).count()
    }

    /// Values of lengths around every word and block boundary the functions above step on,
    /// each also with one byte changed to a value that a signed or little-endian comparison
    /// would order wrongly
    fn boundary_values() -> Vec<Vec<u8>> {
        let mut values = vec![vec![]];
        for len in (1..=40).chain([63, 64, 65, 72, 95, 96, 97, 200]) {
            let base: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            for pos in 0..len {
                for byte in [0x00, 0x7f, 0x80, 0xff] {
                    let mut value = base.clone();
                    value[pos] = byte;
                    values.push(value);
                }
            }
            let mut zero_padded = base.clone();
            zero_padded.push(0);
            values.push(zero_padded);
            values.push(base);
        }
        values
    }

    fn check_all(a: &[u8], b: &[u8]) {
        assert_eq!(
            common_prefix_len(a, b),
            naive_common_prefix_len(a, b),
            "{a:?} {b:?}"
        );
        assert_eq!(cmp_bytes(a, b), a.cmp(b), "{a:?} {b:?}");
    }

    #[test]
    fn test_empty() {
        assert_eq!(common_prefix_len(b"", b""), 0);
        assert_eq!(common_prefix_len(b"", b"a"), 0);
        assert_eq!(cmp_bytes(b"", b""), Ordering::Equal);
        assert_eq!(cmp_bytes(b"", b"\0"), Ordering::Less);
        assert_eq!(cmp_bytes(b"\0", b""), Ordering::Greater);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_boundary_values() {
        let values = boundary_values();
        // Every pair of lengths up to 20 bytes, and pairs of equal length beyond, since those
        // are the ones that reach the word and block loops
        for a in &values {
            for b in &values {
                if a.len() <= 20 || a.len() == b.len() {
                    check_all(a, b);
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_long_shared_prefix() {
        for len in [1023, 1024, 1025, 4096] {
            let a: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
            for mismatch in 0..len {
                let mut b = a.clone();
                b[mismatch] = b[mismatch].wrapping_add(1);
                check_all(&a, &b);
                check_all(&a, &b[..mismatch]);
            }
            check_all(&a, &a);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_unaligned_offsets() {
        // Callers pass slices into shared buffers, so slices start at any alignment
        let data: Vec<u8> = (0..300).map(|i| (i % 3) as u8).collect();
        for start_a in 0..8 {
            for start_b in 0..8 {
                for len in [0, 1, 7, 8, 9, 31, 32, 33, 100] {
                    check_all(
                        &data[start_a..start_a + len],
                        &data[start_b..start_b + len + 3],
                    );
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_random() {
        let mut rng = StdRng::seed_from_u64(42);
        // A small alphabet makes long shared prefixes likely
        for _ in 0..20_000 {
            let len_a = rng.random_range(0..80);
            let len_b = rng.random_range(0..80);
            let a: Vec<u8> = (0..len_a).map(|_| rng.random_range(0..2)).collect();
            let b: Vec<u8> = (0..len_b).map(|_| rng.random_range(0..2)).collect();
            check_all(&a, &b);
        }
    }
}
