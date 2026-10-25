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

//! N-digit division
//!
//! Implementation heavily inspired by [uint]
//!
//! [uint]: https://github.com/paritytech/parity-common/blob/d3a9327124a66e52ca1114bb8640c02c18c134b8/uint/src/uint.rs#L844

/// Unsigned, little-endian, n-digit division with remainder
///
/// # Panics
///
/// Panics if divisor is zero
pub fn div_rem<const N: usize>(numerator: &[u64; N], divisor: &[u64; N]) -> ([u64; N], [u64; N]) {
    let numerator_bits = bits(numerator);
    let divisor_bits = bits(divisor);
    assert_ne!(divisor_bits, 0, "division by zero");

    if numerator_bits < divisor_bits {
        return ([0; N], *numerator);
    }

    if divisor_bits <= 64 {
        return div_rem_small(numerator, divisor[0]);
    }

    let numerator_words = numerator_bits.div_ceil(64);
    let divisor_words = divisor_bits.div_ceil(64);
    let n = divisor_words;
    let m = numerator_words - divisor_words;

    div_rem_knuth(numerator, divisor, n, m)
}

/// Return the least number of bits needed to represent the number
fn bits(arr: &[u64]) -> usize {
    for (idx, v) in arr.iter().enumerate().rev() {
        if *v > 0 {
            return 64 - v.leading_zeros() as usize + 64 * idx;
        }
    }
    0
}

/// Division of numerator by a u64 divisor
fn div_rem_small<const N: usize>(numerator: &[u64; N], divisor: u64) -> ([u64; N], [u64; N]) {
    // `div_rem_word` uses the `div` instruction only on x86_64 outside Miri, which can't run
    // inline assembly. On every other 64-bit target it is a `u128` division, which calls
    // `__udivti3` for every digit, so those targets divide with a reciprocal instead
    #[cfg(all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri)))]
    {
        div_rem_small_reciprocal(numerator, divisor)
    }

    // x86_64 outside Miri keeps `div`. On 32-bit targets, including wasm32, each
    // 64x64-to-128-bit product in the reciprocal path would call `__multi3`
    #[cfg(not(all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri))))]
    {
        div_rem_small_word(numerator, divisor)
    }
}

#[cfg(any(
    test,
    not(all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri)))
))]
fn div_rem_small_word<const N: usize>(numerator: &[u64; N], divisor: u64) -> ([u64; N], [u64; N]) {
    let mut rem = 0u64;
    let mut numerator = *numerator;
    numerator.iter_mut().rev().for_each(|d| {
        let (q, r) = div_rem_word(rem, *d, divisor);
        *d = q;
        rem = r;
    });

    let mut rem_padded = [0; N];
    rem_padded[0] = rem;
    (numerator, rem_padded)
}

/// Like [`div_rem_small_word`], but divides each digit by multiplying with a
/// reciprocal of the divisor, computed once
#[cfg(any(
    test,
    all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri))
))]
fn div_rem_small_reciprocal<const N: usize>(
    numerator: &[u64; N],
    divisor: u64,
) -> ([u64; N], [u64; N]) {
    // Shifting both operands so the divisor's top bit is set leaves the quotient unchanged
    let shift = divisor.leading_zeros();
    let divisor = divisor << shift;
    let reciprocal = reciprocal_word(divisor);
    let numerator = full_shl(numerator, shift);

    // The top digit is below 2^shift, and so below the divisor
    let mut rem = numerator[N];
    let mut quotient = [0; N];
    for i in (0..N).rev() {
        let (q, r) = div_rem_word_reciprocal(rem, numerator[i], divisor, reciprocal);
        quotient[i] = q;
        rem = r;
    }

    let mut rem_padded = [0; N];
    rem_padded[0] = rem >> shift;
    (quotient, rem_padded)
}

/// Returns `floor((2^128 - 1) / divisor) - 2^64` for a divisor with its top bit set
///
/// This is algorithm 2 of [Möller and Granlund, "Improved division by invariant integers" (2011)](https://gmplib.org/~tege/division-paper.pdf),
/// which avoids a `u128` division. It looks up an 11-bit approximation of `2^74 / divisor`,
/// refines it with Newton iterations to 21, 34 and 65 bits, and then corrects the last bit.
///
/// The names follow the paper, where `vK` is the `K`th approximation
#[cfg(any(
    test,
    all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri))
))]
fn reciprocal_word(divisor: u64) -> u64 {
    debug_assert_eq!(divisor >> 63, 1);

    // `floor((2^19 - 3 * 2^8) / d9)` for each value of the top 9 bits `d9`. The paper subtracts
    // `3 * 2^8` to make the error bound of `v0` symmetric around zero
    const TABLE: [u16; 256] = {
        let mut table = [0; 256];
        let mut i = 0;
        while i < 256 {
            table[i] = (((1 << 19) - 3 * (1 << 8)) / (i as u32 + 256)) as u16;
            i += 1;
        }
        table
    };

    let d0 = divisor & 1; // Least significant bit
    let d9 = divisor >> 55; // Most significant 9 bits
    // `d40` and `d63` are rounded up, so they can equal 2^40 and 2^63
    let d40 = (divisor >> 24) + 1; // Most significant 40 bits
    let d63 = (divisor >> 1) + d0; // Most significant 63 bits

    // `d9` is in 256..512, so masking gives `d9 - 256` without a bounds check
    let v0 = u64::from(TABLE[(d9 & 0xff) as usize]);
    // Each Newton iteration `x + x * (1 - x * d)` about doubles the bits of precision. The
    // shifts scale `v1` to about `2^84 / divisor` and `v2` to about `2^97 / divisor`, and keep
    // every product within a `u64`. The `- 1` makes `v1` an underestimate, so the next line's
    // `2^60 - v1 * d40`, the error of `v1`, can't underflow
    let v1 = (v0 << 11) - ((v0 * v0 * d40) >> 40) - 1;
    let v2 = (v1 << 13) + ((v1 * ((1 << 60) - v1 * d40)) >> 47);
    // The error of `v2`, `2^97 - v2 * divisor`, can take 65 bits, so this computes half of it
    // as `2^96 - v2 * d63 + floor(v2 / 2) * d0`, which fits in 64 bits. The `2^96` term is
    // zero modulo 2^64
    let e = ((v2 >> 1) & d0.wrapping_neg()).wrapping_sub(v2.wrapping_mul(d63));
    // `2^64 + v3` is about `2^128 / divisor`. The `2^64` bit is always set, so it isn't stored
    let v3 = (v2 << 31).wrapping_add(((u128::from(v2) * u128::from(e)) >> 65) as u64);
    // `v3` is the result or one less. `floor((v3 + 2^64 + 1) * divisor / 2^64)` is 2^64 in the
    // first case and `2^64 - 1` in the second, so subtracting it modulo 2^64 adds the missing
    // one. Adding `2^64 * divisor` as `divisor` after the shift keeps the product in a `u128`
    let p = (((u128::from(v3) + 1) * u128::from(divisor)) >> 64) as u64;
    v3.wrapping_sub(p.wrapping_add(divisor))
}

/// Divides `hi:lo` by a divisor with its top bit set, given its [`reciprocal_word`]
///
/// This is algorithm 4 of [Möller and Granlund, "Improved division by invariant integers" (2011)](https://gmplib.org/~tege/division-paper.pdf)
#[cfg(any(
    test,
    all(target_pointer_width = "64", any(not(target_arch = "x86_64"), miri))
))]
#[inline]
fn div_rem_word_reciprocal(hi: u64, lo: u64, divisor: u64, reciprocal: u64) -> (u64, u64) {
    debug_assert!(hi < divisor);
    debug_assert_eq!(divisor >> 63, 1);

    // `(2^64 + reciprocal) * hi + lo` is below 2^128 because `hi < divisor`
    let p = u128::from(reciprocal) * u128::from(hi) + ((u128::from(hi) << 64) | u128::from(lo));
    // `q` is the quotient, one more, or one less
    let mut q = ((p >> 64) as u64).wrapping_add(1);
    let mut r = lo.wrapping_sub(q.wrapping_mul(divisor));
    // The low word of `p` tells whether `q` was one too large
    if r > p as u64 {
        q = q.wrapping_sub(1);
        r = r.wrapping_add(divisor);
    }
    // `q` was one too small, which is rare
    if r >= divisor {
        q += 1;
        r -= divisor;
    }
    (q, r)
}

/// Use Knuth Algorithm D to compute `numerator / divisor` returning the
/// quotient and remainder
///
/// `n` is the number of non-zero 64-bit words in `divisor`
/// `m` is the number of non-zero 64-bit words present in `numerator` beyond `divisor`, and
/// therefore the number of words in the quotient
///
/// A good explanation of the algorithm can be found [here](https://ridiculousfish.com/blog/posts/labor-of-division-episode-iv.html)
fn div_rem_knuth<const N: usize>(
    numerator: &[u64; N],
    divisor: &[u64; N],
    n: usize,
    m: usize,
) -> ([u64; N], [u64; N]) {
    assert!(n + m <= N);

    // The algorithm works by incrementally generating guesses `q_hat`, for the next digit
    // of the quotient, starting from the most significant digit.
    //
    // This relies on the property that for any `q_hat` where
    //
    //      (q_hat << (j * 64)) * divisor <= numerator`
    //
    // We can set
    //
    //      q += q_hat << (j * 64)
    //      numerator -= (q_hat << (j * 64)) * divisor
    //
    // And then iterate until `numerator < divisor`

    // We normalize the divisor so that the highest bit in the highest digit of the
    // divisor is set, this ensures our initial guess of `q_hat` is at most 2 off from
    // the correct value for q[j]
    let shift = divisor[n - 1].leading_zeros();
    // As the shift is computed based on leading zeros, don't need to perform full_shl
    let divisor = shl_word(divisor, shift);
    // numerator may have fewer leading zeros than divisor, so must add another digit
    let mut numerator = full_shl(numerator, shift);

    // The two most significant digits of the divisor
    let b0 = divisor[n - 1];
    let b1 = divisor[n - 2];

    let mut q = [0; N];

    for j in (0..=m).rev() {
        let a0 = numerator[j + n];
        let a1 = numerator[j + n - 1];

        let mut q_hat = if a0 < b0 {
            // The first estimate is [a1, a0] / b0, it may be too large by at most 2
            let (mut q_hat, mut r_hat) = div_rem_word(a0, a1, b0);

            // r_hat = [a1, a0] - q_hat * b0
            //
            // Now we want to compute a more precise estimate [a2,a1,a0] / [b1,b0]
            // which can only be less or equal to the current q_hat
            //
            // q_hat is too large if:
            // [a2,a1,a0] < q_hat * [b1,b0]
            // [a2,r_hat] < q_hat * b1
            let a2 = numerator[j + n - 2];
            loop {
                let r = u128::from(q_hat) * u128::from(b1);
                let (lo, hi) = (r as u64, (r >> 64) as u64);
                if (hi, lo) <= (r_hat, a2) {
                    break;
                }

                q_hat -= 1;
                let (new_r_hat, overflow) = r_hat.overflowing_add(b0);
                r_hat = new_r_hat;

                if overflow {
                    break;
                }
            }
            q_hat
        } else {
            u64::MAX
        };

        // q_hat is now either the correct quotient digit, or in rare cases 1 too large

        // Compute numerator -= (q_hat * divisor) << (j * 64)
        let q_hat_v = full_mul_u64(&divisor, q_hat);
        let c = sub_assign(&mut numerator[j..], &q_hat_v[..n + 1]);

        // If underflow, q_hat was too large by 1
        if c {
            // Reduce q_hat by 1
            q_hat -= 1;

            // Add back one multiple of divisor
            let c = add_assign(&mut numerator[j..], &divisor[..n]);
            numerator[j + n] = numerator[j + n].wrapping_add(u64::from(c));
        }

        // q_hat is the correct value for q[j]
        q[j] = q_hat;
    }

    // The remainder is what is left in numerator, with the initial normalization shl reversed
    let remainder = full_shr(&numerator, shift);
    (q, remainder)
}

/// Perform narrowing division of a u128 by a u64 divisor, returning the quotient and remainder
///
/// This method may trap or panic if hi >= divisor, i.e. the quotient would not fit
/// into a 64-bit integer
fn div_rem_word(hi: u64, lo: u64, divisor: u64) -> (u64, u64) {
    debug_assert!(hi < divisor);
    debug_assert_ne!(divisor, 0);

    // LLVM fails to use the div instruction as it is not able to prove
    // that hi < divisor, and therefore the result will fit into 64-bits
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    unsafe {
        let mut quot = lo;
        let mut rem = hi;
        std::arch::asm!(
            "div {divisor}",
            divisor = in(reg) divisor,
            inout("rax") quot,
            inout("rdx") rem,
            options(pure, nomem, nostack)
        );
        (quot, rem)
    }
    #[cfg(any(not(target_arch = "x86_64"), miri))]
    {
        let x = (u128::from(hi) << 64) + u128::from(lo);
        let y = u128::from(divisor);
        ((x / y) as u64, (x % y) as u64)
    }
}

/// Perform `a += b`
fn add_assign(a: &mut [u64], b: &[u64]) -> bool {
    binop_slice(a, b, u64::overflowing_add)
}

/// Perform `a -= b`
fn sub_assign(a: &mut [u64], b: &[u64]) -> bool {
    binop_slice(a, b, u64::overflowing_sub)
}

/// Converts an overflowing binary operation on scalars to one on slices
fn binop_slice(a: &mut [u64], b: &[u64], binop: impl Fn(u64, u64) -> (u64, bool) + Copy) -> bool {
    let mut c = false;
    a.iter_mut().zip(b.iter()).for_each(|(x, y)| {
        let (res1, overflow1) = y.overflowing_add(u64::from(c));
        let (res2, overflow2) = binop(*x, res1);
        *x = res2;
        c = overflow1 || overflow2;
    });
    c
}

/// Widening multiplication of an N-digit array with a u64
fn full_mul_u64<const N: usize>(a: &[u64; N], b: u64) -> ArrayPlusOne<u64, N> {
    let mut carry = 0;
    let mut out = [0; N];
    out.iter_mut().zip(a).for_each(|(o, v)| {
        let r = *v as u128 * b as u128 + carry as u128;
        *o = r as u64;
        carry = (r >> 64) as u64;
    });
    ArrayPlusOne(out, carry)
}

/// Left shift of an N-digit array by at most 63 bits
fn shl_word<const N: usize>(v: &[u64; N], shift: u32) -> [u64; N] {
    full_shl(v, shift).0
}

/// Widening left shift of an N-digit array by at most 63 bits
fn full_shl<const N: usize>(v: &[u64; N], shift: u32) -> ArrayPlusOne<u64, N> {
    debug_assert!(shift < 64);
    if shift == 0 {
        return ArrayPlusOne(*v, 0);
    }
    let mut out = [0u64; N];
    out[0] = v[0] << shift;
    for i in 1..N {
        out[i] = (v[i - 1] >> (64 - shift)) | (v[i] << shift)
    }
    let carry = v[N - 1] >> (64 - shift);
    ArrayPlusOne(out, carry)
}

/// Narrowing right shift of an (N+1)-digit array by at most 63 bits
fn full_shr<const N: usize>(a: &ArrayPlusOne<u64, N>, shift: u32) -> [u64; N] {
    debug_assert!(shift < 64);
    if shift == 0 {
        return a.0;
    }
    let mut out = [0; N];
    for i in 0..N - 1 {
        out[i] = (a[i] >> shift) | (a[i + 1] << (64 - shift))
    }
    out[N - 1] = a[N - 1] >> shift;
    out
}

/// An array of N + 1 elements
///
/// This is a hack around lack of support for const arithmetic
#[repr(C)]
struct ArrayPlusOne<T, const N: usize>([T; N], T);

impl<T, const N: usize> std::ops::Deref for ArrayPlusOne<T, N> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        let x = std::ptr::from_ref::<Self>(self);
        unsafe { std::slice::from_raw_parts(x.cast::<T>(), N + 1) }
    }
}

impl<T, const N: usize> std::ops::DerefMut for ArrayPlusOne<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let x = std::ptr::from_mut::<Self>(self);
        unsafe { std::slice::from_raw_parts_mut(x.cast::<T>(), N + 1) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    fn check_div_rem_small<const N: usize>(numerator: [u64; N], divisor: u64) {
        let expected = div_rem_small_word(&numerator, divisor);
        assert_eq!(
            div_rem_small_reciprocal(&numerator, divisor),
            expected,
            "{numerator:?} / {divisor}"
        );
    }

    #[test]
    fn test_reciprocal_word() {
        let mut divisors = vec![1 << 63, (1 << 63) + 1, u64::MAX - 1, u64::MAX];
        // Each boundary between entries of the lookup table
        for i in 257..512u64 {
            divisors.extend([(i << 55) - 1, i << 55, (i << 55) + 1]);
        }
        let mut rng = StdRng::seed_from_u64(42);
        divisors.extend((0..100_000).map(|_| rng.random::<u64>() | (1 << 63)));
        for divisor in divisors {
            let n = (u128::from(!divisor) << 64) | u128::from(u64::MAX);
            let expected = (n / u128::from(divisor)) as u64;
            assert_eq!(reciprocal_word(divisor), expected, "{divisor}");
        }
    }

    #[test]
    fn test_div_rem_word_reciprocal() {
        // The reciprocal of 2^63 + 2 is 2^64 - 8, so the estimate for
        // (2^127 + 2^64 - 1) / (2^63 + 2) is 2^64 - 3. That is one below the quotient
        // 2^64 - 2, so the second correction runs. Random inputs rarely reach it.
        let (hi, lo, divisor) = (1 << 63, u64::MAX, (1 << 63) + 2);
        assert_eq!(reciprocal_word(divisor), u64::MAX - 7);
        let n = (u128::from(hi) << 64) | u128::from(lo);
        let d = u128::from(divisor);
        assert_eq!(
            div_rem_word_reciprocal(hi, lo, divisor, reciprocal_word(divisor)),
            ((n / d) as u64, (n % d) as u64)
        );

        let mut rng = StdRng::seed_from_u64(42);
        let mut divisors = vec![1 << 63, (1 << 63) + 1, u64::MAX - 1, u64::MAX];
        divisors.extend((0..100).map(|_| rng.random::<u64>() | (1 << 63)));
        for divisor in divisors {
            let reciprocal = reciprocal_word(divisor);
            let mut his = vec![0, 1, divisor / 2, divisor - 1];
            his.extend((0..20).map(|_| rng.random_range(0..divisor)));
            for hi in his {
                let mut los = vec![0, 1, divisor - 1, divisor, u64::MAX];
                los.extend((0..20).map(|_| rng.random::<u64>()));
                for lo in los {
                    let n = (u128::from(hi) << 64) | u128::from(lo);
                    let d = u128::from(divisor);
                    let expected = ((n / d) as u64, (n % d) as u64);
                    assert_eq!(
                        div_rem_word_reciprocal(hi, lo, divisor, reciprocal),
                        expected,
                        "{n} / {divisor}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_div_rem_small_reciprocal() {
        let mut rng = StdRng::seed_from_u64(42);
        let numerators = [
            [0; 4],
            [1, 0, 0, 0],
            [0, 0, 0, 1],
            [u64::MAX; 4],
            [0, 0, 0, u64::MAX],
            [u64::MAX, 0, 0, 0],
            rng.random(),
        ];
        // Every normalization shift, with divisors at and next to each power of two
        for shift in 0..64 {
            let p = 1u64 << shift;
            let mut divisors = vec![p, p.wrapping_mul(2).wrapping_sub(1)];
            divisors.extend(p.checked_add(1));
            divisors.extend(p.checked_sub(1).filter(|d| *d != 0));
            for divisor in divisors {
                for numerator in numerators {
                    check_div_rem_small(numerator, divisor);
                    check_div_rem_small([numerator[0], numerator[1]], divisor);
                    check_div_rem_small([numerator[0]], divisor);
                }
            }
        }
        // Powers of ten, as used when rescaling decimals
        for k in 0..=19 {
            let p = 10u64.pow(k);
            for divisor in [p - 1, p, p + 1].into_iter().filter(|d| *d != 0) {
                for numerator in numerators {
                    check_div_rem_small(numerator, divisor);
                }
            }
        }
        for _ in 0..1000 {
            let divisor = rng.random::<u64>() >> rng.random_range(0..64);
            if divisor != 0 {
                check_div_rem_small(rng.random::<[u64; 4]>(), divisor);
            }
        }
    }
}
