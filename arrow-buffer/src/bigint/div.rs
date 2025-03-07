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

    let numerator_words = (numerator_bits + 63) / 64;
    let divisor_words = (divisor_bits + 63) / 64;
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
        let x = self as *const Self;
        unsafe { std::slice::from_raw_parts(x as *const T, N + 1) }
    }
}

impl<T, const N: usize> std::ops::DerefMut for ArrayPlusOne<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let x = self as *mut Self;
        unsafe { std::slice::from_raw_parts_mut(x as *mut T, N + 1) }
    }
}
