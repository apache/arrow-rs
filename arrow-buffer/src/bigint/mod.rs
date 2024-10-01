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

use crate::arith::derive_arith;
use crate::bigint::div::div_rem;
use num::cast::AsPrimitive;
use num::{BigInt, FromPrimitive, ToPrimitive};
use std::cmp::Ordering;
use std::num::ParseIntError;
use std::ops::{BitAnd, BitOr, BitXor, Neg, Shl, Shr};
use std::str::FromStr;

mod div;

/// An opaque error similar to [`std::num::ParseIntError`]
#[derive(Debug)]
pub struct ParseI256Error {}

impl From<ParseIntError> for ParseI256Error {
    fn from(_: ParseIntError) -> Self {
        Self {}
    }
}

impl std::fmt::Display for ParseI256Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse as i256")
    }
}
impl std::error::Error for ParseI256Error {}

/// Error returned by i256::DivRem
enum DivRemError {
    /// Division by zero
    DivideByZero,
    /// Division overflow
    DivideOverflow,
}

/// A signed 256-bit integer
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Default, Eq, PartialEq, Hash)]
#[repr(C)]
pub struct i256 {
    low: u128,
    high: i128,
}

impl std::fmt::Debug for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BigInt::from_signed_bytes_le(&self.to_le_bytes()))
    }
}

impl FromStr for i256 {
    type Err = ParseI256Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // i128 can store up to 38 decimal digits
        if s.len() <= 38 {
            return Ok(Self::from_i128(i128::from_str(s)?));
        }

        let (negative, s) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };

        // Trim leading 0s
        let s = s.trim_start_matches('0');
        if s.is_empty() {
            return Ok(i256::ZERO);
        }

        if !s.as_bytes()[0].is_ascii_digit() {
            // Ensures no duplicate sign
            return Err(ParseI256Error {});
        }

        parse_impl(s, negative)
    }
}

impl From<i8> for i256 {
    fn from(value: i8) -> Self {
        Self::from_i128(value.into())
    }
}

impl From<i16> for i256 {
    fn from(value: i16) -> Self {
        Self::from_i128(value.into())
    }
}

impl From<i32> for i256 {
    fn from(value: i32) -> Self {
        Self::from_i128(value.into())
    }
}

impl From<i64> for i256 {
    fn from(value: i64) -> Self {
        Self::from_i128(value.into())
    }
}

/// Parse `s` with any sign and leading 0s removed
fn parse_impl(s: &str, negative: bool) -> Result<i256, ParseI256Error> {
    if s.len() <= 38 {
        let low = i128::from_str(s)?;
        return Ok(match negative {
            true => i256::from_parts(low.neg() as _, -1),
            false => i256::from_parts(low as _, 0),
        });
    }

    let split = s.len() - 38;
    if !s.as_bytes()[split].is_ascii_digit() {
        // Ensures not splitting codepoint and no sign
        return Err(ParseI256Error {});
    }
    let (hs, ls) = s.split_at(split);

    let mut low = i128::from_str(ls)?;
    let high = parse_impl(hs, negative)?;

    if negative {
        low = -low;
    }

    let low = i256::from_i128(low);

    high.checked_mul(i256::from_i128(10_i128.pow(38)))
        .and_then(|high| high.checked_add(low))
        .ok_or(ParseI256Error {})
}

impl PartialOrd for i256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for i256 {
    fn cmp(&self, other: &Self) -> Ordering {
        // This is 25x faster than using a variable length encoding such
        // as BigInt as it avoids allocation and branching
        self.high.cmp(&other.high).then(self.low.cmp(&other.low))
    }
}

impl i256 {
    /// The additive identity for this integer type, i.e. `0`.
    pub const ZERO: Self = i256 { low: 0, high: 0 };

    /// The multiplicative identity for this integer type, i.e. `1`.
    pub const ONE: Self = i256 { low: 1, high: 0 };

    /// The multiplicative inverse for this integer type, i.e. `-1`.
    pub const MINUS_ONE: Self = i256 {
        low: u128::MAX,
        high: -1,
    };

    /// The maximum value that can be represented by this integer type
    pub const MAX: Self = i256 {
        low: u128::MAX,
        high: i128::MAX,
    };

    /// The minimum value that can be represented by this integer type
    pub const MIN: Self = i256 {
        low: u128::MIN,
        high: i128::MIN,
    };

    /// Create an integer value from its representation as a byte array in little-endian.
    #[inline]
    pub const fn from_le_bytes(b: [u8; 32]) -> Self {
        let (low, high) = split_array(b);
        Self {
            high: i128::from_le_bytes(high),
            low: u128::from_le_bytes(low),
        }
    }

    /// Create an integer value from its representation as a byte array in big-endian.
    #[inline]
    pub const fn from_be_bytes(b: [u8; 32]) -> Self {
        let (high, low) = split_array(b);
        Self {
            high: i128::from_be_bytes(high),
            low: u128::from_be_bytes(low),
        }
    }

    /// Create an `i256` value from a 128-bit value.
    pub const fn from_i128(v: i128) -> Self {
        Self::from_parts(v as u128, v >> 127)
    }

    /// Create an integer value from its representation as string.
    #[inline]
    pub fn from_string(value_str: &str) -> Option<Self> {
        value_str.parse().ok()
    }

    /// Create an optional i256 from the provided `f64`. Returning `None`
    /// if overflow occurred
    pub fn from_f64(v: f64) -> Option<Self> {
        BigInt::from_f64(v).and_then(|i| {
            let (integer, overflow) = i256::from_bigint_with_overflow(i);
            if overflow {
                None
            } else {
                Some(integer)
            }
        })
    }

    /// Create an i256 from the provided low u128 and high i128
    #[inline]
    pub const fn from_parts(low: u128, high: i128) -> Self {
        Self { low, high }
    }

    /// Returns this `i256` as a low u128 and high i128
    pub const fn to_parts(self) -> (u128, i128) {
        (self.low, self.high)
    }

    /// Converts this `i256` into an `i128` returning `None` if this would result
    /// in truncation/overflow
    pub fn to_i128(self) -> Option<i128> {
        let as_i128 = self.low as i128;

        let high_negative = self.high < 0;
        let low_negative = as_i128 < 0;
        let high_valid = self.high == -1 || self.high == 0;

        (high_negative == low_negative && high_valid).then_some(self.low as i128)
    }

    /// Wraps this `i256` into an `i128`
    pub fn as_i128(self) -> i128 {
        self.low as i128
    }

    /// Return the memory representation of this integer as a byte array in little-endian byte order.
    #[inline]
    pub const fn to_le_bytes(self) -> [u8; 32] {
        let low = self.low.to_le_bytes();
        let high = self.high.to_le_bytes();
        let mut t = [0; 32];
        let mut i = 0;
        while i != 16 {
            t[i] = low[i];
            t[i + 16] = high[i];
            i += 1;
        }
        t
    }

    /// Return the memory representation of this integer as a byte array in big-endian byte order.
    #[inline]
    pub const fn to_be_bytes(self) -> [u8; 32] {
        let low = self.low.to_be_bytes();
        let high = self.high.to_be_bytes();
        let mut t = [0; 32];
        let mut i = 0;
        while i != 16 {
            t[i] = high[i];
            t[i + 16] = low[i];
            i += 1;
        }
        t
    }

    /// Create an i256 from the provided [`BigInt`] returning a bool indicating
    /// if overflow occurred
    fn from_bigint_with_overflow(v: BigInt) -> (Self, bool) {
        let v_bytes = v.to_signed_bytes_le();
        match v_bytes.len().cmp(&32) {
            Ordering::Less => {
                let mut bytes = if num::Signed::is_negative(&v) {
                    [255_u8; 32]
                } else {
                    [0; 32]
                };
                bytes[0..v_bytes.len()].copy_from_slice(&v_bytes[..v_bytes.len()]);
                (Self::from_le_bytes(bytes), false)
            }
            Ordering::Equal => (Self::from_le_bytes(v_bytes.try_into().unwrap()), false),
            Ordering::Greater => (Self::from_le_bytes(v_bytes[..32].try_into().unwrap()), true),
        }
    }

    /// Computes the absolute value of this i256
    #[inline]
    pub fn wrapping_abs(self) -> Self {
        // -1 if negative, otherwise 0
        let sa = self.high >> 127;
        let sa = Self::from_parts(sa as u128, sa);

        // Inverted if negative
        Self::from_parts(self.low ^ sa.low, self.high ^ sa.high).wrapping_sub(sa)
    }

    /// Computes the absolute value of this i256 returning `None` if `Self == Self::MIN`
    #[inline]
    pub fn checked_abs(self) -> Option<Self> {
        (self != Self::MIN).then(|| self.wrapping_abs())
    }

    /// Negates this i256
    #[inline]
    pub fn wrapping_neg(self) -> Self {
        Self::from_parts(!self.low, !self.high).wrapping_add(i256::ONE)
    }

    /// Negates this i256 returning `None` if `Self == Self::MIN`
    #[inline]
    pub fn checked_neg(self) -> Option<Self> {
        (self != Self::MIN).then(|| self.wrapping_neg())
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_add(self, other: Self) -> Self {
        let (low, carry) = self.low.overflowing_add(other.low);
        let high = self.high.wrapping_add(other.high).wrapping_add(carry as _);
        Self { low, high }
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        let r = self.wrapping_add(other);
        ((other.is_negative() && r < self) || (!other.is_negative() && r >= self)).then_some(r)
    }

    /// Performs wrapping subtraction
    #[inline]
    pub fn wrapping_sub(self, other: Self) -> Self {
        let (low, carry) = self.low.overflowing_sub(other.low);
        let high = self.high.wrapping_sub(other.high).wrapping_sub(carry as _);
        Self { low, high }
    }

    /// Performs checked subtraction
    #[inline]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        let r = self.wrapping_sub(other);
        ((other.is_negative() && r > self) || (!other.is_negative() && r <= self)).then_some(r)
    }

    /// Performs wrapping multiplication
    #[inline]
    pub fn wrapping_mul(self, other: Self) -> Self {
        let (low, high) = mulx(self.low, other.low);

        // Compute the high multiples, only impacting the high 128-bits
        let hl = self.high.wrapping_mul(other.low as i128);
        let lh = (self.low as i128).wrapping_mul(other.high);

        Self {
            low,
            high: (high as i128).wrapping_add(hl).wrapping_add(lh),
        }
    }

    /// Performs checked multiplication
    #[inline]
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        if self == i256::ZERO || other == i256::ZERO {
            return Some(i256::ZERO);
        }

        // Shift sign bit down to construct mask of all set bits if negative
        let l_sa = self.high >> 127;
        let r_sa = other.high >> 127;
        let out_sa = (l_sa ^ r_sa) as u128;

        // Compute absolute values
        let l_abs = self.wrapping_abs();
        let r_abs = other.wrapping_abs();

        // Overflow if both high parts are non-zero
        if l_abs.high != 0 && r_abs.high != 0 {
            return None;
        }

        // Perform checked multiplication on absolute values
        let (low, high) = mulx(l_abs.low, r_abs.low);

        // Compute the high multiples, only impacting the high 128-bits
        let hl = (l_abs.high as u128).checked_mul(r_abs.low)?;
        let lh = l_abs.low.checked_mul(r_abs.high as u128)?;

        let high = high.checked_add(hl)?.checked_add(lh)?;

        // Reverse absolute value, if necessary
        let (low, c) = (low ^ out_sa).overflowing_sub(out_sa);
        let high = (high ^ out_sa).wrapping_sub(out_sa).wrapping_sub(c as u128) as i128;

        // Check for overflow in final conversion
        (high.is_negative() == (self.is_negative() ^ other.is_negative()))
            .then_some(Self { low, high })
    }

    /// Division operation, returns (quotient, remainder).
    /// This basically implements [Long division]: `<https://en.wikipedia.org/wiki/Division_algorithm>`
    #[inline]
    fn div_rem(self, other: Self) -> Result<(Self, Self), DivRemError> {
        if other == Self::ZERO {
            return Err(DivRemError::DivideByZero);
        }
        if other == Self::MINUS_ONE && self == Self::MIN {
            return Err(DivRemError::DivideOverflow);
        }

        let a = self.wrapping_abs();
        let b = other.wrapping_abs();

        let (div, rem) = div_rem(&a.as_digits(), &b.as_digits());
        let div = Self::from_digits(div);
        let rem = Self::from_digits(rem);

        Ok((
            if self.is_negative() == other.is_negative() {
                div
            } else {
                div.wrapping_neg()
            },
            if self.is_negative() {
                rem.wrapping_neg()
            } else {
                rem
            },
        ))
    }

    /// Interpret this [`i256`] as 4 `u64` digits, least significant first
    fn as_digits(self) -> [u64; 4] {
        [
            self.low as u64,
            (self.low >> 64) as u64,
            self.high as u64,
            (self.high as u128 >> 64) as u64,
        ]
    }

    /// Interpret 4 `u64` digits, least significant first, as a [`i256`]
    fn from_digits(digits: [u64; 4]) -> Self {
        Self::from_parts(
            digits[0] as u128 | (digits[1] as u128) << 64,
            digits[2] as i128 | (digits[3] as i128) << 64,
        )
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        match self.div_rem(other) {
            Ok((v, _)) => v,
            Err(DivRemError::DivideByZero) => panic!("attempt to divide by zero"),
            Err(_) => Self::MIN,
        }
    }

    /// Performs checked division
    #[inline]
    pub fn checked_div(self, other: Self) -> Option<Self> {
        self.div_rem(other).map(|(v, _)| v).ok()
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        match self.div_rem(other) {
            Ok((_, v)) => v,
            Err(DivRemError::DivideByZero) => panic!("attempt to divide by zero"),
            Err(_) => Self::ZERO,
        }
    }

    /// Performs checked remainder
    #[inline]
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        self.div_rem(other).map(|(_, v)| v).ok()
    }

    /// Performs checked exponentiation
    #[inline]
    pub fn checked_pow(self, mut exp: u32) -> Option<Self> {
        if exp == 0 {
            return Some(i256::from_i128(1));
        }

        let mut base = self;
        let mut acc: Self = i256::from_i128(1);

        while exp > 1 {
            if (exp & 1) == 1 {
                acc = acc.checked_mul(base)?;
            }
            exp /= 2;
            base = base.checked_mul(base)?;
        }
        // since exp!=0, finally the exp must be 1.
        // Deal with the final bit of the exponent separately, since
        // squaring the base afterwards is not necessary and may cause a
        // needless overflow.
        acc.checked_mul(base)
    }

    /// Performs wrapping exponentiation
    #[inline]
    pub fn wrapping_pow(self, mut exp: u32) -> Self {
        if exp == 0 {
            return i256::from_i128(1);
        }

        let mut base = self;
        let mut acc: Self = i256::from_i128(1);

        while exp > 1 {
            if (exp & 1) == 1 {
                acc = acc.wrapping_mul(base);
            }
            exp /= 2;
            base = base.wrapping_mul(base);
        }

        // since exp!=0, finally the exp must be 1.
        // Deal with the final bit of the exponent separately, since
        // squaring the base afterwards is not necessary and may cause a
        // needless overflow.
        acc.wrapping_mul(base)
    }

    /// Returns a number [`i256`] representing sign of this [`i256`].
    ///
    /// 0 if the number is zero
    /// 1 if the number is positive
    /// -1 if the number is negative
    pub const fn signum(self) -> Self {
        if self.is_positive() {
            i256::ONE
        } else if self.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ZERO
        }
    }

    /// Returns `true` if this [`i256`] is negative
    #[inline]
    pub const fn is_negative(self) -> bool {
        self.high.is_negative()
    }

    /// Returns `true` if this [`i256`] is positive
    pub const fn is_positive(self) -> bool {
        self.high.is_positive() || self.high == 0 && self.low != 0
    }
}

/// Temporary workaround due to lack of stable const array slicing
/// See <https://github.com/rust-lang/rust/issues/90091>
const fn split_array<const N: usize, const M: usize>(vals: [u8; N]) -> ([u8; M], [u8; M]) {
    let mut a = [0; M];
    let mut b = [0; M];
    let mut i = 0;
    while i != M {
        a[i] = vals[i];
        b[i] = vals[i + M];
        i += 1;
    }
    (a, b)
}

/// Performs an unsigned multiplication of `a * b` returning a tuple of
/// `(low, high)` where `low` contains the lower 128-bits of the result
/// and `high` the higher 128-bits
///
/// This mirrors the x86 mulx instruction but for 128-bit types
#[inline]
fn mulx(a: u128, b: u128) -> (u128, u128) {
    let split = |a: u128| (a & (u64::MAX as u128), a >> 64);

    const MASK: u128 = u64::MAX as _;

    let (a_low, a_high) = split(a);
    let (b_low, b_high) = split(b);

    // Carry stores the upper 64-bits of low and lower 64-bits of high
    let (mut low, mut carry) = split(a_low * b_low);
    carry += a_high * b_low;

    // Update low and high with corresponding parts of carry
    low += carry << 64;
    let mut high = carry >> 64;

    // Update carry with overflow from low
    carry = low >> 64;
    low &= MASK;

    // Perform multiply including overflow from low
    carry += b_high * a_low;

    // Update low and high with values from carry
    low += carry << 64;
    high += carry >> 64;

    // Perform 4th multiplication
    high += a_high * b_high;

    (low, high)
}

derive_arith!(
    i256,
    Add,
    AddAssign,
    add,
    add_assign,
    wrapping_add,
    checked_add
);
derive_arith!(
    i256,
    Sub,
    SubAssign,
    sub,
    sub_assign,
    wrapping_sub,
    checked_sub
);
derive_arith!(
    i256,
    Mul,
    MulAssign,
    mul,
    mul_assign,
    wrapping_mul,
    checked_mul
);
derive_arith!(
    i256,
    Div,
    DivAssign,
    div,
    div_assign,
    wrapping_div,
    checked_div
);
derive_arith!(
    i256,
    Rem,
    RemAssign,
    rem,
    rem_assign,
    wrapping_rem,
    checked_rem
);

impl Neg for i256 {
    type Output = i256;

    #[cfg(debug_assertions)]
    fn neg(self) -> Self::Output {
        self.checked_neg().expect("i256 overflow")
    }

    #[cfg(not(debug_assertions))]
    fn neg(self) -> Self::Output {
        self.wrapping_neg()
    }
}

impl BitAnd for i256 {
    type Output = i256;

    #[inline]
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {
            low: self.low & rhs.low,
            high: self.high & rhs.high,
        }
    }
}

impl BitOr for i256 {
    type Output = i256;

    #[inline]
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            low: self.low | rhs.low,
            high: self.high | rhs.high,
        }
    }
}

impl BitXor for i256 {
    type Output = i256;

    #[inline]
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {
            low: self.low ^ rhs.low,
            high: self.high ^ rhs.high,
        }
    }
}

impl Shl<u8> for i256 {
    type Output = i256;

    #[inline]
    fn shl(self, rhs: u8) -> Self::Output {
        if rhs == 0 {
            self
        } else if rhs < 128 {
            Self {
                high: self.high << rhs | (self.low >> (128 - rhs)) as i128,
                low: self.low << rhs,
            }
        } else {
            Self {
                high: (self.low << (rhs - 128)) as i128,
                low: 0,
            }
        }
    }
}

impl Shr<u8> for i256 {
    type Output = i256;

    #[inline]
    fn shr(self, rhs: u8) -> Self::Output {
        if rhs == 0 {
            self
        } else if rhs < 128 {
            Self {
                high: self.high >> rhs,
                low: self.low >> rhs | ((self.high as u128) << (128 - rhs)),
            }
        } else {
            Self {
                high: self.high >> 127,
                low: (self.high >> (rhs - 128)) as u128,
            }
        }
    }
}

macro_rules! define_as_primitive {
    ($native_ty:ty) => {
        impl AsPrimitive<i256> for $native_ty {
            fn as_(self) -> i256 {
                i256::from_i128(self as i128)
            }
        }
    };
}

define_as_primitive!(i8);
define_as_primitive!(i16);
define_as_primitive!(i32);
define_as_primitive!(i64);
define_as_primitive!(u8);
define_as_primitive!(u16);
define_as_primitive!(u32);
define_as_primitive!(u64);

impl ToPrimitive for i256 {
    fn to_i64(&self) -> Option<i64> {
        let as_i128 = self.low as i128;

        let high_negative = self.high < 0;
        let low_negative = as_i128 < 0;
        let high_valid = self.high == -1 || self.high == 0;

        if high_negative == low_negative && high_valid {
            let (low_bytes, high_bytes) = split_array(u128::to_le_bytes(self.low));
            let high = i64::from_le_bytes(high_bytes);
            let low = i64::from_le_bytes(low_bytes);

            let high_negative = high < 0;
            let low_negative = low < 0;
            let high_valid = self.high == -1 || self.high == 0;

            (high_negative == low_negative && high_valid).then_some(low)
        } else {
            None
        }
    }

    fn to_u64(&self) -> Option<u64> {
        let as_i128 = self.low as i128;

        let high_negative = self.high < 0;
        let low_negative = as_i128 < 0;
        let high_valid = self.high == -1 || self.high == 0;

        if high_negative == low_negative && high_valid {
            self.low.to_u64()
        } else {
            None
        }
    }
}

#[cfg(all(test, not(miri)))] // llvm.x86.subborrow.64 not supported by MIRI
mod tests {
    use super::*;
    use num::Signed;
    use rand::{thread_rng, Rng};

    #[test]
    fn test_signed_cmp() {
        let a = i256::from_parts(i128::MAX as u128, 12);
        let b = i256::from_parts(i128::MIN as u128, 12);
        assert!(a < b);

        let a = i256::from_parts(i128::MAX as u128, 12);
        let b = i256::from_parts(i128::MIN as u128, -12);
        assert!(a > b);
    }

    #[test]
    fn test_to_i128() {
        let vals = [
            BigInt::from_i128(-1).unwrap(),
            BigInt::from_i128(i128::MAX).unwrap(),
            BigInt::from_i128(i128::MIN).unwrap(),
            BigInt::from_u128(u128::MIN).unwrap(),
            BigInt::from_u128(u128::MAX).unwrap(),
        ];

        for v in vals {
            let (t, overflow) = i256::from_bigint_with_overflow(v.clone());
            assert!(!overflow);
            assert_eq!(t.to_i128(), v.to_i128(), "{v} vs {t}");
        }
    }

    /// Tests operations against the two provided [`i256`]
    fn test_ops(il: i256, ir: i256) {
        let bl = BigInt::from_signed_bytes_le(&il.to_le_bytes());
        let br = BigInt::from_signed_bytes_le(&ir.to_le_bytes());

        // Comparison
        assert_eq!(il.cmp(&ir), bl.cmp(&br), "{bl} cmp {br}");

        // Conversions
        assert_eq!(i256::from_le_bytes(il.to_le_bytes()), il);
        assert_eq!(i256::from_be_bytes(il.to_be_bytes()), il);
        assert_eq!(i256::from_le_bytes(ir.to_le_bytes()), ir);
        assert_eq!(i256::from_be_bytes(ir.to_be_bytes()), ir);

        // To i128
        assert_eq!(il.to_i128(), bl.to_i128(), "{bl}");
        assert_eq!(ir.to_i128(), br.to_i128(), "{br}");

        // Absolute value
        let (abs, overflow) = i256::from_bigint_with_overflow(bl.abs());
        assert_eq!(il.wrapping_abs(), abs);
        assert_eq!(il.checked_abs().is_none(), overflow);

        let (abs, overflow) = i256::from_bigint_with_overflow(br.abs());
        assert_eq!(ir.wrapping_abs(), abs);
        assert_eq!(ir.checked_abs().is_none(), overflow);

        // Negation
        let (neg, overflow) = i256::from_bigint_with_overflow(bl.clone().neg());
        assert_eq!(il.wrapping_neg(), neg);
        assert_eq!(il.checked_neg().is_none(), overflow);

        // Negation
        let (neg, overflow) = i256::from_bigint_with_overflow(br.clone().neg());
        assert_eq!(ir.wrapping_neg(), neg);
        assert_eq!(ir.checked_neg().is_none(), overflow);

        // Addition
        let actual = il.wrapping_add(ir);
        let (expected, overflow) = i256::from_bigint_with_overflow(bl.clone() + br.clone());
        assert_eq!(actual, expected);

        let checked = il.checked_add(ir);
        match overflow {
            true => assert!(checked.is_none()),
            false => assert_eq!(checked, Some(actual)),
        }

        // Subtraction
        let actual = il.wrapping_sub(ir);
        let (expected, overflow) = i256::from_bigint_with_overflow(bl.clone() - br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let checked = il.checked_sub(ir);
        match overflow {
            true => assert!(checked.is_none()),
            false => assert_eq!(checked, Some(actual), "{bl} - {br} = {expected}"),
        }

        // Multiplication
        let actual = il.wrapping_mul(ir);
        let (expected, overflow) = i256::from_bigint_with_overflow(bl.clone() * br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let checked = il.checked_mul(ir);
        match overflow {
            true => assert!(
                checked.is_none(),
                "{il} * {ir} = {actual} vs {bl} * {br} = {expected}"
            ),
            false => assert_eq!(
                checked,
                Some(actual),
                "{il} * {ir} = {actual} vs {bl} * {br} = {expected}"
            ),
        }

        // Division
        if ir != i256::ZERO {
            let actual = il.wrapping_div(ir);
            let expected = bl.clone() / br.clone();
            let checked = il.checked_div(ir);

            if ir == i256::MINUS_ONE && il == i256::MIN {
                // BigInt produces an integer over i256::MAX
                assert_eq!(actual, i256::MIN);
                assert!(checked.is_none());
            } else {
                assert_eq!(actual.to_string(), expected.to_string());
                assert_eq!(checked.unwrap().to_string(), expected.to_string());
            }
        } else {
            // `wrapping_div` panics on division by zero
            assert!(il.checked_div(ir).is_none());
        }

        // Remainder
        if ir != i256::ZERO {
            let actual = il.wrapping_rem(ir);
            let expected = bl.clone() % br.clone();
            let checked = il.checked_rem(ir);

            assert_eq!(actual.to_string(), expected.to_string(), "{il} % {ir}");

            if ir == i256::MINUS_ONE && il == i256::MIN {
                assert!(checked.is_none());
            } else {
                assert_eq!(checked.unwrap().to_string(), expected.to_string());
            }
        } else {
            // `wrapping_rem` panics on division by zero
            assert!(il.checked_rem(ir).is_none());
        }

        // Exponentiation
        for exp in vec![0, 1, 2, 3, 8, 100].into_iter() {
            let actual = il.wrapping_pow(exp);
            let (expected, overflow) = i256::from_bigint_with_overflow(bl.clone().pow(exp));
            assert_eq!(actual.to_string(), expected.to_string());

            let checked = il.checked_pow(exp);
            match overflow {
                true => assert!(
                    checked.is_none(),
                    "{il} ^ {exp} = {actual} vs {bl} * {exp} = {expected}"
                ),
                false => assert_eq!(
                    checked,
                    Some(actual),
                    "{il} ^ {exp} = {actual} vs {bl} ^ {exp} = {expected}"
                ),
            }
        }

        // Bit operations
        let actual = il & ir;
        let (expected, _) = i256::from_bigint_with_overflow(bl.clone() & br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let actual = il | ir;
        let (expected, _) = i256::from_bigint_with_overflow(bl.clone() | br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let actual = il ^ ir;
        let (expected, _) = i256::from_bigint_with_overflow(bl.clone() ^ br);
        assert_eq!(actual.to_string(), expected.to_string());

        for shift in [0_u8, 1, 4, 126, 128, 129, 254, 255] {
            let actual = il << shift;
            let (expected, _) = i256::from_bigint_with_overflow(bl.clone() << shift);
            assert_eq!(actual.to_string(), expected.to_string());

            let actual = il >> shift;
            let (expected, _) = i256::from_bigint_with_overflow(bl.clone() >> shift);
            assert_eq!(actual.to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_i256() {
        let candidates = [
            i256::ZERO,
            i256::ONE,
            i256::MINUS_ONE,
            i256::from_i128(2),
            i256::from_i128(-2),
            i256::from_parts(u128::MAX, 1),
            i256::from_parts(u128::MAX, -1),
            i256::from_parts(0, 1),
            i256::from_parts(0, -1),
            i256::from_parts(1, -1),
            i256::from_parts(1, 1),
            i256::from_parts(0, i128::MAX),
            i256::from_parts(0, i128::MIN),
            i256::from_parts(1, i128::MAX),
            i256::from_parts(1, i128::MIN),
            i256::from_parts(u128::MAX, i128::MIN),
            i256::from_parts(100, 32),
            i256::MIN,
            i256::MAX,
            i256::MIN >> 1,
            i256::MAX >> 1,
            i256::ONE << 127,
            i256::ONE << 128,
            i256::ONE << 129,
            i256::MINUS_ONE << 127,
            i256::MINUS_ONE << 128,
            i256::MINUS_ONE << 129,
        ];

        for il in candidates {
            for ir in candidates {
                test_ops(il, ir)
            }
        }
    }

    #[test]
    fn test_signed_ops() {
        // signum
        assert_eq!(i256::from_i128(1).signum(), i256::ONE);
        assert_eq!(i256::from_i128(0).signum(), i256::ZERO);
        assert_eq!(i256::from_i128(-0).signum(), i256::ZERO);
        assert_eq!(i256::from_i128(-1).signum(), i256::MINUS_ONE);

        // is_positive
        assert!(i256::from_i128(1).is_positive());
        assert!(!i256::from_i128(0).is_positive());
        assert!(!i256::from_i128(-0).is_positive());
        assert!(!i256::from_i128(-1).is_positive());

        // is_negative
        assert!(!i256::from_i128(1).is_negative());
        assert!(!i256::from_i128(0).is_negative());
        assert!(!i256::from_i128(-0).is_negative());
        assert!(i256::from_i128(-1).is_negative());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_i256_fuzz() {
        let mut rng = thread_rng();

        for _ in 0..1000 {
            let mut l = [0_u8; 32];
            let len = rng.gen_range(0..32);
            l.iter_mut().take(len).for_each(|x| *x = rng.gen());

            let mut r = [0_u8; 32];
            let len = rng.gen_range(0..32);
            r.iter_mut().take(len).for_each(|x| *x = rng.gen());

            test_ops(i256::from_le_bytes(l), i256::from_le_bytes(r))
        }
    }

    #[test]
    fn test_i256_to_primitive() {
        let a = i256::MAX;
        assert!(a.to_i64().is_none());
        assert!(a.to_u64().is_none());

        let a = i256::from_i128(i128::MAX);
        assert!(a.to_i64().is_none());
        assert!(a.to_u64().is_none());

        let a = i256::from_i128(i64::MAX as i128);
        assert_eq!(a.to_i64().unwrap(), i64::MAX);
        assert_eq!(a.to_u64().unwrap(), i64::MAX as u64);

        let a = i256::from_i128(i64::MAX as i128 + 1);
        assert!(a.to_i64().is_none());
        assert_eq!(a.to_u64().unwrap(), i64::MAX as u64 + 1);

        let a = i256::MIN;
        assert!(a.to_i64().is_none());
        assert!(a.to_u64().is_none());

        let a = i256::from_i128(i128::MIN);
        assert!(a.to_i64().is_none());
        assert!(a.to_u64().is_none());

        let a = i256::from_i128(i64::MIN as i128);
        assert_eq!(a.to_i64().unwrap(), i64::MIN);
        assert!(a.to_u64().is_none());

        let a = i256::from_i128(i64::MIN as i128 - 1);
        assert!(a.to_i64().is_none());
        assert!(a.to_u64().is_none());
    }

    #[test]
    fn test_i256_as_i128() {
        let a = i256::from_i128(i128::MAX).wrapping_add(i256::from_i128(1));
        let i128 = a.as_i128();
        assert_eq!(i128, i128::MIN);

        let a = i256::from_i128(i128::MAX).wrapping_add(i256::from_i128(2));
        let i128 = a.as_i128();
        assert_eq!(i128, i128::MIN + 1);

        let a = i256::from_i128(i128::MIN).wrapping_sub(i256::from_i128(1));
        let i128 = a.as_i128();
        assert_eq!(i128, i128::MAX);

        let a = i256::from_i128(i128::MIN).wrapping_sub(i256::from_i128(2));
        let i128 = a.as_i128();
        assert_eq!(i128, i128::MAX - 1);
    }

    #[test]
    fn test_string_roundtrip() {
        let roundtrip_cases = [
            i256::ZERO,
            i256::ONE,
            i256::MINUS_ONE,
            i256::from_i128(123456789),
            i256::from_i128(-123456789),
            i256::from_i128(i128::MIN),
            i256::from_i128(i128::MAX),
            i256::MIN,
            i256::MAX,
        ];
        for case in roundtrip_cases {
            let formatted = case.to_string();
            let back: i256 = formatted.parse().unwrap();
            assert_eq!(case, back);
        }
    }

    #[test]
    fn test_from_string() {
        let cases = [
            (
                "000000000000000000000000000000000000000011",
                Some(i256::from_i128(11)),
            ),
            (
                "-000000000000000000000000000000000000000011",
                Some(i256::from_i128(-11)),
            ),
            (
                "-0000000000000000000000000000000000000000123456789",
                Some(i256::from_i128(-123456789)),
            ),
            ("-", None),
            ("+", None),
            ("--1", None),
            ("-+1", None),
            ("000000000000000000000000000000000000000", Some(i256::ZERO)),
            ("0000000000000000000000000000000000000000-11", None),
            ("11-1111111111111111111111111111111111111", None),
            (
                "115792089237316195423570985008687907853269984665640564039457584007913129639936",
                None,
            ),
        ];
        for (case, expected) in cases {
            assert_eq!(i256::from_string(case), expected)
        }
    }

    #[allow(clippy::op_ref)]
    fn test_reference_op(il: i256, ir: i256) {
        let r1 = il + ir;
        let r2 = &il + ir;
        let r3 = il + &ir;
        let r4 = &il + &ir;
        assert_eq!(r1, r2);
        assert_eq!(r1, r3);
        assert_eq!(r1, r4);

        let r1 = il - ir;
        let r2 = &il - ir;
        let r3 = il - &ir;
        let r4 = &il - &ir;
        assert_eq!(r1, r2);
        assert_eq!(r1, r3);
        assert_eq!(r1, r4);

        let r1 = il * ir;
        let r2 = &il * ir;
        let r3 = il * &ir;
        let r4 = &il * &ir;
        assert_eq!(r1, r2);
        assert_eq!(r1, r3);
        assert_eq!(r1, r4);

        let r1 = il / ir;
        let r2 = &il / ir;
        let r3 = il / &ir;
        let r4 = &il / &ir;
        assert_eq!(r1, r2);
        assert_eq!(r1, r3);
        assert_eq!(r1, r4);
    }

    #[test]
    fn test_i256_reference_op() {
        let candidates = [
            i256::ONE,
            i256::MINUS_ONE,
            i256::from_i128(2),
            i256::from_i128(-2),
            i256::from_i128(3),
            i256::from_i128(-3),
        ];

        for il in candidates {
            for ir in candidates {
                test_reference_op(il, ir)
            }
        }
    }
}
