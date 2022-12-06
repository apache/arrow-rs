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

use num::cast::AsPrimitive;
use num::{BigInt, FromPrimitive, ToPrimitive};
use std::cmp::Ordering;

/// A signed 256-bit integer
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Default, Eq, PartialEq, Hash)]
pub struct i256 {
    low: u128,
    high: i128,
}

impl std::fmt::Debug for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BigInt::from_signed_bytes_le(&self.to_le_bytes()))
    }
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

    /// Create an integer value from its representation as a byte array in little-endian.
    #[inline]
    pub const fn from_be_bytes(b: [u8; 32]) -> Self {
        let (high, low) = split_array(b);
        Self {
            high: i128::from_be_bytes(high),
            low: u128::from_be_bytes(low),
        }
    }

    pub const fn from_i128(v: i128) -> Self {
        Self::from_parts(v as u128, v >> 127)
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
            Ordering::Greater => {
                (Self::from_le_bytes(v_bytes[..32].try_into().unwrap()), true)
            }
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
        let (low, carry) = self.low.overflowing_add(other.low);
        let high = self.high.checked_add(other.high)?.checked_add(carry as _)?;
        Some(Self { low, high })
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
        let (low, carry) = self.low.overflowing_sub(other.low);
        let high = self.high.checked_sub(other.high)?.checked_sub(carry as _)?;
        Some(Self { low, high })
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
        // Shift sign bit down to construct mask of all set bits if negative
        let l_sa = self.high >> 127;
        let r_sa = other.high >> 127;
        let out_sa = l_sa ^ r_sa;

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

        let high: i128 = high.checked_add(hl)?.checked_add(lh)?.try_into().ok()?;

        // Reverse absolute value, if necessary
        let (low, c) = (low ^ out_sa as u128).overflowing_sub(out_sa as u128);
        let high = (high ^ out_sa).wrapping_sub(out_sa).wrapping_sub(c as i128);

        Some(Self { low, high })
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        Self::from_bigint_with_overflow(l / r).0
    }

    /// Performs checked division
    #[inline]
    pub fn checked_div(self, other: Self) -> Option<Self> {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        let (val, overflow) = Self::from_bigint_with_overflow(l / r);
        (!overflow).then_some(val)
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        Self::from_bigint_with_overflow(l % r).0
    }

    /// Performs checked remainder
    #[inline]
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        if other == Self::ZERO {
            return None;
        }

        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        let (val, overflow) = Self::from_bigint_with_overflow(l % r);
        (!overflow).then_some(val)
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
}

/// Temporary workaround due to lack of stable const array slicing
/// See <https://github.com/rust-lang/rust/issues/90091>
const fn split_array<const N: usize, const M: usize>(
    vals: [u8; N],
) -> ([u8; M], [u8; M]) {
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

macro_rules! derive_op {
    ($t:ident, $op:ident, $wrapping:ident, $checked:ident) => {
        impl std::ops::$t for i256 {
            type Output = i256;

            #[cfg(debug_assertions)]
            fn $op(self, rhs: Self) -> Self::Output {
                self.$checked(rhs).expect("i256 overflow")
            }

            #[cfg(not(debug_assertions))]
            fn $op(self, rhs: Self) -> Self::Output {
                self.$wrapping(rhs)
            }
        }
    };
}

derive_op!(Add, add, wrapping_add, checked_add);
derive_op!(Sub, sub, wrapping_sub, checked_sub);
derive_op!(Mul, mul, wrapping_mul, checked_mul);
derive_op!(Div, div, wrapping_div, checked_div);
derive_op!(Rem, rem, wrapping_rem, checked_rem);

impl std::ops::Neg for i256 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use num::{BigInt, FromPrimitive, Signed, ToPrimitive};
    use rand::{thread_rng, Rng};
    use std::ops::Neg;

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
            assert_eq!(t.to_i128(), v.to_i128(), "{} vs {}", v, t);
        }
    }

    /// Tests operations against the two provided [`i256`]
    fn test_ops(il: i256, ir: i256) {
        let bl = BigInt::from_signed_bytes_le(&il.to_le_bytes());
        let br = BigInt::from_signed_bytes_le(&ir.to_le_bytes());

        // Comparison
        assert_eq!(il.cmp(&ir), bl.cmp(&br), "{} cmp {}", bl, br);

        // Conversions
        assert_eq!(i256::from_le_bytes(il.to_le_bytes()), il);
        assert_eq!(i256::from_be_bytes(il.to_be_bytes()), il);
        assert_eq!(i256::from_le_bytes(ir.to_le_bytes()), ir);
        assert_eq!(i256::from_be_bytes(ir.to_be_bytes()), ir);

        // To i128
        assert_eq!(il.to_i128(), bl.to_i128(), "{}", bl);
        assert_eq!(ir.to_i128(), br.to_i128(), "{}", br);

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
        let (expected, overflow) =
            i256::from_bigint_with_overflow(bl.clone() + br.clone());
        assert_eq!(actual, expected);

        let checked = il.checked_add(ir);
        match overflow {
            true => assert!(checked.is_none()),
            false => assert_eq!(checked.unwrap(), actual),
        }

        // Subtraction
        let actual = il.wrapping_sub(ir);
        let (expected, overflow) =
            i256::from_bigint_with_overflow(bl.clone() - br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let checked = il.checked_sub(ir);
        match overflow {
            true => assert!(checked.is_none()),
            false => assert_eq!(checked.unwrap(), actual),
        }

        // Multiplication
        let actual = il.wrapping_mul(ir);
        let (expected, overflow) =
            i256::from_bigint_with_overflow(bl.clone() * br.clone());
        assert_eq!(actual.to_string(), expected.to_string());

        let checked = il.checked_mul(ir);
        match overflow {
            true => assert!(
                checked.is_none(),
                "{} * {} = {} vs {} * {} = {}",
                il,
                ir,
                actual,
                bl,
                br,
                expected
            ),
            false => assert_eq!(
                checked.unwrap(),
                actual,
                "{} * {} = {} vs {} * {} = {}",
                il,
                ir,
                actual,
                bl,
                br,
                expected
            ),
        }

        // Exponentiation
        for exp in vec![0, 1, 3, 8, 100].into_iter() {
            let actual = il.wrapping_pow(exp);
            let (expected, overflow) =
                i256::from_bigint_with_overflow(bl.clone().pow(exp));
            assert_eq!(actual.to_string(), expected.to_string());

            let checked = il.checked_pow(exp);
            match overflow {
                true => assert!(
                    checked.is_none(),
                    "{} ^ {} = {} vs {} * {} = {}",
                    il,
                    exp,
                    actual,
                    bl,
                    exp,
                    expected
                ),
                false => assert_eq!(
                    checked.unwrap(),
                    actual,
                    "{} ^ {} = {} vs {} * {} = {}",
                    il,
                    exp,
                    actual,
                    bl,
                    exp,
                    expected
                ),
            }
        }
    }

    #[test]
    fn test_i256() {
        let candidates = [
            i256::from_parts(0, 0),
            i256::from_parts(0, 1),
            i256::from_parts(0, -1),
            i256::from_parts(u128::MAX, 1),
            i256::from_parts(u128::MAX, -1),
            i256::from_parts(0, 1),
            i256::from_parts(0, -1),
            i256::from_parts(100, 32),
        ];

        for il in candidates {
            for ir in candidates {
                test_ops(il, ir)
            }
        }
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
}
