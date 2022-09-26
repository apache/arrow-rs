use num::BigInt;
use std::cmp::Ordering;

/// A signed 256-bit integer
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Default, Eq, PartialEq, Hash)]
pub struct i256([u8; 32]);

impl std::fmt::Debug for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BigInt::from_signed_bytes_le(&self.0))
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
        let (left_low, left_high) = self.as_parts();
        let (right_low, right_high) = other.as_parts();
        left_high.cmp(&right_high).then(left_low.cmp(&right_low))
    }
}

impl i256 {
    /// Create an integer value from its representation as a byte array in little-endian.
    #[inline]
    pub fn from_le_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }

    /// Return the memory representation of this integer as a byte array in little-endian byte order.
    #[inline]
    pub fn to_le_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Returns this i256 as a low u128 and high i128
    #[inline]
    fn as_parts(self) -> (u128, i128) {
        (
            u128::from_le_bytes(self.0[0..16].try_into().unwrap()),
            i128::from_le_bytes(self.0[16..32].try_into().unwrap()),
        )
    }

    /// Create an i256 from the provided low and high i128
    #[inline]
    fn from_parts(low: u128, high: i128) -> Self {
        let mut t = [0; 32];
        let t_low: &mut [u8; 16] = (&mut t[0..16]).try_into().unwrap();
        *t_low = low.to_le_bytes();
        let t_high: &mut [u8; 16] = (&mut t[16..32]).try_into().unwrap();
        *t_high = high.to_le_bytes();
        Self(t)
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
                (Self(bytes), false)
            }
            Ordering::Equal => (Self(v_bytes.try_into().unwrap()), false),
            Ordering::Greater => (Self(v_bytes[..32].try_into().unwrap()), true),
        }
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_add(self, other: Self) -> Self {
        let (left_low, left_high) = self.as_parts();
        let (right_low, right_high) = other.as_parts();

        let (low, carry) = left_low.overflowing_add(right_low);
        let high = left_high.wrapping_add(right_high).wrapping_add(carry as _);
        Self::from_parts(low, high)
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        let (left_low, left_high) = self.as_parts();
        let (right_low, right_high) = other.as_parts();

        let (low, carry) = left_low.overflowing_add(right_low);
        let high = left_high.checked_add(right_high)?.checked_add(carry as _)?;
        Some(Self::from_parts(low, high))
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_sub(self, other: Self) -> Self {
        let (left_low, left_high) = self.as_parts();
        let (right_low, right_high) = other.as_parts();

        let (low, carry) = left_low.overflowing_sub(right_low);
        let high = left_high.wrapping_sub(right_high).wrapping_sub(carry as _);
        Self::from_parts(low, high)
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        let (left_low, left_high) = self.as_parts();
        let (right_low, right_high) = other.as_parts();

        let (low, carry) = left_low.overflowing_sub(right_low);
        let high = left_high.checked_sub(right_high)?.checked_sub(carry as _)?;
        Some(Self::from_parts(low, high))
    }

    /// Performs wrapping multiplication
    #[inline]
    pub fn wrapping_mul(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        Self::from_bigint_with_overflow(l * r).0
    }

    /// Performs checked multiplication
    #[inline]
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        let (val, overflow) = Self::from_bigint_with_overflow(l * r);
        (!overflow).then(|| val)
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        Self::from_bigint_with_overflow(l / r).0
    }

    /// Performs checked division
    #[inline]
    pub fn checked_div(self, other: Self) -> Option<Self> {
        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        let (val, overflow) = Self::from_bigint_with_overflow(l / r);
        (!overflow).then(|| val)
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        Self::from_bigint_with_overflow(l % r).0
    }

    /// Performs checked division
    #[inline]
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        if other.0 == [0; 32] {
            return None;
        }

        let l = BigInt::from_signed_bytes_le(&self.0);
        let r = BigInt::from_signed_bytes_le(&other.0);
        let (val, overflow) = Self::from_bigint_with_overflow(l % r);
        (!overflow).then(|| val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num::BigInt;
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
    fn test_i256() {
        let mut rng = thread_rng();

        for _ in 0..1000 {
            let mut l = [0_u8; 32];
            let len = rng.gen_range(0..32);
            l.iter_mut().take(len).for_each(|x| *x = rng.gen());

            let mut r = [0_u8; 32];
            let len = rng.gen_range(0..32);
            r.iter_mut().take(len).for_each(|x| *x = rng.gen());

            let il = i256::from_le_bytes(l);
            let ir = i256::from_le_bytes(r);

            let bl = BigInt::from_signed_bytes_le(&l);
            let br = BigInt::from_signed_bytes_le(&r);

            // Comparison
            assert_eq!(il.cmp(&ir), bl.cmp(&br), "{} cmp {}", bl, br);

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
        }
    }
}
