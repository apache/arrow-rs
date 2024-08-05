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

use arrow_buffer::{i256, ArrowNativeType, IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::ArrowError;
use half::f16;
use num::complex::ComplexFloat;
use std::cmp::Ordering;

/// Trait for [`ArrowNativeType`] that adds checked and unchecked arithmetic operations,
/// and totally ordered comparison operations
///
/// The APIs with `_wrapping` suffix do not perform overflow-checking. For integer
/// types they will wrap around the boundary of the type. For floating point types they
/// will overflow to INF or -INF preserving the expected sign value
///
/// Note `div_wrapping` and `mod_wrapping` will panic for integer types if `rhs` is zero
/// although this may be subject to change <https://github.com/apache/arrow-rs/issues/2647>
///
/// The APIs with `_checked` suffix perform overflow-checking. For integer types
/// these will return `Err` instead of wrapping. For floating point types they will
/// overflow to INF or -INF preserving the expected sign value
///
/// Comparison of integer types is as per normal integer comparison rules, floating
/// point values are compared as per IEEE 754's totalOrder predicate see [`f32::total_cmp`]
///
pub trait ArrowNativeTypeOp: ArrowNativeType {
    /// The additive identity
    const ZERO: Self;

    /// The multiplicative identity
    const ONE: Self;

    /// The minimum value and identity for the `max` aggregation.
    /// Note that the aggregation uses the total order predicate for floating point values,
    /// which means that this value is a negative NaN.
    const MIN_TOTAL_ORDER: Self;

    /// The maximum value and identity for the `min` aggregation.
    /// Note that the aggregation uses the total order predicate for floating point values,
    /// which means that this value is a positive NaN.
    const MAX_TOTAL_ORDER: Self;

    /// Checked addition operation
    fn add_checked(self, rhs: Self) -> Result<Self, ArrowError>;

    /// Wrapping addition operation
    fn add_wrapping(self, rhs: Self) -> Self;

    /// Checked subtraction operation
    fn sub_checked(self, rhs: Self) -> Result<Self, ArrowError>;

    /// Wrapping subtraction operation
    fn sub_wrapping(self, rhs: Self) -> Self;

    /// Checked multiplication operation
    fn mul_checked(self, rhs: Self) -> Result<Self, ArrowError>;

    /// Wrapping multiplication operation
    fn mul_wrapping(self, rhs: Self) -> Self;

    /// Checked division operation
    fn div_checked(self, rhs: Self) -> Result<Self, ArrowError>;

    /// Wrapping division operation
    fn div_wrapping(self, rhs: Self) -> Self;

    /// Checked remainder operation
    fn mod_checked(self, rhs: Self) -> Result<Self, ArrowError>;

    /// Wrapping remainder operation
    fn mod_wrapping(self, rhs: Self) -> Self;

    /// Checked negation operation
    fn neg_checked(self) -> Result<Self, ArrowError>;

    /// Wrapping negation operation
    fn neg_wrapping(self) -> Self;

    /// Checked exponentiation operation
    fn pow_checked(self, exp: u32) -> Result<Self, ArrowError>;

    /// Wrapping exponentiation operation
    fn pow_wrapping(self, exp: u32) -> Self;

    /// Returns true if zero else false
    fn is_zero(self) -> bool;

    /// Compare operation
    fn compare(self, rhs: Self) -> Ordering;

    /// Equality operation
    fn is_eq(self, rhs: Self) -> bool;

    /// Not equal operation
    #[inline]
    fn is_ne(self, rhs: Self) -> bool {
        !self.is_eq(rhs)
    }

    /// Less than operation
    #[inline]
    fn is_lt(self, rhs: Self) -> bool {
        self.compare(rhs).is_lt()
    }

    /// Less than equals operation
    #[inline]
    fn is_le(self, rhs: Self) -> bool {
        self.compare(rhs).is_le()
    }

    /// Greater than operation
    #[inline]
    fn is_gt(self, rhs: Self) -> bool {
        self.compare(rhs).is_gt()
    }

    /// Greater than equals operation
    #[inline]
    fn is_ge(self, rhs: Self) -> bool {
        self.compare(rhs).is_ge()
    }
}

macro_rules! native_type_op {
    ($t:tt) => {
        native_type_op!($t, 0, 1);
    };
    ($t:tt, $zero:expr, $one: expr) => {
        native_type_op!($t, $zero, $one, $t::MIN, $t::MAX);
    };
    ($t:tt, $zero:expr, $one: expr, $min: expr, $max: expr) => {
        impl ArrowNativeTypeOp for $t {
            const ZERO: Self = $zero;
            const ONE: Self = $one;
            const MIN_TOTAL_ORDER: Self = $min;
            const MAX_TOTAL_ORDER: Self = $max;

            #[inline]
            fn add_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                self.checked_add(rhs).ok_or_else(|| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Overflow happened on: {:?} + {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn add_wrapping(self, rhs: Self) -> Self {
                self.wrapping_add(rhs)
            }

            #[inline]
            fn sub_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                self.checked_sub(rhs).ok_or_else(|| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Overflow happened on: {:?} - {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn sub_wrapping(self, rhs: Self) -> Self {
                self.wrapping_sub(rhs)
            }

            #[inline]
            fn mul_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                self.checked_mul(rhs).ok_or_else(|| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Overflow happened on: {:?} * {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn mul_wrapping(self, rhs: Self) -> Self {
                self.wrapping_mul(rhs)
            }

            #[inline]
            fn div_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    self.checked_div(rhs).ok_or_else(|| {
                        ArrowError::ArithmeticOverflow(format!(
                            "Overflow happened on: {:?} / {:?}",
                            self, rhs
                        ))
                    })
                }
            }

            #[inline]
            fn div_wrapping(self, rhs: Self) -> Self {
                self.wrapping_div(rhs)
            }

            #[inline]
            fn mod_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    self.checked_rem(rhs).ok_or_else(|| {
                        ArrowError::ArithmeticOverflow(format!(
                            "Overflow happened on: {:?} % {:?}",
                            self, rhs
                        ))
                    })
                }
            }

            #[inline]
            fn mod_wrapping(self, rhs: Self) -> Self {
                self.wrapping_rem(rhs)
            }

            #[inline]
            fn neg_checked(self) -> Result<Self, ArrowError> {
                self.checked_neg().ok_or_else(|| {
                    ArrowError::ArithmeticOverflow(format!("Overflow happened on: - {:?}", self))
                })
            }

            #[inline]
            fn pow_checked(self, exp: u32) -> Result<Self, ArrowError> {
                self.checked_pow(exp).ok_or_else(|| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Overflow happened on: {:?} ^ {exp:?}",
                        self
                    ))
                })
            }

            #[inline]
            fn pow_wrapping(self, exp: u32) -> Self {
                self.wrapping_pow(exp)
            }

            #[inline]
            fn neg_wrapping(self) -> Self {
                self.wrapping_neg()
            }

            #[inline]
            fn is_zero(self) -> bool {
                self == Self::ZERO
            }

            #[inline]
            fn compare(self, rhs: Self) -> Ordering {
                self.cmp(&rhs)
            }

            #[inline]
            fn is_eq(self, rhs: Self) -> bool {
                self == rhs
            }
        }
    };
}

native_type_op!(i8);
native_type_op!(i16);
native_type_op!(i32);
native_type_op!(i64);
native_type_op!(i128);
native_type_op!(u8);
native_type_op!(u16);
native_type_op!(u32);
native_type_op!(u64);
native_type_op!(i256, i256::ZERO, i256::ONE, i256::MIN, i256::MAX);

native_type_op!(IntervalDayTime, IntervalDayTime::ZERO, IntervalDayTime::ONE);
native_type_op!(
    IntervalMonthDayNano,
    IntervalMonthDayNano::ZERO,
    IntervalMonthDayNano::ONE
);

macro_rules! native_type_float_op {
    ($t:tt, $zero:expr, $one:expr, $min:expr, $max:expr) => {
        impl ArrowNativeTypeOp for $t {
            const ZERO: Self = $zero;
            const ONE: Self = $one;
            const MIN_TOTAL_ORDER: Self = $min;
            const MAX_TOTAL_ORDER: Self = $max;

            #[inline]
            fn add_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                Ok(self + rhs)
            }

            #[inline]
            fn add_wrapping(self, rhs: Self) -> Self {
                self + rhs
            }

            #[inline]
            fn sub_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                Ok(self - rhs)
            }

            #[inline]
            fn sub_wrapping(self, rhs: Self) -> Self {
                self - rhs
            }

            #[inline]
            fn mul_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                Ok(self * rhs)
            }

            #[inline]
            fn mul_wrapping(self, rhs: Self) -> Self {
                self * rhs
            }

            #[inline]
            fn div_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    Ok(self / rhs)
                }
            }

            #[inline]
            fn div_wrapping(self, rhs: Self) -> Self {
                self / rhs
            }

            #[inline]
            fn mod_checked(self, rhs: Self) -> Result<Self, ArrowError> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    Ok(self % rhs)
                }
            }

            #[inline]
            fn mod_wrapping(self, rhs: Self) -> Self {
                self % rhs
            }

            #[inline]
            fn neg_checked(self) -> Result<Self, ArrowError> {
                Ok(-self)
            }

            #[inline]
            fn neg_wrapping(self) -> Self {
                -self
            }

            #[inline]
            fn pow_checked(self, exp: u32) -> Result<Self, ArrowError> {
                Ok(self.powi(exp as i32))
            }

            #[inline]
            fn pow_wrapping(self, exp: u32) -> Self {
                self.powi(exp as i32)
            }

            #[inline]
            fn is_zero(self) -> bool {
                self == $zero
            }

            #[inline]
            fn compare(self, rhs: Self) -> Ordering {
                <$t>::total_cmp(&self, &rhs)
            }

            #[inline]
            fn is_eq(self, rhs: Self) -> bool {
                // Equivalent to `self.total_cmp(&rhs).is_eq()`
                // but LLVM isn't able to realise this is bitwise equality
                // https://rust.godbolt.org/z/347nWGxoW
                self.to_bits() == rhs.to_bits()
            }
        }
    };
}

// the smallest/largest bit patterns for floating point numbers are NaN, but differ from the canonical NAN constants.
// See test_float_total_order_min_max for details.
native_type_float_op!(
    f16,
    f16::ZERO,
    f16::ONE,
    f16::from_bits(-1 as _),
    f16::from_bits(i16::MAX as _)
);
// from_bits is not yet stable as const fn, see https://github.com/rust-lang/rust/issues/72447
native_type_float_op!(
    f32,
    0.,
    1.,
    unsafe { std::mem::transmute(-1_i32) },
    unsafe { std::mem::transmute(i32::MAX) }
);
native_type_float_op!(
    f64,
    0.,
    1.,
    unsafe { std::mem::transmute(-1_i64) },
    unsafe { std::mem::transmute(i64::MAX) }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_type_is_zero() {
        assert!(0_i8.is_zero());
        assert!(0_i16.is_zero());
        assert!(0_i32.is_zero());
        assert!(0_i64.is_zero());
        assert!(0_i128.is_zero());
        assert!(i256::ZERO.is_zero());
        assert!(0_u8.is_zero());
        assert!(0_u16.is_zero());
        assert!(0_u32.is_zero());
        assert!(0_u64.is_zero());
        assert!(f16::ZERO.is_zero());
        assert!(0.0_f32.is_zero());
        assert!(0.0_f64.is_zero());
    }

    #[test]
    fn test_native_type_comparison() {
        // is_eq
        assert!(8_i8.is_eq(8_i8));
        assert!(8_i16.is_eq(8_i16));
        assert!(8_i32.is_eq(8_i32));
        assert!(8_i64.is_eq(8_i64));
        assert!(8_i128.is_eq(8_i128));
        assert!(i256::from_parts(8, 0).is_eq(i256::from_parts(8, 0)));
        assert!(8_u8.is_eq(8_u8));
        assert!(8_u16.is_eq(8_u16));
        assert!(8_u32.is_eq(8_u32));
        assert!(8_u64.is_eq(8_u64));
        assert!(f16::from_f32(8.0).is_eq(f16::from_f32(8.0)));
        assert!(8.0_f32.is_eq(8.0_f32));
        assert!(8.0_f64.is_eq(8.0_f64));

        // is_ne
        assert!(8_i8.is_ne(1_i8));
        assert!(8_i16.is_ne(1_i16));
        assert!(8_i32.is_ne(1_i32));
        assert!(8_i64.is_ne(1_i64));
        assert!(8_i128.is_ne(1_i128));
        assert!(i256::from_parts(8, 0).is_ne(i256::from_parts(1, 0)));
        assert!(8_u8.is_ne(1_u8));
        assert!(8_u16.is_ne(1_u16));
        assert!(8_u32.is_ne(1_u32));
        assert!(8_u64.is_ne(1_u64));
        assert!(f16::from_f32(8.0).is_ne(f16::from_f32(1.0)));
        assert!(8.0_f32.is_ne(1.0_f32));
        assert!(8.0_f64.is_ne(1.0_f64));

        // is_lt
        assert!(8_i8.is_lt(10_i8));
        assert!(8_i16.is_lt(10_i16));
        assert!(8_i32.is_lt(10_i32));
        assert!(8_i64.is_lt(10_i64));
        assert!(8_i128.is_lt(10_i128));
        assert!(i256::from_parts(8, 0).is_lt(i256::from_parts(10, 0)));
        assert!(8_u8.is_lt(10_u8));
        assert!(8_u16.is_lt(10_u16));
        assert!(8_u32.is_lt(10_u32));
        assert!(8_u64.is_lt(10_u64));
        assert!(f16::from_f32(8.0).is_lt(f16::from_f32(10.0)));
        assert!(8.0_f32.is_lt(10.0_f32));
        assert!(8.0_f64.is_lt(10.0_f64));

        // is_gt
        assert!(8_i8.is_gt(1_i8));
        assert!(8_i16.is_gt(1_i16));
        assert!(8_i32.is_gt(1_i32));
        assert!(8_i64.is_gt(1_i64));
        assert!(8_i128.is_gt(1_i128));
        assert!(i256::from_parts(8, 0).is_gt(i256::from_parts(1, 0)));
        assert!(8_u8.is_gt(1_u8));
        assert!(8_u16.is_gt(1_u16));
        assert!(8_u32.is_gt(1_u32));
        assert!(8_u64.is_gt(1_u64));
        assert!(f16::from_f32(8.0).is_gt(f16::from_f32(1.0)));
        assert!(8.0_f32.is_gt(1.0_f32));
        assert!(8.0_f64.is_gt(1.0_f64));
    }

    #[test]
    fn test_native_type_add() {
        // add_wrapping
        assert_eq!(8_i8.add_wrapping(2_i8), 10_i8);
        assert_eq!(8_i16.add_wrapping(2_i16), 10_i16);
        assert_eq!(8_i32.add_wrapping(2_i32), 10_i32);
        assert_eq!(8_i64.add_wrapping(2_i64), 10_i64);
        assert_eq!(8_i128.add_wrapping(2_i128), 10_i128);
        assert_eq!(
            i256::from_parts(8, 0).add_wrapping(i256::from_parts(2, 0)),
            i256::from_parts(10, 0)
        );
        assert_eq!(8_u8.add_wrapping(2_u8), 10_u8);
        assert_eq!(8_u16.add_wrapping(2_u16), 10_u16);
        assert_eq!(8_u32.add_wrapping(2_u32), 10_u32);
        assert_eq!(8_u64.add_wrapping(2_u64), 10_u64);
        assert_eq!(
            f16::from_f32(8.0).add_wrapping(f16::from_f32(2.0)),
            f16::from_f32(10.0)
        );
        assert_eq!(8.0_f32.add_wrapping(2.0_f32), 10_f32);
        assert_eq!(8.0_f64.add_wrapping(2.0_f64), 10_f64);

        // add_checked
        assert_eq!(8_i8.add_checked(2_i8).unwrap(), 10_i8);
        assert_eq!(8_i16.add_checked(2_i16).unwrap(), 10_i16);
        assert_eq!(8_i32.add_checked(2_i32).unwrap(), 10_i32);
        assert_eq!(8_i64.add_checked(2_i64).unwrap(), 10_i64);
        assert_eq!(8_i128.add_checked(2_i128).unwrap(), 10_i128);
        assert_eq!(
            i256::from_parts(8, 0)
                .add_checked(i256::from_parts(2, 0))
                .unwrap(),
            i256::from_parts(10, 0)
        );
        assert_eq!(8_u8.add_checked(2_u8).unwrap(), 10_u8);
        assert_eq!(8_u16.add_checked(2_u16).unwrap(), 10_u16);
        assert_eq!(8_u32.add_checked(2_u32).unwrap(), 10_u32);
        assert_eq!(8_u64.add_checked(2_u64).unwrap(), 10_u64);
        assert_eq!(
            f16::from_f32(8.0).add_checked(f16::from_f32(2.0)).unwrap(),
            f16::from_f32(10.0)
        );
        assert_eq!(8.0_f32.add_checked(2.0_f32).unwrap(), 10_f32);
        assert_eq!(8.0_f64.add_checked(2.0_f64).unwrap(), 10_f64);
    }

    #[test]
    fn test_native_type_sub() {
        // sub_wrapping
        assert_eq!(8_i8.sub_wrapping(2_i8), 6_i8);
        assert_eq!(8_i16.sub_wrapping(2_i16), 6_i16);
        assert_eq!(8_i32.sub_wrapping(2_i32), 6_i32);
        assert_eq!(8_i64.sub_wrapping(2_i64), 6_i64);
        assert_eq!(8_i128.sub_wrapping(2_i128), 6_i128);
        assert_eq!(
            i256::from_parts(8, 0).sub_wrapping(i256::from_parts(2, 0)),
            i256::from_parts(6, 0)
        );
        assert_eq!(8_u8.sub_wrapping(2_u8), 6_u8);
        assert_eq!(8_u16.sub_wrapping(2_u16), 6_u16);
        assert_eq!(8_u32.sub_wrapping(2_u32), 6_u32);
        assert_eq!(8_u64.sub_wrapping(2_u64), 6_u64);
        assert_eq!(
            f16::from_f32(8.0).sub_wrapping(f16::from_f32(2.0)),
            f16::from_f32(6.0)
        );
        assert_eq!(8.0_f32.sub_wrapping(2.0_f32), 6_f32);
        assert_eq!(8.0_f64.sub_wrapping(2.0_f64), 6_f64);

        // sub_checked
        assert_eq!(8_i8.sub_checked(2_i8).unwrap(), 6_i8);
        assert_eq!(8_i16.sub_checked(2_i16).unwrap(), 6_i16);
        assert_eq!(8_i32.sub_checked(2_i32).unwrap(), 6_i32);
        assert_eq!(8_i64.sub_checked(2_i64).unwrap(), 6_i64);
        assert_eq!(8_i128.sub_checked(2_i128).unwrap(), 6_i128);
        assert_eq!(
            i256::from_parts(8, 0)
                .sub_checked(i256::from_parts(2, 0))
                .unwrap(),
            i256::from_parts(6, 0)
        );
        assert_eq!(8_u8.sub_checked(2_u8).unwrap(), 6_u8);
        assert_eq!(8_u16.sub_checked(2_u16).unwrap(), 6_u16);
        assert_eq!(8_u32.sub_checked(2_u32).unwrap(), 6_u32);
        assert_eq!(8_u64.sub_checked(2_u64).unwrap(), 6_u64);
        assert_eq!(
            f16::from_f32(8.0).sub_checked(f16::from_f32(2.0)).unwrap(),
            f16::from_f32(6.0)
        );
        assert_eq!(8.0_f32.sub_checked(2.0_f32).unwrap(), 6_f32);
        assert_eq!(8.0_f64.sub_checked(2.0_f64).unwrap(), 6_f64);
    }

    #[test]
    fn test_native_type_mul() {
        // mul_wrapping
        assert_eq!(8_i8.mul_wrapping(2_i8), 16_i8);
        assert_eq!(8_i16.mul_wrapping(2_i16), 16_i16);
        assert_eq!(8_i32.mul_wrapping(2_i32), 16_i32);
        assert_eq!(8_i64.mul_wrapping(2_i64), 16_i64);
        assert_eq!(8_i128.mul_wrapping(2_i128), 16_i128);
        assert_eq!(
            i256::from_parts(8, 0).mul_wrapping(i256::from_parts(2, 0)),
            i256::from_parts(16, 0)
        );
        assert_eq!(8_u8.mul_wrapping(2_u8), 16_u8);
        assert_eq!(8_u16.mul_wrapping(2_u16), 16_u16);
        assert_eq!(8_u32.mul_wrapping(2_u32), 16_u32);
        assert_eq!(8_u64.mul_wrapping(2_u64), 16_u64);
        assert_eq!(
            f16::from_f32(8.0).mul_wrapping(f16::from_f32(2.0)),
            f16::from_f32(16.0)
        );
        assert_eq!(8.0_f32.mul_wrapping(2.0_f32), 16_f32);
        assert_eq!(8.0_f64.mul_wrapping(2.0_f64), 16_f64);

        // mul_checked
        assert_eq!(8_i8.mul_checked(2_i8).unwrap(), 16_i8);
        assert_eq!(8_i16.mul_checked(2_i16).unwrap(), 16_i16);
        assert_eq!(8_i32.mul_checked(2_i32).unwrap(), 16_i32);
        assert_eq!(8_i64.mul_checked(2_i64).unwrap(), 16_i64);
        assert_eq!(8_i128.mul_checked(2_i128).unwrap(), 16_i128);
        assert_eq!(
            i256::from_parts(8, 0)
                .mul_checked(i256::from_parts(2, 0))
                .unwrap(),
            i256::from_parts(16, 0)
        );
        assert_eq!(8_u8.mul_checked(2_u8).unwrap(), 16_u8);
        assert_eq!(8_u16.mul_checked(2_u16).unwrap(), 16_u16);
        assert_eq!(8_u32.mul_checked(2_u32).unwrap(), 16_u32);
        assert_eq!(8_u64.mul_checked(2_u64).unwrap(), 16_u64);
        assert_eq!(
            f16::from_f32(8.0).mul_checked(f16::from_f32(2.0)).unwrap(),
            f16::from_f32(16.0)
        );
        assert_eq!(8.0_f32.mul_checked(2.0_f32).unwrap(), 16_f32);
        assert_eq!(8.0_f64.mul_checked(2.0_f64).unwrap(), 16_f64);
    }

    #[test]
    fn test_native_type_div() {
        // div_wrapping
        assert_eq!(8_i8.div_wrapping(2_i8), 4_i8);
        assert_eq!(8_i16.div_wrapping(2_i16), 4_i16);
        assert_eq!(8_i32.div_wrapping(2_i32), 4_i32);
        assert_eq!(8_i64.div_wrapping(2_i64), 4_i64);
        assert_eq!(8_i128.div_wrapping(2_i128), 4_i128);
        assert_eq!(
            i256::from_parts(8, 0).div_wrapping(i256::from_parts(2, 0)),
            i256::from_parts(4, 0)
        );
        assert_eq!(8_u8.div_wrapping(2_u8), 4_u8);
        assert_eq!(8_u16.div_wrapping(2_u16), 4_u16);
        assert_eq!(8_u32.div_wrapping(2_u32), 4_u32);
        assert_eq!(8_u64.div_wrapping(2_u64), 4_u64);
        assert_eq!(
            f16::from_f32(8.0).div_wrapping(f16::from_f32(2.0)),
            f16::from_f32(4.0)
        );
        assert_eq!(8.0_f32.div_wrapping(2.0_f32), 4_f32);
        assert_eq!(8.0_f64.div_wrapping(2.0_f64), 4_f64);

        // div_checked
        assert_eq!(8_i8.div_checked(2_i8).unwrap(), 4_i8);
        assert_eq!(8_i16.div_checked(2_i16).unwrap(), 4_i16);
        assert_eq!(8_i32.div_checked(2_i32).unwrap(), 4_i32);
        assert_eq!(8_i64.div_checked(2_i64).unwrap(), 4_i64);
        assert_eq!(8_i128.div_checked(2_i128).unwrap(), 4_i128);
        assert_eq!(
            i256::from_parts(8, 0)
                .div_checked(i256::from_parts(2, 0))
                .unwrap(),
            i256::from_parts(4, 0)
        );
        assert_eq!(8_u8.div_checked(2_u8).unwrap(), 4_u8);
        assert_eq!(8_u16.div_checked(2_u16).unwrap(), 4_u16);
        assert_eq!(8_u32.div_checked(2_u32).unwrap(), 4_u32);
        assert_eq!(8_u64.div_checked(2_u64).unwrap(), 4_u64);
        assert_eq!(
            f16::from_f32(8.0).div_checked(f16::from_f32(2.0)).unwrap(),
            f16::from_f32(4.0)
        );
        assert_eq!(8.0_f32.div_checked(2.0_f32).unwrap(), 4_f32);
        assert_eq!(8.0_f64.div_checked(2.0_f64).unwrap(), 4_f64);
    }

    #[test]
    fn test_native_type_mod() {
        // mod_wrapping
        assert_eq!(9_i8.mod_wrapping(2_i8), 1_i8);
        assert_eq!(9_i16.mod_wrapping(2_i16), 1_i16);
        assert_eq!(9_i32.mod_wrapping(2_i32), 1_i32);
        assert_eq!(9_i64.mod_wrapping(2_i64), 1_i64);
        assert_eq!(9_i128.mod_wrapping(2_i128), 1_i128);
        assert_eq!(
            i256::from_parts(9, 0).mod_wrapping(i256::from_parts(2, 0)),
            i256::from_parts(1, 0)
        );
        assert_eq!(9_u8.mod_wrapping(2_u8), 1_u8);
        assert_eq!(9_u16.mod_wrapping(2_u16), 1_u16);
        assert_eq!(9_u32.mod_wrapping(2_u32), 1_u32);
        assert_eq!(9_u64.mod_wrapping(2_u64), 1_u64);
        assert_eq!(
            f16::from_f32(9.0).mod_wrapping(f16::from_f32(2.0)),
            f16::from_f32(1.0)
        );
        assert_eq!(9.0_f32.mod_wrapping(2.0_f32), 1_f32);
        assert_eq!(9.0_f64.mod_wrapping(2.0_f64), 1_f64);

        // mod_checked
        assert_eq!(9_i8.mod_checked(2_i8).unwrap(), 1_i8);
        assert_eq!(9_i16.mod_checked(2_i16).unwrap(), 1_i16);
        assert_eq!(9_i32.mod_checked(2_i32).unwrap(), 1_i32);
        assert_eq!(9_i64.mod_checked(2_i64).unwrap(), 1_i64);
        assert_eq!(9_i128.mod_checked(2_i128).unwrap(), 1_i128);
        assert_eq!(
            i256::from_parts(9, 0)
                .mod_checked(i256::from_parts(2, 0))
                .unwrap(),
            i256::from_parts(1, 0)
        );
        assert_eq!(9_u8.mod_checked(2_u8).unwrap(), 1_u8);
        assert_eq!(9_u16.mod_checked(2_u16).unwrap(), 1_u16);
        assert_eq!(9_u32.mod_checked(2_u32).unwrap(), 1_u32);
        assert_eq!(9_u64.mod_checked(2_u64).unwrap(), 1_u64);
        assert_eq!(
            f16::from_f32(9.0).mod_checked(f16::from_f32(2.0)).unwrap(),
            f16::from_f32(1.0)
        );
        assert_eq!(9.0_f32.mod_checked(2.0_f32).unwrap(), 1_f32);
        assert_eq!(9.0_f64.mod_checked(2.0_f64).unwrap(), 1_f64);
    }

    #[test]
    fn test_native_type_neg() {
        // neg_wrapping
        assert_eq!(8_i8.neg_wrapping(), -8_i8);
        assert_eq!(8_i16.neg_wrapping(), -8_i16);
        assert_eq!(8_i32.neg_wrapping(), -8_i32);
        assert_eq!(8_i64.neg_wrapping(), -8_i64);
        assert_eq!(8_i128.neg_wrapping(), -8_i128);
        assert_eq!(i256::from_parts(8, 0).neg_wrapping(), i256::from_i128(-8));
        assert_eq!(8_u8.neg_wrapping(), u8::MAX - 7_u8);
        assert_eq!(8_u16.neg_wrapping(), u16::MAX - 7_u16);
        assert_eq!(8_u32.neg_wrapping(), u32::MAX - 7_u32);
        assert_eq!(8_u64.neg_wrapping(), u64::MAX - 7_u64);
        assert_eq!(f16::from_f32(8.0).neg_wrapping(), f16::from_f32(-8.0));
        assert_eq!(8.0_f32.neg_wrapping(), -8_f32);
        assert_eq!(8.0_f64.neg_wrapping(), -8_f64);

        // neg_checked
        assert_eq!(8_i8.neg_checked().unwrap(), -8_i8);
        assert_eq!(8_i16.neg_checked().unwrap(), -8_i16);
        assert_eq!(8_i32.neg_checked().unwrap(), -8_i32);
        assert_eq!(8_i64.neg_checked().unwrap(), -8_i64);
        assert_eq!(8_i128.neg_checked().unwrap(), -8_i128);
        assert_eq!(
            i256::from_parts(8, 0).neg_checked().unwrap(),
            i256::from_i128(-8)
        );
        assert!(8_u8.neg_checked().is_err());
        assert!(8_u16.neg_checked().is_err());
        assert!(8_u32.neg_checked().is_err());
        assert!(8_u64.neg_checked().is_err());
        assert_eq!(
            f16::from_f32(8.0).neg_checked().unwrap(),
            f16::from_f32(-8.0)
        );
        assert_eq!(8.0_f32.neg_checked().unwrap(), -8_f32);
        assert_eq!(8.0_f64.neg_checked().unwrap(), -8_f64);
    }

    #[test]
    fn test_native_type_pow() {
        // pow_wrapping
        assert_eq!(8_i8.pow_wrapping(2_u32), 64_i8);
        assert_eq!(8_i16.pow_wrapping(2_u32), 64_i16);
        assert_eq!(8_i32.pow_wrapping(2_u32), 64_i32);
        assert_eq!(8_i64.pow_wrapping(2_u32), 64_i64);
        assert_eq!(8_i128.pow_wrapping(2_u32), 64_i128);
        assert_eq!(
            i256::from_parts(8, 0).pow_wrapping(2_u32),
            i256::from_parts(64, 0)
        );
        assert_eq!(8_u8.pow_wrapping(2_u32), 64_u8);
        assert_eq!(8_u16.pow_wrapping(2_u32), 64_u16);
        assert_eq!(8_u32.pow_wrapping(2_u32), 64_u32);
        assert_eq!(8_u64.pow_wrapping(2_u32), 64_u64);
        assert_eq!(f16::from_f32(8.0).pow_wrapping(2_u32), f16::from_f32(64.0));
        assert_eq!(8.0_f32.pow_wrapping(2_u32), 64_f32);
        assert_eq!(8.0_f64.pow_wrapping(2_u32), 64_f64);

        // pow_checked
        assert_eq!(8_i8.pow_checked(2_u32).unwrap(), 64_i8);
        assert_eq!(8_i16.pow_checked(2_u32).unwrap(), 64_i16);
        assert_eq!(8_i32.pow_checked(2_u32).unwrap(), 64_i32);
        assert_eq!(8_i64.pow_checked(2_u32).unwrap(), 64_i64);
        assert_eq!(8_i128.pow_checked(2_u32).unwrap(), 64_i128);
        assert_eq!(
            i256::from_parts(8, 0).pow_checked(2_u32).unwrap(),
            i256::from_parts(64, 0)
        );
        assert_eq!(8_u8.pow_checked(2_u32).unwrap(), 64_u8);
        assert_eq!(8_u16.pow_checked(2_u32).unwrap(), 64_u16);
        assert_eq!(8_u32.pow_checked(2_u32).unwrap(), 64_u32);
        assert_eq!(8_u64.pow_checked(2_u32).unwrap(), 64_u64);
        assert_eq!(
            f16::from_f32(8.0).pow_checked(2_u32).unwrap(),
            f16::from_f32(64.0)
        );
        assert_eq!(8.0_f32.pow_checked(2_u32).unwrap(), 64_f32);
        assert_eq!(8.0_f64.pow_checked(2_u32).unwrap(), 64_f64);
    }

    #[test]
    fn test_float_total_order_min_max() {
        assert!(<f64 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(f64::NEG_INFINITY));
        assert!(<f64 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f64::INFINITY));

        assert!(<f64 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_nan());
        assert!(<f64 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_sign_negative());
        assert!(<f64 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(-f64::NAN));

        assert!(<f64 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_nan());
        assert!(<f64 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_sign_positive());
        assert!(<f64 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f64::NAN));

        assert!(<f32 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(f32::NEG_INFINITY));
        assert!(<f32 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f32::INFINITY));

        assert!(<f32 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_nan());
        assert!(<f32 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_sign_negative());
        assert!(<f32 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(-f32::NAN));

        assert!(<f32 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_nan());
        assert!(<f32 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_sign_positive());
        assert!(<f32 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f32::NAN));

        assert!(<f16 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(f16::NEG_INFINITY));
        assert!(<f16 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f16::INFINITY));

        assert!(<f16 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_nan());
        assert!(<f16 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_sign_negative());
        assert!(<f16 as ArrowNativeTypeOp>::MIN_TOTAL_ORDER.is_lt(-f16::NAN));

        assert!(<f16 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_nan());
        assert!(<f16 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_sign_positive());
        assert!(<f16 as ArrowNativeTypeOp>::MAX_TOTAL_ORDER.is_gt(f16::NAN));
    }
}
