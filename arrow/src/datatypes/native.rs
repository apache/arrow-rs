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

use crate::error::{ArrowError, Result};
pub use arrow_array::ArrowPrimitiveType;
pub use arrow_buffer::{ArrowNativeType, ToByteSlice};
use half::f16;

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

    fn add_checked(self, rhs: Self) -> Result<Self>;

    fn add_wrapping(self, rhs: Self) -> Self;

    fn sub_checked(self, rhs: Self) -> Result<Self>;

    fn sub_wrapping(self, rhs: Self) -> Self;

    fn mul_checked(self, rhs: Self) -> Result<Self>;

    fn mul_wrapping(self, rhs: Self) -> Self;

    fn div_checked(self, rhs: Self) -> Result<Self>;

    fn div_wrapping(self, rhs: Self) -> Self;

    fn mod_checked(self, rhs: Self) -> Result<Self>;

    fn mod_wrapping(self, rhs: Self) -> Self;

    fn neg_checked(self) -> Result<Self>;

    fn neg_wrapping(self) -> Self;

    fn is_zero(self) -> bool;

    fn is_eq(self, rhs: Self) -> bool;

    fn is_ne(self, rhs: Self) -> bool;

    fn is_lt(self, rhs: Self) -> bool;

    fn is_le(self, rhs: Self) -> bool;

    fn is_gt(self, rhs: Self) -> bool;

    fn is_ge(self, rhs: Self) -> bool;
}

macro_rules! native_type_op {
    ($t:tt) => {
        impl ArrowNativeTypeOp for $t {
            const ZERO: Self = 0;
            const ONE: Self = 1;

            fn add_checked(self, rhs: Self) -> Result<Self> {
                self.checked_add(rhs).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Overflow happened on: {:?} + {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn add_wrapping(self, rhs: Self) -> Self {
                self.wrapping_add(rhs)
            }

            fn sub_checked(self, rhs: Self) -> Result<Self> {
                self.checked_sub(rhs).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Overflow happened on: {:?} - {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn sub_wrapping(self, rhs: Self) -> Self {
                self.wrapping_sub(rhs)
            }

            fn mul_checked(self, rhs: Self) -> Result<Self> {
                self.checked_mul(rhs).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Overflow happened on: {:?} * {:?}",
                        self, rhs
                    ))
                })
            }

            #[inline]
            fn mul_wrapping(self, rhs: Self) -> Self {
                self.wrapping_mul(rhs)
            }

            fn div_checked(self, rhs: Self) -> Result<Self> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    self.checked_div(rhs).ok_or_else(|| {
                        ArrowError::ComputeError(format!(
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

            fn mod_checked(self, rhs: Self) -> Result<Self> {
                if rhs.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    self.checked_rem(rhs).ok_or_else(|| {
                        ArrowError::ComputeError(format!(
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

            fn neg_checked(self) -> Result<Self> {
                self.checked_neg().ok_or_else(|| {
                    ArrowError::ComputeError(format!("Overflow happened on: {:?}", self))
                })
            }

            #[inline]
            fn neg_wrapping(self) -> Self {
                self.wrapping_neg()
            }

            #[inline]
            fn is_zero(self) -> bool {
                self == 0
            }

            #[inline]
            fn is_eq(self, rhs: Self) -> bool {
                self == rhs
            }

            #[inline]
            fn is_ne(self, rhs: Self) -> bool {
                self != rhs
            }

            #[inline]
            fn is_lt(self, rhs: Self) -> bool {
                self < rhs
            }

            #[inline]
            fn is_le(self, rhs: Self) -> bool {
                self <= rhs
            }

            #[inline]
            fn is_gt(self, rhs: Self) -> bool {
                self > rhs
            }

            #[inline]
            fn is_ge(self, rhs: Self) -> bool {
                self >= rhs
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

macro_rules! native_type_float_op {
    ($t:tt, $zero:expr, $one:expr) => {
        impl ArrowNativeTypeOp for $t {
            const ZERO: Self = $zero;
            const ONE: Self = $one;

            #[inline]
            fn add_checked(self, rhs: Self) -> Result<Self> {
                Ok(self + rhs)
            }

            #[inline]
            fn add_wrapping(self, rhs: Self) -> Self {
                self + rhs
            }

            #[inline]
            fn sub_checked(self, rhs: Self) -> Result<Self> {
                Ok(self - rhs)
            }

            #[inline]
            fn sub_wrapping(self, rhs: Self) -> Self {
                self - rhs
            }

            #[inline]
            fn mul_checked(self, rhs: Self) -> Result<Self> {
                Ok(self * rhs)
            }

            #[inline]
            fn mul_wrapping(self, rhs: Self) -> Self {
                self * rhs
            }

            fn div_checked(self, rhs: Self) -> Result<Self> {
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

            fn mod_checked(self, rhs: Self) -> Result<Self> {
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
            fn neg_checked(self) -> Result<Self> {
                Ok(-self)
            }

            #[inline]
            fn neg_wrapping(self) -> Self {
                -self
            }

            #[inline]
            fn is_zero(self) -> bool {
                self == $zero
            }

            #[inline]
            fn is_eq(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_eq()
            }

            #[inline]
            fn is_ne(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_ne()
            }

            #[inline]
            fn is_lt(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_lt()
            }

            #[inline]
            fn is_le(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_le()
            }

            #[inline]
            fn is_gt(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_gt()
            }

            #[inline]
            fn is_ge(self, rhs: Self) -> bool {
                self.total_cmp(&rhs).is_ge()
            }
        }
    };
}

native_type_float_op!(f16, f16::ONE, f16::ZERO);
native_type_float_op!(f32, 0., 1.);
native_type_float_op!(f64, 0., 1.);
