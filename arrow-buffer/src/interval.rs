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
use std::ops::Neg;

/// Value of an IntervalMonthDayNano array
///
///  ## Representation
///
/// This type is stored as a single 128 bit integer, interpreted as three
/// different signed integral fields:
///
/// 1. The number of months (32 bits)
/// 2. The number days (32 bits)
/// 2. The number of nanoseconds (64 bits).
///
/// Nanoseconds does not allow for leap seconds.
///
/// Each field is independent (e.g. there is no constraint that the quantity of
/// nanoseconds represents less than a day's worth of time).
///
/// ```text
/// ┌───────────────┬─────────────┬─────────────────────────────┐
/// │     Months    │     Days    │            Nanos            │
/// │   (32 bits)   │  (32 bits)  │          (64 bits)          │
/// └───────────────┴─────────────┴─────────────────────────────┘
/// 0            32             64                           128 bit offset
/// ```
/// Please see the [Arrow Spec](https://github.com/apache/arrow/blob/081b4022fe6f659d8765efc82b3f4787c5039e3c/format/Schema.fbs#L409-L415) for more details
///
///## Note on Comparing and Ordering for Calendar Types
///
/// Values of `IntervalMonthDayNano` are compared using their binary
/// representation, which can lead to surprising results.
///
/// Spans of time measured in calendar units are not fixed in absolute size (e.g.
/// number of seconds) which makes defining comparisons and ordering non trivial.
/// For example `1 month` is 28 days for February but `1 month` is 31 days
/// in December.
///
/// This makes the seemingly simple operation of comparing two intervals
/// complicated in practice. For example is `1 month` more or less than `30
/// days`? The answer depends on what month you are talking about.
///
/// This crate defines comparisons for calendar types using their binary
/// representation which is fast and efficient, but leads
/// to potentially surprising results.
///
/// For example a
/// `IntervalMonthDayNano` of `1 month` will compare as **greater** than a
/// `IntervalMonthDayNano` of `100 days` because the binary representation of `1 month`
/// is larger than the binary representation of 100 days.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(C)]
pub struct IntervalMonthDayNano {
    /// Number of months
    pub months: i32,
    /// Number of days
    pub days: i32,
    /// Number of nanoseconds
    pub nanoseconds: i64,
}

impl IntervalMonthDayNano {
    /// The additive identity i.e. `0`.
    pub const ZERO: Self = Self::new(0, 0, 0);

    /// The multiplicative identity, i.e. `1`.
    pub const ONE: Self = Self::new(1, 1, 1);

    /// The multiplicative inverse, i.e. `-1`.
    pub const MINUS_ONE: Self = Self::new(-1, -1, -1);

    /// The maximum value that can be represented
    pub const MAX: Self = Self::new(i32::MAX, i32::MAX, i64::MAX);

    /// The minimum value that can be represented
    pub const MIN: Self = Self::new(i32::MIN, i32::MIN, i64::MIN);

    /// Create a new [`IntervalMonthDayNano`]
    #[inline]
    pub const fn new(months: i32, days: i32, nanoseconds: i64) -> Self {
        Self {
            months,
            days,
            nanoseconds,
        }
    }

    /// Computes the absolute value
    #[inline]
    pub fn wrapping_abs(self) -> Self {
        Self {
            months: self.months.wrapping_abs(),
            days: self.days.wrapping_abs(),
            nanoseconds: self.nanoseconds.wrapping_abs(),
        }
    }

    /// Computes the absolute value
    #[inline]
    pub fn checked_abs(self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_abs()?,
            days: self.days.checked_abs()?,
            nanoseconds: self.nanoseconds.checked_abs()?,
        })
    }

    /// Negates the value
    #[inline]
    pub fn wrapping_neg(self) -> Self {
        Self {
            months: self.months.wrapping_neg(),
            days: self.days.wrapping_neg(),
            nanoseconds: self.nanoseconds.wrapping_neg(),
        }
    }

    /// Negates the value
    #[inline]
    pub fn checked_neg(self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_neg()?,
            days: self.days.checked_neg()?,
            nanoseconds: self.nanoseconds.checked_neg()?,
        })
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_add(self, other: Self) -> Self {
        Self {
            months: self.months.wrapping_add(other.months),
            days: self.days.wrapping_add(other.days),
            nanoseconds: self.nanoseconds.wrapping_add(other.nanoseconds),
        }
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_add(other.months)?,
            days: self.days.checked_add(other.days)?,
            nanoseconds: self.nanoseconds.checked_add(other.nanoseconds)?,
        })
    }

    /// Performs wrapping subtraction
    #[inline]
    pub fn wrapping_sub(self, other: Self) -> Self {
        Self {
            months: self.months.wrapping_sub(other.months),
            days: self.days.wrapping_sub(other.days),
            nanoseconds: self.nanoseconds.wrapping_sub(other.nanoseconds),
        }
    }

    /// Performs checked subtraction
    #[inline]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_sub(other.months)?,
            days: self.days.checked_sub(other.days)?,
            nanoseconds: self.nanoseconds.checked_sub(other.nanoseconds)?,
        })
    }

    /// Performs wrapping multiplication
    #[inline]
    pub fn wrapping_mul(self, other: Self) -> Self {
        Self {
            months: self.months.wrapping_mul(other.months),
            days: self.days.wrapping_mul(other.days),
            nanoseconds: self.nanoseconds.wrapping_mul(other.nanoseconds),
        }
    }

    /// Performs checked multiplication
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_mul(other.months)?,
            days: self.days.checked_mul(other.days)?,
            nanoseconds: self.nanoseconds.checked_mul(other.nanoseconds)?,
        })
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        Self {
            months: self.months.wrapping_div(other.months),
            days: self.days.wrapping_div(other.days),
            nanoseconds: self.nanoseconds.wrapping_div(other.nanoseconds),
        }
    }

    /// Performs checked division
    pub fn checked_div(self, other: Self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_div(other.months)?,
            days: self.days.checked_div(other.days)?,
            nanoseconds: self.nanoseconds.checked_div(other.nanoseconds)?,
        })
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        Self {
            months: self.months.wrapping_rem(other.months),
            days: self.days.wrapping_rem(other.days),
            nanoseconds: self.nanoseconds.wrapping_rem(other.nanoseconds),
        }
    }

    /// Performs checked remainder
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        Some(Self {
            months: self.months.checked_rem(other.months)?,
            days: self.days.checked_rem(other.days)?,
            nanoseconds: self.nanoseconds.checked_rem(other.nanoseconds)?,
        })
    }

    /// Performs wrapping exponentiation
    #[inline]
    pub fn wrapping_pow(self, exp: u32) -> Self {
        Self {
            months: self.months.wrapping_pow(exp),
            days: self.days.wrapping_pow(exp),
            nanoseconds: self.nanoseconds.wrapping_pow(exp),
        }
    }

    /// Performs checked exponentiation
    #[inline]
    pub fn checked_pow(self, exp: u32) -> Option<Self> {
        Some(Self {
            months: self.months.checked_pow(exp)?,
            days: self.days.checked_pow(exp)?,
            nanoseconds: self.nanoseconds.checked_pow(exp)?,
        })
    }
}

impl Neg for IntervalMonthDayNano {
    type Output = Self;

    #[cfg(debug_assertions)]
    fn neg(self) -> Self::Output {
        self.checked_neg().expect("IntervalMonthDayNano overflow")
    }

    #[cfg(not(debug_assertions))]
    fn neg(self) -> Self::Output {
        self.wrapping_neg()
    }
}

derive_arith!(
    IntervalMonthDayNano,
    Add,
    AddAssign,
    add,
    add_assign,
    wrapping_add,
    checked_add
);
derive_arith!(
    IntervalMonthDayNano,
    Sub,
    SubAssign,
    sub,
    sub_assign,
    wrapping_sub,
    checked_sub
);
derive_arith!(
    IntervalMonthDayNano,
    Mul,
    MulAssign,
    mul,
    mul_assign,
    wrapping_mul,
    checked_mul
);
derive_arith!(
    IntervalMonthDayNano,
    Div,
    DivAssign,
    div,
    div_assign,
    wrapping_div,
    checked_div
);
derive_arith!(
    IntervalMonthDayNano,
    Rem,
    RemAssign,
    rem,
    rem_assign,
    wrapping_rem,
    checked_rem
);

/// Value of an IntervalDayTime array
///
/// ## Representation
///
/// This type is stored as a single 64 bit integer, interpreted as two i32
/// fields:
///
/// 1. the number of elapsed days
/// 2. The number of milliseconds (no leap seconds),
///
/// ```text
/// ┌──────────────┬──────────────┐
/// │     Days     │ Milliseconds │
/// │  (32 bits)   │  (32 bits)   │
/// └──────────────┴──────────────┘
/// 0              31            63 bit offset
/// ```
///
/// Please see the [Arrow Spec](https://github.com/apache/arrow/blob/081b4022fe6f659d8765efc82b3f4787c5039e3c/format/Schema.fbs#L406-L408) for more details
///
/// ## Note on Comparing and Ordering for Calendar Types
///
/// Values of `IntervalDayTime` are compared using their binary representation,
/// which can lead to surprising results. Please see the description of ordering on
/// [`IntervalMonthDayNano`] for more details
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(C)]
pub struct IntervalDayTime {
    /// Number of days
    pub days: i32,
    /// Number of milliseconds
    pub milliseconds: i32,
}

impl IntervalDayTime {
    /// The additive identity i.e. `0`.
    pub const ZERO: Self = Self::new(0, 0);

    /// The multiplicative identity, i.e. `1`.
    pub const ONE: Self = Self::new(1, 1);

    /// The multiplicative inverse, i.e. `-1`.
    pub const MINUS_ONE: Self = Self::new(-1, -1);

    /// The maximum value that can be represented
    pub const MAX: Self = Self::new(i32::MAX, i32::MAX);

    /// The minimum value that can be represented
    pub const MIN: Self = Self::new(i32::MIN, i32::MIN);

    /// Create a new [`IntervalDayTime`]
    #[inline]
    pub const fn new(days: i32, milliseconds: i32) -> Self {
        Self { days, milliseconds }
    }

    /// Computes the absolute value
    #[inline]
    pub fn wrapping_abs(self) -> Self {
        Self {
            days: self.days.wrapping_abs(),
            milliseconds: self.milliseconds.wrapping_abs(),
        }
    }

    /// Computes the absolute value
    #[inline]
    pub fn checked_abs(self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_abs()?,
            milliseconds: self.milliseconds.checked_abs()?,
        })
    }

    /// Negates the value
    #[inline]
    pub fn wrapping_neg(self) -> Self {
        Self {
            days: self.days.wrapping_neg(),
            milliseconds: self.milliseconds.wrapping_neg(),
        }
    }

    /// Negates the value
    #[inline]
    pub fn checked_neg(self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_neg()?,
            milliseconds: self.milliseconds.checked_neg()?,
        })
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_add(self, other: Self) -> Self {
        Self {
            days: self.days.wrapping_add(other.days),
            milliseconds: self.milliseconds.wrapping_add(other.milliseconds),
        }
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_add(other.days)?,
            milliseconds: self.milliseconds.checked_add(other.milliseconds)?,
        })
    }

    /// Performs wrapping subtraction
    #[inline]
    pub fn wrapping_sub(self, other: Self) -> Self {
        Self {
            days: self.days.wrapping_sub(other.days),
            milliseconds: self.milliseconds.wrapping_sub(other.milliseconds),
        }
    }

    /// Performs checked subtraction
    #[inline]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_sub(other.days)?,
            milliseconds: self.milliseconds.checked_sub(other.milliseconds)?,
        })
    }

    /// Performs wrapping multiplication
    #[inline]
    pub fn wrapping_mul(self, other: Self) -> Self {
        Self {
            days: self.days.wrapping_mul(other.days),
            milliseconds: self.milliseconds.wrapping_mul(other.milliseconds),
        }
    }

    /// Performs checked multiplication
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_mul(other.days)?,
            milliseconds: self.milliseconds.checked_mul(other.milliseconds)?,
        })
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        Self {
            days: self.days.wrapping_div(other.days),
            milliseconds: self.milliseconds.wrapping_div(other.milliseconds),
        }
    }

    /// Performs checked division
    pub fn checked_div(self, other: Self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_div(other.days)?,
            milliseconds: self.milliseconds.checked_div(other.milliseconds)?,
        })
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        Self {
            days: self.days.wrapping_rem(other.days),
            milliseconds: self.milliseconds.wrapping_rem(other.milliseconds),
        }
    }

    /// Performs checked remainder
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        Some(Self {
            days: self.days.checked_rem(other.days)?,
            milliseconds: self.milliseconds.checked_rem(other.milliseconds)?,
        })
    }

    /// Performs wrapping exponentiation
    #[inline]
    pub fn wrapping_pow(self, exp: u32) -> Self {
        Self {
            days: self.days.wrapping_pow(exp),
            milliseconds: self.milliseconds.wrapping_pow(exp),
        }
    }

    /// Performs checked exponentiation
    #[inline]
    pub fn checked_pow(self, exp: u32) -> Option<Self> {
        Some(Self {
            days: self.days.checked_pow(exp)?,
            milliseconds: self.milliseconds.checked_pow(exp)?,
        })
    }
}

impl Neg for IntervalDayTime {
    type Output = Self;

    #[cfg(debug_assertions)]
    fn neg(self) -> Self::Output {
        self.checked_neg().expect("IntervalDayMillisecond overflow")
    }

    #[cfg(not(debug_assertions))]
    fn neg(self) -> Self::Output {
        self.wrapping_neg()
    }
}

derive_arith!(
    IntervalDayTime,
    Add,
    AddAssign,
    add,
    add_assign,
    wrapping_add,
    checked_add
);
derive_arith!(
    IntervalDayTime,
    Sub,
    SubAssign,
    sub,
    sub_assign,
    wrapping_sub,
    checked_sub
);
derive_arith!(
    IntervalDayTime,
    Mul,
    MulAssign,
    mul,
    mul_assign,
    wrapping_mul,
    checked_mul
);
derive_arith!(
    IntervalDayTime,
    Div,
    DivAssign,
    div,
    div_assign,
    wrapping_div,
    checked_div
);
derive_arith!(
    IntervalDayTime,
    Rem,
    RemAssign,
    rem,
    rem_assign,
    wrapping_rem,
    checked_rem
);
