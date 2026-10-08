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

//! Integer divisors for dividing many values by the same divisor

macro_rules! unsigned_divisor {
    ($name:ident, $t:ty, $wide:ty) => {
        #[doc = concat!("A `", stringify!($t), "` divisor with a precomputed multiplier, for dividing many values by it")]
        ///
        /// Division by a divisor that is only known at run time needs a divide instruction
        /// or a library call. This type computes a multiplier once, with the method in
        /// figure 4.1 of [Granlund and Montgomery (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf),
        /// so that each division is a high multiply, two shifts, an addition and a subtraction.
        /// A divisor that is a power of two uses a shift and a mask instead.
        ///
        /// Create it once, before a loop, and call [`Self::div_rem`] in the loop.
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            shift: u32,
            power_of_two: bool,
        }

        impl $name {
            /// Returns a divisor for `divisor`, or `None` if `divisor` is zero
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("assert!(", stringify!($name), "::new(7).is_some());")]
            #[doc = concat!("assert!(", stringify!($name), "::new(0).is_none());")]
            /// ```
            pub fn new(divisor: $t) -> Option<Self> {
                if divisor == 0 {
                    return None;
                }
                if divisor.is_power_of_two() {
                    return Some(Self {
                        divisor,
                        multiplier: 0,
                        shift: divisor.trailing_zeros(),
                        power_of_two: true,
                    });
                }
                // ceil(log2(divisor)), which is at least 2 because divisor >= 3
                let l = <$t>::BITS - (divisor - 1).leading_zeros();
                // 2^l - divisor < divisor, so the multiplier fits in the narrow type
                let numerator = ((1 << l) - divisor as $wide) << <$t>::BITS;
                Some(Self {
                    divisor,
                    multiplier: (numerator / divisor as $wide + 1) as $t,
                    shift: l - 1,
                    power_of_two: false,
                })
            }

            /// Returns `(n / divisor, n % divisor)`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let divisor = ", stringify!($name), "::new(7).unwrap();")]
            /// assert_eq!(divisor.div_rem(100), (14, 2));
            /// ```
            #[inline]
            pub fn div_rem(&self, n: $t) -> ($t, $t) {
                // The branch doesn't depend on `n`, so LLVM can move it out of a loop
                if self.power_of_two {
                    return (n >> self.shift, n & (self.divisor - 1));
                }
                let t = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                let q = (t + ((n - t) >> 1)) >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

macro_rules! signed_divisor {
    ($name:ident, $t:ty, $unsigned:ident) => {
        #[doc = concat!("A `", stringify!($t), "` divisor with a precomputed multiplier, for dividing many values by it")]
        ///
        #[doc = concat!("It divides the magnitudes with [`", stringify!($unsigned), "`] and then applies the signs, so")]
        #[doc = concat!("the quotient truncates toward zero and the remainder has the sign of the dividend,")]
        #[doc = concat!("like `", stringify!($t), "::wrapping_div` and `", stringify!($t), "::wrapping_rem`.")]
        ///
        /// Create it once, before a loop, and call [`Self::div_rem`] or
        /// [`Self::checked_div_rem`] in the loop.
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            magnitude: $unsigned,
            /// -1 if the divisor is negative, otherwise 0
            sign: $t,
        }

        impl $name {
            /// Returns a divisor for `divisor`, or `None` if `divisor` is zero
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("assert!(", stringify!($name), "::new(-7).is_some());")]
            #[doc = concat!("assert!(", stringify!($name), "::new(0).is_none());")]
            /// ```
            pub fn new(divisor: $t) -> Option<Self> {
                Some(Self {
                    magnitude: $unsigned::new(divisor.unsigned_abs())?,
                    sign: divisor >> (<$t>::BITS - 1),
                })
            }

            #[doc = concat!("Returns `(n.wrapping_div(divisor), n.wrapping_rem(divisor))`, so `", stringify!($t), "::MIN / -1` returns `(", stringify!($t), "::MIN, 0)`")]
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let divisor = ", stringify!($name), "::new(2).unwrap();")]
            /// assert_eq!(divisor.div_rem(-7), (-3, -1));
            ///
            #[doc = concat!("let minus_one = ", stringify!($name), "::new(-1).unwrap();")]
            #[doc = concat!("assert_eq!(minus_one.div_rem(", stringify!($t), "::MIN), (", stringify!($t), "::MIN, 0));")]
            /// ```
            #[inline]
            pub fn div_rem(&self, n: $t) -> ($t, $t) {
                let (q, r) = self.magnitude.div_rem(n.unsigned_abs());
                // `(x ^ sign) - sign` negates `x` when `sign` is -1. Unlike an `if`, LLVM
                // doesn't turn it into a select that it computes the division on both sides of.
                // `q as $t` wraps only for `MIN / 1` and `MIN / -1`, where it gives `MIN` like
                // `wrapping_div`.
                let n_sign = n >> (<$t>::BITS - 1);
                let q_sign = n_sign ^ self.sign;
                (
                    (q as $t ^ q_sign).wrapping_sub(q_sign),
                    (r as $t ^ n_sign).wrapping_sub(n_sign),
                )
            }

            #[doc = concat!("Returns `(n / divisor, n % divisor)`, or `None` if the quotient overflows, which happens only for `", stringify!($t), "::MIN / -1`")]
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let divisor = ", stringify!($name), "::new(2).unwrap();")]
            /// assert_eq!(divisor.checked_div_rem(-7), Some((-3, -1)));
            ///
            #[doc = concat!("let minus_one = ", stringify!($name), "::new(-1).unwrap();")]
            #[doc = concat!("assert_eq!(minus_one.checked_div_rem(", stringify!($t), "::MIN), None);")]
            /// ```
            #[inline]
            pub fn checked_div_rem(&self, n: $t) -> Option<($t, $t)> {
                let minus_one = self.sign == -1 && self.magnitude.divisor == 1;
                (n != <$t>::MIN || !minus_one).then(|| self.div_rem(n))
            }
        }
    };
}

unsigned_divisor!(DivisorU32, u32, u64);
unsigned_divisor!(DivisorU64, u64, u128);
signed_divisor!(DivisorI32, i32, DivisorU32);
signed_divisor!(DivisorI64, i64, DivisorU64);

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    macro_rules! test_unsigned {
        ($name:ident, $divisor:ident, $t:ty) => {
            #[test]
            #[cfg_attr(miri, ignore)] // Takes too long
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;

                let mut divisors: Vec<$t> = (1..=2000).collect();
                divisors.extend([<$t>::MAX, <$t>::MAX - 1]);
                for k in 0..bits {
                    let p = 1 << k;
                    divisors.extend([p, p - 1, p + 1]);
                }
                let mut p: $t = 10;
                while let Some(next) = p.checked_mul(10) {
                    divisors.extend([p - 1, p, p + 1]);
                    p = next;
                }
                divisors.extend((0..500).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                divisors.retain(|d| *d != 0);

                let mut values: Vec<$t> = vec![0, 1, 2, <$t>::MAX, <$t>::MAX - 1];
                values.extend([
                    1 << (bits - 1),
                    (1 << (bits - 1)) - 1,
                    (1 << (bits - 1)) + 1,
                ]);
                values.extend((0..500).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));

                assert!($divisor::new(0).is_none());
                for d in divisors {
                    let divisor = $divisor::new(d).unwrap();
                    let near = [d - 1, d, d.wrapping_add(1), d.wrapping_mul(7)];
                    for &n in values.iter().chain(&near) {
                        assert_eq!(divisor.div_rem(n), (n / d, n % d), "{n} / {d}");
                    }
                }
            }
        };
    }

    macro_rules! test_signed {
        ($name:ident, $divisor:ident, $t:ty, $u:ty) => {
            #[test]
            #[cfg_attr(miri, ignore)] // Takes too long
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;

                let mut magnitudes: Vec<$t> = (1..=1000).collect();
                for k in 0..bits - 1 {
                    let p: $t = 1 << k;
                    magnitudes.extend([p, p - 1, p + 1]);
                }
                let mut p: $t = 10;
                while let Some(next) = p.checked_mul(10) {
                    magnitudes.extend([p - 1, p, p + 1]);
                    p = next;
                }
                magnitudes.extend(
                    (0..200).map(|_| (rng.random::<$u>() >> rng.random_range(1..bits)) as $t),
                );

                let mut divisors: Vec<$t> = magnitudes.iter().flat_map(|d| [*d, -*d]).collect();
                divisors.extend([<$t>::MIN, <$t>::MAX, -1, 1]);
                divisors.retain(|d| *d != 0);

                let mut values: Vec<$t> = vec![0, 1, -1, <$t>::MIN, <$t>::MAX, <$t>::MIN + 1];
                values.extend((0..200).flat_map(|_| {
                    let v = (rng.random::<$u>() >> rng.random_range(1..bits)) as $t;
                    [v, -v]
                }));

                assert!($divisor::new(0).is_none());
                for d in divisors {
                    let divisor = $divisor::new(d).unwrap();
                    let near = [d.wrapping_sub(1), d, d.wrapping_add(1), d.wrapping_mul(7)];
                    for &n in values.iter().chain(&near) {
                        let expected = (n.wrapping_div(d), n.wrapping_rem(d));
                        assert_eq!(divisor.div_rem(n), expected, "{n} / {d}");
                        let checked = n.checked_div(d).zip(n.checked_rem(d));
                        assert_eq!(divisor.checked_div_rem(n), checked, "{n} / {d}");
                    }
                }
            }
        };
    }

    test_unsigned!(test_divisor_u32, DivisorU32, u32);
    test_unsigned!(test_divisor_u64, DivisorU64, u64);
    test_signed!(test_divisor_i32, DivisorI32, i32, u32);
    test_signed!(test_divisor_i64, DivisorI64, i64, u64);
}
