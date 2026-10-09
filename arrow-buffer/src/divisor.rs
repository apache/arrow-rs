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

macro_rules! divisor_enum {
    ($name:ident, $power_of_two:ident, $multiplier:ident, $t:ty) => {
        #[doc = concat!("A `", stringify!($t), "` divisor, precomputed once to divide many values by it")]
        ///
        /// Division by a divisor that is only known at run time needs a divide instruction.
        /// [`Self::new`] precomputes a shift and a mask when the magnitude of the divisor is a
        /// power of two, and otherwise a multiplier with the method in
        /// [Granlund and Montgomery (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf),
        /// so that each division is a high multiply, shifts and additions.
        ///
        /// To divide many values, match on the variant once, before the loop, and call the
        /// variant's methods in the loop. They don't branch on the divisor, so the loop doesn't
        /// depend on the compiler moving a branch out of it. Move the variant into the loop's
        /// closure, so that the compiler doesn't reload its fields through a reference for each
        /// value. [`Self::wrapping_div_rem`] matches on each call.
        ///
        /// # Example
        /// ```
        #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
        #[doc = concat!("let values: Vec<", stringify!($t), "> = vec![100, 7, 64];")]
        #[doc = concat!("let quotients: Vec<_> = match ", stringify!($name), "::new(7).unwrap() {")]
        #[doc = concat!("    ", stringify!($name), "::PowerOfTwo(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),")]
        #[doc = concat!("    ", stringify!($name), "::Multiplier(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),")]
        /// };
        /// assert_eq!(quotients, [14, 1, 9]);
        /// ```
        #[derive(Debug, Clone, Copy)]
        pub enum $name {
            /// A divisor whose magnitude is a power of two
            PowerOfTwo($power_of_two),
            /// Any other divisor
            Multiplier($multiplier),
        }

        impl $name {
            /// Returns a divisor for `divisor`, or `None` if `divisor` is zero
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("const SEVEN: ", stringify!($name), " = ", stringify!($name), "::new(7).unwrap();")]
            #[doc = concat!("assert!(", stringify!($name), "::new(0).is_none());")]
            /// ```
            pub const fn new(divisor: $t) -> Option<Self> {
                if divisor == 0 {
                    return None;
                }
                Some(match $power_of_two::new(divisor) {
                    Some(d) => Self::PowerOfTwo(d),
                    None => Self::Multiplier($multiplier::new(divisor)),
                })
            }

            /// Returns `(n.wrapping_div(divisor), n.wrapping_rem(divisor))`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let divisor = ", stringify!($name), "::new(7).unwrap();")]
            /// assert_eq!(divisor.wrapping_div_rem(100), (14, 2));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                match self {
                    Self::PowerOfTwo(d) => d.wrapping_div_rem(n),
                    Self::Multiplier(d) => d.wrapping_div_rem(n),
                }
            }
        }
    };
}

macro_rules! unsigned_divisor {
    ($name:ident, $power_of_two:ident, $multiplier:ident, $t:ty, $wide:ty) => {
        divisor_enum!($name, $power_of_two, $multiplier, $t);

        #[doc = concat!("A `", stringify!($t), "` divisor that is a power of two, see [`", stringify!($name), "`]")]
        #[derive(Debug, Clone, Copy)]
        pub struct $power_of_two {
            shift: u32,
            mask: $t,
        }

        impl $power_of_two {
            const fn new(divisor: $t) -> Option<Self> {
                if !divisor.is_power_of_two() {
                    return None;
                }
                Some(Self {
                    shift: divisor.trailing_zeros(),
                    mask: divisor - 1,
                })
            }

            /// Returns `(n / divisor, n % divisor)`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let ", stringify!($name), "::PowerOfTwo(divisor) = ", stringify!($name), "::new(8).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(100), (12, 4));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                (n >> self.shift, n & self.mask)
            }

        }

        #[doc = concat!("A `", stringify!($t), "` divisor that isn't a power of two, see [`", stringify!($name), "`]")]
        ///
        /// It uses the multiplier from figure 4.1 of Granlund and Montgomery (1994).
        #[derive(Debug, Clone, Copy)]
        pub struct $multiplier {
            divisor: $t,
            multiplier: $t,
            shift: u32,
        }

        impl $multiplier {
            /// `divisor` must be at least 3 and not a power of two
            const fn new(divisor: $t) -> Self {
                // ceil(log2(divisor)), which is at least 2 because divisor >= 3
                let l = <$t>::BITS - (divisor - 1).leading_zeros();
                // 2^l - divisor < divisor, so the multiplier fits in the narrow type
                let numerator = ((1 << l) - divisor as $wide) << <$t>::BITS;
                Self {
                    divisor,
                    multiplier: (numerator / divisor as $wide + 1) as $t,
                    shift: l - 1,
                }
            }

            /// Returns `(n / divisor, n % divisor)`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let ", stringify!($name), "::Multiplier(divisor) = ", stringify!($name), "::new(7).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(100), (14, 2));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let t = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                let q = (t + ((n - t) >> 1)) >> self.shift;
                (q, n - q * self.divisor)
            }

        }
    };
}

macro_rules! signed_divisor {
    ($name:ident, $power_of_two:ident, $multiplier:ident, $t:ty, $wide:ty) => {
        divisor_enum!($name, $power_of_two, $multiplier, $t);

        impl $name {
            #[doc = concat!("Returns `(n / divisor, n % divisor)`, or `None` for `", stringify!($t), "::MIN / -1`")]
            ///
            /// It matches on the variant on each call. Only [`Self::PowerOfTwo`] can return `None`.
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let divisor = ", stringify!($name), "::new(-7).unwrap();")]
            /// assert_eq!(divisor.checked_div_rem(-100), Some((14, -2)));
            ///
            #[doc = concat!("let minus_one = ", stringify!($name), "::new(-1).unwrap();")]
            #[doc = concat!("assert_eq!(minus_one.checked_div_rem(", stringify!($t), "::MIN), None);")]
            /// ```
            #[inline]
            pub const fn checked_div_rem(self, n: $t) -> Option<($t, $t)> {
                match self {
                    Self::PowerOfTwo(d) => d.checked_div_rem(n),
                    Self::Multiplier(d) => Some(d.wrapping_div_rem(n)),
                }
            }
        }

        #[doc = concat!("A `", stringify!($t), "` divisor whose magnitude is a power of two, see [`", stringify!($name), "`]")]
        #[derive(Debug, Clone, Copy)]
        pub struct $power_of_two {
            shift: u32,
            /// `|divisor| - 1`
            mask: $t,
            /// -1 if the divisor is negative, otherwise 0
            sign: $t,
        }

        impl $power_of_two {
            const fn new(divisor: $t) -> Option<Self> {
                let magnitude = divisor.unsigned_abs();
                if !magnitude.is_power_of_two() {
                    return None;
                }
                Some(Self {
                    shift: magnitude.trailing_zeros(),
                    mask: (magnitude - 1) as $t,
                    sign: divisor >> (<$t>::BITS - 1),
                })
            }

            #[doc = concat!("Returns `(n.wrapping_div(divisor), n.wrapping_rem(divisor))`, so `", stringify!($t), "::MIN / -1` returns `(", stringify!($t), "::MIN, 0)`")]
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let ", stringify!($name), "::PowerOfTwo(divisor) = ", stringify!($name), "::new(-8).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(-100), (12, -4));
            ///
            #[doc = concat!("let ", stringify!($name), "::PowerOfTwo(minus_one) = ", stringify!($name), "::new(-1).unwrap() else { unreachable!() };")]
            #[doc = concat!("assert_eq!(minus_one.wrapping_div_rem(", stringify!($t), "::MIN), (", stringify!($t), "::MIN, 0));")]
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                // Adding `|divisor| - 1` to a negative `n` makes the shift round toward zero
                let t = n + ((n >> (<$t>::BITS - 1)) & self.mask);
                let q = t >> self.shift;
                // `(x ^ sign) - sign` negates `x` when `sign` is -1, and wraps only for `MIN / -1`
                ((q ^ self.sign).wrapping_sub(self.sign), n - (t & !self.mask))
            }

            #[doc = concat!("Returns `(n / divisor, n % divisor)`, or `None` for `", stringify!($t), "::MIN / -1`")]
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let ", stringify!($name), "::PowerOfTwo(divisor) = ", stringify!($name), "::new(-8).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.checked_div_rem(-100), Some((12, -4)));
            ///
            #[doc = concat!("let ", stringify!($name), "::PowerOfTwo(minus_one) = ", stringify!($name), "::new(-1).unwrap() else { unreachable!() };")]
            #[doc = concat!("assert_eq!(minus_one.checked_div_rem(", stringify!($t), "::MIN), None);")]
            /// ```
            #[inline]
            pub const fn checked_div_rem(self, n: $t) -> Option<($t, $t)> {
                // -1 is the only divisor with a negative sign and a zero mask
                if n == <$t>::MIN && self.sign == -1 && self.mask == 0 {
                    None
                } else {
                    Some(self.wrapping_div_rem(n))
                }
            }
        }

        #[doc = concat!("A `", stringify!($t), "` divisor whose magnitude isn't a power of two, see [`", stringify!($name), "`]")]
        ///
        /// It uses the signed multiplier from figure 5.2 of Granlund and Montgomery (1994).
        #[derive(Debug, Clone, Copy)]
        pub struct $multiplier {
            divisor: $t,
            multiplier: $t,
            shift: u32,
        }

        impl $multiplier {
            /// `|divisor|` must be at least 3 and not a power of two
            const fn new(divisor: $t) -> Self {
                let magnitude = divisor.unsigned_abs();
                // ceil(log2(|divisor|)), which is at least 2 because |divisor| >= 3
                let l = <$t>::BITS - (magnitude - 1).leading_zeros();
                // m = 1 + floor(2^(N + l - 1) / |divisor|) is between 2^(N - 1) and 2^N, so
                // m - 2^N fits in the narrow type
                let m = 1 + (1 << (<$t>::BITS + l - 1)) / magnitude as $wide;
                Self {
                    divisor,
                    multiplier: (m - (1 << <$t>::BITS)) as $t,
                    shift: l - 1,
                }
            }

            /// Returns `(n / divisor, n % divisor)`, which can't overflow
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("let ", stringify!($name), "::Multiplier(divisor) = ", stringify!($name), "::new(-7).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(-100), (14, -2));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let high = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                // The shift rounds down, and subtracting the sign of `n` rounds toward zero instead
                let q = ((n + high) >> self.shift) - (n >> (<$t>::BITS - 1));
                let sign = self.divisor >> (<$t>::BITS - 1);
                let q = (q ^ sign) - sign;
                (q, n - q * self.divisor)
            }

        }
    };
}

unsigned_divisor!(
    DivisorU32,
    PowerOfTwoDivisorU32,
    MultiplierDivisorU32,
    u32,
    u64
);
unsigned_divisor!(
    DivisorU64,
    PowerOfTwoDivisorU64,
    MultiplierDivisorU64,
    u64,
    u128
);
signed_divisor!(
    DivisorI32,
    PowerOfTwoDivisorI32,
    MultiplierDivisorI32,
    i32,
    i64
);
signed_divisor!(
    DivisorI64,
    PowerOfTwoDivisorI64,
    MultiplierDivisorI64,
    i64,
    i128
);

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
                    let power_of_two = matches!(divisor, $divisor::PowerOfTwo(_));
                    assert_eq!(power_of_two, d.is_power_of_two(), "{d}");
                    let near = [d - 1, d, d.wrapping_add(1), d.wrapping_mul(7)];
                    for &n in values.iter().chain(&near) {
                        assert_eq!(divisor.wrapping_div_rem(n), (n / d, n % d), "{n} / {d}");
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
                    let power_of_two = matches!(divisor, $divisor::PowerOfTwo(_));
                    assert_eq!(power_of_two, d.unsigned_abs().is_power_of_two(), "{d}");
                    let near = [d.wrapping_sub(1), d, d.wrapping_add(1), d.wrapping_mul(7)];
                    for &n in values.iter().chain(&near) {
                        let expected = (n.wrapping_div(d), n.wrapping_rem(d));
                        assert_eq!(divisor.wrapping_div_rem(n), expected, "{n} / {d}");
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

    #[test]
    fn test_const_new() {
        const TEN: DivisorU64 = DivisorU64::new(10).unwrap();
        const MINUS_EIGHT: DivisorI32 = DivisorI32::new(-8).unwrap();
        assert_eq!(TEN.wrapping_div_rem(1234), (123, 4));
        assert_eq!(MINUS_EIGHT.wrapping_div_rem(-17), (2, -1));
    }
}
