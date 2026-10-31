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

/// Whether a saturating increment on `u32` lanes is cheap: one `uqadd` on aarch64, and `pminud`
/// from SSE4.1 and a subtract on x86. Baseline x86-64 has no unsigned 32-bit min and emulates it
/// with compares, so there the saturating increment of [`DivisorU32::Down`] costs more than the
/// add and halve of [`DivisorU32::AddIndicator`]
const CHEAP_SATURATION: bool = cfg!(any(
    target_arch = "aarch64",
    all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_feature = "sse4.1"
    )
));

macro_rules! divisor_enum {
    ($name:ident, $t:ty, $($(#[$doc:meta])* $variant:ident($inner:ident),)+) => {
        #[doc = concat!("A `", stringify!($t), "` divisor, precomputed once to divide many values by it")]
        ///
        /// Division by a divisor that is only known at run time needs a divide instruction.
        /// [`Self::new`] precomputes a shift and a mask when the magnitude of the divisor is a
        /// power of two, and otherwise a multiplier, so that each division is a high multiply,
        /// shifts and additions. [`Self::new`] picks the variant from the divisor and the target.
        ///
        /// To divide many values, match on the variant once, before the loop, and call the
        /// variant's methods in the loop. They don't branch on the divisor, so the loop doesn't
        /// depend on the compiler moving a branch out of it. Move the variant into the loop's
        /// closure, so that the compiler doesn't reload its fields through a reference for each
        /// value. [`Self::wrapping_div_rem`] matches on each call, so a match can fall back to it
        /// for the variants it doesn't specialize, including variants added later.
        ///
        /// # Example
        /// ```
        #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
        #[doc = concat!("let values: Vec<", stringify!($t), "> = vec![100, 7, 64];")]
        #[doc = concat!("let quotients: Vec<_> = match ", stringify!($name), "::new(7).unwrap() {")]
        #[doc = concat!("    ", stringify!($name), "::PowerOfTwo(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),")]
        #[doc = concat!("    ", stringify!($name), "::AddIndicator(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),")]
        ///     d => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),
        /// };
        /// assert_eq!(quotients, [14, 1, 9]);
        /// ```
        #[derive(Debug, Clone, Copy)]
        #[non_exhaustive]
        pub enum $name {
            $($(#[$doc])* $variant($inner),)+
        }

        impl $name {
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
                    $(Self::$variant(d) => d.wrapping_div_rem(n),)+
                }
            }
        }
    };
}

/// A divisor with an N-bit multiplier, which computes the quotient as `MULUH(m, n') >> shift`
/// for `n'` from `$body`. `Up` and `Down` differ only in `n'`, and the macro keeps them from
/// duplicating the rest. `|n| ...` looks like a closure but is macro syntax for the expression,
/// because a `const fn` can't call a closure
macro_rules! n_bit_divisor {
    ($(#[$doc:meta])* $name:ident, $enum:ident, $t:ty, $wide:ty, |$n:ident| $body:expr, $example:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            shift: u32,
        }

        impl $name {
            /// Returns `(n / divisor, n % divisor)`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($enum), ";")]
            #[doc = $example]
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let $n = n;
                let n1: $t = $body;
                let high = ((self.multiplier as $wide * n1 as $wide) >> <$t>::BITS) as $t;
                let q = high >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

macro_rules! unsigned_divisor {
    ($name:ident, $power_of_two:ident, $up:ident, $add:ident, $t:ty, $wide:ty $(, $down:ident)?) => {
        divisor_enum!(
            $name,
            $t,
            /// A divisor that is a power of two
            PowerOfTwo($power_of_two),
            /// A divisor whose rounded-up multiplier is exact in N bits
            Up($up),
            $(
                /// A divisor whose rounded-up multiplier isn't exact in N bits, with the
                /// rounded-down multiplier, on targets where `u32` saturating adds are cheap
                Down($down),
            )?
            /// Any other divisor
            AddIndicator($add),
        );

        impl $name {
            /// Returns a divisor for `divisor`, or `None` if `divisor` is zero
            ///
            /// With `p = floor(log2(divisor))`, it picks [`Self::Up`] when the rounded-up
            /// multiplier `floor(2^(N+p) / divisor) + 1` satisfies theorem 4.2 of
            /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf).
            /// Checking one `p` is enough, because a multiplier that is exact for a smaller `p` is
            /// also exact for this one, see
            /// [ridiculous_fish, "Labor of Division (Episode I)"](https://ridiculousfish.com/blog/posts/labor-of-division-episode-i.html).
            $(
                #[doc = concat!("Otherwise it picks [`Self::Down`] (a [`", stringify!($down), "`]) on aarch64 and on x86 with SSE4.1, where a saturating increment takes one or two vector instructions.")]
            )?
            /// It picks [`Self::AddIndicator`] for any divisor the other variants don't cover.
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
                if let Some(d) = $power_of_two::new(divisor) {
                    return Some(Self::PowerOfTwo(d));
                }
                let p = <$t>::BITS - 1 - divisor.leading_zeros();
                let one: $wide = 1;
                let pow = one << (<$t>::BITS + p);
                let down = pow / divisor as $wide;
                // divisor > 2^p, so down + 1 < 2^N
                if (down + 1) * divisor as $wide - pow <= one << p {
                    return Some(Self::Up($up {
                        divisor,
                        multiplier: (down + 1) as $t,
                        shift: p,
                    }));
                }
                $(
                    // When the rounded-up multiplier isn't exact, the rounded-down one with the
                    // dividend increased by 1 is, by the proof of universality in
                    // "Labor of Division (Episode III)"
                    if CHEAP_SATURATION {
                        return Some(Self::Down($down {
                            divisor,
                            multiplier: down as $t,
                            shift: p,
                        }));
                    }
                )?
                Some(Self::AddIndicator($add::new(divisor, p, pow, down)))
            }
        }

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

        n_bit_divisor!(
            #[doc = concat!("A `", stringify!($t), "` divisor whose rounded-up multiplier is exact in N bits, see [`", stringify!($name), "`]")]
            ///
            /// It computes `MULUH(m, n) >> shift`, where `m` satisfies theorem 4.2 of
            /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf)
            /// with `l = shift` and fits in N bits, so it needs no add and halve as in figure 4.1.
            $up,
            $name,
            $t,
            $wide,
            |n| n,
            concat!("let ", stringify!($name), "::Up(divisor) = ", stringify!($name), "::new(13).unwrap() else { unreachable!() };\nassert_eq!(divisor.wrapping_div_rem(100), (7, 9));")
        );

        #[doc = concat!("A `", stringify!($t), "` divisor that isn't a power of two, see [`", stringify!($name), "`]")]
        ///
        /// It uses the `N + 1`-bit multiplier from figure 4.1 of
        /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf).
        $(
            #[doc = ""]
            #[doc = concat!("[`", stringify!($name), "::new`] returns it only on targets without [`", stringify!($down), "`], so the example checks nothing on aarch64 and on x86 with SSE4.1.")]
        )?
        #[derive(Debug, Clone, Copy)]
        pub struct $add {
            divisor: $t,
            /// `m'` in figure 4.1
            multiplier: $t,
            /// `sh2` in figure 4.1
            shift: u32,
        }

        impl $add {
            /// `divisor` must be at least 3 and not a power of two, `p` is `floor(log2(divisor))`,
            /// `pow` is `2^(N+p)` and `down` is `pow / divisor`
            const fn new(divisor: $t, p: u32, pow: $wide, down: $wide) -> Self {
                // Figure 4.1 uses `l = ceil(log2(divisor)) = p + 1`, so `sh1` is 1, `sh2` is `p`
                // and `m' = floor(2^(N+l) / divisor) - 2^N + 1`. `2^(N+l)` is `2 * pow`, and
                // `floor(2a / d) = 2 * floor(a / d) + (2 * (a mod d) >= d)`, so `new` gets the
                // quotient from `down` and its remainder instead of a second wide division, which
                // is a call to `__udivti3` for `u64`. `2^l < 2 * divisor`, so `m'` fits in N bits
                let below = pow - down * divisor as $wide;
                let carry = (2 * below >= divisor as $wide) as $wide;
                Self {
                    divisor,
                    multiplier: (2 * down + carry + 1 - (pow >> p)) as $t,
                    shift: p,
                }
            }

            /// Returns `(n / divisor, n % divisor)`
            ///
            /// # Example
            /// ```
            #[doc = concat!("# use arrow_buffer::", stringify!($name), ";")]
            #[doc = concat!("if let ", stringify!($name), "::AddIndicator(divisor) = ", stringify!($name), "::new(7).unwrap() {")]
            ///     assert_eq!(divisor.wrapping_div_rem(100), (14, 2));
            /// }
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let t = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                // `(n + t) >> l` could overflow, so this halves `n - t` first, as figure 4.1 does
                let q = (t + ((n - t) >> 1)) >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

macro_rules! signed_divisor {
    ($name:ident, $power_of_two:ident, $add:ident, $t:ty, $u:ty, $wide:ty, $uwide:ty) => {
        divisor_enum!(
            $name,
            $t,
            /// A divisor whose magnitude is a power of two
            PowerOfTwo($power_of_two),
            /// Any other divisor
            AddIndicator($add),
        );

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
                    None => Self::AddIndicator($add::new(divisor)),
                })
            }

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
                    Self::AddIndicator(d) => Some(d.wrapping_div_rem(n)),
                }
            }
        }

        #[doc = concat!("A `", stringify!($t), "` divisor whose magnitude is a power of two, see [`", stringify!($name), "`]")]
        ///
        /// The quotient and remainder follow sections 10-1 and 10-2 of Warren, "Hacker's Delight",
        /// 2nd edition. Those sections compute the addend `2^k - 1` for a negative `n` with two
        /// shifts. This type computes it with one shift that spreads the sign bit of `n` and an AND
        /// with `2^k - 1`, which it computes once for the divisor.
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
                // `t & !mask` is `n` rounded toward zero to a multiple of `|divisor|`
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
        /// It uses the signed multiplier from figure 5.1 of
        /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf).
        #[derive(Debug, Clone, Copy)]
        pub struct $add {
            divisor: $t,
            /// `m'` in figure 5.1
            multiplier: $t,
            /// `sh_post` in figure 5.1
            shift: u32,
        }

        impl $add {
            /// `|divisor|` must be at least 3 and not a power of two
            const fn new(divisor: $t) -> Self {
                let magnitude = divisor.unsigned_abs();
                // ceil(log2(|divisor|)), which is at least 2 because |divisor| >= 3, so figure
                // 5.1's `max(..., 1)` isn't needed
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
            #[doc = concat!("let ", stringify!($name), "::AddIndicator(divisor) = ", stringify!($name), "::new(-7).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(-100), (14, -2));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                // Signed high-product correction as in libdivide_mullhi_s64_vec512:
                // https://github.com/ridiculousfish/libdivide/blob/master/libdivide.h#L2530-L2537
                // Since m is negative, the -n correction cancels the original +n.
                let high = (((self.multiplier as $u as $uwide) * (n as $u as $uwide))
                    >> <$t>::BITS) as $t;
                let t = high.wrapping_sub((n >> (<$t>::BITS - 1)) & self.multiplier);
                // The shift rounds down; subtracting the sign rounds toward zero.
                let q = (t >> self.shift) - (n >> (<$t>::BITS - 1));
                let sign = self.divisor >> (<$t>::BITS - 1);
                // The remainder doesn't depend on the sign of the divisor, as section 3.1 of
                // Lemire, Kaser and Kurz, "Faster Remainder by Direct Computation",
                // https://arxiv.org/abs/1902.01961, states, so it uses `q` before the sign fix
                // and `|divisor|`. `divisor` is never `MIN`, whose magnitude is a power of two
                let magnitude = (self.divisor ^ sign) - sign;
                ((q ^ sign) - sign, n - q * magnitude)
            }
        }
    };
}

unsigned_divisor!(
    DivisorU32,
    PowerOfTwoDivisorU32,
    UpDivisorU32,
    AddIndicatorDivisorU32,
    u32,
    u64,
    DownDivisorU32
);
unsigned_divisor!(
    DivisorU64,
    PowerOfTwoDivisorU64,
    UpDivisorU64,
    AddIndicatorDivisorU64,
    u64,
    u128
);
signed_divisor!(
    DivisorI32,
    PowerOfTwoDivisorI32,
    AddIndicatorDivisorI32,
    i32,
    u32,
    i64,
    u64
);
signed_divisor!(
    DivisorI64,
    PowerOfTwoDivisorI64,
    AddIndicatorDivisorI64,
    i64,
    u64,
    i128,
    u128
);

n_bit_divisor!(
    /// A `u32` divisor whose rounded-up multiplier isn't exact in N bits, see [`DivisorU32`]
    ///
    /// It computes `MULUH(m, n + 1) >> shift` with the rounded-down multiplier
    /// `m = floor(2^(N+shift) / divisor)`, from
    /// [ridiculous_fish, "Labor of Division (Episode III)"](https://ridiculousfish.com/blog/posts/labor-of-division-episode-iii.html).
    /// The increment saturates, so `u32::MAX` gives the quotient of `u32::MAX - 1`. The two are
    /// equal unless the divisor divides `2^N - 1`, and those divisors always have an exact
    /// rounded-up multiplier, as the post proves, so they never use this type.
    ///
    /// [`DivisorU32::new`] returns it only on aarch64 and on x86 with SSE4.1, so the example
    /// checks nothing on other targets.
    DownDivisorU32,
    DivisorU32,
    u32,
    u64,
    |n| n.saturating_add(1),
    "if let DivisorU32::Down(divisor) = DivisorU32::new(7).unwrap() {\n    assert_eq!(divisor.wrapping_div_rem(100), (14, 2));\n}"
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
                    // The quotient is monotone in `n`, so an error shows first at an end of a
                    // quotient step `[k * d, k * d + d - 1]`, and the last steps are where the
                    // multiplier's error is largest
                    let top = <$t>::MAX / d * d;
                    let near = [d - 1, d, d.wrapping_add(1), d.wrapping_mul(7)];
                    let ends = [top, top.wrapping_sub(1), top.wrapping_sub(d)];
                    for &n in values.iter().chain(&near).chain(&ends) {
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
                    // The ends of the last quotient steps on each side of zero, as in the
                    // unsigned test
                    let ad = d.unsigned_abs();
                    let top = <$t>::MAX as $u / ad * ad;
                    let bottom = <$t>::MIN.unsigned_abs() / ad * ad;
                    let ends = [top, top.wrapping_sub(1), top.wrapping_sub(ad)]
                        .map(|n| n as $t)
                        .into_iter()
                        .chain(
                            [bottom, bottom.wrapping_sub(1), bottom.wrapping_sub(ad)]
                                .map(|n| n.wrapping_neg() as $t),
                        )
                        .collect::<Vec<_>>();
                    let near = [d.wrapping_sub(1), d, d.wrapping_add(1), d.wrapping_mul(7)];
                    for &n in values.iter().chain(&near).chain(&ends) {
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

    /// Checks which variant `new` picks: `Up` when theorem 4.2 holds at `p = floor(log2(d))`,
    /// otherwise `Down` when `$down` is true, otherwise `AddIndicator`. `$is_down` checks for `Down`,
    /// which only `u32` has
    macro_rules! test_unsigned_variant {
        ($name:ident, $divisor:ident, $t:ty, $wide:ty, $down:expr, $is_down:expr, $primes:expr) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut divisors: Vec<$t> = (1..=3000).collect();
                divisors.extend([<$t>::MAX, <$t>::MAX - 1, (1 << (bits - 1)) + 1]);
                // Every divisor of 2^N - 1, whose rounded-up multiplier always fits in N bits,
                // which `Down`'s saturating increment relies on
                let primes: &[$t] = &$primes;
                assert_eq!(primes.iter().product::<$t>(), <$t>::MAX);
                let mut factors = vec![];
                for subset in 1..1u32 << primes.len() {
                    let d = (0..primes.len())
                        .filter(|i| subset & (1 << i) != 0)
                        .fold(1, |d, i| d * primes[i]);
                    factors.push(d);
                }
                divisors.extend(&factors);
                divisors.extend((0..2000).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                divisors.retain(|d| *d != 0);

                for d in divisors {
                    let divisor = $divisor::new(d).unwrap();
                    if d.is_power_of_two() {
                        assert!(matches!(divisor, $divisor::PowerOfTwo(_)), "{d}");
                        continue;
                    }
                    let p = bits - 1 - d.leading_zeros();
                    let one: $wide = 1;
                    let pow = one << (bits + p);
                    let up = pow / d as $wide + 1;
                    if up * d as $wide - pow <= 1 << p {
                        assert!(matches!(divisor, $divisor::Up(_)), "{d}");
                    } else if $down {
                        assert!(!factors.contains(&d), "{d}");
                        assert!($is_down(&divisor), "{d}");
                    } else {
                        assert!(matches!(divisor, $divisor::AddIndicator(_)), "{d}");
                    }
                }
                for d in factors
                    .into_iter()
                    .filter(|d| *d > 1 && !d.is_power_of_two())
                {
                    let divisor = $divisor::new(d).unwrap();
                    assert!(matches!(divisor, $divisor::Up(_)), "{d}");
                    // `Down` would be wrong for these divisors at `MAX`, so check the values too.
                    // `d` divides `MAX`, so `MAX` is the end of the last quotient step
                    let max = <$t>::MAX;
                    for n in [0, 1, d - 1, d, d.wrapping_add(1), max, max - 1, max - d] {
                        assert_eq!(divisor.wrapping_div_rem(n), (n / d, n % d), "{n} / {d}");
                    }
                }
            }
        };
    }

    test_unsigned_variant!(
        test_variant_u32,
        DivisorU32,
        u32,
        u64,
        CHEAP_SATURATION,
        |d: &DivisorU32| matches!(d, DivisorU32::Down(_)),
        [3, 5, 17, 257, 65537]
    );
    test_unsigned_variant!(
        test_variant_u64,
        DivisorU64,
        u64,
        u128,
        false,
        |_: &DivisorU64| false,
        [3, 5, 17, 257, 641, 65537, 6700417]
    );

    /// `DivisorU32::new` picks `AddIndicator` only on targets without cheap saturation, so this
    /// checks it directly for the divisors that would use it
    #[test]
    fn test_add_indicator_u32() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut divisors: Vec<u32> = (3..=3000).collect();
        divisors.extend([u32::MAX - 1, (1 << 31) + 1]);
        divisors.extend((0..2000).map(|_| rng.random::<u32>() >> rng.random_range(0..32)));
        divisors.retain(|d| *d >= 3 && !d.is_power_of_two());
        for d in divisors {
            let p = u32::BITS - 1 - d.leading_zeros();
            let pow = 1u64 << (u32::BITS + p);
            let divisor = AddIndicatorDivisorU32::new(d, p, pow, pow / d as u64);
            let top = u32::MAX / d * d;
            let values = [
                0,
                1,
                d - 1,
                d,
                d + 1,
                top,
                top - 1,
                top - d,
                u32::MAX,
                u32::MAX - 1,
            ];
            for n in values {
                assert_eq!(divisor.wrapping_div_rem(n), (n / d, n % d), "{n} / {d}");
            }
        }
    }

    #[test]
    fn test_variant_signed() {
        for d in [3, -3, 7, -7, 13, -13, 100, -100, i32::MAX, i32::MIN + 1] {
            assert!(
                matches!(DivisorI32::new(d).unwrap(), DivisorI32::AddIndicator(_)),
                "{d}"
            );
            let d = d as i64;
            assert!(
                matches!(DivisorI64::new(d).unwrap(), DivisorI64::AddIndicator(_)),
                "{d}"
            );
        }
    }

    #[test]
    fn test_const_new() {
        const TEN: DivisorU64 = DivisorU64::new(10).unwrap();
        const MINUS_EIGHT: DivisorI32 = DivisorI32::new(-8).unwrap();
        assert_eq!(TEN.wrapping_div_rem(1234), (123, 4));
        assert_eq!(MINUS_EIGHT.wrapping_div_rem(-17), (2, -1));
    }
}
