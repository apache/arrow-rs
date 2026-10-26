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
        /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf),
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
        /// It uses the multiplier from figure 4.1 of
        /// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf).
        #[derive(Debug, Clone, Copy)]
        pub struct $multiplier {
            divisor: $t,
            /// `m'` in figure 4.1
            multiplier: $t,
            /// `sh2` in figure 4.1
            shift: u32,
        }

        impl $multiplier {
            /// `divisor` must be at least 3 and not a power of two
            const fn new(divisor: $t) -> Self {
                // ceil(log2(divisor)), which is at least 2 because divisor >= 3. So figure 4.1's
                // `sh1 = min(l, 1)` is always 1 and `sh2 = max(l - 1, 0)` is `l - 1`
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
                // `(n + t) >> l` could overflow, so this halves `n - t` first, as figure 4.1 does
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
        pub struct $multiplier {
            divisor: $t,
            /// `m'` in figure 5.1
            multiplier: $t,
            /// `sh_post` in figure 5.1
            shift: u32,
        }

        impl $multiplier {
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
            #[doc = concat!("let ", stringify!($name), "::Multiplier(divisor) = ", stringify!($name), "::new(-7).unwrap() else { unreachable!() };")]
            /// assert_eq!(divisor.wrapping_div_rem(-100), (14, -2));
            /// ```
            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                // `MULSH(m', n)` in figure 5.1
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

// Experiment: the `m < 2^N` case of figure 4.2 of Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994),
// https://gmplib.org/~tege/divcnst-pldi94.pdf,
// where the multiplier fits in N bits and the quotient is `MULUH(m, n) >> p`, without figure 4.1's
// add and halve. The search over `p` checks theorem 4.2. ridiculous_fish, "Labor of Division
// (Episode I)", https://ridiculousfish.com/blog/posts/labor-of-division-episode-i.html, shows that
// only `p = floor(log2(d))` needs checking, because a multiplier that works for a smaller `p` also
// works for every larger one, and libdivide checks only that `p`:
// https://github.com/ridiculousfish/libdivide/blob/53b36fcacfd031ec0a0837db759f057eb03bacbe/libdivide.h#L940-L989
macro_rules! fig42_no_pre_shift {
    ($name:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            shift: u32,
        }

        impl $name {
            /// Returns `None` if `divisor` is below 3, a power of two, or needs the fixup
            pub const fn try_new(divisor: $t) -> Option<Self> {
                if divisor < 3 || divisor.is_power_of_two() {
                    return None;
                }
                let l = <$t>::BITS - (divisor - 1).leading_zeros();
                // m < 2^N needs 2^p < divisor, so p < l
                let mut p = 0;
                while p < l {
                    let pow = (1 as $wide) << (<$t>::BITS + p);
                    let m = pow / divisor as $wide + 1;
                    // Theorem 4.2 of Granlund and Montgomery: 2^(N+p) <= m*d <= 2^(N+p) + 2^p
                    if m < (1 as $wide) << <$t>::BITS && m * divisor as $wide - pow <= 1 << p {
                        return Some(Self {
                            divisor,
                            multiplier: m as $t,
                            shift: p,
                        });
                    }
                    p += 1;
                }
                None
            }

            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let high = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                let q = high >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

fig42_no_pre_shift!(Fig42NoPreShiftDivisorU32, u32, u64);
fig42_no_pre_shift!(Fig42NoPreShiftDivisorU64, u64, u128);

// Experiment: CHOOSE_MULTIPLIER from figure 6.2 of Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994),
// https://gmplib.org/~tege/divcnst-pldi94.pdf. `d` must be between 3
// and `2^(N-1)`, so that `2^(N+l) + 2^(N+l-prec)` fits in the wide type
macro_rules! choose_multiplier {
    ($fn:ident, $t:ty, $wide:ty) => {
        const fn $fn(d: $t, prec: u32) -> ($wide, u32) {
            let l = <$t>::BITS - (d - 1).leading_zeros();
            let mut post_shift = l;
            let base = (1 as $wide) << (<$t>::BITS + l);
            let mut m_low = base / d as $wide;
            let mut m_high = (base + ((1 as $wide) << (<$t>::BITS + l - prec))) / d as $wide;
            while m_low / 2 < m_high / 2 && post_shift > 0 {
                m_low /= 2;
                m_high /= 2;
                post_shift -= 1;
            }
            (m_high, post_shift)
        }
    };
}

choose_multiplier!(choose_multiplier_u32, u32, u64);
choose_multiplier!(choose_multiplier_u64, u64, u128);

// Experiment: the two N-bit cases of figure 4.2 of Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994),
// https://gmplib.org/~tege/divcnst-pldi94.pdf,
// `MULUH(m, n >> pre_shift) >> post_shift`, where `pre_shift` is 0 unless the divisor is even and
// its multiplier doesn't fit in N bits. "Labor of Division (Episode III)", section "A Shift In Time
// Saves Fourteen", describes the same pre-shift
macro_rules! fig42 {
    ($name:ident, $choose:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            pre_shift: u32,
            post_shift: u32,
        }

        impl $name {
            /// Returns `None` if `divisor` is below 3, a power of two, above `2^(N-1)`, or odd
            /// with a multiplier that doesn't fit in N bits
            pub const fn try_new(divisor: $t) -> Option<Self> {
                if divisor < 3 || divisor.is_power_of_two() || divisor > 1 << (<$t>::BITS - 1) {
                    return None;
                }
                let fits = (1 as $wide) << <$t>::BITS;
                let (mut multiplier, mut post_shift) = $choose(divisor, <$t>::BITS);
                let mut pre_shift = 0;
                if multiplier >= fits {
                    if divisor % 2 == 1 {
                        return None;
                    }
                    pre_shift = divisor.trailing_zeros();
                    let (m, s) = $choose(divisor >> pre_shift, <$t>::BITS - pre_shift);
                    if m >= fits {
                        return None;
                    }
                    multiplier = m;
                    post_shift = s;
                }
                Some(Self {
                    divisor,
                    multiplier: multiplier as $t,
                    pre_shift,
                    post_shift,
                })
            }

            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let n_odd = n >> self.pre_shift;
                let high = ((self.multiplier as $wide * n_odd as $wide) >> <$t>::BITS) as $t;
                let q = high >> self.post_shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

fig42!(Fig42DivisorU32, choose_multiplier_u32, u32, u64);
fig42!(Fig42DivisorU64, choose_multiplier_u64, u64, u128);

// Experiment: the `m < 2^(N-1)` case of figure 5.2 of Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994),
// https://gmplib.org/~tege/divcnst-pldi94.pdf,
// `(MULSH(m, n) >> post_shift) - XSIGN(n)`, negated for a negative divisor. "Labor of Division
// (Episode III)", section "Extension to Signed Division", expects little gain for signed division
macro_rules! fig52 {
    ($name:ident, $choose:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            post_shift: u32,
            /// -1 if the divisor is negative, otherwise 0
            sign: $t,
        }

        impl $name {
            /// Returns `None` if `|divisor|` is below 3, a power of two, or has a multiplier that
            /// doesn't fit in N - 1 bits
            pub const fn try_new(divisor: $t) -> Option<Self> {
                let magnitude = divisor.unsigned_abs();
                if magnitude < 3 || magnitude.is_power_of_two() {
                    return None;
                }
                let (multiplier, post_shift) = $choose(magnitude, <$t>::BITS - 1);
                if multiplier >= 1 << (<$t>::BITS - 1) {
                    return None;
                }
                Some(Self {
                    divisor,
                    multiplier: multiplier as $t,
                    post_shift,
                    sign: divisor >> (<$t>::BITS - 1),
                })
            }

            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                // `MULSH(m, n)`
                let high = ((self.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                let q = (high >> self.post_shift) - (n >> (<$t>::BITS - 1));
                let q = (q ^ self.sign) - self.sign;
                (q, n - q * self.divisor)
            }
        }
    };
}

fig52!(Fig52DivisorI32, choose_multiplier_u32, i32, i64);
fig52!(Fig52DivisorI64, choose_multiplier_u64, i64, i128);

// Experiment: the round-down multiplier from ridiculous_fish, "Labor of Division (Episode III)",
// https://ridiculousfish.com/blog/posts/labor-of-division-episode-iii.html, for the divisors whose
// rounded-up multiplier doesn't fit in N bits. With `f = floor(log2(d))`, `m = floor(2^(N+f) / d)`
// and the quotient is `MULUH(m, n + 1) >> f`. The two methods are the post's two ways to handle
// `n + 1` overflowing (section "Overflow Handling"): a saturating increment, and adding `m` to the
// double-width product. The post proves the saturating increment correct at `n = 2^N - 1`, because
// every divisor of `2^N - 1` has an N-bit rounded-up multiplier and never uses this type. The
// public domain reference code it links computes the same multiplier in
// `compute_unsigned_magic_info`:
// https://github.com/ridiculousfish/libdivide/blob/53b36fcacfd031ec0a0837db759f057eb03bacbe/doc/divide_by_constants_codegen_reference.c
macro_rules! round_down {
    ($name:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            shift: u32,
        }

        impl $name {
            /// Returns `None` if `divisor` is below 3, a power of two, or has a rounded-up
            /// multiplier that fits in N bits
            pub const fn try_new(divisor: $t) -> Option<Self> {
                match NBit::<$t>::parts(divisor) {
                    Some((multiplier, 1, shift)) => Some(Self {
                        divisor,
                        multiplier,
                        shift,
                    }),
                    _ => None,
                }
            }

            #[inline]
            pub const fn wrapping_div_rem_sat(self, n: $t) -> ($t, $t) {
                let n1 = n.saturating_add(1);
                let high = ((self.multiplier as $wide * n1 as $wide) >> <$t>::BITS) as $t;
                let q = high >> self.shift;
                (q, n - q * self.divisor)
            }

            #[inline]
            pub const fn wrapping_div_rem_dist(self, n: $t) -> ($t, $t) {
                let m = self.multiplier as $wide;
                let high = ((m * n as $wide + m) >> <$t>::BITS) as $t;
                let q = high >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

// Experiment, not from the sources: one type for every divisor that isn't a power of two, with an
// N-bit multiplier and an increment of 0 (rounded-up multiplier) or 1 (rounded-down multiplier).
// "Labor of Division (Episode III)", section "Proof of Universality", proves that one of the two
// always fits in N bits at `k = N + floor(log2(d))`. The rounded-up check is theorem 4.2 of
// Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994),
// https://gmplib.org/~tege/divcnst-pldi94.pdf, and the rounded-down check is the
// condition `2^(N+p) mod d <= 2^p` from the post's section "Proof of Correctness"
macro_rules! n_bit {
    ($name:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name {
            divisor: $t,
            multiplier: $t,
            /// 0 or 1
            increment: $t,
            /// `multiplier * increment`
            addend: $t,
            shift: u32,
        }

        impl NBit<$t> {
            /// Returns `(multiplier, increment, shift)`, or `None` if `divisor` is below 3 or a
            /// power of two
            const fn parts(divisor: $t) -> Option<($t, $t, u32)> {
                if divisor < 3 || divisor.is_power_of_two() {
                    return None;
                }
                let f = <$t>::BITS - 1 - divisor.leading_zeros();
                let pow = (1 as $wide) << (<$t>::BITS + f);
                let floor = pow / divisor as $wide;
                // 2^f < divisor, so floor < 2^N
                let below = pow - floor * divisor as $wide;
                let up = floor + 1;
                // Theorem 4.2 of Granlund and Montgomery: m * d - 2^(N+f) <= 2^f
                if up < (1 as $wide) << <$t>::BITS && up * divisor as $wide - pow <= 1 << f {
                    return Some((up as $t, 0, f));
                }
                // Episode III: 2^(N+f) mod d <= 2^f
                assert!(below <= 1 << f);
                Some((floor as $t, 1, f))
            }
        }

        impl $name {
            /// Returns `None` if `divisor` is below 3 or a power of two
            pub const fn try_new(divisor: $t) -> Option<Self> {
                match NBit::<$t>::parts(divisor) {
                    Some((multiplier, increment, shift)) => Some(Self {
                        divisor,
                        multiplier,
                        increment,
                        addend: multiplier * increment,
                        shift,
                    }),
                    None => None,
                }
            }

            #[inline]
            pub const fn wrapping_div_rem_sat(self, n: $t) -> ($t, $t) {
                let n1 = n.saturating_add(self.increment);
                let high = ((self.multiplier as $wide * n1 as $wide) >> <$t>::BITS) as $t;
                let q = high >> self.shift;
                (q, n - q * self.divisor)
            }

            #[inline]
            pub const fn wrapping_div_rem_dist(self, n: $t) -> ($t, $t) {
                let product = self.multiplier as $wide * n as $wide + self.addend as $wide;
                let q = ((product >> <$t>::BITS) as $t) >> self.shift;
                (q, n - q * self.divisor)
            }
        }
    };
}

/// Experiment: namespace for `parts`, so that it's generated once per type
struct NBit<T>(T);

n_bit!(NBitDivisorU32, u32, u64);
n_bit!(NBitDivisorU64, u64, u128);
round_down!(RoundDownDivisorU32, u32, u64);
round_down!(RoundDownDivisorU64, u64, u128);

// Experiment: the remainder without the quotient, from figures 1 and 2 of Lemire, Kaser and Kurz,
// "Faster Remainder by Direct Computation", https://arxiv.org/abs/1902.01961. With
// `c = ceil(2^64 / |d|)`, the low 64 bits of `c * n` are the fraction of `n / |d|`, and multiplying
// them by `|d|` gives the remainder in the high 64 bits (section 3, theorem 1). Figure 1 is the
// unsigned version, from the fastmod library: https://github.com/lemire/fastmod
#[derive(Debug, Clone, Copy)]
pub struct LkkRemDivisorU32 {
    divisor: u32,
    c: u64,
}

impl LkkRemDivisorU32 {
    /// Returns `None` if `divisor` is below 2
    pub const fn try_new(divisor: u32) -> Option<Self> {
        if divisor < 2 {
            return None;
        }
        Some(Self {
            divisor,
            c: u64::MAX / divisor as u64 + 1,
        })
    }

    #[inline]
    pub const fn wrapping_rem(self, n: u32) -> u32 {
        let low = self.c.wrapping_mul(n as u64);
        ((low as u128 * self.divisor as u128) >> 64) as u32
    }
}

/// Experiment: the signed remainder from figure 2 of Lemire, Kaser and Kurz, which adds 1 to `c`
/// for a power of two (section 3.1, theorem 2). Figure 2 doesn't handle -1, 0, 1 and `i32::MIN`
#[derive(Debug, Clone, Copy)]
pub struct LkkRemDivisorI32 {
    magnitude: u32,
    c: u64,
}

impl LkkRemDivisorI32 {
    /// Returns `None` for -1, 0, 1 and `i32::MIN`, which figure 2 doesn't handle
    pub const fn try_new(divisor: i32) -> Option<Self> {
        if divisor == i32::MIN || divisor.unsigned_abs() < 2 {
            return None;
        }
        let magnitude = divisor.unsigned_abs();
        Some(Self {
            magnitude,
            c: u64::MAX / magnitude as u64 + 1 + magnitude.is_power_of_two() as u64,
        })
    }

    #[inline]
    pub const fn wrapping_rem(self, n: i32) -> i32 {
        let low = self.c.wrapping_mul(n as i64 as u64);
        let high = ((low as u128 * self.magnitude as u128) >> 64) as i32;
        // `high - 1 + |d|` for a negative `n`
        high - ((self.magnitude as i32 - 1) & (n >> 31))
    }
}

// Experiment: figure 5.1 with devanbenz's remainder, `n - q0 * |d|` from the quotient before the
// sign fix, computing `|d|` in the method. It holds because the remainder doesn't depend on the sign
// of the divisor, as section 3.1 of Lemire, Kaser and Kurz, "Faster Remainder by Direct
// Computation", https://arxiv.org/abs/1902.01961, states: "n mod d = n mod -d for all integers n".
// devanbenz suggested it in
// https://github.com/apache/arrow-rs/pull/11467#discussion_r4379365875
macro_rules! fig51_magnitude {
    ($name:ident, $multiplier:ident, $enum:ident, $t:ty, $wide:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name($multiplier);

        impl $name {
            /// Returns `None` if `|divisor|` is below 3 or a power of two
            pub const fn try_new(divisor: $t) -> Option<Self> {
                match $enum::new(divisor) {
                    Some($enum::Multiplier(m)) => Some(Self(m)),
                    _ => None,
                }
            }

            #[inline]
            pub const fn wrapping_div_rem(self, n: $t) -> ($t, $t) {
                let d = self.0;
                let high = ((d.multiplier as $wide * n as $wide) >> <$t>::BITS) as $t;
                let q0 = ((n + high) >> d.shift) - (n >> (<$t>::BITS - 1));
                let sign = d.divisor >> (<$t>::BITS - 1);
                // `divisor` is never `MIN`, whose magnitude is a power of two
                let magnitude = (d.divisor ^ sign) - sign;
                ((q0 ^ sign) - sign, n - q0 * magnitude)
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

fig51_magnitude!(
    Fig51MagnitudeDivisorI32,
    MultiplierDivisorI32,
    DivisorI32,
    i32,
    i64
);
fig51_magnitude!(
    Fig51MagnitudeDivisorI64,
    MultiplierDivisorI64,
    DivisorI64,
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

    // Experiment: the fixup-free multiplier against `/` and `%`
    macro_rules! test_fig42_no_pre_shift {
        ($name:ident, $no_fixup:ident, $t:ty) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut divisors: Vec<$t> = (1..=2000).collect();
                divisors.extend([<$t>::MAX, <$t>::MAX - 1]);
                divisors.extend((0..2000).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                divisors.retain(|d| *d != 0);
                let mut found = 0;
                for d in divisors {
                    let Some(divisor) = $no_fixup::try_new(d) else {
                        continue;
                    };
                    found += 1;
                    // The last value of the last full quotient step is where the error peaks
                    let last = (<$t>::MAX - (d - 1)) / d * d + (d - 1);
                    let mut values =
                        vec![0, 1, d - 1, d, d.wrapping_add(1), last, last - 1, <$t>::MAX];
                    values
                        .extend((0..200).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                    for n in values {
                        assert_eq!(divisor.wrapping_div_rem(n), (n / d, n % d), "{n} / {d}");
                    }
                }
                assert!(found > 1000, "{found}");
                assert!($no_fixup::try_new(13).is_some());
                assert!($no_fixup::try_new(7).is_none());
            }
        };
    }

    test_fig42_no_pre_shift!(test_fig42_no_pre_shift_u32, Fig42NoPreShiftDivisorU32, u32);
    test_fig42_no_pre_shift!(test_fig42_no_pre_shift_u64, Fig42NoPreShiftDivisorU64, u64);

    // Experiment: figure 4.2 with the pre-shift against `/` and `%`
    macro_rules! test_fig42 {
        ($name:ident, $fig42:ident, $t:ty) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut divisors: Vec<$t> = (1..=2000).collect();
                let mut p: $t = 10;
                while let Some(next) = p.checked_mul(10) {
                    divisors.extend([p - 1, p, p + 1]);
                    p = next;
                }
                divisors.extend([<$t>::MAX >> 1, (<$t>::MAX >> 1) - 1]);
                divisors.extend((0..2000).map(|_| rng.random::<$t>() >> rng.random_range(1..bits)));
                divisors.retain(|d| *d != 0);
                let mut found = 0;
                for d in divisors {
                    let Some(divisor) = $fig42::try_new(d) else {
                        continue;
                    };
                    found += 1;
                    // The last value of the last full quotient step is where the error peaks
                    let last = (<$t>::MAX - (d - 1)) / d * d + (d - 1);
                    let mut values = vec![0, 1, d - 1, d, d + 1, last, last - 1, <$t>::MAX];
                    values
                        .extend((0..200).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                    for n in values {
                        assert_eq!(divisor.wrapping_div_rem(n), (n / d, n % d), "{n} / {d}");
                    }
                }
                assert!(found > 2000, "{found}");
                // 13 needs no pre-shift, 100 and 1000 need one for `u64`, 7 needs figure 4.1
                for d in [13, 100, 1000] {
                    assert!($fig42::try_new(d).is_some(), "{d}");
                }
                assert!($fig42::try_new(7).is_none());
            }
        };
    }

    test_fig42!(test_fig42_u32, Fig42DivisorU32, u32);
    test_fig42!(test_fig42_u64, Fig42DivisorU64, u64);

    // Experiment: the `m < 2^(N-1)` case of figure 5.2 against `wrapping_div` and `wrapping_rem`
    macro_rules! test_fig52 {
        ($name:ident, $fig52:ident, $t:ty, $u:ty, $needs_add:expr) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut magnitudes: Vec<$t> = (1..=2000).collect();
                magnitudes.extend(
                    (0..2000).map(|_| (rng.random::<$u>() >> rng.random_range(2..bits)) as $t),
                );
                let mut divisors: Vec<$t> = magnitudes.iter().flat_map(|d| [*d, -*d]).collect();
                divisors.retain(|d| *d != 0);
                let mut found = 0;
                for d in divisors {
                    let Some(divisor) = $fig52::try_new(d) else {
                        continue;
                    };
                    found += 1;
                    // The ends of the last full quotient step on each side of zero
                    let ad = d.unsigned_abs();
                    let last = ((<$t>::MAX as $u - (ad - 1)) / ad * ad + (ad - 1)) as $t;
                    let first = (<$t>::MIN.unsigned_abs() - (ad - 1)) / ad * ad + (ad - 1);
                    let first = (0 as $u).wrapping_sub(first) as $t;
                    let mut values = vec![0, 1, -1, last, last - 1, first, first + 1];
                    values.extend([<$t>::MIN, <$t>::MIN + 1, <$t>::MAX, d, d.wrapping_sub(1)]);
                    values.extend((0..200).flat_map(|_| {
                        let v = (rng.random::<$u>() >> rng.random_range(1..bits)) as $t;
                        [v, -v]
                    }));
                    for n in values {
                        let expected = (n.wrapping_div(d), n.wrapping_rem(d));
                        assert_eq!(divisor.wrapping_div_rem(n), expected, "{n} / {d}");
                    }
                }
                assert!(found > 1000, "{found}");
                for d in [3, -3, 10, -10] {
                    assert!($fig52::try_new(d).is_some(), "{d}");
                }
                assert!($fig52::try_new($needs_add).is_none());
            }
        };
    }

    test_fig52!(test_fig52_i32, Fig52DivisorI32, i32, u32, 7);
    test_fig52!(test_fig52_i64, Fig52DivisorI64, i64, u64, 15);

    // Experiment: the N-bit and round-down multipliers against `/` and `%`
    macro_rules! test_n_bit {
        ($name:ident, $n_bit:ident, $round_down:ident, $t:ty, $primes:expr) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut divisors: Vec<$t> = (1..=3000).collect();
                divisors.extend([<$t>::MAX, <$t>::MAX - 1, (<$t>::MAX >> 1) + 2]);
                let mut p: $t = 10;
                while let Some(next) = p.checked_mul(10) {
                    divisors.extend([p - 1, p, p + 1]);
                    p = next;
                }
                // Every divisor of 2^N - 1, where the saturating increment relies on the
                // rounded-up multiplier fitting in N bits
                let primes: &[$t] = &$primes;
                for subset in 1..1u32 << primes.len() {
                    let d = (0..primes.len())
                        .filter(|i| subset & (1 << i) != 0)
                        .fold(1, |d, i| d * primes[i]);
                    divisors.extend([d, d - 1, d.wrapping_add(1)]);
                }
                assert_eq!(primes.iter().product::<$t>(), <$t>::MAX);
                divisors.extend((0..2000).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                divisors.retain(|d| *d != 0);

                let mut round_down = 0;
                for d in divisors {
                    let Some(n_bit) = $n_bit::try_new(d) else {
                        assert!(d < 3 || d.is_power_of_two(), "{d}");
                        continue;
                    };
                    let rd = $round_down::try_new(d);
                    assert_eq!(rd.is_some(), n_bit.increment == 1, "{d}");
                    if (<$t>::MAX % d) == 0 {
                        assert_eq!(n_bit.increment, 0, "{d}");
                    }
                    // The last value of the last full quotient step is where the error peaks
                    let last = (<$t>::MAX - (d - 1)) / d * d + (d - 1);
                    let mut values = vec![0, 1, d - 1, d, d.wrapping_add(1), last, last - 1];
                    values.extend([
                        <$t>::MAX,
                        <$t>::MAX - 1,
                        <$t>::MAX / d * d,
                        <$t>::MAX / d * d - 1,
                    ]);
                    values
                        .extend((0..200).map(|_| rng.random::<$t>() >> rng.random_range(0..bits)));
                    for n in values {
                        let expected = (n / d, n % d);
                        assert_eq!(n_bit.wrapping_div_rem_sat(n), expected, "sat {n} / {d}");
                        assert_eq!(n_bit.wrapping_div_rem_dist(n), expected, "dist {n} / {d}");
                        if let Some(rd) = rd {
                            assert_eq!(rd.wrapping_div_rem_sat(n), expected, "rd sat {n} / {d}");
                            assert_eq!(rd.wrapping_div_rem_dist(n), expected, "rd dist {n} / {d}");
                        }
                    }
                    round_down += rd.is_some() as usize;
                }
                assert!(round_down > 500, "{round_down}");
                assert!($round_down::try_new(7).is_some());
                assert!($round_down::try_new(13).is_none());
            }
        };
    }

    test_n_bit!(
        test_n_bit_u32,
        NBitDivisorU32,
        RoundDownDivisorU32,
        u32,
        [3, 5, 17, 257, 65537]
    );
    test_n_bit!(
        test_n_bit_u64,
        NBitDivisorU64,
        RoundDownDivisorU64,
        u64,
        [3, 5, 17, 257, 641, 65537, 6700417]
    );

    // Experiment: figure 5.1 with devanbenz's remainder against `wrapping_div` and `wrapping_rem`
    macro_rules! test_fig51_magnitude {
        ($name:ident, $magnitude:ident, $t:ty, $u:ty) => {
            #[test]
            fn $name() {
                let mut rng = StdRng::seed_from_u64(42);
                let bits = <$t>::BITS;
                let mut magnitudes: Vec<$t> = (1..=2000).collect();
                magnitudes.extend([<$t>::MAX, <$t>::MAX - 1]);
                magnitudes.extend(
                    (0..2000).map(|_| (rng.random::<$u>() >> rng.random_range(1..bits)) as $t),
                );
                let mut divisors: Vec<$t> = magnitudes.iter().flat_map(|d| [*d, -*d]).collect();
                divisors.extend([<$t>::MIN, <$t>::MIN + 1]);
                divisors.retain(|d| *d != 0);
                for d in divisors {
                    let Some(divisor) = $magnitude::try_new(d) else {
                        assert!(
                            d.unsigned_abs() < 3 || d.unsigned_abs().is_power_of_two(),
                            "{d}"
                        );
                        continue;
                    };
                    // The ends of the last full quotient step on each side of zero
                    let ad = d.unsigned_abs();
                    let last = ((<$t>::MAX as $u - (ad - 1)) / ad * ad + (ad - 1)) as $t;
                    let first = (<$t>::MIN.unsigned_abs() - (ad - 1)) / ad * ad + (ad - 1);
                    let first = (0 as $u).wrapping_sub(first) as $t;
                    let mut values = vec![0, 1, -1, last, last - 1, first, first + 1];
                    values.extend([<$t>::MIN, <$t>::MIN + 1, <$t>::MAX, d, d.wrapping_sub(1)]);
                    values.extend((0..200).flat_map(|_| {
                        let v = (rng.random::<$u>() >> rng.random_range(1..bits)) as $t;
                        [v, -v]
                    }));
                    for n in values {
                        let expected = (n.wrapping_div(d), n.wrapping_rem(d));
                        assert_eq!(divisor.wrapping_div_rem(n), expected, "{n} / {d}");
                    }
                }
            }
        };
    }

    // Experiment: the direct remainder against `%`, at every power of two and its neighbors
    #[test]
    fn test_lkk_rem() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut magnitudes: Vec<u32> = (2..=3000).collect();
        magnitudes.extend([u32::MAX, u32::MAX - 1, i32::MAX as u32, i32::MAX as u32 - 1]);
        for k in 1..32 {
            magnitudes.extend([1 << k, (1 << k) - 1, (1 << k) + 1]);
        }
        magnitudes.extend((0..2000).map(|_| rng.random::<u32>() >> rng.random_range(0..32)));
        magnitudes.retain(|d| *d >= 2);
        for d in magnitudes {
            let values = |rng: &mut StdRng| {
                let mut v = vec![0, 1, d - 1, d, d.wrapping_add(1), u32::MAX, u32::MAX - 1];
                v.extend([
                    u32::MAX / d * d,
                    u32::MAX / d * d - 1,
                    1 << 31,
                    (1 << 31) - 1,
                ]);
                v.extend((0..200).map(|_| rng.random::<u32>() >> rng.random_range(0..32)));
                v
            };
            if let Some(u) = LkkRemDivisorU32::try_new(d) {
                for n in values(&mut rng) {
                    assert_eq!(u.wrapping_rem(n), n % d, "{n} % {d}");
                }
            }
            if d <= i32::MAX as u32 {
                for d in [d as i32, -(d as i32)] {
                    let s = LkkRemDivisorI32::try_new(d).unwrap();
                    let mut values: Vec<i32> = values(&mut rng).iter().map(|v| *v as i32).collect();
                    values.extend([i32::MIN, i32::MIN + 1, i32::MAX, -1, d, -d]);
                    for n in values.iter().flat_map(|v| [*v, v.wrapping_neg()]) {
                        assert_eq!(s.wrapping_rem(n), n.wrapping_rem(d), "{n} % {d}");
                    }
                }
            }
        }
        assert!(LkkRemDivisorI32::try_new(i32::MIN).is_none());
        assert!(LkkRemDivisorI32::try_new(-1).is_none());
    }

    test_fig51_magnitude!(test_fig51_magnitude_i32, Fig51MagnitudeDivisorI32, i32, u32);
    test_fig51_magnitude!(test_fig51_magnitude_i64, Fig51MagnitudeDivisorI64, i64, u64);

    #[test]
    fn test_const_new() {
        const TEN: DivisorU64 = DivisorU64::new(10).unwrap();
        const MINUS_EIGHT: DivisorI32 = DivisorI32::new(-8).unwrap();
        assert_eq!(TEN.wrapping_div_rem(1234), (123, 4));
        assert_eq!(MINUS_EIGHT.wrapping_div_rem(-17), (2, -1));
    }
}

#[cfg(test)]
mod experiment_shape_tests {
    use super::*;

    #[test]
    fn test_fig42_shapes() {
        let shape = |d: u64| Fig42DivisorU64::try_new(d).map(|x| (x.pre_shift, x.post_shift));
        let shape32 = |d: u32| Fig42DivisorU32::try_new(d).map(|x| (x.pre_shift, x.post_shift));
        println!(
            "u64: 10 {:?} 13 {:?} 100 {:?} 1000 {:?}",
            shape(10),
            shape(13),
            shape(100),
            shape(1000)
        );
        println!(
            "u32: 10 {:?} 13 {:?} 100 {:?} 1000 {:?}",
            shape32(10),
            shape32(13),
            shape32(100),
            shape32(1000)
        );
        assert!(shape(100).unwrap().0 > 0 && shape(1000).unwrap().0 > 0);
    }
}
