// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::bigint::div::div_rem;
use crate::bigint::mulx;
use crate::i256;

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
    (
        $name:ident,
        $t:ty,
        [$(#[$enum_doc:meta])*],
        $($(#[$doc:meta])* $variant:ident($inner:ident),)+
    ) => {
        divisor_enum!(
            $name,
            $t,
            concat!(
                "# use arrow_buffer::", stringify!($name), ";\n",
                "let values: Vec<", stringify!($t), "> = vec![100, 7, 64];\n",
                "let quotients: Vec<_> = match ", stringify!($name), "::new(7).unwrap() {\n",
                "    ", stringify!($name), "::PowerOfTwo(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
                "    ", stringify!($name), "::AddIndicator(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
                "    d => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
                "};\n",
                "assert_eq!(quotients, [14, 1, 9]);",
            ),
            concat!(
                "# use arrow_buffer::", stringify!($name), ";\n",
                "let divisor = ", stringify!($name), "::new(7).unwrap();\n",
                "assert_eq!(divisor.wrapping_div_rem(100), (14, 2));",
            ),
            "They don't branch on the divisor, so the loop doesn't depend on the compiler moving a branch out of it.",
            [$(#[$enum_doc])*],
            $($(#[$doc])* $variant($inner),)+
        );
    };
    (
        $name:ident,
        $t:ty,
        $example:expr,
        $div_rem_example:expr,
        $branch_doc:expr,
        [$(#[$enum_doc:meta])*],
        $($(#[$doc:meta])* $variant:ident($inner:ident),)+
    ) => {
        #[doc = concat!("A `", stringify!($t), "` divisor, precomputed once to divide many values by it")]
        ///
        /// Division by a divisor that is only known at run time needs a divide instruction.
        /// [`Self::new`] precomputes a shift and a mask when the magnitude of the divisor is a
        /// power of two, and otherwise a multiplier, so that each division is a high multiply,
        /// shifts and additions. [`Self::new`] picks the variant from the divisor and the target.
        ///
        /// To divide many values, match on the variant once, before the loop, and call the
        /// variant's methods in the loop.
        #[doc = $branch_doc]
        /// Move the variant into the loop's closure, so that the compiler doesn't reload its
        /// fields through a reference for each value. [`Self::wrapping_div_rem`] matches on each
        /// call, so a match can fall back to it for the variants it doesn't specialize, including
        /// variants added later.
        $(#[$enum_doc])*
        ///
        /// # Example
        /// ```
        #[doc = $example]
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
            #[doc = $div_rem_example]
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
            [],
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

/// `$multiplier` computes `m - 2^N` from figure 5.1 for `|divisor|` and `l`, and `$high` computes
/// `n + MULSH(m - 2^N, n)`, which is `floor(m * n / 2^N)`. `[const]` makes `new` a `const fn`
macro_rules! signed_divisor {
    (
        $name:ident,
        $power_of_two:ident,
        $add:ident,
        $t:ty,
        [$($c:tt)?],
        |$magnitude:ident, $l:ident| $multiplier:expr,
        |$m:ident, $n:ident| $high:expr,
        [$(#[$enum_doc:meta])*]
    ) => {
        divisor_enum!(
            $name,
            $t,
            [$(#[$enum_doc])*],
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
            $(#[doc = concat!(stringify!($c), " SEVEN: ", stringify!($name), " = ", stringify!($name), "::new(7).unwrap();")])?
            #[doc = concat!("assert!(", stringify!($name), "::new(0).is_none());")]
            /// ```
            pub $($c)? fn new(divisor: $t) -> Option<Self> {
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
            $($c)? fn new(divisor: $t) -> Self {
                let $magnitude = divisor.unsigned_abs();
                // ceil(log2(|divisor|)), which is at least 2 because |divisor| >= 3, so figure
                // 5.1's `max(..., 1)` isn't needed
                let $l = <$t>::BITS - ($magnitude - 1).leading_zeros();
                Self {
                    divisor,
                    multiplier: $multiplier,
                    shift: $l - 1,
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
                let ($m, $n) = (self.multiplier, n);
                // The shift rounds down, and subtracting the sign of `n` rounds toward zero instead
                let q = ($high >> self.shift) - (n >> (<$t>::BITS - 1));
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
/// Expands to the arguments of `signed_divisor!` that compute with `$wide`, a type twice as wide
/// as `$t`
macro_rules! signed_divisor_wide {
    ($name:ident, $power_of_two:ident, $add:ident, $t:ty, $wide:ty) => {
        signed_divisor!(
            $name,
            $power_of_two,
            $add,
            $t,
            [const],
            // m = 1 + floor(2^(N + l - 1) / |divisor|) is between 2^(N - 1) and 2^N, so m - 2^N
            // fits in the narrow type
            |magnitude, l| {
                let m = 1 + (1 << (<$t>::BITS + l - 1)) / magnitude as $wide;
                (m - (1 << <$t>::BITS)) as $t
            },
            // `n + MULSH(m', n)` in figure 5.1
            |m, n| n + ((m as $wide * n as $wide) >> <$t>::BITS) as $t,
            []
        );
    };
}

signed_divisor_wide!(
    DivisorI32,
    PowerOfTwoDivisorI32,
    AddIndicatorDivisorI32,
    i32,
    i64
);
signed_divisor_wide!(
    DivisorI64,
    PowerOfTwoDivisorI64,
    AddIndicatorDivisorI64,
    i64,
    i128
);
signed_divisor!(
    DivisorI128,
    PowerOfTwoDivisorI128,
    AddIndicatorDivisorI128,
    i128,
    [],
    // m = 1 + floor(2^(127 + l) / |divisor|), as for the narrower types. 2^(127 + l) is at most
    // 2^254, so `i256` holds it, and the quotient is below 2^128
    |magnitude, l| {
        let pow = i256::ONE << (127 + l) as u8;
        let (q, _) = pow.wrapping_div(i256::from_parts(magnitude, 0)).to_parts();
        q.wrapping_add(1) as i128
    },
    // Figure 5.1 computes `n + MULSH(m', n)`, and `i128` has no wider type to compute `MULSH`
    // in. Section 3 of the paper gives `MULSH(x, y) = MULUH(x, y) - AND(x, XSIGN(y))
    // - AND(y, XSIGN(x))`, which is program (1) in section 8-3 of Warren, "Hacker's Delight", 2nd
    // edition. `m'` is negative for every divisor but 1 and -1, which are powers of two, so
    // `AND(n, XSIGN(m'))` is `n` and cancels the add, leaving `MULUH(m', n) - AND(m', XSIGN(n))`
    |m, n| (mulx(m as u128, n as u128).1 as i128).wrapping_sub((n >> 127) & m),
    [
        ///
        /// When every value of a batch fits in `i64`, dividing in `i64` with [`DivisorI64`] gives
        /// the same results, except for a divisor of -1, because `i64::MIN / -1` overflows in
        /// `i64` but not in `i128`. A divisor that doesn't fit in `i64` gives a quotient of 0 and
        /// leaves the value as the remainder, except that `i64::MIN / 2^63` is -1 with a
        /// remainder of 0.
        ///
        /// ```
        /// # use arrow_buffer::{DivisorI128, DivisorI64};
        /// let values: Vec<i128> = vec![-12_345, 678, i64::MIN as i128];
        /// let divisor: i128 = 100;
        /// let narrow = values.iter().all(|n| i64::try_from(*n).is_ok());
        /// let quotients: Vec<i128> = match i64::try_from(divisor) {
        ///     Ok(d) if narrow && d != -1 => {
        ///         let d = DivisorI64::new(d).unwrap();
        ///         values.iter().map(|n| d.wrapping_div_rem(*n as i64).0 as i128).collect()
        ///     }
        ///     _ => {
        ///         let d = DivisorI128::new(divisor).unwrap();
        ///         values.iter().map(|n| d.wrapping_div_rem(*n).0).collect()
        ///     }
        /// };
        /// assert_eq!(quotients, [-123, 6, -92233720368547758]);
        /// ```
    ]
);

// `signed_divisor!` can't build `DivisorI256`, because `i256` has no `BITS` or `unsigned_abs` and
// its operators can't be called in a `const fn`. Its variants compute on the parts of the values
// with the helpers below `impl DivisorI256` instead
divisor_enum!(
    DivisorI256,
    i256,
    concat!(
        "# use arrow_buffer::{i256, DivisorI256};\n",
        "let values: Vec<i256> = [100, 7, 64].map(i256::from_i128).to_vec();\n",
        "let quotients: Vec<_> = match DivisorI256::new(i256::from_i128(7)).unwrap() {\n",
        "    DivisorI256::PowerOfTwo(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
        "    DivisorI256::AddIndicator(d) => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
        "    d => values.iter().map(move |n| d.wrapping_div_rem(*n).0).collect(),\n",
        "};\n",
        "assert_eq!(quotients, [14, 1, 9].map(i256::from_i128));",
    ),
    concat!(
        "# use arrow_buffer::{i256, DivisorI256};\n",
        "let divisor = DivisorI256::new(i256::from_i128(7)).unwrap();\n",
        "let (q, r) = divisor.wrapping_div_rem(i256::from_i128(100));\n",
        "assert_eq!((q, r), (i256::from_i128(14), i256::from_i128(2)));",
    ),
    "Unlike those of the other divisor types, they branch on the divisor's shift. The shift is the same for every value, so the branch is predicted, and a form without it was slower.",
    [
        ///
        /// When every value of a batch fits in `i128`, dividing in `i128` with [`DivisorI128`]
        /// gives the same results, except for a divisor of -1, because `i128::MIN / -1` overflows
        /// in `i128` but not in `i256`. A divisor that doesn't fit in `i128` gives a quotient of 0
        /// and leaves the value as the remainder, except that `i128::MIN / 2^127` is -1 with a
        /// remainder of 0.
    ],
    /// A divisor whose magnitude is a power of two
    PowerOfTwo(PowerOfTwoDivisorI256),
    /// Any other divisor
    AddIndicator(AddIndicatorDivisorI256),
);

impl DivisorI256 {
    /// Returns a divisor for `divisor`, or `None` if `divisor` is zero
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::{i256, DivisorI256};
    /// assert!(DivisorI256::new(i256::ZERO).is_none());
    /// ```
    pub fn new(divisor: i256) -> Option<Self> {
        if divisor == i256::ZERO {
            return None;
        }
        Some(match PowerOfTwoDivisorI256::new(divisor) {
            Some(d) => Self::PowerOfTwo(d),
            None => Self::AddIndicator(AddIndicatorDivisorI256::new(divisor)),
        })
    }

    /// Returns `(n / divisor, n % divisor)`, or `None` for `i256::MIN / -1`
    ///
    /// It matches on the variant on each call. Only [`Self::PowerOfTwo`] can return `None`.
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::{i256, DivisorI256};
    /// let divisor = DivisorI256::new(i256::from_i128(-7)).unwrap();
    /// let qr = divisor.checked_div_rem(i256::from_i128(-100));
    /// assert_eq!(qr, Some((i256::from_i128(14), i256::from_i128(-2))));
    ///
    /// let minus_one = DivisorI256::new(i256::MINUS_ONE).unwrap();
    /// assert_eq!(minus_one.checked_div_rem(i256::MIN), None);
    /// ```
    #[inline]
    pub const fn checked_div_rem(self, n: i256) -> Option<(i256, i256)> {
        match self {
            Self::PowerOfTwo(d) => d.checked_div_rem(n),
            Self::AddIndicator(d) => Some(d.wrapping_div_rem(n)),
        }
    }
}

/// `i256` operations that a `const fn` can call, on the parts of the values. `shr` is `SRA` from
/// table 3.1 of Granlund and Montgomery for `0..=255` bits
///
/// It branches on the shift, so the `i256` variants branch on the divisor, unlike the other
/// divisor types. The shift is the same for every value, so the branch is predicted. A
/// branch-free form that shifts both halves and selects was slower
const fn shr(x: i256, shift: u32) -> i256 {
    let (low, high) = x.to_parts();
    if shift == 0 {
        x
    } else if shift < 128 {
        i256::from_parts(
            (low >> shift) | ((high as u128) << (128 - shift)),
            high >> shift,
        )
    } else {
        i256::from_parts((high >> (shift - 128)) as u128, high >> 127)
    }
}

const fn and(a: i256, b: i256) -> i256 {
    let ((al, ah), (bl, bh)) = (a.to_parts(), b.to_parts());
    i256::from_parts(al & bl, ah & bh)
}

const fn xor(a: i256, b: i256) -> i256 {
    let ((al, ah), (bl, bh)) = (a.to_parts(), b.to_parts());
    i256::from_parts(al ^ bl, ah ^ bh)
}

const fn not(a: i256) -> i256 {
    let (low, high) = a.to_parts();
    i256::from_parts(!low, !high)
}

const fn eq(a: i256, b: i256) -> bool {
    let ((al, ah), (bl, bh)) = (a.to_parts(), b.to_parts());
    al == bl && ah == bh
}

/// The sign of `x` spread to every bit, -1 if `x` is negative, otherwise 0
const fn sign(x: i256) -> i256 {
    let s = x.to_parts().1 >> 127;
    i256::from_parts(s as u128, s)
}

/// The high 256 bits of the unsigned product of the bits of `a` and `b`
#[inline]
const fn mul_high_u256(a: i256, b: i256) -> i256 {
    let ((a0, a1), (b0, b1)) = (a.to_parts(), b.to_parts());
    let (a1, b1) = (a1 as u128, b1 as u128);
    let (_, p00) = mulx(a0, b0);
    let (p01_low, p01_high) = mulx(a0, b1);
    let (p10_low, p10_high) = mulx(a1, b0);
    let (p11_low, p11_high) = mulx(a1, b1);
    // Bits 128 to 255 of the product, of which only the carries matter
    let (s, c1) = p00.overflowing_add(p01_low);
    let (_, c2) = s.overflowing_add(p10_low);
    let (low, c3) = p01_high.overflowing_add(p10_high);
    let (low, c4) = low.overflowing_add(p11_low);
    let (low, c5) = low.overflowing_add(c1 as u128 + c2 as u128);
    // The product is below 2^512, so this doesn't overflow
    let high = p11_high + c3 as u128 + c4 as u128 + c5 as u128;
    i256::from_parts(low, high as i128)
}

/// An `i256` divisor whose magnitude is a power of two, see [`DivisorI256`]
///
/// The quotient and remainder follow sections 10-1 and 10-2 of Warren, "Hacker's Delight", 2nd
/// edition. Those sections compute the addend `2^k - 1` for a negative `n` with two shifts. This
/// type computes it with one shift that spreads the sign bit of `n` and an AND with `2^k - 1`,
/// which it computes once for the divisor.
#[derive(Debug, Clone, Copy)]
pub struct PowerOfTwoDivisorI256 {
    shift: u32,
    /// `|divisor| - 1`
    mask: i256,
    /// -1 if the divisor is negative, otherwise 0
    sign: i256,
}

impl PowerOfTwoDivisorI256 {
    fn new(divisor: i256) -> Option<Self> {
        // `wrapping_abs` leaves `MIN` as it is, whose bits are those of its magnitude, 2^255
        let magnitude = divisor.wrapping_abs();
        let mask = magnitude.wrapping_sub(i256::ONE);
        if !eq(and(magnitude, mask), i256::ZERO) {
            return None;
        }
        Some(Self {
            shift: magnitude.trailing_zeros(),
            mask,
            sign: sign(divisor),
        })
    }

    /// Returns `(n.wrapping_div(divisor), n.wrapping_rem(divisor))`, so `i256::MIN / -1` returns
    /// `(i256::MIN, 0)`
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::{i256, DivisorI256};
    /// let DivisorI256::PowerOfTwo(divisor) = DivisorI256::new(i256::from_i128(-8)).unwrap() else { unreachable!() };
    /// let (q, r) = divisor.wrapping_div_rem(i256::from_i128(-100));
    /// assert_eq!((q, r), (i256::from_i128(12), i256::from_i128(-4)));
    ///
    /// let DivisorI256::PowerOfTwo(minus_one) = DivisorI256::new(i256::MINUS_ONE).unwrap() else { unreachable!() };
    /// assert_eq!(minus_one.wrapping_div_rem(i256::MIN), (i256::MIN, i256::ZERO));
    /// ```
    #[inline]
    pub const fn wrapping_div_rem(self, n: i256) -> (i256, i256) {
        // Adding `|divisor| - 1` to a negative `n` makes the shift round toward zero
        let t = n.wrapping_add(and(sign(n), self.mask));
        let q = shr(t, self.shift);
        // `(x ^ sign) - sign` negates `x` when `sign` is -1, and wraps only for `MIN / -1`
        // `t & !mask` is `n` rounded toward zero to a multiple of `|divisor|`
        (
            xor(q, self.sign).wrapping_sub(self.sign),
            n.wrapping_sub(and(t, not(self.mask))),
        )
    }

    /// Returns `(n / divisor, n % divisor)`, or `None` for `i256::MIN / -1`
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::{i256, DivisorI256};
    /// let DivisorI256::PowerOfTwo(divisor) = DivisorI256::new(i256::from_i128(-8)).unwrap() else { unreachable!() };
    /// let qr = divisor.checked_div_rem(i256::from_i128(-100));
    /// assert_eq!(qr, Some((i256::from_i128(12), i256::from_i128(-4))));
    ///
    /// let DivisorI256::PowerOfTwo(minus_one) = DivisorI256::new(i256::MINUS_ONE).unwrap() else { unreachable!() };
    /// assert_eq!(minus_one.checked_div_rem(i256::MIN), None);
    /// ```
    #[inline]
    pub const fn checked_div_rem(self, n: i256) -> Option<(i256, i256)> {
        // -1 is the only divisor with a negative sign and a zero mask
        if eq(n, i256::MIN) && eq(self.sign, i256::MINUS_ONE) && eq(self.mask, i256::ZERO) {
            None
        } else {
            Some(self.wrapping_div_rem(n))
        }
    }
}

/// An `i256` divisor whose magnitude isn't a power of two, see [`DivisorI256`]
///
/// It uses the signed multiplier from figure 5.1 of
/// [Granlund and Montgomery, "Division by Invariant Integers using Multiplication" (1994)](https://gmplib.org/~tege/divcnst-pldi94.pdf),
/// as [`AddIndicatorDivisorI64`] does.
#[derive(Debug, Clone, Copy)]
pub struct AddIndicatorDivisorI256 {
    divisor: i256,
    /// `m'` in figure 5.1
    multiplier: i256,
    /// `sh_post` in figure 5.1
    shift: u32,
}

impl AddIndicatorDivisorI256 {
    /// `|divisor|` must be at least 3 and not a power of two
    fn new(divisor: i256) -> Self {
        let magnitude = divisor.wrapping_abs();
        // ceil(log2(|divisor|)), between 2 and 255
        let l = 256 - magnitude.wrapping_sub(i256::ONE).leading_zeros();
        // m = 1 + floor(2^(255 + l) / |divisor|) is between 2^255 and 2^256, so its bits fit in
        // an `i256`, and they are the bits of m - 2^256
        let bit = 255 + l as usize;
        let mut pow = [0u64; 8];
        pow[bit / 64] = 1 << (bit % 64);
        let (low, high) = magnitude.to_parts();
        let high = high as u128;
        let d = [
            low as u64,
            (low >> 64) as u64,
            high as u64,
            (high >> 64) as u64,
        ];
        let (q, _) = div_rem(&pow, &[d[0], d[1], d[2], d[3], 0, 0, 0, 0]);
        let q = i256::from_parts(
            q[0] as u128 | ((q[1] as u128) << 64),
            (q[2] as u128 | ((q[3] as u128) << 64)) as i128,
        );
        Self {
            divisor,
            multiplier: q.wrapping_add(i256::ONE),
            shift: l - 1,
        }
    }

    /// Returns `(n / divisor, n % divisor)`, which can't overflow
    ///
    /// # Example
    /// ```
    /// # use arrow_buffer::{i256, DivisorI256};
    /// let DivisorI256::AddIndicator(divisor) = DivisorI256::new(i256::from_i128(-7)).unwrap() else { unreachable!() };
    /// let (q, r) = divisor.wrapping_div_rem(i256::from_i128(-100));
    /// assert_eq!((q, r), (i256::from_i128(14), i256::from_i128(-2)));
    /// ```
    // At `#[inline]`, LLVM calls this out of line for each value, without LTO and with thin or
    // fat LTO, at 16 codegen units and at 1
    #[inline(always)]
    pub const fn wrapping_div_rem(self, n: i256) -> (i256, i256) {
        // `n + MULSH(m', n)` from figure 5.1, computed as `MULUH(m', n) - AND(m', XSIGN(n))`,
        // because `m'` is negative. See the identity in section 3 and `DivisorI128`
        let high = mul_high_u256(self.multiplier, n).wrapping_sub(and(sign(n), self.multiplier));
        // The shift rounds down, and subtracting the sign of `n` rounds toward zero instead
        let q = shr(high, self.shift).wrapping_sub(sign(n));
        let divisor_sign = sign(self.divisor);
        // Figure 5.1 gives only the quotient. The remainder doesn't depend on the sign of the
        // divisor, as section 3.1 of Lemire, Kaser and Kurz, "Faster Remainder by Direct
        // Computation", https://arxiv.org/abs/1902.01961, states, so it uses `q` before the sign
        // fix and `|divisor|`, which saves negating `q` before the multiply. `divisor` is never
        // `MIN`, whose magnitude is a power of two
        let magnitude = xor(self.divisor, divisor_sign).wrapping_sub(divisor_sign);
        (
            xor(q, divisor_sign).wrapping_sub(divisor_sign),
            n.wrapping_sub(q.wrapping_mul(magnitude)),
        )
    }
}

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
                // Each side of every power of two, which includes the 64-bit limbs of the
                // multiply and the edges of the narrower types a kernel may divide in instead,
                // and the largest values of each decimal precision
                for k in 0..bits - 1 {
                    let p: $t = 1 << k;
                    values.extend([p - 1, p, p + 1, 1 - p, -p, -p - 1]);
                }
                let mut p: $t = 10;
                while let Some(next) = p.checked_mul(10) {
                    values.extend([p - 1, p, p + 1, 1 - p, -p, -p - 1]);
                    p = next;
                }
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
    test_signed!(test_divisor_i128, DivisorI128, i128, u128);

    /// `test_signed!` for `i256`, which has no unsigned type for magnitudes and no literals
    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_divisor_i256() {
        let mut rng = StdRng::seed_from_u64(42);
        let one = i256::ONE;
        // A non-negative value with a random number of significant bits
        let mut random = || {
            let v = i256::from_parts(rng.random(), rng.random::<i128>() & i128::MAX);
            v >> rng.random_range(1..=254u8)
        };

        let mut magnitudes: Vec<i256> = (1..=1000).map(i256::from_i128).collect();
        for k in 0..255u8 {
            let p = one << k;
            magnitudes.extend([p, p.wrapping_sub(one), p.wrapping_add(one)]);
        }
        let ten = i256::from_i128(10);
        let mut p = ten;
        while let Some(next) = p.checked_mul(ten) {
            magnitudes.extend([p - one, p, p + one]);
            p = next;
        }
        magnitudes.extend((0..200).map(|_| random()));

        let mut divisors: Vec<i256> = magnitudes.iter().flat_map(|d| [*d, -*d]).collect();
        divisors.extend([i256::MIN, i256::MAX, i256::MINUS_ONE, one]);
        divisors.retain(|d| *d != i256::ZERO);

        let mut values = vec![
            i256::ZERO,
            one,
            i256::MINUS_ONE,
            i256::MIN,
            i256::MAX,
            i256::MIN + one,
        ];
        let mut edges = vec![];
        for k in 0..255u8 {
            edges.push(one << k);
        }
        let mut p = ten;
        while let Some(next) = p.checked_mul(ten) {
            edges.push(p);
            p = next;
        }
        for p in edges {
            values.extend([p - one, p, p + one, one - p, -p, -p - one]);
        }
        values.extend((0..200).flat_map(|_| {
            let v = random();
            [v, -v]
        }));

        assert!(DivisorI256::new(i256::ZERO).is_none());
        for d in divisors {
            let divisor = DivisorI256::new(d).unwrap();
            // `wrapping_abs` leaves `MIN` as it is, and `MIN & MAX` is zero
            let ad = d.wrapping_abs();
            let power_of_two = ad & ad.wrapping_sub(one) == i256::ZERO;
            assert_eq!(
                matches!(divisor, DivisorI256::PowerOfTwo(_)),
                power_of_two,
                "{d}"
            );
            // The ends of the last quotient steps on each side of zero, as in `test_signed!`
            let top = i256::MAX.wrapping_div(d).wrapping_mul(d);
            let bottom = i256::MIN.wrapping_div(d).wrapping_mul(d);
            let ends = [
                top,
                top.wrapping_sub(one),
                top.wrapping_sub(ad),
                bottom,
                bottom.wrapping_add(one),
                bottom.wrapping_add(ad),
            ];
            let near = [
                d.wrapping_sub(one),
                d,
                d.wrapping_add(one),
                d.wrapping_mul(i256::from_i128(7)),
            ];
            for &n in values.iter().chain(&near).chain(&ends) {
                let expected = n.wrapping_div_rem(d);
                assert_eq!(divisor.wrapping_div_rem(n), expected, "{n} / {d}");
                assert_eq!(
                    divisor.checked_div_rem(n),
                    n.checked_div_rem(d),
                    "{n} / {d}"
                );
            }
        }
    }

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
            let d = d as i128;
            assert!(
                matches!(DivisorI128::new(d).unwrap(), DivisorI128::AddIndicator(_)),
                "{d}"
            );
            let d = i256::from_i128(d);
            assert!(
                matches!(DivisorI256::new(d).unwrap(), DivisorI256::AddIndicator(_)),
                "{d}"
            );
        }
        for d in [i128::MAX, i128::MIN + 1, 10i128.pow(38), -(10i128.pow(18))] {
            assert!(
                matches!(DivisorI128::new(d).unwrap(), DivisorI128::AddIndicator(_)),
                "{d}"
            );
        }
        for d in [
            i256::MAX,
            i256::MIN + i256::ONE,
            i256::from_i128(10).wrapping_pow(76),
        ] {
            assert!(
                matches!(DivisorI256::new(d).unwrap(), DivisorI256::AddIndicator(_)),
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
