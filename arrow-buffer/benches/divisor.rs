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

use arrow_buffer::{
    DivisorI32, DivisorI64, DivisorU32, DivisorU64, Fig42DivisorU32, Fig42DivisorU64,
    Fig42NoPreShiftDivisorU32, Fig42NoPreShiftDivisorU64, Fig51MagnitudeDivisorI32,
    Fig51MagnitudeDivisorI64, Fig52DivisorI32, Fig52DivisorI64, LkkRemDivisorI32, LkkRemDivisorU32,
    MultiplierDivisorI32, MultiplierDivisorI64, MultiplierDivisorU32, MultiplierDivisorU64,
    NBitDivisorU32, NBitDivisorU64, RoundDownDivisorU32, RoundDownDivisorU64,
};
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const SIZE: usize = 8192;

/// Matches on the variant of `$divisor` once, outside the loop, and moves it into the
/// closure, as a kernel would
macro_rules! div_rem_all {
    ($values:expr, $divisor:expr, $enum:ident, $field:tt) => {
        match $divisor {
            $enum::PowerOfTwo(d) => $values
                .iter()
                .map(move |x| d.wrapping_div_rem(*x).$field)
                .collect::<Vec<_>>(),
            $enum::Multiplier(d) => $values
                .iter()
                .map(move |x| d.wrapping_div_rem(*x).$field)
                .collect::<Vec<_>>(),
        }
    };
}

/// Divides `SIZE` random values by 13 and by 16 with `/` and `%`, and with the divisor type
macro_rules! bench_divisor {
    ($c:expr, $divisor:ident, $t:ty) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("divisor_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in [13, 16] {
            // Hide the divisor from the compiler, as it is when it comes from a `Scalar`
            let d: $t = hint::black_box(d);
            let divisor = $divisor::new(d).unwrap();
            group.bench_function(format!("div {d}/operator"), |b| {
                b.iter(|| values.iter().map(move |x| x / d).collect::<Vec<_>>())
            });
            group.bench_function(format!("div {d}/divisor"), |b| {
                b.iter(|| div_rem_all!(values, divisor, $divisor, 0))
            });
            group.bench_function(format!("rem {d}/operator"), |b| {
                b.iter(|| values.iter().map(move |x| x % d).collect::<Vec<_>>())
            });
            group.bench_function(format!("rem {d}/divisor"), |b| {
                b.iter(|| div_rem_all!(values, divisor, $divisor, 1))
            });
        }
        group.finish();
    }};
}

/// Experiment: one division method, so that each method and type has its own symbol
trait Method<T>: Copy {
    fn div_rem(self, n: T) -> (T, T);
}

macro_rules! impl_method {
    ($($method:ty => $t:ty),*) => {
        $(impl Method<$t> for $method {
            #[inline]
            fn div_rem(self, n: $t) -> ($t, $t) {
                self.wrapping_div_rem(n)
            }
        })*
    };
}

impl_method!(
    MultiplierDivisorU32 => u32,
    MultiplierDivisorU64 => u64,
    MultiplierDivisorI32 => i32,
    MultiplierDivisorI64 => i64,
    Fig42NoPreShiftDivisorU32 => u32,
    Fig42NoPreShiftDivisorU64 => u64,
    Fig42DivisorU32 => u32,
    Fig42DivisorU64 => u64,
    Fig52DivisorI32 => i32,
    Fig52DivisorI64 => i64,
    Fig51MagnitudeDivisorI32 => i32,
    Fig51MagnitudeDivisorI64 => i64
);

/// Experiment: the saturating increment method of a round-down or N-bit divisor
#[derive(Clone, Copy)]
struct Sat<D>(D);

/// Experiment: the double-width `m * n + m` method of a round-down or N-bit divisor
#[derive(Clone, Copy)]
struct Dist<D>(D);

macro_rules! impl_increment_method {
    ($($divisor:ty => $t:ty),*) => {
        $(impl Method<$t> for Sat<$divisor> {
            #[inline]
            fn div_rem(self, n: $t) -> ($t, $t) {
                self.0.wrapping_div_rem_sat(n)
            }
        }

        impl Method<$t> for Dist<$divisor> {
            #[inline]
            fn div_rem(self, n: $t) -> ($t, $t) {
                self.0.wrapping_div_rem_dist(n)
            }
        })*
    };
}

impl_increment_method!(
    RoundDownDivisorU32 => u32,
    RoundDownDivisorU64 => u64,
    NBitDivisorU32 => u32,
    NBitDivisorU64 => u64
);

#[inline(never)]
fn div_all<T: Copy, M: Method<T>>(values: &[T], m: M) -> Vec<T> {
    values.iter().map(move |x| m.div_rem(*x).0).collect()
}

#[inline(never)]
fn rem_all<T: Copy, M: Method<T>>(values: &[T], m: M) -> Vec<T> {
    values.iter().map(move |x| m.div_rem(*x).1).collect()
}

#[inline(never)]
fn div_into<T: Copy, M: Method<T>>(values: &[T], out: &mut [T], m: M) {
    for (o, x) in out.iter_mut().zip(values) {
        *o = m.div_rem(*x).0;
    }
}

/// Experiment: `u64 / 13` with the output at `offset` values past the end of the input
fn bench_layout(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("layout_u64");
    group.throughput(Throughput::Elements(SIZE as u64));
    let d: u64 = hint::black_box(13);
    let DivisorU64::Multiplier(fig41) = DivisorU64::new(d).unwrap() else {
        unreachable!()
    };
    let fig42 = Fig42NoPreShiftDivisorU64::try_new(d).unwrap();
    for offset in [0, 1, 2, 8, 64, 256, 500, 511, 512] {
        let mut buf: Vec<u64> = (0..2 * SIZE + 1024).map(|_| rng.random()).collect();
        let (input, rest) = buf.split_at_mut(SIZE);
        let out = &mut rest[offset..offset + SIZE];
        group.bench_function(format!("div 13 offset {offset}/fig4.1"), |b| {
            b.iter(|| div_into(input, out, fig41))
        });
        group.bench_function(format!("div 13 offset {offset}/fig4.2_no_pre_shift"), |b| {
            b.iter(|| div_into(input, out, fig42))
        });
    }
    group.finish();
}

/// Experiment: `/` and `%` by `$d` with `$method`, if it supports `$d`
macro_rules! bench_method {
    ($group:expr, $values:expr, $d:expr, $name:expr, $method:expr) => {
        if let Some(m) = $method {
            let values = &$values;
            $group.bench_function(format!("div {}/{}", $d, $name), |b| {
                b.iter(|| div_all(values, m))
            });
            $group.bench_function(format!("rem {}/{}", $d, $name), |b| {
                b.iter(|| rem_all(values, m))
            });
        }
    };
}

/// Experiment: figure 4.1 against figure 4.2 with and without the pre-shift
macro_rules! bench_unsigned_methods {
    ($c:expr, $t:ty, $enum:ident, $no_pre_shift:ident, $fig42:ident, $divisors:expr) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("method_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in $divisors {
            let d: $t = hint::black_box(d);
            let fig41 = match $enum::new(d).unwrap() {
                $enum::Multiplier(m) => Some(m),
                $enum::PowerOfTwo(_) => None,
            };
            bench_method!(group, values, d, "fig4.1", fig41);
            bench_method!(
                group,
                values,
                d,
                "fig4.2_no_pre_shift",
                $no_pre_shift::try_new(d)
            );
            bench_method!(group, values, d, "fig4.2", $fig42::try_new(d));
        }
        group.finish();
    }};
}

/// Experiment: figure 5.1 against figure 5.2
macro_rules! bench_signed_methods {
    ($c:expr, $t:ty, $enum:ident, $fig52:ident, $divisors:expr) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("method_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in $divisors {
            let d: $t = hint::black_box(d);
            let fig51 = match $enum::new(d).unwrap() {
                $enum::Multiplier(m) => Some(m),
                $enum::PowerOfTwo(_) => None,
            };
            bench_method!(group, values, d, "fig5.1", fig51);
            bench_method!(group, values, d, "fig5.2", $fig52::try_new(d));
        }
        group.finish();
    }};
}

/// Experiment: the remainder of Lemire, Kaser and Kurz, which computes no quotient
#[inline(never)]
fn lkk_rem_all_u32(values: &[u32], d: LkkRemDivisorU32) -> Vec<u32> {
    values.iter().map(move |x| d.wrapping_rem(*x)).collect()
}

#[inline(never)]
fn lkk_rem_all_i32(values: &[i32], d: LkkRemDivisorI32) -> Vec<i32> {
    values.iter().map(move |x| d.wrapping_rem(*x)).collect()
}

/// Experiment: `%` with the multipliers against the remainder of Lemire, Kaser and Kurz
fn bench_lkk(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let values: Vec<u32> = (0..SIZE).map(|_| rng.random()).collect();
    let mut group = c.benchmark_group("lkk_u32");
    group.throughput(Throughput::Elements(SIZE as u64));
    for d in [7u32, 13, 100, 1000] {
        let d = hint::black_box(d);
        if let DivisorU32::Multiplier(m) = DivisorU32::new(d).unwrap() {
            group.bench_function(format!("rem {d}/fig4.1"), |b| {
                b.iter(|| rem_all(&values, m))
            });
        }
        if let Some(m) = Fig42NoPreShiftDivisorU32::try_new(d) {
            let name = format!("rem {d}/fig4.2_no_pre_shift");
            group.bench_function(name, |b| b.iter(|| rem_all(&values, m)));
        }
        let lkk = LkkRemDivisorU32::try_new(d).unwrap();
        group.bench_function(format!("rem {d}/lkk"), |b| {
            b.iter(|| lkk_rem_all_u32(&values, lkk))
        });
    }
    group.finish();

    let values: Vec<i32> = (0..SIZE).map(|_| rng.random()).collect();
    let mut group = c.benchmark_group("lkk_i32");
    group.throughput(Throughput::Elements(SIZE as u64));
    for d in [7i32, 13, -13] {
        let d = hint::black_box(d);
        if let DivisorI32::Multiplier(m) = DivisorI32::new(d).unwrap() {
            group.bench_function(format!("rem {d}/fig5.1"), |b| {
                b.iter(|| rem_all(&values, m))
            });
        }
        let m = Fig51MagnitudeDivisorI32::try_new(d).unwrap();
        group.bench_function(format!("rem {d}/fig5.1_magnitude"), |b| {
            b.iter(|| rem_all(&values, m))
        });
        let lkk = LkkRemDivisorI32::try_new(d).unwrap();
        group.bench_function(format!("rem {d}/lkk"), |b| {
            b.iter(|| lkk_rem_all_i32(&values, lkk))
        });
    }
    group.finish();
}

/// Experiment: figure 4.1 against the N-bit multipliers, rounded up or down
macro_rules! bench_round_down {
    ($c:expr, $t:ty, $enum:ident, $no_pre_shift:ident, $round_down:ident, $n_bit:ident) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("round_down_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in [7, 13, 100, 1000] {
            let d: $t = hint::black_box(d);
            let fig41 = match $enum::new(d).unwrap() {
                $enum::Multiplier(m) => Some(m),
                $enum::PowerOfTwo(_) => None,
            };
            bench_method!(group, values, d, "fig4.1", fig41);
            bench_method!(
                group,
                values,
                d,
                "fig4.2_no_pre_shift",
                $no_pre_shift::try_new(d)
            );
            bench_method!(
                group,
                values,
                d,
                "round_down_sat",
                $round_down::try_new(d).map(Sat)
            );
            bench_method!(
                group,
                values,
                d,
                "round_down_dist",
                $round_down::try_new(d).map(Dist)
            );
            bench_method!(group, values, d, "n_bit_sat", $n_bit::try_new(d).map(Sat));
            bench_method!(group, values, d, "n_bit_dist", $n_bit::try_new(d).map(Dist));
        }
        group.finish();
    }};
}

/// Experiment: figure 5.1 against figure 5.1 with devanbenz's remainder
macro_rules! bench_magnitude {
    ($c:expr, $t:ty, $enum:ident, $magnitude:ident) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("magnitude_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in [7, 13, -13] {
            let d: $t = hint::black_box(d);
            let fig51 = match $enum::new(d).unwrap() {
                $enum::Multiplier(m) => Some(m),
                $enum::PowerOfTwo(_) => None,
            };
            bench_method!(group, values, d, "fig5.1", fig51);
            bench_method!(group, values, d, "fig5.1_magnitude", $magnitude::try_new(d));
        }
        group.finish();
    }};
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_lkk(c);
    bench_round_down!(
        c,
        u32,
        DivisorU32,
        Fig42NoPreShiftDivisorU32,
        RoundDownDivisorU32,
        NBitDivisorU32
    );
    bench_round_down!(
        c,
        u64,
        DivisorU64,
        Fig42NoPreShiftDivisorU64,
        RoundDownDivisorU64,
        NBitDivisorU64
    );
    bench_magnitude!(c, i32, DivisorI32, Fig51MagnitudeDivisorI32);
    bench_magnitude!(c, i64, DivisorI64, Fig51MagnitudeDivisorI64);
    bench_layout(c);
    bench_unsigned_methods!(
        c,
        u32,
        DivisorU32,
        Fig42NoPreShiftDivisorU32,
        Fig42DivisorU32,
        [13, 100]
    );
    bench_unsigned_methods!(
        c,
        u64,
        DivisorU64,
        Fig42NoPreShiftDivisorU64,
        Fig42DivisorU64,
        [13, 100, 1000]
    );
    bench_signed_methods!(c, i32, DivisorI32, Fig52DivisorI32, [3, 10]);
    bench_signed_methods!(c, i64, DivisorI64, Fig52DivisorI64, [3, 10]);
    bench_divisor!(c, DivisorU32, u32);
    bench_divisor!(c, DivisorI32, i32);
    bench_divisor!(c, DivisorU64, u64);
    bench_divisor!(c, DivisorI64, i64);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
