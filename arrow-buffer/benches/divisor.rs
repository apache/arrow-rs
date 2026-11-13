// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_buffer::{
    DivisorI32, DivisorI64, DivisorI128, DivisorI256, DivisorU32, DivisorU64, i256,
};
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const SIZE: usize = 8192;

/// Matches on the variant of `$divisor` once, outside the loop, and moves it into the
/// closure, as a kernel would. The last arm covers variants the enum may add later
macro_rules! div_rem_all {
    ($values:expr, $divisor:expr, $enum:ident, $field:tt, [$($variant:ident),+]) => {
        match $divisor {
            $($enum::$variant(d) => $values
                .iter()
                .map(move |x| d.wrapping_div_rem(*x).$field)
                .collect::<Vec<_>>(),)+
            d => $values
                .iter()
                .map(move |x| d.wrapping_div_rem(*x).$field)
                .collect::<Vec<_>>(),
        }
    };
}

/// Divides `SIZE` random values by 7, 13 and 16 with `/` and `%`, and with the divisor type.
/// 13 uses `Up` where the type has it, 16 uses `PowerOfTwo`, and 7 uses `Down` for `u32` on
/// targets with cheap saturation and `AddIndicator` otherwise
macro_rules! bench_divisor {
    ($c:expr, $divisor:ident, $t:ty, $variants:tt) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<$t> = (0..SIZE).map(|_| rng.random()).collect();
        let mut group = $c.benchmark_group(concat!("divisor_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for d in [7, 13, 16] {
            // Hide the divisor from the compiler, as it is when it comes from a `Scalar`
            let d: $t = hint::black_box(d);
            let divisor = $divisor::new(d).unwrap();
            group.bench_function(format!("div {d}/operator"), |b| {
                b.iter(|| values.iter().map(move |x| x / d).collect::<Vec<_>>())
            });
            group.bench_function(format!("div {d}/divisor"), |b| {
                b.iter(|| div_rem_all!(values, divisor, $divisor, 0, $variants))
            });
            group.bench_function(format!("rem {d}/operator"), |b| {
                b.iter(|| values.iter().map(move |x| x % d).collect::<Vec<_>>())
            });
            group.bench_function(format!("rem {d}/divisor"), |b| {
                b.iter(|| div_rem_all!(values, divisor, $divisor, 1, $variants))
            });
        }
        group.finish();
    }};
}

/// Divides `SIZE` values by a divisor of each variant and by a power of ten that a decimal kernel
/// divides by, with `/` and `%`, and with the divisor type. `full` values use every bit, and
/// `narrow` values fit in the next narrower type, where `/` and `%` take a faster path
macro_rules! bench_wide {
    ($c:expr, $divisor:ident, $t:ty, $full:expr, $narrow:expr, $divisors:expr, $variants:tt) => {{
        let mut rng = StdRng::seed_from_u64(42);
        let full: Vec<$t> = (0..SIZE).map(|_| $full(&mut rng)).collect();
        let narrow: Vec<$t> = (0..SIZE).map(|_| $narrow(&mut rng)).collect();
        let mut group = $c.benchmark_group(concat!("divisor_", stringify!($t)));
        group.throughput(Throughput::Elements(SIZE as u64));
        for (width, values) in [("full", &full), ("narrow", &narrow)] {
            for (label, d) in $divisors {
                let d: $t = hint::black_box(d);
                let divisor = $divisor::new(d).unwrap();
                group.bench_function(format!("div {label} {width}/operator"), |b| {
                    b.iter(|| values.iter().map(move |x| *x / d).collect::<Vec<_>>())
                });
                group.bench_function(format!("div {label} {width}/divisor"), |b| {
                    b.iter(|| div_rem_all!(values, divisor, $divisor, 0, $variants))
                });
                group.bench_function(format!("rem {label} {width}/operator"), |b| {
                    b.iter(|| values.iter().map(move |x| *x % d).collect::<Vec<_>>())
                });
                group.bench_function(format!("rem {label} {width}/divisor"), |b| {
                    b.iter(|| div_rem_all!(values, divisor, $divisor, 1, $variants))
                });
            }
        }
        group.finish();
    }};
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_divisor!(c, DivisorU32, u32, [PowerOfTwo, Up, Down, AddIndicator]);
    bench_divisor!(c, DivisorI32, i32, [PowerOfTwo, AddIndicator]);
    bench_divisor!(c, DivisorU64, u64, [PowerOfTwo, Up, AddIndicator]);
    bench_divisor!(c, DivisorI64, i64, [PowerOfTwo, AddIndicator]);
    bench_wide!(
        c,
        DivisorI128,
        i128,
        |rng: &mut StdRng| rng.random::<i128>(),
        |rng: &mut StdRng| rng.random::<i64>() as i128,
        [("7", 7), ("16", 16), ("1e18", 10i128.pow(18))],
        [PowerOfTwo, AddIndicator]
    );
    let i256_of = i256::from_i128;
    bench_wide!(
        c,
        DivisorI256,
        i256,
        |rng: &mut StdRng| i256::from_parts(rng.random(), rng.random()),
        |rng: &mut StdRng| i256::from_i128(rng.random()),
        [
            ("7", i256_of(7)),
            ("16", i256_of(16)),
            ("1e38", i256_of(10i128.pow(38)))
        ],
        [PowerOfTwo, AddIndicator]
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
