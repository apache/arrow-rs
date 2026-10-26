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

use arrow_buffer::{DivisorI32, DivisorI64, DivisorU32, DivisorU64};
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

fn criterion_benchmark(c: &mut Criterion) {
    bench_divisor!(c, DivisorU32, u32, [PowerOfTwo, Up, Down, AddIndicator]);
    bench_divisor!(c, DivisorI32, i32, [PowerOfTwo, AddIndicator]);
    bench_divisor!(c, DivisorU64, u64, [PowerOfTwo, Up, AddIndicator]);
    bench_divisor!(c, DivisorI64, i64, [PowerOfTwo, AddIndicator]);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
