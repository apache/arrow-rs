// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;

use arrow_array::{PrimitiveArray, types::*};
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const SIZE: usize = 8192;

/// Returns `SIZE` values spread over roughly 63 years either side of zero, without nulls and
/// with a fifth of them null
fn arrays<T: ArrowPrimitiveType<Native = i64>>(
    units_per_second: i64,
) -> (PrimitiveArray<T>, PrimitiveArray<T>) {
    let mut rng = StdRng::seed_from_u64(42);
    let range = -2_000_000_000 * units_per_second..2_000_000_000 * units_per_second;
    let values: Vec<i64> = (0..SIZE).map(|_| rng.random_range(range.clone())).collect();
    let mixed_nulls = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    (PrimitiveArray::<T>::new(values.into(), None), mixed_nulls)
}

fn benchmark<T: ArrowTimestampType>(c: &mut Criterion, unit: &str, units_per_second: i64) {
    let (no_nulls, mixed_nulls) = arrays::<T>(units_per_second);
    // Timezone-aware casts go through Chrono and serve as a control.
    let zoned = no_nulls.clone().with_timezone("+05:45");
    let inputs = [
        ("no_nulls", &no_nulls),
        ("mixed_nulls", &mixed_nulls),
        ("timezone_control", &zoned),
    ];
    let targets = [
        ("date32", DataType::Date32),
        ("time32_s", DataType::Time32(TimeUnit::Second)),
        ("time32_ms", DataType::Time32(TimeUnit::Millisecond)),
        ("time64_us", DataType::Time64(TimeUnit::Microsecond)),
        ("time64_ns", DataType::Time64(TimeUnit::Nanosecond)),
    ];
    let mut group = c.benchmark_group(format!("timestamp_{unit}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (name, target) in &targets {
        for (input, array) in inputs {
            group.bench_function(format!("{name}/{input}"), |b| {
                b.iter(|| black_box(cast(black_box(array), target).unwrap()))
            });
        }
    }
    // Casts to a coarser unit divide each value, and casts to a finer unit multiply it
    for (to_unit, to) in other_units(T::UNIT) {
        let target = DataType::Timestamp(to, None);
        for (input, array) in [("no_nulls", &no_nulls), ("mixed_nulls", &mixed_nulls)] {
            group.bench_function(format!("timestamp_{to_unit}/{input}"), |b| {
                b.iter(|| black_box(cast(black_box(array), &target).unwrap()))
            });
        }
    }
    group.finish();
}

/// Returns the names and units other than `unit`
fn other_units(unit: TimeUnit) -> impl Iterator<Item = (&'static str, TimeUnit)> {
    [
        ("s", TimeUnit::Second),
        ("ms", TimeUnit::Millisecond),
        ("us", TimeUnit::Microsecond),
        ("ns", TimeUnit::Nanosecond),
    ]
    .into_iter()
    .filter(move |(_, u)| *u != unit)
}

fn duration_benchmark<T: ArrowPrimitiveType<Native = i64>>(
    c: &mut Criterion,
    unit: &str,
    from: TimeUnit,
    units_per_second: i64,
) {
    let (no_nulls, mixed_nulls) = arrays::<T>(units_per_second);
    let mut group = c.benchmark_group(format!("duration_{unit}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (to_unit, to) in other_units(from) {
        let target = DataType::Duration(to);
        for (input, array) in [("no_nulls", &no_nulls), ("mixed_nulls", &mixed_nulls)] {
            group.bench_function(format!("duration_{to_unit}/{input}"), |b| {
                b.iter(|| black_box(cast(black_box(array), &target).unwrap()))
            });
        }
    }
    group.finish();
}

fn cast_timestamp(c: &mut Criterion) {
    benchmark::<TimestampSecondType>(c, "s", 1);
    benchmark::<TimestampMillisecondType>(c, "ms", 1_000);
    benchmark::<TimestampMicrosecondType>(c, "us", 1_000_000);
    benchmark::<TimestampNanosecondType>(c, "ns", 1_000_000_000);
    duration_benchmark::<DurationSecondType>(c, "s", TimeUnit::Second, 1);
    duration_benchmark::<DurationMillisecondType>(c, "ms", TimeUnit::Millisecond, 1_000);
    duration_benchmark::<DurationMicrosecondType>(c, "us", TimeUnit::Microsecond, 1_000_000);
    duration_benchmark::<DurationNanosecondType>(c, "ns", TimeUnit::Nanosecond, 1_000_000_000);
}

criterion_group!(benches, cast_timestamp);
criterion_main!(benches);
