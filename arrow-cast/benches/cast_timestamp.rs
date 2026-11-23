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

fn benchmark<T: ArrowTimestampType>(c: &mut Criterion, unit: &str, units_per_second: i64) {
    // Timestamps spread over roughly 63 years either side of the epoch.
    let mut rng = StdRng::seed_from_u64(42);
    let range = -2_000_000_000 * units_per_second..2_000_000_000 * units_per_second;
    let values: Vec<i64> = (0..SIZE).map(|_| rng.random_range(range.clone())).collect();
    let mixed_nulls = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    let no_nulls = PrimitiveArray::<T>::new(values.into(), None);
    // Timezone-aware casts go through Chrono and serve as a control.
    let zoned = no_nulls.clone().with_timezone("+05:45");
    let inputs = [
        ("no_nulls", &no_nulls),
        ("mixed_nulls", &mixed_nulls),
        ("timezone_control", &zoned),
    ];
    let mut targets = vec![
        ("date32".to_string(), DataType::Date32),
        ("time32_s".to_string(), DataType::Time32(TimeUnit::Second)),
        (
            "time32_ms".to_string(),
            DataType::Time32(TimeUnit::Millisecond),
        ),
        (
            "time64_us".to_string(),
            DataType::Time64(TimeUnit::Microsecond),
        ),
        (
            "time64_ns".to_string(),
            DataType::Time64(TimeUnit::Nanosecond),
        ),
    ];
    // Casts to a coarser unit divide each value, and casts to a finer unit multiply it
    for (to_unit, to) in other_units(T::UNIT) {
        targets.push((
            format!("timestamp_{to_unit}"),
            DataType::Timestamp(to, None),
        ));
    }
    let mut group = c.benchmark_group(format!("timestamp_{unit}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (name, target) in &targets {
        for (input, array) in inputs {
            // A cast to a timestamp without a time zone doesn't adjust, so it isn't a control
            if input == "timezone_control" && matches!(target, DataType::Timestamp(_, _)) {
                continue;
            }
            group.bench_function(format!("{name}/{input}"), |b| {
                b.iter(|| black_box(cast(black_box(array), target).unwrap()))
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
    // Durations of up to roughly 63 years either way
    let mut rng = StdRng::seed_from_u64(42);
    let range = -2_000_000_000 * units_per_second..2_000_000_000 * units_per_second;
    let values: Vec<i64> = (0..SIZE).map(|_| rng.random_range(range.clone())).collect();
    let mixed_nulls = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    let no_nulls = PrimitiveArray::<T>::new(values.into(), None);
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
