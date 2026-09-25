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

use std::hint::black_box;

use arrow_arith::temporal::{DatePart, date_part};
use arrow_array::PrimitiveArray;
use arrow_array::types::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const SIZE: usize = 8192;

const TIME_PARTS: [DatePart; 6] = [
    DatePart::Hour,
    DatePart::Minute,
    DatePart::Second,
    DatePart::Millisecond,
    DatePart::Microsecond,
    DatePart::Nanosecond,
];

fn benchmark<T: ArrowTimestampType>(c: &mut Criterion, unit: &str, units_per_second: i64) {
    // A deterministic mix of dates before and after the epoch, including subseconds.
    let values: Vec<i64> = (0..SIZE)
        .map(|i| {
            let seconds = (i as i64 * 1_000_003) % 4_000_000_000 - 2_000_000_000;
            seconds * units_per_second + (i as i64 * 7919) % units_per_second
        })
        .collect();
    let with_nulls = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    let array = PrimitiveArray::<T>::new(values.into(), None);
    let mut group = c.benchmark_group(format!("timestamp_{unit}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    // Second timestamps have no fractional part, so their subsecond parts are
    // constant zero and not worth measuring.
    let parts = if units_per_second == 1 {
        &TIME_PARTS[..3]
    } else {
        &TIME_PARTS[..]
    };
    for &part in parts {
        group.bench_function(format!("{part}/no_nulls"), |b| {
            b.iter(|| black_box(date_part(black_box(&array), part).unwrap()))
        });
    }
    // Validity is not consulted per element, so one nullable case suffices.
    group.bench_function("Hour/mixed_nulls", |b| {
        b.iter(|| black_box(date_part(black_box(&with_nulls), DatePart::Hour).unwrap()))
    });
    // Reference for the calendar conversion path.
    group.bench_function("Year/control", |b| {
        b.iter(|| black_box(date_part(black_box(&array), DatePart::Year).unwrap()))
    });
    // Timestamps with a timezone always take the calendar conversion path.
    let with_timezone = array.with_timezone("+05:45");
    for part in [DatePart::Minute, DatePart::Nanosecond] {
        group.bench_function(format!("{part}/timezone_control"), |b| {
            b.iter(|| black_box(date_part(black_box(&with_timezone), part).unwrap()))
        });
    }
    group.finish();
}

fn temporal(c: &mut Criterion) {
    benchmark::<TimestampSecondType>(c, "s", 1);
    benchmark::<TimestampMillisecondType>(c, "ms", 1_000);
    benchmark::<TimestampMicrosecondType>(c, "us", 1_000_000);
    benchmark::<TimestampNanosecondType>(c, "ns", 1_000_000_000);
}

criterion_group!(benches, temporal);
criterion_main!(benches);
