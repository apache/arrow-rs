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

//! Benchmarks for named (IANA) timezones, which the `chrono-tz` and `jiff` features resolve.
//!
//! Run with `--features chrono-tz` and again with `--features chrono-tz,jiff` to compare the two
//! implementations behind [`Tz`].

use arrow::array::timezone::Tz;
use arrow::array::{TimestampMicrosecondArray, TimestampMillisecondArray};
use arrow::compute::{DatePart, cast, date_part};
use arrow::datatypes::DataType;
use chrono::{DateTime, NaiveDateTime, TimeZone};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

const ZONES: [&str; 4] = ["UTC", "America/Los_Angeles", "Europe/London", "Asia/Tokyo"];

/// Microseconds since the epoch at the start of 2038, 2100 and 2300
const Y2038: i64 = 2_145_916_800_000_000;
const Y2100: i64 = 4_102_444_800_000_000;
const Y2300: i64 = 10_413_792_000_000_000;

/// Instant ranges in microseconds. Up to 2038 both read a transition table, as fat TZif data, and
/// so jiff's tables, stop there. From 2038 to 2100, jiff evaluates the zone's POSIX TZ rule while
/// chrono-tz still reads its table, which runs to 2099, and both agree. After 2100, chrono-tz
/// keeps its last offset, standard time for these zones, where jiff still applies daylight saving.
const RANGES: [(&str, i64, i64); 3] = [
    ("1970_to_2038", 0, Y2038),
    ("2038_to_2100", Y2038, Y2100),
    ("2100_to_2300", Y2100, Y2300),
];

/// Microsecond instants spread evenly over the range
fn instants(len: usize, start: i64, end: i64) -> Vec<i64> {
    (0..len)
        .map(|i| start + (end - start) / len as i64 * i as i64)
        .collect()
}

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("tz_parse");
    for zone in ZONES {
        group.bench_function(zone, |b| {
            b.iter(|| black_box(black_box(zone).parse::<Tz>().unwrap()))
        });
    }
    group.finish();
}

fn bench_offset_lookup(c: &mut Criterion) {
    for (range, start, end) in RANGES {
        let utc: Vec<NaiveDateTime> = instants(8192, start, end)
            .into_iter()
            .map(|micros| DateTime::from_timestamp_micros(micros).unwrap().naive_utc())
            .collect();

        let mut group = c.benchmark_group(format!("tz_offset_from_utc_datetime/{range}"));
        group.throughput(criterion::Throughput::Elements(utc.len() as u64));
        for zone in ZONES {
            let tz: Tz = zone.parse().unwrap();
            group.bench_function(zone, |b| {
                b.iter(|| {
                    for dt in &utc {
                        black_box(tz.offset_from_utc_datetime(black_box(dt)));
                    }
                })
            });
        }
        group.finish();

        let mut group = c.benchmark_group(format!("tz_offset_from_local_datetime/{range}"));
        group.throughput(criterion::Throughput::Elements(utc.len() as u64));
        for zone in ZONES {
            let tz: Tz = zone.parse().unwrap();
            group.bench_function(zone, |b| {
                b.iter(|| {
                    for dt in &utc {
                        black_box(tz.offset_from_local_datetime(black_box(dt)));
                    }
                })
            });
        }
        group.finish();
    }
}

fn bench_kernels(c: &mut Criterion) {
    for (range, start, end) in RANGES {
        let micros = TimestampMicrosecondArray::from(instants(1 << 20, start, end));
        let millis = TimestampMillisecondArray::from(
            instants(1 << 17, start, end)
                .into_iter()
                .map(|v| v / 1000)
                .collect::<Vec<_>>(),
        );

        let mut group = c.benchmark_group(format!("date_part_hour/1M/{range}"));
        for zone in ZONES {
            let array = micros.clone().with_timezone(zone);
            group.bench_function(zone, |b| {
                b.iter(|| black_box(date_part(black_box(&array), DatePart::Hour).unwrap()))
            });
        }
        group.finish();

        let mut group = c.benchmark_group(format!("cast_timestamp_to_utf8/128K/{range}"));
        for zone in ZONES {
            let array = millis.clone().with_timezone(zone);
            group.bench_function(zone, |b| {
                b.iter(|| black_box(cast(black_box(&array), &DataType::Utf8).unwrap()))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_parse, bench_offset_lookup, bench_kernels);
criterion_main!(benches);
