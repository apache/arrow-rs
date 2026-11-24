// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;

use arrow_array::{Array, Date64Array, TimestampNanosecondArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

const ARRAY_LEN: usize = 8192;
const SHORT_FORMAT: &str = "%Y-%m-%d";
const LONG_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

fn format_array(c: &mut Criterion) {
    let date64 = Date64Array::from_value(1_754_668_645_123, ARRAY_LEN);
    let timestamp = TimestampNanosecondArray::from_value(1_754_668_645_123_456_789, ARRAY_LEN);
    let timestamp_tz = timestamp.clone().with_timezone("+08:00");

    let mut group = c.benchmark_group("format_temporal");

    for (id, array, options) in [
        (
            BenchmarkId::new("date64", "default"),
            &date64 as &dyn Array,
            FormatOptions::new(),
        ),
        (
            BenchmarkId::new("date64", "custom_short"),
            &date64 as &dyn Array,
            FormatOptions::new().with_datetime_format(Some(SHORT_FORMAT)),
        ),
        (
            BenchmarkId::new("date64", "custom_long"),
            &date64 as &dyn Array,
            FormatOptions::new().with_datetime_format(Some(LONG_FORMAT)),
        ),
        (
            BenchmarkId::new("timestamp", "custom_long"),
            &timestamp as &dyn Array,
            FormatOptions::new().with_timestamp_format(Some(LONG_FORMAT)),
        ),
        (
            BenchmarkId::new("timestamp_tz", "custom_long"),
            &timestamp_tz as &dyn Array,
            FormatOptions::new().with_timestamp_tz_format(Some(LONG_FORMAT)),
        ),
    ] {
        let formatter = ArrayFormatter::try_new(array, &options).unwrap();
        let mut output = String::with_capacity(32);

        group.bench_function(id, |b| {
            b.iter(|| {
                for idx in 0..array.len() {
                    output.clear();
                    formatter.value(idx).write(&mut output).unwrap();
                }
                black_box(&output);
            })
        });
    }

    group.finish();
}

criterion_group!(benches, format_array);
criterion_main!(benches);
