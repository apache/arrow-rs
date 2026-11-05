// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_cast::parse::string_to_timestamp_nanos;
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    let timestamps = [
        "2020-09-08",
        "2020-09-08T13:42:29",
        "2020-09-08T13:42:29.190",
        "2020-09-08T13:42:29.190855",
        "2020-09-08T13:42:29.190855999",
        "2020-09-08T13:42:29+00:00",
        "2020-09-08T13:42:29.190+00:00",
        "2020-09-08T13:42:29.190855+00:00",
        "2020-09-08T13:42:29.190855999-05:00",
        "2020-09-08T13:42:29.190855Z",
    ];

    for timestamp in timestamps {
        let t = hint::black_box(timestamp);
        c.bench_function(t, |b| {
            b.iter(|| string_to_timestamp_nanos(t).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
