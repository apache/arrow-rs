// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_cast::parse::string_to_time_nanoseconds;
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    let timestamps = [
        "9:50",
        "09:50",
        "09:50 PM",
        "9:50:12 AM",
        "09:50:12 PM",
        "09:50:12.123456789",
        "9:50:12.123456789",
        "09:50:12.123456789 PM",
    ];

    for timestamp in timestamps {
        let t = hint::black_box(timestamp);
        c.bench_function(t, |b| {
            b.iter(|| string_to_time_nanoseconds(t).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
