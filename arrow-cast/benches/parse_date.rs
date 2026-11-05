// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::types::Date32Type;
use arrow_cast::parse::Parser;
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    let timestamps = ["2020-09-08", "2020-9-8", "2020-09-8", "2020-9-08"];

    for timestamp in timestamps {
        let t = hint::black_box(timestamp);
        c.bench_function(t, |b| {
            b.iter(|| Date32Type::parse(t).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
