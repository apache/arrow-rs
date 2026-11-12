// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::StringRunBuilder;
use arrow::datatypes::Int32Type;
use arrow::util::bench_util::create_string_array_for_runs;
use criterion::{Criterion, criterion_group, criterion_main};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_run_builder");

    let mut do_bench = |physical_array_len: usize, logical_array_len: usize, string_len: usize| {
        group.bench_function(
                format!(
                    "(run_array_len:{logical_array_len}, physical_array_len:{physical_array_len}, string_len: {string_len})",
                ),
                |b| {
                    let strings =
                    create_string_array_for_runs(physical_array_len, logical_array_len, string_len);
                    b.iter(|| {
                        let mut builder = StringRunBuilder::<Int32Type>::with_capacity(
                            physical_array_len,
                            (string_len + 1) * physical_array_len,
                        );

                        for val in &strings {
                            builder.append_value(val);
                        }

                        builder.finish();
                    })
                },
            );
    };

    do_bench(20, 1000, 5);
    do_bench(100, 1000, 5);
    do_bench(100, 1000, 10);
    do_bench(100, 10000, 10);
    do_bench(100, 10000, 100);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
