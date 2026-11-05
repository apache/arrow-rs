// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::PrimitiveDictionaryBuilder;
use arrow::datatypes::{ArrowPrimitiveType, Decimal128Type, Int32Type, UInt32Type, UInt64Type};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::distr::{Distribution, StandardUniform};
use rand::{RngExt, rng};

/// Note: this is best effort, not all values are necessarily present or unique
fn build_values<T: Copy>(dict_size: usize, total_size: usize) -> Vec<T>
where
    StandardUniform: Distribution<T>,
{
    let mut rng = rng();
    let values: Vec<T> = (0..dict_size).map(|_| rng.random()).collect();

    (0..total_size)
        .map(|_| values[rng.random_range(0..dict_size)])
        .collect()
}

fn bench_values<V>(c: &mut Criterion, name: &str)
where
    V: ArrowPrimitiveType,
    StandardUniform: Distribution<V::Native>,
{
    let mut group = c.benchmark_group("primitive_dictionary_builder");

    let mut do_bench = |dict_size: usize, total_size: usize| {
        group.bench_function(
            format!("({name}, dict_size:{dict_size}, len:{total_size})"),
            |b| {
                let values = build_values::<V::Native>(dict_size, total_size);
                b.iter(|| {
                    let mut builder = PrimitiveDictionaryBuilder::<Int32Type, V>::with_capacity(
                        values.len(),
                        dict_size,
                    );

                    for val in &values {
                        builder.append(*val).unwrap();
                    }

                    builder.finish();
                })
            },
        );
    };

    do_bench(20, 1000);
    do_bench(100, 10000);
    do_bench(10000, 100000);
    do_bench(8192, 512000);

    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_values::<UInt32Type>(c, "UInt32");
    bench_values::<UInt64Type>(c, "UInt64");
    bench_values::<Decimal128Type>(c, "Decimal128");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
