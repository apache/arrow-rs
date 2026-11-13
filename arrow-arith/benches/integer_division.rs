// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;

use arrow_arith::numeric::{div, rem};
use arrow_array::types::{Int32Type, Int64Type, UInt32Type, UInt64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType, Datum, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::ArrowError;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::distr::{Distribution, StandardUniform};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const SIZE: usize = 8192;

type Kernel = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;

fn benchmark<T: ArrowPrimitiveType>(c: &mut Criterion, name: &str)
where
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = StdRng::seed_from_u64(42);
    let array = PrimitiveArray::<T>::from_iter_values((0..SIZE).map(|_| rng.random()));
    // A fifth, half, and nine tenths of the values null, at random positions. Visiting only the
    // valid values does less work as the share of nulls grows
    let with_nulls: Vec<_> = [
        ("mixed_nulls", 4, 5),
        ("half_nulls", 1, 2),
        ("mostly_nulls", 1, 10),
    ]
    .into_iter()
    .map(|(name, valid, of)| {
        let nulls = NullBuffer::from_iter((0..SIZE).map(|_| rng.random_ratio(valid, of)));
        (
            name,
            PrimitiveArray::<T>::new(array.values().clone(), Some(nulls)),
        )
    })
    .collect();
    let mut group = c.benchmark_group(format!("integer_division_{name}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (op, kernel) in [("div", div as Kernel), ("rem", rem)] {
        // A precomputed divisor picks a variant from the divisor: for unsigned types 7 needs a
        // multiplier wider than N bits and 13 doesn't, and 16 is a power of two, which a shift
        // or mask replaces
        for d in [7, 13, 16] {
            let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(d));
            group.bench_function(format!("{op}_{d}/no_nulls"), |b| {
                b.iter(|| black_box(kernel(black_box(&array), black_box(&divisor)).unwrap()))
            });
        }
        let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(13));
        for (nulls, array) in &with_nulls {
            group.bench_function(format!("{op}_13/{nulls}"), |b| {
                b.iter(|| black_box(kernel(black_box(array), black_box(&divisor)).unwrap()))
            });
        }
    }
    // A small array, where building a divisor is a larger share of the work. On fewer values,
    // the fixed cost of each call is far larger than building a divisor for these types.
    let small = array.slice(0, 128);
    let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(13));
    group.throughput(Throughput::Elements(small.len() as u64));
    group.bench_function("div_13/128_values", |b| {
        b.iter(|| black_box(div(black_box(&small), black_box(&divisor)).unwrap()))
    });
    group.finish();
}

fn integer_division(c: &mut Criterion) {
    benchmark::<Int32Type>(c, "int32");
    benchmark::<UInt32Type>(c, "uint32");
    benchmark::<Int64Type>(c, "int64");
    benchmark::<UInt64Type>(c, "uint64");
}

criterion_group!(benches, integer_division);
criterion_main!(benches);
