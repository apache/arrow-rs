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
use std::sync::Arc;

use arrow_array::ArrowNativeTypeOp;
use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_array::{ArrayRef, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::DataType;
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const ARRAY_LEN: usize = 8192;

/// Creates an array of `len` random values with `digits` significant digits
/// (no leading zero, half of them negative) for the given `precision` and `scale`
fn decimal_array<T: DecimalType>(
    len: usize,
    digits: u32,
    precision: u8,
    scale: i8,
) -> PrimitiveArray<T> {
    let mut rng = StdRng::seed_from_u64(42);
    let ten = T::Native::usize_as(10);
    let values = (0..len).map(|_| {
        let mut value = T::Native::usize_as(rng.random_range(1..10));
        for _ in 1..digits {
            value = value
                .mul_wrapping(ten)
                .add_wrapping(T::Native::usize_as(rng.random_range(0..10)));
        }
        if rng.random::<bool>() {
            value.neg_wrapping()
        } else {
            value
        }
    });
    PrimitiveArray::<T>::from_iter_values(values)
        .with_precision_and_scale(precision, scale)
        .unwrap()
}

/// Returns an array of [`ARRAY_LEN`] values from [`decimal_array`] without nulls,
/// and the same values with every tenth one null
fn inputs<T: DecimalType>(digits: u32, precision: u8, scale: i8) -> Vec<(&'static str, ArrayRef)> {
    let array = decimal_array::<T>(ARRAY_LEN, digits, precision, scale);
    let (data_type, values, _) = array.clone().into_parts();
    let nulls = NullBuffer::from_iter((0..ARRAY_LEN).map(|i| i % 10 != 0));
    let mixed_nulls = PrimitiveArray::<T>::new(values, Some(nulls)).with_data_type(data_type);
    vec![
        ("no_nulls", Arc::new(array)),
        ("mixed_nulls", Arc::new(mixed_nulls)),
    ]
}

/// Benchmarks casting each of `inputs` to `to_type`, once for each value of
/// `CastOptions::safe` in `modes`
fn bench_cast(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    inputs: Vec<(&str, ArrayRef)>,
    to_type: DataType,
    modes: &[bool],
) {
    for (input, array) in inputs {
        for &safe in modes {
            let options = CastOptions {
                safe,
                ..Default::default()
            };
            let mode = if safe { "safe" } else { "strict" };
            group.throughput(Throughput::Elements(array.len() as u64));
            group.bench_function(format!("{name}/{input}/{mode}"), |b| {
                b.iter(|| {
                    black_box(cast_with_options(black_box(&array), &to_type, &options).unwrap())
                })
            });
        }
    }
}

fn downscale(c: &mut Criterion) {
    let mut group = c.benchmark_group("decimal_downscale");
    bench_cast(
        &mut group,
        "decimal32 (9, 4) -> (9, 2) 9 digits",
        inputs::<Decimal32Type>(9, 9, 4),
        DataType::Decimal32(9, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal64 (18, 6) -> (18, 2) 18 digits",
        inputs::<Decimal64Type>(18, 18, 6),
        DataType::Decimal64(18, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal128 (18, 6) -> (18, 2) 18 digits",
        inputs::<Decimal128Type>(18, 18, 6),
        DataType::Decimal128(18, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal128 (38, 10) -> (38, 2) 15 digits",
        inputs::<Decimal128Type>(15, 38, 10),
        DataType::Decimal128(38, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal128 (38, 10) -> (38, 2) 38 digits",
        inputs::<Decimal128Type>(38, 38, 10),
        DataType::Decimal128(38, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal128 (38, 20) -> (38, 2) 38 digits",
        inputs::<Decimal128Type>(38, 38, 20),
        DataType::Decimal128(38, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal128 (38, 10) -> (30, 2) 30 digits fallible",
        inputs::<Decimal128Type>(30, 38, 10),
        DataType::Decimal128(30, 2),
        &[true, false],
    );
    bench_cast(
        &mut group,
        "decimal256 (76, 10) -> (76, 2) 38 digits",
        inputs::<Decimal256Type>(38, 76, 10),
        DataType::Decimal256(76, 2),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal256 (76, 10) -> (76, 2) 60 digits",
        inputs::<Decimal256Type>(60, 76, 10),
        DataType::Decimal256(76, 2),
        &[true],
    );
    // The next three are the result types of Decimal256 multiplies rescaled to
    // Decimal128(38, 6), as when a Decimal128 multiply is evaluated in Decimal256
    bench_cast(
        &mut group,
        "decimal256 (41, 20) -> decimal128(38, 6) 30 digits",
        inputs::<Decimal256Type>(30, 41, 20),
        DataType::Decimal128(38, 6),
        &[true],
    );
    bench_cast(
        &mut group,
        "decimal256 (76, 20) -> decimal128(38, 6) 48 digits fallible",
        inputs::<Decimal256Type>(48, 76, 20),
        DataType::Decimal128(38, 6),
        &[true, false],
    );
    bench_cast(
        &mut group,
        "decimal256 (76, 40) -> decimal128(38, 6) 42 digits fallible",
        inputs::<Decimal256Type>(42, 76, 40),
        DataType::Decimal128(38, 6),
        &[true, false],
    );
    group.finish();
}

/// Small arrays, where the cost of setting up a division can outweigh the per-value work
fn downscale_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("decimal_downscale_small");
    for len in [1, 16, 128] {
        bench_cast(
            &mut group,
            &format!("decimal128 (38, 20) -> (38, 2) 38 digits/{len}"),
            vec![(
                "no_nulls",
                Arc::new(decimal_array::<Decimal128Type>(len, 38, 38, 20)),
            )],
            DataType::Decimal128(38, 2),
            &[true],
        );
        bench_cast(
            &mut group,
            &format!("decimal256 (41, 20) -> decimal128(38, 6) 30 digits/{len}"),
            vec![(
                "no_nulls",
                Arc::new(decimal_array::<Decimal256Type>(len, 30, 41, 20)),
            )],
            DataType::Decimal128(38, 6),
            &[true],
        );
        bench_cast(
            &mut group,
            &format!("decimal256 (76, 20) -> decimal128(38, 6) 48 digits/{len}"),
            vec![(
                "no_nulls",
                Arc::new(decimal_array::<Decimal256Type>(len, 48, 76, 20)),
            )],
            DataType::Decimal128(38, 6),
            &[true],
        );
    }
    group.finish();
}

fn to_integer(c: &mut Criterion) {
    let mut group = c.benchmark_group("decimal_to_int64");
    for (name, inputs) in [
        (
            "decimal64 (18, 6) 18 digits",
            inputs::<Decimal64Type>(18, 18, 6),
        ),
        (
            "decimal128 (38, 10) 18 digits",
            inputs::<Decimal128Type>(18, 38, 10),
        ),
        (
            "decimal128 (38, 20) 38 digits",
            inputs::<Decimal128Type>(38, 38, 20),
        ),
        (
            "decimal256 (76, 20) 38 digits",
            inputs::<Decimal256Type>(38, 76, 20),
        ),
        (
            "decimal256 (76, 40) 58 digits",
            inputs::<Decimal256Type>(58, 76, 40),
        ),
    ] {
        bench_cast(&mut group, name, inputs, DataType::Int64, &[true, false]);
    }
    group.finish();
}

criterion_group!(benches, downscale, downscale_small, to_integer);
criterion_main!(benches);
