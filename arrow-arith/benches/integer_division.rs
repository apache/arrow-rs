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

/// Creates an array of `len` uniformly random values
fn random_array<T: ArrowPrimitiveType>(len: usize) -> PrimitiveArray<T>
where
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = StdRng::seed_from_u64(42);
    PrimitiveArray::from_iter_values((0..len).map(|_| rng.random()))
}

fn benchmark<T: ArrowPrimitiveType>(c: &mut Criterion, name: &str)
where
    StandardUniform: Distribution<T::Native>,
{
    let array = random_array::<T>(SIZE);
    let nulls = NullBuffer::from_iter((0..SIZE).map(|i| i % 5 != 0));
    let with_nulls = PrimitiveArray::<T>::new(array.values().clone(), Some(nulls));
    let mut group = c.benchmark_group(name);
    for (op, kernel) in [("div", div as Kernel), ("rem", rem)] {
        // 16 is a power of two, which a precomputed divisor can replace with a shift or mask
        for d in [13, 16] {
            let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(d));
            group.throughput(Throughput::Elements(SIZE as u64));
            group.bench_function(format!("{op}_{d}/no_nulls"), |b| {
                b.iter(|| black_box(kernel(black_box(&array), black_box(&divisor)).unwrap()))
            });
        }
        // `try_unary` visits only the valid values when the array has nulls
        let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(13));
        group.bench_function(format!("{op}_13/mixed_nulls"), |b| {
            b.iter(|| black_box(kernel(black_box(&with_nulls), black_box(&divisor)).unwrap()))
        });
    }
    // Small arrays, where the cost of setting up a division can outweigh the per-value work
    let divisor = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(13));
    for len in [1, 16, 128] {
        let array = random_array::<T>(len);
        group.throughput(Throughput::Elements(len as u64));
        group.bench_function(format!("div_13/{len}_values"), |b| {
            b.iter(|| black_box(div(black_box(&array), black_box(&divisor)).unwrap()))
        });
    }
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
