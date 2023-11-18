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

#[macro_use]
extern crate criterion;
use criterion::{Criterion, Throughput};
use rand::distributions::{Distribution, Standard};

extern crate arrow;

use arrow::compute::kernels::aggregate::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use arrow_array::types::{Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};

const BATCH_SIZE: usize = 64 * 1024;

fn primitive_benchmark<T: ArrowNumericType>(c: &mut Criterion, name: &str)
where
    Standard: Distribution<T::Native>,
{
    let nonnull_array = create_primitive_array::<T>(BATCH_SIZE, 0.0);
    let nullable_array = create_primitive_array::<T>(BATCH_SIZE, 0.5);
    c.benchmark_group(name)
        .throughput(Throughput::Bytes(
            (std::mem::size_of::<T::Native>() * BATCH_SIZE) as u64,
        ))
        .bench_function("sum nonnull", |b| b.iter(|| sum(&nonnull_array)))
        .bench_function("min nonnull", |b| b.iter(|| min(&nonnull_array)))
        .bench_function("max nonnull", |b| b.iter(|| max(&nonnull_array)))
        .bench_function("sum nullable", |b| b.iter(|| sum(&nullable_array)))
        .bench_function("min nullable", |b| b.iter(|| min(&nullable_array)))
        .bench_function("max nullable", |b| b.iter(|| max(&nullable_array)));
}

fn add_benchmark(c: &mut Criterion) {
    primitive_benchmark::<Float32Type>(c, "float32");
    primitive_benchmark::<Float64Type>(c, "float64");

    primitive_benchmark::<Int8Type>(c, "int8");
    primitive_benchmark::<Int16Type>(c, "int16");
    primitive_benchmark::<Int32Type>(c, "int32");
    primitive_benchmark::<Int64Type>(c, "int64");

    {
        let nonnull_strings = create_string_array::<i32>(BATCH_SIZE, 0.0);
        let nullable_strings = create_string_array::<i32>(BATCH_SIZE, 0.5);
        c.benchmark_group("string")
            .throughput(Throughput::Elements(BATCH_SIZE as u64))
            .bench_function("min nonnull", |b| b.iter(|| min_string(&nonnull_strings)))
            .bench_function("max nonnull", |b| b.iter(|| max_string(&nonnull_strings)))
            .bench_function("min nullable", |b| b.iter(|| min_string(&nullable_strings)))
            .bench_function("max nullable", |b| b.iter(|| max_string(&nullable_strings)));
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
