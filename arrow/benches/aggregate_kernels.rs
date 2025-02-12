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
        let nonnull_strings = create_string_array_with_len::<i32>(BATCH_SIZE, 0.0, 16);
        let nullable_strings = create_string_array_with_len::<i32>(BATCH_SIZE, 0.5, 16);
        c.benchmark_group("string")
            .throughput(Throughput::Elements(BATCH_SIZE as u64))
            .bench_function("min nonnull", |b| b.iter(|| min_string(&nonnull_strings)))
            .bench_function("max nonnull", |b| b.iter(|| max_string(&nonnull_strings)))
            .bench_function("min nullable", |b| b.iter(|| min_string(&nullable_strings)))
            .bench_function("max nullable", |b| b.iter(|| max_string(&nullable_strings)));
    }

    {
        let nonnull_strings = create_string_view_array_with_len(BATCH_SIZE, 0.0, 16, false);
        let nullable_strings = create_string_view_array_with_len(BATCH_SIZE, 0.5, 16, false);
        c.benchmark_group("string view")
            .throughput(Throughput::Elements(BATCH_SIZE as u64))
            .bench_function("min nonnull", |b| {
                b.iter(|| min_string_view(&nonnull_strings))
            })
            .bench_function("max nonnull", |b| {
                b.iter(|| max_string_view(&nonnull_strings))
            })
            .bench_function("min nullable", |b| {
                b.iter(|| min_string_view(&nullable_strings))
            })
            .bench_function("max nullable", |b| {
                b.iter(|| max_string_view(&nullable_strings))
            });
    }

    {
        let nonnull_bools_mixed = create_boolean_array(BATCH_SIZE, 0.0, 0.5);
        let nonnull_bools_all_false = create_boolean_array(BATCH_SIZE, 0.0, 0.0);
        let nonnull_bools_all_true = create_boolean_array(BATCH_SIZE, 0.0, 1.0);
        let nullable_bool_mixed = create_boolean_array(BATCH_SIZE, 0.5, 0.5);
        let nullable_bool_all_false = create_boolean_array(BATCH_SIZE, 0.5, 0.0);
        let nullable_bool_all_true = create_boolean_array(BATCH_SIZE, 0.5, 1.0);
        c.benchmark_group("bool")
            .throughput(Throughput::Elements(BATCH_SIZE as u64))
            .bench_function("min nonnull mixed", |b| {
                b.iter(|| min_boolean(&nonnull_bools_mixed))
            })
            .bench_function("max nonnull mixed", |b| {
                b.iter(|| max_boolean(&nonnull_bools_mixed))
            })
            .bench_function("or nonnull mixed", |b| {
                b.iter(|| bool_or(&nonnull_bools_mixed))
            })
            .bench_function("and nonnull mixed", |b| {
                b.iter(|| bool_and(&nonnull_bools_mixed))
            })
            .bench_function("min nonnull false", |b| {
                b.iter(|| min_boolean(&nonnull_bools_all_false))
            })
            .bench_function("max nonnull false", |b| {
                b.iter(|| max_boolean(&nonnull_bools_all_false))
            })
            .bench_function("or nonnull false", |b| {
                b.iter(|| bool_or(&nonnull_bools_all_false))
            })
            .bench_function("and nonnull false", |b| {
                b.iter(|| bool_and(&nonnull_bools_all_false))
            })
            .bench_function("min nonnull true", |b| {
                b.iter(|| min_boolean(&nonnull_bools_all_true))
            })
            .bench_function("max nonnull true", |b| {
                b.iter(|| max_boolean(&nonnull_bools_all_true))
            })
            .bench_function("or nonnull true", |b| {
                b.iter(|| bool_or(&nonnull_bools_all_true))
            })
            .bench_function("and nonnull true", |b| {
                b.iter(|| bool_and(&nonnull_bools_all_true))
            })
            .bench_function("min nullable mixed", |b| {
                b.iter(|| min_boolean(&nullable_bool_mixed))
            })
            .bench_function("max nullable mixed", |b| {
                b.iter(|| max_boolean(&nullable_bool_mixed))
            })
            .bench_function("or nullable mixed", |b| {
                b.iter(|| bool_or(&nullable_bool_mixed))
            })
            .bench_function("and nullable mixed", |b| {
                b.iter(|| bool_and(&nullable_bool_mixed))
            })
            .bench_function("min nullable false", |b| {
                b.iter(|| min_boolean(&nullable_bool_all_false))
            })
            .bench_function("max nullable false", |b| {
                b.iter(|| max_boolean(&nullable_bool_all_false))
            })
            .bench_function("or nullable false", |b| {
                b.iter(|| bool_or(&nullable_bool_all_false))
            })
            .bench_function("and nullable false", |b| {
                b.iter(|| bool_and(&nullable_bool_all_false))
            })
            .bench_function("min nullable true", |b| {
                b.iter(|| min_boolean(&nullable_bool_all_true))
            })
            .bench_function("max nullable true", |b| {
                b.iter(|| max_boolean(&nullable_bool_all_true))
            })
            .bench_function("or nullable true", |b| {
                b.iter(|| bool_or(&nullable_bool_all_true))
            })
            .bench_function("and nullable true", |b| {
                b.iter(|| bool_and(&nullable_bool_all_true))
            });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
