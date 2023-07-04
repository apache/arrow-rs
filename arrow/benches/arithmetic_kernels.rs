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

use criterion::*;

extern crate arrow;

use arrow::compute::kernels::numeric::*;
use arrow::datatypes::Float32Type;
use arrow::util::bench_util::*;
use arrow_array::Scalar;

fn add_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 64 * 1024;
    for null_density in [0., 0.1, 0.5, 0.9, 1.0] {
        let arr_a = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let arr_b = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let scalar_a = create_primitive_array::<Float32Type>(1, 0.);
        let scalar = Scalar::new(&scalar_a);

        c.bench_function(&format!("add({null_density})"), |b| {
            b.iter(|| criterion::black_box(add_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_checked({null_density})"), |b| {
            b.iter(|| criterion::black_box(add(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_scalar({null_density})"), |b| {
            b.iter(|| criterion::black_box(add_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("subtract({null_density})"), |b| {
            b.iter(|| criterion::black_box(sub_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_checked({null_density})"), |b| {
            b.iter(|| criterion::black_box(sub(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_scalar({null_density})"), |b| {
            b.iter(|| criterion::black_box(sub_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("multiply({null_density})"), |b| {
            b.iter(|| criterion::black_box(mul_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_checked({null_density})"), |b| {
            b.iter(|| criterion::black_box(mul(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_scalar({null_density})"), |b| {
            b.iter(|| criterion::black_box(mul_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("divide({null_density})"), |b| {
            b.iter(|| criterion::black_box(div(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("divide_scalar({null_density})"), |b| {
            b.iter(|| criterion::black_box(div(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("modulo({null_density})"), |b| {
            b.iter(|| criterion::black_box(rem(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("modulo_scalar({null_density})"), |b| {
            b.iter(|| criterion::black_box(rem(&arr_a, &scalar).unwrap()))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
