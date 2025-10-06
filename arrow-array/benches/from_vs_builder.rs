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

use arrow_array::{builder::PrimitiveBuilder, ArrowPrimitiveType, PrimitiveArray};
use criterion::*;
use std::hint;

fn generic_builder<T, V, F>(vec: Vec<V>, mut append: F) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    F: FnMut(&mut PrimitiveBuilder<T>, V),
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(vec.len());
    for v in vec {
        append(&mut builder, v);
    }
    builder.finish()
}

// From<Vec<T>>
fn generic_from<T, V>(vec: Vec<V>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    PrimitiveArray<T>: From<Vec<V>>,
{
    vec.into()
}

fn criterion_benchmark(c: &mut Criterion) {
    use arrow_array::types::Int32Type;

    let v1: Vec<i32> = (0..80586_i32).collect();

    c.bench_function("primitive_array_into_no_null", |b| {
        b.iter(|| hint::black_box(generic_from::<Int32Type, i32>(v1.clone())));
    });
    c.bench_function("builder_no_null", |b| {
        b.iter(|| {
            hint::black_box(generic_builder::<Int32Type, i32, _>(
                v1.clone(),
                |builder, v| builder.append_value(v),
            ))
        });
    });

    let mut v2: Vec<Option<i32>> = vec![];
    for i in 0..80586 {
        if i % 2 == 0 {
            v2.push(None);
        }
        v2.push(Some(i));
    }

    c.bench_function("primitive_array_into", |b| {
        b.iter(|| hint::black_box(generic_from::<Int32Type, Option<i32>>(v2.clone())));
    });
    c.bench_function("builder", |b| {
        b.iter(|| {
            hint::black_box(generic_builder::<Int32Type, Option<i32>, _>(
                v2.clone(),
                |builder, v| builder.append_option(v),
            ))
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
