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
use arrow::util::bench_util::create_primitive_array;
use arrow_arith::arithmetic::{add_scalar_dyn, add_scalar_dyn_mut};
use arrow_array::types::Float32Type;
use arrow_array::{make_array, Array, ArrayRef, PrimitiveArray};
use arrow_schema::ArrowError;
use criterion::Criterion;

extern crate arrow;

const BATCH_SIZE: i32 = 163840;
const BATCH_NUM: i32 = 1024 * 10;
fn add_cow() {
    let mut vec = vec![];
    for i in 0..BATCH_NUM {
        let array = PrimitiveArray::<Float32Type>::from_iter_values(
            (0..BATCH_SIZE).map(|x| x as f32),
        );
        vec.push(make_array(array.into_data()));
    }

    for i in 0..vec.len() {
        add_scalar_dyn_mut::<Float32Type>(vec.pop().unwrap(), 1 as f32)
            .expect("panic message");
    }
}

fn add_copy() {
    let mut vec = vec![];
    for i in 0..BATCH_NUM {
        let array = PrimitiveArray::<Float32Type>::from_iter_values(
            (0..BATCH_SIZE).map(|x| x as f32),
        );
        vec.push(make_array(array.into_data()));
    }

    for i in 0..vec.len() {
        add_scalar_dyn::<Float32Type>(vec.pop().unwrap().as_ref(), 1 as f32)
            .expect("panic message");
    }
}

fn cow_benchmark(c: &mut Criterion) {
    c.bench_function(&format!("add"), |b| {
        b.iter(|| criterion::black_box(add_cow()))
    });
}

criterion_group!(benches, cow_benchmark);
criterion_main!(benches);
