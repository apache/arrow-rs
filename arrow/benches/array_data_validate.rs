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
use criterion::Criterion;

extern crate arrow;

use arrow::{array::*, buffer::Buffer, datatypes::DataType};

fn create_binary_array_data(length: i32) -> ArrayData {
    let value_buffer = Buffer::from_iter(0_i32..length);
    let offsets_buffer = Buffer::from_iter(0_i32..length + 1);
    ArrayData::try_new(
        DataType::Binary,
        length as usize,
        None,
        None,
        0,
        vec![offsets_buffer, value_buffer],
        vec![],
    )
    .unwrap()
}

fn array_slice_benchmark(c: &mut Criterion) {
    c.bench_function("validate_binary_array_data 20000", |b| {
        b.iter(|| create_binary_array_data(20000))
    });
}

criterion_group!(benches, array_slice_benchmark);
criterion_main!(benches);
