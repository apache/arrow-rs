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

use std::hint;

use arrow_array::Int32Array;
use arrow_array::types::Int32Type;
use criterion::{Criterion, criterion_group, criterion_main};

const BATCH_SIZE: usize = 64 * 1024;

fn primitive_array_unary(c: &mut Criterion) {
    let arrays = [
        (
            "no_input_nulls",
            Int32Array::from_iter_values(0..BATCH_SIZE as i32),
        ),
        (
            "20pct_input_nulls",
            Int32Array::from_iter(
                (0..BATCH_SIZE as i32).map(|value| (value % 5 != 0).then_some(value)),
            ),
        ),
    ];

    let mut group = c.benchmark_group("primitive_array_unary");
    for (name, array) in arrays {
        group.bench_function(format!("try_unary/{name}"), |b| {
            b.iter(|| {
                hint::black_box(
                    array
                        .try_unary::<_, Int32Type, ()>(|value| Ok(value + 1))
                        .unwrap(),
                )
            })
        });
        group.bench_function(format!("unary_opt/{name}"), |b| {
            b.iter(|| {
                hint::black_box(
                    array.unary_opt::<_, Int32Type>(|value| (value % 7 != 0).then_some(value + 1)),
                )
            })
        });
    }
}

criterion_group!(benches, primitive_array_unary);
criterion_main!(benches);
