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

use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, RunArray, StringArray};
use criterion::*;
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    for physical_array_len in [256, 1024, 4096, 8192] {
        // Strictly increasing, strictly positive run ends `[1, 2, ..., physical]`,
        // with one value per run.
        let run_ends = Int32Array::from_iter_values(1..=physical_array_len);

        // Primitive (i32) values.
        let i32_values = Int32Array::from_iter_values(0..physical_array_len);
        c.bench_function(
            &format!("try_new(values: i32, physical_array_len: {physical_array_len})"),
            |b| {
                b.iter(|| {
                    hint::black_box(
                        RunArray::<Int32Type>::try_new(&run_ends, &i32_values).unwrap(),
                    );
                });
            },
        );

        // Utf8 (string) values.
        let utf8_values = StringArray::from_iter_values(
            (0..physical_array_len).map(|i| format!("value-{i:028}")),
        );
        c.bench_function(
            &format!("try_new(values: utf8, physical_array_len: {physical_array_len})"),
            |b| {
                b.iter(|| {
                    hint::black_box(
                        RunArray::<Int32Type>::try_new(&run_ends, &utf8_values).unwrap(),
                    );
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
