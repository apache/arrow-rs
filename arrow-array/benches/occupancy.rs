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
use arrow_array::{DictionaryArray, Int32Array};
use arrow_buffer::NullBuffer;
use criterion::*;
use rand::{rng, Rng};
use std::sync::Arc;

fn gen_dict(
    len: usize,
    values_len: usize,
    occupancy: f64,
    null_percent: f64,
) -> DictionaryArray<Int32Type> {
    let mut rng = rng();
    let values = Int32Array::from(vec![0; values_len]);
    let max_key = (values_len as f64 * occupancy) as i32;
    let keys = (0..len).map(|_| rng.random_range(0..max_key)).collect();
    let nulls = (0..len).map(|_| !rng.random_bool(null_percent)).collect();

    let keys = Int32Array::new(keys, Some(NullBuffer::new(nulls)));
    DictionaryArray::new(keys, Arc::new(values))
}

fn criterion_benchmark(c: &mut Criterion) {
    for values in [10, 100, 512] {
        for occupancy in [1., 0.5, 0.1] {
            for null_percent in [0.0, 0.1, 0.5, 0.9] {
                let dict = gen_dict(1024, values, occupancy, null_percent);
                c.bench_function(&format!("occupancy(values: {values}, occupancy: {occupancy}, null_percent: {null_percent})"), |b| {
                    b.iter(|| {
                        black_box(&dict).occupancy()
                    });
                });
            }
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
