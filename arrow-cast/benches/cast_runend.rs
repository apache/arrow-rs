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

use std::iter;

use arrow_array::{Int32Array, RunArray};
use arrow_schema::DataType;
use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let run_ends: Vec<i32> = (1..1_000_001).map(|x| x * 10).collect();
    let values: Vec<i32> = iter::repeat(100).take(run_ends.len()).collect();
    let ra = RunArray::try_new(&Int32Array::from(run_ends), &Int32Array::from(values)).unwrap();

    c.bench_function("cast run end to flat", |b| {
        b.iter(|| arrow_cast::cast(&ra, &DataType::Int32).unwrap());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
