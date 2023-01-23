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

use arrow::array::StringRunBuilder;
use arrow::datatypes::Int32Type;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};

fn build_strings(
    physical_array_len: usize,
    logical_array_len: usize,
    string_len: usize,
) -> Vec<String> {
    let mut rng = thread_rng();
    let run_len = logical_array_len / physical_array_len;
    let mut values: Vec<String> = (0..physical_array_len)
        .map(|_| (0..string_len).map(|_| rng.gen::<char>()).collect())
        .flat_map(|s| std::iter::repeat(s).take(run_len))
        .collect();
    while values.len() < logical_array_len {
        let last_val = values[values.len() - 1].clone();
        values.push(last_val);
    }
    values
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_run_builder");

    let mut do_bench =
        |physical_array_len: usize, logical_array_len: usize, string_len: usize| {
            group.bench_function(
                format!(
                    "(run_array_len:{}, physical_array_len:{}, string_len: {})",
                    logical_array_len, physical_array_len, string_len
                ),
                |b| {
                    let strings =
                        build_strings(physical_array_len, logical_array_len, string_len);
                    b.iter(|| {
                        let mut builder = StringRunBuilder::<Int32Type>::with_capacity(
                            physical_array_len,
                            (string_len + 1) * physical_array_len,
                        );

                        for val in &strings {
                            builder.append_value(val);
                        }

                        builder.finish();
                    })
                },
            );
        };

    do_bench(20, 1000, 5);
    do_bench(100, 1000, 5);
    do_bench(100, 1000, 10);
    do_bench(100, 10000, 10);
    do_bench(100, 10000, 100);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
