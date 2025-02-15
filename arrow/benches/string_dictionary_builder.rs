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

use arrow::array::StringDictionaryBuilder;
use arrow::datatypes::Int32Type;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};

/// Note: this is best effort, not all keys are necessarily present or unique
fn build_strings(dict_size: usize, total_size: usize, key_len: usize) -> Vec<String> {
    let mut rng = thread_rng();
    let values: Vec<String> = (0..dict_size)
        .map(|_| (0..key_len).map(|_| rng.gen::<char>()).collect())
        .collect();

    (0..total_size)
        .map(|_| values[rng.gen_range(0..dict_size)].clone())
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_dictionary_builder");

    let mut do_bench = |dict_size: usize, total_size: usize, key_len: usize| {
        group.bench_function(
            format!("(dict_size:{dict_size}, len:{total_size}, key_len: {key_len})"),
            |b| {
                let strings = build_strings(dict_size, total_size, key_len);
                b.iter(|| {
                    let mut builder = StringDictionaryBuilder::<Int32Type>::with_capacity(
                        strings.len(),
                        key_len + 1,
                        (key_len + 1) * dict_size,
                    );

                    for val in &strings {
                        builder.append(val).unwrap();
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
