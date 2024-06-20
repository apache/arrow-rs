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

use arrow_array::{StringArray, StringViewArray};
use arrow_ord::cmp::{eq, neq};
use criterion::*;
use rand::{prelude::SliceRandom, rngs::ThreadRng, thread_rng, Rng};

fn make_string_array(size: usize, rng: &mut ThreadRng) -> Vec<String> {
    (0..size)
        .map(|_| {
            let len = rng.gen_range(0..64);
            let bytes = (0..len).map(|_| rng.gen_range(0..128)).collect();
            String::from_utf8(bytes).unwrap()
        })
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut vec_array = make_string_array(1024 * 1024 * 8, &mut thread_rng());
    let string_left = StringArray::from_iter_values(&vec_array);
    let string_view_left = StringViewArray::from_iter_values(&vec_array);

    vec_array.shuffle(&mut thread_rng());
    let string_right = StringArray::from_iter_values(&vec_array);
    let string_view_right = StringViewArray::from_iter_values(&vec_array);

    c.bench_function("Eq/StringArray", |b| {
        b.iter(|| {
            black_box(eq(&string_left, &string_right).unwrap());
        });
    });

    c.bench_function("Eq/StringViewArray", |b| {
        b.iter(|| {
            black_box(eq(&string_view_left, &string_view_right).unwrap());
        })
    });

    c.bench_function("Neq/StringArray", |b| {
        b.iter(|| {
            black_box(neq(&string_left, &string_right).unwrap());
        });
    });

    c.bench_function("Neq/StringViewArray", |b| {
        b.iter(|| {
            black_box(neq(&string_view_left, &string_view_right).unwrap());
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
