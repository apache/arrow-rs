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
use arrow_array::{DictionaryArray, StringArray, UInt16Array};
use std::sync::Arc;

use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let dict_vals = Arc::new(StringArray::from_iter_values(["a", "b", "c"]));
    for len in [128, 1536, 8092] {
        c.bench_function(&format!("null_dict/len={len}"), |b| {
            b.iter_batched(
                || dict_vals.clone(),
                |dict_vals| {
                    std::hint::black_box(DictionaryArray::new(
                        UInt16Array::new_null(len),
                        dict_vals.clone(),
                    ))
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
