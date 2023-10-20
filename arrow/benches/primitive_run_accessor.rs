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

use arrow::datatypes::Int32Type;
use arrow::{array::PrimitiveArray, util::bench_util::create_primitive_run_array};
use arrow_array::ArrayAccessor;
use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_run_accessor");

    let mut do_bench = |physical_array_len: usize, logical_array_len: usize| {
        group.bench_function(
            format!("(run_array_len:{logical_array_len}, physical_array_len:{physical_array_len})"),
            |b| {
                let run_array = create_primitive_run_array::<Int32Type, Int32Type>(
                    logical_array_len,
                    physical_array_len,
                );
                let typed = run_array.downcast::<PrimitiveArray<Int32Type>>().unwrap();
                b.iter(|| {
                    for i in 0..logical_array_len {
                        let _ = unsafe { typed.value_unchecked(i) };
                    }
                })
            },
        );
    };

    do_bench(128, 512);
    do_bench(256, 1024);
    do_bench(512, 2048);
    do_bench(1024, 4096);
    do_bench(2048, 8192);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
