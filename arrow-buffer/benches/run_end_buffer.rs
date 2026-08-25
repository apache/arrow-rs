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

use arrow_buffer::RunEndBuffer;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn get_physical_indices(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_physical_indices");

    for run_count in [1_024, 1_048_576] {
        let run_ends = (1..=run_count as i32).collect::<Vec<_>>();
        let buffer = RunEndBuffer::new(run_ends.into(), 0, run_count);
        let indices = [0_u32, 2];

        for (name, buffer) in [
            ("prefix", buffer.clone()),
            ("sliced_prefix", buffer.slice(run_count / 2, 3)),
        ] {
            group.bench_with_input(BenchmarkId::new(name, run_count), &buffer, |b, buffer| {
                b.iter(|| {
                    black_box(buffer)
                        .get_physical_indices(black_box(&indices))
                        .unwrap()
                })
            });
        }

        let indices = (0..run_count as u32).collect::<Vec<_>>();
        group.bench_with_input(
            BenchmarkId::new("all_indices", run_count),
            &buffer,
            |b, buffer| {
                b.iter(|| {
                    black_box(buffer)
                        .get_physical_indices(black_box(&indices))
                        .unwrap()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, get_physical_indices);
criterion_main!(benches);
