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

    for run_count in [32, 1_024, 1_048_576] {
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

        let last = run_count as u32 - 1;
        for (name, indices) in [
            ("last_single", vec![last]),
            ("last_pair", vec![last - 1, last]),
            ("prefix_eight", (0..8).collect()),
            (
                "distant_eight",
                (0..8).map(|index| index * last / 7).collect(),
            ),
            ("prefix_nine", (0..9).collect()),
            ("all_indices", (0..run_count as u32).collect()),
        ] {
            group.bench_with_input(BenchmarkId::new(name, run_count), &buffer, |b, buffer| {
                b.iter(|| {
                    black_box(buffer)
                        .get_physical_indices(black_box(&indices))
                        .unwrap()
                })
            });
        }
    }

    // Check the lookup-count and remaining-run boundaries with a large backing buffer.
    let run_count = 1_048_576;
    let run_ends = (1..=run_count as i32).collect::<Vec<_>>();
    let buffer = RunEndBuffer::new(run_ends.into(), 0, run_count);
    for remaining_runs in [1_023, 1_024, 1_025] {
        let sliced = buffer.slice(run_count - remaining_runs, remaining_runs);
        for (name, index_count) in [("sliced_distant_eight", 8), ("sliced_distant_nine", 9)] {
            let indices = (0..index_count)
                .map(|index| index * (remaining_runs as u32 - 1) / (index_count - 1))
                .collect::<Vec<_>>();
            group.bench_with_input(
                BenchmarkId::new(name, remaining_runs),
                &sliced,
                |b, buffer| {
                    b.iter(|| {
                        black_box(buffer)
                            .get_physical_indices(black_box(&indices))
                            .unwrap()
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, get_physical_indices);
criterion_main!(benches);
