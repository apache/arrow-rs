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

mod variant_get;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet_variant_compute::variant_get as get;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("variant_get_list_validity");
    variant_get::for_each_fixture(|fixture, rows| {
        // The shared preflight labels the old implementation's known validity bug.
        black_box(fixture.known_missing_as_null);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new(&fixture.name, rows), |b| {
            b.iter(|| {
                // Options cloning and output destruction are both timed.
                drop(black_box(
                    get(black_box(&fixture.input), fixture.options.clone()).unwrap(),
                ));
            });
        });
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
