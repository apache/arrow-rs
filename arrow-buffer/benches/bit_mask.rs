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

use arrow_buffer::bit_mask::set_bits;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bit_mask");

    for offset_write in [0, 5] {
        for offset_read in [0, 5] {
            for len in [1, 17, 65] {
                for datum in [0u8, 0xADu8] {
                    let x = (offset_write, offset_read, len, datum);
                    group.bench_with_input(
                        BenchmarkId::new(
                            "set_bits",
                            format!(
                                "offset_write_{}_offset_read_{}_len_{}_datum_{}",
                                x.0, x.1, x.2, x.3
                            ),
                        ),
                        &x,
                        |b, &x| {
                            b.iter(|| {
                                set_bits(
                                    black_box(&mut [0u8; 9]),
                                    black_box(&[x.3; 9]),
                                    black_box(x.0),
                                    black_box(x.1),
                                    black_box(x.2),
                                )
                            });
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
