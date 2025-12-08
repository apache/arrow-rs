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

use arrow_buffer::Buffer;
use criterion::*;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("MutableBuffer repeat slice");
    let mut rng = StdRng::seed_from_u64(42);

    for slice_length in [3, 20, 100] {
        let slice_to_repeat: Vec<u8> = hint::black_box(
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(slice_length)
                .collect(),
        );
        let slice_to_repeat: &[u8] = slice_to_repeat.as_ref();

        for repeat_count in [3, 64, 1024, 8192] {
            let parameter_string = format!("slice_len={slice_length} n={repeat_count}");

            group.bench_with_input(
                BenchmarkId::new("repeat_slice_n_times", &parameter_string),
                &(repeat_count),
                |b, &repeat_count| {
                    b.iter(|| {
                        let mut mutable_buffer = arrow_buffer::MutableBuffer::with_capacity(0);

                        mutable_buffer.repeat_slice_n_times(slice_to_repeat, repeat_count);

                        Buffer::from(mutable_buffer)
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("extend_from_slice loop", &parameter_string),
                &(repeat_count),
                |b, &repeat_count| {
                    b.iter(|| {
                        let mut mutable_buffer = arrow_buffer::MutableBuffer::with_capacity(
                            size_of_val(slice_to_repeat) * repeat_count,
                        );

                        for _ in 0..repeat_count {
                            mutable_buffer.extend_from_slice(slice_to_repeat);
                        }

                        Buffer::from(mutable_buffer)
                    })
                },
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
