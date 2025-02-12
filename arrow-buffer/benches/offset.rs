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

use arrow_buffer::{OffsetBuffer, OffsetBufferBuilder};
use criterion::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const SIZE: usize = 1024;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let lengths: Vec<usize> = black_box((0..SIZE).map(|_| rng.gen_range(0..40)).collect());

    c.bench_function("OffsetBuffer::from_lengths", |b| {
        b.iter(|| OffsetBuffer::<i32>::from_lengths(lengths.iter().copied()));
    });

    c.bench_function("OffsetBufferBuilder::push_length", |b| {
        b.iter(|| {
            let mut builder = OffsetBufferBuilder::<i32>::new(lengths.len());
            lengths.iter().for_each(|x| builder.push_length(*x));
            builder.finish()
        });
    });

    let offsets = OffsetBuffer::<i32>::from_lengths(lengths.iter().copied()).into_inner();

    c.bench_function("OffsetBuffer::new", |b| {
        b.iter(|| OffsetBuffer::new(black_box(offsets.clone())));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
