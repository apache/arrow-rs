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

use arrow::array::BooleanBufferBuilder;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};

fn rand_bytes(len: usize) -> Vec<u8> {
    let mut rng = thread_rng();
    let mut buf = vec![0_u8; len];
    rng.fill(buf.as_mut_slice());
    buf
}

fn boolean_append_packed(c: &mut Criterion) {
    let mut rng = thread_rng();
    let source = rand_bytes(1024);
    let ranges: Vec<_> = (0..100)
        .map(|_| {
            let start: usize = rng.gen_range(0..1024 * 8);
            let end: usize = rng.gen_range(start..1024 * 8);
            start..end
        })
        .collect();

    let total_bits: usize = ranges.iter().map(|x| x.end - x.start).sum();

    c.bench_function("boolean_append_packed", |b| {
        b.iter(|| {
            let mut buffer = BooleanBufferBuilder::new(total_bits);
            for range in &ranges {
                buffer.append_packed_range(range.clone(), &source);
            }
            assert_eq!(buffer.len(), total_bits);
        })
    });
}

criterion_group!(benches, boolean_append_packed);
criterion_main!(benches);
