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

#[macro_use]
extern crate criterion;

use criterion::{Criterion, Throughput};

extern crate arrow;

use arrow::buffer::{Buffer, MutableBuffer};
use std::hint;

///  Helper function to create arrays
fn create_buffer(size: usize) -> Buffer {
    let mut result = MutableBuffer::new(size).with_bitset(size, false);

    for i in 0..size {
        result.as_slice_mut()[i] = 0b01010101 << i << (i % 4);
    }

    result.into()
}

fn bench_buffer_and(left: &Buffer, right: &Buffer) {
    hint::black_box(left.bitwise_binary(right, 0, 0, left.len() * 8, |a, b| a & b));
}

fn bench_buffer_or(left: &Buffer, right: &Buffer) {
    hint::black_box(left.bitwise_binary(right, 0, 0, left.len() * 8, |a, b| a | b));
}

fn bench_buffer_not(buffer: &Buffer) {
    hint::black_box(buffer.bitwise_unary(0, buffer.len() * 8, |a| !a));
}

fn bench_buffer_and_with_offsets(
    left: &Buffer,
    left_offset: usize,
    right: &Buffer,
    right_offset: usize,
    len: usize,
) {
    hint::black_box(left.bitwise_binary(right, left_offset, right_offset, len, |a, b| a & b));
}

fn bench_buffer_or_with_offsets(
    left: &Buffer,
    left_offset: usize,
    right: &Buffer,
    right_offset: usize,
    len: usize,
) {
    hint::black_box(left.bitwise_binary(right, left_offset, right_offset, len, |a, b| a | b));
}

fn bench_buffer_not_with_offsets(buffer: &Buffer, offset: usize, len: usize) {
    hint::black_box(buffer.bitwise_unary(offset, len, |a| !a));
}

fn bench_mutable_buffer_and(left: &mut MutableBuffer, right: &Buffer) {
    hint::black_box(left.bitwise_binary_inplace(right, 0, 0, left.len() * 8, |a, b| a & b));
}

fn bench_mutable_buffer_or(left: &mut MutableBuffer, right: &Buffer) {
    hint::black_box(left.bitwise_binary_inplace(right, 0, 0, left.len() * 8, |a, b| a | b));
}

fn bench_mutable_buffer_not(buffer: &mut MutableBuffer) {
    hint::black_box(buffer.bitwise_unary_inplace(0, buffer.len() * 8, |a| !a));
}

fn bit_ops_benchmark(c: &mut Criterion) {
    let left = create_buffer(512 * 10);
    let right = create_buffer(512 * 10);
    let mut mutable_left = MutableBuffer::from(left.as_slice().to_vec());
    let mutable_right = Buffer::from(right.as_slice().to_vec());

    // Allocating benchmarks
    c.benchmark_group("buffer_bitwise_alloc_binary_ops")
        .throughput(Throughput::Bytes(3 * left.len() as u64))
        .bench_function("and", |b| b.iter(|| bench_buffer_and(&left, &right)))
        .bench_function("or", |b| b.iter(|| bench_buffer_or(&left, &right)))
        .bench_function("and_with_offset", |b| {
            b.iter(|| bench_buffer_and_with_offsets(&left, 1, &right, 2, left.len() * 8 - 5))
        })
        .bench_function("or_with_offset", |b| {
            b.iter(|| bench_buffer_or_with_offsets(&left, 1, &right, 2, left.len() * 8 - 5))
        });

    c.benchmark_group("buffer_bitwise_alloc_unary_ops")
        .throughput(Throughput::Bytes(2 * left.len() as u64))
        .bench_function("not", |b| b.iter(|| bench_buffer_not(&left)))
        .bench_function("not_with_offset", |b| {
            b.iter(|| bench_buffer_not_with_offsets(&left, 1, left.len() * 8 - 5))
        });

    // In-place benchmarks
    c.benchmark_group("buffer_bitwise_inplace_binary_ops")
        .throughput(Throughput::Bytes(2 * mutable_left.len() as u64))
        .bench_function("and", |b| b.iter(|| bench_mutable_buffer_and(&mut mutable_left, &mutable_right)))
        .bench_function("or", |b| b.iter(|| bench_mutable_buffer_or(&mut mutable_left, &mutable_right)));

    c.benchmark_group("buffer_bitwise_inplace_unary_ops")
        .throughput(Throughput::Bytes(mutable_left.len() as u64))
        .bench_function("not", |b| b.iter(|| bench_mutable_buffer_not(&mut mutable_left)));
}

criterion_group!(benches, bit_ops_benchmark);
criterion_main!(benches);
