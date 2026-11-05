#[macro_use]
extern crate criterion;

use criterion::{Criterion, Throughput};

use arrow::buffer::{Buffer, MutableBuffer, buffer_bin_and, buffer_bin_or, buffer_unary_not};
use std::hint;

///  Helper function to create arrays
fn create_buffer(size: usize) -> Buffer {
    let mut result = MutableBuffer::new(size).with_bitset(size, false);

    for i in 0..size {
        result.as_slice_mut()[i] = 0b01010101u8
            .wrapping_shl(i as u32)
            .wrapping_shl((i % 4) as u32);
    }

    result.into()
}

fn bench_buffer_and(left: &Buffer, right: &Buffer) {
    hint::black_box(buffer_bin_and(left, 0, right, 0, left.len() * 8));
}

fn bench_buffer_or(left: &Buffer, right: &Buffer) {
    hint::black_box(buffer_bin_or(left, 0, right, 0, left.len() * 8));
}

fn bench_buffer_not(buffer: &Buffer) {
    hint::black_box(buffer_unary_not(buffer, 0, buffer.len() * 8));
}

fn bench_buffer_and_with_offsets(
    left: &Buffer,
    left_offset: usize,
    right: &Buffer,
    right_offset: usize,
    len: usize,
) {
    hint::black_box(buffer_bin_and(left, left_offset, right, right_offset, len));
}

fn bench_buffer_or_with_offsets(
    left: &Buffer,
    left_offset: usize,
    right: &Buffer,
    right_offset: usize,
    len: usize,
) {
    hint::black_box(buffer_bin_or(left, left_offset, right, right_offset, len));
}

fn bench_buffer_not_with_offsets(buffer: &Buffer, offset: usize, len: usize) {
    hint::black_box(buffer_unary_not(buffer, offset, len));
}

fn bit_ops_benchmark(c: &mut Criterion) {
    let left = create_buffer(512 * 10);
    let right = create_buffer(512 * 10);

    c.benchmark_group("buffer_binary_ops")
        .throughput(Throughput::Bytes(3 * left.len() as u64))
        .bench_function("and", |b| b.iter(|| bench_buffer_and(&left, &right)))
        .bench_function("or", |b| b.iter(|| bench_buffer_or(&left, &right)))
        .bench_function("and_with_offset", |b| {
            b.iter(|| bench_buffer_and_with_offsets(&left, 1, &right, 2, left.len() * 8 - 5))
        })
        .bench_function("or_with_offset", |b| {
            b.iter(|| bench_buffer_or_with_offsets(&left, 1, &right, 2, left.len() * 8 - 5))
        });

    c.benchmark_group("buffer_unary_ops")
        .throughput(Throughput::Bytes(2 * left.len() as u64))
        .bench_function("not", |b| b.iter(|| bench_buffer_not(&left)))
        .bench_function("not_with_offset", |b| {
            b.iter(|| bench_buffer_not_with_offsets(&left, 1, left.len() * 8 - 5))
        });
}

criterion_group!(benches, bit_ops_benchmark);
criterion_main!(benches);
