use arrow_buffer::{OffsetBuffer, OffsetBufferBuilder};
use criterion::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint;

const SIZE: usize = 1024;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let lengths: Vec<usize> = hint::black_box((0..SIZE).map(|_| rng.random_range(0..40)).collect());

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
        b.iter(|| OffsetBuffer::new(hint::black_box(offsets.clone())));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
