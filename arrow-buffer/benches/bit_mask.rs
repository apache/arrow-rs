use arrow_buffer::bit_mask::set_bits;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint;

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
                                    hint::black_box(&mut [0u8; 9]),
                                    hint::black_box(&[x.3; 9]),
                                    hint::black_box(x.0),
                                    hint::black_box(x.1),
                                    hint::black_box(x.2),
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
