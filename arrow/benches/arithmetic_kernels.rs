use criterion::*;

use arrow::compute::kernels::numeric::*;
use arrow::datatypes::Float32Type;
use arrow::util::bench_util::*;
use arrow_array::Scalar;
use std::hint;

fn add_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 64 * 1024;
    for null_density in [0., 0.1, 0.5, 0.9, 1.0] {
        let arr_a = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let arr_b = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let scalar_a = create_primitive_array::<Float32Type>(1, 0.);
        let scalar = Scalar::new(&scalar_a);

        c.bench_function(&format!("add({null_density})"), |b| {
            b.iter(|| hint::black_box(add_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(add(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(add_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("subtract({null_density})"), |b| {
            b.iter(|| hint::black_box(sub_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(sub(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(sub_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("multiply({null_density})"), |b| {
            b.iter(|| hint::black_box(mul_wrapping(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_checked({null_density})"), |b| {
            b.iter(|| hint::black_box(mul(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(mul_wrapping(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("divide({null_density})"), |b| {
            b.iter(|| hint::black_box(div(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("divide_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(div(&arr_a, &scalar).unwrap()))
        });
        c.bench_function(&format!("modulo({null_density})"), |b| {
            b.iter(|| hint::black_box(rem(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("modulo_scalar({null_density})"), |b| {
            b.iter(|| hint::black_box(rem(&arr_a, &scalar).unwrap()))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
